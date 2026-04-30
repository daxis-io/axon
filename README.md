# Axon

Axon is a Rust workspace for a hybrid query engine over Delta Lake tables. The same `QueryRequest` runs inside a browser tab against cloud hosted Parquet files, or falls back to a native DataFusion runtime when the query needs more than the browser slice supports.

Status: early. The browser path is narrow today. It ships a deterministic planner and a small executor over a curated SQL subset. The native path is the correctness oracle. See [Scope and status](#scope-and-status).

## Why it exists

Most analytics stacks round-trip every query through a query service. The browser asks the service, the service reads object storage, the service returns rows. Every dashboard needs that service running, and interactive exploration is bottlenecked by it.

Axon takes a different approach:

* If a query is safe to run in a browser tab, fetch only the Parquet byte ranges it needs over signed URLs and run it there.
* Otherwise, route the same `QueryRequest` to the native DataFusion runtime.
* Share one query contract, one Delta snapshot resolver, and one fallback taxonomy across both tiers.

## How it works

```text
            ┌──────────────────────────────┐
            │       Caller (browser)       │
            │   QueryRequest → axon_table  │
            └──────────────┬───────────────┘
                           │
                           ▼
            ┌──────────────────────────────┐
            │   query-router: browser?     │
            │   (capabilities + policy)    │
            └────────┬───────────┬─────────┘
            yes      │           │     no / fallback
                     ▼           ▼
   ┌──────────────────────┐   ┌──────────────────────┐
   │  Browser runtime     │   │  Native runtime      │
   │  (WASM)              │   │  (DataFusion +       │
   │   HTTP range reads   │   │   delta-rs)          │
   │   Parquet planning   │   │   Full SQL           │
   │   Narrow executor    │   │   Oracle for tests   │
   └──────────┬───────────┘   └──────────┬───────────┘
              │                          │
              └────────────┬─────────────┘
                           ▼
            ┌──────────────────────────────┐
            │   delta-control-plane        │
            │   (snapshot resolution +     │
            │    table policy)             │
            └──────────────────────────────┘
```

Three rules keep the tiers consistent:

1. **One contract.** `query-contract` defines the request, response, capability flags, and structured fallback reasons that both runtimes return.
2. **Native is the oracle.** Every browser SQL case has a native counterpart in the test corpus. Results must match or the browser path fails closed to the native runtime.
3. **No silent capability drift.** Anything the browser cannot do (unsupported aggregate, partition without a known type, multi-partition execution, missing footer stats, identity drift between bootstrap and read) returns a structured fallback instead of a wrong answer.

## Repo tour

The Rust workspace lives in [`crates/`](crates/), grouped by role.

### Shared contract

* [`query-contract`](crates/query-contract/). Request and response types, capability flags, fallback reasons.
* [`query-router`](crates/query-router/). Decides browser vs. native and produces structured fallback decisions.

### Native tier

* [`native-query-runtime`](crates/native-query-runtime/). DataFusion plus delta-rs reference runtime. The correctness oracle.
* [`delta-runtime-support`](crates/delta-runtime-support/). Feature detection and error mapping shared by Delta aware code.

### Browser tier (compiles to `wasm32-unknown-unknown`)

* [`wasm-http-object-store`](crates/wasm-http-object-store/). Validated HTTP byte range reads with extent caching. Redacts URL secrets in errors.
* [`wasm-parquet-engine`](crates/wasm-parquet-engine/). Browser side Parquet planning and async footer plus scan primitives.
* [`wasm-delta-snapshot`](crates/wasm-delta-snapshot/). Browser safe Delta snapshot reconstruction (log replay plus checkpoints).
* [`wasm-query-runtime`](crates/wasm-query-runtime/). Constrained browser runtime envelope. Bootstraps snapshots, plans, prunes, and runs the supported SQL subset.
* [`wasm-query-session`](crates/wasm-query-session/). In memory session shell. Caches materialized and bootstrapped snapshots across queries with a memory budget.
* [`browser-sdk`](crates/browser-sdk/). Embedding surface. Worker request envelopes, Arrow IPC results, fallback propagation.
* [`browser-engine-worker`](crates/browser-engine-worker/). Linked worker artifact used to measure WASM size, cold start, and memory footprint.

### Trusted control plane

* [`delta-control-plane`](crates/delta-control-plane/). Snapshot resolution and table policy enforcement. Mints the descriptor seam that a (not yet shipped) signing service will fill in with per file URLs.

### Scaffolds (not yet wired up)

* [`udf-abi`](crates/udf-abi/), [`udf-host-wasi`](crates/udf-host-wasi/). Placeholders for hosted UDF execution.

## Scope and status

### What works in repo today

* Native SQL over Delta tables, with snapshot pinning, partition pruning, and execution derived metrics.
* A browser runtime that bootstraps a snapshot, plans a candidate file set, prunes partitions and integer footer stats, and executes a curated SQL subset (filter, project, group, the common aggregates, output aligned `ORDER BY` / `LIMIT`).
* Delta snapshot reconstruction is already repo-owned in `crates/wasm-delta-snapshot`; the shipped browser V1 remains narrow runtime + streaming scan + in-memory session shell.
* A query router that returns structured fallback decisions instead of guessing.
* CI gates for `wasm32` build, host tests, WASM smoke tests, a real `browser-engine-worker.wasm` size budget, and dependency guardrails that prevent cloud SDKs from leaking into browser bundles.

### What is not in this repo yet

* A `services/query-api` HTTP service. signed URL issuance, proxy-mode request issuance, audit logging, request correlation, and CORS/origin validation are external blockers.
* OPFS / IndexedDB persistent caches. The hook trait exists lower in the stack, but the session shell is in-memory only and no backend ships.
* A broad browser DataFusion engine. The browser path is a focused interpreter, not a general SQL engine.

The full launch checklist lives in [`docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md`](docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md). External dependencies are tracked in [`docs/release-gates/browser-wasm-delta-gcs-external-blockers.md`](docs/release-gates/browser-wasm-delta-gcs-external-blockers.md).

## Quick start

```bash
# Build the workspace.
cargo check --workspace

# Run the host side tests for the contract and the two runtimes.
cargo test -p query-contract
cargo test -p native-query-runtime
cargo test -p wasm-query-runtime

# Confirm the browser crates still compile to wasm.
cargo check \
  -p wasm-query-runtime -p wasm-http-object-store \
  -p wasm-parquet-engine -p wasm-delta-snapshot \
  -p browser-sdk -p browser-engine-worker \
  --target wasm32-unknown-unknown
```

For the WASM smoke suites and the worker artifact gates, see [Development](#development).

## Going deeper

* Architecture and intent: [`docs/program/browser-lakehouse-engine-strategy.md`](docs/program/browser-lakehouse-engine-strategy.md)
* Release handoff and integration runbook: [`docs/program/browser-lakehouse-release-handoff.md`](docs/program/browser-lakehouse-release-handoff.md), [`docs/program/browser-release-integration-runbook.md`](docs/program/browser-release-integration-runbook.md)
* Browser dependency review: [`docs/program/browser-dependency-compatibility-review-checklist.md`](docs/program/browser-dependency-compatibility-review-checklist.md)
* Observability contract: [`docs/program/browser-observability-contract.md`](docs/program/browser-observability-contract.md)
* ADRs and epic notes: [`docs/adr/`](docs/adr/), [`docs/epics/`](docs/epics/)
* Security reporting: [`SECURITY.md`](SECURITY.md)

## Development

### WASM smoke suites

```bash
cargo install wasm-bindgen-cli --version 0.2.114 --locked

cargo test -p browser-sdk            --target wasm32-unknown-unknown --locked --test wasm_smoke
cargo test -p wasm-parquet-engine    --target wasm32-unknown-unknown --locked --test wasm_smoke
cargo test -p wasm-delta-snapshot    --target wasm32-unknown-unknown --locked --test wasm_smoke
cargo test -p wasm-query-runtime     --target wasm32-unknown-unknown --locked --test wasm_smoke
cargo test -p wasm-http-object-store --target wasm32-unknown-unknown --locked --test wasm_smoke
cargo test -p browser-engine-worker  --target wasm32-unknown-unknown --locked --test wasm_smoke -- --nocapture
```

### Worker artifact and security gates

```bash
cargo test -p browser-engine-worker --locked
bash tests/perf/report_browser_worker_artifact.sh
bash tests/security/verify_browser_dependency_guardrails.sh
```

See [`tests/perf/README.md`](tests/perf/README.md) and [`tests/security/README.md`](tests/security/README.md) for the size, startup, footprint, and dependency guardrail gates.

### Optional GCS smokes

The native runtime ships env gated smokes against real Google Cloud Storage Delta tables. They assume Application Default Credentials are already available and the configured tables match a narrow fixture contract (partition column, expected snapshot versions, and so on). See [`crates/native-query-runtime`](crates/native-query-runtime/) for the full set. The most common starting points:

```bash
AXON_GCS_TEST_TABLE_URI=gs://your-bucket/your-table \
  cargo test -p native-query-runtime --locked \
  bootstrap_table_supports_env_gated_gcs_smoke -- --exact --nocapture

AXON_GCS_TEST_TABLE_URI=gs://your-bucket/your-table \
  cargo test -p native-query-runtime --locked \
  execute_query_supports_env_gated_gcs_smoke -- --exact --nocapture
```

CI runs the same commands behind `google-github-actions/auth` when `AXON_GCP_CREDENTIALS_JSON` is configured. Negative smokes (forbidden, not found, stale history, missing object) and the partitioned pruning and snapshot version smokes use additional `AXON_GCS_TEST_*` variables documented alongside their tests.

## Repository layout

* [`crates/`](crates/). Rust workspace packages. See [Repo tour](#repo-tour).
* [`tests/conformance/`](tests/conformance/). Native SQL corpora that double as the oracle for the browser planner and executor.
* [`tests/perf/`](tests/perf/). Performance budgets, the `browser-engine-worker.wasm` size gate, and benchmark scaffolding.
* [`tests/security/`](tests/security/). Security reporting guidance, browser dependency and bundle guardrails.
* [`docs/`](docs/). Program, ADR, epic, plan, and release gate documentation.
* [`.github/workflows/ci.yml`](.github/workflows/ci.yml). CI configuration.
