# Axon

Axon is an embeddable lakehouse query engine and workbench runtime for inspecting and querying open lakehouse data. It owns the browser/native execution path for Delta and Parquet reads: provider contracts, descriptor validation, byte-range I/O, scan planning, pruning, caching, Arrow IPC, runtime budgets, metrics, and deterministic fallback to a native DataFusion runtime when the browser cannot make a safe correctness claim.

> Axon is early. Browser support is intentionally bounded, and the native side is what we test browser results against. Expect things to change.

## What is Axon?

Querying lakehouse data usually means running something. A warehouse, a query service, a serverless worker — whatever it is, you pay for it while it's up and you wait on it when it isn't. The data is sitting in open formats on local disk or object storage, but direct inspection is still awkward.

Axon gives that data a workbench. For queries the browser can handle, it pulls Parquet byte ranges from browser-safe local files, public objects, signed URLs, proxy URLs, or brokered object routes and runs the query in the tab. No query service sits in the middle of supported browser scans.

For queries the browser can't handle, the same request runs on a native DataFusion runtime instead. That side is a Rust crate, so you run it wherever you already have compute. Laptop, container, VM, whatever.

Axon is the engine layer, not the business analytics platform around it. Host products provide identity, tenancy, policy, catalog governance, billing, audit, workflow, dashboards, agents, and rollout. Axon integrates with those hosts through browser-safe session, descriptor, read-plan, and execution contracts while keeping secrets and product authority outside the engine.

What that buys you:

- Lower cost. Browser queries are about as cheap as the byte-range reads they make. No always-on compute.
- Portable. Native side is a crate, not a vendor-locked service. Browser side is signed URLs and a frontend.
- Lakehouse queries without managed query compute. For supported reads, once the data is exposed through a browser-safe descriptor or provider-backed access plan, Axon can query Delta and Parquet data directly in the tab.

## How it fits together

```text
            Workbench / SDK
                  │
                  ▼
        Lakehouse provider model
   LocalDelta | ObjectStorage | UnityCatalog | DeltaSharing
                  │
                  ▼
           query-router: browser?
                  │
      yes ────────┴──────── no / fallback
       │                         │
       ▼                         ▼
 Browser runtime           Native runtime
 DataFusion WASM           DataFusion + delta-rs
 Browser-safe reads        Correctness oracle
       │                         │
       └────────────┬────────────┘
                    ▼
       Result, metrics, fallback contract
```

Three rules keep the providers and execution targets honest:

1. **Providers are not execution engines.** `LocalDelta`, `ObjectStorage`, `UnityCatalog`, and `DeltaSharing` describe how Axon opens lakehouse data; execution targets decide where a query runs.
2. **One contract.** Browser and native runtimes use the same request and response types from `query-contract`, including the reasons for fallback.
3. **Native is the oracle.** Every SQL case the browser claims to support has a matching native test. If they disagree, the browser stops and the native runtime takes the query.
4. **The browser tells you when it can't help.** Unsupported aggregates, unknown partition types, multi-partition execution, missing footer stats, identity drift — all come back as a fallback reason instead of a guess.

Control-plane integrations can provide governed descriptors, approved read plans, release evidence, or deployment policy. They do not become lakehouse providers unless they expose a provider contract that Axon can use independently of that integration policy.

## Get Started

You'll need Rust (workspace MSRV is pinned in `rust-toolchain.toml`) and the `wasm32-unknown-unknown` target if you want to build the browser crates.

```bash
git clone <repo>
cd axon

# Build the workspace
cargo check --workspace

# Run the host-side tests
cargo test -p query-contract
cargo test -p native-query-runtime
cargo test -p wasm-query-runtime

# Confirm browser crates still build for wasm
cargo check \
  -p wasm-query-runtime -p wasm-http-object-store \
  -p wasm-parquet-engine -p wasm-delta-snapshot \
  -p browser-sdk -p browser-engine-worker \
  --target wasm32-unknown-unknown
```

If you want the architecture context first:

- [Axon workbench architecture](docs/program/axon-workbench-architecture.md)
- [Provider model](docs/program/provider-model.md)
- [Browser lakehouse engine strategy](docs/program/browser-lakehouse-engine-strategy.md)
- [Browser Delta compatibility matrix](docs/program/browser-delta-compatibility-matrix.md)

## Repo tour

```text
# Shared contract
query-contract              Request/response types, capability flags, fallback reasons
query-router                Decides browser vs. native

# Native tier (correctness oracle)
native-query-runtime        DataFusion + delta-rs reference runtime
delta-runtime-support       Feature detection and Delta error mapping

# Browser tier (wasm32-unknown-unknown)
wasm-http-object-store      HTTP + local byte-range reads, OPFS extent cache, redacted errors
wasm-parquet-engine         Browser-side Parquet planning, async footer + scan
wasm-delta-snapshot         Browser-safe Delta snapshot reconstruction (log replay + checkpoints)
wasm-query-runtime          Constrained browser runtime: bootstrap, plan, prune, execute
wasm-datafusion-session     DataFusion-backed browser session (production UI path)
wasm-query-session          Legacy in-memory session shell (isolated for removal)
browser-sdk                 Embedding surface: worker envelopes, Arrow IPC, fallback propagation
browser-engine-worker       Linked worker artifact for size, cold start, footprint gates
apps/axon-web               Production browser app: SQL editor, catalog connect, DataFusion

# Control plane
delta-control-plane         Snapshot resolution and table policy

# Scaffolds (not yet wired up)
udf-abi, udf-host-wasi      Placeholders for hosted UDF execution
```

## What works today

- Native SQL over Delta tables, with snapshot pinning, partition pruning, and execution-derived metrics.
- Delta snapshot reconstruction is already repo-owned in `crates/wasm-delta-snapshot`.
- A browser runtime that bootstraps a snapshot, plans a candidate file set, prunes partitions and integer footer stats, and runs a known SQL subset (filter, project, group, common aggregates, aligned `ORDER BY` / `LIMIT`).
- Browser-side DataFusion through `wasm-datafusion-session` for the sandbox UI runtime and the `axon-web-wasm` browser DataFusion app worker.
- Standard Parquet datasets through browser-safe file descriptors, using the same Parquet range-read and DataFusion query path without requiring Delta log metadata.
- An OPFS persistent extent cache in `wasm-http-object-store`, bounded per object identity. If persistence fails, it's a cache miss, not an error.
- A TypeScript SDK with a manifest-based bundle selector. The default bundle is single-threaded; SIMD and threaded tiers exist but aren't assumed.
- A query router that returns a fallback reason instead of guessing.
- CI gates for `wasm32` build, host tests, WASM smokes, a real `browser-engine-worker.wasm` size budget, and dependency guardrails that keep cloud SDKs out of browser bundles.

## What's not here yet

- A `services/query-api` HTTP service. Signed URL issuance, proxy-mode request issuance, audit logging, request correlation, and CORS/origin validation are all external for now.
- Session-level persistent caches in `wasm-query-session`. The lower-level OPFS adapter exists; OPFS / IndexedDB session-level persistence is deferred, and the session itself is still in-memory only.
- A standalone npm package asset pipeline that copies `axon-web-worker.js` and `axon_web_wasm_bg.wasm` artifacts into `dist/worker/`; app deployments package those assets today. The legacy `browser-engine-worker` artifact remains compatibility evidence, not the default app worker.

The full launch checklist lives in [`docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md`](docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md). External blockers are in [`docs/release-gates/browser-wasm-delta-gcs-external-blockers.md`](docs/release-gates/browser-wasm-delta-gcs-external-blockers.md).

## Integrations And Governed Deployments

Axon integrations should use generic boundaries such as `HostProductIntegration`, `ControlPlaneIntegration`, `ApprovedReadPlanSource`, `GovernedDescriptorSource`, and `SessionAccessSource`. These boundaries let external products approve reads, provide descriptors, attach rollout policy, connect session-aware access, or package deployments without changing Axon's provider taxonomy.

Consumer-specific governed deployment strategies, operational maturity contracts, external proof handoffs, and compatibility examples live under [`docs/integrations/`](docs/integrations/). Release-gate artifacts for those integrations remain under [`docs/release-gates/`](docs/release-gates/).

## Development

### WASM smoke tests

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

More detail in [`tests/perf/README.md`](tests/perf/README.md) and [`tests/security/README.md`](tests/security/README.md).

### GCS smoke tests (optional)

The native runtime has env-gated smokes that run against real GCS Delta tables. They assume Application Default Credentials are already set up and that the table matches a narrow fixture contract (partition column, expected snapshot versions, and so on). See [`crates/native-query-runtime`](crates/native-query-runtime/) for the full list. The two common starting points:

```bash
AXON_GCS_TEST_TABLE_URI=gs://your-bucket/your-table \
  cargo test -p native-query-runtime --locked \
  bootstrap_table_supports_env_gated_gcs_smoke -- --exact --nocapture

AXON_GCS_TEST_TABLE_URI=gs://your-bucket/your-table \
  cargo test -p native-query-runtime --locked \
  execute_query_supports_env_gated_gcs_smoke -- --exact --nocapture
```

CI runs the same commands behind `google-github-actions/auth` when `AXON_GCP_CREDENTIALS_JSON` is set. The negative smokes (forbidden, not found, stale history, missing object) and the partitioned and snapshot-version smokes use additional `AXON_GCS_TEST_*` variables documented alongside their tests.

## Going deeper

- [Axon workbench architecture](docs/program/axon-workbench-architecture.md)
- [Provider model](docs/program/provider-model.md)
- [Browser lakehouse engine strategy](docs/program/browser-lakehouse-engine-strategy.md)
- [Browser Delta compatibility matrix](docs/program/browser-delta-compatibility-matrix.md)
- [Browser embedding and deployment](docs/program/browser-embedding-deployment.md)
- [Release handoff](docs/program/browser-lakehouse-release-handoff.md) and [integration runbook](docs/program/browser-release-integration-runbook.md)
- [Browser dependency review](docs/program/browser-dependency-compatibility-review-checklist.md)
- [Observability contract](docs/program/browser-observability-contract.md)
- ADRs and epic notes: [`docs/adr/`](docs/adr/), [`docs/epics/`](docs/epics/)

## Repository layout

- [`crates/`](crates/) — Rust workspace packages.
- [`apps/axon-web/`](apps/axon-web/) — production browser runtime app.
- [`tests/conformance/`](tests/conformance/) — native SQL corpora that double as the browser oracle.
- [`tests/perf/`](tests/perf/) — performance budgets and the `.wasm` size gate.
- [`tests/security/`](tests/security/) — security guidance and dependency guardrails.
- [`docs/`](docs/) — program, ADR, epic, plan, and release-gate documentation.
- [`.github/workflows/ci.yml`](.github/workflows/ci.yml) — CI config.

## Contributing

- [Security policy](SECURITY.md)

## License

Apache License 2.0 — see [LICENSE](LICENSE).
