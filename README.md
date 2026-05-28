# Axon

A query engine for Delta Lake that runs in the browser. When the browser can't handle a query, the same request falls back to a native DataFusion runtime.

> Axon is early. The browser path is narrow on purpose — the native side is what we test browser results against. Expect things to change.

## What is Axon?

Querying a Delta lake usually means running something. A warehouse, a query service, a serverless worker — whatever it is, you pay for it while it's up and you wait on it when it isn't. The data is sitting in open formats on object storage, but you can't just go read it.

Axon lets the browser read it. For queries the browser can handle, it pulls Parquet byte ranges directly from object storage over signed URLs and runs the query in the tab. No query service sits in the middle of the scan.

For queries the browser can't handle, the same request runs on a native DataFusion runtime instead. That side is a Rust crate, so you run it wherever you already have compute. Laptop, container, VM, whatever.

What that buys you:

- Lower cost. Browser queries are about as cheap as the byte-range reads they make. No always-on compute.
- Portable. Native side is a crate, not a vendor-locked service. Browser side is signed URLs and a frontend.
- Lakehouse queries without managed query compute. For supported reads, once the data is exposed through a browser-safe descriptor or public/brokered object access, Axon can query Delta object storage directly in the tab.

## How it fits together

```text
            Caller (browser)
            QueryRequest → axon_table
                    │
                    ▼
            query-router: browser?
                    │
        yes  ───────┴───────  no / fallback
         │                         │
         ▼                         ▼
   Browser runtime          Native runtime
   (WASM)                   (DataFusion + delta-rs)
   HTTP range reads         Full SQL
   Parquet planning         Correctness oracle
   Narrow executor
         │                         │
         └────────────┬────────────┘
                      ▼
              delta-control-plane
              (snapshot resolution,
               table policy)
```

Three rules keep the two tiers honest:

1. **One contract.** Both runtimes use the same request and response types from `query-contract`, including the reasons for any fallback.
2. **Native is the oracle.** Every SQL case the browser claims to support has a matching native test. If they disagree, the browser stops and the native runtime takes the query.
3. **The browser tells you when it can't help.** Unsupported aggregates, unknown partition types, multi-partition execution, missing footer stats, identity drift — all come back as a fallback reason instead of a guess.

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

- [Browser lakehouse engine strategy](docs/program/browser-lakehouse-engine-strategy.md)
- [Browser Delta compatibility matrix](docs/program/browser-delta-compatibility-matrix.md)
- [Browser embedding and deployment](docs/program/browser-embedding-deployment.md)

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
- A browser runtime that bootstraps a snapshot, plans a candidate file set, prunes partitions and integer footer stats, and runs a known SQL subset (filter, project, group, common aggregates, aligned `ORDER BY` / `LIMIT`).
- Browser-side DataFusion through `wasm-datafusion-session` for the sandbox UI runtime.
- An OPFS persistent extent cache in `wasm-http-object-store`, bounded per object identity. If persistence fails, it's a cache miss, not an error.
- A TypeScript SDK with a manifest-based bundle selector. The default bundle is single-threaded; SIMD and threaded tiers exist but aren't assumed.
- A query router that returns a fallback reason instead of guessing.
- CI gates for `wasm32` build, host tests, WASM smokes, a real `browser-engine-worker.wasm` size budget, and dependency guardrails that keep cloud SDKs out of browser bundles.

## What's not here yet

- A `services/query-api` HTTP service. Signed URL issuance, proxy-mode requests, audit logging, request correlation, and CORS validation are all external for now.
- Session-level persistent caches in `wasm-query-session`. The lower-level OPFS adapter exists; the session itself is still in-memory only.
- A shipped worker artifact with broad browser DataFusion. DataFusion is reachable through the sandbox UI session, but the default worker reports `browser_datafusion = false` on purpose.

The full launch checklist lives in [`docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md`](docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md). External blockers are in [`docs/release-gates/browser-wasm-delta-gcs-external-blockers.md`](docs/release-gates/browser-wasm-delta-gcs-external-blockers.md).

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
