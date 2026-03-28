# Axon

Axon is a Rust workspace for building a hybrid query platform with native and browser runtimes, shared query contracts, and supporting infrastructure for control-plane and UDF execution.

## Workspace

- `crates/query-contract` contains shared request and response types, capability flags, and fallback reasons.
- `crates/native-query-runtime` is the native execution reference runtime.
- `crates/wasm-query-runtime` is the browser-oriented runtime envelope for constrained object access and future browser SQL execution.
- `crates/delta-control-plane` contains the in-repo control-plane slice for snapshot resolution and table policy enforcement.
- `crates/wasm-http-object-store` contains the thin EPIC-04 HTTP byte-range slice for browser-safe object reads.
- `crates/query-router`, `crates/browser-sdk`, `crates/udf-abi`, and `crates/udf-host-wasi` remain scaffolds around routing, browser access, and hosted UDF execution.

## Getting Started

```bash
cargo check --workspace
cargo test -p query-contract
cargo test -p native-query-runtime
cargo test -p wasm-query-runtime
cargo test -p wasm-http-object-store
cargo check -p wasm-query-runtime -p wasm-http-object-store -p browser-sdk --target wasm32-unknown-unknown
```

## Browser Runtime Envelope

`crates/wasm-query-runtime` now contains the current in-repo EPIC-04 browser preflight slice:

- `BrowserRuntimeConfig` validates the constrained browser envelope before any object access is attempted, including a nonzero request timeout for runtime-owned readers.
- `BrowserRuntimeConfig` also carries a nonzero snapshot-preflight deadline plus bounded metadata-fetch concurrency so browser snapshot bootstrap is not forced into unbounded serial I/O.
- `BrowserRuntimeSession::new(config)` constructs a runtime handle with runtime-owned HTTP client timeout policy, while `BrowserRuntimeSession::with_reader(config, reader)` remains available for injected host-side readers and tests.
- `BrowserObjectSource::from_url(url)` is the typed browser object source boundary for URL-backed access and only accepts HTTPS object URLs in production browser mode, with loopback-only plain HTTP reserved for native host-side tests.
- `MaterializedBrowserFile::new(...)` and `MaterializedBrowserSnapshot::new(...)` provide runtime-owned constructors for already-validated object sources without broadening `query-contract`.
- `BrowserRuntimeSession::materialize_snapshot(&descriptor)` converts a shared HTTPS-only `BrowserHttpSnapshotDescriptor` into runtime-owned validated object sources while preserving file order and metadata without performing any network I/O.
- `BrowserRuntimeSession::probe(&source, range)` delegates exact range reads to `crates/wasm-http-object-store` without reimplementing HTTP logic.
- `BrowserRuntimeSession::read_parquet_footer_for_file(&file)` validates descriptor size against observed object metadata while bootstrapping raw footer bytes.
- `BrowserRuntimeSession::read_parquet_metadata_for_file(&file)` decodes strongly typed Parquet file metadata from those footer bytes, including per-file integer `BrowserParquetFieldStats` min/max/null-count summaries when footer statistics are present.
- `BootstrappedBrowserFile::new(...)` and `BootstrappedBrowserSnapshot::new(...)` now validate size/path invariants up front and expose bootstrapped state through read-only accessors instead of public mutable fields.
- `BrowserRuntimeSession::bootstrap_snapshot_metadata(&snapshot)` now buffers metadata fetches up to the configured concurrency limit and enforces a snapshot-level deadline, while `BootstrappedBrowserSnapshot::{validate_uniform_schema,summarize}` produces deterministic Parquet payload-field summaries plus sorted partition-column names and row/byte totals without attempting browser SQL execution.
- `BrowserRuntimeSession::analyze_query_shape(&self, sql)` validates the current read-only browser SQL envelope over `axon_table` and returns a deterministic `BrowserQueryShape` without attempting execution.
- `BrowserRuntimeSession::plan_query(&self, snapshot, request)` binds a supported `QueryRequest` to a bootstrapped snapshot and returns a `BrowserPlannedQuery` candidate-file set plus `BrowserPruningSummary`, including lossless partition pruning for `=`, `IN`, `IS NULL`, and `IS NOT NULL` filters and integer footer-stat pruning for `=`, `>`, `>=`, `<`, and `<=` predicates when complete file stats are available.
- `BrowserRuntimeSession::build_execution_plan(&self, snapshot, request)` now lowers the currently accepted browser SQL subset into a typed `BrowserExecutionPlan` over the already-planned candidate-file set, including passthrough output columns, grouped columns, and aliased aggregate measures for `AVG`, `ARRAY_AGG`, `BOOL_AND`, `BOOL_OR`, `COUNT`, `SUM`, `MIN`, and `MAX`, plus output-aligned `ORDER BY` / `LIMIT` metadata. `DISTINCT`, `HAVING`, wildcard projections, non-lossless projections, and non-output-aligned `ORDER BY` expressions remain rejected without attempting execution.
- The runtime rejects multi-partition execution as a structured native fallback, rejects unsupported object URL schemes during source construction, rejects cloud credentials as a security policy violation, and allows plain HTTP only for loopback host-side tests.

Local validation:

```bash
cargo install wasm-bindgen-cli --version 0.2.114 --locked
cargo test -p wasm-query-runtime --locked
cargo test -p wasm-query-runtime --target wasm32-unknown-unknown --locked --test wasm_smoke
```

This slice is intentionally small: it does not register tables with DataFusion, execute browser SQL, expose `browser-sdk`, orchestrate `query-router`, or implement any `services/query-api` behavior. The current browser output is a deterministic planning/pruning plus execution-plan-lowering layer over bootstrapped Parquet metadata, not an executing browser SQL engine.

## Native Runtime Slice

`crates/native-query-runtime` now contains the first callable EPIC-02 slice:

- `bootstrap_table(table_uri)` opens a Delta table and validates the Sprint 1 compatibility envelope.
- `execute_query(request)` registers the table as `axon_table`, executes read-only SQL, and returns Arrow batches, execution-derived scan metrics, wall-clock duration, and optional explain output.
- `QueryRequest.snapshot_version` optionally pins execution to a specific Delta snapshot version; omitting it keeps the current latest-snapshot behavior.

Local/offline validation:

```bash
cargo test -p native-query-runtime --locked
```

Sprint 2 tightens native metrics around the executed plan:

- `bytes_fetched` is sourced from scan-level `bytes_scanned` metrics when available.
- `files_touched` reports scanned files rather than total active snapshot files.
- `files_skipped` reports partition/file pruning outcomes when the scan path exposes them, and otherwise falls back to `active_files - files_touched`.

Offline native coverage now includes both the original unpartitioned SQL corpus and a partitioned latest-snapshot corpus that asserts pruning-visible metrics.
Sprint 4 expands that local oracle coverage to:

- a 12-case latest-snapshot unpartitioned SQL corpus,
- a 10-case latest-snapshot partitioned SQL corpus with explicit scan-metric assertion flags so pruning expectations are only enforced where they are stable,
- a 4-case snapshot-version SQL corpus over the local multi-version fixture.

The local `cargo test -p native-query-runtime --locked` suite also carries deterministic negative-path coverage for:

- invalid table locations,
- unavailable or negative snapshot versions,
- missing local data files,
- Unix permission-denied local data files.

These local failures are the baseline oracle checks; the GCS smokes below remain optional environment-backed coverage for cloud-specific paths.

Env-gated GCS smoke validation:

```bash
AXON_GCS_TEST_TABLE_URI=gs://your-bucket/your-table \
cargo test -p native-query-runtime --locked bootstrap_table_supports_env_gated_gcs_smoke -- --exact --nocapture
```

Env-gated GCS query execution smoke:

```bash
AXON_GCS_TEST_TABLE_URI=gs://your-bucket/your-table \
cargo test -p native-query-runtime --locked execute_query_supports_env_gated_gcs_smoke -- --exact --nocapture
```

The GCS smoke path assumes standard Google ADC is already available in the shell or runner environment, and the configured table should be non-empty so the query smoke can assert `LIMIT 1` execution.
GitHub Actions uses the same command behind an explicit `google-github-actions/auth` step and requires the `AXON_GCP_CREDENTIALS_JSON` secret when any env-gated GCS fixture is configured.

Env-gated partitioned GCS pruning smoke:

```bash
AXON_GCS_TEST_PARTITIONED_TABLE_URI=gs://your-bucket/your-partitioned-table \
cargo test -p native-query-runtime --locked execute_query_supports_env_gated_partitioned_gcs_pruning_smoke -- --exact --nocapture
```

Env-gated partitioned GCS snapshot-version smoke:

```bash
AXON_GCS_TEST_PARTITIONED_TABLE_URI=gs://your-bucket/your-partitioned-table \
AXON_GCS_TEST_PARTITIONED_TABLE_SNAPSHOT_VERSION=1 \
cargo test -p native-query-runtime --locked execute_query_supports_env_gated_partitioned_gcs_snapshot_version_smoke -- --exact --nocapture
```

The partitioned GCS fixture contract is intentionally narrow:

- the table must be partitioned by a `category` column,
- the latest snapshot must include at least one row in the `category = 'C'` partition,
- the pinned historical snapshot version must be readable and return a different `COUNT(*)` result than latest,
- the latest pruning query should visibly skip at least one file so the smoke can assert `files_skipped > 0`.

Env-gated negative GCS smokes:

```bash
AXON_GCS_TEST_FORBIDDEN_TABLE_URI=gs://your-bucket/forbidden-table \
cargo test -p native-query-runtime --locked bootstrap_table_rejects_env_gated_forbidden_gcs_smoke -- --exact --nocapture

AXON_GCS_TEST_NOT_FOUND_TABLE_URI=gs://your-bucket/missing-table \
cargo test -p native-query-runtime --locked bootstrap_table_rejects_env_gated_not_found_gcs_smoke -- --exact --nocapture

AXON_GCS_TEST_STALE_HISTORY_TABLE_URI=gs://your-bucket/history-trimmed-table \
AXON_GCS_TEST_STALE_HISTORY_SNAPSHOT_VERSION=1 \
cargo test -p native-query-runtime --locked execute_query_rejects_env_gated_stale_history_gcs_smoke -- --exact --nocapture

AXON_GCS_TEST_MISSING_OBJECT_TABLE_URI=gs://your-bucket/missing-object-table \
cargo test -p native-query-runtime --locked execute_query_rejects_env_gated_missing_object_gcs_smoke -- --exact --nocapture
```

Negative fixture contract:

- `AXON_GCS_TEST_FORBIDDEN_TABLE_URI` must point at a table path that exists but returns `403` or equivalent access denial for the runner identity.
- `AXON_GCS_TEST_NOT_FOUND_TABLE_URI` must point at a table path that returns `404` or equivalent not-found behavior during bootstrap.
- `AXON_GCS_TEST_STALE_HISTORY_TABLE_URI` and `AXON_GCS_TEST_STALE_HISTORY_SNAPSHOT_VERSION` must be configured together, and the table must have a readable latest snapshot whose configured historical version is no longer available.
- `AXON_GCS_TEST_MISSING_OBJECT_TABLE_URI` must point at a table whose log is readable but whose current snapshot references at least one missing data object; the smoke issues a full-table aggregate to force every current file to be opened.

Fixture provisioning, IAM policy, and CI variable population for these negative smokes remain external dependencies outside this repository.
Among the negative GCS fixtures, only the paired stale-history env vars are hard-validated in CI; the single-variable negative fixture URIs remain independently optional and simply skip when unset.

## Trusted Control-Plane Slice

`crates/delta-control-plane` now contains the first in-repo EPIC-03 slice:

- `resolve_snapshot(request)` validates the table locator, resolves the latest or explicit historical Delta snapshot, and returns a metadata-only descriptor.
- `resolve_snapshot_with_policy(request, policy)` applies exact-match per-table allow/deny rules after URI normalization and before snapshot I/O.
- `attach_browser_http_urls(resolved_snapshot, object_urls_by_path)` converts a resolved metadata-only snapshot into a browser HTTP descriptor once a trusted caller supplies exact per-file URLs.
- `SnapshotAccessPolicy` canonicalizes equivalent locators so raw local paths, `file://` URLs, remote bucket/root variants, redundant-slash variants, whitespace variants, and trailing-slash variants cannot bypass table policy.
- `SnapshotResolutionRequest` carries `table_uri` plus an optional `snapshot_version`.
- `ResolvedSnapshotDescriptor` returns the normalized `table_uri`, the concrete resolved `snapshot_version`, and deterministically ordered active file metadata as `ResolvedFileDescriptor` entries.
- `BrowserHttpSnapshotDescriptor` and `BrowserHttpFileDescriptor` provide the shared in-repo browser-facing contract for explicit per-file HTTPS access without changing the existing metadata-only snapshot path.

This slice still does not mint signed URLs or proxy endpoints. Instead, it now defines and validates the descriptor seam that future trusted service code will use after it generates exact per-file browser-safe URLs. Tokens, credentials, audit fields, TTL, request correlation, and CORS/origin behavior remain out of repo.

Local validation:

```bash
cargo test -p delta-runtime-support --locked
cargo test -p delta-control-plane --locked
```

Cross-crate handoff coverage in `crates/delta-control-plane/tests` checks the resolved `table_uri` / `snapshot_version` pair against `crates/native-query-runtime`, validates the descriptor's active-file metadata against the local fixture without changing `QueryRequest`, proves browser HTTP URL attachment preserves file order and metadata, proves invalid or duplicate browser URL inputs fail deterministically without leaking query strings, and confirms the resulting HTTPS descriptors materialize cleanly into `crates/wasm-query-runtime` runtime-owned object sources.
Additional cross-crate browser-preflight coverage now resolves real local Delta snapshots, serves their Parquet files over loopback HTTP in host-side tests, bootstraps runtime-owned Parquet metadata and snapshot summaries through `crates/wasm-query-runtime`, proves the resulting `file_count`, `snapshot_version`, `total_bytes`, `total_rows`, integer footer stats, and curated browser-planning candidate-file counts remain aligned with the resolved snapshot descriptor and the native `COUNT(*)` / `files_touched` / `files_skipped` oracle, and now also asserts typed browser execution-plan shape over the curated supported-browser SQL corpus while keeping explicit native-only divergence checks so browser-envelope drift is caught in CI.
Authenticated HTTP service work remains out of repo: there is still no `services/query-api` directory here, so signed URL issuance, proxy reads, audit logging, request correlation, and CORS/origin validation remain external blockers rather than shipped repository scope.

## HTTP Range-Read Slice

`crates/wasm-http-object-store` now contains the thin in-repo EPIC-04 opening slice:

- `HttpByteRange` models full, bounded, from-offset, and suffix reads without introducing signing or proxy assumptions.
- `HttpRangeReader::with_client(client)` allows callers to inject a preconfigured `reqwest::Client` for timeout or redirect policy control.
- `HttpRangeReader::read_range(url, range)` performs exact HTTP byte-range requests and returns `bytes::Bytes` plus `HttpObjectMetadata` without an extra payload copy.
- Returned metadata and error messages redact URL query strings and fragments so signed URL secrets do not leak past the transport boundary.
- Deterministic local HTTP tests cover footer-style reads plus `401`, `403`, `404`, `416`, and malformed partial-response handling.
- The crate maps transport failures to `ExecutionFailed`, auth failures to `AccessDenied`, and range/protocol failures to `ObjectStoreProtocol` using the existing shared query error taxonomy.

Local validation:

```bash
cargo test -p wasm-http-object-store --locked
cargo check -p wasm-query-runtime -p wasm-http-object-store -p browser-sdk --target wasm32-unknown-unknown --locked
```

This slice is intentionally small: it does not register tables with DataFusion, execute browser SQL, expose a browser SDK surface, or implement any `services/query-api` behavior. Signed URL issuance, read-proxy mode, audit logging, request correlation, and production-shape CORS/origin validation remain external blockers outside this repository.

## Repository Layout

- `crates/` contains the Rust workspace packages.
- `tests/conformance/` contains scaffold checks plus native SQL corpora whose partition-pruning expectations now serve as the local oracle for narrow browser-planning parity coverage.
- `tests/perf/` contains performance test scaffolding.
- `tests/security/` contains security notes and will grow into service-level secret/CORS coverage once `services/query-api` exists.
- `.github/workflows/ci.yml` contains the CI configuration.
