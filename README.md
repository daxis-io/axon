# Axon

Axon is a Rust workspace for building a hybrid query platform with native and browser runtimes, shared query contracts, and supporting infrastructure for control-plane and UDF execution.

## Workspace

- `crates/query-contract` contains shared request and response types, capability flags, and fallback reasons.
- `crates/native-query-runtime` is the native execution runtime scaffold.
- `crates/wasm-query-runtime` is the browser-oriented runtime scaffold.
- `crates/delta-control-plane` is the control-plane scaffold for snapshot resolution and browser-safe access.
- `crates/query-router`, `crates/browser-sdk`, `crates/wasm-http-object-store`, `crates/udf-abi`, and `crates/udf-host-wasi` provide the supporting packages around routing, browser access, and hosted UDF execution.

## Getting Started

```bash
cargo check --workspace
cargo test -p query-contract
cargo test -p native-query-runtime
cargo check -p wasm-query-runtime -p wasm-http-object-store -p browser-sdk --target wasm32-unknown-unknown
```

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
Only paired snapshot-history env vars are hard-validated in CI; the single-variable negative fixture URIs remain independently optional and simply skip when unset.

## Trusted Control-Plane Slice

`crates/delta-control-plane` now contains the first in-repo EPIC-03 slice:

- `resolve_snapshot(request)` validates the table locator, resolves the latest or explicit historical Delta snapshot, and returns a metadata-only descriptor.
- `SnapshotResolutionRequest` carries `table_uri` plus an optional `snapshot_version`.
- `ResolvedSnapshotDescriptor` returns the normalized `table_uri`, the concrete resolved `snapshot_version`, and deterministically ordered active file metadata as `ResolvedFileDescriptor` entries.

The descriptor intentionally excludes signed URLs, proxy endpoints, tokens, and credentials. It is only the minimal handoff needed for the existing native oracle.

Local validation:

```bash
cargo test -p delta-control-plane --locked
```

Cross-crate handoff coverage in `crates/delta-control-plane/tests` checks the resolved `table_uri` / `snapshot_version` pair against `crates/native-query-runtime` and validates the descriptor's active-file metadata against the local fixture without changing `QueryRequest`.
Authenticated HTTP service work remains out of repo: there is still no `services/query-api` directory here.

## Repository Layout

- `crates/` contains the Rust workspace packages.
- `tests/conformance/` contains scaffold checks plus latest-snapshot and snapshot-version native SQL corpora with golden expectations.
- `tests/perf/` contains performance test scaffolding.
- `tests/security/` contains security test scaffolding.
- `.github/workflows/ci.yml` contains the CI configuration.
