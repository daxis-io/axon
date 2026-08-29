# Axon Engine UAT

This suite is the operator-facing query UAT path for Axon's engine behavior.
It combines broad host-engine SQL coverage, actual `wasm32` execution coverage,
native-oracle checks, browser-parity checks, and performance probes.

Run the default deterministic suite from the repository root:

```bash
bash tests/uat/run_axon_engine_uat.sh
```

The runner writes a Markdown timing report to
`target/uat/axon-engine-query-uat/summary.md`.

The script defaults to lean cargo test builds with `CARGO_INCREMENTAL=0` and
`CARGO_PROFILE_TEST_DEBUG=0` to keep local UAT runs from filling the checkout's
`target/` directory. Override those environment variables before running the
script if you need full debug artifacts.

## Coverage

The default suite is offline and deterministic. It covers:

- `tests/conformance/axon-engine-query-uat-corpus.json` through
  `crates/wasm-datafusion-poc/tests/axon_engine_uat_corpus.rs`, executing every
  UAT query through both in-memory DataFusion tables and the descriptor-backed
  `AxonParquetScanExec` path in host tests for the WASM engine implementation.
- `crates/wasm-datafusion-poc/tests/axon_engine_wasm_uat_corpus.rs` through
  `wasm-bindgen-test`, executing a representative UAT query slice under the
  actual `wasm32-unknown-unknown` build and asserting the single-partition
  descriptor-backed `AxonParquetScanExec` physical plan.
- Native latest-snapshot, partitioned-pruning, and historical snapshot corpora.
- Browser/native planning and result parity checks on the partitioned fixture.
- The existing browser DataFusion performance smoke, including first-query,
  repeated-query, Parquet metadata, real Delta/Parquet, and scan-metrics probes.

The native runtime rows are correctness oracles and fallback-comparison checks.
They are not the proof that the WASM engine ran; the `actual wasm32 UAT query
slice` row is the deterministic compiled-WASM gate.

Live public-object-storage proof remains env-gated outside this default suite.
Use `npm run test:browser:public-gcs-live` from `apps/axon-web` only when
`AXON_LIVE_PUBLIC_GCS_TABLE_URI` is set and the browser test environment is
ready.
