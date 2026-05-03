# DataFusion WASM Compile Spike

- Status: Complete
- Date: 2026-05-03
- Scope: isolated DataFusion browser compile and in-memory Arrow query proof

## Summary

`crates/wasm-datafusion-poc` is the isolated Axon browser DataFusion spike. It is not a
dependency of the shipped custom browser query runtime, worker, session shell, or browser SDK.

The spike proves:

- DataFusion 52.4.0 compiles for `wasm32-unknown-unknown` in this workspace.
- A tiny in-memory Arrow `RecordBatch` with `id: Int32`, `value: Int32`, and
  `category: Utf8` can be registered as a DataFusion `MemTable` named `t`.
- SQL can run against that batch on host and wasm.
- Query results can be returned as Arrow IPC stream bytes.
- A wasm-bindgen export can run the synthetic query and return Arrow IPC bytes to JavaScript.

Delta files, Delta snapshot reconstruction, browser range reads, and the shipped custom query
runtime are intentionally out of scope for this phase.

## Crate And API Surface

- Crate: `crates/wasm-datafusion-poc`
- Exported wasm API: `run_datafusion_smoke_query() -> Vec<u8>`
- Core Rust APIs:
  - `query_record_batches(sql, batch) -> Vec<RecordBatch>`
  - `query_record_batch_to_arrow_ipc(sql, batch) -> Vec<u8>`
  - `synthetic_record_batch() -> RecordBatch`
- Smoke query:

```sql
SELECT id, value
FROM t
WHERE category = 'B' AND value > 10
ORDER BY id
```

The expected result rows are `(2, 12)` and `(3, 25)`.

## Feature Flags And Target Dependencies

- `datafusion = "=52.4.0"` with `default-features = false` and `features = ["sql"]`
- `arrow-array`, `arrow-schema`, and `arrow-ipc` pinned to `=57.3.0`
- wasm target support:
  - `chrono` with `wasmbind`
  - `getrandom` with `wasm_js`
  - `wasm-bindgen = "=0.2.114"`
  - `wasm-bindgen-futures = "=0.4.64"`
- Crate type: `cdylib` and `rlib`

No DataFusion feature is enabled on `wasm-query-runtime`, `wasm-query-session`,
`browser-engine-worker`, or `browser-sdk`.

## Verification

Commands run successfully:

```bash
cargo test -p wasm-datafusion-poc --locked --test guardrails --test memtable_query --test arrow_ipc_export
cargo build -p wasm-datafusion-poc --target wasm32-unknown-unknown --locked
cargo test -p wasm-datafusion-poc --target wasm32-unknown-unknown --locked --test wasm_smoke
cargo build -p wasm-datafusion-poc --target wasm32-unknown-unknown --release --locked
wasm-bindgen target/wasm32-unknown-unknown/release/wasm_datafusion_poc.wasm --target web --out-dir target/wasm-datafusion-poc-bindgen --typescript
```

The wasm smoke executed three tests, including the wasm-bindgen export that decodes the returned
Arrow IPC stream.

## Bundle Size

Measured from the release build without wasm-opt, gzip, Brotli, or application bundler tree shaking:

- Raw release wasm: `79M` at `target/wasm32-unknown-unknown/release/wasm_datafusion_poc.wasm`
- wasm-bindgen web output: `73M` total at `target/wasm-datafusion-poc-bindgen`
- wasm-bindgen generated wasm: `73M`
- wasm-bindgen JS shim: `12K`

This is too large for Axon's default browser runtime SKU. Any DataFusion browser path should stay
behind an explicit experimental SKU or feature gate until size budgets and operator scope are
defined.

## Blockers And Risks

- No DataFusion compile blocker was observed for the current pinned dependency set.
- The wasm debug test artifact is large enough to require substantial temporary disk space during
  `wasm-bindgen-test-runner`; the first smoke attempt failed while the local data volume had only a
  few hundred MB free. Clearing generated Cargo build artifacts resolved the environment issue.
- Bundle size is the main product blocker for enabling this in the default browser path.
- This spike only proves in-memory `RecordBatch` execution; it does not prove DataFusion over
  browser HTTP range reads, Parquet scans, Delta active-file descriptors, or query budget handling.

## Next Integration Step

Keep the custom browser runtime unchanged. The next step is an experimental DataFusion table
registration path that takes resolved Delta descriptors, obtains streamed `RecordBatch` values from
`wasm-parquet-engine`, registers them with DataFusion, runs a narrow SQL profile, and returns Arrow
IPC through the same worker result boundary. Delta file integration should wait until that bridge has
size, memory, and fallback guardrails.
