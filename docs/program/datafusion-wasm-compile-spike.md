# DataFusion WASM Compile Spike

- Status: Complete
- Date: 2026-05-03
- Scope: isolated DataFusion browser compile and in-memory Arrow query proof
- Related:
  - [Browser DataFusion Size Audit](./browser-datafusion-size-audit.md)
  - [Browser Lakehouse Engine Strategy](./browser-lakehouse-engine-strategy.md)

## Summary

`crates/wasm-datafusion-poc` is the seed for Axon's browser DataFusion engine. It proves that the
DataFusion runtime can compile and execute in `wasm32-unknown-unknown`; the next product step is to
promote this shape into `wasm-datafusion-engine`.

The spike proves:

- DataFusion 52.4.0 compiles for `wasm32-unknown-unknown` in this workspace.
- A tiny in-memory Arrow `RecordBatch` with `id: Int32`, `value: Int32`, and
  `category: Utf8` can be registered as a DataFusion `MemTable` named `t`.
- SQL can run against that batch on host and wasm.
- Query results can be returned as Arrow IPC stream bytes.
- A wasm-bindgen export can run the synthetic query and return Arrow IPC bytes to JavaScript.

Delta files, Delta snapshot reconstruction, browser range reads, and DataFusion table-provider
registration are intentionally out of scope for this compile spike. They are the next integration
work, not a reason to keep DataFusion out of the product engine.

The 30-day browser DataFusion gate result is recorded canonically in the browser DataFusion size
audit: continue toward a DataFusion physical execution engine for browser Delta/Parquet queries.
This spike remains compile and runtime evidence for that decision.

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

The current shipped worker/runtime still does not depend on this POC by default. That is the current
implementation state, not the destination architecture. The browser engine target is a promoted
DataFusion engine crate with custom Axon Delta/Parquet table access.

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

This is large enough to require explicit browser budgets, but it is not disqualifying for the target
DataFusion browser engine. The measured transfer and startup profile says optimize, split, cache,
and benchmark rather than replacing DataFusion physical execution with a custom runtime.

## Blockers And Risks

- No DataFusion compile blocker was observed for the current pinned dependency set.
- The wasm debug test artifact is large enough to require substantial temporary disk space during
  `wasm-bindgen-test-runner`; the first smoke attempt failed while the local data volume had only a
  few hundred MB free. Clearing generated Cargo build artifacts resolved the environment issue.
- Bundle size is a product budget for the DataFusion engine, not a reason by itself to avoid
  DataFusion execution.
- This spike only proves in-memory `RecordBatch` execution; it does not prove DataFusion over
  browser HTTP range reads, Parquet scans, Delta active-file descriptors, or query budget handling.

## Next Integration Step

Promote the POC toward `wasm-datafusion-engine`. The next step is a custom
`AxonDeltaTableProvider` that registers Delta snapshot descriptors with DataFusion and a custom
`AxonParquetScanExec` that streams browser-read Parquet `RecordBatch` values through DataFusion
physical execution. The old custom runtime remains useful as fallback and correctness scaffold during
the migration, but it is not the destination query engine.
