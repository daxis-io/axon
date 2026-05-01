# Browser Delta Sandbox

This example proves that a real browser can load Axon's WASM Delta snapshot facade and resolve a Delta log over browser HTTP semantics.

The fixture is intentionally small:

- static Delta JSON log files under `public/fixtures/table/_delta_log/`
- a same-origin manifest at `public/fixtures/delta-log-manifest.json`
- no cloud credentials, signed URL service, or Parquet data-file scan

Run it locally:

```bash
rustup target add wasm32-unknown-unknown
cargo install wasm-bindgen-cli --version 0.2.114 --locked
npm install
npm run build
npm run test:e2e
```

The E2E test starts Vite over HTTPS, opens Chromium through Playwright, asks the WASM facade to resolve the manifest-backed Delta log, and asserts that snapshot version `1` returns `data/b.parquet` as the active file.
