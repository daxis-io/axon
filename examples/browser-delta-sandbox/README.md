# Browser Delta Sandbox

This example proves that a real browser can load Axon's WASM Delta snapshot facade and resolve Delta logs over browser HTTP semantics.

It has two fixture paths:

- a tiny checked-in JSON log smoke fixture under `public/fixtures/table/_delta_log/`
- a generated prod-like fixture under `public/fixtures/prod-like/`

The prod-like fixture is generated with delta-rs before dev/build/test. It creates real partitioned Parquet data files, multiple Delta commits, a Snappy-compressed checkpoint parquet at version `2`, `_last_checkpoint`, stats-bearing add actions, and an overwrite commit at version `3` that removes old files and adds the latest active files.

Run it locally:

```bash
rustup target add wasm32-unknown-unknown
cargo install wasm-bindgen-cli --version 0.2.114 --locked
npm install
npm run build:fixture
npm run build
npm run test:e2e
```

The E2E test starts Vite over HTTPS, opens Chromium through Playwright, asks the WASM facade to resolve both fixture manifests, and asserts that the prod-like fixture resolves snapshot version `3` from checkpoint version `2` plus one replay commit.
