# examples

The production web runtime lives at [`apps/axon-web/`](../apps/axon-web/) — that
is where the SQL editor, catalog connect workflow, and `@axon/web` package are
built and deployed. This `examples/` directory holds short snippets that
illustrate **how to embed the browser engine in a host application**; it does
not contain a runnable application by itself.

## Layout

- [`embed/`](embed/) — minimal HTML + JS sample showing the `createAxonBrowserClient()`
  embedding contract. Useful as a starting point for hosts that want to ship
  their own UI around the Axon worker / WASM.

If you are looking for the actual editor + connect-catalog experience, run:

```sh
cd apps/axon-web
npm install
npm run dev
```

For the embedding contract that the production app honours, see
[`docs/program/browser-embedding-deployment.md`](../docs/program/browser-embedding-deployment.md).
