# ADR-0003: Browser Services Are Explicit Host Capabilities

- Status: Proposed
- Date: 2026-07-23
- Scope: `object_store` HTTP, retry, entropy, and task scheduling

## Decision

`object_store/http-base` remains host-neutral. An explicit `web` feature supplies JavaScript host
services for Fetch bridging, timers, local task scheduling, and browser entropy where needed.

Target detection determines which code can compile. Feature selection determines which host the
application provides.

## Context

`wasm32-unknown-unknown` does not guarantee JavaScript. The Rust target has no filesystem or native
threads, and `getrandom` cannot select a JavaScript API from the target triple.

The pinned `object_store` source already has a reqwest-on-WASM adapter that uses
`wasm_bindgen_futures::spawn_local`. The missing work lies in feature ownership and runtime services,
not a second transport API.

## Consequences

- Custom hosts can use `http-base` without browser bindings.
- Browser applications opt into a documented Fetch runtime.
- Retry jitter no longer requires OS entropy in base logic.
- Native HTTP and filesystem defaults remain intact.
- Browser tests cover timers and task scheduling instead of relying on compile checks.

## Rejected Options

### Infer JavaScript from the target triple

The target also serves non-web hosts, and Rust documents no `cfg` that proves a browser.

### Enable `getrandom/wasm_js` from `http-base`

That makes a host-neutral feature depend on wasm-bindgen and hides the host decision.

### Create a parallel Fetch public API

The existing `HttpConnector` and `HttpService` seams already carry the required request and response
abstractions.

## References

- [`wasm32-unknown-unknown` target guide](https://doc.rust-lang.org/rustc/platform-support/wasm32-unknown-unknown.html)
- [`getrandom` WebAssembly support](https://docs.rs/getrandom/latest/getrandom/#webassembly-support)
- [`object_store` browser architecture](../docs/04-object-store-browser-architecture.md)
