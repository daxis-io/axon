# `object_store` Browser Architecture

## Decision

`object_store` keeps `HttpConnector` and `HttpService` as its transport seam. The project formalizes
the existing reqwest-on-WASM Fetch adapter as the built-in browser transport and removes native-only
dependencies and runtime assumptions from its active browser path.

`http-base` becomes host-neutral. The new `web` feature names the JavaScript host and supplies Fetch
bridging, timers, local task scheduling, and browser entropy only where a call site needs entropy.

Primary source:

- [`object_store` manifest at the research revision](https://github.com/apache/arrow-rs-object-store/blob/84d24eb8efcec9448566de09e94d2d4b74b21ebe/Cargo.toml)
- [Existing WASM reqwest adapter](https://github.com/apache/arrow-rs-object-store/blob/84d24eb8efcec9448566de09e94d2d4b74b21ebe/src/client/http/connection.rs)
- [Retry backoff implementation](https://github.com/apache/arrow-rs-object-store/blob/84d24eb8efcec9448566de09e94d2d4b74b21ebe/src/client/backoff.rs)
- [Multipart implementation](https://github.com/apache/arrow-rs-object-store/blob/84d24eb8efcec9448566de09e94d2d4b74b21ebe/src/multipart.rs)

## Existing Fetch Path

The pinned source implements `HttpService` for `reqwest::Client` on
`all(feature = "reqwest", target_arch = "wasm32", target_os = "unknown")`. It runs reqwest's
non-`Send` Fetch future with `wasm_bindgen_futures::spawn_local` and bridges response parts and body
chunks through channels.

The work should support and test that path. A second public Fetch transport would duplicate the
connector seam and split behavior.

## Feature Profiles

| Feature                | Required contract                                                                                                                             |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `http-base`            | Request construction, response handling, object semantics, and retry policy with no JS, OS entropy, filesystem, or Tokio runtime requirement. |
| `reqwest`              | Built-in reqwest connector. On WASM, reqwest supplies Fetch.                                                                                  |
| `web`                  | Explicit JS host: wasm-bindgen bridge, browser timer, local task scheduler, and browser entropy if required.                                  |
| `http`                 | Batteries-included HTTP. On `WASM_UNKNOWN`, it may imply `web`; native composition remains unchanged.                                         |
| `aws-lc-rs` and `ring` | Explicit crypto-provider choices; inactive on unsupported targets unless a dedicated provider contract says otherwise.                        |
| `fs`                   | Native default filesystem support; WASM operations return `NotSupported` and do not activate native implementation dependencies.              |

The initial logical composition may remain:

```toml
[features]
http = [
    "http-base",
    "reqwest",
    "reqwest/rustls",
    "aws-lc-rs",
    "web",
]
```

The `aws-lc-rs` dependency itself moves under the inverse `WASM_UNKNOWN` target table. Matching source
references receive the same `cfg`. Adding `web` has no native implementation effect when its
dependencies live in the exact WASM target table.

This preserves the native public feature contract. Removing `aws-lc-rs` from `http` can be reviewed
as a separate cleanup after graph evidence proves that edge redundant.

## Host-Neutral Retry

The pinned graph has:

```text
http-base -> cloud-base -> rand -> getrandom
```

The backoff code already exposes an internal `Backoff::new_with_rng` seam. The base path should use
one of these policies:

- an injected internal jitter source;
- deterministic or zero jitter when no host source exists;
- a small deterministic PRNG initialized from an explicit seed.

Native and `web` profiles may inject randomized jitter. `http-base` cannot obtain an OS-seeded thread
RNG by default.

The `getrandom` project does not infer JavaScript from `wasm32-unknown-unknown` and advises libraries
against enabling `wasm_js` unless they already commit to wasm-bindgen or JavaScript on that target.
That matches the `web` feature and rules out activation from `http-base`. See
[`getrandom` WebAssembly support](https://docs.rs/getrandom/latest/getrandom/#webassembly-support).

## Clock And Sleep

The pinned crate already uses a WASM-compatible clock dependency. Retry delay still depends on Tokio
sleep behavior.

Complete one internal runtime service:

```rust
trait RetryRuntime {
    fn now(&self) -> Instant;
    async fn sleep(&self, duration: Duration) -> Result<()>;
}
```

Implementations:

| Host          | Behavior                                                                                                        |
| ------------- | --------------------------------------------------------------------------------------------------------------- |
| Native        | Tokio clock and sleep.                                                                                          |
| Browser `web` | `web-time` clock and a JS timer future.                                                                         |
| Host-neutral  | Clock for accounting; return `NotSupported` only when a delayed retry is requested and no sleep service exists. |

The public builder can keep its present shape if the connector or client options install the runtime
service inside the crate.

## Multipart Task Scheduling

Multipart work uses Tokio task assumptions such as `JoinSet`. Browser-safe scheduling is a separate
review unit from retry timing.

The browser implementation can spawn local futures and collect their results through a futures
collection. Native code retains Tokio scheduling. The change must preserve cancellation, error
ordering, concurrency limits, and the public error type.

## Filesystem And Crypto

Native defaults stay intact:

- `fs` remains the default feature;
- native builds keep `walkdir`, Tokio filesystem support, and platform dependencies;
- `http` keeps its native TLS provider composition.

For `WASM_UNKNOWN`:

- filesystem implementation dependencies are inactive;
- filesystem construction or first use returns `Error::NotSupported`;
- `aws-lc-rs` and `aws-lc-sys` are absent from the active graph;
- generic browser HTTP does not select `ring`;
- cloud-provider browser builders remain outside the first support claim.

## Browser HTTP Contract

### Request forms

The Fetch standard makes a single `Range` request CORS-safelisted only when the parsed start is
present. The header value must also stay within the 128-byte per-header safelist limit, and the
combined safelisted request-header values must stay within Fetch's 1024-byte limit. The browser
adapter should prefer start-based ranges.

| Header                   | CORS behavior                                                                |
| ------------------------ | ---------------------------------------------------------------------------- |
| `Range: bytes=N-M`       | Safelisted single range.                                                     |
| `Range: bytes=N-`        | Safelisted single range.                                                     |
| `Range: bytes=-N`        | Valid HTTP range, but requires preflight because the parsed start is absent. |
| Multiple ranges          | Requires preflight.                                                          |
| `If-Range` or `If-Match` | Requires preflight and explicit allow-header policy.                         |

Source: [Fetch CORS-safelisted request-header algorithm](https://fetch.spec.whatwg.org/#cors-safelisted-request-header).

### Successful partial response

A successful non-full range requires:

- status `206`;
- a valid `Content-Range`;
- a body length consistent with the requested range;
- an identity content representation for the ranged bytes.

Fetch appends `Accept-Encoding: identity` to requests containing `Range`. If a server, CDN, or proxy
returns a non-identity encoded range response, the adapter rejects it as an incompatible ranged
object response. Source: [Fetch HTTP-network-or-cache algorithm](https://fetch.spec.whatwg.org/#http-network-or-cache-fetch).

### Response metadata

| Class                                            | Header or method                                                                                              |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------- |
| Required when validation uses it                 | `ETag` exposed through CORS.                                                                                  |
| Required when encoding validation uses it        | `Content-Encoding` exposed through CORS.                                                                      |
| Required for partial-response validation         | `Content-Range` exposed through CORS.                                                                         |
| Required when the selected metadata path uses it | `HEAD` allowed and its needed headers exposed.                                                                |
| Required when a conditional request uses it      | `If-Range` or `If-Match` allowed by preflight.                                                                |
| Advisory                                         | `Accept-Ranges`; absence does not prove range failure, and presence does not prove the next request succeeds. |

`Content-Length` is a CORS-safelisted response header. `ETag`, `Content-Range`,
`Content-Encoding`, and `Accept-Ranges` need `Access-Control-Expose-Headers` when script reads them.
See [Fetch CORS response-header rules](https://fetch.spec.whatwg.org/#http-new-header-syntax).

### Validator behavior

`If-Range` with a matching strong validator permits `206`. A mismatch instructs the server to ignore
`Range` and return the selected full representation as `200`. `If-Match` failure returns `412`.
Source: [RFC 9110 If-Range](https://httpwg.org/specs/rfc9110.html#field.if-range).

The adapter treats responses as follows:

| Response                                           | Policy                                                                                   |
| -------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `206` with valid range metadata                    | Accept.                                                                                  |
| `200` for an ordinary non-full range               | Reject by default to avoid buffering the whole object.                                   |
| `200` after an `If-Range` validator mismatch       | Invalidate the object identity and replan; do not treat the body as the requested range. |
| `200` under an explicit bounded full-body fallback | Accept only when the configured maximum and declared object size permit it.              |
| `412` after `If-Match`                             | Report object-changed and refresh metadata.                                              |
| `416`                                              | Report unsatisfied range with parsed object length when available.                       |

### Diagnostics

Fetch maps many CORS failures to a network error. The adapter can say:

```text
request failed in the browser; possible CORS, network, redirect, or browser policy failure
```

It cannot claim that a particular response header was missing unless script received enough response
metadata to prove that fact.

## Runtime Test Server

The browser protocol suite needs two origins:

- an application origin that runs the WASM client;
- an object origin that serves controlled responses.

The object origin must offer cases for valid `206`, ignored Range as `200`, validator changes,
`412`, `416`, missing exposed headers, non-identity encoding, preflight denial, redirects, delayed
retry, and bounded uploads.
