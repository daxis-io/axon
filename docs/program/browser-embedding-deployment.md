# Browser Embedding Deployment Guide

- Date: 2026-05-15
- Scope: browser host deployment guidance for Axon's worker-first WASM embedding surface
- Related:
  - [Browser Lakehouse Engine Strategy](./browser-lakehouse-engine-strategy.md)
  - [Browser Lakehouse Release Handoff](./browser-lakehouse-release-handoff.md)
  - [Browser Observability Contract](./browser-observability-contract.md)
  - [ADR-0002: Browser Access Uses Signed HTTPS Or A Narrow Proxy, Never Cloud Secrets](../adr/ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md)
  - [Axon Web Runtime](../../apps/axon-web/README.md)

## Current Package State

Axon's browser deployment shape is worker-first:

- browser hosts create a module `Worker` from a `workerUrl`
- large query results cross the worker boundary as Arrow IPC bytes, not row JSON
- table data reaches the browser only through browser-safe snapshot descriptors and object URLs minted by a trusted control-plane seam
- server query execution remains an opt-in fallback module and correctness oracle, not something the default browser build invokes implicitly

The current repository does not yet contain a publishable npm package. The browser-facing TypeScript wrapper lives in the private app package at [`apps/axon-web/src/axon-browser-sdk.ts`](../../apps/axon-web/src/axon-browser-sdk.ts), and the only `package.json` in the main checkout is the private `@axon/web` runtime package. The Rust worker artifact is tracked as `browser_engine_worker.wasm`, but a production JavaScript worker bootstrap and npm export map are still implementation work.

That means deployments can use this guide in two ways:

- as the contract for an application-owned embedding while the SDK is example-owned
- as the export and asset layout target for the future publishable browser package

## Deployment Artifacts

| Artifact              | Current repo source                                                                                                                                                                                                                                              | Runtime URL                         | Required content type                                     | Cache policy                                         |
| --------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------- | --------------------------------------------------------- | ---------------------------------------------------- |
| TypeScript SDK module | `apps/axon-web/src/axon-browser-sdk.ts`                                                                                                                                                                                                                          | app bundle or future package import | `application/javascript; charset=utf-8` after bundling    | bundle with the app release                          |
| Worker module         | host-owned worker JS that speaks `BrowserWorkerCommand` / `BrowserWorkerResponseEnvelope`                                                                                                                                                                        | `workerUrl` manifest field          | `application/javascript; charset=utf-8`                   | immutable when content-hashed; otherwise short cache |
| Worker WASM           | measured raw artifact at `target/wasm32-unknown-unknown/release/browser_engine_worker.wasm` after `cargo build -p browser-engine-worker --target wasm32-unknown-unknown --release --locked`; packaged browser output is TBD until the JS worker bootstrap exists | `wasmUrl` manifest field            | `application/wasm`                                        | immutable when content-hashed                        |
| Bundle manifest       | `AXON_BROWSER_BUNDLE_MANIFEST` or host-supplied equivalent                                                                                                                                                                                                       | app bundle or JSON endpoint         | `application/json` for external manifests                 | short cache or revalidate                            |
| Snapshot descriptor   | browser-produced or explicit server snapshot resolver response                                                                                                                                                                                                   | app API response or browser runtime | `application/json`                                        | no-store or request-scoped                           |
| Data objects          | signed HTTPS object URLs or a narrow read proxy                                                                                                                                                                                                                  | descriptor `active_files[*].url`    | object content type is not semantically important to Axon | bounded by URL TTL and object identity               |

`wasmUrl` is deployment metadata in the current TypeScript wrapper. `createAxonBrowserClient()` constructs the module worker from `workerUrl`; the worker module is responsible for loading the matching WASM asset. The current `browser_engine_worker.wasm` file is a real size-gated Rust artifact, not a complete browser package by itself.

## Delta Location Opening Modes

`openDeltaLocation()` is the browser SDK convenience layer for source-based
Delta opens. Its default mode is browser-owned descriptor production:

- table-root/object-grant sources use the browser snapshot resolver to read
  `_delta_log` and produce `BrowserHttpSnapshotDescriptor`
- manifest, Delta Sharing URL-mode file actions, provider file lists, and
  trusted descriptors use the descriptor materializer directly
- every successful browser-owned path forwards the descriptor unchanged through
  `openDeltaTable(name, descriptor)`

Access-broker integration is explicit. An access broker may authenticate,
authorize, vend an opaque grant, return browser-safe manifest material, or expose
narrow list/head/range routes, but it does not reconstruct snapshots or execute
SQL by default.

Server snapshot resolution is also explicit. In that mode,
`openDeltaLocation()` sends a typed request to a trusted server snapshot resolver:

- `provider`: `s3`, `gcs`, or `azure_blob`
- `table_uri`: logical URI such as `s3://bucket/table`, `gs://bucket/table`,
  `az://account/container/table`, or
  `abfs://container@account.dfs.core.windows.net/table`
- `credential_profile`: opaque storage access profile, not a cloud credential
- `requested_access_mode`: server snapshot resolver-only `auto`, `signed_url`,
  or `proxy`
- `snapshot_version`: optional exact Delta snapshot version

The server snapshot resolver response is authoritative. It includes the returned
`BrowserHttpSnapshotDescriptor`, `resolved_snapshot_version`, resolver-only
`actual_access_mode`, `expires_at_epoch_ms`, optional `correlation_id`, and
optional warnings. The SDK validates that `resolved_snapshot_version` matches
`descriptor.snapshot_version`, rejects expired descriptors with
`descriptor_expired`, forwards the descriptor unchanged through the existing
`openDeltaTable()` worker command, and returns the non-descriptor metadata to the
caller for display and retry decisions. Reopening through the server snapshot
resolver is the M1 expiry behavior; any refresh endpoint must enforce that
refresh never advances the resolved snapshot version.

The server snapshot resolver owns cloud authentication, Delta log
listing/checkpoint selection, active-file materialization, signed URL or proxy
URL construction, CORS validation, policy, audit, and request correlation.
`auto` normally lets it choose signed URLs to reduce service data-plane
bandwidth; `proxy` is appropriate for centralized audit/control, private
networking, or CORS avoidance at higher service cost.

## Worker And WASM URLs

Use explicit URLs in the bundle manifest. The baseline manifest shape is:

```ts
import {
  AXON_BROWSER_BUNDLE_MANIFEST,
  createAxonBrowserClient,
  getPlatformFeatures,
  selectBundle,
} from "./axon-browser-sdk";

const features = getPlatformFeatures();
const selection = selectBundle(AXON_BROWSER_BUNDLE_MANIFEST, features);

const client = createAxonBrowserClient({
  workerUrl: selection.bundle.workerUrl,
  workerOptions: {
    name: `axon-${selection.bundle.id}`,
  },
});
```

Production applications should prefer content-hashed worker and WASM filenames and update the manifest atomically with the app release. Do not point `workerUrl` at a blob URL unless the deployment CSP explicitly permits that. Do not mark SIMD, threaded, or SIMD-threaded bundle entries as `available` until those exact artifacts are built, hosted, size-gated, and covered by browser smoke tests.

The default baseline remains single-threaded and must work without cross-origin isolation, SIMD, or shared memory.

## Vite Embedding

The Axon web app uses Vite for local browser testing. For an application-owned embedding, place the worker JS and WASM under `public/workers/` or import package assets once a publishable package exists.

`apps/axon-web` has one product HTML entry: `index.html` for the editor SPA. The editor uses History API routes, so static hosts must rewrite `/connect` and any future editor routes to `index.html`. The legacy sandbox page is not a production entrypoint.

Example manifest override:

```ts
import {
  createAxonBrowserClient,
  getPlatformFeatures,
  selectBundle,
  type BrowserBundleManifest,
} from "./axon-browser-sdk";

const manifest: BrowserBundleManifest = {
  bundles: [
    {
      id: "baseline",
      tier: "baseline",
      workerUrl: new URL(
        "/workers/browser-engine-worker.js",
        window.location.origin,
      ),
      wasmUrl: new URL(
        "/workers/browser_engine_worker.wasm",
        window.location.origin,
      ),
    },
  ],
};

const selection = selectBundle(manifest, getPlatformFeatures());
const client = createAxonBrowserClient({
  workerUrl: selection.bundle.workerUrl,
});
```

Development headers can live in `vite.config.ts`:

```ts
import { defineConfig } from "vite";

const enableCrossOriginIsolation = process.env.AXON_BROWSER_COI === "1";

export default defineConfig({
  server: {
    headers: {
      "Content-Security-Policy": [
        "default-src 'self'",
        "script-src 'self' 'wasm-unsafe-eval'",
        "worker-src 'self'",
        "connect-src 'self' https://storage.googleapis.com https://*.storage.googleapis.com",
        "object-src 'none'",
        "base-uri 'none'",
      ].join("; "),
      ...(enableCrossOriginIsolation
        ? {
            "Cross-Origin-Opener-Policy": "same-origin",
            "Cross-Origin-Embedder-Policy": "require-corp",
          }
        : {}),
    },
  },
});
```

Some browser/CSP combinations still require `'unsafe-eval'` for WASM compilation when `'wasm-unsafe-eval'` is not honored. Treat that as a deployment exception and keep it scoped to the app that loads Axon, not to unrelated origins.

## Plain Browser Embedding

For a no-bundler host, serve ESM files and static assets directly:

```html
<script type="module">
  import {
    createAxonBrowserClient,
    getPlatformFeatures,
    selectBundle,
  } from "/vendor/axon/axon-browser-sdk.js";

  const manifest = {
    bundles: [
      {
        id: "baseline",
        tier: "baseline",
        workerUrl: "/vendor/axon/browser-engine-worker.js",
        wasmUrl: "/vendor/axon/browser_engine_worker.wasm",
      },
    ],
  };

  const { bundle } = selectBundle(manifest, getPlatformFeatures());
  const client = createAxonBrowserClient({ workerUrl: bundle.workerUrl });

  const snapshot = await fetch("/api/tables/events/browser-snapshot", {
    credentials: "include",
  }).then((response) => response.json());

  await client.openDeltaTable("events", snapshot);
  const result = await client.query(
    "events",
    "SELECT COUNT(*) AS row_count FROM events",
  );
  console.log(result.result.content_type, result.result.bytes.byteLength);
</script>
```

Serve the static assets with:

```text
/vendor/axon/*.js
  Content-Type: application/javascript; charset=utf-8
  Cache-Control: public, max-age=31536000, immutable

/vendor/axon/*.wasm
  Content-Type: application/wasm
  Cache-Control: public, max-age=31536000, immutable

/vendor/axon/manifest.json
  Content-Type: application/json
  Cache-Control: no-cache
```

Use immutable caching only when filenames include a content hash or release version. Otherwise use `Cache-Control: no-cache` for worker and WASM assets so clients do not mix an old worker with a new WASM binary.

If worker or WASM assets are served from a CDN or another origin, that origin must explicitly allow the app origin and must be compatible with the chosen isolation mode:

```text
Access-Control-Allow-Origin: https://app.example.com
Cross-Origin-Resource-Policy: cross-origin
```

For COEP deployments, every worker script, WASM response, and imported module must be CORS-readable or carry a compatible `Cross-Origin-Resource-Policy` header. Same-origin hosting is the simpler baseline.

## CSP

Minimum production CSP for a same-origin worker and same-origin WASM asset:

```text
Content-Security-Policy:
  default-src 'self';
  script-src 'self' 'wasm-unsafe-eval';
  worker-src 'self';
  connect-src 'self' https://storage.googleapis.com https://*.storage.googleapis.com;
  object-src 'none';
  base-uri 'none'
```

Adjust `connect-src` to the exact trusted service and object URL origins used by the deployment. If worker assets are served from a CDN, add that CDN to `script-src` and `worker-src`. If data objects are read through a proxy, include only the proxy origin in `connect-src`.

Avoid `blob:` workers for the baseline package shape. If a host chooses blob workers for bundler reasons, it must add `blob:` to `worker-src` and accept that the deployed worker URL is no longer directly represented by the Axon manifest.

## Optional COOP/COEP Mode

Cross-origin isolation is optional for the baseline. It is required only for threaded bundle tiers that need `SharedArrayBuffer` and shared WASM memory.

Headers for the strict mode:

```text
Cross-Origin-Opener-Policy: same-origin
Cross-Origin-Embedder-Policy: require-corp
```

`Cross-Origin-Embedder-Policy: credentialless` can be evaluated by host applications that understand its credential behavior, but Axon's threaded bundles should still be gated by `getPlatformFeatures()` and browser smoke tests.

When COEP is enabled, every cross-origin subresource must be compatible with CORS or CORP. For Axon this includes CDN-hosted worker/WASM assets and object data URLs. Deployments that cannot guarantee those headers must stay on the baseline or another single-threaded bundle.

## CORS And Range Headers For Data Objects

Axon's HTTP object-store path uses `GET` with single-range `Range` requests. Metadata probes use `Range: bytes=0-0`; partial reads require `206 Partial Content` plus a valid `Content-Range`; identity-validated reads may send `If-Range` and require browser-visible `ETag`.

Object storage or proxy responses should allow:

```text
Access-Control-Allow-Origin: https://app.example.com
Access-Control-Allow-Methods: GET, OPTIONS
Access-Control-Allow-Headers: Range, If-Range
Access-Control-Expose-Headers: Accept-Ranges, Content-Length, Content-Range, ETag
Access-Control-Max-Age: 600
Vary: Origin
Accept-Ranges: bytes
```

If the narrow read proxy uses cookies or another credentialed browser request mode, it must also return:

```text
Access-Control-Allow-Credentials: true
Access-Control-Allow-Origin: https://app.example.com
Vary: Origin
```

Do not use `Access-Control-Allow-Origin: *` with credentialed proxy responses. Signed object URLs should normally avoid browser credentials and rely on the URL signature, object scoping, TTL, and exact-origin CORS policy instead.

Partial responses must include:

```text
HTTP/1.1 206 Partial Content
Content-Range: bytes <start>-<end>/<total>
Content-Length: <range-length>
ETag: "<stable-object-identity>"
```

Expose `ETag` whenever persistent or identity-validated extent reuse is enabled. Without browser-visible object identity, Axon cannot safely reuse validated extents across object replacement. For empty objects, a `416 Range Not Satisfiable` response to the `bytes=0-0` metadata probe must still expose `Content-Range: bytes */0` if the deployment expects empty objects to be handled through metadata probing.

## Signed URL Constraints

Browser object URLs must be HTTPS in production. Host-side loopback HTTP is only a test allowance.

Signed URL mode must preserve these constraints:

- URLs are object-scoped, not bucket-scoped or prefix-scoped.
- The descriptor URL set exactly covers the resolved snapshot active files; missing or extra paths are rejected by the control-plane attachment seam.
- URL TTL must cover snapshot bootstrap, query execution, range retries, and expected user interaction. This repo does not implement signed URL renewal.
- Query strings and fragments are treated as secrets. Axon redacts them in errors; host applications should also avoid logging full signed URLs.
- CORS must be validated against the exact production endpoint style. For GCS, this means the XML API endpoint shape used by signed URLs, not the authenticated browser download endpoint.
- If a provider signs request headers or method constraints, the service must allow browser `GET` range requests and any required `If-Range` validation.

No browser package may include service-account JSON, long-lived cloud tokens, cloud SDK credential flows, signing code, or arbitrary bucket traversal configuration.

## Cache Behavior

Static worker and WASM assets should be cached like application release artifacts. Use content hashes or versioned paths for immutable caching; otherwise force revalidation.

Snapshot descriptors and signed URL payloads should be request-scoped or short-lived. They should not be stored in shared browser caches. If a service returns descriptors through an authenticated app API, use `Cache-Control: no-store` unless the service has an explicit per-user cache policy.

Data object HTTP caching is deployment-specific. Axon's own extent cache keys validated bytes by resource and object identity. The object-store seam now has an OPFS-backed persistent extent adapter, but persistence is opportunistic: persistent cache load/store failures become cache misses and should not fail the query. `wasm-query-session` remains in-memory only; IndexedDB and session-level persistent table caches are not implemented in this repository.

Rotating signed URL query strings for the same object can reduce the browser HTTP cache hit rate because browser caches usually key by the full URL. Axon's persistent extent cache should rely on stable object identity such as `ETag`, not on signed query parameters.

## Package And Export Guidance

When Axon adds a publishable browser package, the package should expose the browser SDK, manifest helpers, and worker assets without leaking Rust crate internals into application imports.

Recommended package shape:

```json
{
  "name": "@axon/browser",
  "type": "module",
  "files": ["dist"],
  "exports": {
    ".": {
      "types": "./dist/index.d.ts",
      "import": "./dist/index.js"
    },
    "./manifest": {
      "types": "./dist/manifest.d.ts",
      "import": "./dist/manifest.js"
    },
    "./worker": {
      "types": "./dist/worker.d.ts",
      "import": "./dist/worker.js"
    },
    "./worker/browser-engine-worker.js": "./dist/worker/browser-engine-worker.js",
    "./worker/browser_engine_worker.wasm": "./dist/worker/browser_engine_worker.wasm"
  }
}
```

The top-level export should include:

- `createAxonBrowserClient`
- `getPlatformFeatures`
- `selectBundle`
- `AXON_BROWSER_BUNDLE_MANIFEST`
- `AxonWorkerError`, `AxonSdkError`, and `AxonProtocolError`
- worker command/response, snapshot descriptor, fallback, bundle manifest, and Arrow IPC result types
- `ARROW_IPC_STREAM_CONTENT_TYPE` and `ARROW_IPC_FILE_CONTENT_TYPE`

The worker subpath should provide a module worker entry that listens for `BrowserWorkerCommand` messages, calls the WASM worker implementation, and posts `BrowserWorkerResponseEnvelope` messages. The worker implementation must continue to return Arrow IPC for large results.

The package should document manifest-driven construction:

```ts
import {
  AXON_BROWSER_BUNDLE_MANIFEST,
  createAxonBrowserClient,
  getPlatformFeatures,
  selectBundle,
} from "@axon/browser";

const selection = selectBundle(
  AXON_BROWSER_BUNDLE_MANIFEST,
  getPlatformFeatures(),
);

const client = createAxonBrowserClient({
  workerUrl: selection.bundle.workerUrl,
});
```

and direct asset imports for bundlers that support URL imports:

```ts
import { createAxonBrowserClient } from "@axon/browser";
import workerUrl from "@axon/browser/worker/browser-engine-worker.js?url";

const client = createAxonBrowserClient({ workerUrl });
```

Do not expose cloud-provider signing helpers from the browser package. Signed URL issuance and proxy-mode read issuance belong to the trusted service boundary.

## Implementation Gaps

These gaps remain after the current docs-only deployment guidance:

- No publishable npm package exists in the main checkout.
- The private sandbox package has no package `exports` map because it is not intended for publication.
- The manifest references worker and WASM URLs, but the repo does not yet ship a production JavaScript worker bootstrap that loads `browser_engine_worker.wasm` and bridges browser `postMessage` to the Rust worker command handler.
- SIMD, threaded, and SIMD-threaded bundle entries are future tiers, not shipped artifacts.
- Signed URL issuance, proxy-mode request issuance, signed URL renewal, audit logging, request correlation, and production CORS/origin validation remain external service work.
