# Browser Embedding Deployment Guide

- Date: 2026-05-15
- Audit revision: 2026-07-15
- Scope: browser host deployment guidance for Axon's worker-first WASM embedding surface
- Related:
  - [Browser Lakehouse Engine Strategy](./browser-lakehouse-engine-strategy.md)
  - [Browser Lakehouse Release Handoff](./browser-lakehouse-release-handoff.md)
  - [Browser Observability Contract](./browser-observability-contract.md)
  - [ADR-0002: Browser Access Uses Signed HTTPS Or A Narrow Proxy, Never Cloud Secrets](../adr/ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md)
  - [Axon Web Runtime](../../apps/axon-web/README.md)

## Current Package State

Axon's browser deployment is worker-first:

- browser hosts create a module `Worker` from a `workerUrl`
- large query results cross the worker boundary as Arrow IPC bytes, not row JSON
- table data reaches the browser only through browser-safe snapshot descriptors and object URLs minted by a trusted control-plane seam
- server query execution is an opt-in fallback module and correctness oracle, not something the default browser build calls on its own

There's no publishable npm package in the repo yet. The browser-facing TypeScript wrapper lives in the private app package at [`apps/axon-web/src/axon-browser-sdk.ts`](../../apps/axon-web/src/axon-browser-sdk.ts), and the only `package.json` in the main checkout is the private `@axon/web` runtime package. The app package already has the Daxis-facing worker bridge and generated WASM outputs, but it does not have a public npm export map or package asset-copy step.

The Daxis-facing default bundle is the browser DataFusion app worker. Its worker entrypoint is `apps/axon-web/src/sandbox-query-worker.ts`, and `npm run build:wasm` generates `apps/axon-web/src/wasm/axon_web_wasm.js` plus `axon_web_wasm_bg.wasm` from the `axon-web-wasm` crate. App-owned deployments should package that worker as `axon-web-worker.js` and host the matching `axon_web_wasm_bg.wasm` asset. The legacy `browser-engine-worker` artifact remains compatibility-only and is not the Daxis default worker.

So you can use this guide two ways:

- as the contract for an application-owned embedding while the SDK is example-owned
- as the export and asset layout target for the future publishable browser package

## Current And Target Read Handoff

The current SDK passes `BrowserHttpSnapshotDescriptor` to `openDeltaTable()`
and returns source-specific expiry and correlation metadata beside it. The
target keeps that worker command and makes the caller hold one
`ResolvedBrowserRead` binding containing the descriptor, access class, not-after time when one
exists, exact selected and resolved resource identity, provenance, and optional
correlation ID.

The target execution path validates source identity and expiry before opening
or querying the table. If the binding has expired, it discards the binding and
resolves the same selected source again. It does not reuse signed URLs, advance
an explicitly pinned snapshot, or choose a sample or different connected source.
The unified binding is target contract work; the current SDK caller must keep
the equivalent descriptor and metadata together until that work lands.

## Deployment Artifacts

| Artifact              | Current repo source                                                                                                                                                                                              | Runtime URL                         | Required content type                                     | Cache policy                                                                           |
| --------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------- | --------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| TypeScript SDK module | `apps/axon-web/src/axon-browser-sdk.ts`                                                                                                                                                                          | app bundle or future package import | `application/javascript; charset=utf-8` after bundling    | bundle with the app release                                                            |
| Worker module         | Daxis-facing default bundle from `apps/axon-web/src/sandbox-query-worker.ts`, packaged by the host as `axon-web-worker.js`; legacy `browser-engine-worker` remains compatibility-only                            | `workerUrl` manifest field          | `application/javascript; charset=utf-8`                   | immutable when content-hashed; otherwise short cache                                   |
| Worker WASM           | `axon_web_wasm_bg.wasm` generated by `npm run build:wasm` from the `axon-web-wasm` crate; the release size gate measures the matching raw artifact at `target/wasm32-unknown-unknown/release/axon_web_wasm.wasm` | `wasmUrl` manifest field            | `application/wasm`                                        | immutable when content-hashed                                                          |
| Bundle manifest       | `AXON_BROWSER_BUNDLE_MANIFEST` or host-supplied equivalent                                                                                                                                                       | app bundle or JSON endpoint         | `application/json` for external manifests                 | short cache or revalidate                                                              |
| Snapshot descriptor   | browser-produced or explicit server snapshot resolver response                                                                                                                                                   | app API response or browser runtime | `application/json`                                        | memory-only; service responses use `no-store`                                          |
| Data objects          | public HTTPS, signed HTTPS, or a narrow read proxy                                                                                                                                                               | descriptor `active_files[*].url`    | object content type is not semantically important to Axon | durable only for eligible local/public access; governed access defaults to memory-only |

`wasmUrl` is deployment metadata in the current TypeScript wrapper. `createAxonBrowserClient()` builds the module worker from `workerUrl`; the worker module loads the matching WASM asset. The Daxis-facing default bundle names `axon-web-worker.js` and `axon_web_wasm_bg.wasm`, matching the `axon-web-wasm` artifact report and size-gate commands.

## Delta Location Opening Modes

`openDeltaLocation()` is the browser SDK convenience layer for source-based Delta opens. Its default mode is browser-owned descriptor production:

- table-root/object-grant sources use the browser snapshot resolver to read `_delta_log` and produce `BrowserHttpSnapshotDescriptor`
- manifest, Delta Sharing URL-mode file actions, provider file lists, and trusted descriptors use the descriptor materializer directly
- every successful browser-owned path forwards the descriptor unchanged through `openDeltaTable(name, descriptor)`

Those bullets describe current behavior. The target materializer attaches the
access class, lifetime, selected resource, resolved resource, provenance, and
correlation to form the `ResolvedBrowserRead` binding before the execution path
calls the same worker command.

Access-broker integration is explicit. An access broker may authenticate, authorize, vend an opaque grant, return browser-safe manifest material, or expose narrow list/head/range routes. It doesn't reconstruct snapshots or execute SQL by default.

Unity Catalog discovery and read planning are session-proxied metadata calls.
The browser uses the deployment's session-aware BFF and an opaque cookie; it
does not store a UC token or attach provider authorization from browser state.
Browser-local policy may improve UX or reject an impossible target early. The
broker or server enforces remote resource policy and owns the authoritative
audit record.

Source selection is exact. A missing route target, stale connection, or
descriptor whose table identity differs from the selected source returns an
error. The embedding must not fall back to the first queryable table or the
sample fixture. A sample runs only after the host selects it explicitly.

Server snapshot resolution is also explicit. In that mode, `openDeltaLocation()` sends a typed request to a trusted server snapshot resolver:

- `provider`: `s3`, `gcs`, or `azure_blob`
- `table_uri`: logical URI such as `s3://bucket/table`, `gs://bucket/table`, `az://account/container/table`, or `abfs://container@account.dfs.core.windows.net/table`
- `credential_profile`: opaque storage access profile, not a cloud credential
- `requested_access_mode`: server snapshot resolver-only `auto`, `signed_url`, or `proxy`
- `snapshot_version`: optional exact Delta snapshot version

The server snapshot resolver response is authoritative. It includes the returned `BrowserHttpSnapshotDescriptor`, `resolved_snapshot_version`, resolver-only `actual_access_mode`, `expires_at_epoch_ms`, optional `correlation_id`, and optional warnings. The SDK checks that `resolved_snapshot_version` matches `descriptor.snapshot_version`, rejects expired descriptors with `descriptor_expired`, forwards the descriptor unchanged through the existing `openDeltaTable()` worker command, and returns the non-descriptor metadata to the caller for display and retry decisions. Reopening through the server snapshot resolver is the M1 expiry behavior; any refresh endpoint has to enforce that refresh never advances the resolved snapshot version.

In the target contract, those response fields become one resolved browser read
`ResolvedBrowserRead` binding. The execution path checks its not-after time again immediately before
opening and before a later query dispatch. Expiry triggers a fresh resolution of
the same source and snapshot intent; it never triggers a source fallback.

The server snapshot resolver owns cloud authentication, Delta log listing/checkpoint selection, active-file materialization, signed URL or proxy URL construction, CORS validation, policy, audit, and request correlation. `auto` normally lets it pick signed URLs to cut service data-plane bandwidth. `proxy` fits centralized audit/control, private networking, or CORS avoidance, at higher service cost.

## Worker And WASM URLs

Use explicit URLs in the bundle manifest. The baseline manifest shape is:

```ts
import {
  AXON_BROWSER_BUNDLE_MANIFEST,
  createAxonBrowserClient,
  getPlatformFeatures,
  selectBundle,
} from './axon-browser-sdk';

const features = getPlatformFeatures();
const selection = selectBundle(AXON_BROWSER_BUNDLE_MANIFEST, features);

const client = createAxonBrowserClient({
  workerUrl: selection.bundle.workerUrl,
  workerOptions: {
    name: `axon-${selection.bundle.id}`,
  },
});
```

Production apps should use content-hashed worker and WASM filenames and update the manifest atomically with the app release. Don't point `workerUrl` at a blob URL unless the deployment CSP explicitly permits it. Don't mark SIMD, threaded, or SIMD-threaded bundle entries as `available` until those exact artifacts are built, hosted, size-gated, and covered by browser smoke tests.

The default baseline is single-threaded and has to work without cross-origin isolation, SIMD, or shared memory.

## Vite Embedding

The Axon web app uses Vite for local browser testing. For an application-owned embedding, place the worker JS and WASM under `public/workers/` or import package assets once a publishable package exists.

`apps/axon-web` has one product HTML entry: `index.html` for the editor SPA. The editor uses History API routes, so static hosts have to rewrite `/connect` and any future editor routes to `index.html`. The legacy sandbox page is not a production entrypoint.

Example manifest override:

```ts
import {
  createAxonBrowserClient,
  getPlatformFeatures,
  selectBundle,
  type BrowserBundleManifest,
} from './axon-browser-sdk';

const manifest: BrowserBundleManifest = {
  bundles: [
    {
      id: 'baseline',
      tier: 'baseline',
      workerUrl: new URL('/workers/axon-web-worker.js', window.location.origin),
      wasmUrl: new URL('/workers/axon_web_wasm_bg.wasm', window.location.origin),
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
import { defineConfig } from 'vite';

const enableCrossOriginIsolation = process.env.AXON_BROWSER_COI === '1';

export default defineConfig({
  server: {
    headers: {
      'Content-Security-Policy': [
        "default-src 'self'",
        "script-src 'self' 'wasm-unsafe-eval'",
        "worker-src 'self'",
        "connect-src 'self' https://storage.googleapis.com https://*.storage.googleapis.com",
        "object-src 'none'",
        "base-uri 'none'",
      ].join('; '),
      ...(enableCrossOriginIsolation
        ? {
            'Cross-Origin-Opener-Policy': 'same-origin',
            'Cross-Origin-Embedder-Policy': 'require-corp',
          }
        : {}),
    },
  },
});
```

Some browser/CSP combinations still need `'unsafe-eval'` for WASM compilation when `'wasm-unsafe-eval'` isn't honored. Treat that as a deployment exception and keep it scoped to the app that loads Axon, not to unrelated origins.

## Plain Browser Embedding

For a no-bundler host, serve ESM files and static assets directly:

```html
<script type="module">
  import {
    createAxonBrowserClient,
    getPlatformFeatures,
    selectBundle,
  } from '/vendor/axon/axon-browser-sdk.js';

  const manifest = {
    bundles: [
      {
        id: 'baseline',
        tier: 'baseline',
        workerUrl: '/vendor/axon/axon-web-worker.js',
        wasmUrl: '/vendor/axon/axon_web_wasm_bg.wasm',
      },
    ],
  };

  const { bundle } = selectBundle(manifest, getPlatformFeatures());
  const client = createAxonBrowserClient({ workerUrl: bundle.workerUrl });

  const snapshot = await fetch('/api/tables/events/browser-snapshot', {
    credentials: 'include',
  }).then((response) => response.json());

  await client.openDeltaTable('events', snapshot);
  const result = await client.query('events', 'SELECT COUNT(*) AS row_count FROM events');
  console.log(result.result.content_type, result.result.bytes.byteLength);
</script>
```

This snippet demonstrates the current bare-descriptor interface. Do not copy it
for signed or granted access. Governed deployments should use the resolver or
broker path that preserves expiry and correlation with the descriptor, return
that response with `Cache-Control: no-store`, and apply the resolved-binding
checks above.

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

Use immutable caching only when filenames include a content hash or release version. Otherwise use `Cache-Control: no-cache` for worker and WASM assets so clients don't mix an old worker with a new WASM binary.

If worker or WASM assets are served from a CDN or another origin, that origin has to allow the app origin and be compatible with the chosen isolation mode:

```text
Access-Control-Allow-Origin: https://app.example.com
Cross-Origin-Resource-Policy: cross-origin
```

For COEP deployments, every worker script, WASM response, and imported module has to be CORS-readable or carry a compatible `Cross-Origin-Resource-Policy` header. Same-origin hosting is the simpler baseline.

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

Set `connect-src` to the exact trusted service and object URL origins the deployment uses. If worker assets are served from a CDN, add that CDN to `script-src` and `worker-src`. If data objects are read through a proxy, put only the proxy origin in `connect-src`.

Avoid `blob:` workers for the baseline package shape. If a host chooses blob workers for bundler reasons, it has to add `blob:` to `worker-src` and accept that the deployed worker URL is no longer directly represented by the Axon manifest.

## Optional COOP/COEP Mode

Cross-origin isolation is optional for the baseline. It's required only for threaded bundle tiers that need `SharedArrayBuffer` and shared WASM memory.

Headers for the strict mode:

```text
Cross-Origin-Opener-Policy: same-origin
Cross-Origin-Embedder-Policy: require-corp
```

Host applications that understand its credential behavior can evaluate `Cross-Origin-Embedder-Policy: credentialless`, but Axon's threaded bundles should still be gated by `getPlatformFeatures()` and browser smoke tests.

When COEP is enabled, every cross-origin subresource has to be compatible with CORS or CORP. For Axon that's CDN-hosted worker/WASM assets and object data URLs. Deployments that can't guarantee those headers should stay on the baseline or another single-threaded bundle.

## CORS And Range Headers For Data Objects

Axon's HTTP object-store path uses `GET` with single-range `Range` requests. Metadata probes use `Range: bytes=0-0`; partial reads need `206 Partial Content` plus a valid `Content-Range`; identity-validated reads may send `If-Range` and need a browser-visible `ETag`.

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

If the narrow read proxy uses cookies or another credentialed browser request mode, it also has to return:

```text
Access-Control-Allow-Credentials: true
Access-Control-Allow-Origin: https://app.example.com
Vary: Origin
```

Don't use `Access-Control-Allow-Origin: *` with credentialed proxy responses. Signed object URLs should normally avoid browser credentials and rely on the URL signature, object scoping, TTL, and exact-origin CORS policy instead.

Partial responses have to include:

```text
HTTP/1.1 206 Partial Content
Content-Range: bytes <start>-<end>/<total>
Content-Length: <range-length>
ETag: "<stable-object-identity>"
```

Expose `ETag` whenever persistent or identity-validated extent reuse is enabled. Without browser-visible object identity, Axon can't safely reuse validated extents across object replacement. For empty objects, a `416 Range Not Satisfiable` response to the `bytes=0-0` metadata probe still has to expose `Content-Range: bytes */0` if the deployment expects empty objects to be handled through metadata probing.

## Signed URL Constraints

Browser object URLs have to be HTTPS in production. Host-side loopback HTTP is only a test allowance.

Signed URL mode has to hold these constraints:

- URLs are object-scoped, not bucket-scoped or prefix-scoped.
- The descriptor URL set exactly covers the resolved snapshot active files; missing or extra paths are rejected by the control-plane attachment seam.
- URL TTL should cover snapshot bootstrap, query execution, and expected range retries, but TTL sizing is not an authorization check. The current repo does not renew signed URLs. The target binding records the earliest not-after time and fails closed before open or query dispatch; the caller then resolves the same source again.
- Query strings and fragments are secrets. Axon redacts them in errors; host applications should also avoid logging full signed URLs.
- CORS has to be validated against the exact production endpoint style. For GCS, that's the XML API endpoint shape used by signed URLs, not the authenticated browser download endpoint.
- If a provider signs request headers or method constraints, the service has to allow browser `GET` range requests and any required `If-Range` validation.

No browser package may include service-account JSON, long-lived cloud tokens, cloud SDK credential flows, signing code, or arbitrary bucket traversal configuration.

## Cache Behavior

Cache static worker and WASM assets like application release artifacts. Use content hashes or versioned paths for immutable caching; otherwise force revalidation.

Resolved bindings, descriptors, grants, and signed URL payloads are scoped to one
admission/execution and memory-only. Rejection or terminal execution disposes
them; a later execution resolves new access. Do not place them in localStorage, IndexedDB,
OPFS, Cache Storage, persisted query state, logs, or saved documents. Services
that return them use `Cache-Control: no-store`.

Axon's extent cache keys validated bytes by resource and object identity. Apply
this retention policy before enabling its OPFS-backed persistent adapter:

- Local-file and anonymous public-object bytes may use durable storage when the
  cache key includes stable source identity and a strong validator such as
  `ETag` or provider generation.
- Governed, signed, Delta Sharing, and grant-backed bytes remain memory-only by
  default. Retaining those bytes can extend access beyond expiry, logout, or
  revocation.
- A deployment may enable persistent governed bytes only with a
  principal-and-session namespace and guaranteed logout and revocation
  invalidation. The deployment owns retention and audit for that cache.
- Signed query strings and grant IDs never form cache identity. A rotated signed
  URL for the same validated object does not authorize persistence.

The current repository has a generic OPFS-backed extent adapter, but it does not
yet carry the target access-class policy through one resolved binding. Its
existence does not make governed persistence safe. Keep persistent writes off
for governed and signed sources until that policy gate and invalidation contract
exist. `wasm-query-session` remains in-memory only; IndexedDB and session-level
persistent table caches are not implemented here.

## Package And Export Guidance

When Axon adds a publishable browser package, it should expose the browser SDK, manifest helpers, and worker assets without leaking Rust crate internals into application imports.

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
    "./worker/axon-web-worker.js": "./dist/worker/axon-web-worker.js",
    "./worker/axon_web_wasm_bg.wasm": "./dist/worker/axon_web_wasm_bg.wasm"
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

The worker subpath should provide a module worker entry that listens for
`BrowserWorkerCommand` messages, calls the WASM worker implementation, and posts
`BrowserWorkerResponseEnvelope` messages. Results remain Arrow IPC. The initial
path returns one byte-budgeted buffer whose collector and encoder stop at the
budget plus fixed overhead; chunked result delivery requires explicit credits or
acknowledgements and a bounded-queue test.

The package should document manifest-driven construction:

```ts
import {
  AXON_BROWSER_BUNDLE_MANIFEST,
  createAxonBrowserClient,
  getPlatformFeatures,
  selectBundle,
} from '@axon/browser';

const selection = selectBundle(AXON_BROWSER_BUNDLE_MANIFEST, getPlatformFeatures());

const client = createAxonBrowserClient({
  workerUrl: selection.bundle.workerUrl,
});
```

and direct asset imports for bundlers that support URL imports:

```ts
import { createAxonBrowserClient } from '@axon/browser';
import workerUrl from '@axon/browser/worker/axon-web-worker.js?url';

const client = createAxonBrowserClient({ workerUrl });
```

Don't expose cloud-provider signing helpers from the browser package. Signed URL issuance and proxy-mode read issuance belong to the trusted service boundary.

## Implementation Gaps

What's still missing after the current docs-only deployment guidance:

- No publishable npm package in the main checkout.
- The private sandbox package has no package `exports` map because it isn't meant for publication.
- The repo has a private Vite-packaged worker bridge in `apps/axon-web/src/sandbox-query-worker.ts`, but not a standalone npm-package asset pipeline that copies `axon-web-worker.js` and `axon_web_wasm_bg.wasm` into `dist/worker/`.
- SIMD, threaded, and SIMD-threaded bundle entries are future tiers, not shipped artifacts.
- Signed URL issuance, proxy-mode request issuance, signed URL renewal, audit logging, request correlation, and production CORS/origin validation are external service work.
- The SDK does not yet expose one `ResolvedBrowserRead` binding or revalidate it
  at both table open and query dispatch.
- The generic persistent extent adapter does not yet gate writes by access class;
  governed and signed sources must remain memory-only until it does.
- Legacy app source selection can still choose a first or sample source outside
  the route resolver. The target embedding treats missing or stale selection as
  an error.
