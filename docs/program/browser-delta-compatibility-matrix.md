# Browser Delta Compatibility Matrix

- Status: Current repo contract
- Date: 2026-05-19
- Scope: browser-safe Delta reads through `apps/axon-web`, `query-contract`,
  `wasm-delta-snapshot`, `wasm-datafusion-session`, and the trusted resolver /
  broker contracts

This matrix is the browser Delta support boundary for the current repository. It
does not describe a production signing service, OAuth service, Unity Catalog BFF,
or object-store proxy. Those services remain trusted control-plane
responsibilities outside browser-target packages.

## Supported Browser Paths

| Path | State | Notes |
| --- | --- | --- |
| Trusted HTTP snapshot descriptor | Supported | Browser receives a `BrowserHttpSnapshotDescriptor` and opens it through the existing worker/session handoff. |
| Delta location resolver contract | Supported | Browser SDK sends logical object-store table URIs and non-secret storage profile handles; a trusted resolver returns a browser-safe descriptor and metadata. |
| Brokered Unity Catalog read plan | Supported as contract | Browser consumes `ReadAccessPlan` variants and object-grant capabilities. The production BFF and broker remain service-owned. |
| Delta Sharing URL-mode descriptors | Supported | Browser consumes externally governed files without browser-owned bearer-token persistence. |
| Browser DataFusion session in `apps/axon-web` | Supported app path | The app uses `wasm-datafusion-session` for DataFusion-backed SQL over opened descriptors. |
| Legacy narrow query runtime | Compatibility scaffold | `wasm-query-runtime` remains as a constrained planner/executor and test oracle during migration. |

## Snapshot Resolution

| Behavior | State | Notes |
| --- | --- | --- |
| Latest snapshot from trusted descriptor | Supported | Descriptor version and active files are treated as authoritative. |
| Explicit snapshot version from trusted descriptor | Supported | Refresh must preserve the resolved snapshot version unless the caller explicitly reopens latest. |
| Browser-side Delta log replay in `wasm-delta-snapshot` | Supported for tested layouts | Used for repo-owned fixture and brokered-object paths; unsupported layouts fail closed. |
| `_last_checkpoint` hints | Supported | Hints are used only when consistent with listed log objects. |
| Classic checkpoints | Supported | Covered by `wasm-delta-snapshot` checkpoint tests. |
| Multipart checkpoints | Supported when complete | Missing parts fail closed. |
| V2 / UUID checkpoints and sidecars | Supported for tested layouts | Sidecar paths must stay within the table root. |
| Unknown checkpoint layouts | Unsupported | Browser must fail closed instead of inferring protocol support. |

## Delta Features

| Feature | State | Browser behavior |
| --- | --- | --- |
| Append-only/read-only tables | Supported | Browser paths are read-only. |
| Partition values | Supported | String, integer, and boolean partition values are represented explicitly when known. |
| File statistics pruning | Supported for tested numeric stats | Browser may prune with validated metadata; residual filters remain above scan when needed. |
| Time travel | Supported through pinned descriptors | The trusted resolver or descriptor producer chooses the stable version. |
| Change data feed | Not executed in browser | Requires native/service fallback. |
| Column mapping | Not executed in browser | Requires native/service fallback unless a later compatibility contract adds support. |
| Deletion vectors | Not executed in browser | Detection/descriptor facts may exist, but scan application is not a current browser success claim. |
| Writes, OPTIMIZE, VACUUM, MERGE | Unsupported | Browser package is read-only. |

## Object Access

| Access mode | State | Notes |
| --- | --- | --- |
| HTTPS signed file URLs | Supported as descriptor input | URLs must be browser-safe, redacted in diagnostics, and expire explicitly. |
| Resolver `auto` mode | Supported | Trusted resolver decides signed URL versus proxy and reports the actual mode. |
| Resolver `proxy` mode | Supported as contract | Browser fetches narrow proxy URLs; production proxy implementation is outside this repo. |
| Object grants | Supported as contract/runtime seam | Browser can use grant-scoped list/head/batch-sign/range operations. |
| Raw cloud credentials in browser | Rejected | Access keys, SAS strings, OAuth tokens, service-account JSON, and bearer tokens are not accepted as browser SDK credentials. |
| Already-authenticated table URIs | Rejected for location open | Signed HTTPS table roots and credential-bearing URI userinfo/query parameters must not enter `openDeltaLocation`. |

## SQL Execution

| SQL surface | State | Notes |
| --- | --- | --- |
| Projection, filters, grouping, common aggregates | Supported where covered by corpus | Browser/native parity corpus is the source of truth. |
| `ORDER BY` / `LIMIT` over supported outputs | Supported where covered by corpus | Unsupported order expressions fail closed. |
| Broad SQL / joins / set operations / windows | Native or service fallback | Browser must return structured fallback or invalid-request errors rather than a wrong result. |
| DataFusion-backed app execution | Supported in `apps/axon-web` | This is the production app path; the shipped worker artifact still has separate size and capability gates. |

## Verification Anchors

- `cargo test -p query-contract --locked`
- `cargo test -p wasm-delta-snapshot --locked`
- `cargo test -p wasm-http-object-store --locked`
- `cargo test -p wasm-datafusion-session --locked`
- `cargo test -p wasm-query-runtime --locked`
- `npm run test:sdk` in `apps/axon-web`
- `npm run test:e2e` in `apps/axon-web`
- `bash tests/security/verify_browser_dependency_guardrails.sh`

Anything not explicitly supported here should be treated as unsupported in the
browser until a focused compatibility update and regression test land.
