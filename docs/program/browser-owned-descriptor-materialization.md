# Browser-Owned Descriptor Materialization

- Status: Current repo contract
- Date: 2026-05-19
- Scope: browser-safe Delta descriptor production in `apps/axon-web`,
  `query-contract`, `wasm-delta-snapshot`, and broker/runtime contracts
- Related:
  - [Browser Delta Compatibility Matrix](./browser-delta-compatibility-matrix.md)
  - [Browser Unity Catalog Brokered Runtime Contract](./browser-uc-brokered-runtime-contract.md)
  - [Browser Embedding Deployment Guide](./browser-embedding-deployment.md)

## Product Contract

The default browser product path is:

```text
browser-safe access material
  -> browser reconstructs or materializes the snapshot/descriptor
  -> BrowserHttpSnapshotDescriptor
  -> openDeltaTable()
  -> local query execution
```

When the source exposes table-root object access:

```text
browser reads/lists _delta_log
  -> wasm-delta-snapshot reconstructs snapshot
  -> BrowserHttpSnapshotDescriptor
```

When the source exposes file actions, manifests, or descriptors:

```text
browser-safe manifest / Delta Sharing file actions / provider file list
  -> browser materializes BrowserHttpSnapshotDescriptor directly
```

Core invariants:

- BFF/access broker does not query by default.
- BFF/access broker does not reconstruct snapshots by default.
- Browser/runtime owns local descriptor production when it has safe material.
- Server snapshot mode is an explicit enterprise server snapshot resolver mode.
- Server query mode is only explicit fallback, such as `sql_fallback_required`.
- Raw long-lived cloud credentials and provider secrets do not enter browser packages.
- `openDeltaTable(name, descriptor)` remains the single browser execution handoff.

Use these terms consistently:

- Access broker: authenticates, authorizes, and vends browser-safe access material.
- Browser snapshot resolver: browser-owned `_delta_log` reconstruction over
  table-root/object-grant access.
- Descriptor materializer: browser-owned conversion from manifests, Delta
  Sharing file actions, file lists, or explicit server snapshot resolver
  descriptors into `BrowserHttpSnapshotDescriptor`.
- Server snapshot resolver: optional enterprise mode that returns a descriptor.
- Server fallback: server-side SQL execution for governed assets.

## Source Matrix

| Source | Auth Owner | Access Broker | Snapshot Owner | Descriptor Material Source | Query Owner |
| --- | --- | --- | --- | --- | --- |
| Local files | Browser/user grant | None | Browser | Local file handles | Browser |
| Public bucket / HTTP | Browser-readable storage | None | Browser | HTTP table root / `_delta_log` when list/head/range are available | Browser |
| Private bucket | BFF brokers narrow access | BFF | Browser by default | Object grants, signed URLs, proxy URLs, manifests, or brokered descriptor | Browser |
| Delta Sharing URL mode | Sharing provider | Sharing server or app broker | Browser descriptor materialization | Sharing query response / file URLs | Browser |
| Delta Sharing dir mode | Sharing provider / BFF | Sharing server or BFF | Browser by default if safe object grants exist | Temporary directory-scoped grants, object grants, or brokered descriptor | Browser |
| Unity Catalog | UC/BFF | BFF | Browser by default | `ReadAccessPlan`, object grants, manifests, or descriptors | Browser unless fallback required |
| Governed UC fallback | UC/BFF | BFF | Server | SQL result batches | Databricks SQL / server fallback |

## Reconstruction Versus Materialization

Table-root reconstruction applies only when the browser has browser-safe access
to list/head/read the Delta table root and `_delta_log`. Examples include public
HTTP Delta tables with CORS and range support, local file handles, and brokered
object grants with list/head/range capabilities. This path reads Delta log
objects in the browser and uses `wasm-delta-snapshot` to produce the descriptor.

Descriptor materialization applies when another protocol or provider already
supplies the active file set, metadata, or a complete descriptor. Examples
include HTTP manifests, brokered manifests, provider file lists, Delta Sharing
URL-mode file actions, and trusted descriptors returned by an explicit server
snapshot resolver. These inputs are not forced back through `_delta_log` listing.

Both paths converge on `BrowserHttpSnapshotDescriptor -> openDeltaTable()`.

## Access Broker Boundary

An access broker may authenticate a user, authorize a table, evaluate policy,
issue opaque grant IDs, return browser-safe manifests, return short-lived
object-scoped URLs, or expose narrow list/head/range routes. It does not
reconstruct snapshots or execute SQL by default.

The access broker is not a cloud credential pass-through. Browser package APIs
must not accept raw cloud credentials, UC tokens, Databricks tokens, provider
bearer tokens, service-account JSON, broad SAS material, or signing secrets.

## Explicit Server Modes

A server snapshot resolver is an explicit mode for deployments that choose to
produce `BrowserHttpSnapshotDescriptor` server-side. It may own cloud
authentication, Delta log listing, active-file materialization, signed URL or
proxy URL construction, policy checks, audit, and correlation. The browser then
validates and opens the returned descriptor through `openDeltaTable()`.

Server fallback is a separate explicit mode for server-side SQL execution when a
governed asset cannot be read locally. Examples include `sql_fallback_required`
for row filters, column masks, views, unsupported table types, missing
external-engine support, or unavailable browser object access. UC fallback and
blocked states must not call `openDeltaTable()`.

## References

- [Delta Sharing protocol access modes](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#access-modes): URL mode returns pre-signed file URLs; directory mode issues temporary table-root cloud access for clients that can read the Delta log directly.
- [Databricks Unity Catalog credential vending](https://docs.databricks.com/aws/en/external-access/credential-vending): external-engine access is short-lived, eligibility-gated, and excludes governed shapes such as row filters, column masks, views, shared tables, materialized views, streaming tables, online tables, and foreign tables.
