# E1 — Catalog Providers and Table Explorer — Revised Execution Plan

- Original date: 2026-06-20
- Revised: 2026-07-16
- Status: Proposed, narrowed after architecture review
- Audited baseline: `origin/main` at
  `7681f1dfa5bdaaae3ff2ccff79cc8be76ec1503a`
- Depends on: E0 state, routing, and persistence; E9 Slice 1 source authority and
  lifecycle; the following E3A pre-adoption contract correction; E6
  session-proxied remote access
- Feeds: E2 catalog-aware SQL and E9 table execution
- Related: [provider model](../program/provider-model.md),
  [ADR-0002](../adr/ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md),
  [ADR-0010](../adr/ADR-0010-pluggable-catalog-providers.md), and
  [E9 vertical slices](./2026-07-15-e9-execution-provider-vertical-slice-plan.md)

## Outcome

E1 introduces one discovery-only `CatalogProvider` interface and a table-first
Catalog Explorer. It migrates the working local Delta and public object-storage
sources behind that discovery seam, then adds Unity Catalog metadata through a
same-origin session proxy.

E1 ends when a selected table is represented by a stable logical reference.
Read authorization, descriptor materialization, preview, query execution, and
fallback belong to E9. E1 may discover a minimal volume root and hand its
`VolumeRef` to E8; directory traversal, file access, and preview belong to E8.

## Current implementation

The audited baseline already has:

- local Delta and public object-storage connection and query paths;
- a persisted connected-catalog tree and active table selection;
- a single-table runtime source model in `services/query-source.ts`;
- generated `axon/catalog/v1` messages, not yet adopted by catalog providers;
- generated `axon/fs/v1` messages, with E8 provider, adapter, UI, and runtime
  adoption still deferred;
- generated `axon.common.v1.ProviderCapabilities.authority`, an unadopted field
  that E3A must remove before E1 adoption because discovery does not own access
  or enforcement authority;
- Unity Catalog connection UI whose production access posture is not yet the
  E1 target end to end.

There is not yet one `CatalogProvider` interface used by all sources. Discovery,
connection state, and execution-source selection remain coupled in existing UI
and service modules. E1 must preserve the working query paths while it extracts
discovery.

## Target boundary

### `CatalogProvider` owns discovery only

The provider answers what tables exist and returns table metadata. It does not
decide whether a table may be read, mint access, materialize browser
descriptors, execute SQL, or preview rows.

The first interface is deliberately small:

```ts
type DiscoveryContext = {
  signal: AbortSignal;
  deadlineEpochMs?: number;
};
type DiscoveryPageRequest = DiscoveryContext & { cursor?: string };

interface CatalogProvider {
  readonly connection: CatalogConnection;
  listCatalogs(request: DiscoveryPageRequest): Promise<Page<CatalogNode>>;
  listSchemas(catalog: CatalogRef, request: DiscoveryPageRequest): Promise<Page<SchemaNode>>;
  listTables(schema: SchemaRef, request: DiscoveryPageRequest): Promise<Page<TableNode>>;
  getTableMetadata(ref: TableRef, context: DiscoveryContext): Promise<TableMetadata>;
  listVolumeRoots(schema: SchemaRef, request: DiscoveryPageRequest): Promise<Page<VolumeNode>>;
}
```

Views may be returned as table nodes with an explicit table kind. Do not add
separate methods until a UI consumer needs different behavior.

The page's next cursor is the only pagination signal. Table kind distinguishes a
view from a table. A provider with no volume roots returns an empty page. Do not
add a generic discovery-capability bag until two implemented consumers require
a fact that the method and returned value cannot express.

TanStack Query wrappers live above the interface. Providers return plain async
pages so tests and non-React consumers do not import UI cache types. The wrapper
passes its abort signal and optional deadline through the local request context.

### A source profile composes the seams

A connection selects a source profile that composes compatible implementations
of catalog discovery, data-access resolution, execution, and, when present,
filesystem access. `CatalogProvider` is not the umbrella provider and must not
grow methods from the other seams.

Every `CatalogRef`, `SchemaRef`, `TableRef`, and `VolumeRef` is a kind-constrained
view of ADR-0010's `CanonicalResourceRef`: connection ID, versioned provider
namespace, resource kind, and exactly one non-empty provider-object-ID or
canonical-locator arm. Locator-based providers return capability-free output from
their versioned, fixture-tested canonicalizer. Display paths remain sibling
metadata and are never identity or authority.

```text
connection + catalog + schema + table
  -> CatalogProvider metadata
  -> TableRef
  -> E9 DataAccessResolver and ExecutionProvider
```

The Explorer never resolves a `TableRef` into credentials or a browser
descriptor.

A discovered `VolumeNode` is treated only as catalog identity in E1. The UI
hands its `VolumeRef` to E8; metadata such as a storage location is not an
access grant. E1 does not list directories, expose storage credentials, resolve
file access, or preview bytes.

## Provider profiles in E1

| Profile                       | Discovery behavior                                                             | Remote access posture                                                        |
| ----------------------------- | ------------------------------------------------------------------------------ | ---------------------------------------------------------------------------- |
| Local Delta                   | Synthesizes a flat catalog/schema/table hierarchy for the selected local table | No remote metadata request                                                   |
| Public object storage         | Synthesizes a flat hierarchy for the configured table root                     | Anonymous public metadata only                                               |
| Session-proxied Unity Catalog | Lists catalogs, schemas, tables, table metadata, and minimal volume roots      | Same-origin request with an opaque session; proxy holds upstream credentials |

Use the name `SessionProxiedUnityCatalog` in design prose and code unless a
shorter code token is required. "Direct Unity Catalog" is ambiguous because the
browser must not call an authenticated catalog with a PAT or cloud credential.

A future governed-host catalog can implement the same discovery interface. It
is not an E1 deliverable and is not needed to validate the seam.

## Session-proxied Unity Catalog contract

The browser calls a same-origin endpoint through one `SessionHttp` wrapper:

```text
browser -- opaque HttpOnly session --> Axon proxy -- upstream credential --> Unity Catalog
```

The wrapper:

- uses `credentials: "include"`;
- attaches a correlation ID;
- maps 401 to session expired, 403 to access denied, 404 to not found, and
  transport failure to unavailable;
- never reads or attaches a bearer token from browser state;
- validates the provider response at the adapter boundary before mapping it to
  `axon/catalog/v1` messages.

Development may route the same path through Vite to an OSS Unity Catalog
fixture. That is a development substitute for the proxy, not a production
credential model.

Unity Catalog's OpenAPI types remain its wire contract. The adapter maps only
the fields E1 needs into Axon's normalized catalog messages. Do not re-proto the
Unity Catalog REST API.

## Cache and persistence contract

Navigation metadata has an authority-scoped cache. Keys include provider
connection, principal/session namespace for protected metadata, catalog, schema,
object kind, and cursor. Local and public profiles use a non-session namespace.
Disconnecting or replacing a connection invalidates its full prefix; logout or
session replacement invalidates the protected namespace.

Cache rules:

- list and metadata responses may be cached and background-refreshed;
- list results may seed table-detail entries when they contain the same fields;
- only non-sensitive local or anonymous-public connection locators, labels,
  selections, and normalized metadata persist by default;
- session-proxied metadata remains memory-only unless the enforcing deployment
  approves principal-scoped persistence and supplies logout and revocation
  invalidation;
- session cookies, upstream credentials, signed URLs, read grants, access
  descriptors, and E9 resolution outcomes are never stored in the catalog
  cache or persisted with E1 state;
- a session or authorization failure invalidates affected remote metadata
  without deleting unrelated local/public state.

E2 consumes the normalized metadata cache. Deferred consumers do not justify a
parallel catalog store or additional E1 fields.

## UI scope

The first explorer is table-first:

- connection, catalog, and schema navigation;
- paginated table and view listing;
- table overview and columns;
- route-owned table selection and deep links;
- "Open in SQL editor" as a logical-reference handoff to E9;
- a disabled or explicitly unavailable preview action until the relevant E9
  slice lands.

Do not add functions, registered models, volume detail surfaces, file browsing,
lineage, permissions editing, or service embeds to make the tree look complete.
A volume root may appear only as a minimal E8 handoff when that route is
available.

Functions and registered models are deferred until a concrete workflow needs
them. E1 owns only the volume's catalog identity. E8 owns directory browsing and
file metadata through its filesystem adapter; selected-file access resolution
and preview reuse the E9 resolver and executor seams. E1 should not expose a
volume root in the UI until it can hand the `VolumeRef` to that experience.

## Milestones

### M0 — Extract discovery without changing execution

Entry gate: E9 Slice 1 has removed implicit source substitution and established
one accepted-run lifecycle. The E3A correction is the current generated-contract
baseline. E1 must not preserve or wrap the old wrong-source behavior.

- Add the minimal provider types, registry, and query wrappers.
- Express local Delta and public object-storage connections as flat discovery
  providers using generated `axon/catalog/v1` values.
- Leave the current execution path unchanged.
- Remove no legacy path until the provider contract tests prove equivalent
  discovery.

Gate: local Delta, public object-storage, and editor smoke tests remain green;
provider tests cover registry dispatch, table identity, pagination absence, and
structured errors.

### M1 — Table-first connections and Explorer

- Establish a stable connection-scoped ID distinct from table identity.
- Make the route the source of truth for selected connection, catalog, schema,
  and table.
- Add table/view tree navigation and table overview/columns.
- Make invalid route or stale selection states explicit. Never substitute the
  sample table or first queryable table for an explicitly selected missing
  table.
- Hand `TableRef` to the editor without resolving or executing it in E1.

Gate: deep-link, reload, disconnect, invalid-selection, and open-in-editor tests
pass. An invalid selected table produces an unavailable state, not different
data.

### M2 — Session-proxied Unity Catalog metadata

- Vendor or pin the official Unity Catalog OpenAPI schema and generate its wire
  types reproducibly.
- Implement catalog, schema, table, and table-metadata methods through
  `SessionHttp`.
- If the E8 handoff route is present, list minimal Unity Catalog volume roots
  and map them to `VolumeRef`; do not call a volume files API.
- Map and validate responses at one adapter boundary.
- Support cursors, list-to-detail seeding, and intent prefetch through the shared
  query wrappers.
- Replace decorative connection success with a real metadata request.

Gate: mock and integration tests cover pagination, view/table kinds, columns,
401, 403, 404, unavailable, malformed upstream data, correlation IDs, and
session loss. Persistence and bundle scans contain no credential material.

### M3 — Active cache consumer and cleanup

- Publish only the stable normalized metadata fields the active E2 consumer uses.
- Verify connection-prefix invalidation and bounded cache retention.
- Remove decorative Unity Catalog and Delta Sharing copy that implies an
  implemented execution path.
- Remove only discovery code made redundant by the adopted providers.

Gate: the E2 fixture consumer compiles against generated catalog messages; all
E1 regression and security checks pass; no E9 or E8 behavior is hidden in E1.

## Explicit non-goals

- Read plans, signed URLs, proxy grants, descriptor materialization, SQL
  fallback, preview, and query execution.
- A governed-host provider.
- Volume directory traversal, file metadata, access resolution, and preview;
  these are E8.
- Functions and registered models.
- A generic catalog transport or protobuf service.
- Browser-held Unity Catalog, cloud, or signing credentials.
- New client-state or HTTP libraries when E0 and `SessionHttp` already provide
  the needed seams.

## Error semantics

Provider methods return typed discovery errors with a safe message and
correlation ID. Unknown upstream states map to unavailable or invalid response;
they do not produce partial trusted metadata. Pagination cursor loops and
duplicate identities are adapter errors. The request `AbortSignal` propagates
from the query wrapper through the provider to the HTTP request. A cancelled
request does not populate the cache.

Retries are limited to idempotent metadata reads and only before the caller's
deadline. Do not retry access denied, session expired, invalid response, or a
cancelled request.

## Validation

At each milestone, run focused provider and UI tests plus:

```bash
cd apps/axon-web && npm run codegen:uc:check
cd apps/axon-web && npm run codegen:contracts:check
cd apps/axon-web && npm exec -- tsc --noEmit
cd apps/axon-web && npm run lint
cd apps/axon-web && npm run format:check
bash tests/security/verify_browser_dependency_guardrails.sh
```

The release gate is one usable table-discovery slice per provider profile, not
the number of object kinds represented in the explorer.

## Exit criteria

E1 is complete when all three provider profiles discover tables through the
same minimal interface, selected-table identity is explicit and stable,
session-proxied Unity Catalog metadata satisfies the browser secret boundary,
E2 can read the normalized metadata it actually needs, and E1 hands a logical
table reference to E9 without resolving access or executing a query. Any
discovered volume root is limited to a `VolumeRef` handoff to E8.
