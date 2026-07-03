# Query Foundation

This directory is the query substrate for `axon-web`. It provides the shared React Query client factory, a stable default client for app runtime use, typed query key factories, and the browser cache persistence policy for safe server-state families.

## Query Client

`createAxonQueryClient()` returns a `QueryClient` with the Axon retry policy and a query `gcTime` aligned to the persisted cache max age.

`queryClient` is the module-level default client used by `AppProviders`. Tests can inject their own client through `AppProviders` instead of creating clients during render.

## Query Cache Persistence

`createAxonQueryPersistOptions()` configures `PersistQueryClientProvider` with an IndexedDB-backed persister built on the repo-local `KeyValueStore`. Persistence is best-effort: unavailable IndexedDB, open failures, read failures, write failures, and remove failures are treated as no-ops so the in-memory query cache remains the runtime authority.

The persisted cache is versioned by `AXON_QUERY_CACHE_BUSTER` and expires after `AXON_QUERY_CACHE_MAX_AGE_MS` (six hours). Mutations are never persisted. Query dehydration is routed through `shouldPersistAxonQuery(query)`, the single allow/deny policy for this cache.

Persisted query families are intentionally narrow:

- `['catalog', 'source', ['manifest', ...], 'table-derived' | 'commits']`
- `['catalog', 'source', ['object_store_table_root', 'gcs' | 's3', ...], 'table-derived' | 'commits']`
- `['local', 'history']`
- `['local', 'saved']`

The policy rejects `local_delta` catalog keys, unknown query families, failed queries, mutations, signed URL strings, token/grant/credential-shaped data, openable browser handles, descriptors, object lists, active-file/session/worker/run-result payloads, metrics, plans, and capabilities.

## Retry Policy

`shouldRetryQuery` does not retry aborted or cancelled requests. It also does not retry known 4xx client errors, including `401`, `403`, and `404`.

Unknown failures, network-style failures, and 5xx-style failures may retry up to two times after the first failure.

## Query Keys

`queryKeys` is the canonical key factory for this slice. Catalog keys are scoped by a deterministic source identity that uses the same fields as `sameQuerySource`, not raw object identity:

- `queryKeys.catalog.root()`
- `queryKeys.catalog.source(source)`
- `queryKeys.catalog.tableDerived(source)`
- `queryKeys.catalog.commits(source)`
- `queryKeys.local.root()`
- `queryKeys.local.history()`
- `queryKeys.local.saved()`

Keep keys stable and route new query key families through this module.

## Catalog Server State

`catalogQueryOptions(source)` reads the current table-derived catalog through the legacy catalog service and seeds the query with `snapshotCatalog(source)`.

`commitsQueryOptions(source)` wraps commit-log loading. `AppProviders` installs a ref-counted runtime bridge that writes published runtime catalogs to the matching catalog query and invalidates the matching commits query.

## Local Metadata Server State

`historyQueryOptions()` and `savedQueriesQueryOptions()` wrap the existing metadata services. The mutation helpers call the same services and then update `QueryClient` with the returned entries so IndexedDB-unavailable fallback still updates current in-memory UI state.

## Boundary

The query layer owns cache identity, retry defaults, bridge wiring, local metadata adapters, and the safe persisted-cache policy. It does not own provider-specific execution logic, auth/session-expiry purging, live object-store credentials, route definitions, or result/run-state persistence.

This layer also does not add `CatalogProvider`, `DataAccessResolver`, `ExecutionProvider`, or any provider-specific execution logic. Those belong to later slices.
