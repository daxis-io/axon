# Query Foundation

This directory is the query substrate for `axon-web`. It provides the shared React Query client factory, a stable default client for app runtime use, typed query key factories, and the browser cache persistence policy for safe server-state families.

## Query Client

`createAxonQueryClient()` returns a `QueryClient` with the Axon retry policy and a query `gcTime` aligned to the persisted cache max age through `AXON_QUERY_GC_TIME_MS`.

`queryClient` is the module-level default client used by `AppProviders`. Tests can inject their own client through `AppProviders` instead of creating clients during render.

Resource adapters set explicit cache policy:

- Catalog summaries: `staleTime` is five minutes.
- Commit timelines: `staleTime` is one minute.
- Local history and saved queries: `staleTime` is one minute.
- All server-state query families use `gcTime === AXON_QUERY_CACHE_MAX_AGE_MS`.
- Placeholder `initialData` is marked stale with `initialDataUpdatedAt: 0`, so existing IndexedDB/local metadata still loads on first mount.

## Query Cache Persistence

`createAxonQueryPersistOptions()` configures `PersistQueryClientProvider` with an IndexedDB-backed persister built on the repo-local `KeyValueStore`. Persistence is best-effort: unavailable IndexedDB, open failures, read failures, write failures, and remove failures are treated as no-ops so the in-memory query cache remains the runtime authority.

The persisted cache is versioned by `AXON_QUERY_CACHE_BUSTER` and expires after `AXON_QUERY_CACHE_MAX_AGE_MS` (six hours). The buster changes when the app/cache schema changes. Mutations are never persisted. Query dehydration is routed through `shouldPersistAxonQuery(query)`, the single allow/deny policy for this cache.

Persisted query families are intentionally narrow:

- Explicit sample fixture catalog/commit leaves under `catalog/provider/axon.fixture/v1/.../authority/fixture`
- Anonymous public GCS/S3 catalog/commit leaves under `catalog/provider/axon.public-{gcs,s3}/v1/.../authority/non-session`
- `['local', 'history']`
- `['local', 'saved']`

The strict schema-v3 parser validates the full provider namespace, connection ID, authority, resource kind, canonical identity arm/value, requested snapshot (including explicit zero), and leaf. The policy rejects local catalog keys, session-scoped or unknown providers, unsafe locators, malformed identities, unknown query families, failed queries, mutations, signed URL strings, token/grant/credential-shaped data, openable browser handles, descriptors, object lists, active-file/session/worker/run-result payloads, metrics, plans, and capabilities. These fields remain runtime-only even when they appear inside otherwise allowed key families.

## Retry Policy

`shouldRetryQuery` does not retry aborted, cancelled, deadline-exceeded, invalid-request, or not-found provider requests. It also does not retry known 4xx client errors, including `401`, `403`, and `404`.

Unknown failures, network-style failures, and 5xx-style failures may retry up to two times after the first failure.

## Query Keys

`queryKeys` is the canonical key factory for this slice. Local/public catalog keys use generated canonical resource identity beneath a connection prefix; aliases, labels, metrics, and descriptors do not participate:

- `queryKeys.catalog.root()`
- `queryKeys.catalog.connection(source)`
- `queryKeys.catalog.table(source)`
- `queryKeys.catalog.tableDerived(source)`
- `queryKeys.catalog.commits(source)`
- `queryKeys.local.root()`
- `queryKeys.local.history()`
- `queryKeys.local.saved()`

Keep keys stable and route new query key families through this module.

## Catalog Server State

`catalogQueryOptions(selection)` passes TanStack Query's exact abort signal and a fresh correlation ID to provider-driven local/public catalog discovery. It seeds an available selection with `snapshotCatalog(source)` while discovery runs. Only the exact built-in sample fixture remains on the isolated manifest compatibility path. Missing, empty, stale, and unqueryable selections use source-free stable keys with `enabled: false` and `skipToken`, so they cannot invoke a catalog loader.

`commitsQueryOptions(selection)` wraps commit-log loading and uses the same disabled behavior for unavailable selection. `AppProviders` installs a ref-counted runtime bridge that writes published runtime catalogs to the matching catalog query and invalidates the matching commits query.

`purgeCatalogSourceCache(queryClient, source)` cancels, removes, and invalidates the entire `queryKeys.catalog.connection(source)` subtree, in that order, then clears matching runtime presentation state. It is used when connected sources are removed or replaced. The catalog and commits query adapters also call `purgeCatalogSourceCacheForError` so auth/session-style failures (`401`, `403`, `419`, `440`) discard only that connection-scoped catalog cache. These helpers do not purge `queryKeys.local.history()` or `queryKeys.local.saved()`.

## Local Metadata Server State

`historyQueryOptions()` and `savedQueriesQueryOptions()` wrap the existing metadata services. The mutation helpers call the same services and then update `QueryClient` with the returned entries so IndexedDB-unavailable fallback still updates current in-memory UI state.

## Boundary

The query layer owns cache identity, retry defaults, source-scoped catalog cache purging, bridge wiring, local metadata adapters, and the safe persisted-cache policy. It does not own provider-specific execution logic, durable auth/session state, live object-store credentials, route definitions, or result/run-state persistence.

`CatalogProvider` remains below this layer and owns generated discovery only. This layer does not own `DataAccessResolver`, `ExecutionProvider`, or provider-specific execution logic.
