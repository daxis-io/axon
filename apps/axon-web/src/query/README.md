# Query Foundation

This directory is the M0 query substrate for `axon-web`. It provides the shared React Query client factory, a stable default client for app runtime use, and typed query key factories.

## Query Client

`createAxonQueryClient()` returns a `QueryClient` with minimal defaults. The only default policy in M0 is `queries.retry: shouldRetryQuery`.

`queryClient` is the module-level default client used by `AppProviders`. Tests can inject their own client through `AppProviders` instead of creating clients during render.

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

## M0 Boundary

M0 intentionally did not add query functions, fetchers, mutations, catalog loading, invalidation policy, route data loading, persisted caches, or product data behavior. M3 starts moving existing editor server state onto TanStack Query without adding persistent query caches.

M0 also does not add `CatalogProvider`, `DataAccessResolver`, `ExecutionProvider`, or any provider-specific execution logic. Those belong to later slices.
