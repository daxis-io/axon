# Query Foundation

This directory is the M0 query substrate for `axon-web`. It provides the shared React Query client factory, a stable default client for app runtime use, and typed query key factories.

## Query Client

`createAxonQueryClient()` returns a `QueryClient` with minimal defaults. The only default policy in M0 is `queries.retry: shouldRetryQuery`.

`queryClient` is the module-level default client used by `AppProviders`. Tests can inject their own client through `AppProviders` instead of creating clients during render.

## Retry Policy

`shouldRetryQuery` does not retry aborted or cancelled requests. It also does not retry known 4xx client errors, including `401`, `403`, and `404`.

Unknown failures, network-style failures, and 5xx-style failures may retry up to two times after the first failure.

## Query Keys

`queryKeys` is the canonical key factory for this slice:

- `queryKeys.catalog.root(connectionId)`
  - returns `['catalog', connectionId]`
- `queryKeys.local.history()`
- `queryKeys.local.saved()`

Keep keys stable and route new query key families through this module.

## M0 Boundary

M0 intentionally does not add query functions, fetchers, mutations, catalog loading, invalidation policy, route data loading, persisted caches, or product data behavior.

M0 also does not add `CatalogProvider`, `DataAccessResolver`, `ExecutionProvider`, or any provider-specific execution logic. Those belong to later slices.
