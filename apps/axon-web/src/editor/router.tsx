import {
  RouterProvider,
  createRootRoute,
  createRoute,
  createRouter,
  useParams,
  type RouterHistory,
} from '@tanstack/react-router';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { Suspense, lazy, useEffect, useMemo } from 'react';
import { catalogQueryOptions, commitsQueryOptions } from '../query/catalog.ts';
import { savedQueriesQueryOptions } from '../query/local.ts';
import {
  selectActiveConnectedTableRef,
  selectAvailableConnectedCatalogs,
  selectConnectionActions,
  selectTabActions,
  useAxonClientStore,
} from '../state/hooks.ts';
import {
  catalogTablePath,
  resolveCatalogTableRoute,
  savedQueryPath,
  tableRefForRouteSelection,
  type CatalogTableHref,
  type CatalogTableRouteParams,
  type SavedQueryHref,
} from './catalog-navigation.ts';

export { catalogTablePath, savedQueryPath };

export const editorRouteTemplates = {
  root: '/',
  connect: '/connect',
  catalogs: '/catalogs',
  catalogTable: '/catalog/$catalogId/$schemaName/$tableName',
  savedQuery: '/saved/$savedId',
} as const;

export type EditorRouteHref = '/' | '/connect' | '/catalogs' | CatalogTableHref | SavedQueryHref;

const App = lazy(() => import('./App.tsx').then((module) => ({ default: module.App })));
const ConnectPage = lazy(() =>
  import('./ConnectPage.tsx').then((module) => ({ default: module.ConnectPage })),
);
const CatalogsPage = lazy(() =>
  import('./CatalogsPage.tsx').then((module) => ({ default: module.CatalogsPage })),
);

function WorkspaceRoute() {
  return (
    <Suspense fallback={null}>
      <App />
    </Suspense>
  );
}

function ConnectRoute() {
  return (
    <Suspense fallback={null}>
      <ConnectPage />
    </Suspense>
  );
}

function CatalogsRoute() {
  return (
    <Suspense fallback={null}>
      <CatalogsPage />
    </Suspense>
  );
}

function CatalogTableRoute() {
  const params = useParams({ from: editorRouteTemplates.catalogTable });
  const availableCatalogs = useAxonClientStore(selectAvailableConnectedCatalogs);
  const activeTable = useAxonClientStore(selectActiveConnectedTableRef);
  const connectionActions = useAxonClientStore(selectConnectionActions);
  const queryClient = useQueryClient();
  const resolution = useMemo(
    () => resolveCatalogTableRoute(availableCatalogs, params),
    [availableCatalogs, params],
  );

  useEffect(() => {
    if (resolution.status !== 'valid') return;

    const nextRef = tableRefForRouteSelection(resolution, activeTable);
    if (nextRef) {
      connectionActions.selectTable(nextRef);
    }

    void queryClient.ensureQueryData(catalogQueryOptions(resolution.source));
    void queryClient.ensureQueryData(commitsQueryOptions(resolution.source));
  }, [activeTable, connectionActions, queryClient, resolution]);

  if (resolution.status !== 'valid') {
    return (
      <RouteEmptyState
        title="Table route not found"
        detail="The catalog, schema, or table in this URL is not connected."
        actionLabel="View catalogs"
        actionHref="/catalogs"
      />
    );
  }

  return <WorkspaceRoute />;
}

function SavedQueryRoute() {
  const params = useParams({ from: editorRouteTemplates.savedQuery });
  const tabActions = useAxonClientStore(selectTabActions);
  const savedQueries = useQuery(savedQueriesQueryOptions());
  const savedQuery = useMemo(
    () => savedQueries.data.find((query) => query.id === params.savedId),
    [params.savedId, savedQueries.data],
  );

  useEffect(() => {
    if (!savedQuery) return;
    tabActions.openSavedQuery(savedQuery);
  }, [savedQuery, tabActions]);

  if (!savedQuery && savedQueries.isFetching) {
    return null;
  }

  if (!savedQuery) {
    return (
      <RouteEmptyState
        title="Saved query not found"
        detail="The saved query in this URL is not available in local metadata."
        actionLabel="Back to workspace"
        actionHref="/"
      />
    );
  }

  return <WorkspaceRoute />;
}

function createRouteTree() {
  const rootRoute = createRootRoute();
  const indexRoute = createRoute({
    getParentRoute: () => rootRoute,
    path: editorRouteTemplates.root,
    component: WorkspaceRoute,
  });
  const connectRoute = createRoute({
    getParentRoute: () => rootRoute,
    path: editorRouteTemplates.connect,
    component: ConnectRoute,
  });
  const catalogsRoute = createRoute({
    getParentRoute: () => rootRoute,
    path: editorRouteTemplates.catalogs,
    component: CatalogsRoute,
  });
  const catalogTableRoute = createRoute({
    getParentRoute: () => rootRoute,
    path: editorRouteTemplates.catalogTable,
    component: CatalogTableRoute,
  });
  const savedRoute = createRoute({
    getParentRoute: () => rootRoute,
    path: editorRouteTemplates.savedQuery,
    component: SavedQueryRoute,
  });

  return rootRoute.addChildren([
    indexRoute,
    connectRoute,
    catalogsRoute,
    catalogTableRoute,
    savedRoute,
  ]);
}

export function createEditorRouter(options: { history?: RouterHistory } = {}) {
  return createRouter({
    routeTree: createRouteTree(),
    history: options.history,
  });
}

export const router = createEditorRouter();

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router;
  }
}

export function AppRouter() {
  return <RouterProvider router={router} />;
}

export function navigate(next: EditorRouteHref): void {
  void router.navigate({ href: next });
}

export function catalogTableParamsPath(params: CatalogTableRouteParams): string {
  return catalogTablePath(params);
}

function RouteEmptyState({
  title,
  detail,
  actionLabel,
  actionHref,
}: {
  title: string;
  detail: string;
  actionLabel: string;
  actionHref: EditorRouteHref;
}) {
  return (
    <div className="route-empty">
      <div className="route-empty-mark">A</div>
      <h1>{title}</h1>
      <p>{detail}</p>
      <button className="cc-btn primary" onClick={() => navigate(actionHref)}>
        {actionLabel}
      </button>
    </div>
  );
}
