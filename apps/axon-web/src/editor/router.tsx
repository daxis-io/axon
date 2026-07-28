import {
  RouterProvider,
  createRootRoute,
  createRoute,
  createRouter,
  useNavigate,
  useParams,
  type RouterHistory,
} from '@tanstack/react-router';
import { useQuery } from '@tanstack/react-query';
import { Suspense, lazy, useEffect, useMemo } from 'react';
import { savedQueriesQueryOptions } from '../query/local.ts';
import type { ActiveConnectedTableRef } from '../services/query-source.ts';
import {
  selectActiveConnectedTableRef,
  selectAvailableConnectedCatalogs,
  selectConnectionActions,
  selectTabActions,
  useAxonClientStore,
} from '../state/hooks.ts';
import {
  catalogTablePath,
  catalogTableSqlPath,
  resolveCatalogTableRoute,
  resolveLegacyCatalogTableRoute,
  savedQueryPath,
  tableRefForRouteSelection,
  type CatalogTableHref,
  type CatalogTableRouteParams,
  type CatalogTableSqlHref,
  type LegacyCatalogTableHref,
  type SavedQueryHref,
} from './catalog-navigation.ts';

export { catalogTablePath, catalogTableSqlPath, savedQueryPath };

export const editorRouteTemplates = {
  root: '/',
  connect: '/connect',
  catalogs: '/catalogs',
  settings: '/settings',
  catalogTable: '/catalog/$connectionId/table/$providerNamespace/$identityArm/$identityValue',
  catalogTableSql:
    '/catalog/$connectionId/table/$providerNamespace/$identityArm/$identityValue/sql',
  legacyCatalogTable: '/catalog/$catalogId/$schemaName/$tableName',
  savedQuery: '/saved/$savedId',
} as const;

export type EditorRouteHref =
  | '/'
  | '/connect'
  | '/catalogs'
  | '/settings'
  | CatalogTableHref
  | CatalogTableSqlHref
  | LegacyCatalogTableHref
  | SavedQueryHref;

const App = lazy(() => import('./App.tsx').then((module) => ({ default: module.App })));
const ConnectPage = lazy(() =>
  import('./ConnectPage.tsx').then((module) => ({ default: module.ConnectPage })),
);
const CatalogsPage = lazy(() =>
  import('./CatalogsPage.tsx').then((module) => ({ default: module.CatalogsPage })),
);
const SettingsPage = lazy(() =>
  import('./SettingsPage.tsx').then((module) => ({ default: module.SettingsPage })),
);

function WorkspaceRoute({ routeTable }: { routeTable?: ActiveConnectedTableRef }) {
  return (
    <Suspense fallback={null}>
      <App routeTable={routeTable} />
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

function CatalogsRoute({ routeTable }: { routeTable?: ActiveConnectedTableRef }) {
  return (
    <Suspense fallback={<CatalogExplorerLoadingState />}>
      <CatalogsPage routeTable={routeTable} />
    </Suspense>
  );
}

function CatalogExplorerLoadingState() {
  return (
    <main className="route-empty" aria-live="polite" aria-busy="true">
      <span className="route-empty-mark" aria-hidden="true">
        A
      </span>
      <h1>Loading catalog metadata</h1>
      <p>The Catalog Explorer is preparing generated resource metadata.</p>
    </main>
  );
}

function SettingsRoute() {
  return (
    <Suspense fallback={null}>
      <SettingsPage />
    </Suspense>
  );
}

function CanonicalCatalogTableRoute() {
  const params = useParams({ from: editorRouteTemplates.catalogTable });
  const resolution = useCanonicalCatalogRoute(params);
  useMirrorRouteSelection(resolution);

  if (resolution.status !== 'valid') {
    return <CatalogRouteEmptyState reason={resolution.reason} />;
  }
  return <CatalogsRoute routeTable={resolution.ref} />;
}

function CanonicalCatalogTableSqlRoute() {
  const params = useParams({ from: editorRouteTemplates.catalogTableSql });
  const availableCatalogs = useAxonClientStore(selectAvailableConnectedCatalogs);
  const resolution = useMemo(
    () => resolveCatalogTableRoute(availableCatalogs, params, { requireQueryable: true }),
    [availableCatalogs, params],
  );
  useMirrorRouteSelection(resolution);

  if (resolution.status !== 'valid') {
    return <CatalogRouteEmptyState reason={resolution.reason} />;
  }
  return <WorkspaceRoute routeTable={resolution.ref} />;
}

function LegacyCatalogTableRoute() {
  const params = useParams({ from: editorRouteTemplates.legacyCatalogTable });
  const availableCatalogs = useAxonClientStore(selectAvailableConnectedCatalogs);
  const routeNavigate = useNavigate();
  const resolution = useMemo(
    () => resolveLegacyCatalogTableRoute(availableCatalogs, params),
    [availableCatalogs, params],
  );

  useEffect(() => {
    if (resolution.status !== 'valid') return;
    void routeNavigate({ href: resolution.redirect, replace: true });
  }, [resolution, routeNavigate]);

  if (resolution.status === 'valid') return null;
  return (
    <RouteEmptyState
      title={
        resolution.reason === 'ambiguous_legacy_route'
          ? 'Legacy table route is ambiguous'
          : 'Legacy table route unavailable'
      }
      detail="This display-name link does not resolve to exactly one connected logical table."
      actionLabel="View catalogs"
      actionHref="/catalogs"
    />
  );
}

function useCanonicalCatalogRoute(params: CatalogTableRouteParams) {
  const availableCatalogs = useAxonClientStore(selectAvailableConnectedCatalogs);
  return useMemo(
    () => resolveCatalogTableRoute(availableCatalogs, params),
    [availableCatalogs, params],
  );
}

function useMirrorRouteSelection(resolution: ReturnType<typeof resolveCatalogTableRoute>): void {
  const activeTable = useAxonClientStore(selectActiveConnectedTableRef);
  const connectionActions = useAxonClientStore(selectConnectionActions);

  useEffect(() => {
    if (resolution.status !== 'valid') return;
    const nextRef = tableRefForRouteSelection(resolution, activeTable);
    if (nextRef) connectionActions.selectTable(nextRef);
  }, [activeTable, connectionActions, resolution]);
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

  if (!savedQuery && savedQueries.isFetching) return null;
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
  const settingsRoute = createRoute({
    getParentRoute: () => rootRoute,
    path: editorRouteTemplates.settings,
    component: SettingsRoute,
  });
  const catalogTableRoute = createRoute({
    getParentRoute: () => rootRoute,
    path: editorRouteTemplates.catalogTable,
    component: CanonicalCatalogTableRoute,
  });
  const catalogTableSqlRoute = createRoute({
    getParentRoute: () => rootRoute,
    path: editorRouteTemplates.catalogTableSql,
    component: CanonicalCatalogTableSqlRoute,
  });
  const legacyCatalogTableRoute = createRoute({
    getParentRoute: () => rootRoute,
    path: editorRouteTemplates.legacyCatalogTable,
    component: LegacyCatalogTableRoute,
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
    settingsRoute,
    catalogTableRoute,
    catalogTableSqlRoute,
    legacyCatalogTableRoute,
    savedRoute,
  ]);
}

export function createEditorRouter(options: { history?: RouterHistory } = {}) {
  return createRouter({ routeTree: createRouteTree(), history: options.history });
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

function CatalogRouteEmptyState({
  reason,
}: {
  reason: Exclude<ReturnType<typeof resolveCatalogTableRoute>, { status: 'valid' }>['reason'];
}) {
  const detail = {
    malformed_route: 'The canonical table identity in this URL is malformed.',
    disconnected_connection: 'The connection in this URL is no longer connected.',
    stale_resource: 'The exact table resource in this URL is no longer reported.',
    ambiguous_resource: 'The exact table resource resolves to more than one current location.',
    non_queryable:
      'This exact table can be browsed, but it is not queryable in this browser build.',
  }[reason];
  return (
    <RouteEmptyState
      title="Table route unavailable"
      detail={detail}
      actionLabel="View catalogs"
      actionHref="/catalogs"
    />
  );
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
