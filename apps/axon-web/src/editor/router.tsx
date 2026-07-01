import {
  RouterProvider,
  createRootRoute,
  createRoute,
  createRouter,
  type RouterHistory,
} from '@tanstack/react-router';
import { Suspense, lazy } from 'react';
import {
  catalogTablePath,
  savedQueryPath,
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
  return <WorkspaceRoute />;
}

function SavedQueryRoute() {
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
