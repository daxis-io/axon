import { createMemoryHistory } from '@tanstack/react-router';
import { describe, expect, it } from 'vitest';
import { createLocalDeltaCanonicalTable } from '../services/canonical-table-identity.ts';
import {
  catalogTablePath,
  catalogTableSqlPath,
  createEditorRouter,
  editorRouteTemplates,
  savedQueryPath,
} from './router.tsx';

describe('editor router', () => {
  it('matches canonical Explorer deep links with decoded identity params', async () => {
    const table = createLocalDeltaCanonicalTable({
      registryId: 'local registry/id',
      tableName: 'events',
    });
    const router = createEditorRouter({
      history: createMemoryHistory({ initialEntries: [catalogTablePath(table)] }),
    });

    await router.load();

    const leaf = router.state.matches.at(-1);
    expect(leaf?.routeId).toBe(editorRouteTemplates.catalogTable);
    expect(leaf?.params).toEqual({
      connectionId: 'axon-connection://local-delta/local%20registry%2Fid',
      providerNamespace: 'axon.local-delta/v1',
      identityArm: 'provider-object-id',
      identityValue: 'local registry/id',
    });
  });

  it('matches the canonical SQL editor route separately from Explorer', async () => {
    const table = createLocalDeltaCanonicalTable({
      registryId: 'local-registry',
      tableName: 'events',
    });
    const router = createEditorRouter({
      history: createMemoryHistory({ initialEntries: [catalogTableSqlPath(table)] }),
    });

    await router.load();

    expect(router.state.matches.at(-1)?.routeId).toBe(editorRouteTemplates.catalogTableSql);
  });

  it('retains the published three-segment route only as a legacy consumer', async () => {
    const router = createEditorRouter({
      history: createMemoryHistory({
        initialEntries: ['/catalog/catalog-workspace/default/events'],
      }),
    });

    await router.load();

    expect(router.state.matches.at(-1)?.routeId).toBe(editorRouteTemplates.legacyCatalogTable);
  });

  it('navigates by href while preserving browser history entries', async () => {
    const history = createMemoryHistory({ initialEntries: ['/'] });
    const router = createEditorRouter({ history });

    await router.load();
    await router.navigate({ href: savedQueryPath('saved 1') });

    expect(router.state.location.pathname).toBe('/saved/saved 1');
    expect(router.state.matches.at(-1)?.routeId).toBe(editorRouteTemplates.savedQuery);
  });

  it('matches the routed settings surface', async () => {
    const router = createEditorRouter({
      history: createMemoryHistory({ initialEntries: ['/settings'] }),
    });

    await router.load();

    expect(router.state.location.pathname).toBe('/settings');
    expect(router.state.matches.at(-1)?.routeId).toBe(editorRouteTemplates.settings);
  });
});
