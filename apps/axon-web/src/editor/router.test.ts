import { createMemoryHistory } from '@tanstack/react-router';
import { describe, expect, it } from 'vitest';
import {
  catalogTablePath,
  createEditorRouter,
  editorRouteTemplates,
  savedQueryPath,
} from './router.tsx';

describe('editor router', () => {
  it('matches catalog table deep links with decoded route params', async () => {
    const router = createEditorRouter({
      history: createMemoryHistory({
        initialEntries: [
          catalogTablePath({ catalogId: 'cat 1', schemaName: 'sales', tableName: 'orders' }),
        ],
      }),
    });

    await router.load();

    const leaf = router.state.matches.at(-1);
    expect(leaf?.routeId).toBe(editorRouteTemplates.catalogTable);
    expect(leaf?.params).toEqual({
      catalogId: 'cat 1',
      schemaName: 'sales',
      tableName: 'orders',
    });
  });

  it('navigates by href while preserving browser history entries', async () => {
    const history = createMemoryHistory({ initialEntries: ['/'] });
    const router = createEditorRouter({ history });

    await router.load();
    await router.navigate({ href: savedQueryPath('saved 1') });

    expect(router.state.location.pathname).toBe('/saved/saved 1');
    expect(router.state.matches.at(-1)?.routeId).toBe(editorRouteTemplates.savedQuery);
  });
});
