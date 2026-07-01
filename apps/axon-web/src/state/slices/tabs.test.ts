import { describe, expect, it } from 'vitest';
import type { SavedQuery } from '../../services/types.ts';
import { selectActiveSqlTab, selectActiveTab, selectTabActions, selectTabs } from '../hooks.ts';
import {
  CLIENT_STATE_STORAGE_KEY,
  createAxonClientStore,
  createMemoryClientStateStorage,
} from '../store.ts';

function persistedClientState(storage: ReturnType<typeof createMemoryClientStateStorage>) {
  return JSON.parse(storage.getItem(CLIENT_STATE_STORAGE_KEY) ?? '{}') as {
    state?: Record<string, unknown>;
  };
}

function savedQuery(overrides: Partial<SavedQuery> = {}): SavedQuery {
  return {
    id: 'saved-1',
    name: 'revenue by day',
    owner: 'you',
    edited: '10:30',
    target: 'browser_wasm',
    sql: 'SELECT revenue FROM daily_revenue',
    ...overrides,
  };
}

describe('tabs slice', () => {
  it('uses the current editor tab defaults', () => {
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });

    expect(selectTabs(store.getState())).toEqual({
      activeTabId: 'q1',
      items: [
        {
          kind: 'sql',
          id: 'q1',
          title: 'count-rows.sql',
          sql: 'SELECT COUNT(*) AS row_count FROM events',
          dirty: false,
          pin: null,
          preferred: 'browser_wasm',
        },
        {
          kind: 'sql',
          id: 'q2',
          title: 'category-totals.sql',
          sql: 'SELECT category,\n       COUNT(*) AS rows,\n       SUM(value) AS total_value\nFROM events\nGROUP BY category\nORDER BY category',
          dirty: false,
          pin: null,
          preferred: 'browser_wasm',
        },
      ],
    });
    expect(selectActiveTab(store.getState())?.id).toBe('q1');
    expect(selectActiveSqlTab(store.getState())?.kind).toBe('sql');
    expect(selectTabActions(store.getState()).updateActiveSql).toEqual(expect.any(Function));
  });

  it('adds SQL tabs using the provided default target and activates them', () => {
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });

    selectTabActions(store.getState()).addSqlTab('native');

    const { items, activeTabId } = selectTabs(store.getState());
    const added = items[2];

    expect(items).toHaveLength(3);
    expect(added).toMatchObject({
      kind: 'sql',
      title: 'untitled-3.sql',
      sql: 'SELECT ',
      dirty: true,
      pin: null,
      preferred: 'native',
    });
    expect(added?.id).toMatch(/^q3-[a-z0-9]{4}$/);
    expect(activeTabId).toBe(added?.id);
    expect(selectActiveSqlTab(store.getState())).toBe(added);
  });

  it('falls back to the previous tab when closing the active tab and keeps the last tab open', () => {
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });
    const actions = selectTabActions(store.getState());

    actions.selectTab('q2');
    actions.closeTab('q2');

    expect(selectTabs(store.getState()).items.map((tab) => tab.id)).toEqual(['q1']);
    expect(selectTabs(store.getState()).activeTabId).toBe('q1');

    actions.closeTab('q1');

    expect(selectTabs(store.getState()).items.map((tab) => tab.id)).toEqual(['q1']);
    expect(selectTabs(store.getState()).activeTabId).toBe('q1');
  });

  it('updates, inserts into, and formats the active SQL while marking it dirty', () => {
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });
    const actions = selectTabActions(store.getState());

    actions.updateActiveSql('select *');

    expect(selectActiveSqlTab(store.getState())).toMatchObject({
      sql: 'select *',
      dirty: true,
    });

    actions.markActiveClean();
    actions.insertIntoActiveSql('from events');

    expect(selectActiveSqlTab(store.getState())).toMatchObject({
      sql: 'select *\nfrom events',
      dirty: true,
    });

    actions.markActiveClean();
    actions.formatActiveSql((sql) => sql.toUpperCase());

    expect(selectActiveSqlTab(store.getState())).toMatchObject({
      sql: 'SELECT *\nFROM EVENTS',
      dirty: true,
    });
  });

  it('sets the active preferred target and toggles a snapshot pin', () => {
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });
    const actions = selectTabActions(store.getState());

    actions.setActivePreferred('native');
    actions.toggleActivePin(42);

    expect(selectActiveSqlTab(store.getState())).toMatchObject({
      preferred: 'native',
      pin: 42,
    });

    actions.toggleActivePin(99);

    expect(selectActiveSqlTab(store.getState())?.pin).toBeNull();
  });

  it('renames on save and can mark the active tab clean without renaming', () => {
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });
    const actions = selectTabActions(store.getState());

    actions.updateActiveSql('SELECT 1');
    actions.markActiveSaved('orders.sql');

    expect(selectActiveSqlTab(store.getState())).toMatchObject({
      title: 'orders.sql',
      dirty: false,
    });

    actions.updateActiveSql('SELECT 2');
    actions.markActiveClean();

    expect(selectActiveSqlTab(store.getState())).toMatchObject({
      title: 'orders.sql',
      dirty: false,
    });
  });

  it('can mark the original tab clean after the active tab changes', () => {
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });
    const actions = selectTabActions(store.getState());

    actions.updateActiveSql('SELECT 1');
    actions.selectTab('q2');
    actions.updateActiveSql('SELECT 2');

    actions.markActiveClean('q1');
    actions.markActiveSaved('category.sql', 'q2');

    expect(selectTabs(store.getState()).items).toEqual([
      {
        kind: 'sql',
        id: 'q1',
        title: 'count-rows.sql',
        sql: 'SELECT 1',
        dirty: false,
        pin: null,
        preferred: 'browser_wasm',
      },
      {
        kind: 'sql',
        id: 'q2',
        title: 'category.sql',
        sql: 'SELECT 2',
        dirty: false,
        pin: null,
        preferred: 'browser_wasm',
      },
    ]);
  });

  it('opens a saved query in a new clean tab and reuses it by saved id', () => {
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });
    const actions = selectTabActions(store.getState());

    actions.openSavedQuery(savedQuery({ id: 'saved-1', target: 'native' }));

    const opened = selectActiveSqlTab(store.getState());
    expect(opened).toMatchObject({
      title: 'revenue by day.sql',
      sql: 'SELECT revenue FROM daily_revenue',
      dirty: false,
      pin: null,
      preferred: 'native',
      savedQueryId: 'saved-1',
    });
    expect(selectTabs(store.getState()).items).toHaveLength(3);

    actions.updateActiveSql('SELECT dirty');
    actions.openSavedQuery(
      savedQuery({
        id: 'saved-1',
        name: 'revenue by day',
        sql: 'SELECT revenue FROM daily_revenue ORDER BY 1',
      }),
    );

    expect(selectTabs(store.getState()).items).toHaveLength(3);
    expect(selectActiveSqlTab(store.getState())).toMatchObject({
      id: opened?.id,
      title: 'revenue by day.sql',
      sql: 'SELECT revenue FROM daily_revenue ORDER BY 1',
      dirty: false,
      savedQueryId: 'saved-1',
    });
  });

  it('persists tabs without persisting actions or unrelated client state', () => {
    const storage = createMemoryClientStateStorage();
    const first = createAxonClientStore({ storage });
    const actions = selectTabActions(first.getState());

    actions.addSqlTab('auto');
    actions.updateActiveSql('SELECT 42');
    actions.toggleActivePin(5);
    actions.markActiveSaved('answer.sql');

    const persisted = persistedClientState(storage);
    expect(Object.keys(persisted.state ?? {}).sort()).toEqual(['layout', 'settings', 'tabs']);
    expect(persisted.state).not.toHaveProperty('tabsActions');
    expect(persisted.state).not.toHaveProperty('resultData');
    expect(persisted.state).not.toHaveProperty('connections');

    const second = createAxonClientStore({ storage });

    expect(selectTabs(second.getState())).toEqual({
      activeTabId: selectTabs(first.getState()).activeTabId,
      items: [
        ...selectTabs(first.getState()).items.slice(0, 2),
        {
          kind: 'sql',
          id: selectTabs(first.getState()).items[2]?.id,
          title: 'answer.sql',
          sql: 'SELECT 42',
          dirty: false,
          pin: 5,
          preferred: 'auto',
        },
      ],
    });
  });

  it('whitelists tab fields when partializing client-state persistence', () => {
    const storage = createMemoryClientStateStorage();
    const store = createAxonClientStore({ storage });

    store.setState((state) => ({
      ...state,
      tabs: {
        ...state.tabs,
        items: [
          {
            ...state.tabs.items[0],
            savedQueryId: 'saved-safe',
            resultData: { rows: [] },
            signedUrl: 'https://example.invalid/private',
            fileBytes: 'abc',
            localDeltaCatalog: { tables: [] },
            secretAccessKey: 'secret',
          } as (typeof state.tabs.items)[number],
          ...state.tabs.items.slice(1),
        ],
      },
    }));

    const persisted = persistedClientState(storage);
    const firstTab = (persisted.state?.tabs as { items?: unknown[] } | undefined)?.items?.[0];

    expect(firstTab).toEqual({
      kind: 'sql',
      id: 'q1',
      title: 'count-rows.sql',
      sql: 'SELECT COUNT(*) AS row_count FROM events',
      dirty: false,
      pin: null,
      preferred: 'browser_wasm',
      savedQueryId: 'saved-safe',
    });
  });

  it('sanitizes persisted tabs before hydrating client state', () => {
    const storage = createMemoryClientStateStorage({
      [CLIENT_STATE_STORAGE_KEY]: JSON.stringify({
        version: 1,
        state: {
          tabs: {
            activeTabId: 'missing',
            secretToken: 'do-not-hydrate',
            items: [
              {
                kind: 'sql',
                id: 'safe',
                title: 'safe.sql',
                sql: 'SELECT 1',
                dirty: true,
                pin: 7,
                preferred: 'native',
                savedQueryId: 'safe-saved-query',
                resultData: { rows: [] },
                signedUrl: 'https://example.invalid/private',
                fileBytes: 'abc',
                localRegistryId: 'local-registry',
                secretAccessKey: 'secret',
                actions: {},
              },
              {
                kind: 'file',
                id: 'future',
                title: 'future.parquet',
                sql: 'SELECT 2',
                dirty: false,
                pin: null,
                preferred: 'browser_wasm',
                savedQueryId: 123,
              },
              {
                kind: 'sql',
                id: 'invalid-target',
                title: 'invalid.sql',
                sql: 'SELECT 3',
                dirty: false,
                pin: null,
                preferred: 'credential',
              },
            ],
          },
          tabsActions: {},
          resultData: { rows: [] },
          signedUrl: 'https://example.invalid/private',
          fileBytes: 'abc',
          localDeltaCatalog: { tables: [] },
          secretAccessKey: 'secret',
        },
      }),
    });

    const store = createAxonClientStore({ storage });

    expect(selectTabs(store.getState())).toEqual({
      activeTabId: 'safe',
      items: [
        {
          kind: 'sql',
          id: 'safe',
          title: 'safe.sql',
          sql: 'SELECT 1',
          dirty: true,
          pin: 7,
          preferred: 'native',
          savedQueryId: 'safe-saved-query',
        },
      ],
    });
  });
});
