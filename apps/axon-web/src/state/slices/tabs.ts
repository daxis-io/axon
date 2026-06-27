import type { DefaultTargetSetting } from './settings.ts';
import { isDefaultTargetSetting } from './settings.ts';

export type SqlTab = {
  kind: 'sql';
  id: string;
  title: string;
  sql: string;
  dirty: boolean;
  pin: number | null;
  preferred: DefaultTargetSetting;
};

export type EditorTab = SqlTab;

export type TabsState = {
  items: EditorTab[];
  activeTabId: string;
};

export type TabsActions = {
  addSqlTab(defaultTarget: DefaultTargetSetting): void;
  selectTab(id: string): void;
  closeTab(id: string): void;
  updateActiveSql(sql: string): void;
  insertIntoActiveSql(text: string): void;
  formatActiveSql(formatter: (sql: string) => string): void;
  setActivePreferred(target: DefaultTargetSetting): void;
  toggleActivePin(snapshot: number | null | undefined): void;
  markActiveSaved(title: string, id?: string): void;
  markActiveClean(id?: string): void;
};

export type TabsSlice = {
  tabs: TabsState;
  tabsActions: TabsActions;
};

type StoreSet<TState> = (
  partial: Partial<TState> | ((state: TState) => Partial<TState>),
  replace?: false,
) => void;

type StoreGet<TState> = () => TState;

const SAMPLE_QUERIES = {
  count: 'SELECT COUNT(*) AS row_count FROM events',
  category:
    'SELECT category,\n       COUNT(*) AS rows,\n       SUM(value) AS total_value\nFROM events\nGROUP BY category\nORDER BY category',
} as const;

export const DEFAULT_TABS_STATE: TabsState = {
  activeTabId: 'q1',
  items: [
    {
      kind: 'sql',
      id: 'q1',
      title: 'count-rows.sql',
      sql: SAMPLE_QUERIES.count,
      dirty: false,
      pin: null,
      preferred: 'browser_wasm',
    },
    {
      kind: 'sql',
      id: 'q2',
      title: 'category-totals.sql',
      sql: SAMPLE_QUERIES.category,
      dirty: false,
      pin: null,
      preferred: 'browser_wasm',
    },
  ],
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object';
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

function cloneTab(tab: EditorTab): EditorTab {
  return {
    kind: 'sql',
    id: tab.id,
    title: tab.title,
    sql: tab.sql,
    dirty: tab.dirty,
    pin: tab.pin,
    preferred: tab.preferred,
  };
}

export function cloneTabsState(state: TabsState): TabsState {
  return {
    activeTabId: state.activeTabId,
    items: state.items.map(cloneTab),
  };
}

function updateActiveSqlTab(
  tabs: TabsState,
  update: (tab: SqlTab) => SqlTab,
  id?: string,
): TabsState {
  const targetId = id ?? tabs.activeTabId;
  const index = tabs.items.findIndex((tab) => tab.id === targetId);
  if (index < 0) {
    return tabs;
  }

  return {
    ...tabs,
    items: tabs.items.map((tab, itemIndex) =>
      itemIndex === index && tab.kind === 'sql' ? update(tab) : tab,
    ),
  };
}

function pinForSnapshot(snapshot: number | null | undefined): number | null {
  return typeof snapshot === 'number' && Number.isFinite(snapshot) ? snapshot : null;
}

function createTabId(count: number): string {
  return `q${count + 1}-${Math.random().toString(36).slice(2, 6).padEnd(4, '0')}`;
}

function sanitizeTab(input: unknown, seenIds: Set<string>): EditorTab | undefined {
  if (!isRecord(input)) {
    return undefined;
  }

  if (
    input.kind !== 'sql' ||
    !isNonEmptyString(input.id) ||
    seenIds.has(input.id) ||
    !isNonEmptyString(input.title) ||
    typeof input.sql !== 'string' ||
    typeof input.dirty !== 'boolean' ||
    !isDefaultTargetSetting(input.preferred)
  ) {
    return undefined;
  }

  if (input.pin !== null && (typeof input.pin !== 'number' || !Number.isFinite(input.pin))) {
    return undefined;
  }

  seenIds.add(input.id);
  return {
    kind: 'sql',
    id: input.id,
    title: input.title,
    sql: input.sql,
    dirty: input.dirty,
    pin: input.pin,
    preferred: input.preferred,
  };
}

export function sanitizeTabsState(
  input: unknown,
  fallback: TabsState = DEFAULT_TABS_STATE,
): TabsState {
  if (!isRecord(input) || !Array.isArray(input.items)) {
    return cloneTabsState(fallback);
  }

  const seenIds = new Set<string>();
  const items = input.items.flatMap((item) => {
    const sanitized = sanitizeTab(item, seenIds);
    return sanitized ? [sanitized] : [];
  });

  if (items.length === 0) {
    return cloneTabsState(fallback);
  }

  const activeTabId =
    typeof input.activeTabId === 'string' && items.some((tab) => tab.id === input.activeTabId)
      ? input.activeTabId
      : items[0].id;

  return { items, activeTabId };
}

export function createTabsSlice<TState extends TabsSlice>(
  set: StoreSet<TState>,
  get: StoreGet<TState>,
): TabsSlice {
  return {
    tabs: cloneTabsState(DEFAULT_TABS_STATE),
    tabsActions: {
      addSqlTab(defaultTarget) {
        set((state) => {
          const tabs = get().tabs;
          const id = createTabId(tabs.items.length);
          return {
            ...state,
            tabs: {
              activeTabId: id,
              items: [
                ...tabs.items,
                {
                  kind: 'sql',
                  id,
                  title: `untitled-${tabs.items.length + 1}.sql`,
                  sql: 'SELECT ',
                  dirty: true,
                  pin: null,
                  preferred: isDefaultTargetSetting(defaultTarget) ? defaultTarget : 'browser_wasm',
                },
              ],
            },
          };
        });
      },
      selectTab(id) {
        set((state) => {
          if (!state.tabs.items.some((tab) => tab.id === id)) {
            return state;
          }
          return {
            ...state,
            tabs: {
              ...state.tabs,
              activeTabId: id,
            },
          };
        });
      },
      closeTab(id) {
        set((state) => {
          const index = state.tabs.items.findIndex((tab) => tab.id === id);
          if (index < 0 || state.tabs.items.length <= 1) {
            return state;
          }

          const items = state.tabs.items.filter((tab) => tab.id !== id);
          const activeTabId =
            state.tabs.activeTabId === id
              ? (items[Math.max(0, index - 1)]?.id ?? items[0].id)
              : state.tabs.activeTabId;

          return {
            ...state,
            tabs: {
              items,
              activeTabId,
            },
          };
        });
      },
      updateActiveSql(sql) {
        set((state) => ({
          ...state,
          tabs: updateActiveSqlTab(state.tabs, (tab) => ({ ...tab, sql, dirty: true })),
        }));
      },
      insertIntoActiveSql(text) {
        set((state) => ({
          ...state,
          tabs: updateActiveSqlTab(state.tabs, (tab) => ({
            ...tab,
            sql: tab.sql + (tab.sql.endsWith('\n') ? '' : '\n') + text,
            dirty: true,
          })),
        }));
      },
      formatActiveSql(formatter) {
        set((state) => ({
          ...state,
          tabs: updateActiveSqlTab(state.tabs, (tab) => ({
            ...tab,
            sql: formatter(tab.sql),
            dirty: true,
          })),
        }));
      },
      setActivePreferred(target) {
        if (!isDefaultTargetSetting(target)) {
          return;
        }
        set((state) => ({
          ...state,
          tabs: updateActiveSqlTab(state.tabs, (tab) => ({ ...tab, preferred: target })),
        }));
      },
      toggleActivePin(snapshot) {
        set((state) => ({
          ...state,
          tabs: updateActiveSqlTab(state.tabs, (tab) => ({
            ...tab,
            pin: tab.pin == null ? pinForSnapshot(snapshot) : null,
          })),
        }));
      },
      markActiveSaved(title, id) {
        set((state) => ({
          ...state,
          tabs: updateActiveSqlTab(
            state.tabs,
            (tab) => ({
              ...tab,
              title,
              dirty: false,
            }),
            id,
          ),
        }));
      },
      markActiveClean(id) {
        set((state) => ({
          ...state,
          tabs: updateActiveSqlTab(state.tabs, (tab) => ({ ...tab, dirty: false }), id),
        }));
      },
    },
  };
}
