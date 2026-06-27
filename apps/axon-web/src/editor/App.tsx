import {
  useCallback,
  useEffect,
  lazy,
  useMemo,
  useRef,
  useState,
  Suspense,
  type CSSProperties,
  type MouseEvent,
} from 'react';
import { loadCatalog, subscribeCatalog, snapshotCatalog } from '../services/catalog.ts';
import {
  CAPABILITY_ORDER,
  defaultCapabilityMatrix,
  overlayCapabilityReport,
} from '../services/capabilities.ts';
import { subscribeEngineStatus } from '../services/engine.ts';
import { CONNECTOR_FEATURES } from '../services/connector-features.ts';
import { appendHistory, loadHistory } from '../services/history.ts';
import { hasLocalDeltaRuntime } from '../services/local-delta-session.ts';
import {
  appendResultPage,
  queryResultPageRun,
  queryResultPageRunRequest,
  sameQueryResultPageRun,
  type QueryResultPageRun,
} from '../services/query-pagination.ts';
import {
  firstQueryableTableRef,
  querySourceFromConnectedCatalogs,
  resolveActiveTableRef,
  type ActiveConnectedTableRef,
} from '../services/query-source.ts';
import { loadSaved, saveQuery } from '../services/saved.ts';
import { SERVER_QUERY_FALLBACK_ENABLED } from '../services/server-fallback.ts';
import { subscribeCommits } from '../services/snapshot.ts';
import type {
  CapabilityMatrixRow,
  Catalog,
  CommitEntry,
  EngineStatus,
  HistoryEntry,
  PlanSummary,
  QueryEvent,
  QueryExecRequest,
  QueryMetricsSummary,
  QueryResultData,
  SavedQuery,
} from '../services/types.ts';
import {
  selectAppearanceSettings,
  selectDefaultTarget,
  selectLayout,
  selectLayoutActions,
  selectSettingsActions,
  useAxonClientStore,
} from '../state/hooks.ts';
import {
  ACCENT_VALUES,
  APPEARANCE_DENSITY_VALUES,
  APPEARANCE_THEME_VALUES,
  MONO_FONT_VALUES,
  UI_FONT_VALUES,
  availableExecutionTargetValues,
  coerceDefaultTargetForAvailability,
} from '../state/slices/settings.ts';
import { CapabilityPopover } from './components/Capabilities.tsx';
import { Editor } from './components/Editor.tsx';
import { Results, type RunUiState } from './components/Results.tsx';
import { SaveDialog } from './components/SaveDialog.tsx';
import { Sidebar } from './components/Sidebar.tsx';
import {
  IconChevDownTiny,
  IconDatabase,
  IconFormat,
  IconHistory,
  IconPlay,
  IconPlus,
  IconRefresh,
  IconSave,
  IconStop,
  IconTable,
} from './components/icons.tsx';
import { ConnectedCatalogsPanel } from './connect/ConnectedCatalogs.tsx';
import type { SourceId } from './connect/data.ts';
import {
  buildCatalogFromResult,
  catalogsAvailableForFeatures,
  loadConnectedCatalogs,
  localRegistryIdsForCatalogs,
  saveConnectedCatalogs,
  upsertConnectedCatalog,
} from './connect/store.ts';
import type { ConnectedCatalog, ConnectResult } from './connect/types.ts';
import { formatBytes, formatRows, hexToSoft, prettifySql } from './lib/format.ts';
import {
  TweakColor,
  TweakRadio,
  TweakSection,
  TweakSelect,
  TweaksPanel,
} from './tweaks/TweaksPanel.tsx';

const ConnectModal = lazy(() =>
  import('./connect/ConnectModal.tsx').then((module) => ({ default: module.ConnectModal })),
);

type Tab = {
  id: string;
  title: string;
  sql: string;
  dirty: boolean;
  pin: number | null;
  preferred: 'auto' | 'browser_wasm' | 'native';
};

const SAMPLE_QUERIES = {
  count: 'SELECT COUNT(*) AS row_count FROM events',
  category:
    'SELECT category,\n       COUNT(*) AS rows,\n       SUM(value) AS total_value\nFROM events\nGROUP BY category\nORDER BY category',
  top: 'SELECT id, category, value\nFROM events\nWHERE value >= 90\nORDER BY value DESC\nLIMIT 20',
} as const;

const TARGET_OPTIONS = SERVER_QUERY_FALLBACK_ENABLED
  ? [
      { id: 'auto' as const, short: 'Auto', cls: 'auto' },
      { id: 'browser_wasm' as const, short: 'Browser', cls: 'browser' },
      { id: 'native' as const, short: 'Native', cls: 'native' },
    ]
  : [{ id: 'browser_wasm' as const, short: 'Browser', cls: 'browser' }];

function targetTitle(id: Tab['preferred']): string {
  if (!SERVER_QUERY_FALLBACK_ENABLED) {
    return 'Run in Browser (WASM)';
  }
  if (id === 'auto') {
    return 'Router picks browser_wasm when possible, native otherwise';
  }
  if (id === 'browser_wasm') {
    return 'Prefer browser (WASM) — falls back to native when needed';
  }
  return 'Run on native DataFusion runtime';
}

function connectedTableForRef(
  catalogs: ConnectedCatalog[],
  ref?: ActiveConnectedTableRef,
): ConnectedCatalog['schemas'][number]['tables'][number] | undefined {
  if (!ref) return undefined;
  const catalog = catalogs.find((candidate) => candidate.id === ref.catalogId);
  const schema = catalog?.schemas.find((candidate) => candidate.name === ref.schemaName);
  return schema?.tables.find((candidate) => candidate.name === ref.tableName);
}

export function App() {
  const { sidebarW, resultsH } = useAxonClientStore(selectLayout);
  const layoutActions = useAxonClientStore(selectLayoutActions);
  const appearance = useAxonClientStore(selectAppearanceSettings);
  const configuredDefaultTarget = useAxonClientStore(selectDefaultTarget);
  const defaultTarget = coerceDefaultTargetForAvailability(
    configuredDefaultTarget,
    SERVER_QUERY_FALLBACK_ENABLED,
  );
  const defaultTargetOptions = availableExecutionTargetValues(SERVER_QUERY_FALLBACK_ENABLED);
  const settingsActions = useAxonClientStore(selectSettingsActions);

  const [catalog, setCatalog] = useState<Catalog | undefined>(() => snapshotCatalog());
  const [history, setHistory] = useState<HistoryEntry[]>([]);
  const [saved, setSaved] = useState<SavedQuery[]>([]);
  const [saveOpen, setSaveOpen] = useState(false);

  const [tabs, setTabs] = useState<Tab[]>([
    {
      id: 'q1',
      title: 'count-rows.sql',
      sql: SAMPLE_QUERIES.count,
      dirty: false,
      pin: null,
      preferred: 'browser_wasm',
    },
    {
      id: 'q2',
      title: 'category-totals.sql',
      sql: SAMPLE_QUERIES.category,
      dirty: false,
      pin: null,
      preferred: 'browser_wasm',
    },
  ]);
  const [activeTab, setActiveTab] = useState('q1');
  const active = tabs.find((x) => x.id === activeTab) ?? tabs[0];

  const tableMeta = catalog?.tables[0];

  const [runState, setRunState] = useState<RunUiState>({ status: 'idle' });
  const [resultData, setResultData] = useState<QueryResultData | undefined>();
  const [resultPageRun, setResultPageRun] = useState<QueryResultPageRun | undefined>();
  const [loadingMoreRows, setLoadingMoreRows] = useState(false);
  const [metrics, setMetrics] = useState<QueryMetricsSummary | undefined>();
  const [events, setEvents] = useState<QueryEvent[]>([]);
  const [capMatrix, setCapMatrix] = useState<CapabilityMatrixRow[]>(() =>
    defaultCapabilityMatrix(),
  );
  const [plan, setPlan] = useState<PlanSummary | undefined>(undefined);
  const [commits, setCommits] = useState<CommitEntry[]>([]);
  const [engineStatus, setEngineStatus] = useState<EngineStatus | undefined>(undefined);
  const [capsOpen, setCapsOpen] = useState(false);
  const [toast, setToast] = useState<{ msg: string; kind: 'ok' | 'warn' } | null>(null);

  const cancelRef = useRef<AbortController | null>(null);
  const runTimer = useRef<number | null>(null);

  // ─── Connect-catalog workflow state ────────────────────
  const [connectModalOpen, setConnectModalOpen] = useState(false);
  const [connectInitialStep, setConnectInitialStep] = useState<1 | 2 | 3>(1);
  const [connectInitialSource, setConnectInitialSource] = useState<SourceId | null>(null);
  const [connectedPanelOpen, setConnectedPanelOpen] = useState(false);
  const [connectedCatalogs, setConnectedCatalogs] = useState<ConnectedCatalog[]>(() =>
    loadConnectedCatalogs(),
  );
  const [selectedTableRef, setSelectedTableRef] = useState<ActiveConnectedTableRef | undefined>();
  const [freshCatalogId, setFreshCatalogId] = useState<string | null>(null);
  const availableConnectedCatalogs = useMemo(
    () => catalogsAvailableForFeatures(connectedCatalogs, CONNECTOR_FEATURES),
    [connectedCatalogs],
  );
  const activeTableRef = useMemo(
    () => resolveActiveTableRef(availableConnectedCatalogs, selectedTableRef),
    [availableConnectedCatalogs, selectedTableRef],
  );
  const querySource = useMemo(
    () => querySourceFromConnectedCatalogs(availableConnectedCatalogs, activeTableRef),
    [activeTableRef, availableConnectedCatalogs],
  );
  const activeConnectedTable = useMemo(
    () => connectedTableForRef(availableConnectedCatalogs, activeTableRef),
    [activeTableRef, availableConnectedCatalogs],
  );
  const localAccessNeedsReselect =
    querySource.kind === 'local_delta' &&
    activeConnectedTable?.localPersistence === 'metadata_only_reselect' &&
    !hasLocalDeltaRuntime(querySource.localRegistryId);

  const activeResultPageRun = useMemo(() => {
    const tab = tabs.find((candidate) => candidate.id === activeTab);
    if (!tab) return undefined;
    return queryResultPageRun(
      {
        sql: tab.sql,
        table_name: tableMeta?.name ?? querySource.tableName,
        preferred_target: tab.preferred,
        snapshot_version: tab.pin ?? undefined,
      },
      querySource,
    );
  }, [activeTab, querySource, tableMeta?.name, tabs]);
  const canLoadMoreRows =
    resultData?.page?.has_more === true &&
    resultPageRun !== undefined &&
    activeResultPageRun !== undefined &&
    sameQueryResultPageRun(resultPageRun, activeResultPageRun);
  const resultPageRunRef = useRef<QueryResultPageRun | undefined>(undefined);
  const activeResultPageRunRef = useRef<QueryResultPageRun | undefined>(undefined);

  useEffect(() => {
    resultPageRunRef.current = resultPageRun;
  }, [resultPageRun]);

  useEffect(() => {
    activeResultPageRunRef.current = activeResultPageRun;
  }, [activeResultPageRun]);

  useEffect(() => {
    saveConnectedCatalogs(connectedCatalogs);
  }, [connectedCatalogs]);

  const openConnectModal = useCallback((step: 1 | 2 | 3 = 1, src: SourceId | null = null) => {
    setConnectInitialStep(step);
    setConnectInitialSource(src);
    setConnectModalOpen(true);
  }, []);
  const reselectLocalFolder = useCallback(() => {
    setConnectedPanelOpen(false);
    openConnectModal(2, 'local');
  }, [openConnectModal]);

  const handleConnected = useCallback(
    (result: ConnectResult) => {
      const catalog = buildCatalogFromResult(result);
      const upsert = upsertConnectedCatalog(connectedCatalogs, catalog);
      const mergedCatalogId =
        upsert.catalogs.find(
          (candidate) =>
            candidate.alias.trim().toLowerCase() === catalog.alias.trim().toLowerCase(),
        )?.id ?? catalog.id;
      setConnectedCatalogs(upsert.catalogs);
      const firstTable = firstQueryableTableRef([catalog]);
      if (firstTable) setSelectedTableRef({ ...firstTable, catalogId: mergedCatalogId });
      const replacedRegistryIds = localRegistryIdsForCatalogs(upsert.replaced);
      if (replacedRegistryIds.length > 0) {
        if (upsert.replaced.some((replaced) => replaced.id === selectedTableRef?.catalogId)) {
          discardActiveQuerySession();
        }
        unregisterLocalDeltaRuntimeIds(
          replacedRegistryIds,
          'failed to unregister duplicate local Delta catalog:',
        );
      }
      setFreshCatalogId(mergedCatalogId);
      setConnectModalOpen(false);
      window.setTimeout(() => setFreshCatalogId(null), 4500);
      setToast({
        msg: `${upsert.replaced.length > 0 ? 'Updated' : 'Connected'} · ${
          catalog.alias
        } · ${catalog.schemas.reduce((a, s) => a + s.tables.length, 0)} tables`,
        kind: 'ok',
      });
      window.setTimeout(() => setToast(null), 2400);
    },
    [connectedCatalogs, selectedTableRef?.catalogId],
  );

  const removeConnectedCatalog = useCallback(
    (id: string) => {
      setConnectedCatalogs((cs) => {
        const removed = cs.find((catalog) => catalog.id === id);
        const removedLocalRegistryIds = removed ? localRegistryIdsForCatalogs([removed]) : [];
        const next = cs.filter((c) => c.id !== id);
        const nextAvailable = catalogsAvailableForFeatures(next, CONNECTOR_FEATURES);
        setSelectedTableRef((current) =>
          current?.catalogId === id
            ? firstQueryableTableRef(nextAvailable)
            : resolveActiveTableRef(nextAvailable, current),
        );
        if (removedLocalRegistryIds.length > 0) {
          if (selectedTableRef?.catalogId === id) discardActiveQuerySession();
          unregisterLocalDeltaRuntimeIds(
            removedLocalRegistryIds,
            'failed to unregister local Delta catalog:',
          );
        }
        return next;
      });
    },
    [selectedTableRef],
  );

  // ─── Apply theme + tokens ──────────────────────────────
  useEffect(() => {
    const root = document.documentElement;
    root.setAttribute('data-theme', appearance.theme === 'dark' ? 'dark' : 'light');
    root.setAttribute('data-density', appearance.density);
    root.style.setProperty('--accent', appearance.accent);
    root.style.setProperty(
      '--accent-soft',
      hexToSoft(appearance.accent, appearance.theme === 'dark'),
    );
    root.style.setProperty('--ui', `"${appearance.uiFont}", ui-sans-serif, system-ui, sans-serif`);
    root.style.setProperty('--mono', `"${appearance.monoFont}", ui-monospace, Menlo, monospace`);
  }, [appearance]);

  // ─── Subscribe to catalog + kick off session bootstrap ──
  useEffect(() => {
    let active = true;
    const unsubCatalog = subscribeCatalog(setCatalog, querySource);
    const unsubCommits = subscribeCommits(setCommits, querySource);
    const unsubEngine = subscribeEngineStatus(setEngineStatus);
    loadCatalog(querySource)
      .then((loaded) => {
        if (active) setCatalog(loaded);
      })
      .catch((err) => {
        if (!active) return;
        console.error('failed to load catalog:', err);
      });
    return () => {
      active = false;
      unsubCatalog();
      unsubCommits();
      unsubEngine();
    };
  }, [querySource]);

  useEffect(() => {
    let active = true;
    loadHistory()
      .then((entries) => {
        if (active) setHistory(entries);
      })
      .catch((err) => console.warn('failed to load query history:', err));
    loadSaved()
      .then((entries) => {
        if (active) setSaved(entries);
      })
      .catch((err) => console.warn('failed to load saved queries:', err));
    return () => {
      active = false;
    };
  }, []);

  const engine = engineStatus;

  // ─── Toast helper ──────────────────────────────────────
  const showToast = useCallback((msg: string, kind: 'ok' | 'warn' = 'ok') => {
    setToast({ msg, kind });
    window.setTimeout(() => setToast(null), 2400);
  }, []);

  // ─── Tab editing ───────────────────────────────────────
  const updateActiveSql = useCallback(
    (sql: string) => {
      setTabs((tt) => tt.map((x) => (x.id === activeTab ? { ...x, sql, dirty: true } : x)));
    },
    [activeTab],
  );

  const insertAtCursor = useCallback(
    (text: string) => {
      setTabs((tt) =>
        tt.map((x) =>
          x.id === activeTab
            ? { ...x, sql: x.sql + (x.sql.endsWith('\n') ? '' : '\n') + text, dirty: true }
            : x,
        ),
      );
    },
    [activeTab],
  );

  const setActivePreferred = useCallback(
    (target: Tab['preferred']) => {
      setTabs((tt) => tt.map((x) => (x.id === activeTab ? { ...x, preferred: target } : x)));
    },
    [activeTab],
  );

  const togglePin = useCallback(() => {
    setTabs((tt) =>
      tt.map((x) => {
        if (x.id !== activeTab) return x;
        const pinned = x.pin != null;
        return { ...x, pin: pinned ? null : (tableMeta?.snapshot ?? null) };
      }),
    );
  }, [activeTab, tableMeta?.snapshot]);

  // ─── Run lifecycle ─────────────────────────────────────
  const runActive = useCallback(async () => {
    if (runState.status === 'running') return;
    const tab = tabs.find((x) => x.id === activeTab);
    if (!tab) return;
    if (localAccessNeedsReselect) {
      setResultData(undefined);
      setResultPageRun(undefined);
      setMetrics(undefined);
      setEvents([]);
      setPlan(undefined);
      setRunState({ status: 'idle' });
      setLoadingMoreRows(false);
      reselectLocalFolder();
      showToast('Reselect this local Delta folder to restore browser file access.', 'warn');
      return;
    }

    cancelRef.current?.abort();
    const ctrl = new AbortController();
    cancelRef.current = ctrl;

    const target = tab.preferred === 'native' ? 'native' : 'browser_wasm';

    setLoadingMoreRows(false);
    setResultPageRun(undefined);
    setEvents([]);
    setPlan(undefined);
    setRunState({ status: 'running', target, elapsed: 0 });
    if (runTimer.current != null) window.clearInterval(runTimer.current);
    const startedAt = performance.now();
    runTimer.current = window.setInterval(() => {
      setRunState((prev) =>
        prev.status === 'running' ? { ...prev, elapsed: performance.now() - startedAt } : prev,
      );
    }, 80);

    const req: QueryExecRequest = {
      sql: tab.sql,
      table_name: tableMeta?.name ?? querySource.tableName,
      preferred_target: tab.preferred,
      snapshot_version: tab.pin ?? undefined,
    };

    const { runQuery } = await import('../services/query.ts');
    const outcome = await runQuery(
      req,
      (event) => {
        if (!SERVER_QUERY_FALLBACK_ENABLED && event.kind === 'fallback') {
          return;
        }
        setEvents((prev) => [...prev, event]);
        if (event.kind === 'metrics') setMetrics(event.metrics);
      },
      ctrl.signal,
      querySource,
    );

    if (runTimer.current != null) window.clearInterval(runTimer.current);

    if (outcome.status === 'done') {
      setResultData(outcome.result);
      setResultPageRun(queryResultPageRun(req, querySource));
      setMetrics(outcome.metrics);
      if (outcome.explain) {
        setPlan({ tree: outcome.explain });
      } else {
        setPlan(undefined);
      }
      setCapMatrix(
        overlayCapabilityReport(defaultCapabilityMatrix(), outcome.capabilities.capabilities ?? {}),
      );
      const fb = SERVER_QUERY_FALLBACK_ENABLED ? outcome.fallback_reason : undefined;
      const fallbackPretty =
        typeof fb === 'string'
          ? { code: fb, detail: 'rerouted to native runtime' }
          : fb && 'capability_gate' in fb
            ? {
                code: 'capability_gate',
                detail: `${fb.capability_gate.capability} required (${fb.capability_gate.required_state})`,
              }
            : null;
      setRunState({
        status: 'done',
        target: outcome.executed_on,
        ms: outcome.elapsed_ms,
        rows: outcome.result.row_count,
        fallback: fallbackPretty,
      });
      const entry = await appendHistory({
        ms: outcome.elapsed_ms,
        rows: outcome.result.row_count,
        status: 'ok',
        target: outcome.executed_on,
        fallback: SERVER_QUERY_FALLBACK_ENABLED
          ? typeof fb === 'string'
            ? fb
            : fb
              ? 'capability_gate'
              : null
          : null,
        sql: tab.sql,
      });
      setHistory((h) => [entry, ...h].slice(0, 100));
      showToast(
        `Query OK · ${outcome.result.row_count.toLocaleString()} rows · ${outcome.elapsed_ms} ms · ${
          outcome.executed_on === 'browser_wasm' ? 'browser' : 'native'
        }`,
      );
      setTabs((tt) => tt.map((x) => (x.id === activeTab ? { ...x, dirty: false } : x)));
    } else {
      setPlan(undefined);
      setRunState({
        status: 'error',
        target: outcome.target,
        ms: outcome.elapsed_ms,
        message: outcome.message,
        code: outcome.code,
      });
      const entry = await appendHistory({
        ms: outcome.elapsed_ms,
        rows: 0,
        status: 'error',
        target: outcome.target ?? target,
        fallback:
          SERVER_QUERY_FALLBACK_ENABLED && outcome.fallback_reason
            ? typeof outcome.fallback_reason === 'string'
              ? outcome.fallback_reason
              : 'capability_gate'
            : null,
        sql: tab.sql,
      });
      setHistory((h) => [entry, ...h].slice(0, 100));
      showToast(outcome.message, 'warn');
    }
  }, [
    activeTab,
    localAccessNeedsReselect,
    querySource,
    reselectLocalFolder,
    runState.status,
    showToast,
    tabs,
    tableMeta?.name,
  ]);

  const loadMoreRows = useCallback(async () => {
    if (runState.status === 'running' || loadingMoreRows) return;
    const currentResult = resultData;
    const runForPage = resultPageRun;
    const activeRun = activeResultPageRun;
    if (!currentResult || !runForPage || !activeRun) return;
    const page = currentResult.page;
    if (!page?.has_more || page.next_offset == null) return;
    if (!sameQueryResultPageRun(runForPage, activeRun)) {
      showToast('Run the current query before loading more rows.', 'warn');
      return;
    }

    const ctrl = new AbortController();
    cancelRef.current = ctrl;
    setLoadingMoreRows(true);

    const req = queryResultPageRunRequest(runForPage, {
      offset: page.next_offset,
      size: page.size,
    });

    const { runQuery } = await import('../services/query.ts');
    const outcome = await runQuery(
      req,
      (event) => {
        if (!SERVER_QUERY_FALLBACK_ENABLED && event.kind === 'fallback') {
          return;
        }
        setEvents((prev) => [...prev, event]);
        if (event.kind === 'metrics') setMetrics(event.metrics);
      },
      ctrl.signal,
      runForPage.source,
    );

    setLoadingMoreRows(false);

    const latestResultRun = resultPageRunRef.current;
    const latestActiveRun = activeResultPageRunRef.current;
    if (
      !latestResultRun ||
      !latestActiveRun ||
      !sameQueryResultPageRun(latestResultRun, runForPage) ||
      !sameQueryResultPageRun(latestActiveRun, runForPage)
    ) {
      showToast('Result batch discarded because the query changed.', 'warn');
      return;
    }

    if (outcome.status === 'done') {
      const merged = appendResultPage(currentResult, outcome.result);
      setResultData(merged);
      setResultPageRun(runForPage);
      setMetrics(outcome.metrics);
      if (outcome.explain) setPlan({ tree: outcome.explain });
      setRunState((prev) =>
        prev.status === 'done' ? { ...prev, ms: outcome.elapsed_ms, rows: merged.row_count } : prev,
      );
      showToast(`Loaded ${outcome.result.rows.length.toLocaleString()} more rows`);
      return;
    }

    showToast(outcome.message, 'warn');
  }, [activeResultPageRun, loadingMoreRows, resultData, resultPageRun, runState.status, showToast]);

  const cancelRun = useCallback(() => {
    cancelRef.current?.abort();
    if (runTimer.current != null) window.clearInterval(runTimer.current);
    setLoadingMoreRows(false);
    setResultPageRun(undefined);
    setRunState({ status: 'idle' });
    showToast('Query cancelled', 'warn');
  }, [showToast]);

  const formatSql = useCallback(() => {
    setTabs((tt) =>
      tt.map((x) => (x.id === activeTab ? { ...x, sql: prettifySql(x.sql), dirty: true } : x)),
    );
    showToast('Formatted');
  }, [activeTab, showToast]);

  const addTab = useCallback(() => {
    const id = 'q' + (tabs.length + 1) + '-' + Math.random().toString(36).slice(2, 6);
    setTabs((tt) => [
      ...tt,
      {
        id,
        title: `untitled-${tt.length + 1}.sql`,
        sql: 'SELECT ',
        dirty: true,
        pin: null,
        preferred: defaultTarget,
      },
    ]);
    setActiveTab(id);
  }, [defaultTarget, tabs.length]);

  const closeTab = useCallback(
    (id: string, e: MouseEvent) => {
      e.stopPropagation();
      const idx = tabs.findIndex((x) => x.id === id);
      const next = tabs.filter((x) => x.id !== id);
      if (next.length === 0) return;
      setTabs(next);
      if (activeTab === id) setActiveTab(next[Math.max(0, idx - 1)].id);
    },
    [activeTab, tabs],
  );

  // ─── Resize handles ────────────────────────────────────
  const startResizeSidebar = useCallback(
    (e: MouseEvent) => {
      e.preventDefault();
      const sx = e.clientX;
      const sw = sidebarW;
      const move = (ev: globalThis.MouseEvent) => layoutActions.setSidebarW(sw + ev.clientX - sx);
      const up = () => {
        window.removeEventListener('mousemove', move);
        window.removeEventListener('mouseup', up);
      };
      window.addEventListener('mousemove', move);
      window.addEventListener('mouseup', up);
    },
    [layoutActions, sidebarW],
  );

  const startResizeResults = useCallback(
    (e: MouseEvent) => {
      e.preventDefault();
      const sy = e.clientY;
      const sh = resultsH;
      const move = (ev: globalThis.MouseEvent) =>
        layoutActions.setResultsH(sh - (ev.clientY - sy), { viewportHeight: window.innerHeight });
      const up = () => {
        window.removeEventListener('mousemove', move);
        window.removeEventListener('mouseup', up);
      };
      window.addEventListener('mousemove', move);
      window.addEventListener('mouseup', up);
    },
    [layoutActions, resultsH],
  );

  // ─── Save handlers ─────────────────────────────────────
  const onSaveConfirm = useCallback(
    (name: string) => {
      const tab = tabs.find((x) => x.id === activeTab);
      if (!tab) return;
      const target = tab.preferred === 'native' ? 'native' : 'browser_wasm';
      void saveQuery({ name, sql: tab.sql, target }).then((entry) => {
        setSaved((prev) => [entry, ...prev.filter((s) => s.name !== entry.name)]);
        setSaveOpen(false);
        setTabs((tt) =>
          tt.map((x) => (x.id === activeTab ? { ...x, dirty: false, title: `${name}.sql` } : x)),
        );
        showToast(`Saved · ${name}`);
      });
    },
    [activeTab, tabs, showToast],
  );

  // ─── Keyboard shortcuts ────────────────────────────────
  useEffect(() => {
    const h = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 's') {
        e.preventDefault();
        setSaveOpen(true);
      }
    };
    window.addEventListener('keydown', h);
    return () => window.removeEventListener('keydown', h);
  }, []);

  const supportedCount = CAPABILITY_ORDER.length;

  return (
    <div
      className="shell"
      style={
        {
          ['--sidebar-w' as string]: sidebarW + 'px',
          ['--results-h' as string]: resultsH + 'px',
        } as CSSProperties
      }
    >
      <header className="topbar">
        <div className="brand">
          <div className="brand-mark">A</div>
          <div className="brand-name">
            axon <span>· query</span>
          </div>
        </div>

        <button
          className="conn-pill"
          title={
            availableConnectedCatalogs.length
              ? `${availableConnectedCatalogs.length} connected catalog(s) · click to manage`
              : 'Switch catalog'
          }
          onClick={() => availableConnectedCatalogs.length > 0 && setConnectedPanelOpen(true)}
        >
          <span className="conn-dot" />
          <span className="conn-name">
            {catalog?.name ?? 'loading'} <span className="sep">/</span>{' '}
            <span className="db">{catalog?.region ?? '—'} · delta</span>
          </span>
          {availableConnectedCatalogs.length > 0 && (
            <span className="cat-count" title="Connected catalogs">
              +{availableConnectedCatalogs.length}
            </span>
          )}
          <IconChevDownTiny size={9} className="chev" />
        </button>

        <button
          className="btn ghost"
          onClick={() => openConnectModal(1)}
          title="Connect a Delta source (local file, object storage, Unity Catalog, or Delta Sharing)"
        >
          <IconPlus size={11} /> Connect
        </button>

        <div className="topbar-spacer" />

        <button
          className="engine-chip"
          data-cap-trigger
          onClick={() => setCapsOpen((v) => !v)}
          title="Engine capabilities"
        >
          <span className="dot" />
          <span className="label">Capabilities</span>
          <span className="v">{supportedCount}</span>
          <IconChevDownTiny size={9} />
        </button>

        <div className="target-seg" role="group" aria-label="Execution target">
          {TARGET_OPTIONS.map((opt) => (
            <button
              key={opt.id}
              className={(active.preferred === opt.id ? 'active ' : '') + opt.cls}
              onClick={() => setActivePreferred(opt.id)}
              title={targetTitle(opt.id)}
            >
              <span className="ico-dot" />
              {opt.short}
            </button>
          ))}
        </div>

        <span className="tb-divider" />

        <div className="tb-actions">
          <button className="btn ghost" onClick={formatSql} title="Format (⌘⇧F)">
            <IconFormat size={13} /> Format
          </button>
          <button className="btn ghost icon" title="Save (⌘S)" onClick={() => setSaveOpen(true)}>
            <IconSave size={13} />
          </button>
        </div>

        <span className="tb-divider" />

        <div className="run-group">
          {runState.status === 'running' ? (
            <button
              className="btn primary"
              onClick={cancelRun}
              style={{ background: 'var(--danger)', borderColor: 'var(--danger)' }}
            >
              <IconStop size={11} /> Cancel
              <span className="kbd">esc</span>
            </button>
          ) : (
            <>
              <button className="btn primary" onClick={runActive}>
                <IconPlay size={11} /> Run
                <span className="kbd">⌘</span>
                <span className="kbd">⏎</span>
              </button>
              <button className="btn primary" title="Run options">
                <IconChevDownTiny size={9} />
              </button>
            </>
          )}
        </div>

        {capsOpen && (
          <CapabilityPopover
            matrix={capMatrix}
            serverFallbackEnabled={SERVER_QUERY_FALLBACK_ENABLED}
            onClose={() => setCapsOpen(false)}
            anchorRight={300}
            anchorTop={50}
          />
        )}
      </header>

      <div className="main">
        <Sidebar
          catalog={catalog}
          connectedCatalogs={availableConnectedCatalogs}
          activeTable={activeTableRef}
          saved={saved}
          history={history}
          serverFallbackEnabled={SERVER_QUERY_FALLBACK_ENABLED}
          width={sidebarW}
          onInsert={insertAtCursor}
          onResize={startResizeSidebar}
          onPickConnectedTable={setSelectedTableRef}
        />

        <div className="workspace">
          <div className="tabbar">
            {tabs.map((tb) => (
              <div
                key={tb.id}
                className={
                  'qtab ' + (tb.id === activeTab ? 'active ' : '') + (tb.dirty ? 'dirty ' : '')
                }
                onClick={() => setActiveTab(tb.id)}
              >
                <span className="name">{tb.title}</span>
                <span
                  className="x"
                  onClick={(e) => closeTab(tb.id, e)}
                  title={tb.dirty ? 'Close (unsaved changes)' : 'Close'}
                >
                  <span className="x-dot" aria-hidden="true" />
                  <span className="x-close" aria-hidden="true">
                    ×
                  </span>
                </span>
              </div>
            ))}
            <div className="new-tab" onClick={addTab} title="New query (⌘T)">
              +
            </div>
            <div className="spacer" />
          </div>

          <div className="queryref-bar">
            <span className="lbl">From</span>
            <span className="qref" title={tableMeta?.uri}>
              <IconTable size={11} />
              {tableMeta?.name ?? '—'}
              <span className="v">v{tableMeta?.snapshot ?? '—'}</span>
            </span>
            {localAccessNeedsReselect && (
              <button
                className="btn ghost qref-action warn"
                onClick={reselectLocalFolder}
                title="Reselect this local Delta folder to restore browser file access"
              >
                <IconRefresh size={11} /> Reselect folder
              </button>
            )}
            <button
              className={'snap-pill ' + (active.pin != null ? 'pinned' : '')}
              onClick={togglePin}
              title={active.pin != null ? 'Unpin snapshot' : 'Pin snapshot version (time travel)'}
            >
              <IconHistory size={10} />
              {active.pin != null ? (
                <>
                  pinned <span className="v">v{active.pin}</span>
                </>
              ) : (
                'latest'
              )}
            </button>
            <span className="qref-sep" />
            {tableMeta?.partition_columns.length ? (
              <span className="qref-info">
                partitions:{' '}
                <span className="qref-info-val">
                  {tableMeta.partition_columns.map((p) => p.name).join(', ')}
                </span>
              </span>
            ) : (
              <span className="qref-info muted">unpartitioned</span>
            )}
            {tableMeta && (
              <span className="qref-stats">
                {formatBytes(tableMeta.size_bytes)} · {formatRows(tableMeta.row_count)} rows ·{' '}
                {tableMeta.file_count} files
              </span>
            )}
          </div>

          <div className="split">
            <Editor
              value={active.sql}
              catalog={catalog}
              running={runState.status === 'running'}
              onChange={updateActiveSql}
              onRun={runActive}
              onFormat={formatSql}
            />
            <div className="split-resizer" onMouseDown={startResizeResults} />
            <Results
              runState={runState}
              resultData={resultData}
              metrics={metrics}
              events={events}
              history={history}
              serverFallbackEnabled={SERVER_QUERY_FALLBACK_ENABLED}
              plan={plan}
              commits={commits}
              capabilities={capMatrix}
              snapshotPin={active.pin}
              tableSnapshot={tableMeta?.snapshot}
              tableUri={tableMeta?.uri}
              loadingMoreRows={loadingMoreRows}
              onLoadMoreRows={canLoadMoreRows ? loadMoreRows : undefined}
              protocolVersion={
                tableMeta
                  ? {
                      reader: tableMeta.protocol.minReaderVersion,
                      writer: tableMeta.protocol.minWriterVersion,
                      features: tableMeta.protocol.features,
                    }
                  : undefined
              }
            />
          </div>
        </div>
      </div>

      <footer className="statusbar">
        <span className="grp">
          <span className="dot" />
          <span>{catalog ? 'Connected' : 'Connecting'}</span>
        </span>
        <span className="sep" />
        <span className="grp">
          <IconDatabase size={11} />
          <span className="mono">{catalog?.storage ?? '—'}</span>
        </span>
        <span className="sep" />
        <span className="grp">
          <span>WASM</span>
          <span className="mono">{engine?.bundle_tier ?? '—'}</span>
        </span>
        {engine && engine.cold_start_ms > 0 && (
          <>
            <span className="sep" />
            <span className="grp">
              <span>Cold start</span>
              <span className="mono">{engine.cold_start_ms} ms</span>
            </span>
          </>
        )}
        {engine && engine.cache.opfs_budget_mb > 0 && (
          <>
            <span className="sep" />
            <span
              className="grp cache-meter"
              title={`Session cache · ${engine.cache.extents.toLocaleString()} extents`}
            >
              <span>Cache</span>
              <span className="bar">
                <span
                  className="fill"
                  style={{
                    width: `${(engine.cache.opfs_used_mb / engine.cache.opfs_budget_mb) * 100}%`,
                  }}
                />
              </span>
              <span className="mono">
                {engine.cache.opfs_used_mb.toFixed(1)}/{engine.cache.opfs_budget_mb} MB
              </span>
            </span>
          </>
        )}
        <span className="spacer" />
        <span className="grp">
          <span className="mono">{engine?.proto ?? '—'}</span>
        </span>
        <span className="sep" />
        <span className="grp">
          <span>Ln {active.sql.split('\n').length}, Col 1</span>
        </span>
        <span className="sep" />
        <span className="grp">
          <span>SQL</span>
        </span>
      </footer>

      <div className={'toast ' + (toast ? 'show' : '')}>
        <span
          className="dot"
          style={{
            background: toast?.kind === 'warn' ? 'var(--warning)' : 'var(--success)',
          }}
        />
        <span>{toast?.msg ?? ''}</span>
      </div>

      {saveOpen && (
        <SaveDialog
          initialName={active.title.replace(/\.sql$/, '')}
          onCancel={() => setSaveOpen(false)}
          onSave={onSaveConfirm}
        />
      )}

      {connectModalOpen && (
        <Suspense fallback={null}>
          <ConnectModal
            initialStep={connectInitialStep}
            initialSource={connectInitialSource}
            serverFallbackEnabled={SERVER_QUERY_FALLBACK_ENABLED}
            connectorFeatures={CONNECTOR_FEATURES}
            onClose={() => setConnectModalOpen(false)}
            onConnect={handleConnected}
          />
        </Suspense>
      )}

      {connectedPanelOpen && (
        <ConnectedCatalogsPanel
          catalogs={availableConnectedCatalogs}
          activeTable={activeTableRef}
          freshId={freshCatalogId}
          onActivate={setSelectedTableRef}
          onAdd={() => {
            setConnectedPanelOpen(false);
            openConnectModal(1);
          }}
          onRemove={removeConnectedCatalog}
          onClose={() => setConnectedPanelOpen(false)}
        />
      )}

      <TweaksPanel>
        <TweakSection label="Theme" />
        <TweakRadio
          label="Mode"
          value={appearance.theme}
          options={APPEARANCE_THEME_VALUES}
          onChange={(v) => settingsActions.setAppearanceValue('theme', v)}
        />
        <TweakColor
          label="Accent"
          value={appearance.accent}
          options={ACCENT_VALUES}
          onChange={(v) => settingsActions.setAppearanceValue('accent', v)}
        />
        <TweakRadio
          label="Density"
          value={appearance.density}
          options={APPEARANCE_DENSITY_VALUES}
          onChange={(v) => settingsActions.setAppearanceValue('density', v)}
        />
        <TweakSection label="Typography" />
        <TweakSelect
          label="UI font"
          value={appearance.uiFont}
          options={UI_FONT_VALUES}
          onChange={(v) => settingsActions.setAppearanceValue('uiFont', v)}
        />
        <TweakSelect
          label="Code font"
          value={appearance.monoFont}
          options={MONO_FONT_VALUES}
          onChange={(v) => settingsActions.setAppearanceValue('monoFont', v)}
        />
        <TweakSection label="Engine defaults" />
        <TweakRadio
          label="Default target"
          value={defaultTarget}
          options={defaultTargetOptions}
          onChange={settingsActions.setDefaultTarget}
        />
      </TweaksPanel>
    </div>
  );
}

function discardActiveQuerySession(): void {
  void import('../services/query.ts')
    .then(({ discardQuerySession }) => discardQuerySession())
    .catch((error) => console.warn('failed to discard query session:', error));
}

function unregisterLocalDeltaRuntimeIds(registryIds: string[], message: string): void {
  void import('../services/local-delta.ts')
    .then(({ unregisterLocalDeltaRuntime }) =>
      Promise.all(registryIds.map((registryId) => unregisterLocalDeltaRuntime(registryId))),
    )
    .catch((error) => console.warn(message, error));
}
