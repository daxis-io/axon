import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
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
import { unregisterLocalDeltaRuntime } from '../services/local-delta.ts';
import { discardQuerySession, runQuery } from '../services/query.ts';
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
import { CapabilityPopover } from './components/Capabilities.tsx';
import { Editor } from './components/Editor.tsx';
import { Results, type RunUiState } from './components/Results.tsx';
import { SaveDialog } from './components/SaveDialog.tsx';
import { Sidebar } from './components/Sidebar.tsx';
import {
  IconBranch,
  IconChevDownTiny,
  IconDatabase,
  IconFormat,
  IconHistory,
  IconPlay,
  IconPlus,
  IconSave,
  IconShare,
  IconSparkle,
  IconStop,
  IconTable,
} from './components/icons.tsx';
import { ConnectModal } from './connect/ConnectModal.tsx';
import { ConnectedCatalogsPanel } from './connect/ConnectedCatalogs.tsx';
import type { SourceId } from './connect/data.ts';
import {
  buildCatalogFromResult,
  catalogsAvailableForFeatures,
  loadConnectedCatalogs,
  saveConnectedCatalogs,
} from './connect/store.ts';
import type { ConnectedCatalog, ConnectResult } from './connect/types.ts';
import { formatBytes, formatRows, hexToSoft, prettifySql } from './lib/format.ts';
import {
  TweakColor,
  TweakRadio,
  TweakSection,
  TweakSelect,
  TweaksPanel,
  useTweaks,
} from './tweaks/TweaksPanel.tsx';

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

const TWEAK_DEFAULTS = {
  theme: 'light' as 'light' | 'dark',
  accent: '#1F4FE0',
  density: 'regular' as 'compact' | 'regular' | 'comfy',
  uiFont: 'Geist' as 'Geist' | 'Inter Tight' | 'IBM Plex Sans',
  monoFont: 'Geist Mono' as 'Geist Mono' | 'JetBrains Mono' | 'IBM Plex Mono' | 'Fira Code',
  preferredTarget: 'auto' as 'auto' | 'browser_wasm' | 'native',
};

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

export function App() {
  const [t, setTweak] = useTweaks(TWEAK_DEFAULTS);

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
  const [sidebarW, setSidebarW] = useState(264);
  const [resultsH, setResultsH] = useState(360);

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

  useEffect(() => {
    saveConnectedCatalogs(connectedCatalogs);
  }, [connectedCatalogs]);

  const openConnectModal = useCallback((step: 1 | 2 | 3 = 1, src: SourceId | null = null) => {
    setConnectInitialStep(step);
    setConnectInitialSource(src);
    setConnectModalOpen(true);
  }, []);

  const handleConnected = useCallback((result: ConnectResult) => {
    const catalog = buildCatalogFromResult(result);
    setConnectedCatalogs((cs) => [catalog, ...cs]);
    const firstTable = firstQueryableTableRef([catalog]);
    if (firstTable) setSelectedTableRef(firstTable);
    setFreshCatalogId(catalog.id);
    setConnectModalOpen(false);
    window.setTimeout(() => setFreshCatalogId(null), 4500);
    setToast({
      msg: `Connected · ${catalog.alias} · ${catalog.schemas.reduce(
        (a, s) => a + s.tables.length,
        0,
      )} tables`,
      kind: 'ok',
    });
    window.setTimeout(() => setToast(null), 2400);
  }, []);

  const removeConnectedCatalog = useCallback(
    (id: string) => {
      setConnectedCatalogs((cs) => {
        const removed = cs.find((catalog) => catalog.id === id);
        const removedLocalRegistryIds =
          removed?.kind === 'local'
            ? removed.schemas.flatMap((schema) =>
                schema.tables.flatMap((table) => table.localRegistryId ?? []),
              )
            : [];
        const next = cs.filter((c) => c.id !== id);
        const nextAvailable = catalogsAvailableForFeatures(next, CONNECTOR_FEATURES);
        setSelectedTableRef((current) =>
          current?.catalogId === id
            ? firstQueryableTableRef(nextAvailable)
            : resolveActiveTableRef(nextAvailable, current),
        );
        if (removedLocalRegistryIds.length > 0) {
          if (selectedTableRef?.catalogId === id) discardQuerySession();
          void Promise.all(
            removedLocalRegistryIds.map((registryId) => unregisterLocalDeltaRuntime(registryId)),
          ).catch((error) => console.warn('failed to unregister local Delta catalog:', error));
        }
        return next;
      });
    },
    [selectedTableRef],
  );

  // ─── Apply theme + tokens ──────────────────────────────
  useEffect(() => {
    const root = document.documentElement;
    root.setAttribute('data-theme', t.theme === 'dark' ? 'dark' : 'light');
    root.setAttribute('data-density', t.density);
    root.style.setProperty('--accent', t.accent);
    root.style.setProperty('--accent-soft', hexToSoft(t.accent, t.theme === 'dark'));
    root.style.setProperty('--ui', `"${t.uiFont}", ui-sans-serif, system-ui, sans-serif`);
    root.style.setProperty('--mono', `"${t.monoFont}", ui-monospace, Menlo, monospace`);
  }, [t]);

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

    cancelRef.current?.abort();
    const ctrl = new AbortController();
    cancelRef.current = ctrl;

    const target = tab.preferred === 'native' ? 'native' : 'browser_wasm';

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
  }, [activeTab, querySource, runState.status, showToast, tabs, tableMeta?.name]);

  const cancelRun = useCallback(() => {
    cancelRef.current?.abort();
    if (runTimer.current != null) window.clearInterval(runTimer.current);
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
        preferred: 'browser_wasm',
      },
    ]);
    setActiveTab(id);
  }, [tabs.length]);

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
      const move = (ev: globalThis.MouseEvent) =>
        setSidebarW(Math.max(220, Math.min(480, sw + ev.clientX - sx)));
      const up = () => {
        window.removeEventListener('mousemove', move);
        window.removeEventListener('mouseup', up);
      };
      window.addEventListener('mousemove', move);
      window.addEventListener('mouseup', up);
    },
    [sidebarW],
  );

  const startResizeResults = useCallback(
    (e: MouseEvent) => {
      e.preventDefault();
      const sy = e.clientY;
      const sh = resultsH;
      const move = (ev: globalThis.MouseEvent) =>
        setResultsH(Math.max(80, Math.min(window.innerHeight - 240, sh - (ev.clientY - sy))));
      const up = () => {
        window.removeEventListener('mousemove', move);
        window.removeEventListener('mouseup', up);
      };
      window.addEventListener('mousemove', move);
      window.addEventListener('mouseup', up);
    },
    [resultsH],
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

        <button className="btn ghost" title="Branch (Git-style versioning)">
          <IconBranch size={13} /> main
          <IconChevDownTiny size={9} style={{ marginLeft: 2, color: 'var(--ink-4)' }} />
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

        <button className="btn ghost" onClick={formatSql} title="Format (⌘⇧F)">
          <IconFormat size={13} /> Format
        </button>
        <button className="btn ghost" title="Explain plan (Phase 2)">
          <IconSparkle size={13} /> Explain
        </button>
        <button className="btn ghost icon" title="Save (⌘S)" onClick={() => setSaveOpen(true)}>
          <IconSave size={13} />
        </button>
        <button className="btn ghost icon" title="Share">
          <IconShare size={13} />
        </button>

        <div
          style={{ width: 1, alignSelf: 'stretch', background: 'var(--line)', margin: '0 4px' }}
        />

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
          saved={saved}
          history={history}
          serverFallbackEnabled={SERVER_QUERY_FALLBACK_ENABLED}
          width={sidebarW}
          onInsert={insertAtCursor}
          onResize={startResizeSidebar}
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
                <span className="dot" />
                <span className="name">{tb.title}</span>
                <span className="x" onClick={(e) => closeTab(tb.id, e)}>
                  {tb.dirty ? '●' : '×'}
                </span>
              </div>
            ))}
            <div className="new-tab" onClick={addTab} title="New query (⌘T)">
              +
            </div>
            <div className="spacer" />
          </div>

          <div className="queryref-bar">
            <span className="lbl">FROM</span>
            <span className="qref" title={tableMeta?.uri}>
              <IconTable size={11} />
              {tableMeta?.name ?? '—'}
              <span style={{ color: 'var(--ink-4)' }}>·</span>
              <span className="v">v{tableMeta?.snapshot ?? '—'}</span>
            </span>
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
            <span className="lbl" style={{ marginLeft: 8 }}>
              ·
            </span>
            <span style={{ color: 'var(--ink-3)' }}>
              {tableMeta?.partition_columns.length ? (
                <>
                  partitions:{' '}
                  <span style={{ fontFamily: 'var(--mono)', color: 'var(--ink-2)' }}>
                    {tableMeta.partition_columns.map((p) => p.name).join(', ')}
                  </span>
                </>
              ) : (
                <span style={{ color: 'var(--ink-4)' }}>unpartitioned</span>
              )}
            </span>
            <span
              style={{
                marginLeft: 'auto',
                color: 'var(--ink-4)',
                fontFamily: 'var(--mono)',
                fontSize: 11,
              }}
            >
              {tableMeta && (
                <>
                  {formatBytes(tableMeta.size_bytes)} · {formatRows(tableMeta.row_count)} rows ·{' '}
                  {tableMeta.file_count} files
                </>
              )}
            </span>
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
        <ConnectModal
          initialStep={connectInitialStep}
          initialSource={connectInitialSource}
          serverFallbackEnabled={SERVER_QUERY_FALLBACK_ENABLED}
          connectorFeatures={CONNECTOR_FEATURES}
          onClose={() => setConnectModalOpen(false)}
          onConnect={handleConnected}
        />
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
          value={t.theme}
          options={['light', 'dark']}
          onChange={(v) => setTweak('theme', v)}
        />
        <TweakColor
          label="Accent"
          value={t.accent}
          options={['#1F4FE0', '#7A3CD7', '#0F9D74', '#C2410C']}
          onChange={(v) => setTweak('accent', v)}
        />
        <TweakRadio
          label="Density"
          value={t.density}
          options={['compact', 'regular', 'comfy']}
          onChange={(v) => setTweak('density', v)}
        />
        <TweakSection label="Typography" />
        <TweakSelect
          label="UI font"
          value={t.uiFont}
          options={['Geist', 'Inter Tight', 'IBM Plex Sans']}
          onChange={(v) => setTweak('uiFont', v)}
        />
        <TweakSelect
          label="Code font"
          value={t.monoFont}
          options={['Geist Mono', 'JetBrains Mono', 'IBM Plex Mono', 'Fira Code']}
          onChange={(v) => setTweak('monoFont', v)}
        />
        <TweakSection label="Engine defaults" />
        <TweakRadio
          label="Default target"
          value={t.preferredTarget}
          options={['auto', 'browser_wasm', 'native']}
          onChange={(v) => setTweak('preferredTarget', v)}
        />
      </TweaksPanel>
    </div>
  );
}
