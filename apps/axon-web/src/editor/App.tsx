import {
  useCallback,
  useEffect,
  lazy,
  useMemo,
  useRef,
  Suspense,
  type CSSProperties,
  type MouseEvent,
} from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { catalogQueryOptions, commitsQueryOptions } from '../query/catalog.ts';
import {
  appendHistoryEntry,
  historyQueryOptions,
  saveSavedQuery,
  savedQueriesQueryOptions,
} from '../query/local.ts';
import {
  CAPABILITY_ORDER,
  defaultCapabilityMatrix,
  overlayCapabilityReport,
} from '../services/capabilities.ts';
import { subscribeEngineStatus } from '../services/engine.ts';
import { CONNECTOR_FEATURES } from '../services/connector-features.ts';
import { hasLocalDeltaRuntime } from '../services/local-delta-session.ts';
import {
  queryResultPageRun,
  queryResultPageRunRequest,
  sameQueryResultPageRun,
  type QueryResultPageRun,
} from '../services/query-pagination.ts';
import {
  querySourceFromConnectedCatalogs,
  type ActiveConnectedTableRef,
} from '../services/query-source.ts';
import { SERVER_QUERY_FALLBACK_ENABLED } from '../services/server-fallback.ts';
import type { QueryExecRequest } from '../services/types.ts';
import {
  selectActiveConnectedTableRef,
  selectActiveSqlTab,
  selectAvailableConnectedCatalogs,
  selectConnectionActions,
  selectDefaultTarget,
  selectEngineActions,
  selectEngineStatus,
  selectFreshCatalogId,
  selectLayout,
  selectLayoutActions,
  selectRunActions,
  selectRunCapabilities,
  selectRunIsRunning,
  selectRunLoadingMoreRows,
  selectRunResultData,
  selectRunResultPageRun,
  selectTabActions,
  selectTabs,
  selectUi,
  selectUiActions,
  useAxonClientStore,
} from '../state/hooks.ts';
import { axonClientStore } from '../state/store.ts';
import { coerceDefaultTargetForAvailability } from '../state/slices/settings.ts';
import type { EngineActions } from '../state/slices/engine.ts';
import type { SqlTab } from '../state/slices/tabs.ts';
import { CapabilityPopover } from './components/Capabilities.tsx';
import { Editor } from './components/Editor.tsx';
import { RunResultsPanel } from './components/RunResultsPanel.tsx';
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
  IconSettings,
  IconStop,
  IconTable,
} from './components/icons.tsx';
import { ConnectedCatalogsPanel } from './connect/ConnectedCatalogs.tsx';
import { catalogTablePath, savedQueryPath } from './catalog-navigation.ts';
import type { ConnectedCatalog, ConnectResult } from './connect/types.ts';
import { formatBytes, formatRows, prettifySql } from './lib/format.ts';
import { navigate } from './router.tsx';

const ConnectModal = lazy(() =>
  import('./connect/ConnectModal.tsx').then((module) => ({ default: module.ConnectModal })),
);

const TARGET_OPTIONS = SERVER_QUERY_FALLBACK_ENABLED
  ? [
      { id: 'auto' as const, short: 'Auto', cls: 'auto' },
      { id: 'browser_wasm' as const, short: 'Browser', cls: 'browser' },
      { id: 'native' as const, short: 'Native', cls: 'native' },
    ]
  : [{ id: 'browser_wasm' as const, short: 'Browser', cls: 'browser' }];

function targetTitle(id: SqlTab['preferred']): string {
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

export function subscribeAppEngineStatus(
  engineActions: Pick<EngineActions, 'setStatus'>,
  subscribe: typeof subscribeEngineStatus = subscribeEngineStatus,
): () => void {
  return subscribe(engineActions.setStatus);
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
  const configuredDefaultTarget = useAxonClientStore(selectDefaultTarget);
  const defaultTarget = coerceDefaultTargetForAvailability(
    configuredDefaultTarget,
    SERVER_QUERY_FALLBACK_ENABLED,
  );
  const availableConnectedCatalogs = useAxonClientStore(selectAvailableConnectedCatalogs);
  const activeTableRef = useAxonClientStore(selectActiveConnectedTableRef);
  const freshCatalogId = useAxonClientStore(selectFreshCatalogId);
  const connectionActions = useAxonClientStore(selectConnectionActions);
  const tabsState = useAxonClientStore(selectTabs);
  const activeSqlTab = useAxonClientStore(selectActiveSqlTab);
  const tabActions = useAxonClientStore(selectTabActions);
  const engine = useAxonClientStore(selectEngineStatus);
  const engineActions = useAxonClientStore(selectEngineActions);
  const {
    saveOpen,
    capsOpen,
    toast,
    connectModalOpen,
    connectInitialStep,
    connectInitialSource,
    connectedPanelOpen,
  } = useAxonClientStore(selectUi);
  const uiActions = useAxonClientStore(selectUiActions);
  const tabs = tabsState.items;
  const activeTabId = tabsState.activeTabId;
  const active = activeSqlTab ?? tabs[0]!;
  const queryClient = useQueryClient();
  const querySource = useMemo(
    () => querySourceFromConnectedCatalogs(availableConnectedCatalogs, activeTableRef),
    [activeTableRef, availableConnectedCatalogs],
  );
  const { data: catalog } = useQuery(catalogQueryOptions(querySource));
  const { data: commits = [] } = useQuery(commitsQueryOptions(querySource));
  const { data: history = [] } = useQuery(historyQueryOptions());
  const { data: saved = [] } = useQuery(savedQueriesQueryOptions());

  const tableMeta = catalog?.tables[0];

  const runIsRunning = useAxonClientStore(selectRunIsRunning);
  const runActions = useAxonClientStore(selectRunActions);

  const cancelRef = useRef<AbortController | null>(null);
  const runTimer = useRef<number | null>(null);
  const activeConnectedTable = useMemo(
    () => connectedTableForRef(availableConnectedCatalogs, activeTableRef),
    [activeTableRef, availableConnectedCatalogs],
  );
  const localAccessNeedsReselect =
    querySource.kind === 'local_delta' &&
    activeConnectedTable?.localPersistence === 'metadata_only_reselect' &&
    !hasLocalDeltaRuntime(querySource.localRegistryId);

  const activeResultPageRun = useMemo(() => {
    return queryResultPageRun(
      {
        sql: active.sql,
        table_name: tableMeta?.name ?? querySource.tableName,
        preferred_target: active.preferred,
        snapshot_version: active.pin ?? undefined,
      },
      querySource,
    );
  }, [active.pin, active.preferred, active.sql, querySource, tableMeta?.name]);
  const activeResultPageRunRef = useRef<QueryResultPageRun | undefined>(undefined);

  useEffect(() => {
    activeResultPageRunRef.current = activeResultPageRun;
  }, [activeResultPageRun]);

  const showToast = useCallback(
    (message: string, kind: 'ok' | 'warn' = 'ok') => {
      uiActions.showToast(message, kind);
      window.setTimeout(() => uiActions.clearToast(), 2400);
    },
    [uiActions],
  );

  const reselectLocalFolder = useCallback(() => {
    uiActions.closeConnectedPanel();
    uiActions.openConnectModal(2, 'local');
  }, [uiActions]);

  const handleConnected = useCallback(
    (result: ConnectResult) => {
      const mutation = connectionActions.connect(result);
      if (mutation.shouldDiscardActiveQuerySession) {
        discardActiveQuerySession();
      }
      if (mutation.localRegistryIdsToUnregister.length > 0) {
        unregisterLocalDeltaRuntimeIds(
          mutation.localRegistryIdsToUnregister,
          'failed to unregister duplicate local Delta catalog:',
        );
      }
      uiActions.closeConnectModal();
      window.setTimeout(() => connectionActions.clearFreshCatalogId(), 4500);
      showToast(
        `${mutation.replaced.length > 0 ? 'Updated' : 'Connected'} · ${
          mutation.catalogAlias ?? 'catalog'
        } · ${mutation.tableCount} tables`,
      );
    },
    [connectionActions, showToast, uiActions],
  );

  const removeConnectedCatalog = useCallback(
    (id: string) => {
      const mutation = connectionActions.removeCatalog(id);
      if (mutation.shouldDiscardActiveQuerySession) {
        discardActiveQuerySession();
      }
      if (mutation.localRegistryIdsToUnregister.length > 0) {
        unregisterLocalDeltaRuntimeIds(
          mutation.localRegistryIdsToUnregister,
          'failed to unregister local Delta catalog:',
        );
      }
    },
    [connectionActions],
  );

  const navigateToConnectedTable = useCallback((ref: ActiveConnectedTableRef) => {
    navigate(catalogTablePath(ref));
  }, []);

  // ─── Subscribe to engine status ─────────────────────────
  useEffect(() => {
    return subscribeAppEngineStatus(engineActions);
  }, [engineActions]);

  // ─── Tab editing ───────────────────────────────────────
  const updateActiveSql = useCallback(
    (sql: string) => {
      tabActions.updateActiveSql(sql);
    },
    [tabActions],
  );

  const insertAtCursor = useCallback(
    (text: string) => {
      tabActions.insertIntoActiveSql(text);
    },
    [tabActions],
  );

  const setActivePreferred = useCallback(
    (target: SqlTab['preferred']) => {
      tabActions.setActivePreferred(target);
    },
    [tabActions],
  );

  const togglePin = useCallback(() => {
    tabActions.toggleActivePin(tableMeta?.snapshot);
  }, [tabActions, tableMeta?.snapshot]);

  // ─── Run lifecycle ─────────────────────────────────────
  const runActive = useCallback(async () => {
    if (runIsRunning) return;
    const tab = active;
    if (localAccessNeedsReselect) {
      runActions.clearForLocalAccessReselect();
      reselectLocalFolder();
      showToast('Reselect this local Delta folder to restore browser file access.', 'warn');
      return;
    }

    cancelRef.current?.abort();
    const ctrl = new AbortController();
    cancelRef.current = ctrl;

    const target = tab.preferred === 'native' ? 'native' : 'browser_wasm';

    runActions.startRun(target);
    if (runTimer.current != null) window.clearInterval(runTimer.current);
    const startedAt = performance.now();
    runTimer.current = window.setInterval(() => {
      runActions.updateRunElapsed(performance.now() - startedAt);
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
        runActions.appendRunEvent(event);
      },
      ctrl.signal,
      querySource,
    );

    if (runTimer.current != null) window.clearInterval(runTimer.current);

    if (outcome.status === 'done') {
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
      runActions.finishRunSuccess({
        runState: {
          status: 'done',
          target: outcome.executed_on,
          ms: outcome.elapsed_ms,
          rows: outcome.result.row_count,
          fallback: fallbackPretty,
        },
        resultData: outcome.result,
        resultPageRun: queryResultPageRun(req, querySource),
        metrics: outcome.metrics,
        plan: outcome.explain ? { tree: outcome.explain } : undefined,
        capabilities: overlayCapabilityReport(
          defaultCapabilityMatrix(),
          outcome.capabilities.capabilities ?? {},
        ),
      });
      await appendHistoryEntry(queryClient, {
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
      showToast(
        `Query OK · ${outcome.result.row_count.toLocaleString()} rows · ${outcome.elapsed_ms} ms · ${
          outcome.executed_on === 'browser_wasm' ? 'browser' : 'native'
        }`,
      );
      tabActions.markActiveClean(tab.id);
    } else {
      runActions.finishRunError({
        status: 'error',
        target: outcome.target,
        ms: outcome.elapsed_ms,
        message: outcome.message,
        code: outcome.code,
      });
      await appendHistoryEntry(queryClient, {
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
      showToast(outcome.message, 'warn');
    }
  }, [
    active,
    localAccessNeedsReselect,
    queryClient,
    querySource,
    reselectLocalFolder,
    runIsRunning,
    runActions,
    showToast,
    tabActions,
    tableMeta?.name,
  ]);

  const loadMoreRows = useCallback(async () => {
    const runSnapshot = axonClientStore.getState();
    if (selectRunIsRunning(runSnapshot) || selectRunLoadingMoreRows(runSnapshot)) return;
    const currentResult = selectRunResultData(runSnapshot);
    const runForPage = selectRunResultPageRun(runSnapshot);
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
    runActions.startLoadMoreRows();

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
        runActions.appendRunEvent(event);
      },
      ctrl.signal,
      runForPage.source,
    );

    const latestResultRun = selectRunResultPageRun(axonClientStore.getState());
    const latestActiveRun = activeResultPageRunRef.current;
    if (
      !latestResultRun ||
      !latestActiveRun ||
      !sameQueryResultPageRun(latestResultRun, runForPage) ||
      !sameQueryResultPageRun(latestActiveRun, runForPage)
    ) {
      runActions.finishLoadMoreRows();
      showToast('Result batch discarded because the query changed.', 'warn');
      return;
    }

    if (outcome.status === 'done') {
      const update = runActions.finishLoadMoreRowsSuccess({
        runForPage,
        activeRun: latestActiveRun,
        resultData: outcome.result,
        metrics: outcome.metrics,
        plan: outcome.explain ? { tree: outcome.explain } : undefined,
        elapsedMs: outcome.elapsed_ms,
      });
      if (update.discarded) {
        showToast('Result batch discarded because the query changed.', 'warn');
        return;
      }
      showToast(`Loaded ${outcome.result.rows.length.toLocaleString()} more rows`);
      return;
    }

    runActions.finishLoadMoreRows();
    showToast(outcome.message, 'warn');
  }, [activeResultPageRun, runActions, showToast]);

  const cancelRun = useCallback(() => {
    cancelRef.current?.abort();
    if (runTimer.current != null) window.clearInterval(runTimer.current);
    runActions.cancelRun();
    showToast('Query cancelled', 'warn');
  }, [runActions, showToast]);

  const formatSql = useCallback(() => {
    tabActions.formatActiveSql(prettifySql);
    showToast('Formatted');
  }, [showToast, tabActions]);

  const addTab = useCallback(() => {
    tabActions.addSqlTab(defaultTarget);
  }, [defaultTarget, tabActions]);

  const closeTab = useCallback(
    (id: string, e: MouseEvent) => {
      e.stopPropagation();
      tabActions.closeTab(id);
    },
    [tabActions],
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
      const tab = active;
      const target = tab.preferred === 'native' ? 'native' : 'browser_wasm';
      void saveSavedQuery(queryClient, { name, sql: tab.sql, target }).then((entry) => {
        uiActions.closeSaveDialog();
        tabActions.markActiveSaved(`${entry.name}.sql`, tab.id, entry.id);
        showToast(`Saved · ${entry.name}`);
      });
    },
    [active, queryClient, showToast, tabActions, uiActions],
  );

  // ─── Keyboard shortcuts ────────────────────────────────
  useEffect(() => {
    const h = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 's') {
        e.preventDefault();
        uiActions.openSaveDialog();
      }
    };
    window.addEventListener('keydown', h);
    return () => window.removeEventListener('keydown', h);
  }, [uiActions]);

  const supportedCount = CAPABILITY_ORDER.length;
  const capMatrix = selectRunCapabilities(axonClientStore.getState());

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
          onClick={() => availableConnectedCatalogs.length > 0 && uiActions.openConnectedPanel()}
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
          onClick={() => uiActions.openConnectModal(1)}
          title="Connect a Delta source (local file, object storage, Unity Catalog, or Delta Sharing)"
        >
          <IconPlus size={11} /> Connect
        </button>

        <div className="topbar-spacer" />

        <button
          className="engine-chip"
          data-cap-trigger
          onClick={uiActions.toggleCapabilityPopover}
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
          <button
            className="btn ghost icon"
            title="Settings"
            aria-label="Open settings"
            onClick={() => navigate('/settings')}
          >
            <IconSettings size={13} />
          </button>
          <button className="btn ghost icon" title="Save (⌘S)" onClick={uiActions.openSaveDialog}>
            <IconSave size={13} />
          </button>
        </div>

        <span className="tb-divider" />

        <div className="run-group">
          {runIsRunning ? (
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
            onClose={uiActions.closeCapabilityPopover}
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
          onPickConnectedTable={navigateToConnectedTable}
          onOpenSavedQuery={(query) => navigate(savedQueryPath(query.id))}
        />

        <div className="workspace">
          <div className="tabbar">
            {tabs.map((tb) => (
              <div
                key={tb.id}
                className={
                  'qtab ' + (tb.id === activeTabId ? 'active ' : '') + (tb.dirty ? 'dirty ' : '')
                }
                onClick={() => tabActions.selectTab(tb.id)}
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
              running={runIsRunning}
              onChange={updateActiveSql}
              onRun={runActive}
              onFormat={formatSql}
            />
            <div className="split-resizer" onMouseDown={startResizeResults} />
            <RunResultsPanel
              history={history}
              serverFallbackEnabled={SERVER_QUERY_FALLBACK_ENABLED}
              commits={commits}
              snapshotPin={active.pin}
              tableSnapshot={tableMeta?.snapshot}
              tableUri={tableMeta?.uri}
              activeResultPageRun={activeResultPageRun}
              onLoadMoreRows={loadMoreRows}
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
        <span>{toast?.message ?? ''}</span>
      </div>

      {saveOpen && (
        <SaveDialog
          initialName={active.title.replace(/\.sql$/, '')}
          onCancel={uiActions.closeSaveDialog}
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
            onClose={uiActions.closeConnectModal}
            onConnect={handleConnected}
          />
        </Suspense>
      )}

      {connectedPanelOpen && (
        <ConnectedCatalogsPanel
          catalogs={availableConnectedCatalogs}
          activeTable={activeTableRef}
          freshId={freshCatalogId}
          onActivate={navigateToConnectedTable}
          onAdd={() => {
            uiActions.closeConnectedPanel();
            uiActions.openConnectModal(1);
          }}
          onRemove={removeConnectedCatalog}
          onClose={uiActions.closeConnectedPanel}
        />
      )}
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
