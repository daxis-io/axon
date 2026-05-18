import { useMemo, useRef, useState } from 'react';
import type {
  CapabilityMatrixRow,
  CommitEntry,
  ExecutionTarget,
  HistoryEntry,
  PlanSummary,
  QueryEvent,
  QueryMetricsSummary,
  QueryResultData,
  ResultCell,
  ResultColumn,
} from '../../services/types.ts';
import { formatBytes } from '../lib/format.ts';
import {
  IconArrowDown,
  IconArrowUp,
  IconChevR,
  IconChevUD,
  IconCopy,
  IconExport,
  IconPlus,
  IconRefresh,
  IconSearch,
} from './icons.tsx';

export type RunUiState =
  | { status: 'idle' }
  | { status: 'running'; target: ExecutionTarget; elapsed: number }
  | {
      status: 'done';
      target: ExecutionTarget;
      ms: number;
      rows: number;
      fallback?: { code: string; detail: string } | null;
    }
  | {
      status: 'error';
      target?: ExecutionTarget;
      ms: number;
      message: string;
      code?: string;
    };

type ResultsProps = {
  runState: RunUiState;
  resultData: QueryResultData | undefined;
  metrics: QueryMetricsSummary | undefined;
  events: QueryEvent[];
  history: HistoryEntry[];
  serverFallbackEnabled: boolean;
  plan: PlanSummary | undefined;
  commits: CommitEntry[];
  capabilities?: CapabilityMatrixRow[];
  snapshotPin: number | null;
  tableSnapshot: number | undefined;
  tableUri: string | undefined;
  protocolVersion: { reader: number; writer: number; features: string[] } | undefined;
};

type ResultsTab = 'results' | 'plan' | 'snapshot' | 'messages' | 'history';

export function Results({
  runState,
  resultData,
  metrics,
  events,
  history,
  serverFallbackEnabled,
  plan,
  commits,
  snapshotPin,
  tableSnapshot,
  tableUri,
  protocolVersion,
}: ResultsProps) {
  const [tab, setTab] = useState<ResultsTab>('results');
  const [search, setSearch] = useState('');
  const [sort, setSort] = useState<{ col: string | null; dir: 'asc' | 'desc' }>({
    col: null,
    dir: 'asc',
  });
  const [selectedRow, setSelectedRow] = useState(0);
  const [scrollTop, setScrollTop] = useState(0);
  const [copyState, setCopyState] = useState<'idle' | 'copied'>('idle');
  const [viewedCell, setViewedCell] = useState<{
    column: string;
    row: number;
    value: ResultCell;
  } | null>(null);
  const tableWrapRef = useRef<HTMLDivElement | null>(null);

  const cols: ResultColumn[] = resultData?.columns ?? [];
  const allRows: ResultCell[][] = resultData?.rows ?? [];

  const filtered = useMemo(() => {
    if (!search) return allRows;
    const q = search.toLowerCase();
    return allRows.filter((r) => r.some((v) => String(v).toLowerCase().includes(q)));
  }, [search, allRows]);

  const sorted = useMemo(() => {
    if (sort.col == null) return filtered;
    const idx = cols.findIndex((c) => c.name === sort.col);
    if (idx === -1) return filtered;
    const copy = [...filtered];
    copy.sort((a, b) => {
      const av = a[idx];
      const bv = b[idx];
      if (av === bv) return 0;
      if (av == null) return 1;
      if (bv == null) return -1;
      const r = av < bv ? -1 : 1;
      return sort.dir === 'asc' ? r : -r;
    });
    return copy;
  }, [filtered, sort, cols]);

  const rowHeight = 28;
  const overscan = 8;
  const windowSize = 80;
  const startIndex = Math.max(0, Math.floor(scrollTop / rowHeight) - overscan);
  const endIndex = Math.min(sorted.length, startIndex + windowSize);
  const visible = sorted.slice(startIndex, endIndex);
  const paddingTop = startIndex * rowHeight;
  const paddingBottom = Math.max(0, (sorted.length - endIndex) * rowHeight);
  const pageStart = sorted.length === 0 ? 0 : startIndex + 1;
  const pageEnd = Math.min(sorted.length, endIndex);

  function clickHeader(c: ResultColumn) {
    setSort((s) => {
      if (s.col !== c.name) return { col: c.name, dir: 'asc' };
      if (s.dir === 'asc') return { col: c.name, dir: 'desc' };
      return { col: null, dir: 'asc' };
    });
    tableWrapRef.current?.scrollTo({ top: 0 });
    setScrollTop(0);
  }

  async function copyResults() {
    await copyText(toCsv(cols, sorted));
    setCopyState('copied');
    window.setTimeout(() => setCopyState('idle'), 1800);
  }

  function exportResults() {
    const blob = new Blob([toCsv(cols, sorted)], { type: 'text/csv;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `axon-query-results-${timestampForFilename()}.csv`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
  }

  if (runState.status === 'idle') {
    return (
      <div className="results">
        <div className="res-tabs">
          <div className="res-tab active">Results</div>
          <div className="res-tab">Plan</div>
          <div className="res-tab">Snapshot</div>
          <div className="res-tab">Messages</div>
          <div className="res-meta">
            <span>
              Press <span className="kbd">⌘</span>
              <span className="kbd">⏎</span> to run
            </span>
          </div>
        </div>
        <div
          style={{
            flex: 1,
            display: 'grid',
            placeItems: 'center',
            color: 'var(--ink-3)',
            padding: 24,
          }}
        >
          <div style={{ textAlign: 'center' }}>
            <div style={{ fontSize: 13, color: 'var(--ink-2)', marginBottom: 4 }}>
              No results yet
            </div>
            <div style={{ fontSize: 12 }}>
              Run a query to see rows, plan, pruning and snapshot details.
            </div>
          </div>
        </div>
      </div>
    );
  }

  if (runState.status === 'running') {
    return (
      <div className="results">
        <div className="res-tabs">
          <div className="res-tab active">Results</div>
          <div className="res-tab">Plan</div>
          <div className="res-tab">Messages</div>
          <div className="res-meta">
            <span className="dot" style={{ background: 'var(--warning)' }} />
            <span>
              Running on{' '}
              <b style={{ color: 'var(--ink-2)' }}>
                {runState.target === 'browser_wasm' ? 'Browser (WASM)' : 'Native'}
              </b>
            </span>
            <span className="mono">{(runState.elapsed / 1000).toFixed(1)} s</span>
          </div>
        </div>
        <div className="progress" />
        <div
          style={{
            flex: 1,
            display: 'grid',
            placeItems: 'center',
            color: 'var(--ink-3)',
          }}
        >
          <div style={{ textAlign: 'center' }}>
            <div>Bootstrapping snapshot · pruning partitions · streaming row groups…</div>
            <div style={{ marginTop: 6, fontSize: 11, color: 'var(--ink-4)' }}>
              {snapshotPin != null ? `pinned snapshot v${snapshotPin}` : 'latest snapshot'}
            </div>
          </div>
        </div>
      </div>
    );
  }

  const totalFiles = (metrics?.files_touched ?? 0) + (metrics?.files_skipped ?? 0);
  const totalRG = (metrics?.row_groups_touched ?? 0) + (metrics?.row_groups_skipped ?? 0);
  const touchedPct = totalFiles ? ((metrics?.files_touched ?? 0) / totalFiles) * 100 : 0;
  const rgTouchedPct = totalRG ? ((metrics?.row_groups_touched ?? 0) / totalRG) * 100 : 0;
  const fallback =
    serverFallbackEnabled && runState.status === 'done' ? runState.fallback : undefined;
  const visibleEvents = serverFallbackEnabled
    ? events
    : events.filter((event) => event.kind !== 'fallback');

  return (
    <div className="results">
      <div className="res-tabs">
        <div
          className={'res-tab ' + (tab === 'results' ? 'active' : '')}
          onClick={() => setTab('results')}
        >
          Results <span className="count">{sorted.length}</span>
        </div>
        <div
          className={'res-tab ' + (tab === 'plan' ? 'active' : '')}
          onClick={() => setTab('plan')}
        >
          Plan{' '}
          <span className="count">
            {metrics?.files_touched ?? 0}/{totalFiles}
          </span>
        </div>
        <div
          className={'res-tab ' + (tab === 'snapshot' ? 'active' : '')}
          onClick={() => setTab('snapshot')}
        >
          Snapshot <span className="count">v{tableSnapshot ?? '—'}</span>
        </div>
        <div
          className={'res-tab ' + (tab === 'messages' ? 'active' : '')}
          onClick={() => setTab('messages')}
        >
          Messages <span className="count">{visibleEvents.length}</span>
        </div>
        <div
          className={'res-tab ' + (tab === 'history' ? 'active' : '')}
          onClick={() => setTab('history')}
        >
          History
        </div>

        <div className="res-meta">
          {runState.status === 'done' && (
            <>
              <span
                className={'tgt ' + (runState.target === 'browser_wasm' ? 'browser' : 'native')}
              >
                {runState.target === 'browser_wasm' ? 'browser · wasm' : 'native'}
              </span>
              <span style={{ color: 'var(--ink-4)' }}>·</span>
              <span>{sorted.length.toLocaleString()} rows</span>
              <span style={{ color: 'var(--ink-4)' }}>·</span>
              <span className="mono">{runState.ms} ms</span>
              {metrics && (
                <>
                  <span style={{ color: 'var(--ink-4)' }}>·</span>
                  <span className="mono">{formatBytes(metrics.bytes_fetched)} fetched</span>
                </>
              )}
            </>
          )}
          {runState.status === 'error' && (
            <>
              <span style={{ color: 'var(--danger)' }}>● error</span>
              <span className="mono">{runState.ms} ms</span>
            </>
          )}
        </div>
      </div>

      {fallback && <FallbackBanner code={fallback.code} detail={fallback.detail} />}

      {runState.status === 'error' && tab === 'results' && (
        <div style={{ padding: 20 }}>
          <div style={{ color: 'var(--danger)', fontWeight: 600, marginBottom: 4 }}>
            {runState.code ?? 'error'}
          </div>
          <div style={{ font: '12.5px var(--mono)', color: 'var(--ink-2)' }}>
            {runState.message}
          </div>
        </div>
      )}

      {tab === 'results' && runState.status === 'done' && (
        <>
          <div className="res-toolbar">
            <div className="search">
              <IconSearch size={11} />
              <input
                placeholder="Filter rows…"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
              />
            </div>
            <button className="btn ghost" title="Add filter">
              <IconPlus size={12} /> Filter
            </button>
            <button className="btn ghost" title="Sort">
              <IconChevUD size={12} /> Sort
            </button>
            <div className="spacer" />
            <span className="pagestat">
              {pageStart} – {pageEnd} of {sorted.length}
            </span>
            <button className="miniicon" title="Previous page">
              <IconChevR size={12} style={{ transform: 'rotate(180deg)' }} />
            </button>
            <button className="miniicon" title="Next page">
              <IconChevR size={12} />
            </button>
            <div style={{ width: 1, alignSelf: 'stretch', background: 'var(--line)' }} />
            <button className="miniicon" title="Refresh">
              <IconRefresh size={12} />
            </button>
            <button
              className="miniicon"
              title="Copy results as CSV"
              onClick={() => void copyResults()}
              disabled={cols.length === 0}
            >
              <IconCopy size={12} />
            </button>
            <button
              className="miniicon"
              title="Export results as CSV"
              onClick={exportResults}
              disabled={cols.length === 0}
            >
              <IconExport size={12} />
            </button>
            {copyState === 'copied' && <span className="copy-state">copied</span>}
          </div>

          <div
            className="table-wrap"
            ref={tableWrapRef}
            onScroll={(event) => setScrollTop(event.currentTarget.scrollTop)}
          >
            <table className="grid">
              <thead>
                <tr>
                  <th style={{ width: 44 }} />
                  {cols.map((c) => (
                    <th
                      key={c.name}
                      className={
                        (isNumeric(c.type) ? 'numeric ' : '') +
                        (sort.col === c.name ? 'sorted' : '')
                      }
                      onClick={() => clickHeader(c)}
                    >
                      {c.name}
                      <span className="type">{c.type}</span>
                      <span className="sort">
                        {sort.col === c.name ? (
                          sort.dir === 'asc' ? (
                            <IconArrowUp size={10} />
                          ) : (
                            <IconArrowDown size={10} />
                          )
                        ) : null}
                      </span>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {paddingTop > 0 && (
                  <tr aria-hidden="true">
                    <td colSpan={cols.length + 1} style={{ height: paddingTop, padding: 0 }} />
                  </tr>
                )}
                {visible.map((row, i) => (
                  <tr
                    key={startIndex + i}
                    className={startIndex + i === selectedRow ? 'sel' : ''}
                    onClick={() => setSelectedRow(startIndex + i)}
                  >
                    <td className="gutter-cell">{startIndex + i + 1}</td>
                    {row.map((v, j) => {
                      const c = cols[j];
                      const numeric = isNumeric(c?.type);
                      if (v == null) {
                        return (
                          <td key={j}>
                            <span className="cell-null">NULL</span>
                          </td>
                        );
                      }
                      let disp: string | number | boolean = v;
                      if (numeric && typeof v === 'number') {
                        disp = v.toLocaleString(undefined, { maximumFractionDigits: 4 });
                      }
                      return (
                        <td
                          key={j}
                          className={numeric ? 'numeric cell-num' : ''}
                          onDoubleClick={(event) => {
                            event.stopPropagation();
                            setViewedCell({
                              column: c?.name ?? `column_${j + 1}`,
                              row: startIndex + i + 1,
                              value: v,
                            });
                          }}
                        >
                          {String(disp)}
                        </td>
                      );
                    })}
                  </tr>
                ))}
                {paddingBottom > 0 && (
                  <tr aria-hidden="true">
                    <td colSpan={cols.length + 1} style={{ height: paddingBottom, padding: 0 }} />
                  </tr>
                )}
              </tbody>
            </table>
          </div>

          {viewedCell && (
            <div className="cell-viewer" role="dialog" aria-label="Cell value">
              <div className="cell-viewer-head">
                <span>
                  {viewedCell.column} · row {viewedCell.row}
                </span>
                <button
                  className="miniicon"
                  title="Close cell value"
                  onClick={() => setViewedCell(null)}
                >
                  ×
                </button>
              </div>
              <pre>{formatCell(viewedCell.value)}</pre>
            </div>
          )}
        </>
      )}

      {tab === 'plan' && (
        <div className="plan-body">
          <div className="plan-section">
            <h4>Snapshot bootstrap &amp; I/O</h4>
            <div className="grid-3">
              <KpiTile
                accent
                label="Bytes fetched"
                value={metrics ? (metrics.bytes_fetched / 1024 / 1024).toFixed(2) : '—'}
                unit="MB"
                sub={tableUri ? `from ${tableUri}` : undefined}
              />
              <KpiTile
                label="Files touched"
                value={`${metrics?.files_touched ?? 0}`}
                unit={`/ ${totalFiles.toLocaleString()}`}
                sub={`${(metrics?.files_skipped ?? 0).toLocaleString()} pruned`}
              />
              <KpiTile
                label="Row groups touched"
                value={`${metrics?.row_groups_touched ?? 0}`}
                unit={`/ ${totalRG.toLocaleString()}`}
                sub={`${(metrics?.row_groups_skipped ?? 0).toLocaleString()} pruned by stats`}
              />
              <KpiTile
                success
                label="Snapshot bootstrap"
                value={`${metrics?.snapshot_bootstrap_duration_ms ?? '—'}`}
                unit="ms"
                sub={`${metrics?.footer_reads ?? 0} footer reads`}
              />
            </div>
          </div>

          <div className="plan-section">
            <h4>Pruning</h4>
            <div style={{ display: 'flex', gap: 24, alignItems: 'flex-start', flexWrap: 'wrap' }}>
              <div style={{ flex: 1, minWidth: 260 }}>
                <div style={{ fontSize: 11.5, color: 'var(--ink-3)', marginBottom: 4 }}>
                  Files · partition-prune + stats-prune
                </div>
                <div className="prune-bar">
                  <span className="seg touched" style={{ flex: metrics?.files_touched ?? 0 }} />
                  <span className="seg skipped" style={{ flex: metrics?.files_skipped ?? 0 }} />
                </div>
                <div className="prune-legend">
                  <span>
                    <span className="sw touched" /> {metrics?.files_touched ?? 0} touched
                  </span>
                  <span>
                    <span className="sw partition-skipped" />{' '}
                    {(metrics?.files_skipped ?? 0).toLocaleString()} skipped
                  </span>
                  <span
                    style={{
                      marginLeft: 'auto',
                      color: 'var(--ink-2)',
                      fontFamily: 'var(--mono)',
                    }}
                  >
                    {touchedPct.toFixed(1)}%
                  </span>
                </div>
              </div>
              <div style={{ flex: 1, minWidth: 260 }}>
                <div style={{ fontSize: 11.5, color: 'var(--ink-3)', marginBottom: 4 }}>
                  Row groups · footer-stat prune
                </div>
                <div className="prune-bar">
                  <span
                    className="seg touched"
                    style={{ flex: metrics?.row_groups_touched ?? 0 }}
                  />
                  <span
                    className="seg row-group-skipped"
                    style={{ flex: metrics?.row_groups_skipped ?? 0 }}
                  />
                </div>
                <div className="prune-legend">
                  <span>
                    <span className="sw touched" /> {metrics?.row_groups_touched ?? 0} touched
                  </span>
                  <span>
                    <span className="sw row-group-skipped" />{' '}
                    {(metrics?.row_groups_skipped ?? 0).toLocaleString()} skipped
                  </span>
                  <span
                    style={{
                      marginLeft: 'auto',
                      color: 'var(--ink-2)',
                      fontFamily: 'var(--mono)',
                    }}
                  >
                    {rgTouchedPct.toFixed(1)}%
                  </span>
                </div>
              </div>
            </div>
          </div>

          {plan?.files && plan.files.length > 0 && (
            <div className="plan-section" style={{ padding: '12px 0 0' }}>
              <h4 style={{ padding: '0 16px' }}>Files</h4>
              <ul className="plan-files">
                {plan.files.map((f) => (
                  <li key={f.path} className={'plan-file ' + f.status}>
                    <span className="badge" title={f.status} />
                    <span className="path">{f.path}</span>
                    <span className="num">{formatBytes(f.size)}</span>
                    <span className="num">{f.rows.toLocaleString()}</span>
                    <span className="reason">
                      {f.reason || (f.status === 'touched' ? 'scanned' : '')}
                    </span>
                  </li>
                ))}
              </ul>
            </div>
          )}

          {plan?.tree && (
            <div className="plan-section" style={{ borderBottom: 0 }}>
              <h4>Plan tree · DataFusion</h4>
              <div className="plan-tree">{plan.tree}</div>
            </div>
          )}
        </div>
      )}

      {tab === 'snapshot' && (
        <div className="plan-body">
          <div className="plan-section">
            <h4>Resolved snapshot</h4>
            <div className="grid-3">
              <KpiTile
                accent
                label="Snapshot version"
                value={`v${tableSnapshot ?? '—'}`}
                sub={snapshotPin != null ? 'pinned · time travel' : 'latest'}
              />
              <KpiTile label="Active files" value={`${plan?.files?.length ?? '—'}`} />
              <KpiTile
                label="Protocol"
                value={`r${protocolVersion?.reader ?? 0}`}
                unit={`/w${protocolVersion?.writer ?? 0}`}
                sub={(protocolVersion?.features ?? []).join(', ') || '—'}
              />
            </div>
          </div>

          {commits.length > 0 && (
            <div className="plan-section" style={{ padding: '0', borderBottom: 0 }}>
              <h4 style={{ padding: '14px 16px 8px' }}>Commit log</h4>
              <div className="commits">
                {commits.map((c) => (
                  <div key={c.v} className={'commit ' + (c.current ? 'current' : '')}>
                    <span className="rail" />
                    <span className="dot" />
                    <span className="v">v{c.v}</span>
                    <span className={'op ' + c.op}>{c.op}</span>
                    <span className="ts">{c.ts.replace('T', ' ')}</span>
                    <span className="note">
                      {c.note}
                      {c.checkpoint && <span className="checkpoint-flag">CHECKPOINT</span>}
                    </span>
                    <span className="files">
                      +{c.adds} / −{c.removes}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {commits.length === 0 && (
            <div className="plan-section">
              <div
                style={{
                  padding: 16,
                  color: 'var(--ink-4)',
                  fontSize: 12,
                  textAlign: 'center',
                }}
              >
                Commit log will appear here once Delta log parsing is wired (Phase 2).
              </div>
            </div>
          )}
        </div>
      )}

      {tab === 'messages' && (
        <div className="msg-view">
          {visibleEvents.length === 0 ? (
            <div style={{ color: 'var(--ink-4)' }}>No events emitted yet.</div>
          ) : (
            visibleEvents.map((e, i) => (
              <div key={i} className="msg-line">
                <span className="time">{(e.elapsed_ms / 1000).toFixed(3)}s</span>
                <span className={'lvl ' + classifyEvent(e)}>{labelEvent(e)}</span>
                <span>{messageText(e)}</span>
              </div>
            ))
          )}
        </div>
      )}

      {tab === 'history' && (
        <div className="history-list">
          {history.length === 0 && (
            <div style={{ padding: 16, color: 'var(--ink-4)' }}>No queries yet.</div>
          )}
          {history.map((h) => (
            <div key={h.id} className={'history-row ' + (h.status === 'error' ? 'error' : '')}>
              <span className="time">{h.time}</span>
              <span className="ms">{h.ms} ms</span>
              <span
                className="rows"
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 6,
                  justifyContent: 'flex-end',
                }}
              >
                <span className={'tgt ' + (h.target === 'browser_wasm' ? 'browser' : 'native')}>
                  {h.target === 'browser_wasm' ? 'wasm' : 'native'}
                </span>
                {serverFallbackEnabled && h.fallback && (
                  <span style={{ color: 'var(--warning)' }} title={`fallback: ${h.fallback}`}>
                    ↪
                  </span>
                )}
              </span>
              <span className="sql">{h.sql}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function KpiTile({
  label,
  value,
  unit,
  sub,
  accent,
  success,
  warn,
}: {
  label: string;
  value: string;
  unit?: string;
  sub?: string;
  accent?: boolean;
  success?: boolean;
  warn?: boolean;
}) {
  const cls = ['kpi', accent && 'accent', success && 'success', warn && 'warn']
    .filter(Boolean)
    .join(' ');
  return (
    <div className={cls}>
      <div className="l">{label}</div>
      <div className="v">
        {value}
        {unit && <span className="unit">{unit}</span>}
      </div>
      {sub && <div className="sub">{sub}</div>}
    </div>
  );
}

function FallbackBanner({ code, detail }: { code: string; detail: string }) {
  return (
    <div className="fallback-banner">
      <span className="arrow">↪</span>
      <span>
        Rerouted from <b>Browser (WASM)</b> → <b>Native</b>
      </span>
      <span className="reason">{code}</span>
      <span style={{ color: 'var(--ink-3)' }}>· {detail}</span>
      <span className="spacer" />
      <button className="btn ghost" style={{ height: 22, padding: '0 8px', fontSize: 11 }}>
        Always run native
      </button>
    </div>
  );
}

function isNumeric(type: string | undefined): boolean {
  if (!type) return false;
  return ['long', 'double', 'integer', 'int', 'bigint', 'float'].includes(type);
}

function toCsv(columns: ResultColumn[], rows: ResultCell[][]): string {
  const header = columns.map((column) => csvEscape(column.name)).join(',');
  const body = rows.map((row) => row.map(csvEscape).join(','));
  return [header, ...body].join('\n');
}

function csvEscape(value: ResultCell): string {
  if (value == null) return '';
  const raw = formatCell(value);
  return /[",\n\r]/.test(raw) ? `"${raw.replace(/"/g, '""')}"` : raw;
}

function formatCell(value: ResultCell): string {
  return value == null ? 'NULL' : String(value);
}

async function copyText(text: string): Promise<void> {
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    return;
  }

  const textarea = document.createElement('textarea');
  textarea.value = text;
  textarea.setAttribute('readonly', '');
  textarea.style.position = 'fixed';
  textarea.style.opacity = '0';
  document.body.appendChild(textarea);
  textarea.select();
  document.execCommand('copy');
  textarea.remove();
}

function timestampForFilename(): string {
  return new Date().toISOString().slice(0, 19).replace(/[:T]/g, '-');
}

function classifyEvent(e: QueryEvent): string {
  if (e.kind === 'log') {
    if (e.level === 'warn') return 'warn';
    if (e.level === 'error') return 'warn';
    return 'info';
  }
  if (e.kind === 'fallback') return 'warn';
  if (e.kind === 'metrics') return 'info';
  if (e.kind === 'progress' && e.stage === 'finished') return 'ok';
  return 'info';
}

function labelEvent(e: QueryEvent): string {
  switch (e.kind) {
    case 'progress':
      return e.stage.toUpperCase();
    case 'log':
      return e.level.toUpperCase();
    case 'metrics':
      return 'METRICS';
    case 'fallback':
      return 'FALLBACK';
  }
}

function messageText(e: QueryEvent): string {
  switch (e.kind) {
    case 'progress':
      return `stage = ${e.stage}`;
    case 'log':
      return e.message;
    case 'metrics':
      return `${e.metrics.files_touched}/${
        e.metrics.files_touched + e.metrics.files_skipped
      } files touched, ${formatBytes(e.metrics.bytes_fetched)} fetched`;
    case 'fallback':
      return typeof e.reason === 'string'
        ? `reason = ${e.reason}`
        : `capability_gate: ${e.reason.capability_gate.capability} (${e.reason.capability_gate.required_state})`;
  }
}
