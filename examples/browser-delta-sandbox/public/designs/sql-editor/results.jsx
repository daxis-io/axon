// Results panel — Results / Plan / Snapshot / Messages / History.
// Plan tab is the marquee Axon-specific surface: pruning, footer reads, bootstrap timing.

function Results({ runState, queryName, snapshotPin, table }) {
  const [tab, setTab] = React.useState('results');
  const [search, setSearch] = React.useState('');
  const [sort, setSort] = React.useState({ col: null, dir: 'asc' });
  const [selectedRow, setSelectedRow] = React.useState(0);

  const cols = window.AXON_DATA.RESULT_COLUMNS;
  const allRows = window.AXON_DATA.RESULT_ROWS;
  const pageSize = 50;

  const filtered = React.useMemo(() => {
    if (!search) return allRows;
    const q = search.toLowerCase();
    return allRows.filter((r) => r.some((v) => String(v).toLowerCase().includes(q)));
  }, [search, allRows]);

  const sorted = React.useMemo(() => {
    if (sort.col == null) return filtered;
    const idx = cols.findIndex((c) => c.name === sort.col);
    if (idx === -1) return filtered;
    const copy = [...filtered];
    copy.sort((a, b) => {
      const av = a[idx],
        bv = b[idx];
      if (av === bv) return 0;
      const r = av < bv ? -1 : 1;
      return sort.dir === 'asc' ? r : -r;
    });
    return copy;
  }, [filtered, sort, cols]);

  const visible = sorted.slice(0, pageSize);

  function clickHeader(c) {
    setSort((s) => {
      if (s.col !== c.name) return { col: c.name, dir: 'asc' };
      if (s.dir === 'asc') return { col: c.name, dir: 'desc' };
      return { col: null, dir: 'asc' };
    });
  }

  // ─── empty / running states ─────────────────────────────
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
            <span className="mono">{runState.elapsed.toFixed(1)} s</span>
          </div>
        </div>
        <div className="progress" />
        <div style={{ flex: 1, display: 'grid', placeItems: 'center', color: 'var(--ink-3)' }}>
          <div style={{ textAlign: 'center' }}>
            <div>Bootstrapping snapshot · pruning partitions · streaming row groups…</div>
            <div style={{ marginTop: 6, fontSize: 11, color: 'var(--ink-4)' }}>
              {snapshotPin ? `pinned snapshot v${snapshotPin}` : 'latest snapshot'}
            </div>
          </div>
        </div>
      </div>
    );
  }

  const m = window.AXON_DATA.METRICS;
  const planFiles = window.AXON_DATA.PLAN_FILES;
  const commits = window.AXON_DATA.COMMITS;
  const totalFiles = m.files_touched + m.files_skipped;
  const totalRG = m.row_groups_touched + m.row_groups_skipped;
  const touchedPct = (m.files_touched / Math.max(1, totalFiles)) * 100;
  const rgTouchedPct = (m.row_groups_touched / Math.max(1, totalRG)) * 100;

  const fallback = runState.fallback;

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
            {m.files_touched}/{totalFiles}
          </span>
        </div>
        <div
          className={'res-tab ' + (tab === 'snapshot' ? 'active' : '')}
          onClick={() => setTab('snapshot')}
        >
          Snapshot <span className="count">v{table?.snapshot ?? '—'}</span>
        </div>
        <div
          className={'res-tab ' + (tab === 'messages' ? 'active' : '')}
          onClick={() => setTab('messages')}
        >
          Messages
        </div>
        <div
          className={'res-tab ' + (tab === 'history' ? 'active' : '')}
          onClick={() => setTab('history')}
        >
          History
        </div>

        <div className="res-meta">
          <span className={'tgt ' + (runState.target === 'browser_wasm' ? 'browser' : 'native')}>
            {runState.target === 'browser_wasm' ? 'browser · wasm' : 'native'}
          </span>
          <span style={{ color: 'var(--ink-4)' }}>·</span>
          <span>{sorted.length.toLocaleString()} rows</span>
          <span style={{ color: 'var(--ink-4)' }}>·</span>
          <span className="mono">{runState.ms} ms</span>
          <span style={{ color: 'var(--ink-4)' }}>·</span>
          <span className="mono">{formatBytes(m.bytes_fetched)} fetched</span>
        </div>
      </div>

      {/* Fallback banner */}
      {fallback && (
        <div className="fallback-banner">
          <span className="arrow">↪</span>
          <span>
            Rerouted from <b>Browser (WASM)</b> → <b>Native</b>
          </span>
          <span className="reason">{fallback.code}</span>
          <span style={{ color: 'var(--ink-3)' }}>· {fallback.detail}</span>
          <span className="spacer" />
          <button className="btn ghost" style={{ height: 22, padding: '0 8px', fontSize: 11 }}>
            Always run native
          </button>
        </div>
      )}

      {/* ─── RESULTS ─────────────────────────── */}
      {tab === 'results' && (
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
              1 – {visible.length} of {sorted.length}
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
            <button className="miniicon" title="Copy Arrow IPC">
              <IconCopy size={12} />
            </button>
            <button className="miniicon" title="Export CSV / Parquet">
              <IconExport size={12} />
            </button>
          </div>

          <div className="table-wrap">
            <table className="grid">
              <thead>
                <tr>
                  <th style={{ width: 44 }} />
                  {cols.map((c) => (
                    <th
                      key={c.name}
                      className={
                        (['long', 'double', 'integer'].includes(c.type) ? 'numeric ' : '') +
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
                {visible.map((row, i) => (
                  <tr
                    key={i}
                    className={i === selectedRow ? 'sel' : ''}
                    onClick={() => setSelectedRow(i)}
                  >
                    <td className="gutter-cell">{i + 1}</td>
                    {row.map((v, j) => {
                      const c = cols[j];
                      const numeric = ['long', 'double', 'integer'].includes(c.type);
                      if (v == null)
                        return (
                          <td key={j}>
                            <span className="cell-null">NULL</span>
                          </td>
                        );
                      let disp = v;
                      if (c.name === 'revenue_usd') disp = '$' + Number(v).toFixed(2);
                      if (numeric && c.name !== 'id' && typeof v === 'number')
                        disp = v.toLocaleString(undefined, { maximumFractionDigits: 2 });
                      return (
                        <td key={j} className={numeric ? 'numeric cell-num' : ''}>
                          {disp}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}

      {/* ─── PLAN ────────────────────────────── */}
      {tab === 'plan' && (
        <div className="plan-body">
          <div className="plan-section">
            <h4>Snapshot bootstrap & I/O</h4>
            <div className="grid-3">
              <div className="kpi accent">
                <div className="l">Bytes fetched</div>
                <div className="v">
                  {(m.bytes_fetched / 1024 / 1024).toFixed(2)}
                  <span className="unit">MB</span>
                </div>
                <div className="sub">from {table?.uri ?? 'table'}</div>
              </div>
              <div className="kpi">
                <div className="l">Files touched</div>
                <div className="v">
                  {m.files_touched}
                  <span className="unit">/ {totalFiles.toLocaleString()}</span>
                </div>
                <div className="sub">{m.files_skipped.toLocaleString()} pruned</div>
              </div>
              <div className="kpi">
                <div className="l">Row groups touched</div>
                <div className="v">
                  {m.row_groups_touched}
                  <span className="unit">/ {totalRG.toLocaleString()}</span>
                </div>
                <div className="sub">{m.row_groups_skipped.toLocaleString()} pruned by stats</div>
              </div>
              <div className="kpi success">
                <div className="l">Snapshot bootstrap</div>
                <div className="v">
                  {m.snapshot_bootstrap_duration_ms}
                  <span className="unit">ms</span>
                </div>
                <div className="sub">{m.footer_reads} footer reads</div>
              </div>
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
                  <span className="seg touched" style={{ flex: m.files_touched }} />
                  <span className="seg skipped" style={{ flex: m.files_skipped }} />
                </div>
                <div className="prune-legend">
                  <span>
                    <span className="sw touched" /> {m.files_touched} touched
                  </span>
                  <span>
                    <span className="sw partition-skipped" /> {m.files_skipped.toLocaleString()}{' '}
                    skipped
                  </span>
                  <span
                    style={{ marginLeft: 'auto', color: 'var(--ink-2)', fontFamily: 'var(--mono)' }}
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
                  <span className="seg touched" style={{ flex: m.row_groups_touched }} />
                  <span className="seg row-group-skipped" style={{ flex: m.row_groups_skipped }} />
                </div>
                <div className="prune-legend">
                  <span>
                    <span className="sw touched" /> {m.row_groups_touched} touched
                  </span>
                  <span>
                    <span className="sw row-group-skipped" />{' '}
                    {m.row_groups_skipped.toLocaleString()} skipped
                  </span>
                  <span
                    style={{ marginLeft: 'auto', color: 'var(--ink-2)', fontFamily: 'var(--mono)' }}
                  >
                    {rgTouchedPct.toFixed(1)}%
                  </span>
                </div>
              </div>
            </div>
          </div>

          <div className="plan-section" style={{ padding: '12px 0 0' }}>
            <h4 style={{ padding: '0 16px' }}>Files</h4>
            <ul className="plan-files">
              {planFiles.map((f) => (
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

          <div className="plan-section" style={{ borderBottom: 0 }}>
            <h4>Plan tree · DataFusion</h4>
            <div className="plan-tree">{window.AXON_DATA.PLAN_TREE}</div>
          </div>
        </div>
      )}

      {/* ─── SNAPSHOT ────────────────────────── */}
      {tab === 'snapshot' && (
        <div className="plan-body">
          <div className="plan-section">
            <h4>Resolved snapshot</h4>
            <div className="grid-3">
              <div className="kpi accent">
                <div className="l">Snapshot version</div>
                <div className="v">v{table?.snapshot ?? '—'}</div>
                <div className="sub">{snapshotPin ? 'pinned · time travel' : 'latest'}</div>
              </div>
              <div className="kpi">
                <div className="l">Checkpoint</div>
                <div className="v">v314</div>
                <div className="sub">+ 4 replay commits</div>
              </div>
              <div className="kpi">
                <div className="l">Active files</div>
                <div className="v">{table?.file_count?.toLocaleString() ?? '—'}</div>
                <div className="sub">{table?.row_count?.toLocaleString() ?? '—'} rows</div>
              </div>
              <div className="kpi">
                <div className="l">Protocol</div>
                <div className="v">
                  r{table?.protocol?.minReaderVersion ?? 0}
                  <span className="unit">/w{table?.protocol?.minWriterVersion ?? 0}</span>
                </div>
                <div className="sub">{(table?.protocol?.features || []).join(', ') || '—'}</div>
              </div>
            </div>
          </div>

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
        </div>
      )}

      {/* ─── MESSAGES ────────────────────────── */}
      {tab === 'messages' && (
        <div className="msg-view">
          <div className="msg-line">
            <span className="time">14:08:21.012</span>
            <span className="lvl info">ROUTE</span>
            <span>
              preferred_target = <b style={{ color: 'var(--accent)' }}>browser_wasm</b> · router
              decision: <b>browser_wasm</b>
            </span>
          </div>
          <div className="msg-line">
            <span className="time">14:08:21.018</span>
            <span className="lvl info">BOOT</span>
            <span>
              resolved snapshot v{table?.snapshot ?? '—'} from checkpoint v314 + 4 replay commits ·{' '}
              {m.footer_reads} footer reads
            </span>
          </div>
          <div className="msg-line">
            <span className="time">14:08:21.024</span>
            <span className="lvl info">PRUNE</span>
            <span>
              partition: kept {m.files_touched}/{totalFiles.toLocaleString()} files · pruned{' '}
              {m.files_skipped.toLocaleString()} via{' '}
              <span style={{ font: '12px var(--mono)' }}>
                placed_month IN ('2026-04','2026-05')
              </span>
            </span>
          </div>
          <div className="msg-line">
            <span className="time">14:08:21.038</span>
            <span className="lvl info">PRUNE</span>
            <span>
              row-group: kept {m.row_groups_touched}/{totalRG.toLocaleString()} · pruned{' '}
              {m.row_groups_skipped.toLocaleString()} via footer stats on{' '}
              <span style={{ font: '12px var(--mono)' }}>placed_at, status</span>
            </span>
          </div>
          <div className="msg-line">
            <span className="time">14:08:21.214</span>
            <span className="lvl info">FETCH</span>
            <span>
              {formatBytes(m.bytes_fetched)} via {m.access_mode.replace('_', ' ')} · range reads
              from <span style={{ font: '12px var(--mono)' }}>{table?.uri ?? 'table'}</span>
            </span>
          </div>
          <div className="msg-line">
            <span className="time">14:08:21.318</span>
            <span className="lvl ok">DONE</span>
            <span>
              {runState.rows} rows · {m.duration_ms} ms · executed on{' '}
              <b style={{ color: 'var(--accent)' }}>
                {runState.target === 'browser_wasm' ? 'browser_wasm' : 'native'}
              </b>
            </span>
          </div>
        </div>
      )}

      {/* ─── HISTORY ─────────────────────────── */}
      {tab === 'history' && (
        <div className="history-list">
          {window.AXON_DATA.HISTORY.map((h, i) => (
            <div key={i} className={'history-row ' + (h.status === 'error' ? 'error' : '')}>
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
                {h.fallback && (
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

window.Results = Results;
