// Axon · Query — main app.
// Top bar: catalog · branch · capabilities · execution target segmented · run
// Workspace: query tabs · snapshot-pin sub-bar · editor · resizable results
// Status bar: engine bundle · cache meter · cold start · worker mem

const { useState, useEffect, useRef, useMemo } = React;

const TWEAK_DEFAULTS = /*EDITMODE-BEGIN*/ {
  theme: 'light',
  accent: '#1F4FE0',
  density: 'regular',
  uiFont: 'Geist',
  monoFont: 'Geist Mono',
  preferredTarget: 'auto',
}; /*EDITMODE-END*/

function App() {
  const [t, setTweak] = useTweaks(TWEAK_DEFAULTS);

  // Query tabs — each carries SQL + optional snapshot pin per primary table
  const [tabs, setTabs] = useState([
    {
      id: 'q1',
      title: 'top-customers-30d.sql',
      sql: window.AXON_DATA.SAMPLE_QUERY,
      dirty: false,
      table: 'orders',
      pin: null,
      preferred: 'browser_wasm',
    },
    {
      id: 'q2',
      title: 'fulfilment-funnel.sql',
      sql: window.AXON_DATA.SECONDARY_QUERY,
      dirty: false,
      table: 'orders',
      pin: null,
      preferred: 'browser_wasm',
    },
    {
      id: 'q3',
      title: 'products-cdf.sql',
      sql: window.AXON_DATA.THIRD_QUERY,
      dirty: true,
      table: 'products',
      pin: 41,
      preferred: 'native',
    },
  ]);
  const [activeTab, setActiveTab] = useState('q1');
  const active = tabs.find((x) => x.id === activeTab) || tabs[0];
  const cat = window.AXON_DATA.CATALOG;
  const tableMeta = cat.tables.find((x) => x.name === active.table);

  // Last run state
  const [run, setRun] = useState({
    status: 'done',
    rows: 20,
    ms: 312,
    at: Date.now() - 60000,
    elapsed: 0,
    target: 'browser_wasm',
    fallback: null,
  });
  const runTimer = useRef(null);

  // UI state
  const [toast, setToast] = useState(null);
  const [sidebarW, setSidebarW] = useState(264);
  const [resultsH, setResultsH] = useState(360);
  const [capsOpen, setCapsOpen] = useState(false);

  // Apply theme + tokens
  useEffect(() => {
    const root = document.documentElement;
    root.setAttribute('data-theme', t.theme === 'dark' ? 'dark' : 'light');
    root.setAttribute('data-density', t.density);
    root.style.setProperty('--accent', t.accent);
    root.style.setProperty('--accent-soft', hexToSoft(t.accent, t.theme === 'dark'));
    root.style.setProperty('--ui', `"${t.uiFont}", ui-sans-serif, system-ui, sans-serif`);
    root.style.setProperty('--mono', `"${t.monoFont}", ui-monospace, Menlo, monospace`);
  }, [t]);

  function showToast(msg, kind = 'ok') {
    setToast({ msg, kind });
    setTimeout(() => setToast(null), 2400);
  }

  function updateActiveSql(sql) {
    setTabs((tt) => tt.map((x) => (x.id === activeTab ? { ...x, sql, dirty: true } : x)));
  }
  function insertAtCursor(text) {
    setTabs((tt) =>
      tt.map((x) =>
        x.id === activeTab
          ? { ...x, sql: x.sql + (x.sql.endsWith('\n') ? '' : '\n') + text, dirty: true }
          : x,
      ),
    );
  }
  function setActivePreferred(target) {
    setTabs((tt) => tt.map((x) => (x.id === activeTab ? { ...x, preferred: target } : x)));
  }
  function togglePin() {
    setTabs((tt) =>
      tt.map((x) => {
        if (x.id !== activeTab) return x;
        const pinned = x.pin != null;
        return { ...x, pin: pinned ? null : (tableMeta?.snapshot ?? null) };
      }),
    );
  }

  // ─── Run lifecycle ──────────────────────────────────────
  function runQuery() {
    if (run.status === 'running') return;
    const start = performance.now();
    const preferred = active.preferred;
    setRun({
      status: 'running',
      rows: 0,
      ms: 0,
      at: Date.now(),
      elapsed: 0,
      target: preferred === 'auto' ? 'browser_wasm' : preferred,
      fallback: null,
    });
    clearInterval(runTimer.current);
    runTimer.current = setInterval(() => {
      setRun((r) => ({ ...r, elapsed: (performance.now() - start) / 1000 }));
    }, 80);
    const fakeMs = 280 + Math.random() * 420;
    setTimeout(() => {
      clearInterval(runTimer.current);
      // simulate fallback when the active tab references CDF table_changes()
      const wantsCDF = /table_changes\s*\(/i.test(active.sql);
      const wantsDV = active.table === 'shipments';
      let target = preferred === 'auto' ? 'browser_wasm' : preferred;
      let fallback = null;
      if (wantsCDF && target === 'browser_wasm') {
        target = 'native';
        fallback = {
          code: 'capability_gate',
          detail: 'ChangeDataFeed required (NativeOnly on browser_wasm)',
        };
      } else if (wantsDV && target === 'browser_wasm') {
        target = 'native';
        fallback = {
          code: 'capability_gate',
          detail: 'DeletionVectors required (NativeOnly on browser_wasm)',
        };
      }
      setRun({
        status: 'done',
        rows: 20,
        ms: Math.round(fakeMs / 12 + 14),
        at: Date.now(),
        elapsed: 0,
        target,
        fallback,
      });
      showToast(
        `Query OK · 20 rows · ${Math.round(fakeMs / 12 + 14)} ms · ${target === 'browser_wasm' ? 'browser' : 'native'}`,
      );
      setTabs((tt) => tt.map((x) => (x.id === activeTab ? { ...x, dirty: false } : x)));
    }, fakeMs);
  }

  function cancelQuery() {
    clearInterval(runTimer.current);
    setRun((r) => ({ ...r, status: 'done', rows: 0, ms: 0, fallback: null }));
    showToast('Query cancelled', 'warn');
  }

  function formatSql() {
    setTabs((tt) =>
      tt.map((x) => (x.id === activeTab ? { ...x, sql: prettifySql(x.sql), dirty: true } : x)),
    );
    showToast('Formatted');
  }

  function addTab() {
    const id = 'q' + (tabs.length + 1) + '-' + Math.random().toString(36).slice(2, 6);
    const next = {
      id,
      title: `untitled-${tabs.length + 1}.sql`,
      sql: 'SELECT ',
      dirty: true,
      table: 'orders',
      pin: null,
      preferred: 'browser_wasm',
    };
    setTabs([...tabs, next]);
    setActiveTab(id);
  }
  function closeTab(id, e) {
    e.stopPropagation();
    const idx = tabs.findIndex((x) => x.id === id);
    const next = tabs.filter((x) => x.id !== id);
    if (next.length === 0) return;
    setTabs(next);
    if (activeTab === id) setActiveTab(next[Math.max(0, idx - 1)].id);
  }

  // resize handlers
  function startResizeSidebar(e) {
    e.preventDefault();
    const sx = e.clientX,
      sw = sidebarW;
    const move = (ev) => setSidebarW(Math.max(220, Math.min(480, sw + ev.clientX - sx)));
    const up = () => {
      window.removeEventListener('mousemove', move);
      window.removeEventListener('mouseup', up);
    };
    window.addEventListener('mousemove', move);
    window.addEventListener('mouseup', up);
  }
  function startResizeResults(e) {
    e.preventDefault();
    const sy = e.clientY,
      sh = resultsH;
    const move = (ev) =>
      setResultsH(Math.max(80, Math.min(window.innerHeight - 240, sh - (ev.clientY - sy))));
    const up = () => {
      window.removeEventListener('mousemove', move);
      window.removeEventListener('mouseup', up);
    };
    window.addEventListener('mousemove', move);
    window.addEventListener('mouseup', up);
  }

  // shortcuts
  useEffect(() => {
    const h = (e) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 's') {
        e.preventDefault();
        setTabs((tt) => tt.map((x) => (x.id === activeTab ? { ...x, dirty: false } : x)));
        showToast('Saved');
      }
    };
    window.addEventListener('keydown', h);
    return () => window.removeEventListener('keydown', h);
  }, [activeTab]);

  const engine = window.AXON_DATA.ENGINE_STATUS;
  const cachePct = (engine.cache.opfs_used_mb / engine.cache.opfs_budget_mb) * 100;

  return (
    <div
      className="shell"
      style={{ '--sidebar-w': sidebarW + 'px', '--results-h': resultsH + 'px' }}
    >
      {/* ── Top bar ───────────────────────────── */}
      <header className="topbar">
        <div className="brand">
          <div className="brand-mark">A</div>
          <div className="brand-name">
            axon <span>· query</span>
          </div>
        </div>

        <button className="conn-pill" title="Switch catalog">
          <span className="conn-dot" />
          <span className="conn-name">
            {cat.name} <span className="sep">/</span>{' '}
            <span className="db">{cat.region} · delta</span>
          </span>
          <IconChevDownTiny size={9} className="chev" />
        </button>

        <button className="btn ghost" title="Branch (Git-style versioning)">
          <IconBranch size={13} /> main
          <IconChevDownTiny size={9} style={{ marginLeft: 2, color: 'var(--ink-4)' }} />
        </button>

        <div className="topbar-spacer" />

        {/* Capability indicator → opens matrix popover */}
        <button
          className="engine-chip"
          data-cap-trigger
          onClick={() => setCapsOpen((v) => !v)}
          title="Engine capabilities"
        >
          <span className="dot" />
          <span className="label">Capabilities</span>
          <span className="v">10</span>
          <IconChevDownTiny size={9} />
        </button>

        {/* Execution target selector */}
        <div className="target-seg" role="group" aria-label="Execution target">
          {[
            { id: 'auto', short: 'Auto' },
            { id: 'browser_wasm', short: 'Browser' },
            { id: 'native', short: 'Native' },
          ].map((opt) => (
            <button
              key={opt.id}
              className={
                (active.preferred === opt.id ? 'active ' : '') +
                (opt.id === 'browser_wasm' ? 'browser' : opt.id === 'native' ? 'native' : 'auto')
              }
              onClick={() => setActivePreferred(opt.id)}
              title={
                opt.id === 'auto'
                  ? 'Router picks browser_wasm when possible, native otherwise'
                  : opt.id === 'browser_wasm'
                    ? 'Prefer browser (WASM) — falls back to native when needed'
                    : 'Run on native DataFusion runtime'
              }
            >
              <span className="ico-dot" />
              {opt.short}
            </button>
          ))}
        </div>

        <button className="btn ghost" onClick={formatSql} title="Format (⌘⇧F)">
          <IconFormat size={13} /> Format
        </button>
        <button className="btn ghost" title="Explain plan">
          <IconSparkle size={13} /> Explain
        </button>
        <button className="btn ghost icon" title="Save (⌘S)">
          <IconSave size={13} />
        </button>
        <button className="btn ghost icon" title="Share">
          <IconShare size={13} />
        </button>

        <div
          style={{ width: 1, alignSelf: 'stretch', background: 'var(--line)', margin: '0 4px' }}
        />

        <div className="run-group">
          {run.status === 'running' ? (
            <button
              className="btn primary"
              onClick={cancelQuery}
              style={{ background: 'var(--danger)', borderColor: 'var(--danger)' }}
            >
              <IconStop size={11} /> Cancel
              <span className="kbd">esc</span>
            </button>
          ) : (
            <>
              <button className="btn primary" onClick={runQuery}>
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
          <CapabilityPopover onClose={() => setCapsOpen(false)} anchorRight={300} anchorTop={50} />
        )}
      </header>

      {/* ── Main ──────────────────────────────── */}
      <div className="main">
        <Sidebar onInsert={insertAtCursor} width={sidebarW} onResize={startResizeSidebar} />

        <div className="workspace">
          {/* Editor tab bar */}
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
                  {tb.dirty ? '●' : <IconClose size={10} />}
                </span>
              </div>
            ))}
            <div className="new-tab" onClick={addTab} title="New query (⌘T)">
              <IconPlus size={12} />
            </div>
            <div className="spacer" />
          </div>

          {/* Query-context sub-bar: table + snapshot pin */}
          <div className="queryref-bar">
            <span className="lbl">FROM</span>
            <span className="qref" title={tableMeta?.uri}>
              <IconTable size={11} />
              {active.table}
              <span style={{ color: 'var(--ink-4)' }}>·</span>
              <span className="v">v{tableMeta?.snapshot}</span>
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
              {tableMeta?.partition_columns?.length ? (
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

          {/* Editor + Results split */}
          <div className="split">
            <Editor
              value={active.sql}
              onChange={updateActiveSql}
              onRun={runQuery}
              onFormat={formatSql}
              running={run.status === 'running'}
            />
            <div className="split-resizer" onMouseDown={startResizeResults} />
            <Results
              runState={run}
              queryName={active.title}
              snapshotPin={active.pin}
              table={tableMeta}
            />
          </div>
        </div>
      </div>

      {/* ── Status bar ────────────────────────── */}
      <footer className="statusbar">
        <span className="grp">
          <span className="dot" />
          <span>Connected</span>
        </span>
        <span className="sep" />
        <span className="grp">
          <IconDatabase size={11} />
          <span className="mono">{cat.storage}</span>
        </span>
        <span className="sep" />
        <span className="grp">
          <IconBolt size={11} />
          <span>WASM</span>
          <span className="mono">{engine.bundle_tier}</span>
          <span style={{ color: 'var(--ink-4)' }}>· {engine.wasm_size_kb} KB</span>
        </span>
        <span className="sep" />
        <span
          className="grp cache-meter"
          title={`OPFS extent cache · ${engine.cache.extents.toLocaleString()} extents · ${(engine.cache.hit_ratio * 100).toFixed(0)}% hit ratio`}
        >
          <span>OPFS</span>
          <span className="bar">
            <span className="fill" style={{ width: `${cachePct}%` }} />
          </span>
          <span className="mono">
            {engine.cache.opfs_used_mb.toFixed(0)}/{engine.cache.opfs_budget_mb} MB
          </span>
        </span>
        <span className="sep" />
        <span className="grp">
          <span>Worker</span>
          <span className="mono">{engine.worker_mem_mb.toFixed(1)} MB</span>
          <span style={{ color: 'var(--ink-4)' }}>· cold {engine.cold_start_ms}ms</span>
        </span>
        <span className="spacer" />
        <span className="grp">
          <span className="mono">{engine.proto}</span>
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

      {/* ── Toast ─────────────────────────────── */}
      <div className={'toast ' + (toast ? 'show' : '')}>
        <span
          className="dot"
          style={{ background: toast?.kind === 'warn' ? 'var(--warning)' : 'var(--success)' }}
        />
        <span>{toast?.msg || ''}</span>
      </div>

      {/* ── Tweaks ────────────────────────────── */}
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

// Very simple SQL prettifier
function prettifySql(sql) {
  return sql
    .replace(/\s+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .replace(
      /\b(select|from|where|group by|order by|having|limit|join|left join|right join|inner join|on|and|or|with|as|case|when|then|else|end|insert|update|delete|values|distinct|union|all|interval|filter|in|between|like)\b/gi,
      (m) => m.toUpperCase(),
    );
}

function hexToSoft(hex, dark) {
  const h = hex.replace('#', '');
  const r = parseInt(h.substring(0, 2), 16),
    g = parseInt(h.substring(2, 4), 16),
    b = parseInt(h.substring(4, 6), 16);
  return dark ? `rgba(${r},${g},${b},0.22)` : `rgba(${r},${g},${b},0.10)`;
}

ReactDOM.createRoot(document.getElementById('root')).render(<App />);
