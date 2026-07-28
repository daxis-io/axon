// Sidebar — Catalog → Delta tables (with snapshot, partitions, protocol features).

function Sidebar({ onInsert, width, onResize, onPickTable }) {
  const [tab, setTab] = React.useState('catalog');
  const [query, setQuery] = React.useState('');
  const [openTbl, setOpenTbl] = React.useState({ orders: true });
  const [hoverTbl, setHoverTbl] = React.useState(null);

  const cat = window.AXON_DATA.CATALOG;
  const tables = cat.tables.filter(
    (t) =>
      !query ||
      t.name.toLowerCase().includes(query.toLowerCase()) ||
      t.columns.some((c) => c.name.toLowerCase().includes(query.toLowerCase())),
  );

  return (
    <aside className="sidebar" style={{ width }}>
      <div className="sb-tabs">
        {['catalog', 'saved', 'history'].map((tt) => (
          <div
            key={tt}
            className={'sb-tab ' + (tab === tt ? 'active' : '')}
            onClick={() => setTab(tt)}
          >
            {tt === 'catalog' ? 'Catalog' : tt === 'saved' ? 'Saved' : 'History'}
          </div>
        ))}
      </div>

      {tab === 'catalog' && (
        <>
          <div className="cat-card">
            <div className="row1">
              <IconDatabase size={11} />
              <span>{cat.name}</span>
              <span className="pill">● connected</span>
            </div>
            <div className="row2" title={cat.storage}>
              {cat.storage} · {cat.region}
            </div>
          </div>

          <div className="sb-search">
            <IconSearch size={12} />
            <input
              placeholder="Search tables, columns…"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
            />
            <span className="kbd clear-kbd">/</span>
          </div>

          <div className="sb-scroll">
            <div className="sb-section">Delta tables · {cat.tables.length}</div>
            {tables.map((t) => {
              const open = !!openTbl[t.name];
              return (
                <React.Fragment key={t.name}>
                  <div
                    className={'sb-row tbl ' + (open ? 'open ' : '')}
                    onClick={() => setOpenTbl({ ...openTbl, [t.name]: !open })}
                    onDoubleClick={() => onInsert(`SELECT *\nFROM ${t.name}\nLIMIT 100;`)}
                    onMouseEnter={() => setHoverTbl(t.name)}
                    onMouseLeave={() => setHoverTbl(null)}
                    style={{ paddingLeft: 8 }}
                  >
                    <span className="twist">
                      <IconChevR size={9} />
                    </span>
                    <span className="ico">
                      <IconTable size={12} />
                    </span>
                    <span>{t.name}</span>
                    <span className="meta">
                      <span style={{ color: 'var(--accent)', marginRight: 5 }}>v{t.snapshot}</span>
                      {formatRows(t.row_count)}
                    </span>
                  </div>
                  {open &&
                    t.columns.map((c) => (
                      <div
                        key={c.name}
                        className="sb-row col"
                        onDoubleClick={() => onInsert(c.name)}
                      >
                        <span className="ico">
                          {c.part ? <IconKey size={11} /> : <IconColumn size={11} />}
                        </span>
                        <span>{c.name}</span>
                        {c.pk && <span className="badge pk">PK</span>}
                        {c.fk && <span className="badge fk">FK</span>}
                        {c.part && (
                          <span
                            className="badge"
                            style={{ background: 'var(--warning-soft)', color: 'var(--warning)' }}
                          >
                            PART
                          </span>
                        )}
                        <span className="type">{c.type}</span>
                      </div>
                    ))}
                </React.Fragment>
              );
            })}

            <div className="sb-section">SQL functions</div>
            {['date_trunc()', 'now()', 'table_changes()', 'coalesce()', 'json_extract()'].map(
              (f) => (
                <div
                  key={f}
                  className="sb-row"
                  style={{ paddingLeft: 24 }}
                  onDoubleClick={() => onInsert(f)}
                >
                  <span className="ico" style={{ color: 'var(--syn-fn)' }}>
                    <IconBolt size={11} />
                  </span>
                  <span style={{ fontFamily: 'var(--mono)', fontSize: 12 }}>{f}</span>
                </div>
              ),
            )}
          </div>

          {/* Table-detail flyout — hover */}
          {hoverTbl && (
            <TableDetailFlyout
              table={cat.tables.find((t) => t.name === hoverTbl)}
              left={width + 6}
            />
          )}
        </>
      )}

      {tab === 'saved' && (
        <>
          <div className="sb-search">
            <IconSearch size={12} />
            <input placeholder="Search saved queries…" />
          </div>
          <div className="sb-scroll" style={{ padding: 0 }}>
            <div className="sb-section">Recent</div>
            {window.AXON_DATA.SAVED.map((s, i) => (
              <div key={i} className="sb-saved-item">
                <span className="ico" style={{ color: 'var(--accent)' }}>
                  <IconBookmark size={13} />
                </span>
                <div style={{ minWidth: 0, flex: 1 }}>
                  <div
                    className="name"
                    style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
                  >
                    {s.name}
                  </div>
                  <div className="sub" style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    <span>
                      {s.owner} · {s.edited}
                    </span>
                    <span style={{ marginLeft: 'auto' }}>
                      <span
                        className={'tgt ' + (s.target === 'browser_wasm' ? 'browser' : 'native')}
                      >
                        {s.target === 'browser_wasm' ? 'wasm' : 'native'}
                      </span>
                    </span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </>
      )}

      {tab === 'history' && (
        <div className="sb-scroll" style={{ padding: 0 }}>
          <div className="sb-section">Today</div>
          {window.AXON_DATA.HISTORY.map((h, i) => (
            <div
              key={i}
              className="sb-saved-item"
              style={{ alignItems: 'flex-start', flexDirection: 'column', gap: 4 }}
            >
              <div style={{ display: 'flex', gap: 6, width: '100%', alignItems: 'center' }}>
                <span style={{ font: '11.5px var(--mono)', color: 'var(--ink-3)' }}>{h.time}</span>
                <span className={'tgt ' + (h.target === 'browser_wasm' ? 'browser' : 'native')}>
                  {h.target === 'browser_wasm' ? 'wasm' : 'native'}
                </span>
                {h.fallback && (
                  <span
                    style={{ color: 'var(--warning)', fontSize: 10.5 }}
                    title={`fallback: ${h.fallback}`}
                  >
                    ↪
                  </span>
                )}
                <span
                  style={{ marginLeft: 'auto', color: 'var(--ink-4)', font: '11px var(--mono)' }}
                >
                  {h.ms} ms
                </span>
              </div>
              <div
                style={{
                  font: '12px var(--mono)',
                  color: h.status === 'error' ? 'var(--danger)' : 'var(--ink-2)',
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                  maxWidth: '100%',
                }}
              >
                {h.sql}
              </div>
            </div>
          ))}
        </div>
      )}

      <div className="sb-resizer" style={{ left: width - 2 }} onMouseDown={onResize} />
    </aside>
  );
}

function TableDetailFlyout({ table, left }) {
  if (!table) return null;
  return (
    <div className="popover tbl-detail" style={{ left, top: 90, pointerEvents: 'none' }}>
      <div className="hdr">
        <IconTable size={13} />
        <span>{table.name}</span>
        <span
          className="sub"
          style={{ marginLeft: 'auto', color: 'var(--accent)', fontFamily: 'var(--mono)' }}
        >
          v{table.snapshot}
        </span>
      </div>
      <div className="uri">{table.uri}</div>
      <div className="stats">
        <span className="lbl">Rows</span>{' '}
        <span className="val">{table.row_count.toLocaleString()}</span>
        <span className="lbl">Files</span>{' '}
        <span className="val">{table.file_count.toLocaleString()}</span>
        <span className="lbl">Row groups</span>{' '}
        <span className="val">{table.row_group_count.toLocaleString()}</span>
        <span className="lbl">Size</span>{' '}
        <span className="val">{formatBytes(table.size_bytes)}</span>
        <span className="lbl">Partitions</span>{' '}
        <span className="val">
          {table.partition_columns.length
            ? table.partition_columns.map((p) => `${p.name}:${p.type}`).join(', ')
            : '—'}
        </span>
        <span className="lbl">Last commit</span>{' '}
        <span className="val">{table.last_commit.slice(0, 16)}</span>
        <span className="lbl">Protocol</span>{' '}
        <span className="val">
          r{table.protocol.minReaderVersion}/w{table.protocol.minWriterVersion}
        </span>
      </div>
      {table.protocol.features.length > 0 && (
        <div className="protocols">
          {table.protocol.features.map((f) => (
            <span key={f} className="proto-pill">
              {f}
            </span>
          ))}
        </div>
      )}
    </div>
  );
}

function formatRows(n) {
  if (n == null) return '';
  if (n < 1000) return n.toString();
  if (n < 1_000_000) return (n / 1000).toFixed(n < 10000 ? 1 : 0) + 'k';
  if (n < 1_000_000_000) return (n / 1_000_000).toFixed(n < 10_000_000 ? 1 : 0) + 'M';
  return (n / 1_000_000_000).toFixed(n < 10_000_000_000 ? 1 : 0) + 'B';
}

function formatBytes(b) {
  if (b == null) return '—';
  if (b < 1024) return b + ' B';
  if (b < 1024 ** 2) return (b / 1024).toFixed(1) + ' KB';
  if (b < 1024 ** 3) return (b / 1024 ** 2).toFixed(1) + ' MB';
  if (b < 1024 ** 4) return (b / 1024 ** 3).toFixed(1) + ' GB';
  return (b / 1024 ** 4).toFixed(1) + ' TB';
}

window.Sidebar = Sidebar;
window.formatRows = formatRows;
window.formatBytes = formatBytes;
