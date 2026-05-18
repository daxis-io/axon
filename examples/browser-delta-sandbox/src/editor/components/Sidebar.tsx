import { Fragment, useState, type MouseEvent } from 'react';
import type { Catalog, CatalogTable, HistoryEntry, SavedQuery } from '../../services/types.ts';
import { formatBytes, formatRows } from '../lib/format.ts';
import {
  IconBolt,
  IconBookmark,
  IconChevR,
  IconColumn,
  IconDatabase,
  IconKey,
  IconSearch,
  IconTable,
} from './icons.tsx';

type SidebarTab = 'catalog' | 'saved' | 'history';

type SidebarProps = {
  catalog: Catalog | undefined;
  saved: SavedQuery[];
  history: HistoryEntry[];
  width: number;
  onInsert: (text: string) => void;
  onResize: (e: MouseEvent) => void;
  onPickTable?: (table: CatalogTable) => void;
};

export function Sidebar({
  catalog,
  saved,
  history,
  width,
  onInsert,
  onResize,
  onPickTable,
}: SidebarProps) {
  const [tab, setTab] = useState<SidebarTab>('catalog');
  const [query, setQuery] = useState('');
  const [openTbl, setOpenTbl] = useState<Record<string, boolean>>({});
  const [hoverTbl, setHoverTbl] = useState<string | null>(null);

  const filteredTables = (catalog?.tables ?? []).filter(
    (t) =>
      !query ||
      t.name.toLowerCase().includes(query.toLowerCase()) ||
      t.columns.some((c) => c.name.toLowerCase().includes(query.toLowerCase())),
  );

  return (
    <aside className="sidebar" style={{ width }}>
      <div className="sb-tabs">
        {(['catalog', 'saved', 'history'] as const).map((tt) => (
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
              <span>{catalog?.name ?? 'loading…'}</span>
              <span className="pill">{catalog ? '● connected' : '○ resolving'}</span>
            </div>
            <div className="row2" title={catalog?.storage}>
              {catalog ? `${catalog.storage} · ${catalog.region}` : 'snapshot bootstrap…'}
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
            <div className="sb-section">Delta tables · {catalog?.tables.length ?? 0}</div>
            {filteredTables.map((t) => {
              const open = !!openTbl[t.name];
              return (
                <Fragment key={t.name}>
                  <div
                    className={'sb-row tbl ' + (open ? 'open ' : '')}
                    onClick={() => {
                      setOpenTbl({ ...openTbl, [t.name]: !open });
                      onPickTable?.(t);
                    }}
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
                          {c.role === 'partition' ? (
                            <IconKey size={11} />
                          ) : (
                            <IconColumn size={11} />
                          )}
                        </span>
                        <span>{c.name}</span>
                        {c.pk && <span className="badge pk">PK</span>}
                        {c.fk && <span className="badge fk">FK</span>}
                        {c.role === 'partition' && (
                          <span
                            className="badge"
                            style={{
                              background: 'var(--warning-soft)',
                              color: 'var(--warning)',
                            }}
                          >
                            PART
                          </span>
                        )}
                        <span className="type">{c.type}</span>
                      </div>
                    ))}
                </Fragment>
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

          {hoverTbl && (
            <TableDetailFlyout
              table={catalog?.tables.find((t) => t.name === hoverTbl)}
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
            {saved.length === 0 && (
              <div
                style={{
                  padding: '12px 12px',
                  color: 'var(--ink-4)',
                  fontSize: 12,
                }}
              >
                No saved queries yet.
              </div>
            )}
            {saved.map((s) => (
              <div key={s.id} className="sb-saved-item">
                <span className="ico" style={{ color: 'var(--accent)' }}>
                  <IconBookmark size={13} />
                </span>
                <div style={{ minWidth: 0, flex: 1 }}>
                  <div
                    className="name"
                    style={{
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                      whiteSpace: 'nowrap',
                    }}
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
          {history.length === 0 && (
            <div
              style={{
                padding: '12px 12px',
                color: 'var(--ink-4)',
                fontSize: 12,
              }}
            >
              No queries run yet.
            </div>
          )}
          {history.map((h) => (
            <div
              key={h.id}
              className="sb-saved-item"
              style={{ alignItems: 'flex-start', flexDirection: 'column', gap: 4 }}
            >
              <div
                style={{
                  display: 'flex',
                  gap: 6,
                  width: '100%',
                  alignItems: 'center',
                }}
              >
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
                  style={{
                    marginLeft: 'auto',
                    color: 'var(--ink-4)',
                    font: '11px var(--mono)',
                  }}
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

function TableDetailFlyout({ table, left }: { table: CatalogTable | undefined; left: number }) {
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
        <span className="lbl">Rows</span>
        <span className="val">{table.row_count.toLocaleString()}</span>
        <span className="lbl">Files</span>
        <span className="val">{table.file_count.toLocaleString()}</span>
        <span className="lbl">Row groups</span>
        <span className="val">{table.row_group_count.toLocaleString()}</span>
        <span className="lbl">Size</span>
        <span className="val">{formatBytes(table.size_bytes)}</span>
        <span className="lbl">Partitions</span>
        <span className="val">
          {table.partition_columns.length
            ? table.partition_columns.map((p) => `${p.name}:${p.type}`).join(', ')
            : '—'}
        </span>
        <span className="lbl">Protocol</span>
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
