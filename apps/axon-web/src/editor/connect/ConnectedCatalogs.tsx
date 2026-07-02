// Floating panel that lists connected catalogs and lets the user manage them.
// Anchors to the connection pill in the top bar.

import { useState, type KeyboardEvent, type MouseEvent } from 'react';
import { IconChevR, IconClose, IconRefresh, IconTable } from '../components/icons.tsx';
import type { ActiveConnectedTableRef } from '../../services/query-source.ts';
import { isQueryableCatalogTable } from '../catalog-navigation.ts';
import { IconCog, IconDots } from './icons.tsx';
import type { ConnectedCatalog } from './types.ts';

type Props = {
  catalogs: ConnectedCatalog[];
  activeTable?: ActiveConnectedTableRef;
  freshId: string | null;
  onActivate?: (table: ActiveConnectedTableRef) => void;
  onAdd: () => void;
  onRemove: (id: string) => void;
  onClose: () => void;
};

export function ConnectedCatalogsPanel({
  catalogs,
  activeTable,
  freshId,
  onActivate,
  onAdd,
  onRemove,
  onClose,
}: Props) {
  const [openCats, setOpenCats] = useState<Record<string, boolean>>(() => {
    const o: Record<string, boolean> = {};
    catalogs.forEach((c, i) => {
      o[c.id] = i === 0 || c.id === freshId;
    });
    return o;
  });
  const [manage, setManage] = useState<ConnectedCatalog | null>(null);

  return (
    <div
      className="cc-overlay"
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className="cc-modal cc-modal--compact" role="dialog" aria-label="Connected catalogs">
        <header className="cc-head">
          <div className="cc-head-title">
            <h3>Connected catalogs</h3>
            <div className="sub">
              {catalogs.length} catalog{catalogs.length === 1 ? '' : 's'} reachable from this
              workspace.
            </div>
          </div>
          <button className="cc-btn cc-head-action" onClick={onAdd}>
            <span aria-hidden="true">+</span> Connect new source
          </button>
          <button className="cc-x" onClick={onClose} title="Close (Esc)">
            <IconClose size={13} />
          </button>
        </header>

        <div className="cc-body cc-body--list">
          {catalogs.length === 0 ? (
            <div className="cc-no-catalogs">
              <div>No catalogs yet.</div>
              <button className="cc-btn primary" onClick={onAdd}>
                Connect your first source
              </button>
            </div>
          ) : (
            <div className="cc-catalog-list">
              {catalogs.map((cat) => {
                const open = !!openCats[cat.id];
                return (
                  <div key={cat.id} className={'cc-cat ' + (cat.id === freshId ? 'fresh ' : '')}>
                    <div
                      role="button"
                      tabIndex={0}
                      className={'cc-cat-head ' + (open ? 'open' : '')}
                      onClick={() => setOpenCats({ ...openCats, [cat.id]: !open })}
                      onKeyDown={(event: KeyboardEvent<HTMLDivElement>) => {
                        if (event.key !== 'Enter' && event.key !== ' ') return;
                        event.preventDefault();
                        setOpenCats({ ...openCats, [cat.id]: !open });
                      }}
                      aria-expanded={open}
                      aria-label={`${open ? 'Collapse' : 'Expand'} ${cat.alias}`}
                    >
                      <span className="twist">
                        <IconChevR size={9} />
                      </span>
                      <span className={'glyph ' + glyphClass(cat)}>{glyphLabel(cat)}</span>
                      <div className="alias-block">
                        <div className="alias">{cat.alias}</div>
                        <div className="storage">{cat.storage}</div>
                      </div>
                      <span className="status-dot" title="connected" />
                      <button
                        className="more"
                        onClick={(e: MouseEvent) => {
                          e.stopPropagation();
                          setManage(cat);
                        }}
                        title="Manage connection"
                      >
                        <IconDots size={11} />
                      </button>
                    </div>
                    {open &&
                      cat.schemas.map((sch) => (
                        <div key={sch.name} className="cc-sch-block">
                          <div className="cc-sch-row">
                            <span className="name">{sch.name}</span>
                            <span className="count">
                              {sch.tables.length} table{sch.tables.length === 1 ? '' : 's'}
                            </span>
                          </div>
                          {sch.tables.map((tbl) => {
                            const ref = {
                              catalogId: cat.id,
                              schemaName: sch.name,
                              tableName: tbl.name,
                            };
                            const isActive =
                              activeTable?.catalogId === cat.id &&
                              activeTable.schemaName === sch.name &&
                              activeTable.tableName === tbl.name;
                            const queryable = isQueryableCatalogTable(catalogs, ref);
                            return (
                              <button
                                key={tbl.name}
                                type="button"
                                className={'cc-tbl-row ' + (isActive ? 'active ' : '')}
                                disabled={!queryable}
                                aria-pressed={isActive}
                                aria-label={`Activate ${cat.alias} ${sch.name} ${tbl.name}`}
                                title={
                                  queryable
                                    ? `Query ${cat.alias}.${sch.name}.${tbl.name}`
                                    : 'This table is not queryable in this browser build yet'
                                }
                                onClick={(event) => {
                                  event.stopPropagation();
                                  if (!queryable) return;
                                  onActivate?.(ref);
                                  onClose();
                                }}
                              >
                                <span className="ico">
                                  <IconTable size={11} />
                                </span>
                                <span className="name">{tbl.name}</span>
                                <span className="v">v{tbl.snapshot}</span>
                                <span className="rc">{tbl.size}</span>
                                {isActive && <span className="active-label">active</span>}
                              </button>
                            );
                          })}
                        </div>
                      ))}
                  </div>
                );
              })}
            </div>
          )}

          {manage && (
            <ManageDrawer
              catalog={manage}
              onClose={() => setManage(null)}
              onRemove={() => {
                onRemove(manage.id);
                setManage(null);
              }}
            />
          )}
        </div>
      </div>
    </div>
  );
}

function glyphLabel(c: ConnectedCatalog) {
  if (c.kind === 'local') return 'L';
  if (c.kind === 'unity_catalog') return 'UC';
  if (c.kind === 'delta_share') return 'DS';
  return (c.provider || 'OS').toUpperCase();
}
function glyphClass(c: ConnectedCatalog) {
  if (c.kind === 'local') return 'local';
  if (c.kind === 'unity_catalog') return 'uc';
  if (c.kind === 'delta_share') return 'ds';
  return c.provider || 'gcs';
}

function ManageDrawer({
  catalog,
  onClose,
  onRemove,
}: {
  catalog: ConnectedCatalog;
  onClose: () => void;
  onRemove: () => void;
}) {
  const owners = runtimeOwnersFor(catalog);

  return (
    <div className="cc-manage-drawer">
      <div className="hdr">
        <span className={'glyph ' + glyphClass(catalog)}>{glyphLabel(catalog)}</span>
        <span>{catalog.alias}</span>
        <button className="cc-x" onClick={onClose} title="Close drawer">
          <IconClose size={13} />
        </button>
      </div>
      <div className="bd">
        <div className="field-row">
          <span className="l">Type</span>
          <span className="v">
            {catalog.kind === 'local'
              ? 'Local files'
              : catalog.kind === 'unity_catalog'
                ? 'Unity Catalog (brokered)'
                : catalog.kind === 'delta_share'
                  ? 'Delta Sharing'
                  : 'Object storage (' + (catalog.provider || '').toUpperCase() + ')'}
          </span>
        </div>
        <div className="field-row">
          <span className="l">Storage</span>
          <span className="v">{catalog.storage || catalog.host || catalog.path}</span>
        </div>
        <div className="field-row">
          <span className="l">Region</span>
          <span className="v">{catalog.region || 'auto'}</span>
        </div>
        <div className="field-row">
          <span className="l">Schemas</span>
          <span className="v">{catalog.schemas.length}</span>
        </div>
        <div className="field-row">
          <span className="l">Tables</span>
          <span className="v">{catalog.schemas.reduce((a, s) => a + s.tables.length, 0)}</span>
        </div>
        <div className="field-row">
          <span className="l">Access</span>
          <span className="v">{owners.access}</span>
        </div>
        <div className="field-row">
          <span className="l">Snapshot</span>
          <span className="v">{owners.snapshot}</span>
        </div>
        <div className="field-row">
          <span className="l">Query</span>
          <span className="v">{owners.query}</span>
        </div>
        <div className="field-row">
          <span className="l">Connected</span>
          <span className="v">{catalog.connectedAt || 'just now'}</span>
        </div>

        <div style={{ display: 'flex', gap: 8, marginTop: 18 }}>
          <button className="cc-btn" style={{ flex: 1 }}>
            <IconRefresh size={11} /> Resync
          </button>
          <button className="cc-btn" style={{ flex: 1 }}>
            <IconCog size={11} /> Edit session
          </button>
        </div>
        <button
          className="cc-btn danger"
          style={{ marginTop: 8, width: '100%' }}
          onClick={onRemove}
        >
          <IconClose size={11} /> Disconnect catalog
        </button>
      </div>
    </div>
  );
}

function runtimeOwnersFor(catalog: ConnectedCatalog) {
  if (catalog.kind === 'local') {
    return { access: 'Browser', snapshot: 'Browser', query: 'Browser' };
  }
  if (catalog.kind === 'unity_catalog') {
    return { access: 'UC brokered', snapshot: 'Browser', query: 'Browser' };
  }
  if (catalog.kind === 'delta_share') {
    return {
      access: 'Provider brokered',
      snapshot: 'Browser materialized',
      query: 'Browser',
    };
  }
  return {
    access: catalog.region === 'browser-local' ? 'Browser' : 'Brokered',
    snapshot: 'Browser',
    query: 'Browser',
  };
}
