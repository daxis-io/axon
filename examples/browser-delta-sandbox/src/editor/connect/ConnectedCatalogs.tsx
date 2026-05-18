// Floating panel that lists connected catalogs and lets the user manage them.
// Anchors to the connection pill in the top bar.

import { useState, type MouseEvent } from 'react';
import { IconChevR, IconClose, IconRefresh, IconTable } from '../components/icons.tsx';
import { IconCog, IconDots } from './icons.tsx';
import type { ConnectedCatalog } from './types.ts';

type Props = {
  catalogs: ConnectedCatalog[];
  freshId: string | null;
  onAdd: () => void;
  onRemove: (id: string) => void;
  onClose: () => void;
};

export function ConnectedCatalogsPanel({ catalogs, freshId, onAdd, onRemove, onClose }: Props) {
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
      <div
        className="cc-modal"
        role="dialog"
        aria-label="Connected catalogs"
        style={{ height: 'min(560px, 88vh)', width: 'min(720px, 92vw)' }}
      >
        <header className="cc-head">
          <div>
            <h3>Connected catalogs</h3>
            <div className="sub">
              {catalogs.length} catalog{catalogs.length === 1 ? '' : 's'} reachable from this
              workspace.
            </div>
          </div>
          <button className="cc-btn" onClick={onAdd}>
            + Connect new source
          </button>
          <button className="cc-x" onClick={onClose} title="Close (Esc)">
            <IconClose size={13} />
          </button>
        </header>

        <div className="cc-body" style={{ position: 'relative', padding: 0 }}>
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
                      className={'cc-cat-head ' + (open ? 'open' : '')}
                      onClick={() => setOpenCats({ ...openCats, [cat.id]: !open })}
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
                          {sch.tables.map((tbl) => (
                            <div key={tbl.name} className="cc-tbl-row">
                              <span className="ico">
                                <IconTable size={11} />
                              </span>
                              <span className="name">{tbl.name}</span>
                              <span className="v">v{tbl.snapshot}</span>
                              <span className="rc">{tbl.size}</span>
                            </div>
                          ))}
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
                ? 'Unity Catalog (Databricks)'
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
          <span className="l">Authentication</span>
          <span className="v">
            {catalog.kind === 'local'
              ? 'file:// · read-only'
              : catalog.kind === 'unity_catalog'
                ? 'PAT · encrypted'
                : catalog.kind === 'delta_share'
                  ? 'Bearer token · encrypted'
                  : 'Service account · encrypted'}
          </span>
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
            <IconCog size={11} /> Edit auth
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
