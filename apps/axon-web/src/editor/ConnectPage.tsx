// Full-page first-run experience at /connect. Wraps the multi-step connect
// flow with a production app chrome (brand + back link) and the
// design's empty-state illustration as the landing tile.

import { useCallback, useEffect, useState } from 'react';
import { ConnectModal } from './connect/ConnectModal.tsx';
import { ConnectedCatalogsPanel } from './connect/ConnectedCatalogs.tsx';
import type { SourceId } from './connect/data.ts';
import {
  buildCatalogFromResult,
  loadConnectedCatalogs,
  saveConnectedCatalogs,
} from './connect/store.ts';
import type { ConnectedCatalog, ConnectResult } from './connect/types.ts';
import { IconBolt, IconChevR, IconPlus } from './components/icons.tsx';
import { navigate } from './router.ts';
import { SERVER_QUERY_FALLBACK_ENABLED } from '../services/server-fallback.ts';

export function ConnectPage() {
  const [catalogs, setCatalogs] = useState<ConnectedCatalog[]>(() => loadConnectedCatalogs());
  const [freshId, setFreshId] = useState<string | null>(null);
  const [modalOpen, setModalOpen] = useState(false);
  const [modalStep, setModalStep] = useState<1 | 2 | 3>(1);
  const [modalSource, setModalSource] = useState<SourceId | null>(null);
  const [panelOpen, setPanelOpen] = useState(false);

  useEffect(() => {
    saveConnectedCatalogs(catalogs);
  }, [catalogs]);

  const open = useCallback((step: 1 | 2 | 3 = 1, src: SourceId | null = null) => {
    setModalStep(step);
    setModalSource(src);
    setModalOpen(true);
  }, []);

  const onConnect = useCallback((result: ConnectResult) => {
    const cat = buildCatalogFromResult(result);
    setCatalogs((cs) => [...cs, cat]);
    setFreshId(cat.id);
    setModalOpen(false);
    window.setTimeout(() => setFreshId(null), 4500);
  }, []);

  const tableCount = catalogs.reduce(
    (a, c) => a + c.schemas.reduce((b, s) => b + s.tables.length, 0),
    0,
  );

  return (
    <div className="cc-page">
      <header className="cc-page-bar">
        <button className="cc-page-brand" onClick={() => navigate('/')} title="Back to workspace">
          <span className="brand-mark">A</span>
          <span className="brand-name">
            axon <span>· web</span>
          </span>
        </button>

        <span className="cc-page-crumb">/ Connect</span>

        <div className="cc-page-spacer" />

        {catalogs.length > 0 && (
          <button className="cc-btn" onClick={() => setPanelOpen(true)}>
            {catalogs.length} connected
          </button>
        )}
        <button className="cc-btn" onClick={() => navigate('/')}>
          Back to workspace <IconChevR size={11} />
        </button>
      </header>

      <main className="cc-page-main">
        <div className="cc-page-hero">
          <div className="cc-page-illu" aria-hidden>
            <svg className="wire" viewBox="0 0 220 132" fill="none">
              <path d="M58,40 C88,40 100,68 120,72" strokeDasharray="3 3" />
              <path d="M114,82 C130,90 132,98 130,110" strokeDasharray="3 3" />
              <path d="M178,42 C160,52 148,60 138,68" strokeDasharray="3 3" />
            </svg>
            <div className="chip local">
              <span className="g">L</span> orders.parquet
            </div>
            <div className="chip os">
              <span className="g">OS</span> gs://acme-lake
            </div>
            <div className="chip uc">
              <span className="g">UC</span> main.analytics
            </div>
            <div className="chip ds">
              <span className="g">DS</span> acme-partner.share
            </div>
            <div className="chip flow">
              <IconBolt size={9} style={{ color: 'var(--accent)' }} /> SQL · Axon
            </div>
          </div>

          <h1>Connect a Delta source</h1>
          <p>
            Bring a local Delta folder, a cloud bucket, a Unity Catalog workspace, or a Delta
            Sharing endpoint into Axon. Each one slots in as a catalog you can browse and query — no
            data movement, no copies.
          </p>

          <div className="cc-page-actions">
            <button className="cc-btn primary lg" onClick={() => open(1)}>
              <IconPlus size={12} /> Connect a source
            </button>
            {catalogs.length > 0 && (
              <span className="cc-page-stat">
                {catalogs.length} catalog{catalogs.length === 1 ? '' : 's'} · {tableCount} table
                {tableCount === 1 ? '' : 's'}
              </span>
            )}
          </div>

          <div className="cc-page-quicklinks">
            Or jump straight to <a onClick={() => open(2, 'local')}>local folder</a>
            <span>·</span>
            <a onClick={() => open(2, 'object_store')}>cloud bucket</a>
            <span>·</span>
            <a onClick={() => open(2, 'unity_catalog')}>Unity Catalog</a>
            <span>·</span>
            <a onClick={() => open(2, 'delta_share')}>Delta Sharing</a>
          </div>
        </div>

        {catalogs.length > 0 && (
          <section className="cc-page-list">
            <h2>Recently connected</h2>
            <div className="cc-page-grid">
              {catalogs
                .slice()
                .reverse()
                .map((c) => (
                  <button
                    key={c.id}
                    className={'cc-page-card ' + (c.id === freshId ? 'fresh' : '')}
                    onClick={() => navigate('/')}
                  >
                    <span className={'glyph ' + glyphClass(c)}>{glyphLabel(c)}</span>
                    <div className="meta">
                      <div className="alias">{c.alias}</div>
                      <div className="storage">{c.storage}</div>
                      <div className="counts">
                        {c.schemas.length} schema{c.schemas.length === 1 ? '' : 's'} ·{' '}
                        {c.schemas.reduce((a, s) => a + s.tables.length, 0)} tables
                      </div>
                    </div>
                  </button>
                ))}
            </div>
          </section>
        )}
      </main>

      {modalOpen && (
        <ConnectModal
          initialStep={modalStep}
          initialSource={modalSource}
          serverFallbackEnabled={SERVER_QUERY_FALLBACK_ENABLED}
          onClose={() => setModalOpen(false)}
          onConnect={onConnect}
        />
      )}

      {panelOpen && (
        <ConnectedCatalogsPanel
          catalogs={catalogs}
          freshId={freshId}
          onAdd={() => {
            setPanelOpen(false);
            open(1);
          }}
          onRemove={(id) => setCatalogs((cs) => cs.filter((c) => c.id !== id))}
          onClose={() => setPanelOpen(false)}
        />
      )}
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
