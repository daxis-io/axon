// Full-page first-run experience at /connect. Wraps the multi-step connect
// flow with a production app chrome (brand + back link) and the
// design's empty-state illustration as the landing tile.

import { useQueryClient } from '@tanstack/react-query';
import { Suspense, lazy, useCallback, useState } from 'react';
import { purgeCatalogSourcesCache } from '../query/catalog.ts';
import { ConnectedCatalogsPanel } from './connect/ConnectedCatalogs.tsx';
import { availabilityForSource, type SourceId } from './connect/data.ts';
import type { ConnectedCatalog, ConnectResult } from './connect/types.ts';
import { IconBolt, IconChevR, IconPlus, IconSettings } from './components/icons.tsx';
import { navigate } from './router.tsx';
import { CONNECTOR_FEATURES } from '../services/connector-features.ts';
import { SERVER_QUERY_FALLBACK_ENABLED } from '../services/server-fallback.ts';
import {
  selectAvailableConnectedCatalogs,
  selectConnectionActions,
  selectFreshCatalogId,
  useAxonClientStore,
} from '../state/hooks.ts';
import type { ConnectionMutationResult } from '../state/slices/connections.ts';
import type { QueryTableSource } from '../services/query-source.ts';

const ConnectModal = lazy(() =>
  import('./connect/ConnectModal.tsx').then((module) => ({ default: module.ConnectModal })),
);

export function ConnectPage() {
  const availableCatalogs = useAxonClientStore(selectAvailableConnectedCatalogs);
  const queryClient = useQueryClient();
  const freshId = useAxonClientStore(selectFreshCatalogId);
  const connectionActions = useAxonClientStore(selectConnectionActions);
  const [modalOpen, setModalOpen] = useState(false);
  const [modalStep, setModalStep] = useState<1 | 2 | 3>(1);
  const [modalSource, setModalSource] = useState<SourceId | null>(null);
  const [panelOpen, setPanelOpen] = useState(false);

  const open = useCallback((step: 1 | 2 | 3 = 1, src: SourceId | null = null) => {
    setModalStep(step);
    setModalSource(src);
    setModalOpen(true);
  }, []);

  const onConnect = useCallback(
    (result: ConnectResult) => {
      const mutation = connectionActions.connect(result);
      applyConnectPageMutationSideEffects(
        mutation,
        'failed to unregister duplicate local Delta catalog:',
        {
          purgeCatalogSources: (sources) => purgeCatalogSourcesCache(queryClient, sources),
        },
      );
      setModalOpen(false);
      window.setTimeout(() => connectionActions.clearFreshCatalogId(), 4500);
    },
    [connectionActions, queryClient],
  );

  const removeCatalog = useCallback(
    (id: string) => {
      const mutation = connectionActions.removeCatalog(id);
      applyConnectPageMutationSideEffects(mutation, 'failed to unregister local Delta catalog:', {
        purgeCatalogSources: (sources) => purgeCatalogSourcesCache(queryClient, sources),
      });
    },
    [connectionActions, queryClient],
  );

  const tableCount = availableCatalogs.reduce(
    (a, c) => a + c.schemas.reduce((b, s) => b + s.tables.length, 0),
    0,
  );
  const unityCatalogAvailability = availabilityForSource('unity_catalog', CONNECTOR_FEATURES);
  const deltaSharingAvailability = availabilityForSource('delta_share', CONNECTOR_FEATURES);

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

        {availableCatalogs.length > 0 && (
          <button className="cc-btn" onClick={() => setPanelOpen(true)}>
            {availableCatalogs.length} connected
          </button>
        )}
        <button className="cc-btn" onClick={() => navigate('/settings')}>
          <IconSettings size={12} /> Settings
        </button>
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
            Axon ships with a browser-local sample catalog. Object storage sources should resolve
            snapshots in the browser; governed UC and Delta Sharing flows can still use brokered
            contracts when policy requires them.
          </p>

          <div className="cc-page-actions">
            <button className="cc-btn primary lg" onClick={() => open(1)}>
              <IconPlus size={12} /> Connect a source
            </button>
            {availableCatalogs.length > 0 && (
              <span className="cc-page-stat">
                {availableCatalogs.length} catalog{availableCatalogs.length === 1 ? '' : 's'} ·{' '}
                {tableCount} table{tableCount === 1 ? '' : 's'}
              </span>
            )}
          </div>

          <div className="cc-page-quicklinks">
            Or jump straight to{' '}
            <button type="button" onClick={() => open(2, 'local')}>
              local folder
            </button>
            <span>·</span>
            <button type="button" onClick={() => open(2, 'object_store')}>
              cloud bucket
            </button>
            <span>·</span>
            <button
              type="button"
              disabled={!unityCatalogAvailability.enabled}
              title={unityCatalogAvailability.enabled ? undefined : unityCatalogAvailability.reason}
              onClick={() => open(2, 'unity_catalog')}
            >
              Unity Catalog
            </button>
            <span>·</span>
            <button
              type="button"
              disabled={!deltaSharingAvailability.enabled}
              title={deltaSharingAvailability.enabled ? undefined : deltaSharingAvailability.reason}
              onClick={() => open(2, 'delta_share')}
            >
              Delta Sharing
            </button>
          </div>
        </div>

        {availableCatalogs.length > 0 && (
          <section className="cc-page-list">
            <h2>Recently connected</h2>
            <div className="cc-page-grid">
              {availableCatalogs
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
        <Suspense fallback={null}>
          <ConnectModal
            initialStep={modalStep}
            initialSource={modalSource}
            serverFallbackEnabled={SERVER_QUERY_FALLBACK_ENABLED}
            connectorFeatures={CONNECTOR_FEATURES}
            onClose={() => setModalOpen(false)}
            onConnect={onConnect}
          />
        </Suspense>
      )}

      {panelOpen && (
        <ConnectedCatalogsPanel
          catalogs={availableCatalogs}
          freshId={freshId}
          onAdd={() => {
            setPanelOpen(false);
            open(1);
          }}
          onRemove={removeCatalog}
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

type ConnectPageMutationSideEffectOptions = {
  discardActiveQuerySession?: () => void;
  purgeCatalogSources?: (sources: QueryTableSource[]) => void;
  unregisterLocalDeltaRuntimeIds?: (registryIds: string[], message: string) => void;
};

export function applyConnectPageMutationSideEffects(
  mutation: Pick<
    ConnectionMutationResult,
    'discardedSources' | 'localRegistryIdsToUnregister' | 'shouldDiscardActiveQuerySession'
  >,
  unregisterMessage: string,
  options: ConnectPageMutationSideEffectOptions = {},
): void {
  if (mutation.shouldDiscardActiveQuerySession) {
    (options.discardActiveQuerySession ?? discardActiveQuerySession)();
  }

  if (mutation.discardedSources.length > 0) {
    options.purgeCatalogSources?.(mutation.discardedSources);
  }

  if (mutation.localRegistryIdsToUnregister.length > 0) {
    (options.unregisterLocalDeltaRuntimeIds ?? unregisterLocalDeltaRuntimeIds)(
      mutation.localRegistryIdsToUnregister,
      unregisterMessage,
    );
  }
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
