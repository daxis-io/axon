import { useQuery } from '@tanstack/react-query';
import { useMemo } from 'react';
import { catalogQueryOptions } from '../query/catalog.ts';
import { querySourceFromConnectedCatalogs } from '../services/query-source.ts';
import {
  selectActiveConnectedTableRef,
  selectAvailableConnectedCatalogs,
  useAxonClientStore,
} from '../state/hooks.ts';
import { catalogExplorerModel } from './catalog-navigation.ts';
import { IconChevR, IconDatabase, IconPlus, IconTable } from './components/icons.tsx';
import { formatRows } from './lib/format.ts';
import { navigate } from './router.tsx';

export function CatalogsPage() {
  const availableCatalogs = useAxonClientStore(selectAvailableConnectedCatalogs);
  const activeTable = useAxonClientStore(selectActiveConnectedTableRef);
  const model = useMemo(
    () => catalogExplorerModel(availableCatalogs, activeTable),
    [activeTable, availableCatalogs],
  );
  const querySource = useMemo(
    () => querySourceFromConnectedCatalogs(availableCatalogs, activeTable),
    [activeTable, availableCatalogs],
  );
  const { data: activeCatalog } = useQuery(catalogQueryOptions(querySource));

  return (
    <div className="cc-page catalogs-page">
      <header className="cc-page-bar">
        <button className="cc-page-brand" onClick={() => navigate('/')} title="Back to workspace">
          <span className="brand-mark">A</span>
          <span className="brand-name">
            axon <span>· web</span>
          </span>
        </button>

        <span className="cc-page-crumb">/ Catalogs</span>

        <div className="cc-page-spacer" />

        <button className="cc-btn" onClick={() => navigate('/connect')}>
          <IconPlus size={12} /> Connect
        </button>
        <button className="cc-btn" onClick={() => navigate('/')}>
          Workspace <IconChevR size={11} />
        </button>
      </header>

      <main className="catalogs-main">
        <section className="catalogs-summary">
          <div className="catalogs-title">
            <IconDatabase size={15} />
            <span>Catalogs</span>
          </div>
          <div className="catalogs-stats">
            <span>{model.catalogCount} catalogs</span>
            <span>{model.schemaCount} schemas</span>
            <span>
              {model.queryableTableCount}/{model.tableCount} queryable tables
            </span>
          </div>
          <div className="catalogs-active">
            <span className="lbl">Active</span>
            <span className="mono">{activeCatalog?.name ?? 'sample-lake'}</span>
            <span className="muted">{activeCatalog?.region ?? 'browser-local'}</span>
          </div>
        </section>

        {model.status === 'empty' ? (
          <section className="catalog-empty">
            <IconDatabase size={22} />
            <h1>No catalogs connected</h1>
            <button className="cc-btn primary" onClick={() => navigate('/connect')}>
              <IconPlus size={12} /> Connect a source
            </button>
          </section>
        ) : (
          <section className="catalog-explorer" aria-label="Connected catalogs">
            {model.catalogs.map((catalog) => (
              <article key={catalog.id} className="catalog-block">
                <header className="catalog-block-head">
                  <span className="catalog-glyph">{catalogGlyph(catalog.kind)}</span>
                  <div>
                    <h2>{catalog.alias}</h2>
                    <div className="catalog-storage">{catalog.storage}</div>
                  </div>
                  <span className="catalog-meta">
                    {catalog.schemas.length} schema{catalog.schemas.length === 1 ? '' : 's'}
                  </span>
                </header>

                {catalog.schemas.map((schema) => (
                  <div key={`${catalog.id}/${schema.name}`} className="catalog-schema">
                    <div className="catalog-schema-head">
                      <span>{schema.name}</span>
                      <span>
                        {schema.tableCount} table{schema.tableCount === 1 ? '' : 's'}
                      </span>
                    </div>
                    {schema.tables.map((table) => (
                      <button
                        key={table.key}
                        type="button"
                        className={'catalog-table-row ' + (table.active ? 'active ' : '')}
                        disabled={!table.queryable || !table.path}
                        title={
                          table.queryable
                            ? `${catalog.alias}.${schema.name}.${table.name}`
                            : 'This table is not queryable in this browser build yet'
                        }
                        onClick={() => table.path && navigate(table.path)}
                      >
                        <span className="ico">
                          <IconTable size={12} />
                        </span>
                        <span className="name">{table.name}</span>
                        <span className="storage">{table.storage}</span>
                        <span className="meta">v{table.snapshot ?? '—'}</span>
                        <span className="meta">
                          {table.rows != null ? formatRows(table.rows) : (table.size ?? '—')}
                        </span>
                        {table.active && <span className="active-label">active</span>}
                      </button>
                    ))}
                  </div>
                ))}
              </article>
            ))}
          </section>
        )}
      </main>
    </div>
  );
}

function catalogGlyph(kind: string | undefined): string {
  if (kind === 'local') return 'L';
  if (kind === 'unity_catalog') return 'UC';
  if (kind === 'delta_share') return 'DS';
  return 'OS';
}
