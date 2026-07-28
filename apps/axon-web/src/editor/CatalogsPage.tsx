import { useMemo } from 'react';
import type { ActiveConnectedTableRef } from '../services/query-source.ts';
import { selectAvailableConnectedCatalogs, useAxonClientStore } from '../state/hooks.ts';
import {
  catalogExplorerModel,
  catalogExplorerTableDetail,
  type CatalogExplorerTableDetail,
} from './catalog-navigation.ts';
import { IconChevR, IconDatabase, IconPlus, IconSettings, IconTable } from './components/icons.tsx';
import { formatBytes, formatRows } from './lib/format.ts';
import { navigate } from './router.tsx';

export function CatalogsPage({ routeTable }: { routeTable?: ActiveConnectedTableRef } = {}) {
  const availableCatalogs = useAxonClientStore(selectAvailableConnectedCatalogs);
  const model = useMemo(
    () => catalogExplorerModel(availableCatalogs, routeTable),
    [availableCatalogs, routeTable],
  );
  const detail = useMemo(
    () => catalogExplorerTableDetail(availableCatalogs, routeTable),
    [availableCatalogs, routeTable],
  );

  return (
    <div className="cc-page catalogs-page">
      <header className="cc-page-bar">
        <button className="cc-page-brand" onClick={() => navigate('/')} title="Back to workspace">
          <span className="brand-mark">A</span>
          <span className="brand-name">
            axon <span>· web</span>
          </span>
        </button>

        <span className="cc-page-crumb">/ Catalog Explorer</span>

        <div className="cc-page-spacer" />

        <button className="cc-btn" onClick={() => navigate('/connect')}>
          <IconPlus size={12} /> Connect
        </button>
        <button className="cc-btn" onClick={() => navigate('/settings')}>
          <IconSettings size={12} /> Settings
        </button>
        <button className="cc-btn" onClick={() => navigate('/')}>
          Workspace <IconChevR size={11} />
        </button>
      </header>

      <main className="catalogs-main">
        <section className="catalogs-summary" aria-labelledby="catalog-explorer-heading">
          <div className="catalogs-title">
            <IconDatabase size={15} />
            <h1 id="catalog-explorer-heading">Catalog Explorer</h1>
          </div>
          <div className="catalogs-stats">
            <span>{model.catalogCount} connections</span>
            <span>{model.schemaCount} schemas</span>
            <span>
              {model.queryableTableCount}/{model.tableCount} queryable tables
            </span>
          </div>
        </section>

        {model.status === 'empty' ? (
          <section className="catalog-empty">
            <IconDatabase size={22} />
            <h2>No connections available</h2>
            <p>Connect a local or public Delta source to browse its generated catalog metadata.</p>
            <button className="cc-btn primary" onClick={() => navigate('/connect')}>
              <IconPlus size={12} /> Connect a source
            </button>
          </section>
        ) : (
          <div className="catalog-explorer-layout">
            <section className="catalog-explorer" aria-label="Connected catalog resources">
              {model.catalogs.map((catalog) => (
                <article key={catalog.id} className="catalog-block">
                  <header className="catalog-block-head">
                    <span className="catalog-glyph">{catalogGlyph(catalog.kind)}</span>
                    <div>
                      <h2>{catalog.alias}</h2>
                      <div className="catalog-node-label">
                        Catalog · {catalog.catalogName ?? catalog.alias}
                      </div>
                      <div className="catalog-storage">{catalog.storage}</div>
                    </div>
                    <span className="catalog-meta">
                      {catalog.schemas.length} schema{catalog.schemas.length === 1 ? '' : 's'}
                    </span>
                  </header>

                  {catalog.schemas.map((schema) => (
                    <div key={`${catalog.id}/${schema.name}`} className="catalog-schema">
                      <div className="catalog-schema-head">
                        <span>Schema · {schema.name}</span>
                        <span>
                          {schema.tableCount} resource{schema.tableCount === 1 ? '' : 's'}
                        </span>
                      </div>
                      {schema.tables.map((table) => (
                        <button
                          key={table.key}
                          type="button"
                          className={'catalog-table-row ' + (table.active ? 'active ' : '')}
                          disabled={!table.path}
                          aria-current={table.active ? 'page' : undefined}
                          aria-disabled={!table.path}
                          title={`${catalog.alias}.${schema.name}.${table.name} · ${table.tableKind}`}
                          onClick={() => table.path && navigate(table.path)}
                        >
                          <span className="ico">
                            <IconTable size={12} />
                          </span>
                          <span className="name">{table.name}</span>
                          <span className="kind">{table.tableKind}</span>
                          <span className="storage">{table.storage}</span>
                          <span className="meta">v{table.snapshot ?? '—'}</span>
                          <span className="meta">
                            {table.rows != null ? formatRows(table.rows) : (table.size ?? '—')}
                          </span>
                          {table.active && <span className="active-label">selected</span>}
                        </button>
                      ))}
                    </div>
                  ))}
                </article>
              ))}
            </section>

            <CatalogTableDetail detail={detail} />
          </div>
        )}
      </main>
    </div>
  );
}

function CatalogTableDetail({ detail }: { detail: CatalogExplorerTableDetail }) {
  if (detail.status === 'no_selection') {
    return (
      <aside className="catalog-detail catalog-detail-empty" aria-label="Catalog resource detail">
        <IconTable size={22} />
        <h2>Select a table or view</h2>
        <p>Choose an exact logical resource to inspect its generated metadata.</p>
      </aside>
    );
  }
  if (detail.status === 'unavailable') {
    return (
      <aside className="catalog-detail catalog-detail-empty" aria-label="Catalog resource detail">
        <h2>Selected resource is stale</h2>
        <p>The exact logical table is no longer reported by its connection.</p>
      </aside>
    );
  }
  if (detail.status === 'metadata_unavailable') {
    return (
      <aside className="catalog-detail catalog-detail-empty" aria-label="Catalog resource detail">
        <span className="catalog-kind-badge">{detail.tableKind}</span>
        <h2>Metadata unavailable</h2>
        <p>
          {detail.reason === 'invalid'
            ? 'The persisted generated metadata did not validate for this exact resource.'
            : 'This connection did not report generated metadata for the selected resource.'}
        </p>
        {!detail.queryable && <p>This resource cannot be opened in the SQL editor.</p>}
      </aside>
    );
  }

  return (
    <aside className="catalog-detail" aria-label="Catalog resource detail">
      <header className="catalog-detail-head">
        <div>
          <div className="catalog-detail-breadcrumb">
            {detail.connectionAlias} / {detail.catalogName} / {detail.schemaName}
          </div>
          <h2>{detail.tableName}</h2>
          {detail.comment && <p>{detail.comment}</p>}
        </div>
        <span className="catalog-kind-badge">{detail.tableKind}</span>
        {detail.sqlPath && (
          <button
            className="cc-btn primary catalog-open-sql"
            onClick={() => {
              if (detail.sqlPath) navigate(detail.sqlPath);
            }}
          >
            Open in SQL editor
          </button>
        )}
      </header>

      {!detail.queryable && (
        <div className="catalog-detail-notice">
          This {detail.tableKind.toLowerCase()} is browseable but not queryable in this browser
          build.
        </div>
      )}

      <section className="catalog-detail-section" aria-labelledby="catalog-overview-heading">
        <h3 id="catalog-overview-heading">Overview</h3>
        <dl className="catalog-overview">
          <OverviewItem label="Storage" value={detail.overview.storageLocation} mono />
          <OverviewItem label="Snapshot" value={reported(detail.overview.snapshot)} />
          <OverviewItem
            label="Rows"
            value={
              detail.overview.rows === undefined ? 'Not reported' : formatRows(detail.overview.rows)
            }
          />
          <OverviewItem label="Files" value={reported(detail.overview.files)} />
          <OverviewItem
            label="Size"
            value={
              detail.overview.sizeBytes === undefined
                ? 'Not reported'
                : formatBytes(detail.overview.sizeBytes)
            }
          />
          <OverviewItem label="Protocol" value={detail.overview.protocol} />
          <OverviewItem
            label="Features"
            value={detail.overview.features.join(', ') || 'None reported'}
          />
          <OverviewItem
            label="Partitions"
            value={detail.overview.partitions.join(', ') || 'Unpartitioned'}
          />
        </dl>
      </section>

      <section className="catalog-detail-section" aria-labelledby="catalog-columns-heading">
        <h3 id="catalog-columns-heading">Columns</h3>
        {detail.columnsStatus === 'empty' ? (
          <p className="catalog-columns-empty">
            No columns were reported by this catalog provider.
          </p>
        ) : (
          <div className="catalog-columns-scroll">
            <table className="catalog-columns" aria-label={`Columns for ${detail.tableName}`}>
              <thead>
                <tr>
                  <th scope="col">Name</th>
                  <th scope="col">Type</th>
                  <th scope="col">Nullable</th>
                  <th scope="col">Partition</th>
                  <th scope="col">Comment</th>
                </tr>
              </thead>
              <tbody>
                {detail.columns.map((column) => (
                  <tr key={column.name}>
                    <th scope="row">{column.name}</th>
                    <td>{column.type}</td>
                    <td>{column.nullable ? 'Yes' : 'No'}</td>
                    <td>{column.partition ? 'Yes' : 'No'}</td>
                    <td>{column.comment ?? '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </aside>
  );
}

function OverviewItem({
  label,
  value,
  mono = false,
}: {
  label: string;
  value: string;
  mono?: boolean;
}) {
  return (
    <div>
      <dt>{label}</dt>
      <dd className={mono ? 'mono' : undefined}>{value}</dd>
    </div>
  );
}

function reported(value: number | undefined): string {
  return value === undefined ? 'Not reported' : value.toLocaleString();
}

function catalogGlyph(kind: string | undefined): string {
  if (kind === 'local') return 'L';
  if (kind === 'unity_catalog') return 'UC';
  if (kind === 'delta_share') return 'DS';
  return 'OS';
}
