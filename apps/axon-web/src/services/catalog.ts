import type { Catalog, CatalogTable } from './types.ts';
import { deriveCatalogTable, getCurrentSession, getSession, subscribeSession } from './query.ts';
import { SAMPLE_QUERY_SOURCE, sameQuerySource, type QueryTableSource } from './query-source.ts';

export async function loadCatalog(
  source: QueryTableSource = SAMPLE_QUERY_SOURCE,
): Promise<Catalog> {
  try {
    const state = await getSession(source);
    return buildCatalog(deriveCatalogTable(state), state.source);
  } catch (error) {
    if (source.kind === 'local_delta') return buildLocalDeltaCatalogFallback(source);
    throw error;
  }
}

export function snapshotCatalog(
  source: QueryTableSource = SAMPLE_QUERY_SOURCE,
): Catalog | undefined {
  const state = getCurrentSession(source);
  if (!state) return undefined;
  return buildCatalog(deriveCatalogTable(state), state.source);
}

export function subscribeCatalog(
  listener: (catalog: Catalog) => void,
  source: QueryTableSource = SAMPLE_QUERY_SOURCE,
): () => void {
  return subscribeSession((state) => {
    if (sameQuerySource(state.source, source)) {
      listener(buildCatalog(deriveCatalogTable(state), state.source));
    }
  });
}

function buildCatalog(table: CatalogTable, source: QueryTableSource): Catalog {
  return {
    name: source.catalogName,
    region: source.region,
    storage: source.storage,
    tables: [table],
  };
}

function buildLocalDeltaCatalogFallback(
  source: Extract<QueryTableSource, { kind: 'local_delta' }>,
): Catalog {
  return {
    name: source.catalogName,
    region: source.region,
    storage: source.storage,
    tables: [
      {
        name: source.tableName,
        uri: source.storage,
        kind: 'delta',
        snapshot: source.snapshot ?? 0,
        size_bytes: 0,
        row_count: source.rows ?? 0,
        file_count: source.files ?? 0,
        row_group_count: 0,
        partition_columns: [],
        protocol: protocolFromLabel(source.protocol),
        columns: [],
      },
    ],
  };
}

function protocolFromLabel(label: string | undefined) {
  const match = /^r(\d+)\/w(\d+)$/.exec(label ?? '');
  return {
    minReaderVersion: match ? Number(match[1]) : 0,
    minWriterVersion: match ? Number(match[2]) : 0,
    features: [],
  };
}
