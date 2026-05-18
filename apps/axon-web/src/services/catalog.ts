import type { Catalog, CatalogTable } from './types.ts';
import { deriveCatalogTable, getCurrentSession, getSession, subscribeSession } from './query.ts';
import { SAMPLE_QUERY_SOURCE, sameQuerySource, type QueryTableSource } from './query-source.ts';

export async function loadCatalog(
  source: QueryTableSource = SAMPLE_QUERY_SOURCE,
): Promise<Catalog> {
  const state = await getSession(source);
  return buildCatalog(deriveCatalogTable(state), state.source);
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
