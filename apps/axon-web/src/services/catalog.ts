import type { Catalog, CatalogTable } from './types.ts';
import { deriveCatalogTable, getCurrentSession, getSession, subscribeSession } from './query.ts';

const STORAGE_LABEL = 'gs://axon-sandbox';

export async function loadCatalog(): Promise<Catalog> {
  const state = await getSession();
  return buildCatalog(deriveCatalogTable(state));
}

export function snapshotCatalog(): Catalog | undefined {
  const state = getCurrentSession();
  if (!state) return undefined;
  return buildCatalog(deriveCatalogTable(state));
}

export function subscribeCatalog(listener: (catalog: Catalog) => void): () => void {
  return subscribeSession((state) => {
    listener(buildCatalog(deriveCatalogTable(state)));
  });
}

function buildCatalog(table: CatalogTable): Catalog {
  return {
    name: 'axon-sandbox',
    region: 'browser-local',
    storage: STORAGE_LABEL,
    tables: [table],
  };
}
