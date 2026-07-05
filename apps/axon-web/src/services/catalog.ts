import type { Catalog, CatalogTable } from './types.ts';
import { getQueryRuntimeState } from './query-runtime-state.ts';
import { SAMPLE_QUERY_SOURCE, type QueryTableSource } from './query-source.ts';

export async function loadCatalog(
  source: QueryTableSource = SAMPLE_QUERY_SOURCE,
): Promise<Catalog> {
  return snapshotCatalog(source);
}

export function snapshotCatalog(source: QueryTableSource = SAMPLE_QUERY_SOURCE): Catalog {
  return getQueryRuntimeState(source)?.catalog ?? summaryCatalog(source);
}

function summaryCatalog(source: QueryTableSource): Catalog {
  return buildCatalog(summaryCatalogTable(source), source);
}

function buildCatalog(table: CatalogTable, source: QueryTableSource): Catalog {
  return {
    name: source.catalogName,
    region: source.region,
    storage: source.storage,
    tables: [table],
  };
}

function summaryCatalogTable(source: QueryTableSource): CatalogTable {
  return {
    name: source.tableName,
    uri: tableUriFromSource(source),
    kind: 'delta',
    snapshot: summaryNumber(source, 'snapshot'),
    size_bytes: parseSizeLabel(summaryString(source, 'size')),
    row_count: summaryNumber(source, 'rows'),
    file_count: summaryNumber(source, 'files'),
    row_group_count: 0,
    partition_columns: [],
    protocol: protocolFromLabel(summaryString(source, 'protocol')),
    columns: [],
  };
}

function tableUriFromSource(source: QueryTableSource): string {
  if (source.kind === 'object_store_table_root') return source.tableUri;
  return source.storage;
}

function summaryNumber(source: QueryTableSource, field: 'snapshot' | 'rows' | 'files'): number {
  return source[field] ?? 0;
}

function summaryString(source: QueryTableSource, field: 'size' | 'protocol'): string | undefined {
  return source[field];
}

function parseSizeLabel(label: string | undefined): number {
  if (!label) return 0;
  const match = /^([\d.]+)\s*(B|KB|MB|GB)$/i.exec(label.trim());
  if (!match) return 0;
  const value = Number(match[1]);
  if (!Number.isFinite(value)) return 0;
  const unit = match[2].toUpperCase();
  const multiplier =
    unit === 'GB' ? 1024 * 1024 * 1024 : unit === 'MB' ? 1024 * 1024 : unit === 'KB' ? 1024 : 1;
  return Math.round(value * multiplier);
}

function protocolFromLabel(label: string | undefined) {
  const match = /^r(\d+)\/w(\d+)$/.exec(label ?? '');
  return {
    minReaderVersion: match ? Number(match[1]) : 0,
    minWriterVersion: match ? Number(match[2]) : 0,
    features: [],
  };
}
