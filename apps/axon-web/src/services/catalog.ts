import { create, fromJson, type JsonValue } from '@bufbuild/protobuf';
import { PageRequestSchema } from '../generated/contracts/protobuf/axon/common/v1/common_pb.ts';
import {
  TableMetadataSchema,
  type TableMetadata,
} from '../generated/contracts/protobuf/axon/catalog/v1/catalog_pb.ts';
import {
  CatalogProviderError,
  createLocalDeltaCatalogProvider,
  createPublicObjectStorageCatalogProvider,
  discoverFlatCatalog,
  type CatalogDiscoveryContext,
  type CatalogDiscoverySnapshot,
} from './catalog-provider.ts';
import {
  parsePublicObjectStorageTableRoot,
  publicObjectStorageConnectionId,
} from './object-storage.ts';
import type { Catalog, CatalogTable } from './types.ts';
import { getQueryRuntimeState } from './query-runtime-state.ts';
import { SAMPLE_QUERY_SOURCE, type QueryTableSource } from './query-source.ts';

export async function loadCatalog(
  source: QueryTableSource,
  context: CatalogDiscoveryContext,
): Promise<Catalog> {
  if (source.kind === 'manifest') {
    if (!isExplicitSampleFixtureSource(source)) {
      throw new CatalogProviderError(
        'invalid_request',
        'manifest catalog source is not the explicit sample fixture',
        context.correlationId,
      );
    }
    return snapshotCatalog(source);
  }

  const metadata = metadataForCatalogSource(source, context.correlationId);
  const provider = providerForCatalogSource(source, metadata, context.correlationId);
  const discovery = await discoverFlatCatalog(provider, create(PageRequestSchema), context);
  return catalogFromDiscovery(discovery, source);
}

export function snapshotCatalog(source: QueryTableSource): Catalog {
  return getQueryRuntimeState(source)?.catalog ?? summaryCatalog(source);
}

function summaryCatalog(source: QueryTableSource): Catalog {
  return buildCatalog(summaryCatalogTable(source), source);
}

function metadataForCatalogSource(
  source: Exclude<QueryTableSource, { kind: 'manifest' }>,
  correlationId: string,
): TableMetadata {
  if (!source.catalogMetadataJson) {
    return legacySummaryMetadataProjection(source);
  }

  try {
    const metadata = fromJson(TableMetadataSchema, source.catalogMetadataJson as JsonValue);
    if (!metadata.table?.resource) {
      throw new Error('persisted catalog metadata identity is missing');
    }
    return metadata;
  } catch {
    throw new CatalogProviderError(
      'invalid_request',
      'persisted catalog metadata is invalid',
      correlationId,
    );
  }
}

function legacySummaryMetadataProjection(
  source: Exclude<QueryTableSource, { kind: 'manifest' }>,
): TableMetadata {
  return create(TableMetadataSchema, {
    rowCount: source.rows === undefined ? undefined : BigInt(source.rows),
    sizeBytes: source.size === undefined ? undefined : BigInt(parseSizeLabel(source.size)),
    fileCount: source.files === undefined ? undefined : BigInt(source.files),
    latestSnapshotVersion: source.snapshot === undefined ? undefined : BigInt(source.snapshot),
    minReaderVersion: protocolFromLabel(source.protocol).minReaderVersion || undefined,
    minWriterVersion: protocolFromLabel(source.protocol).minWriterVersion || undefined,
    storageLocation:
      source.kind === 'object_store_table_root'
        ? source.tableUri
        : `browser-local://delta-table/${encodeURIComponent(source.localRegistryId)}`,
  });
}

export function validatePersistedCatalogMetadata(
  source: Exclude<QueryTableSource, { kind: 'manifest' }>,
  correlationId = 'catalog-persistence-validation',
): void {
  if (!source.catalogMetadataJson) return;
  const metadata = metadataForCatalogSource(source, correlationId);
  providerForCatalogSource(source, metadata, correlationId);
}

function providerForCatalogSource(
  source: Exclude<QueryTableSource, { kind: 'manifest' }>,
  metadata: TableMetadata,
  correlationId: string,
) {
  if (source.kind === 'local_delta') {
    validateLocalStorageLocation(metadata.storageLocation, correlationId);
    return createLocalDeltaCatalogProvider({
      registryId: source.localRegistryId,
      schemaName: source.schemaName,
      tableName: source.tableName,
      metadata,
    });
  }
  return createPublicProvider(source, metadata, correlationId);
}

function createPublicProvider(
  source: Extract<QueryTableSource, { kind: 'object_store_table_root' }>,
  metadata: TableMetadata,
  correlationId: string,
) {
  let root;
  try {
    root = parsePublicObjectStorageTableRoot({
      provider: source.provider,
      tableUri: source.tableUri,
      region: source.region,
    });
  } catch {
    throw new CatalogProviderError(
      'invalid_request',
      'public catalog identity is invalid',
      correlationId,
    );
  }
  const providerIdentity =
    root.provider === 's3'
      ? { provider: root.provider, region: root.region }
      : { provider: root.provider };
  return createPublicObjectStorageCatalogProvider({
    ...providerIdentity,
    connectionId: publicObjectStorageConnectionId(root),
    normalizedTableUri: root.tableUri,
    schemaName: source.schemaName,
    tableName: source.tableName,
    metadata,
  });
}

function validateLocalStorageLocation(storageLocation: string, correlationId: string): void {
  try {
    const parsed = new URL(storageLocation);
    const encodedRootName = parsed.pathname.replace(/^\/|\/$/g, '');
    const decodedRootName = decodeURIComponent(encodedRootName);
    if (
      parsed.protocol !== 'browser-local:' ||
      parsed.hostname !== 'delta-table' ||
      !decodedRootName ||
      encodedRootName.includes('/') ||
      encodeURIComponent(decodedRootName) !== encodedRootName ||
      parsed.username ||
      parsed.password ||
      parsed.search ||
      parsed.hash
    ) {
      throw new Error('invalid local storage location');
    }
  } catch {
    throw new CatalogProviderError(
      'invalid_request',
      'local catalog storage location is invalid',
      correlationId,
    );
  }
}

function catalogFromDiscovery(
  discovery: CatalogDiscoverySnapshot,
  source: Exclude<QueryTableSource, { kind: 'manifest' }>,
): Catalog {
  const metadata = discovery.metadata;
  const partitionNames = new Set(metadata.partitionColumns);
  return {
    name: discovery.catalog.name,
    region: source.region,
    storage: metadata.storageLocation,
    tables: [
      {
        name: discovery.table.name,
        uri: metadata.storageLocation,
        kind: 'delta',
        snapshot: safeOptionalBigInt(metadata.latestSnapshotVersion),
        size_bytes: safeOptionalBigInt(metadata.sizeBytes),
        row_count: safeOptionalBigInt(metadata.rowCount),
        file_count: safeOptionalBigInt(metadata.fileCount),
        row_group_count: 0,
        partition_columns: metadata.partitionColumns.map((name) => ({
          name,
          type: metadata.columns.find((column) => column.name === name)?.type ?? 'unknown',
          pruning: 'stats',
        })),
        protocol: {
          minReaderVersion: metadata.minReaderVersion ?? 0,
          minWriterVersion: metadata.minWriterVersion ?? 0,
          features: metadata.protocolFeatures.map((feature) => feature.name),
        },
        columns: metadata.columns.map((column) => ({
          name: column.name,
          type: column.type,
          role: partitionNames.has(column.name) ? 'partition' : 'data',
          nullable: column.nullable,
        })),
      },
    ],
  };
}

function safeOptionalBigInt(value: bigint | undefined): number {
  if (value === undefined) return 0;
  const converted = Number(value);
  if (!Number.isSafeInteger(converted)) {
    throw new Error('catalog metadata integer exceeds the safe UI range');
  }
  return converted;
}

function isExplicitSampleFixtureSource(source: Extract<QueryTableSource, { kind: 'manifest' }>) {
  return (
    source.catalogName === SAMPLE_QUERY_SOURCE.catalogName &&
    source.schemaName === SAMPLE_QUERY_SOURCE.schemaName &&
    source.tableName === SAMPLE_QUERY_SOURCE.tableName &&
    source.manifestUrl === SAMPLE_QUERY_SOURCE.manifestUrl &&
    source.storage === SAMPLE_QUERY_SOURCE.storage &&
    source.region === SAMPLE_QUERY_SOURCE.region
  );
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
