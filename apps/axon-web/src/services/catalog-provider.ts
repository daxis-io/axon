import { clone, create, equals } from '@bufbuild/protobuf';
import {
  CatalogNodeSchema,
  GetTableMetadataResponseSchema,
  ListCatalogsResponseSchema,
  ListSchemasResponseSchema,
  ListTablesResponseSchema,
  SchemaNodeSchema,
  TableMetadataSchema,
  TableNodeSchema,
  type CatalogNode,
  type GetTableMetadataResponse,
  type ListCatalogsResponse,
  type ListSchemasResponse,
  type ListTablesResponse,
  type SchemaNode,
  type TableMetadata,
  type TableNode,
} from '../generated/contracts/protobuf/axon/catalog/v1/catalog_pb.ts';
import {
  PageInfoSchema,
  ProviderErrorCode,
  ProviderErrorSchema,
  ResourceKind,
  type PageRequest,
  type ProviderError,
} from '../generated/contracts/protobuf/axon/common/v1/common_pb.ts';
import {
  createLocalDeltaCanonicalTable,
  createPublicObjectStorageCanonicalTable,
  localDeltaConnectionId,
} from './canonical-table-identity.ts';

export type CatalogDiscoveryContext = Readonly<{
  signal: AbortSignal;
  correlationId: string;
  deadlineEpochMs?: number;
}>;

export interface CatalogProvider {
  listCatalogs(page: PageRequest, context: CatalogDiscoveryContext): Promise<ListCatalogsResponse>;
  listSchemas(
    catalog: CatalogNode,
    page: PageRequest,
    context: CatalogDiscoveryContext,
  ): Promise<ListSchemasResponse>;
  listTables(
    schema: SchemaNode,
    page: PageRequest,
    context: CatalogDiscoveryContext,
  ): Promise<ListTablesResponse>;
  getTableMetadata(
    table: TableNode,
    context: CatalogDiscoveryContext,
  ): Promise<GetTableMetadataResponse>;
}

export type CatalogDiscoverySnapshot = Readonly<{
  catalog: CatalogNode;
  schema: SchemaNode;
  table: TableNode;
  metadata: TableMetadata;
}>;

export type CatalogProviderFailureKind =
  | 'cancelled'
  | 'deadline_exceeded'
  | 'invalid_request'
  | 'not_found'
  | 'unavailable';

export class CatalogProviderError extends Error {
  readonly detail: ProviderError;

  constructor(
    readonly kind: CatalogProviderFailureKind,
    message: string,
    correlationId: string,
    readonly retryable: boolean = false,
  ) {
    super(message);
    this.name = kind === 'cancelled' ? 'AbortError' : 'CatalogProviderError';
    this.detail = create(ProviderErrorSchema, {
      code: providerErrorCode(kind),
      message,
      correlationId: correlationId || 'catalog-provider-unknown',
    });
  }
}

export type LocalDeltaCatalogProviderInput = Readonly<{
  registryId: string;
  schemaName: string;
  tableName: string;
  metadata: TableMetadata;
}>;

type PublicObjectStorageCatalogProviderBase = Readonly<{
  connectionId: string;
  normalizedTableUri: string;
  schemaName: string;
  tableName: string;
  metadata: TableMetadata;
}>;

export type PublicObjectStorageCatalogProviderInput =
  | (PublicObjectStorageCatalogProviderBase & Readonly<{ provider: 'gcs'; region?: never }>)
  | (PublicObjectStorageCatalogProviderBase & Readonly<{ provider: 's3'; region: string }>);

export function createLocalDeltaCatalogProvider(
  input: LocalDeltaCatalogProviderInput,
): CatalogProvider {
  let table: TableNode;
  let connectionId: string;
  try {
    table = createLocalDeltaCanonicalTable(input);
    connectionId = localDeltaConnectionId(input.registryId);
    requiredText(input.schemaName, 'schema name');
  } catch {
    throw new CatalogProviderError(
      'invalid_request',
      'local catalog identity is invalid',
      'catalog-provider-construction',
    );
  }
  const catalog = create(CatalogNodeSchema, {
    connectionId,
    name: 'local-delta',
  });
  const schema = create(SchemaNodeSchema, {
    connectionId,
    catalog: catalog.name,
    name: input.schemaName,
  });
  const metadata = clone(TableMetadataSchema, input.metadata);
  if (metadata.table) {
    table.comment = metadata.table.comment;
  }
  if (metadata.table && !equals(TableNodeSchema, metadata.table, table)) {
    throw new CatalogProviderError(
      'invalid_request',
      'local catalog metadata identity is invalid',
      'catalog-provider-construction',
    );
  }
  metadata.table = clone(TableNodeSchema, table);

  return flatCatalogProvider({ catalog, schema, table, metadata });
}

export function createPublicObjectStorageCatalogProvider(
  input: PublicObjectStorageCatalogProviderInput,
): CatalogProvider {
  let table: TableNode;
  let connectionId: string;
  let normalizedTableUri: string;
  try {
    connectionId = input.connectionId;
    normalizedTableUri = input.normalizedTableUri;
    requiredText(input.schemaName, 'schema name');
    table =
      input.provider === 's3'
        ? createPublicObjectStorageCanonicalTable({
            provider: input.provider,
            connectionId,
            normalizedTableUri,
            region: input.region,
            tableName: input.tableName,
          })
        : createPublicObjectStorageCanonicalTable({
            provider: input.provider,
            connectionId,
            normalizedTableUri,
            tableName: input.tableName,
          });
  } catch {
    throw new CatalogProviderError(
      'invalid_request',
      'public catalog identity is invalid',
      'catalog-provider-construction',
    );
  }
  const catalog = create(CatalogNodeSchema, {
    connectionId,
    name: `public-${input.provider}`,
  });
  const schema = create(SchemaNodeSchema, {
    connectionId,
    catalog: catalog.name,
    name: input.schemaName,
  });
  const metadata = clone(TableMetadataSchema, input.metadata);
  if (metadata.table) {
    table.comment = metadata.table.comment;
  }
  if (
    metadata.storageLocation !== normalizedTableUri ||
    (metadata.table && !equals(TableNodeSchema, metadata.table, table))
  ) {
    throw new CatalogProviderError(
      'invalid_request',
      'public catalog metadata identity is invalid',
      'catalog-provider-construction',
    );
  }
  metadata.table = clone(TableNodeSchema, table);

  return flatCatalogProvider({ catalog, schema, table, metadata });
}

export async function discoverFlatCatalog(
  provider: CatalogProvider,
  page: PageRequest,
  context: CatalogDiscoveryContext,
): Promise<CatalogDiscoverySnapshot> {
  validateContext(context);
  const catalogs = await provider.listCatalogs(page, context);
  validateContext(context);
  const catalog = exactlyOne(catalogs.catalogs, 'catalog', context);
  requireTerminatingPage(catalogs.page?.nextCursor, context);
  const schemas = await provider.listSchemas(catalog, page, context);
  validateContext(context);
  const schema = exactlyOne(schemas.schemas, 'schema', context);
  requireTerminatingPage(schemas.page?.nextCursor, context);
  validateSchemaParent(catalog, schema, context);
  const tables = await provider.listTables(schema, page, context);
  validateContext(context);
  const table = exactlyOne(tables.tables, 'table', context);
  requireTerminatingPage(tables.page?.nextCursor, context);
  validateTableParent(schema, table, context);
  const response = await provider.getTableMetadata(table, context);
  validateContext(context);
  const metadata = response.table;
  if (!metadata?.table || !equals(TableNodeSchema, metadata.table, table)) {
    invalidRequest('catalog metadata did not match the listed table', context);
  }
  return {
    catalog: clone(CatalogNodeSchema, catalog),
    schema: clone(SchemaNodeSchema, schema),
    table: clone(TableNodeSchema, table),
    metadata: clone(TableMetadataSchema, metadata),
  };
}

type FlatCatalog = Readonly<{
  catalog: CatalogNode;
  schema: SchemaNode;
  table: TableNode;
  metadata: TableMetadata;
}>;

function flatCatalogProvider(values: FlatCatalog): CatalogProvider {
  return {
    async listCatalogs(page, context) {
      validateRequest(page, context);
      return create(ListCatalogsResponseSchema, {
        catalogs: [clone(CatalogNodeSchema, values.catalog)],
        page: create(PageInfoSchema),
      });
    },
    async listSchemas(catalog, page, context) {
      validateRequest(page, context);
      if (!equals(CatalogNodeSchema, catalog, values.catalog)) {
        invalidRequest('catalog parent did not match the provider connection', context);
      }
      return create(ListSchemasResponseSchema, {
        schemas: [clone(SchemaNodeSchema, values.schema)],
        page: create(PageInfoSchema),
      });
    },
    async listTables(schema, page, context) {
      validateRequest(page, context);
      if (!equals(SchemaNodeSchema, schema, values.schema)) {
        invalidRequest('schema parent did not match the provider connection', context);
      }
      return create(ListTablesResponseSchema, {
        tables: [clone(TableNodeSchema, values.table)],
        page: create(PageInfoSchema),
      });
    },
    async getTableMetadata(table, context) {
      validateContext(context);
      if (!equals(TableNodeSchema, table, values.table)) {
        invalidRequest('table identity did not match the provider connection', context);
      }
      return create(GetTableMetadataResponseSchema, {
        table: clone(TableMetadataSchema, values.metadata),
      });
    },
  };
}

function validateRequest(page: PageRequest, context: CatalogDiscoveryContext): void {
  validateContext(context);
  if (page.cursor.length > 0) {
    invalidRequest('catalog pagination cursor is not supported', context);
  }
  if (!Number.isInteger(page.pageSize) || page.pageSize < 0 || page.pageSize > 0xffff_ffff) {
    invalidRequest('catalog page size is invalid', context);
  }
}

function validateContext(context: CatalogDiscoveryContext): void {
  const correlationId = context.correlationId?.trim();
  if (!correlationId) {
    throw new CatalogProviderError(
      'invalid_request',
      'catalog correlation ID is required',
      'catalog-provider-unknown',
    );
  }
  if (context.signal.aborted) {
    throw new CatalogProviderError('cancelled', 'catalog discovery was cancelled', correlationId);
  }
  if (context.deadlineEpochMs !== undefined) {
    if (!Number.isFinite(context.deadlineEpochMs) || context.deadlineEpochMs <= Date.now()) {
      throw new CatalogProviderError(
        'deadline_exceeded',
        'catalog discovery deadline elapsed',
        correlationId,
      );
    }
  }
}

function validateSchemaParent(
  catalog: CatalogNode,
  schema: SchemaNode,
  context: CatalogDiscoveryContext,
): void {
  if (schema.connectionId !== catalog.connectionId || schema.catalog !== catalog.name) {
    invalidRequest('catalog schema parent is invalid', context);
  }
}

function validateTableParent(
  schema: SchemaNode,
  table: TableNode,
  context: CatalogDiscoveryContext,
): void {
  const resource = table.resource;
  if (
    !resource ||
    resource.connectionId !== schema.connectionId ||
    resource.kind !== ResourceKind.TABLE ||
    !resource.providerNamespace.trim() ||
    (resource.identity.case !== 'providerObjectId' &&
      resource.identity.case !== 'canonicalLocator') ||
    !resource.identity.value.trim()
  ) {
    invalidRequest('catalog table parent or canonical identity is invalid', context);
  }
}

function requireTerminatingPage(
  nextCursor: string | undefined,
  context: CatalogDiscoveryContext,
): void {
  if (nextCursor) {
    invalidRequest('flat catalog provider returned a continuation cursor', context);
  }
}

function exactlyOne<T>(values: readonly T[], label: string, context: CatalogDiscoveryContext): T {
  if (values.length !== 1) {
    invalidRequest(`flat catalog provider must return exactly one ${label}`, context);
  }
  return values[0]!;
}

function invalidRequest(message: string, context: CatalogDiscoveryContext): never {
  throw new CatalogProviderError('invalid_request', message, context.correlationId);
}

function requiredText(value: string, label: string): string {
  if (value.length === 0 || value.trim().length === 0) {
    throw new Error(`${label} must be non-empty`);
  }
  return value;
}

function providerErrorCode(kind: CatalogProviderFailureKind): ProviderErrorCode {
  switch (kind) {
    case 'cancelled':
    case 'deadline_exceeded':
      return ProviderErrorCode.UNSPECIFIED;
    case 'invalid_request':
      return ProviderErrorCode.INVALID;
    case 'not_found':
      return ProviderErrorCode.NOT_FOUND;
    case 'unavailable':
      return ProviderErrorCode.UNAVAILABLE;
  }
}
