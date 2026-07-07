import { create, fromJson, toJson } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';
import {
  ObjectRefSchema,
  PageInfoSchema,
  ProviderAuthority,
  ProviderCapabilitiesSchema,
  ProviderErrorCode,
  ProviderErrorSchema,
} from './protobuf/axon/common/v1/common_pb.ts';
import {
  ColumnNodeSchema,
  DeltaProtocolFeatureSchema,
  ListTablesResponseSchema,
  TableMetadataSchema,
  TableNodeSchema,
  TableType,
} from './protobuf/axon/catalog/v1/catalog_pb.ts';

describe('contract codegen', () => {
  it('imports common and catalog discovery messages and round-trips protobuf JSON', () => {
    const ref = create(ObjectRefSchema, {
      connectionId: 'workspace-1',
      catalog: 'main',
      schema: 'default',
      name: 'events',
    });
    const table = create(TableNodeSchema, {
      ref,
      tableType: TableType.TABLE,
      comment: 'Event rows',
    });
    const metadata = create(TableMetadataSchema, {
      table,
      columns: [
        create(ColumnNodeSchema, {
          name: 'event_date',
          type: 'DATE',
          nullable: true,
          partition: true,
          comment: 'Partition day',
        }),
      ],
      partitionColumns: ['event_date'],
      properties: { format: 'delta' },
      protocolFeatures: [
        create(DeltaProtocolFeatureSchema, {
          name: 'deletionVectors',
          reader: true,
          writer: true,
        }),
      ],
    });
    const response = create(ListTablesResponseSchema, {
      tables: [table],
      page: create(PageInfoSchema, {
        nextCursor: 'cursor-2',
        hasMore: true,
      }),
    });
    const capabilities = create(ProviderCapabilitiesSchema, {
      browsable: true,
      paginated: true,
      supportsVolumes: true,
      supportsColumnMetadata: true,
      authority: ProviderAuthority.BROKERED,
    });
    const error = create(ProviderErrorSchema, {
      code: ProviderErrorCode.SESSION_EXPIRED,
      message: 'Session expired',
      correlationId: 'req-123',
    });

    const metadataJson = toJson(TableMetadataSchema, metadata);
    const responseJson = toJson(ListTablesResponseSchema, response);

    expect(toJson(TableMetadataSchema, fromJson(TableMetadataSchema, metadataJson))).toEqual(
      metadataJson,
    );
    expect(toJson(ListTablesResponseSchema, fromJson(ListTablesResponseSchema, responseJson))).toEqual(
      responseJson,
    );
    expect(capabilities.authority).toBe(ProviderAuthority.BROKERED);
    expect(error.code).toBe(ProviderErrorCode.SESSION_EXPIRED);
    expect(response.tables[0]?.ref?.name).toBe('events');
  });
});
