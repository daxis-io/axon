import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { create, fromJson, toJson, type DescField, type DescMessage } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';
import { parsePublicObjectStorageTableRoot } from '../../services/object-storage.ts';
import {
  CanonicalResourceRefSchema,
  PageInfoSchema,
  ProviderErrorCode,
  ProviderErrorSchema,
  ResourceKind,
  file_axon_common_v1_common,
} from './protobuf/axon/common/v1/common_pb.ts';
import {
  ColumnNodeSchema,
  DeltaProtocolFeatureSchema,
  ListTablesResponseSchema,
  TableMetadataSchema,
  TableNodeSchema,
  TableType,
  VolumeNodeSchema,
  VolumeType,
  file_axon_catalog_v1_catalog,
} from './protobuf/axon/catalog/v1/catalog_pb.ts';

describe('contract codegen', () => {
  it('round-trips the exact canonical resource tuple and catalog display metadata', () => {
    const resource = create(CanonicalResourceRefSchema, {
      connectionId: 'workspace-1',
      providerNamespace: 'axon.public-gcs/v1',
      kind: ResourceKind.TABLE,
      identity: {
        case: 'canonicalLocator',
        value: 'gs://axon-fixtures/tables/events',
      },
    });
    const table = create(TableNodeSchema, {
      resource,
      name: 'events',
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
      }),
    });
    const volume = create(VolumeNodeSchema, {
      resource: create(CanonicalResourceRefSchema, {
        connectionId: 'workspace-1',
        providerNamespace: 'axon.unity-catalog/v1',
        kind: ResourceKind.VOLUME,
        identity: { case: 'providerObjectId', value: 'volume-123' },
      }),
      name: 'landing',
      volumeType: VolumeType.EXTERNAL,
      storageLocation: 's3://workspace-volumes/landing',
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
    expect(error.code).toBe(ProviderErrorCode.SESSION_EXPIRED);
    expect(response.tables[0]?.resource?.identity).toEqual({
      case: 'canonicalLocator',
      value: 'gs://axon-fixtures/tables/events',
    });
    expect(volume.resource?.identity).toEqual({
      case: 'providerObjectId',
      value: 'volume-123',
    });
  });

  it('exposes only the corrected common and catalog declarations', () => {
    expect(
      CanonicalResourceRefSchema.fields.map((field: DescField) => field.name),
    ).toEqual([
      'connection_id',
      'provider_namespace',
      'kind',
      'provider_object_id',
      'canonical_locator',
    ]);
    expect(
      CanonicalResourceRefSchema.oneofs[0]?.fields.map((field: DescField) => field.name),
    ).toEqual(['provider_object_id', 'canonical_locator']);

    const commonMessages = file_axon_common_v1_common.messages.map(
      (message: DescMessage) => message.name,
    );
    const commonEnums = file_axon_common_v1_common.enums.map((value) => value.name);
    expect(commonMessages).not.toContain('ProviderCapabilities');
    expect(commonEnums).not.toContain('ProviderAuthority');

    expect(PageInfoSchema.fields.map((field: DescField) => field.name)).toEqual([
      'next_cursor',
    ]);
    expect(create(PageInfoSchema, {}).nextCursor).toBeUndefined();
    expect(create(PageInfoSchema, { nextCursor: '' }).nextCursor).toBe('');

    const catalogMessages = file_axon_catalog_v1_catalog.messages.map(
      (message: DescMessage) => message.name,
    );
    expect(catalogMessages).not.toContain('FunctionNode');
    expect(catalogMessages).not.toContain('ModelNode');
    expect(catalogMessages).not.toContain('ListFunctionsResponse');
    expect(catalogMessages).not.toContain('ListModelsResponse');
    expect(ColumnNodeSchema.fields.map((field: DescField) => field.name)).not.toContain(
      'partition',
    );
    expect(TableNodeSchema.fields.map((field: DescField) => field.name)).toEqual([
      'resource',
      'table_type',
      'comment',
      'name',
    ]);
    expect(VolumeNodeSchema.fields.map((field: DescField) => field.name)).toEqual([
      'resource',
      'volume_type',
      'storage_location',
      'comment',
      'name',
    ]);
  });

  it('keeps locator canonicalization versioned, deterministic, and capability-free', () => {
    const fixtures = JSON.parse(
      readFileSync(
        resolve('src/generated/contracts/fixtures/canonical-resource-locators.json'),
        'utf8',
      ),
    ) as CanonicalLocatorFixtures;

    for (const fixture of fixtures.accepted) {
      expect(fixture.providerNamespace).toMatch(/\/v\d+$/);
      expect(canonicalizeFixture(fixture)).toBe(fixture.expected);
      expect(containsCapabilityMaterial(fixture.expected)).toBe(false);
    }

    for (const fixture of fixtures.rejected) {
      expect(() => canonicalizeFixture(fixture)).toThrow();
    }
  });

  it('preserves absence of optional table statistics and protocol versions', () => {
    const metadata = create(TableMetadataSchema, {});
    const decoded = fromJson(TableMetadataSchema, {});

    for (const message of [metadata, decoded]) {
      expect(message.rowCount).toBeUndefined();
      expect(message.sizeBytes).toBeUndefined();
      expect(message.fileCount).toBeUndefined();
      expect(message.latestSnapshotVersion).toBeUndefined();
      expect(message.minReaderVersion).toBeUndefined();
      expect(message.minWriterVersion).toBeUndefined();
    }

    const explicitZero = create(TableMetadataSchema, {
      rowCount: 0n,
      sizeBytes: 0n,
      fileCount: 0n,
      latestSnapshotVersion: 0n,
      minReaderVersion: 0,
      minWriterVersion: 0,
    });
    const explicitZeroJson = toJson(TableMetadataSchema, explicitZero);

    expect(explicitZero.rowCount).toBe(0n);
    expect(explicitZeroJson).toMatchObject({
      rowCount: '0',
      sizeBytes: '0',
      fileCount: '0',
      latestSnapshotVersion: '0',
      minReaderVersion: 0,
      minWriterVersion: 0,
    });
  });
});

type CanonicalLocatorFixture = {
  providerNamespace: 'axon.public-gcs/v1' | 'axon.public-s3/v1' | 'axon.sample-fixture/v1';
  provider: 'gcs' | 's3' | 'sample';
  input: string;
  expected?: string;
  region?: string;
};

type CanonicalLocatorFixtures = {
  accepted: Array<CanonicalLocatorFixture & { expected: string }>;
  rejected: CanonicalLocatorFixture[];
};

function canonicalizeFixture(fixture: CanonicalLocatorFixture): string {
  if (fixture.provider === 'sample') {
    const input = fixture.input.trim();
    if (input !== '/fixtures/prod-like/delta-log-manifest.json') {
      throw new TypeError('unknown sample fixture locator');
    }
    return 'axon-fixture://sample-lake/prod_like/events';
  }

  return parsePublicObjectStorageTableRoot({
    provider: fixture.provider,
    tableUri: fixture.input,
    region: fixture.region,
  }).tableUri;
}

function containsCapabilityMaterial(locator: string): boolean {
  return /[?#]|(?:token|signature|credential|grant|x-amz-)/i.test(locator);
}
