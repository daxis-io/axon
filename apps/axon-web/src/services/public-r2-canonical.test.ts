import { create } from '@bufbuild/protobuf';
import { timestampFromMs } from '@bufbuild/protobuf/wkt';
import { describe, expect, it, vi } from 'vitest';
import {
  BrowserAccessClass,
  BrowserHttpSnapshotDescriptorSchema,
} from '../generated/contracts/protobuf/axon/dataaccess/v1/dataaccess_pb.ts';
import { ResourceKind } from '../generated/contracts/protobuf/axon/common/v1/common_pb.ts';
import {
  canonicalTableForSelection,
  dataAccessResolverForSelection,
} from './browser-read-resolution.ts';
import { createPublicObjectStorageCanonicalTable } from './canonical-table-identity.ts';
import {
  querySourceForConnectedTableRef,
  querySourceIdentity,
  type AvailableQuerySourceSelection,
  type QueryCatalogCandidate,
} from './query-source.ts';

const TABLE_URI = 'r2://axon-public-data/fixtures/onboarding-v1/table';
const ENDPOINT = 'https://data.axon.daxistech.io';
const CONNECTION_ID =
  'axon-connection://public-r2/https%3A%2F%2Fdata.axon.daxistech.io/axon-public-data';

function r2Table() {
  return createPublicObjectStorageCanonicalTable({
    provider: 'r2',
    connectionId: CONNECTION_ID,
    normalizedTableUri: TABLE_URI,
    endpoint: ENDPOINT,
    tableName: 'table',
  });
}

function r2Source() {
  return {
    kind: 'object_store_table_root' as const,
    provider: 'r2' as const,
    catalogName: 'Public R2',
    schemaName: 'default',
    tableName: 'table',
    tableUri: TABLE_URI,
    storage: TABLE_URI,
    region: 'auto',
    endpoint: ENDPOINT,
    snapshot: 3,
  };
}

describe('public R2 canonical provider mapping', () => {
  it('uses the public-r2 namespace and endpoint-scoped connection identity', () => {
    expect(r2Table()).toMatchObject({
      name: 'table',
      resource: {
        connectionId: CONNECTION_ID,
        providerNamespace: 'axon.public-r2/v1',
        kind: ResourceKind.TABLE,
        identity: {
          case: 'canonicalLocator',
          value: TABLE_URI,
        },
      },
    });
  });

  it('carries the endpoint from persisted catalog metadata into source identity', () => {
    const table = r2Table();
    const catalog: QueryCatalogCandidate = {
      id: CONNECTION_ID,
      alias: 'Public R2',
      kind: 'object_store',
      provider: 'r2',
      storage: TABLE_URI,
      region: 'auto',
      endpoint: ENDPOINT,
      schemas: [
        {
          name: 'default',
          tables: [
            {
              name: 'table',
              snapshot: 3,
              uri: TABLE_URI,
              logicalTable: table,
              source: {
                storage: TABLE_URI,
                region: 'auto',
                endpoint: ENDPOINT,
              },
            },
          ],
        },
      ],
    };

    const source = querySourceForConnectedTableRef([catalog], table);
    expect(source).toMatchObject(r2Source());
    expect(querySourceIdentity(source!)).not.toEqual(
      querySourceIdentity({ ...r2Source(), endpoint: 'https://qualified.example.com' }),
    );
    expect(querySourceIdentity(source!)).toEqual(
      querySourceIdentity({ ...r2Source(), region: 'display-only-placeholder' }),
    );
  });

  it('resolves R2 through the unchanged PUBLIC browser-read envelope', async () => {
    const selection: AvailableQuerySourceSelection = {
      kind: 'resource',
      ref: r2Table(),
      source: r2Source(),
    };
    const descriptor = create(BrowserHttpSnapshotDescriptorSchema, {
      tableUri: TABLE_URI,
      snapshotVersion: 3n,
    });
    const loadPublicObjectStorageDescriptor = vi.fn(async () => descriptor);
    const resolver = dataAccessResolverForSelection(selection, {
      loadPublicObjectStorageDescriptor,
    });
    const table = canonicalTableForSelection(selection);

    const resolution = await resolver.resolve(table.resource!, {
      executionId: 'execution-r2',
      deadline: timestampFromMs(1_800_000_120_000),
      snapshotVersion: 3,
      signal: new AbortController().signal,
    });

    expect(loadPublicObjectStorageDescriptor).toHaveBeenCalledWith({
      provider: 'r2',
      tableUri: TABLE_URI,
      endpoint: ENDPOINT,
      region: undefined,
      snapshotVersion: 3,
      expectedSnapshotVersion: 3,
      signal: expect.objectContaining({ aborted: false }),
    });
    expect(resolution.outcome).toMatchObject({
      case: 'browserRead',
      value: {
        accessClass: BrowserAccessClass.PUBLIC,
        correlationId: 'execution-r2',
        provenance: {
          resolverId: 'axon.public-r2/v1',
          resolutionId: 'execution-r2:public-r2',
        },
      },
    });
  });
});
