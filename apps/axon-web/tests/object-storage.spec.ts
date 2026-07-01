import { expect, test } from '@playwright/test';

import {
  buildPublicDeltaLogManifest,
  clearPublicObjectStorageRuntimeCache,
  lookupPublicObjectStorageRuntimeCache,
  publicObjectUrl,
  parsePublicObjectStorageTableRoot,
  preflightPublicObjectStorageDescriptorRangeRead,
  registerPublicObjectStorageRuntimeCache,
  resolvePublicObjectStorageDescriptor,
} from '../src/services/object-storage.ts';

test.describe('public object storage', () => {
  test('parses GCS table roots and maps object URLs', () => {
    const root = parsePublicObjectStorageTableRoot({
      provider: 'gcs',
      tableUri: 'gs://axon-public-delta-fixture-20260522-6cf5c6/axon-smoke-delta',
    });

    expect(root).toEqual({
      provider: 'gcs',
      tableUri: 'gs://axon-public-delta-fixture-20260522-6cf5c6/axon-smoke-delta',
      bucket: 'axon-public-delta-fixture-20260522-6cf5c6',
      prefix: 'axon-smoke-delta',
      tableRootUrl:
        'https://storage.googleapis.com/axon-public-delta-fixture-20260522-6cf5c6/axon-smoke-delta/',
    });
    expect(
      publicObjectUrl(root, 'part-00000-afc4ecda-691b-43d8-85cf-da31785877d2-c000.snappy.parquet'),
    ).toBe(
      'https://storage.googleapis.com/axon-public-delta-fixture-20260522-6cf5c6/axon-smoke-delta/part-00000-afc4ecda-691b-43d8-85cf-da31785877d2-c000.snappy.parquet',
    );
  });

  test('parses S3 table roots and maps regional virtual-hosted object URLs', () => {
    const root = parsePublicObjectStorageTableRoot({
      provider: 's3',
      tableUri: 's3://axon-public-s3-fixture-452456948477/fixtures/prod-like/table/',
      region: 'us-east-2',
    });

    expect(root).toEqual({
      provider: 's3',
      tableUri: 's3://axon-public-s3-fixture-452456948477/fixtures/prod-like/table',
      bucket: 'axon-public-s3-fixture-452456948477',
      prefix: 'fixtures/prod-like/table',
      region: 'us-east-2',
      tableRootUrl:
        'https://axon-public-s3-fixture-452456948477.s3.us-east-2.amazonaws.com/fixtures/prod-like/table/',
    });
    expect(publicObjectUrl(root, 'part-00000-c000.snappy.parquet')).toBe(
      'https://axon-public-s3-fixture-452456948477.s3.us-east-2.amazonaws.com/fixtures/prod-like/table/part-00000-c000.snappy.parquet',
    );
  });

  test('rejects credential-bearing or non-logical table roots', () => {
    const cases = [
      'gs://user:pass@bucket/table',
      'gs://bucket/table?X-Goog-Signature=secret',
      'gs://bucket/table#fragment',
      'https://storage.googleapis.com/bucket/table',
      'gs://bucket',
      'gs://AKIAIOSFODNN7EXAMPLE/table',
      'gs://bucket/google_application_credentials/table',
    ];

    for (const tableUri of cases) {
      expect(() =>
        parsePublicObjectStorageTableRoot({
          provider: 'gcs',
          tableUri,
        }),
      ).toThrow(/public object storage/i);
    }
  });

  test('rejects credential-bearing or non-logical S3 table roots', () => {
    const cases = [
      's3://user:pass@bucket/table',
      's3://bucket/table?X-Amz-Signature=secret',
      's3://bucket/table#fragment',
      'https://bucket.s3.us-east-2.amazonaws.com/table',
      's3://bucket',
      's3://AKIAIOSFODNN7EXAMPLE/table',
      's3://bucket/aws_secret_access_key/table',
    ];

    for (const tableUri of cases) {
      expect(() =>
        parsePublicObjectStorageTableRoot({
          provider: 's3',
          tableUri,
          region: 'us-east-2',
        }),
      ).toThrow(/public object storage/i);
    }

    expect(() =>
      parsePublicObjectStorageTableRoot({
        provider: 's3',
        tableUri: 's3://bucket/table',
        region: 'us-east-2;token=secret',
      }),
    ).toThrow(/public object storage/i);
  });

  test('requires an explicit AWS region for S3 table roots', () => {
    for (const region of [undefined, '', 'auto', 'us_east_2']) {
      expect(() =>
        parsePublicObjectStorageTableRoot({
          provider: 's3',
          tableUri: 's3://bucket/table',
          region,
        }),
      ).toThrow(/S3 region/i);
    }

    expect(
      parsePublicObjectStorageTableRoot({
        provider: 's3',
        tableUri: 's3://bucket/table',
        region: 'us-gov-west-1',
      }).region,
    ).toBe('us-gov-west-1');
  });

  test('rejects S3 buckets that cannot use the supported virtual-hosted HTTPS path', () => {
    const cases = [
      's3://bucket.with.dots/table',
      's3://UPPER/table',
      's3://bucket%0aevil/table',
      's3://bucket:443/table',
      's3://ab/table',
      's3://192.168.0.1/table',
    ];

    for (const tableUri of cases) {
      expect(() =>
        parsePublicObjectStorageTableRoot({
          provider: 's3',
          tableUri,
          region: 'us-east-2',
        }),
      ).toThrow(/S3 bucket/i);
    }
  });

  test('rejects object paths that escape the table root', () => {
    const root = parsePublicObjectStorageTableRoot({
      provider: 'gcs',
      tableUri: 'gs://bucket/table',
    });

    expect(() => publicObjectUrl(root, '../secret.parquet')).toThrow(/relative path/i);
    expect(() => publicObjectUrl(root, '/absolute.parquet')).toThrow(/relative path/i);
    expect(() => publicObjectUrl(root, '_delta_log/../../secret.parquet')).toThrow(
      /relative path/i,
    );
  });

  test('lists Delta log objects anonymously through the GCS XML API', async () => {
    const root = parsePublicObjectStorageTableRoot({
      provider: 'gcs',
      tableUri: 'gs://axon-public-delta-fixture-20260522-6cf5c6/axon-smoke-delta',
    });
    const requests: Array<{ url: string; init: RequestInit | undefined }> = [];
    const fetchLog: typeof fetch = async (input, init) => {
      requests.push({ url: String(input), init });
      return new Response(
        `<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult>
          <IsTruncated>false</IsTruncated>
          <Contents>
            <Key>axon-smoke-delta/_delta_log/00000000000000000000.json</Key>
            <Size>1291</Size>
            <ETag>"32ac294b9777717ac15f711f1962ba99"</ETag>
          </Contents>
        </ListBucketResult>`,
        { status: 200, headers: { 'content-type': 'application/xml' } },
      );
    };

    const manifest = await buildPublicDeltaLogManifest(root, { fetch: fetchLog });

    expect(requests).toHaveLength(1);
    expect(requests[0].init).toMatchObject({ credentials: 'omit' });
    const listUrl = new URL(requests[0].url);
    expect(listUrl.origin).toBe('https://storage.googleapis.com');
    expect(listUrl.pathname).toBe('/axon-public-delta-fixture-20260522-6cf5c6');
    expect(listUrl.searchParams.get('list-type')).toBe('2');
    expect(listUrl.searchParams.get('prefix')).toBe('axon-smoke-delta/_delta_log/');
    expect(manifest).toEqual({
      tableUri: 'gs://axon-public-delta-fixture-20260522-6cf5c6/axon-smoke-delta',
      list_request_count: 1,
      list_duration_ms: expect.any(Number),
      objects: [
        {
          relative_path: '_delta_log/00000000000000000000.json',
          url: 'https://storage.googleapis.com/axon-public-delta-fixture-20260522-6cf5c6/axon-smoke-delta/_delta_log/00000000000000000000.json',
          size_bytes: 1291,
          etag: '"32ac294b9777717ac15f711f1962ba99"',
        },
      ],
    });
  });

  test('lists Delta log objects anonymously through the S3 ListBucketV2 API', async () => {
    const root = parsePublicObjectStorageTableRoot({
      provider: 's3',
      tableUri: 's3://axon-public-s3-fixture-452456948477/fixtures/prod-like/table',
      region: 'us-east-2',
    });
    const requests: Array<{ url: string; init: RequestInit | undefined }> = [];
    const fetchLog: typeof fetch = async (input, init) => {
      requests.push({ url: String(input), init });
      return new Response(
        `<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult>
          <IsTruncated>false</IsTruncated>
          <Contents>
            <Key>fixtures/prod-like/table/_delta_log/00000000000000000000.json</Key>
            <Size>1291</Size>
            <ETag>"32ac294b9777717ac15f711f1962ba99"</ETag>
          </Contents>
        </ListBucketResult>`,
        { status: 200, headers: { 'content-type': 'application/xml' } },
      );
    };

    const manifest = await buildPublicDeltaLogManifest(root, { fetch: fetchLog });

    expect(requests).toHaveLength(1);
    expect(requests[0].init).toMatchObject({ credentials: 'omit' });
    const listUrl = new URL(requests[0].url);
    expect(listUrl.origin).toBe(
      'https://axon-public-s3-fixture-452456948477.s3.us-east-2.amazonaws.com',
    );
    expect(listUrl.pathname).toBe('/');
    expect(listUrl.searchParams.get('list-type')).toBe('2');
    expect(listUrl.searchParams.get('prefix')).toBe('fixtures/prod-like/table/_delta_log/');
    expect(manifest).toEqual({
      tableUri: 's3://axon-public-s3-fixture-452456948477/fixtures/prod-like/table',
      list_request_count: 1,
      list_duration_ms: expect.any(Number),
      objects: [
        {
          relative_path: '_delta_log/00000000000000000000.json',
          url: 'https://axon-public-s3-fixture-452456948477.s3.us-east-2.amazonaws.com/fixtures/prod-like/table/_delta_log/00000000000000000000.json',
          size_bytes: 1291,
          etag: '"32ac294b9777717ac15f711f1962ba99"',
        },
      ],
    });
  });

  test('reports blocked public Delta log listing as an access failure', async () => {
    const root = parsePublicObjectStorageTableRoot({
      provider: 'gcs',
      tableUri: 'gs://bucket/table',
    });

    await expect(
      buildPublicDeltaLogManifest(root, {
        fetch: async () => new Response('forbidden', { status: 403 }),
      }),
    ).rejects.toMatchObject({
      code: 'public_storage_access_failed',
    });
  });

  test('builds a browser descriptor from a public table root', async () => {
    const requests: string[] = [];
    const metricEvents: unknown[] = [];
    const descriptor = await resolvePublicObjectStorageDescriptor({
      provider: 'gcs',
      tableUri: 'gs://bucket/table',
      onMetrics: (metrics) => metricEvents.push(metrics),
      fetch: async (input) => {
        requests.push(String(input));
        return new Response(
          `<?xml version="1.0" encoding="UTF-8"?>
          <ListBucketResult>
            <IsTruncated>false</IsTruncated>
            <Contents>
              <Key>table/_delta_log/00000000000000000000.json</Key>
              <Size>512</Size>
            </Contents>
          </ListBucketResult>`,
          { status: 200, headers: { 'content-type': 'application/xml' } },
        );
      },
      resolveDeltaSnapshotFromManifest: async (manifestJson, tableUri) => {
        expect(tableUri).toBe('gs://bucket/table');
        expect(JSON.parse(manifestJson)).toEqual({
          objects: [
            {
              relative_path: '_delta_log/00000000000000000000.json',
              url: 'https://storage.googleapis.com/bucket/table/_delta_log/00000000000000000000.json',
              size_bytes: 512,
            },
          ],
        });
        return JSON.stringify({
          table_uri: tableUri,
          snapshot_version: 0,
          partition_column_types: {},
          browser_compatibility: {
            capabilities: {
              deletion_vectors: 'native_only',
            },
          },
          required_capabilities: {
            capabilities: {
              deletion_vectors: 'native_only',
            },
          },
          active_files: [
            {
              path: 'category=A/part-000.parquet',
              size_bytes: 128,
              partition_values: { category: 'A' },
              stats: '{"numRecords":1}',
            },
          ],
        });
      },
    });

    expect(requests).toHaveLength(1);
    expect(metricEvents).toEqual([
      {
        descriptor_resolution_count: 1,
        delta_log_manifest_list_count: 1,
        delta_log_manifest_list_duration_ms: expect.any(Number),
        snapshot_resolve_count: 1,
        snapshot_resolve_duration_ms: expect.any(Number),
      },
    ]);
    expect(descriptor).toEqual({
      table_uri: 'gs://bucket/table',
      snapshot_version: 0,
      partition_column_types: {},
      browser_compatibility: {
        capabilities: {
          deletion_vectors: 'native_only',
        },
      },
      required_capabilities: {
        capabilities: {
          deletion_vectors: 'native_only',
        },
      },
      active_files: [
        {
          path: 'category=A/part-000.parquet',
          url: 'https://storage.googleapis.com/bucket/table/category%3DA/part-000.parquet',
          size_bytes: 128,
          partition_values: { category: 'A' },
          stats: '{"numRecords":1}',
        },
      ],
    });
  });

  test('builds a browser descriptor from a public S3 table root', async () => {
    const descriptor = await resolvePublicObjectStorageDescriptor({
      provider: 's3',
      tableUri: 's3://bucket/table',
      region: 'us-east-2',
      fetch: async () =>
        new Response(
          `<?xml version="1.0" encoding="UTF-8"?>
          <ListBucketResult>
            <IsTruncated>false</IsTruncated>
            <Contents>
              <Key>table/_delta_log/00000000000000000000.json</Key>
              <Size>512</Size>
            </Contents>
          </ListBucketResult>`,
          { status: 200, headers: { 'content-type': 'application/xml' } },
        ),
      resolveDeltaSnapshotFromManifest: async (manifestJson, tableUri) => {
        expect(tableUri).toBe('s3://bucket/table');
        expect(JSON.parse(manifestJson)).toEqual({
          objects: [
            {
              relative_path: '_delta_log/00000000000000000000.json',
              url: 'https://bucket.s3.us-east-2.amazonaws.com/table/_delta_log/00000000000000000000.json',
              size_bytes: 512,
            },
          ],
        });
        return JSON.stringify({
          table_uri: tableUri,
          snapshot_version: 0,
          active_files: [
            {
              path: 'part-000.parquet',
              size_bytes: 128,
              partition_values: {},
            },
          ],
        });
      },
    });

    expect(descriptor).toMatchObject({
      table_uri: 's3://bucket/table',
      active_files: [
        {
          path: 'part-000.parquet',
          url: 'https://bucket.s3.us-east-2.amazonaws.com/table/part-000.parquet',
          size_bytes: 128,
        },
      ],
    });
  });

  test('avoids repeated public descriptor resolution across connect and first query setup', async () => {
    clearPublicObjectStorageRuntimeCache();
    const metricEvents: Array<{
      descriptor_resolution_count: number;
      snapshot_resolve_count: number;
      delta_log_manifest_list_count: number;
    }> = [];
    let listCalls = 0;
    let snapshotResolves = 0;

    const resolve = () =>
      resolvePublicObjectStorageDescriptor({
        provider: 'gcs',
        tableUri: 'gs://bucket/table',
        onMetrics: (metrics) => metricEvents.push(metrics),
        fetch: async () => {
          listCalls += 1;
          return new Response(
            `<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult>
              <IsTruncated>false</IsTruncated>
              <Contents>
                <Key>table/_delta_log/00000000000000000000.json</Key>
                <Size>512</Size>
              </Contents>
            </ListBucketResult>`,
            { status: 200, headers: { 'content-type': 'application/xml' } },
          );
        },
        resolveDeltaSnapshotFromManifest: async (_manifestJson, tableUri) => {
          snapshotResolves += 1;
          return JSON.stringify({
            table_uri: tableUri,
            snapshot_version: 0,
            active_files: [
              {
                path: 'part-000.parquet',
                size_bytes: 128,
                partition_values: {},
              },
            ],
          });
        },
      });

    const descriptor = await resolve();
    const preflight = await preflightPublicObjectStorageDescriptorRangeRead({
      descriptor,
      preflightParquetMetadataForTargets: async () =>
        JSON.stringify([
          {
            path: 'part-000.parquet',
            url: 'https://storage.googleapis.com/bucket/table/part-000.parquet',
            size_bytes: '128',
            object_etag: '"part-000-v1"',
            footer_length_bytes: '64',
            row_group_count: '1',
            row_count: '1',
            fields: [],
            field_stats: {},
          },
        ]),
    });
    registerPublicObjectStorageRuntimeCache({
      provider: 'gcs',
      tableUri: 'gs://bucket/table',
      snapshot: { kind: 'latest' },
      descriptor,
      preflight,
      nowMs: () => 1_000,
      ttlMs: 60_000,
    });

    const cached = lookupPublicObjectStorageRuntimeCache({
      provider: 'gcs',
      tableUri: ' gs://bucket/table/ ',
      snapshot: { kind: 'latest' },
      expectedSnapshotVersion: 0,
      nowMs: () => 2_000,
    });

    expect(cached?.descriptor).toEqual({
      ...descriptor,
      active_files: [
        {
          path: 'part-000.parquet',
          url: 'https://storage.googleapis.com/bucket/table/part-000.parquet',
          size_bytes: 128,
          partition_values: {},
          object_etag: '"part-000-v1"',
        },
      ],
    });
    expect(cached?.identity).toEqual({
      path: 'part-000.parquet',
      size_bytes: 128,
      object_etag: '"part-000-v1"',
    });
    expect(listCalls).toBe(1);
    expect(snapshotResolves).toBe(1);
    expect(metricEvents).toHaveLength(1);
    expect(
      metricEvents.reduce((sum, metrics) => sum + metrics.descriptor_resolution_count, 0),
    ).toBe(1);
    expect(metricEvents.reduce((sum, metrics) => sum + metrics.snapshot_resolve_count, 0)).toBe(1);
    expect(
      metricEvents.reduce((sum, metrics) => sum + metrics.delta_log_manifest_list_count, 0),
    ).toBe(1);
    clearPublicObjectStorageRuntimeCache();
  });

  test('keeps latest and pinned public descriptor runtime cache identities distinct', () => {
    clearPublicObjectStorageRuntimeCache();
    const descriptor = publicDescriptor();

    registerPublicObjectStorageRuntimeCache({
      provider: 'gcs',
      tableUri: 'gs://bucket/table',
      snapshot: { kind: 'latest' },
      descriptor,
      preflight: [
        {
          path: 'part-000.parquet',
          url: 'https://storage.googleapis.com/bucket/table/part-000.parquet',
          size_bytes: 128,
          object_etag: '"part-000-v1"',
        },
      ],
      nowMs: () => 1_000,
      ttlMs: 60_000,
    });

    expect(
      lookupPublicObjectStorageRuntimeCache({
        provider: 'gcs',
        tableUri: 'gs://bucket/table',
        snapshot: { kind: 'version', version: 0 },
        expectedSnapshotVersion: 0,
        nowMs: () => 2_000,
      }),
    ).toBeUndefined();
    expect(
      lookupPublicObjectStorageRuntimeCache({
        provider: 'gcs',
        tableUri: 'gs://bucket/table',
        snapshot: { kind: 'latest' },
        expectedSnapshotVersion: 0,
        nowMs: () => 2_000,
      })?.descriptor.active_files[0],
    ).toMatchObject({
      path: 'part-000.parquet',
      size_bytes: 128,
      object_etag: '"part-000-v1"',
    });
    clearPublicObjectStorageRuntimeCache();
  });

  test('keeps public S3 descriptor runtime cache entries scoped by bucket region', () => {
    clearPublicObjectStorageRuntimeCache();
    const descriptor = {
      table_uri: 's3://bucket/table',
      snapshot_version: 0,
      partition_column_types: {},
      browser_compatibility: { capabilities: {} },
      required_capabilities: { capabilities: {} },
      active_files: [
        {
          path: 'part-000.parquet',
          url: 'https://bucket.s3.us-east-2.amazonaws.com/table/part-000.parquet',
          size_bytes: 128,
          partition_values: {},
        },
      ],
    };

    expect(
      registerPublicObjectStorageRuntimeCache({
        provider: 's3',
        tableUri: 's3://bucket/table',
        region: 'us-east-2',
        snapshot: { kind: 'latest' },
        descriptor,
        preflight: [
          {
            path: 'part-000.parquet',
            url: 'https://bucket.s3.us-east-2.amazonaws.com/table/part-000.parquet',
            size_bytes: 128,
            object_etag: '"part-000-v1"',
          },
        ],
        nowMs: () => 1_000,
        ttlMs: 60_000,
      }),
    ).toBe(true);

    expect(
      lookupPublicObjectStorageRuntimeCache({
        provider: 's3',
        tableUri: 's3://bucket/table',
        region: 'us-east-1',
        snapshot: { kind: 'latest' },
        expectedSnapshotVersion: 0,
        nowMs: () => 2_000,
      }),
    ).toBeUndefined();
    expect(
      lookupPublicObjectStorageRuntimeCache({
        provider: 's3',
        tableUri: 's3://bucket/table',
        region: 'us-east-2',
        snapshot: { kind: 'latest' },
        expectedSnapshotVersion: 0,
        nowMs: () => 2_000,
      })?.descriptor.active_files[0],
    ).toMatchObject({
      path: 'part-000.parquet',
      object_etag: '"part-000-v1"',
    });
    clearPublicObjectStorageRuntimeCache();
  });

  test('expires public descriptor runtime cache entries before reuse', () => {
    clearPublicObjectStorageRuntimeCache();
    registerPublicObjectStorageRuntimeCache({
      provider: 'gcs',
      tableUri: 'gs://bucket/table',
      snapshot: { kind: 'latest' },
      descriptor: publicDescriptor(),
      preflight: [
        {
          path: 'part-000.parquet',
          url: 'https://storage.googleapis.com/bucket/table/part-000.parquet',
          size_bytes: 128,
          object_etag: '"part-000-v1"',
        },
      ],
      nowMs: () => 1_000,
      ttlMs: 100,
    });

    expect(
      lookupPublicObjectStorageRuntimeCache({
        provider: 'gcs',
        tableUri: 'gs://bucket/table',
        snapshot: { kind: 'latest' },
        expectedSnapshotVersion: 0,
        nowMs: () => 1_101,
      }),
    ).toBeUndefined();
    clearPublicObjectStorageRuntimeCache();
  });

  test('preflights an active data file range-read before accepting a public table root', async () => {
    const targets: unknown[] = [];

    await preflightPublicObjectStorageDescriptorRangeRead({
      descriptor: {
        table_uri: 'gs://bucket/table',
        snapshot_version: 0,
        partition_column_types: {},
        active_files: [
          {
            path: 'category=A/part-000.parquet',
            url: 'https://storage.googleapis.com/bucket/table/category%3DA/part-000.parquet',
            size_bytes: 128,
            partition_values: { category: 'A' },
            stats: '{"numRecords":1}',
          },
          {
            path: 'category=B/part-001.parquet',
            url: 'https://storage.googleapis.com/bucket/table/category%3DB/part-001.parquet',
            size_bytes: 256,
            partition_values: { category: 'B' },
          },
        ],
      },
      preflightParquetMetadataForTargets: async (targetsJson) => {
        targets.push(...JSON.parse(targetsJson));
        return JSON.stringify([{ path: 'category=A/part-000.parquet' }]);
      },
    });

    expect(targets).toEqual([
      {
        path: 'category=A/part-000.parquet',
        url: 'https://storage.googleapis.com/bucket/table/category%3DA/part-000.parquet',
        size_bytes: 128,
        partition_values: { category: 'A' },
        stats: '{"numRecords":1}',
      },
    ]);
  });

  test('preflights cached descriptors without leaking descriptor identity fields', async () => {
    const targets: unknown[] = [];

    await preflightPublicObjectStorageDescriptorRangeRead({
      descriptor: {
        table_uri: 'gs://bucket/table',
        snapshot_version: 0,
        partition_column_types: {},
        active_files: [
          {
            path: 'part-000.parquet',
            url: 'https://storage.googleapis.com/bucket/table/part-000.parquet',
            size_bytes: 128,
            partition_values: {},
            object_etag: '"part-000-v1"',
          },
        ],
      },
      preflightParquetMetadataForTargets: async (targetsJson) => {
        targets.push(...JSON.parse(targetsJson));
        return JSON.stringify([
          {
            path: 'part-000.parquet',
            url: 'https://storage.googleapis.com/bucket/table/part-000.parquet',
            size_bytes: '128',
            object_etag: '"part-000-v1"',
          },
        ]);
      },
    });

    expect(targets).toEqual([
      {
        path: 'part-000.parquet',
        url: 'https://storage.googleapis.com/bucket/table/part-000.parquet',
        size_bytes: 128,
        partition_values: {},
      },
    ]);
  });
});

function publicDescriptor() {
  return {
    table_uri: 'gs://bucket/table',
    snapshot_version: 0,
    partition_column_types: {},
    browser_compatibility: { capabilities: {} },
    required_capabilities: { capabilities: {} },
    active_files: [
      {
        path: 'part-000.parquet',
        url: 'https://storage.googleapis.com/bucket/table/part-000.parquet',
        size_bytes: 128,
        partition_values: {},
      },
    ],
  };
}
