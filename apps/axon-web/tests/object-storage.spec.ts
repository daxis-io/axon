import { expect, test } from '@playwright/test';

import {
  buildPublicDeltaLogManifest,
  publicObjectUrl,
  parsePublicObjectStorageTableRoot,
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
      publicObjectUrl(
        root,
        'part-00000-afc4ecda-691b-43d8-85cf-da31785877d2-c000.snappy.parquet',
      ),
    ).toBe(
      'https://storage.googleapis.com/axon-public-delta-fixture-20260522-6cf5c6/axon-smoke-delta/part-00000-afc4ecda-691b-43d8-85cf-da31785877d2-c000.snappy.parquet',
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
    const descriptor = await resolvePublicObjectStorageDescriptor({
      provider: 'gcs',
      tableUri: 'gs://bucket/table',
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
    expect(descriptor).toEqual({
      table_uri: 'gs://bucket/table',
      snapshot_version: 0,
      partition_column_types: {},
      browser_compatibility: { capabilities: {} },
      required_capabilities: { capabilities: {} },
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
});
