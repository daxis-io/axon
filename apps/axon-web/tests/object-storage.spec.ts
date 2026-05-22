import { expect, test } from '@playwright/test';

import {
  publicObjectUrl,
  parsePublicObjectStorageTableRoot,
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
});
