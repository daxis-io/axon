import { create } from '@bufbuild/protobuf';
import { beforeEach, describe, expect, it } from 'vitest';
import {
  BrowserHttpFileDescriptorSchema,
  BrowserHttpSnapshotDescriptorSchema,
} from '../generated/contracts/protobuf/axon/dataaccess/v1/dataaccess_pb.ts';
import {
  clearPublicObjectStorageRuntimeCache,
  lookupPublicObjectStorageRuntimeCache,
  preflightPublicObjectStorageDescriptorRangeRead,
  registerPublicObjectStorageRuntimeCache,
} from './object-storage.ts';

function descriptor(snapshotVersion: bigint) {
  return create(BrowserHttpSnapshotDescriptorSchema, {
    tableUri: 'gs://public-bucket/events',
    snapshotVersion,
    activeFiles: [
      create(BrowserHttpFileDescriptorSchema, {
        path: 'part.parquet',
        url: 'https://storage.googleapis.com/public-bucket/events/part.parquet',
        sizeBytes: 7n,
      }),
    ],
  });
}

const preflight = [
  {
    path: 'part.parquet',
    url: 'https://storage.googleapis.com/public-bucket/events/part.parquet',
    size_bytes: 7,
    object_etag: '"strong-etag"',
  },
];

describe('public object storage runtime cache ownership', () => {
  beforeEach(() => {
    clearPublicObjectStorageRuntimeCache();
  });

  it('does not let an aborted stale run overwrite the current cache entry', () => {
    expect(
      registerPublicObjectStorageRuntimeCache({
        provider: 'gcs',
        tableUri: 'gs://public-bucket/events',
        snapshot: { kind: 'latest' },
        descriptor: descriptor(2n),
        preflight,
      }),
    ).toBe(true);
    const controller = new AbortController();
    controller.abort();

    expect(
      registerPublicObjectStorageRuntimeCache({
        provider: 'gcs',
        tableUri: 'gs://public-bucket/events',
        snapshot: { kind: 'latest' },
        descriptor: descriptor(1n),
        preflight,
        signal: controller.signal,
      }),
    ).toBe(false);
    expect(
      lookupPublicObjectStorageRuntimeCache({
        provider: 'gcs',
        tableUri: 'gs://public-bucket/events',
        snapshot: { kind: 'latest' },
      })?.descriptor.snapshotVersion,
    ).toBe(2n);
  });

  it('preserves AbortError when cancellation wins during Parquet preflight', async () => {
    const controller = new AbortController();

    await expect(
      preflightPublicObjectStorageDescriptorRangeRead({
        descriptor: descriptor(1n),
        signal: controller.signal,
        preflightParquetMetadataForTargets: async () => {
          controller.abort();
          return '[]';
        },
      }),
    ).rejects.toMatchObject({ name: 'AbortError' });
  });
});
