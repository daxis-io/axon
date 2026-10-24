import { create } from '@bufbuild/protobuf';
import { beforeEach, describe, expect, it } from 'vitest';
import {
  BrowserHttpFileDescriptorSchema,
  BrowserHttpSnapshotDescriptorSchema,
} from '../generated/contracts/protobuf/axon/dataaccess/v1/dataaccess_pb.ts';
import {
  buildPublicDeltaLogManifest,
  clearPublicObjectStorageRuntimeCache,
  lookupPublicObjectStorageRuntimeCache,
  parsePublicObjectStorageTableRoot,
  preflightPublicObjectStorageDescriptorRangeRead,
  publicObjectUrlForDeltaPath,
  registerPublicObjectStorageRuntimeCache,
  resolvePublicObjectStorageDescriptor,
  type PublicObjectStorageTableRoot,
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

// Delta add.path values are URI-encoded (Delta protocol, "Add File and Remove File"). These
// are the exact paths writers put in the log; the physical object key is the path decoded once.
const ENCODED_DELTA_ADD_PATHS = [
  // Spark/Delta 3.2, delta-kernel-rs tests/data/data-reader-timestamp_ntz: the Hive directory
  // escapes ':' as %3A and the log URI-encodes that again plus the space.
  {
    addPath:
      'tsNtzPartition=2013-07-05%2017%253A01%253A00.123456/part-00000-6240e68e-2304-449a-a1e6-0e24866d3508.c000.snappy.parquet',
    objectKey:
      'tsNtzPartition=2013-07-05 17%3A01%3A00.123456/part-00000-6240e68e-2304-449a-a1e6-0e24866d3508.c000.snappy.parquet',
  },
  // delta-rs tests/data/delta-0.8.0-special-partition: the directory keeps %20 and %2F literally.
  {
    addPath: 'x=B%2520B/part-00015-e9abbc6f-85e9-457b-be8e-e9f5b8a22890.c000.snappy.parquet',
    objectKey: 'x=B%20B/part-00015-e9abbc6f-85e9-457b-be8e-e9f5b8a22890.c000.snappy.parquet',
  },
  {
    addPath: 'x=A%252FA/part-00007-b350e235-2832-45df-9918-6cab4f7578f7.c000.snappy.parquet',
    objectKey: 'x=A%2FA/part-00007-b350e235-2832-45df-9918-6cab4f7578f7.c000.snappy.parquet',
  },
  // A Spark string partition value with a space.
  { addPath: 'city=New%20York/part-0.parquet', objectKey: 'city=New York/part-0.parquet' },
  // delta-rs tests/data with a data file prefix that contains '%'.
  {
    addPath:
      'test%25file%25prefix-part-00000-24104cdb-691b-4410-a2a1-afc84fe2ea18-c000.snappy.parquet',
    objectKey:
      'test%file%prefix-part-00000-24104cdb-691b-4410-a2a1-afc84fe2ea18-c000.snappy.parquet',
  },
  // Control: nothing to decode.
  { addPath: 'category=A/part-000.parquet', objectKey: 'category=A/part-000.parquet' },
];

const PUBLIC_ROOTS: PublicObjectStorageTableRoot[] = [
  parsePublicObjectStorageTableRoot({
    provider: 's3',
    tableUri: 's3://public-bucket/tables/events',
    region: 'us-east-1',
  }),
  parsePublicObjectStorageTableRoot({
    provider: 'gcs',
    tableUri: 'gs://public-bucket/tables/events',
  }),
  parsePublicObjectStorageTableRoot({
    provider: 'r2',
    tableUri: 'r2://public-bucket/tables/events',
    endpoint: 'https://data.example.com',
  }),
];

// S3, GCS and R2 percent-decode the request path exactly once to find the object key.
function objectKeyRequestedBy(root: PublicObjectStorageTableRoot, url: string): string {
  expect(url.startsWith(root.tableRootUrl)).toBe(true);
  return url.slice(root.tableRootUrl.length).split('/').map(decodeURIComponent).join('/');
}

describe('public object storage Delta data file URLs', () => {
  it.each(PUBLIC_ROOTS.map((root) => [root.provider, root] as const))(
    'addresses the physical object for URI-encoded add paths on %s',
    (_provider, root) => {
      for (const { addPath, objectKey } of ENCODED_DELTA_ADD_PATHS) {
        expect(objectKeyRequestedBy(root, publicObjectUrlForDeltaPath(root, addPath))).toBe(
          objectKey,
        );
      }
      expect(publicObjectUrlForDeltaPath(root, 'city=New%20York/part-0.parquet')).toBe(
        `${root.tableRootUrl}city%3DNew%20York/part-0.parquet`,
      );
    },
  );

  it.each([
    ['an absolute path', '/part-000.parquet'],
    ['an empty segment', 'category=A//part-000.parquet'],
    ['a trailing separator', 'category=A/'],
    ['a parent segment', '../part-000.parquet'],
    ['an encoded parent segment', '%2E%2E/part-000.parquet'],
    ['an encoded current segment', 'category=A/%2E/part-000.parquet'],
    ['an encoded separator', 'category=A%2F..%2Fsecret.parquet'],
    ['an encoded backslash', 'category=A/..%5Csecret.parquet'],
    ['a malformed escape', 'category=%E0%A4%A/part-000.parquet'],
    ['a bare percent sign', 'category=100%/part-000.parquet'],
  ])('rejects a Delta add path with %s', (_label, addPath) => {
    expect(() => publicObjectUrlForDeltaPath(PUBLIC_ROOTS[0]!, addPath)).toThrow(
      expect.objectContaining({ code: 'invalid_public_object_path' }),
    );
  });

  it('keeps the raw add path as identity and decodes it once for the data URL', async () => {
    const root = PUBLIC_ROOTS[0]!;
    const descriptor = await resolvePublicObjectStorageDescriptor({
      provider: 's3',
      tableUri: root.tableUri,
      region: 'us-east-1',
      fetch: async (input) => listingPage(input),
      resolveDeltaSnapshotFromManifest: async (_manifestJson, tableUri) =>
        JSON.stringify({
          table_uri: tableUri,
          snapshot_version: 0,
          active_files: ENCODED_DELTA_ADD_PATHS.map(({ addPath }) => ({
            path: addPath,
            size_bytes: 128,
          })),
        }),
    });

    expect(descriptor.activeFiles.map((file) => file.path)).toEqual(
      ENCODED_DELTA_ADD_PATHS.map(({ addPath }) => addPath),
    );
    expect(descriptor.activeFiles.map((file) => objectKeyRequestedBy(root, file.url))).toEqual(
      ENCODED_DELTA_ADD_PATHS.map(({ objectKey }) => objectKey),
    );
  });
});

// A listing page in the S3/GCS ListObjectsV2 shape, answered from the requested URL.
function listingPage(
  input: RequestInfo | URL,
  options: { firstVersion?: number; keys?: number; token?: string } = {},
): Response {
  const firstVersion = options.firstVersion ?? 0;
  const contents = Array.from(
    { length: options.keys ?? 1 },
    (_, index) =>
      `<Contents><Key>tables/events/_delta_log/${String(firstVersion + index).padStart(20, '0')}.json</Key><Size>100</Size></Contents>`,
  ).join('');
  const token = options.token
    ? `<NextContinuationToken>${options.token}</NextContinuationToken>`
    : '';
  return withResponseUrl(
    new Response(
      `<ListBucketResult><IsTruncated>${options.token ? 'true' : 'false'}</IsTruncated>${contents}${token}</ListBucketResult>`,
      { status: 200 },
    ),
    String(input),
  );
}

function withResponseUrl(response: Response, url: string, redirected = false): Response {
  Object.defineProperties(response, {
    url: { value: url, configurable: true },
    redirected: { value: redirected, configurable: true },
  });
  return response;
}

// Stops a listing that the code under test fails to bound, so a missing bound fails the test
// instead of hanging it.
function boundedFetch(handler: (input: RequestInfo | URL, request: number) => Response) {
  let requests = 0;
  const inits: Array<RequestInit | undefined> = [];
  const fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    requests += 1;
    inits.push(init);
    if (requests > 500) throw new Error('listing was not bounded');
    return handler(input, requests);
  }) as typeof globalThis.fetch;
  return { fetch, inits, requests: () => requests };
}

describe('public S3/GCS Delta log discovery bounds', () => {
  const s3Root = PUBLIC_ROOTS[0]!;
  const gcsRoot = PUBLIC_ROOTS[1]!;

  it('lists every page anonymously without following redirects', async () => {
    const listing = boundedFetch((input, request) =>
      listingPage(input, {
        firstVersion: (request - 1) * 1000,
        keys: 1000,
        token: request < 3 ? `page-${request + 1}` : undefined,
      }),
    );

    const manifest = await buildPublicDeltaLogManifest(gcsRoot, { fetch: listing.fetch });

    expect(manifest.objects).toHaveLength(3000);
    expect(manifest.list_request_count).toBe(3);
    for (const init of listing.inits) {
      expect(init).toMatchObject({ credentials: 'omit', redirect: 'error' });
    }
  });

  it.each([
    ['a cross-origin redirect', 'https://attacker.example/listing', true, /cross-origin redirect/],
    ['a same-origin redirect', 'https://storage.googleapis.com/other-bucket', true, /redirect/],
    ['an unverifiable response URL', '', false, /unverifiable response URL/],
  ])('rejects %s', async (_label, responseUrl, redirected, message) => {
    const listing = boundedFetch(() =>
      withResponseUrl(listingPage('https://unused.example/'), responseUrl, redirected),
    );

    await expect(buildPublicDeltaLogManifest(gcsRoot, { fetch: listing.fetch })).rejects.toThrow(
      message,
    );
  });

  it('rejects a repeated continuation token', async () => {
    const listing = boundedFetch((input, request) =>
      listingPage(input, { firstVersion: request, token: 'same-token' }),
    );

    await expect(buildPublicDeltaLogManifest(s3Root, { fetch: listing.fetch })).rejects.toThrow(
      /repeated a continuation token/,
    );
    expect(listing.requests()).toBe(2);
  });

  it('rejects a listing beyond the browser object limit', async () => {
    const listing = boundedFetch((input, request) =>
      listingPage(input, {
        firstVersion: (request - 1) * 1000,
        keys: 1000,
        token: request < 60 ? `page-${request + 1}` : undefined,
      }),
    );

    await expect(buildPublicDeltaLogManifest(s3Root, { fetch: listing.fetch })).rejects.toThrow(
      /object limit/,
    );
    expect(listing.requests()).toBeLessThanOrEqual(51);
  });

  it('rejects a listing that never stops paginating', async () => {
    const listing = boundedFetch((input, request) =>
      listingPage(input, { keys: 0, token: `page-${request + 1}` }),
    );

    await expect(buildPublicDeltaLogManifest(s3Root, { fetch: listing.fetch })).rejects.toThrow(
      /maximum page count/,
    );
    expect(listing.requests()).toBeLessThanOrEqual(100);
  });

  it('stops reading a listing page beyond the maximum byte size', async () => {
    let pulledChunks = 0;
    const listing = boundedFetch((input) => {
      const body = new ReadableStream<Uint8Array>({
        pull(controller) {
          pulledChunks += 1;
          if (pulledChunks > 12) {
            controller.close();
            return;
          }
          controller.enqueue(new Uint8Array(1024 * 1024).fill(0x20));
        },
      });
      return withResponseUrl(new Response(body, { status: 200 }), String(input));
    });

    await expect(buildPublicDeltaLogManifest(s3Root, { fetch: listing.fetch })).rejects.toThrow(
      /maximum byte size/,
    );
    expect(pulledChunks).toBeLessThanOrEqual(10);
  });

  it('times out a listing that does not finish within its deadline', async () => {
    const fetch = ((_input: RequestInfo | URL, init?: RequestInit) =>
      new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener('abort', () =>
          reject(new DOMException('aborted', 'AbortError')),
        );
      })) as typeof globalThis.fetch;

    await expect(buildPublicDeltaLogManifest(s3Root, { fetch, timeoutMs: 20 })).rejects.toThrow(
      /timed out/,
    );
  });
});
