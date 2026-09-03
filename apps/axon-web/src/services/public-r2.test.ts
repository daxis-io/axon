import { create } from '@bufbuild/protobuf';
import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import {
  BrowserHttpFileDescriptorSchema,
  BrowserHttpSnapshotDescriptorSchema,
} from '../generated/contracts/protobuf/axon/dataaccess/v1/dataaccess_pb.ts';
import {
  buildPublicDeltaLogManifest,
  clearPublicObjectStorageRuntimeCache,
  lookupPublicObjectStorageRuntimeCache,
  parsePublicDeltaLogIndexV1,
  parsePublicObjectStorageTableRoot,
  publicObjectStorageConnectionId,
  publicObjectUrl,
  registerPublicObjectStorageRuntimeCache,
  resolvePublicObjectStorageDescriptor,
} from './object-storage.ts';

const TABLE_URI = 'r2://axon-public-data/fixtures/onboarding-v1/table';
const ENDPOINT = 'https://data.axon.daxistech.io';

function root() {
  return parsePublicObjectStorageTableRoot({
    provider: 'r2',
    tableUri: TABLE_URI,
    endpoint: ENDPOINT,
  });
}

function index(objects: unknown[]) {
  return {
    schema_version: 1,
    table_uri: TABLE_URI,
    objects,
  };
}

describe('public R2 locator and endpoint', () => {
  it('normalizes the logical table root and account-scoped public endpoint identity', () => {
    const parsed = parsePublicObjectStorageTableRoot({
      provider: 'r2',
      tableUri: `${TABLE_URI}/`,
      endpoint: ' HTTPS://DATA.AXON.DAXISTECH.IO/ ',
    });

    expect(parsed).toEqual({
      provider: 'r2',
      tableUri: TABLE_URI,
      bucket: 'axon-public-data',
      prefix: 'fixtures/onboarding-v1/table',
      endpoint: ENDPOINT,
      tableRootUrl: `${ENDPOINT}/fixtures/onboarding-v1/table/`,
    });
    expect(publicObjectStorageConnectionId(parsed)).toBe(
      'axon-connection://public-r2/https%3A%2F%2Fdata.axon.daxistech.io/axon-public-data',
    );
    expect(publicObjectUrl(parsed, 'part-00000-c000.snappy.parquet')).toBe(
      `${ENDPOINT}/fixtures/onboarding-v1/table/part-00000-c000.snappy.parquet`,
    );
  });

  it.each([
    [undefined, undefined],
    ['http://data.axon.daxistech.io', undefined],
    ['https://user:password@data.axon.daxistech.io', undefined],
    ['https://data.axon.daxistech.io/path', undefined],
    ['https://data.axon.daxistech.io/path/..', undefined],
    ['https://data.axon.daxistech.io/%2e', undefined],
    ['https://data.axon.daxistech.io?token=secret', undefined],
    ['https://data.axon.daxistech.io#fragment', undefined],
    ['https://account.r2.cloudflarestorage.com', undefined],
    ['https://data.axon.daxistech.io', 'auto'],
  ])('rejects a non-public endpoint %s or R2 region %s', (endpoint, region) => {
    expect(() =>
      parsePublicObjectStorageTableRoot({
        provider: 'r2',
        tableUri: TABLE_URI,
        endpoint,
        region,
      }),
    ).toThrow(/R2|endpoint|region/i);
  });

  it.each([
    'r2://axon-public-data/fixtures/../private/table',
    'r2://axon-public-data/fixtures/%2e%2e/private/table',
    'r2://axon-public-data/fixtures%2Fprivate/table',
    'r2://axon-public-data/fixtures//private/table',
  ])('rejects an ambiguous or unsafe logical locator %s', (tableUri) => {
    expect(() =>
      parsePublicObjectStorageTableRoot({
        provider: 'r2',
        tableUri,
        endpoint: ENDPOINT,
      }),
    ).toThrow(/R2|path|URI/i);
  });
});

describe('PublicDeltaLogIndexV1', () => {
  it('publishes a closed JSON Schema for the index envelope and objects', () => {
    const schema = JSON.parse(
      readFileSync(
        new URL('../../schemas/public-delta-log-index-v1.schema.json', import.meta.url),
        'utf8',
      ),
    ) as {
      additionalProperties?: unknown;
      required?: unknown;
      properties?: { objects?: { items?: { additionalProperties?: unknown } } };
    };

    expect(schema.additionalProperties).toBe(false);
    expect(schema.required).toEqual(['schema_version', 'table_uri', 'objects']);
    expect(schema.properties?.objects?.items?.additionalProperties).toBe(false);
  });

  it('validates, derives URLs, and canonicalizes publisher ordering', () => {
    expect(
      parsePublicDeltaLogIndexV1(
        index([
          {
            relative_path: '_delta_log/_last_checkpoint',
            size_bytes: 36,
          },
          {
            relative_path: '_delta_log/00000000000000000000.json',
            size_bytes: 1722,
            etag: '"strong-etag"',
          },
        ]),
        root(),
      ),
    ).toEqual([
      {
        relative_path: '_delta_log/00000000000000000000.json',
        size_bytes: 1722,
        etag: '"strong-etag"',
        url: `${ENDPOINT}/fixtures/onboarding-v1/table/_delta_log/00000000000000000000.json`,
      },
      {
        relative_path: '_delta_log/_last_checkpoint',
        size_bytes: 36,
        url: `${ENDPOINT}/fixtures/onboarding-v1/table/_delta_log/_last_checkpoint`,
      },
    ]);
  });

  it.each([
    ['unknown envelope field', { ...index([]), credentials: 'secret' }],
    ['wrong schema version', { ...index([]), schema_version: 2 }],
    ['wrong table identity', { ...index([]), table_uri: 'r2://other/table' }],
    [
      'unknown object field',
      index([{ relative_path: '_delta_log/00000000000000000000.json', size_bytes: 1, url: 'x' }]),
    ],
    [
      'duplicate path',
      index([
        { relative_path: '_delta_log/00000000000000000000.json', size_bytes: 1 },
        { relative_path: '_delta_log/00000000000000000000.json', size_bytes: 1 },
      ]),
    ],
    ['absolute URL', index([{ relative_path: 'https://evil.example/log.json', size_bytes: 1 }])],
    ['traversal', index([{ relative_path: '_delta_log/../secret', size_bytes: 1 }])],
    [
      'encoded separator',
      index([{ relative_path: '_delta_log%2f00000000000000000000.json', size_bytes: 1 }]),
    ],
    ['backslash separator', index([{ relative_path: '_delta_log\\secret', size_bytes: 1 }])],
    ['non-log object', index([{ relative_path: 'part-00000.parquet', size_bytes: 1 }])],
    [
      'unsafe size',
      index([
        {
          relative_path: '_delta_log/00000000000000000000.json',
          size_bytes: Number.MAX_SAFE_INTEGER + 1,
        },
      ]),
    ],
    [
      'weak ETag',
      index([
        {
          relative_path: '_delta_log/00000000000000000000.json',
          size_bytes: 1,
          etag: 'W/"weak"',
        },
      ]),
    ],
    [
      'malformed strong ETag',
      index([
        {
          relative_path: '_delta_log/00000000000000000000.json',
          size_bytes: 1,
          etag: '"not"strong"',
        },
      ]),
    ],
    [
      'credential material',
      index([
        {
          relative_path: '_delta_log/aws_secret_access_key.json',
          size_bytes: 1,
        },
      ]),
    ],
  ])('rejects %s', (_label, value) => {
    expect(() => parsePublicDeltaLogIndexV1(value, root())).toThrow(/R2|index|Delta log/i);
  });
});

describe('public R2 index acquisition', () => {
  it('fetches one well-known index anonymously without listing and preserves metrics', async () => {
    const requests: Array<{ url: string; init?: RequestInit }> = [];
    const metrics: unknown[] = [];
    const descriptor = await resolvePublicObjectStorageDescriptor({
      provider: 'r2',
      tableUri: TABLE_URI,
      endpoint: ENDPOINT,
      fetch: async (input, init) => {
        requests.push({ url: String(input), init });
        return new Response(
          JSON.stringify(
            index([
              {
                relative_path: '_delta_log/00000000000000000000.json',
                size_bytes: 1722,
                etag: '"strong-log-etag"',
              },
            ]),
          ),
          { status: 200, headers: { 'content-type': 'application/json' } },
        );
      },
      onMetrics: (value) => metrics.push(value),
      resolveDeltaSnapshotFromManifest: async (manifestJson, tableUri) => {
        expect(tableUri).toBe(TABLE_URI);
        expect(JSON.parse(manifestJson)).toEqual({
          objects: [
            {
              relative_path: '_delta_log/00000000000000000000.json',
              size_bytes: 1722,
              etag: '"strong-log-etag"',
              url: `${ENDPOINT}/fixtures/onboarding-v1/table/_delta_log/00000000000000000000.json`,
            },
          ],
        });
        return JSON.stringify({
          table_uri: tableUri,
          snapshot_version: 0,
          active_files: [
            {
              path: 'data/part-00000.parquet',
              size_bytes: 128,
              partition_values: {},
            },
          ],
        });
      },
    });

    expect(requests).toEqual([
      {
        url: `${ENDPOINT}/fixtures/onboarding-v1/table/_axon/public-delta-log-index.json`,
        init: expect.objectContaining({
          credentials: 'omit',
          redirect: 'follow',
        }),
      },
    ]);
    expect(new URL(requests[0]!.url).search).toBe('');
    expect(descriptor.activeFiles[0]?.url).toBe(
      `${ENDPOINT}/fixtures/onboarding-v1/table/data/part-00000.parquet`,
    );
    expect(metrics).toEqual([
      expect.objectContaining({
        descriptor_resolution_count: 1,
        delta_log_manifest_list_count: 1,
        snapshot_resolve_count: 1,
      }),
    ]);
  });

  it('returns an actionable error for a missing index', async () => {
    await expect(
      buildPublicDeltaLogManifest(root(), {
        fetch: async () => new Response('missing', { status: 404 }),
      }),
    ).rejects.toThrow(/public-delta-log-index\.json|publish/i);
  });

  it('returns an actionable error when the index is stale or incomplete', async () => {
    await expect(
      resolvePublicObjectStorageDescriptor({
        provider: 'r2',
        tableUri: TABLE_URI,
        endpoint: ENDPOINT,
        fetch: async () =>
          new Response(
            JSON.stringify(
              index([
                {
                  relative_path: '_delta_log/00000000000000000000.json',
                  size_bytes: 1722,
                },
              ]),
            ),
            { status: 200 },
          ),
        resolveDeltaSnapshotFromManifest: async () => {
          throw new Error('missing _delta_log/00000000000000000001.json?X-Amz-Credential=secret');
        },
      }),
    ).rejects.toThrow(/R2.*index.*stale|stale.*index/i);
  });

  it('treats an invalid snapshot response as a stale index without echoing it', async () => {
    const secret = 'TOP-SECRET-SNAPSHOT';
    await expect(
      resolvePublicObjectStorageDescriptor({
        provider: 'r2',
        tableUri: TABLE_URI,
        endpoint: ENDPOINT,
        fetch: async () =>
          new Response(
            JSON.stringify(
              index([
                {
                  relative_path: '_delta_log/00000000000000000000.json',
                  size_bytes: 1722,
                },
              ]),
            ),
            { status: 200 },
          ),
        resolveDeltaSnapshotFromManifest: async () => `{${secret}`,
      }),
    ).rejects.toThrow(/R2.*index.*stale|stale.*index/i);
  });

  it('rejects a cross-origin redirect before consuming index data', async () => {
    await expect(
      buildPublicDeltaLogManifest(root(), {
        fetch: async () =>
          ({
            ok: true,
            status: 200,
            redirected: true,
            url: 'https://evil.example/public-delta-log-index.json',
            text: async () => JSON.stringify(index([])),
          }) as Response,
      }),
    ).rejects.toThrow(/cross-origin redirect/i);
  });

  it('preserves cancellation after the anonymous index request', async () => {
    const controller = new AbortController();
    await expect(
      buildPublicDeltaLogManifest(root(), {
        signal: controller.signal,
        fetch: async () => {
          controller.abort();
          return new Response(JSON.stringify(index([])), { status: 200 });
        },
      }),
    ).rejects.toMatchObject({ name: 'AbortError' });
  });
});

describe('public R2 descriptor cache identity', () => {
  it('isolates the same bucket and table prefix by endpoint origin', () => {
    clearPublicObjectStorageRuntimeCache();
    const descriptor = create(BrowserHttpSnapshotDescriptorSchema, {
      tableUri: TABLE_URI,
      snapshotVersion: 0n,
      activeFiles: [
        create(BrowserHttpFileDescriptorSchema, {
          path: 'data/part.parquet',
          url: `${ENDPOINT}/fixtures/onboarding-v1/table/data/part.parquet`,
          sizeBytes: 128n,
        }),
      ],
    });

    expect(
      registerPublicObjectStorageRuntimeCache({
        provider: 'r2',
        tableUri: TABLE_URI,
        endpoint: ENDPOINT,
        snapshot: { kind: 'latest' },
        descriptor,
        preflight: [
          {
            path: 'data/part.parquet',
            url: `${ENDPOINT}/fixtures/onboarding-v1/table/data/part.parquet`,
            size_bytes: 128,
            object_etag: '"part-v1"',
          },
        ],
      }),
    ).toBe(true);

    expect(
      lookupPublicObjectStorageRuntimeCache({
        provider: 'r2',
        tableUri: TABLE_URI,
        endpoint: 'https://qualification.example.com',
        snapshot: { kind: 'latest' },
      }),
    ).toBeUndefined();
    expect(
      lookupPublicObjectStorageRuntimeCache({
        provider: 'r2',
        tableUri: TABLE_URI,
        endpoint: ENDPOINT,
        snapshot: { kind: 'latest' },
      })?.descriptor.snapshotVersion,
    ).toBe(0n);
    clearPublicObjectStorageRuntimeCache();
  });
});
