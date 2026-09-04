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

function responseAt(url: string, body?: BodyInit | null, init?: ResponseInit): Response {
  const response = new Response(body, init);
  Object.defineProperty(response, 'url', { value: url });
  return response;
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
    'r2://axon-public-data/fixtures/%ff/private/table',
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
      'x-axon-semantic-constraints'?: unknown;
      required?: unknown;
      properties?: {
        table_uri?: { pattern?: string };
        objects?: {
          maxItems?: number;
          uniqueItems?: boolean;
          items?: {
            additionalProperties?: unknown;
            properties?: { relative_path?: { pattern?: string } };
          };
        };
      };
    };

    expect(schema.additionalProperties).toBe(false);
    expect(schema['x-axon-semantic-constraints']).toEqual([
      'objects.relative_path values are unique',
      'percent escapes decode as valid UTF-8',
    ]);
    expect(schema.required).toEqual(['schema_version', 'table_uri', 'objects']);
    expect(schema.properties?.objects?.items?.additionalProperties).toBe(false);
    expect(schema.properties?.objects).toMatchObject({
      maxItems: 50_000,
      uniqueItems: true,
    });

    const tableUriPattern = new RegExp(schema.properties?.table_uri?.pattern ?? '');
    expect(tableUriPattern.test(TABLE_URI)).toBe(true);
    expect(tableUriPattern.test('r2://axon-public-data/fixtures/../private/table')).toBe(false);
    expect(tableUriPattern.test('r2://axon-public-data/fixtures/%2e%2e/private/table')).toBe(false);
    expect(tableUriPattern.test('r2://axon-public-data/fixtures/.%2e/private/table')).toBe(false);
    expect(tableUriPattern.test('r2://axon-public-data/fixtures//table')).toBe(false);
    expect(tableUriPattern.test('r2://axon-public-data/fixtures/%zz/table')).toBe(false);

    const pathPattern = new RegExp(
      schema.properties?.objects?.items?.properties?.relative_path?.pattern ?? '',
    );
    expect(pathPattern.test('_delta_log/00000000000000000000.json')).toBe(true);
    expect(pathPattern.test('_delta_log/_sidecars/part.parquet')).toBe(true);
    expect(pathPattern.test('_delta_log/')).toBe(false);
    expect(pathPattern.test('_delta_log/../secret')).toBe(false);
    expect(pathPattern.test('_delta_log/%2e%2e/secret')).toBe(false);
    expect(pathPattern.test('_delta_log/.%2e/secret')).toBe(false);
    expect(pathPattern.test('_delta_log/a//b')).toBe(false);
    expect(pathPattern.test('_delta_log/%zz.json')).toBe(false);
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

  it('accepts percent escapes only when they decode as valid UTF-8', () => {
    expect(
      parsePublicDeltaLogIndexV1(
        index([{ relative_path: '_delta_log/%C3%A9.json', size_bytes: 1 }]),
        root(),
      )[0]?.relative_path,
    ).toBe('_delta_log/%C3%A9.json');
  });

  it.each([
    ['empty object list', index([])],
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
    ['encoded traversal', index([{ relative_path: '_delta_log/%2e%2e/secret', size_bytes: 1 }])],
    ['invalid UTF-8 escape', index([{ relative_path: '_delta_log/%ff.json', size_bytes: 1 }])],
    ['truncated UTF-8 escape', index([{ relative_path: '_delta_log/%c3.json', size_bytes: 1 }])],
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

  it('rejects indexes whose object count exceeds the browser resource budget', () => {
    const objects = Array.from({ length: 50_001 }, (_, version) => ({
      relative_path: `_delta_log/${version.toString().padStart(20, '0')}.json`,
      size_bytes: 1,
    }));

    expect(() => parsePublicDeltaLogIndexV1(index(objects), root())).toThrow(/object count|large/i);
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
        return responseAt(
          String(input),
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
          redirect: 'error',
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
        fetch: async (input) => responseAt(String(input), 'missing', { status: 404 }),
      }),
    ).rejects.toThrow(/public-delta-log-index\.json|publish/i);
  });

  it('returns an actionable error when the index is stale or incomplete', async () => {
    await expect(
      resolvePublicObjectStorageDescriptor({
        provider: 'r2',
        tableUri: TABLE_URI,
        endpoint: ENDPOINT,
        fetch: async (input) =>
          responseAt(
            String(input),
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
          throw new Error(
            JSON.stringify({
              code: 'invalid_request',
              message:
                "delta log replay expected commit file '_delta_log/00000000000000000001.json'",
              target: 'browser_wasm',
            }),
          );
        },
      }),
    ).rejects.toThrow(/R2.*index.*stale|stale.*index/i);
  });

  it('classifies an invalid snapshot response as a sanitized runtime failure', async () => {
    const secret = 'TOP-SECRET-SNAPSHOT';
    let failure: unknown;
    try {
      await resolvePublicObjectStorageDescriptor({
        provider: 'r2',
        tableUri: TABLE_URI,
        endpoint: ENDPOINT,
        fetch: async (input) =>
          responseAt(
            String(input),
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
      });
    } catch (error) {
      failure = error;
    }
    expect(failure).toBeInstanceOf(Error);
    expect((failure as Error).message).toMatch(/snapshot resolution failed/i);
    expect((failure as Error).message).not.toMatch(/stale|incomplete/i);
    expect((failure as Error).message).not.toContain(secret);
  });

  it('does not relabel an internal resolver error as a stale publication', async () => {
    const secret = 'AWS_SECRET_ACCESS_KEY=do-not-echo';
    await expect(
      resolvePublicObjectStorageDescriptor({
        provider: 'r2',
        tableUri: TABLE_URI,
        endpoint: ENDPOINT,
        fetch: async (input) =>
          responseAt(
            String(input),
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
          throw new Error(`internal resolver failure: ${secret}`);
        },
      }),
    ).rejects.toThrow(/snapshot resolution failed/i);
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

  it('rejects a same-origin redirect instead of accepting a substituted index path', async () => {
    await expect(
      buildPublicDeltaLogManifest(root(), {
        fetch: async () =>
          ({
            ok: true,
            status: 200,
            redirected: true,
            url: `${ENDPOINT}/fixtures/onboarding-v1/table/_axon/substituted.json`,
            text: async () => JSON.stringify(index([])),
          }) as Response,
      }),
    ).rejects.toThrow(/redirect/i);
  });

  it('rejects an index response whose exact URL cannot be verified', async () => {
    await expect(
      buildPublicDeltaLogManifest(root(), {
        fetch: async () =>
          new Response(
            JSON.stringify(
              index([
                {
                  relative_path: '_delta_log/00000000000000000000.json',
                  size_bytes: 1,
                },
              ]),
            ),
            { status: 200 },
          ),
      }),
    ).rejects.toThrow(/unverifiable|response URL/i);
  });

  it('aborts a stalled index request at the connection-test deadline', async () => {
    const fetch = async (_input: RequestInfo | URL, init?: RequestInit): Promise<Response> =>
      await new Promise((_resolve, reject) => {
        init?.signal?.addEventListener('abort', () => reject(init.signal?.reason), { once: true });
      });

    await expect(buildPublicDeltaLogManifest(root(), { fetch, timeoutMs: 1 })).rejects.toThrow(
      /timed out/i,
    );
  });

  it('keeps the connection-test deadline active while the index body streams', async () => {
    const indexUrl = `${ENDPOINT}/fixtures/onboarding-v1/table/_axon/public-delta-log-index.json`;
    let cancelled = false;
    const body = new ReadableStream<Uint8Array>({
      pull() {
        // Remain pending until the connection deadline aborts the reader.
      },
      cancel() {
        cancelled = true;
      },
    });

    await expect(
      Promise.race([
        buildPublicDeltaLogManifest(root(), {
          fetch: async () => responseAt(indexUrl, body, { status: 200 }),
          timeoutMs: 1,
        }),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error('index body timeout did not reject promptly')), 250),
        ),
      ]),
    ).rejects.toThrow(/timed out/i);
    expect(cancelled).toBe(true);
  });

  it('preserves cancellation after the anonymous index request', async () => {
    const controller = new AbortController();
    await expect(
      buildPublicDeltaLogManifest(root(), {
        signal: controller.signal,
        fetch: async (input) => {
          controller.abort();
          return responseAt(String(input), JSON.stringify(index([])), { status: 200 });
        },
      }),
    ).rejects.toMatchObject({ name: 'AbortError' });
  });

  it('rejects an oversized index from declared or streamed bytes before snapshot resolution', async () => {
    const resolve = async () => {
      throw new Error('snapshot resolution must not run');
    };
    await expect(
      buildPublicDeltaLogManifest(root(), {
        fetch: async (input) =>
          responseAt(String(input), '{}', {
            status: 200,
            headers: { 'content-length': String(8 * 1024 * 1024 + 1) },
          }),
      }),
    ).rejects.toThrow(/large|size/i);

    await expect(
      resolvePublicObjectStorageDescriptor({
        provider: 'r2',
        tableUri: TABLE_URI,
        endpoint: ENDPOINT,
        fetch: async (input) =>
          responseAt(String(input), 'x'.repeat(8 * 1024 * 1024 + 1), { status: 200 }),
        resolveDeltaSnapshotFromManifest: resolve,
      }),
    ).rejects.toThrow(/large|size/i);
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
