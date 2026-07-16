import { create, enumToJson, fromJson, toJson, type DescMessage } from '@bufbuild/protobuf';
import { timestampFromDate, timestampFromMs } from '@bufbuild/protobuf/wkt';
import { describe, expect, it } from 'vitest';
import {
  PageInfoSchema,
  PageRequestSchema,
  ProviderErrorCode,
  ProviderErrorSchema,
} from './protobuf/axon/common/v1/common_pb.ts';
import {
  ObjectGrantRangeRequestSchema,
  ObjectGrantSignedUrlSchema,
} from './protobuf/axon/dataaccess/v1/dataaccess_pb.ts';
import {
  FsBackendKind,
  FsBackendKindSchema,
  FsEntryKind,
  FsEntrySchema,
  FsRootRefSchema,
  ListDirectoryRequestSchema,
  ListDirectoryResponseSchema,
  ObjectReadResolutionSchema,
  ResolveReadRequestSchema,
  StatRequestSchema,
  StatResponseSchema,
  file_axon_fs_v1_fs,
  type FsEntry,
} from './protobuf/axon/fs/v1/fs_pb.ts';

describe('filesystem contract codegen', () => {
  it('projects representative object Contents and CommonPrefixes into filesystem entries', () => {
    // Existing object-storage fixtures use <Contents>. The inline
    // <CommonPrefixes> block documents the equivalent directory projection;
    // it is representative normalization, not legacy parser parity.
    const entries = representativeObjectListToFsEntries(
      `<ListBucketResult>
        <Contents>
          <Key>lake/reports/empty.parquet</Key>
          <LastModified>2026-07-15T14:30:00Z</LastModified>
          <Size>0</Size>
        </Contents>
        <CommonPrefixes>
          <Prefix>lake/reports/archive/</Prefix>
        </CommonPrefixes>
      </ListBucketResult>`,
      'lake/',
      'reports',
    );

    expect(entries).toHaveLength(2);
    expect(entries[0]).toMatchObject({
      name: 'empty.parquet',
      path: 'reports/empty.parquet',
      kind: FsEntryKind.FILE,
      sizeBytes: 0n,
    });
    expect(entries[0]?.contentType).toBeUndefined();
    expect(toJson(FsEntrySchema, entries[0]!)).toEqual({
      name: 'empty.parquet',
      path: 'reports/empty.parquet',
      kind: 'FS_ENTRY_KIND_FILE',
      sizeBytes: '0',
      modifiedAt: '2026-07-15T14:30:00Z',
    });

    expect(entries[1]).toMatchObject({
      name: 'archive',
      path: 'reports/archive',
      kind: FsEntryKind.DIRECTORY,
    });
    expect(entries[1]?.sizeBytes).toBeUndefined();
    expect(entries[1]?.modifiedAt).toBeUndefined();
    expect(entries[1]?.contentType).toBeUndefined();
  });

  it('rejects representative object-list descendants outside the requested directory', () => {
    expect(() =>
      representativeObjectListToFsEntries(
        `<ListBucketResult>
          <Contents>
            <Key>lake/reports/nested/deep.parquet</Key>
            <Size>1</Size>
          </Contents>
        </ListBucketResult>`,
        'lake/',
        'reports',
      ),
    ).toThrow('object listing returned a non-child path: reports/nested/deep.parquet');
  });

  it('normalizes local File metadata while preserving zero size and omitting empty MIME type', () => {
    const entry = representativeLocalFileToFsEntry(
      {
        name: 'empty.csv',
        size: 0,
        lastModified: Date.UTC(2026, 6, 15, 15, 4, 5, 123),
        type: '',
      },
      'imports',
    );
    const unknownSize = create(FsEntrySchema, {
      name: 'pending.bin',
      path: 'imports/pending.bin',
      kind: FsEntryKind.FILE,
    });

    expect(entry.sizeBytes).toBe(0n);
    expect(entry.contentType).toBeUndefined();
    expect(unknownSize.sizeBytes).toBeUndefined();
    expect(toJson(FsEntrySchema, entry)).toEqual({
      name: 'empty.csv',
      path: 'imports/empty.csv',
      kind: 'FS_ENTRY_KIND_FILE',
      sizeBytes: '0',
      modifiedAt: '2026-07-15T15:04:05.123Z',
    });
    expect(toJson(FsEntrySchema, unknownSize)).not.toHaveProperty('sizeBytes');
  });

  it('represents the root path, empty directories, and opaque pagination', () => {
    const root = create(FsRootRefSchema, {
      providerId: 'provider-1',
      rootId: 'root-opaque-1',
      backendKind: FsBackendKind.OBJECT_STORE_PREFIX,
    });
    const request = create(ListDirectoryRequestSchema, {
      root,
      path: '',
      page: create(PageRequestSchema, { cursor: '', pageSize: 0 }),
    });
    const empty = create(ListDirectoryResponseSchema, {
      entries: [],
      page: create(PageInfoSchema, { nextCursor: '', hasMore: false }),
    });
    const nextPage = create(ListDirectoryResponseSchema, {
      entries: [
        create(FsEntrySchema, {
          name: 'part-000.parquet',
          path: 'part-000.parquet',
          kind: FsEntryKind.FILE,
        }),
      ],
      page: create(PageInfoSchema, { nextCursor: 'opaque-page-2', hasMore: true }),
    });

    expect(request.path).toBe('');
    expect(request.page).toMatchObject({ cursor: '', pageSize: 0 });
    expect(empty.entries).toEqual([]);
    expect(empty.page).toMatchObject({ nextCursor: '', hasMore: false });
    expect(toJson(ListDirectoryResponseSchema, nextPage)).toMatchObject({
      entries: [{ name: 'part-000.parquet', path: 'part-000.parquet' }],
      page: { nextCursor: 'opaque-page-2', hasMore: true },
    });
  });

  it('round-trips stat requests and normalized entries', () => {
    const root = create(FsRootRefSchema, {
      providerId: 'local-files',
      rootId: 'selected-folder-1',
      backendKind: FsBackendKind.LOCAL_FOLDER,
    });
    const request = create(StatRequestSchema, {
      root,
      path: 'reports/summary.pdf',
    });
    const response = create(StatResponseSchema, {
      entry: create(FsEntrySchema, {
        name: 'summary.pdf',
        path: 'reports/summary.pdf',
        kind: FsEntryKind.FILE,
        sizeBytes: 4096n,
        contentType: 'application/pdf',
      }),
    });
    const requestJson = toJson(StatRequestSchema, request);
    const responseJson = toJson(StatResponseSchema, response);

    expect(toJson(StatRequestSchema, fromJson(StatRequestSchema, requestJson))).toEqual(
      requestJson,
    );
    expect(toJson(StatResponseSchema, fromJson(StatResponseSchema, responseJson))).toEqual(
      responseJson,
    );
    expect(response.entry?.name).toBe(response.entry?.path.split('/').at(-1));
  });

  it('exposes every backend kind with canonical protobuf enum JSON', () => {
    const expected = [
      [FsBackendKind.UNSPECIFIED, 'FS_BACKEND_KIND_UNSPECIFIED'],
      [FsBackendKind.UNITY_CATALOG_VOLUME, 'FS_BACKEND_KIND_UNITY_CATALOG_VOLUME'],
      [FsBackendKind.OBJECT_STORE_PREFIX, 'FS_BACKEND_KIND_OBJECT_STORE_PREFIX'],
      [FsBackendKind.LOCAL_FOLDER, 'FS_BACKEND_KIND_LOCAL_FOLDER'],
      [FsBackendKind.DOCUMENT, 'FS_BACKEND_KIND_DOCUMENT'],
    ] as const;

    for (const [backendKind, symbol] of expected) {
      const root = create(FsRootRefSchema, {
        providerId: 'provider-1',
        rootId: `root-${backendKind}`,
        backendKind,
      });

      expect(root.backendKind).toBe(backendKind);
      expect(enumToJson(FsBackendKindSchema, backendKind)).toBe(symbol);
    }
  });

  it('round-trips signed URL resolutions with decimal uint64 expiry JSON', () => {
    const resolution = create(ObjectReadResolutionSchema, {
      resolution: {
        case: 'signedUrl',
        value: create(ObjectGrantSignedUrlSchema, {
          path: 'reports/summary.pdf',
          url: 'https://signed.example.test/summary.pdf?sig=redacted',
          expiresAtEpochMs: 1_800_000_000_000n,
        }),
      },
    });
    const json = toJson(ObjectReadResolutionSchema, resolution);
    const decoded = fromJson(ObjectReadResolutionSchema, json);

    expect(json).toEqual({
      signedUrl: {
        path: 'reports/summary.pdf',
        url: 'https://signed.example.test/summary.pdf?sig=redacted',
        expiresAtEpochMs: '1800000000000',
      },
    });
    expect(decoded.resolution.case).toBe('signedUrl');
  });

  it('preserves proxy start zero through request and resolution JSON round trips', () => {
    const request = create(ResolveReadRequestSchema, {
      root: create(FsRootRefSchema, {
        providerId: 'brokered-files',
        rootId: 'volume-1',
        backendKind: FsBackendKind.UNITY_CATALOG_VOLUME,
      }),
      path: 'reports/summary.pdf',
      start: 0n,
      end: 16n,
    });
    const resolution = create(ObjectReadResolutionSchema, {
      resolution: {
        case: 'proxyRange',
        value: create(ObjectGrantRangeRequestSchema, {
          path: 'reports/summary.pdf',
          start: 0n,
          end: 16n,
        }),
      },
    });
    const requestJson = toJson(ResolveReadRequestSchema, request);
    const resolutionJson = toJson(ObjectReadResolutionSchema, resolution);
    const decodedRequest = fromJson(ResolveReadRequestSchema, requestJson);
    const decodedResolution = fromJson(ObjectReadResolutionSchema, resolutionJson);

    expect(requestJson).toMatchObject({ start: '0', end: '16' });
    expect(resolutionJson).toEqual({
      proxyRange: { path: 'reports/summary.pdf', start: '0', end: '16' },
    });
    expect(decodedRequest.start).toBe(0n);
    expect(decodedResolution.resolution.case).toBe('proxyRange');
    if (decodedResolution.resolution.case !== 'proxyRange') {
      throw new Error('expected proxy range resolution');
    }
    expect(decodedResolution.resolution.value.start).toBe(0n);
  });

  it('represents one-sided-range rejection with a structured INVALID denial', () => {
    const request = create(ResolveReadRequestSchema, {
      root: create(FsRootRefSchema, {
        providerId: 'brokered-files',
        rootId: 'volume-1',
        backendKind: FsBackendKind.UNITY_CATALOG_VOLUME,
      }),
      path: 'reports/summary.pdf',
      start: 0n,
    });
    const resolution = create(ObjectReadResolutionSchema, {
      resolution: {
        case: 'denied',
        value: create(ProviderErrorSchema, {
          code: ProviderErrorCode.INVALID,
          message: 'read ranges require both start and end',
          correlationId: 'corr-fs-123',
        }),
      },
    });
    const requestJson = toJson(ResolveReadRequestSchema, request);
    const json = toJson(ObjectReadResolutionSchema, resolution);
    const decoded = fromJson(ObjectReadResolutionSchema, json);

    expect(requestJson).toEqual({
      root: {
        providerId: 'brokered-files',
        rootId: 'volume-1',
        backendKind: 'FS_BACKEND_KIND_UNITY_CATALOG_VOLUME',
      },
      path: 'reports/summary.pdf',
      start: '0',
    });
    expect(request.end).toBeUndefined();
    expect(json).toEqual({
      denied: {
        code: 'PROVIDER_ERROR_CODE_INVALID',
        message: 'read ranges require both start and end',
        correlationId: 'corr-fs-123',
      },
    });
    expect(decoded.resolution.case).toBe('denied');
    if (decoded.resolution.case !== 'denied') {
      throw new Error('expected denied resolution');
    }
    expect(decoded.resolution.value).toMatchObject({
      code: ProviderErrorCode.INVALID,
      message: 'read ranges require both start and end',
      correlationId: 'corr-fs-123',
    });
  });

  it('keeps metadata secret-free, uses lowerCamel JSON names, and defines no service', () => {
    expect(jsonNames(FsRootRefSchema)).toEqual({
      provider_id: 'providerId',
      root_id: 'rootId',
      backend_kind: 'backendKind',
    });
    expect(jsonNames(FsEntrySchema)).toEqual({
      name: 'name',
      path: 'path',
      kind: 'kind',
      size_bytes: 'sizeBytes',
      modified_at: 'modifiedAt',
      content_type: 'contentType',
    });
    expect(jsonNames(ObjectReadResolutionSchema)).toEqual({
      signed_url: 'signedUrl',
      proxy_range: 'proxyRange',
      denied: 'denied',
    });

    const metadataFieldNames = [
      FsRootRefSchema,
      FsEntrySchema,
      ListDirectoryRequestSchema,
      ListDirectoryResponseSchema,
      ResolveReadRequestSchema,
      StatRequestSchema,
      StatResponseSchema,
    ]
      .flatMap((schema) => schema.fields.map((field) => field.name))
      .join(' ');

    expect(metadataFieldNames).not.toMatch(
      /authorization|credential|password|secret|signed_url|token|url|handle|api_key/i,
    );
    expect(file_axon_fs_v1_fs.services).toHaveLength(0);
  });
});

type LocalFileMetadata = Pick<File, 'name' | 'size' | 'lastModified' | 'type'>;

function representativeLocalFileToFsEntry(file: LocalFileMetadata, directoryPath: string): FsEntry {
  const path = [directoryPath, file.name].filter(Boolean).join('/');
  return create(FsEntrySchema, {
    name: file.name,
    path,
    kind: FsEntryKind.FILE,
    sizeBytes: BigInt(file.size),
    modifiedAt: timestampFromMs(file.lastModified),
    contentType: file.type || undefined,
  });
}

function representativeObjectListToFsEntries(
  xml: string,
  rootPrefix: string,
  directoryPath: string,
): FsEntry[] {
  const files = xmlBlocks(xml, 'Contents').map((block) => {
    const path = providerRelativePath(requiredXmlText(block, 'Key'), rootPrefix, false);
    assertImmediateChild(path, directoryPath);
    const size = optionalXmlText(block, 'Size');
    const lastModified = optionalXmlText(block, 'LastModified');
    return create(FsEntrySchema, {
      name: finalPathSegment(path),
      path,
      kind: FsEntryKind.FILE,
      sizeBytes: size === undefined ? undefined : unsignedDecimal(size),
      modifiedAt:
        lastModified === undefined ? undefined : timestampFromDate(validDate(lastModified)),
    });
  });
  const directories = xmlBlocks(xml, 'CommonPrefixes').map((block) => {
    const path = providerRelativePath(requiredXmlText(block, 'Prefix'), rootPrefix, true);
    assertImmediateChild(path, directoryPath);
    return create(FsEntrySchema, {
      name: finalPathSegment(path),
      path,
      kind: FsEntryKind.DIRECTORY,
    });
  });

  return [...files, ...directories];
}

function assertImmediateChild(path: string, directoryPath: string): void {
  const separator = path.lastIndexOf('/');
  const parentPath = separator < 0 ? '' : path.slice(0, separator);
  if (parentPath !== directoryPath) {
    throw new Error(`object listing returned a non-child path: ${path}`);
  }
}

function xmlBlocks(xml: string, tagName: string): string[] {
  return Array.from(
    xml.matchAll(new RegExp(`<${tagName}>([\\s\\S]*?)<\\/${tagName}>`, 'g')),
    (match) => match[1] ?? '',
  );
}

function requiredXmlText(xml: string, tagName: string): string {
  const value = optionalXmlText(xml, tagName);
  if (value === undefined) throw new Error(`object listing omitted ${tagName}`);
  return value;
}

function optionalXmlText(xml: string, tagName: string): string | undefined {
  const match = new RegExp(`<${tagName}>([\\s\\S]*?)<\\/${tagName}>`).exec(xml);
  const value = match?.[1]?.trim();
  return value ? decodeXmlEntities(value) : undefined;
}

function decodeXmlEntities(value: string): string {
  return value
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&');
}

function providerRelativePath(value: string, rootPrefix: string, directory: boolean): string {
  if (!value.startsWith(rootPrefix)) {
    throw new Error(`object path is outside root prefix: ${value}`);
  }
  const relative = value.slice(rootPrefix.length);
  const path = directory && relative.endsWith('/') ? relative.slice(0, -1) : relative;
  if (!path) throw new Error('object path cannot resolve to the root entry');
  return path;
}

function finalPathSegment(path: string): string {
  const name = path.split('/').at(-1);
  if (!name) throw new Error(`object path has no final segment: ${path}`);
  return name;
}

function unsignedDecimal(value: string): bigint {
  if (!/^\d+$/.test(value)) throw new Error(`invalid unsigned decimal: ${value}`);
  return BigInt(value);
}

function validDate(value: string): Date {
  const date = new Date(value);
  if (Number.isNaN(date.valueOf())) throw new Error(`invalid timestamp: ${value}`);
  return date;
}

function jsonNames(schema: DescMessage): Record<string, string> {
  return Object.fromEntries(schema.fields.map((field) => [field.name, field.jsonName]));
}
