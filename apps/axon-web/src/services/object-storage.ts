import type {
  BrowserHttpFileDescriptor,
  BrowserHttpSnapshotDescriptor,
  CapabilityReport,
  PartitionColumnType,
} from '../axon-browser-sdk.ts';

export type PublicObjectStorageProvider = 'gcs' | 's3';

export type PublicObjectStorageTableRoot = {
  provider: PublicObjectStorageProvider;
  tableUri: string;
  bucket: string;
  prefix: string;
  region?: string;
  tableRootUrl: string;
};

export type PublicObjectStorageErrorCode =
  | 'invalid_public_object_storage_uri'
  | 'invalid_public_object_path'
  | 'public_storage_access_failed';

export class PublicObjectStorageError extends Error {
  readonly code: PublicObjectStorageErrorCode;

  constructor(code: PublicObjectStorageErrorCode, message: string) {
    super(message);
    this.name = 'PublicObjectStorageError';
    this.code = code;
  }
}

export type PublicDeltaLogManifestObject = {
  relative_path: string;
  url: string;
  size_bytes?: number;
  etag?: string;
};

export type PublicDeltaLogManifest = {
  tableUri: string;
  objects: PublicDeltaLogManifestObject[];
  list_request_count: number;
  list_duration_ms: number;
};

export type PublicObjectStorageFetch = typeof fetch;

export type PublicObjectStorageDescriptorResolutionMetrics = {
  descriptor_resolution_count: number;
  delta_log_manifest_list_count: number;
  delta_log_manifest_list_duration_ms: number;
  snapshot_resolve_count: number;
  snapshot_resolve_duration_ms: number;
};

export type PublicObjectStorageRuntimeCacheSnapshot =
  { kind: 'latest' } | { kind: 'version'; version: number };

export type PublicObjectStoragePreflightResult = Array<{
  path: string;
  url: string;
  size_bytes: number;
  object_etag?: string;
}>;

export type PublicObjectStorageRuntimeCacheIdentity = {
  path: string;
  size_bytes: number;
  object_etag: string;
};

export type PublicObjectStorageRuntimeCacheEntry = {
  descriptor: BrowserHttpSnapshotDescriptor;
  identity: PublicObjectStorageRuntimeCacheIdentity;
  expiresAtEpochMs: number;
};

type PublicObjectStorageFetchOptions = {
  fetch?: PublicObjectStorageFetch;
};

const DEFAULT_RUNTIME_CACHE_TTL_MS = 2 * 60 * 1000;
const publicObjectStorageRuntimeCache = new Map<string, PublicObjectStorageRuntimeCacheEntry>();

type ResolvedPublicSnapshot = {
  table_uri: string;
  snapshot_version: number;
  partition_column_types?: Partial<Record<string, PartitionColumnType>>;
  browser_compatibility?: CapabilityReport;
  required_capabilities?: CapabilityReport;
  active_files: Array<{
    path: string;
    size_bytes: number;
    partition_values?: Record<string, string | null>;
    stats?: string;
  }>;
};

export function parsePublicObjectStorageTableRoot(input: {
  provider: PublicObjectStorageProvider;
  tableUri: string;
  region?: string;
}): PublicObjectStorageTableRoot {
  const trimmed = input.tableUri.trim().replace(/\/+$/, '');
  if (containsSecretMaterial(trimmed)) {
    throw invalidUri('public object storage table URI must not contain credential material');
  }

  let parsed: URL;
  try {
    parsed = new URL(trimmed);
  } catch (error) {
    throw invalidUri(
      `invalid public object storage table URI: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }

  if (!parsed.hostname || hasUserinfo(parsed) || parsed.search || parsed.hash) {
    throw invalidUri(providerUriShapeMessage(input.provider));
  }

  const prefix = normalizeObjectPath(parsed.pathname);
  if (!prefix) {
    throw invalidUri('public object storage table URI must include a table path');
  }

  const bucket = parsed.hostname;
  if (input.provider === 'gcs') {
    if (parsed.protocol !== 'gs:') {
      throw invalidUri(providerUriShapeMessage(input.provider));
    }
    return {
      provider: input.provider,
      tableUri: `gs://${bucket}/${prefix}`,
      bucket,
      prefix,
      tableRootUrl: `https://storage.googleapis.com/${encodeObjectPath(bucket)}/${encodeObjectPath(
        prefix,
      )}/`,
    };
  }

  if (input.provider === 's3') {
    if (parsed.protocol !== 's3:') {
      throw invalidUri(providerUriShapeMessage(input.provider));
    }
    const bucket = normalizeS3BucketForVirtualHostedHttps(parsed.hostname, parsed.port);
    const region = normalizeS3Region(input.region);
    return {
      provider: input.provider,
      tableUri: `s3://${bucket}/${prefix}`,
      bucket,
      prefix,
      ...(region ? { region } : {}),
      tableRootUrl: `${s3BucketOrigin(bucket, region)}/${encodeObjectPath(prefix)}/`,
    };
  }

  const unsupportedProvider: never = input.provider;
  throw invalidUri(`unsupported public object storage provider: ${String(unsupportedProvider)}`);
}

export function publicObjectUrl(root: PublicObjectStorageTableRoot, relativePath: string): string {
  if (relativePath.startsWith('/')) {
    throw invalidPath('public object relative path must stay inside the table root');
  }
  const normalized = normalizeObjectPath(relativePath);
  if (!normalized || normalized !== relativePath.replace(/^\/+|\/+$/g, '')) {
    throw invalidPath('public object relative path must stay inside the table root');
  }
  return `${root.tableRootUrl}${encodeObjectPath(normalized)}`;
}

export async function buildPublicDeltaLogManifest(
  root: PublicObjectStorageTableRoot,
  options: PublicObjectStorageFetchOptions = {},
): Promise<PublicDeltaLogManifest> {
  const fetcher = options.fetch ?? globalThis.fetch;
  if (typeof fetcher !== 'function') {
    throw accessFailed('global fetch is not available for public object storage');
  }

  const objects: PublicDeltaLogManifestObject[] = [];
  let continuationToken: string | undefined;
  let listRequestCount = 0;
  const listStartedAt = nowMs();

  do {
    listRequestCount += 1;
    const response = await fetcher(publicObjectStorageListUrl(root, continuationToken), {
      credentials: 'omit',
    });
    if (!response.ok) {
      throw accessFailed(
        `public object storage Delta log listing failed (HTTP ${response.status})`,
      );
    }

    const page = parseObjectStorageListResponse(await response.text());
    objects.push(...page.keys.map((entry) => deltaLogObjectFromListEntry(root, entry)));
    continuationToken = page.nextContinuationToken;
  } while (continuationToken);

  if (objects.length === 0) {
    throw accessFailed('public object storage table root did not expose Delta log objects');
  }

  return {
    tableUri: root.tableUri,
    objects,
    list_request_count: listRequestCount,
    list_duration_ms: Math.round(nowMs() - listStartedAt),
  };
}

export async function resolvePublicObjectStorageDescriptor(input: {
  provider: PublicObjectStorageProvider;
  tableUri: string;
  region?: string;
  resolveDeltaSnapshotFromManifest: (manifestJson: string, tableUri: string) => Promise<string>;
  fetch?: PublicObjectStorageFetch;
  onMetrics?: (metrics: PublicObjectStorageDescriptorResolutionMetrics) => void;
}): Promise<BrowserHttpSnapshotDescriptor> {
  const root = parsePublicObjectStorageTableRoot({
    provider: input.provider,
    tableUri: input.tableUri,
    region: input.region,
  });
  const manifest = await buildPublicDeltaLogManifest(root, { fetch: input.fetch });
  const snapshotResolveStartedAt = nowMs();
  const snapshot = JSON.parse(
    await input.resolveDeltaSnapshotFromManifest(
      JSON.stringify({ objects: manifest.objects }),
      root.tableUri,
    ),
  ) as ResolvedPublicSnapshot;
  input.onMetrics?.({
    descriptor_resolution_count: 1,
    delta_log_manifest_list_count: manifest.list_request_count,
    delta_log_manifest_list_duration_ms: manifest.list_duration_ms,
    snapshot_resolve_count: 1,
    snapshot_resolve_duration_ms: Math.round(nowMs() - snapshotResolveStartedAt),
  });

  if (snapshot.table_uri !== root.tableUri) {
    throw accessFailed('public object storage snapshot resolver returned a different table URI');
  }

  return {
    table_uri: root.tableUri,
    snapshot_version: snapshot.snapshot_version,
    partition_column_types: snapshot.partition_column_types ?? {},
    browser_compatibility: snapshot.browser_compatibility ?? { capabilities: {} },
    required_capabilities: snapshot.required_capabilities ?? { capabilities: {} },
    active_files: snapshot.active_files.map((file): BrowserHttpFileDescriptor => ({
      path: file.path,
      url: publicObjectUrl(root, file.path),
      size_bytes: file.size_bytes,
      partition_values: file.partition_values ?? {},
      stats: file.stats,
    })),
  };
}

export async function preflightPublicObjectStorageDescriptorRangeRead(input: {
  descriptor: BrowserHttpSnapshotDescriptor;
  preflightParquetMetadataForTargets: (targetsJson: string) => Promise<string>;
}): Promise<PublicObjectStoragePreflightResult> {
  const target = input.descriptor.active_files[0];
  if (!target) return [];

  try {
    return parsePreflightResult(
      await input.preflightParquetMetadataForTargets(
        JSON.stringify([
          {
            path: target.path,
            url: target.url,
            size_bytes: target.size_bytes,
            partition_values: target.partition_values,
            ...(target.stats === undefined ? {} : { stats: target.stats }),
          },
        ]),
      ),
    );
  } catch (error) {
    throw accessFailed(
      `public object storage active Parquet range-read failed: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }
}

export function registerPublicObjectStorageRuntimeCache(input: {
  provider: PublicObjectStorageProvider;
  tableUri: string;
  region?: string;
  snapshot: PublicObjectStorageRuntimeCacheSnapshot;
  descriptor: BrowserHttpSnapshotDescriptor;
  preflight: PublicObjectStoragePreflightResult;
  nowMs?: () => number;
  ttlMs?: number;
}): boolean {
  const root = parsePublicObjectStorageTableRoot({
    provider: input.provider,
    tableUri: input.tableUri,
    region: input.region,
  });
  if (input.descriptor.table_uri !== root.tableUri) return false;

  const firstFile = input.descriptor.active_files[0];
  const firstPreflight = input.preflight[0];
  if (!firstFile || !firstPreflight || firstFile.path !== firstPreflight.path) return false;
  if (firstFile.url !== firstPreflight.url) return false;
  if (firstFile.size_bytes !== firstPreflight.size_bytes) return false;

  const objectEtag = strongObjectEtag(firstPreflight.object_etag);
  if (!objectEtag) return false;
  const descriptor = cloneDescriptor(input.descriptor);
  descriptor.active_files = descriptor.active_files.map((file, index) =>
    index === 0 ? { ...file, object_etag: objectEtag } : file,
  );

  publicObjectStorageRuntimeCache.set(
    publicObjectStorageRuntimeCacheKey(root.provider, root.tableUri, input.snapshot, root.region),
    {
      descriptor,
      identity: {
        path: firstFile.path,
        size_bytes: firstFile.size_bytes,
        object_etag: objectEtag,
      },
      expiresAtEpochMs: (input.nowMs ?? Date.now)() + (input.ttlMs ?? DEFAULT_RUNTIME_CACHE_TTL_MS),
    },
  );
  return true;
}

export function lookupPublicObjectStorageRuntimeCache(input: {
  provider: PublicObjectStorageProvider;
  tableUri: string;
  region?: string;
  snapshot: PublicObjectStorageRuntimeCacheSnapshot;
  expectedSnapshotVersion?: number;
  nowMs?: () => number;
}): PublicObjectStorageRuntimeCacheEntry | undefined {
  const root = parsePublicObjectStorageTableRoot({
    provider: input.provider,
    tableUri: input.tableUri,
    region: input.region,
  });
  const key = publicObjectStorageRuntimeCacheKey(
    root.provider,
    root.tableUri,
    input.snapshot,
    root.region,
  );
  const entry = publicObjectStorageRuntimeCache.get(key);
  if (!entry) return undefined;

  if (entry.expiresAtEpochMs <= (input.nowMs ?? Date.now)()) {
    publicObjectStorageRuntimeCache.delete(key);
    return undefined;
  }
  if (
    input.expectedSnapshotVersion !== undefined &&
    entry.descriptor.snapshot_version !== input.expectedSnapshotVersion
  ) {
    return undefined;
  }

  return {
    descriptor: cloneDescriptor(entry.descriptor),
    identity: { ...entry.identity },
    expiresAtEpochMs: entry.expiresAtEpochMs,
  };
}

export function clearPublicObjectStorageRuntimeCache(): void {
  publicObjectStorageRuntimeCache.clear();
}

type ObjectStorageListEntry = {
  key: string;
  sizeBytes?: number;
  etag?: string;
};

type ObjectStorageListPage = {
  keys: ObjectStorageListEntry[];
  nextContinuationToken?: string;
};

function publicObjectStorageListUrl(
  root: PublicObjectStorageTableRoot,
  continuationToken: string | undefined,
) {
  const url =
    root.provider === 's3'
      ? new URL(`${s3BucketOrigin(root.bucket, root.region)}/`)
      : new URL(`https://storage.googleapis.com/${encodeObjectPath(root.bucket)}`);
  url.searchParams.set('list-type', '2');
  url.searchParams.set('prefix', `${root.prefix}/_delta_log/`);
  url.searchParams.set('max-keys', '1000');
  if (continuationToken) {
    url.searchParams.set('continuation-token', continuationToken);
  }
  return url.toString();
}

function deltaLogObjectFromListEntry(
  root: PublicObjectStorageTableRoot,
  entry: ObjectStorageListEntry,
): PublicDeltaLogManifestObject {
  const rootPrefix = `${root.prefix}/`;
  if (!entry.key.startsWith(rootPrefix)) {
    throw accessFailed('public object storage listing returned an object outside the table root');
  }
  const relativePath = entry.key.slice(rootPrefix.length);
  if (!relativePath.startsWith('_delta_log/')) {
    throw accessFailed('public object storage listing returned a non-Delta-log object');
  }
  const object: PublicDeltaLogManifestObject = {
    relative_path: relativePath,
    url: publicObjectUrl(root, relativePath),
  };
  if (entry.sizeBytes !== undefined) object.size_bytes = entry.sizeBytes;
  if (entry.etag !== undefined) object.etag = entry.etag;
  return object;
}

function parseObjectStorageListResponse(xml: string): ObjectStorageListPage {
  const domParser = globalThis.DOMParser;
  if (typeof domParser === 'function') {
    return parseObjectStorageListResponseWithDom(xml, domParser);
  }
  return parseObjectStorageListResponseWithRegex(xml);
}

function parseObjectStorageListResponseWithDom(
  xml: string,
  DomParser: typeof DOMParser,
): ObjectStorageListPage {
  const doc = new DomParser().parseFromString(xml, 'application/xml');
  if (doc.getElementsByTagName('parsererror').length > 0) {
    throw accessFailed('public object storage listing returned invalid XML');
  }
  const keys = Array.from(doc.getElementsByTagName('Contents')).map((contents) => ({
    key: requiredXmlText(contents, 'Key'),
    sizeBytes: optionalXmlNumber(contents, 'Size'),
    etag: optionalXmlText(contents, 'ETag'),
  }));
  return {
    keys,
    nextContinuationToken: optionalXmlText(doc.documentElement, 'NextContinuationToken'),
  };
}

function parseObjectStorageListResponseWithRegex(xml: string): ObjectStorageListPage {
  const contents = Array.from(xml.matchAll(/<Contents>([\s\S]*?)<\/Contents>/g)).map((match) => {
    const block = match[1] ?? '';
    return {
      key: requiredTagText(block, 'Key'),
      sizeBytes: optionalTagNumber(block, 'Size'),
      etag: optionalTagText(block, 'ETag'),
    };
  });
  return {
    keys: contents,
    nextContinuationToken: optionalTagText(xml, 'NextContinuationToken'),
  };
}

function requiredXmlText(element: Element, tagName: string): string {
  const text = optionalXmlText(element, tagName);
  if (!text) throw accessFailed(`public object storage listing omitted ${tagName}`);
  return text;
}

function optionalXmlText(element: Element, tagName: string): string | undefined {
  const text = element.getElementsByTagName(tagName)[0]?.textContent?.trim();
  return text ? decodeXmlEntities(text) : undefined;
}

function optionalXmlNumber(element: Element, tagName: string): number | undefined {
  const text = optionalXmlText(element, tagName);
  if (text === undefined) return undefined;
  const parsed = Number(text);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw accessFailed(`public object storage listing contained an invalid ${tagName}`);
  }
  return parsed;
}

function requiredTagText(xml: string, tagName: string): string {
  const text = optionalTagText(xml, tagName);
  if (!text) throw accessFailed(`public object storage listing omitted ${tagName}`);
  return text;
}

function optionalTagText(xml: string, tagName: string): string | undefined {
  const match = new RegExp(`<${tagName}>([\\s\\S]*?)<\\/${tagName}>`).exec(xml);
  return match?.[1] ? decodeXmlEntities(match[1].trim()) : undefined;
}

function optionalTagNumber(xml: string, tagName: string): number | undefined {
  const text = optionalTagText(xml, tagName);
  if (text === undefined) return undefined;
  const parsed = Number(text);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw accessFailed(`public object storage listing contained an invalid ${tagName}`);
  }
  return parsed;
}

function decodeXmlEntities(value: string): string {
  return value
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&');
}

function normalizeObjectPath(path: string): string {
  const parts = path.split('/').filter(Boolean);
  if (parts.some((part) => part === '.' || part === '..')) {
    throw invalidPath('public object relative path must not contain traversal segments');
  }
  return parts.join('/');
}

function encodeObjectPath(path: string): string {
  return path.split('/').map(encodeURIComponent).join('/');
}

function hasUserinfo(url: URL): boolean {
  return Boolean(url.username || url.password);
}

function containsSecretMaterial(value: string): boolean {
  const lower = value.toLowerCase();
  return (
    /akia[0-9a-z]{16}/i.test(value) ||
    lower.includes('x-goog-signature') ||
    lower.includes('x-goog-credential') ||
    lower.includes('x-amz-signature') ||
    lower.includes('x-amz-credential') ||
    lower.includes('x-amz-security-token') ||
    lower.includes('google_application_credentials') ||
    lower.includes('aws_access_key_id') ||
    lower.includes('aws_secret_access_key') ||
    lower.includes('aws_session_token') ||
    lower.includes('private_key') ||
    lower.includes('access_token') ||
    lower.includes('bearer')
  );
}

function providerUriShapeMessage(provider: PublicObjectStorageProvider): string {
  return provider === 's3'
    ? 'public object storage S3 table URI must look like s3://bucket/table'
    : 'public object storage GCS table URI must look like gs://bucket/table';
}

function normalizeS3Region(region: string | undefined): string {
  const normalized = region?.trim().toLowerCase();
  if (!normalized) {
    throw invalidUri('public object storage S3 region is required');
  }
  if (!/^[a-z]{2}(?:-[a-z]+)+-\d+$/.test(normalized)) {
    throw invalidUri('public object storage S3 region must be an AWS region identifier');
  }
  return normalized;
}

function normalizeS3BucketForVirtualHostedHttps(bucket: string, port: string): string {
  if (
    port ||
    bucket !== bucket.toLowerCase() ||
    !/^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/.test(bucket)
  ) {
    throw invalidUri(
      'public object storage S3 bucket must be DNS-compatible without dots for virtual-hosted HTTPS',
    );
  }
  return bucket;
}

function s3BucketOrigin(bucket: string, region: string | undefined): string {
  if (!region) {
    throw invalidUri('public object storage S3 region is required');
  }
  return `https://${bucket}.s3.${region}.amazonaws.com`;
}

function publicObjectStorageRuntimeCacheKey(
  provider: PublicObjectStorageProvider,
  tableUri: string,
  snapshot: PublicObjectStorageRuntimeCacheSnapshot,
  region?: string,
): string {
  const snapshotKey = snapshot.kind === 'latest' ? 'latest' : `version:${snapshot.version}`;
  return `${provider}|${region ?? ''}|${tableUri}|${snapshotKey}`;
}

function parsePreflightResult(json: string): PublicObjectStoragePreflightResult {
  const values = JSON.parse(json) as unknown;
  if (!Array.isArray(values)) return [];
  return values.flatMap((value) => {
    if (typeof value !== 'object' || value === null) return [];
    const record = value as Record<string, unknown>;
    const path = typeof record.path === 'string' ? record.path : undefined;
    const url = typeof record.url === 'string' ? record.url : undefined;
    const sizeBytes = numericPreflightValue(record.size_bytes);
    if (!path || !url || sizeBytes === undefined) return [];
    const objectEtag = typeof record.object_etag === 'string' ? record.object_etag : undefined;
    return [{ path, url, size_bytes: sizeBytes, object_etag: objectEtag }];
  });
}

function numericPreflightValue(value: unknown): number | undefined {
  const parsed = typeof value === 'string' ? Number(value) : value;
  return typeof parsed === 'number' && Number.isSafeInteger(parsed) && parsed >= 0
    ? parsed
    : undefined;
}

function strongObjectEtag(etag: string | undefined): string | undefined {
  const trimmed = etag?.trim();
  if (!trimmed || trimmed.startsWith('W/') || trimmed.startsWith('w/')) return undefined;
  if (!trimmed.startsWith('"') || !trimmed.endsWith('"')) return undefined;
  return trimmed;
}

function cloneDescriptor(descriptor: BrowserHttpSnapshotDescriptor): BrowserHttpSnapshotDescriptor {
  return JSON.parse(JSON.stringify(descriptor)) as BrowserHttpSnapshotDescriptor;
}

function nowMs(): number {
  return globalThis.performance?.now() ?? Date.now();
}

function invalidUri(message: string): PublicObjectStorageError {
  return new PublicObjectStorageError('invalid_public_object_storage_uri', message);
}

function invalidPath(message: string): PublicObjectStorageError {
  return new PublicObjectStorageError('invalid_public_object_path', message);
}

function accessFailed(message: string): PublicObjectStorageError {
  return new PublicObjectStorageError('public_storage_access_failed', message);
}
