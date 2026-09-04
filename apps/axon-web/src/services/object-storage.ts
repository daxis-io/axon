import { clone, create } from '@bufbuild/protobuf';
import { NullValue } from '@bufbuild/protobuf/wkt';
import {
  BrowserHttpFileDescriptorSchema,
  BrowserHttpSnapshotDescriptorSchema,
  CapabilityEntrySchema,
  CapabilityKey,
  CapabilityReportSchema,
  CapabilityState,
  PartitionColumnType,
  PartitionValueSchema,
  type BrowserHttpSnapshotDescriptor,
  type CapabilityReport,
} from '../generated/contracts/protobuf/axon/dataaccess/v1/dataaccess_pb.ts';
import {
  TableMetadataSchema,
  type TableMetadata,
} from '../generated/contracts/protobuf/axon/catalog/v1/catalog_pb.ts';

export type PublicObjectStorageProvider = 'gcs' | 's3' | 'r2';

type PublicObjectStorageTableRootBase = {
  tableUri: string;
  bucket: string;
  prefix: string;
  tableRootUrl: string;
};

export type PublicObjectStorageTableRoot =
  | (PublicObjectStorageTableRootBase & { provider: 'gcs'; region?: never })
  | (PublicObjectStorageTableRootBase & { provider: 's3'; region: string })
  | (PublicObjectStorageTableRootBase & {
      provider: 'r2';
      endpoint: string;
      region?: never;
    });

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

export type PublicDeltaLogIndexV1 = {
  schema_version: 1;
  table_uri: string;
  objects: Array<{
    relative_path: string;
    size_bytes: number;
    etag?: string;
  }>;
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
  | { kind: 'latest' }
  | { kind: 'version'; version: number };

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
  signal?: AbortSignal;
  timeoutMs?: number;
};

const DEFAULT_RUNTIME_CACHE_TTL_MS = 2 * 60 * 1000;
const MAX_PUBLIC_R2_INDEX_BYTES = 8 * 1024 * 1024;
const MAX_PUBLIC_R2_INDEX_OBJECTS = 50_000;
const DEFAULT_PUBLIC_R2_INDEX_TIMEOUT_MS = 30_000;
const publicObjectStorageRuntimeCache = new Map<string, PublicObjectStorageRuntimeCacheEntry>();

type ResolvedPublicSnapshot = {
  table_uri: string;
  snapshot_version: number;
  partition_column_types?: Partial<Record<string, ResolvedPartitionColumnType>>;
  browser_compatibility?: ResolvedCapabilityReport;
  required_capabilities?: ResolvedCapabilityReport;
  active_files: Array<{
    path: string;
    size_bytes: number;
    partition_values?: Record<string, string | null>;
    stats?: string;
  }>;
};

type ResolvedPartitionColumnType = 'string' | 'int64' | 'boolean' | 'unsupported';

type ResolvedCapabilityKey =
  | 'change_data_feed'
  | 'column_mapping'
  | 'deletion_vectors'
  | 'multi_partition_execution'
  | 'proxy_access'
  | 'range_reads'
  | 'signed_url_access'
  | 'time_travel'
  | 'timestamp_ntz'
  | 'unknown_protocol_features';

type ResolvedCapabilityState = 'supported' | 'native_only' | 'unsupported' | 'experimental';

type ResolvedCapabilityReport = {
  capabilities?: Partial<Record<ResolvedCapabilityKey, ResolvedCapabilityState>>;
};

export function parsePublicObjectStorageTableRoot(input: {
  provider: PublicObjectStorageProvider;
  tableUri: string;
  region?: string;
  endpoint?: string;
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
      region,
      tableRootUrl: `${s3BucketOrigin(bucket, region)}/${encodeObjectPath(prefix)}/`,
    };
  }

  if (input.provider === 'r2') {
    if (parsed.protocol !== 'r2:') {
      throw invalidUri(providerUriShapeMessage(input.provider));
    }
    validatePublicR2LogicalPath(trimmed);
    if (input.region?.trim()) {
      throw invalidUri('public R2 object storage does not accept a region');
    }
    const bucket = normalizePublicR2Bucket(parsed.hostname, parsed.port);
    const endpoint = normalizePublicR2Endpoint(input.endpoint);
    return {
      provider: input.provider,
      tableUri: `r2://${bucket}/${prefix}`,
      bucket,
      prefix,
      endpoint,
      tableRootUrl: `${endpoint}/${encodeObjectPath(prefix)}/`,
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

export function publicObjectStorageConnectionId(root: PublicObjectStorageTableRoot): string {
  if (root.provider === 'gcs') {
    return `axon-connection://public-gcs/${encodeURIComponent(root.bucket)}`;
  }
  if (root.provider === 'r2') {
    return `axon-connection://public-r2/${encodeURIComponent(root.endpoint)}/${encodeURIComponent(
      root.bucket,
    )}`;
  }
  if (!root.region) throw invalidUri('public object storage S3 region is required');
  return `axon-connection://public-s3/${encodeURIComponent(root.region)}/${encodeURIComponent(
    root.bucket,
  )}`;
}

export function publicObjectStorageProviderForNamespace(
  namespace: string,
): PublicObjectStorageProvider | undefined {
  if (namespace === 'axon.public-gcs/v1') return 'gcs';
  if (namespace === 'axon.public-s3/v1') return 's3';
  if (namespace === 'axon.public-r2/v1') return 'r2';
  return undefined;
}

export function parsePublicObjectStorageTableRootFromConnection(input: {
  provider: PublicObjectStorageProvider;
  tableUri: string;
  connectionId: string;
}): PublicObjectStorageTableRoot {
  const prefix = `axon-connection://public-${input.provider}/`;
  if (!input.connectionId.startsWith(prefix)) {
    throw invalidUri('public object storage connection identity is invalid');
  }
  const segments = input.connectionId.slice(prefix.length).split('/');
  const expectedSegmentCount = input.provider === 'gcs' ? 1 : 2;
  if (segments.length !== expectedSegmentCount || segments.some((segment) => !segment)) {
    throw invalidUri('public object storage connection identity is invalid');
  }

  let region: string | undefined;
  let endpoint: string | undefined;
  try {
    if (input.provider === 's3') region = decodeURIComponent(segments[0]!);
    if (input.provider === 'r2') endpoint = decodeURIComponent(segments[0]!);
  } catch {
    throw invalidUri('public object storage connection identity is invalid');
  }

  const root = parsePublicObjectStorageTableRoot({
    provider: input.provider,
    tableUri: input.tableUri,
    region,
    endpoint,
  });
  if (publicObjectStorageConnectionId(root) !== input.connectionId) {
    throw invalidUri('public object storage connection identity is invalid');
  }
  return root;
}

export function publicObjectStorageCatalogMetadata(
  descriptor: BrowserHttpSnapshotDescriptor,
): TableMetadata {
  const rows = descriptor.activeFiles.reduce((total, file) => {
    const value = rowsFromDescriptorStats(file.stats);
    return value === undefined ? total : total + value;
  }, 0);
  if (!Number.isSafeInteger(rows) || rows < 0) {
    throw accessFailed('public object storage descriptor row count is invalid');
  }
  const sizeBytes = descriptor.activeFiles.reduce((total, file) => {
    if (file.sizeBytes === undefined || file.sizeBytes < 0n) {
      throw accessFailed('public object storage descriptor active file size is invalid');
    }
    return total + file.sizeBytes;
  }, 0n);
  if (descriptor.snapshotVersion === undefined || descriptor.snapshotVersion < 0n) {
    throw accessFailed('public object storage descriptor snapshot version is invalid');
  }

  return create(TableMetadataSchema, {
    partitionColumns: Object.keys(descriptor.partitionColumnTypes).sort(),
    rowCount: BigInt(rows),
    sizeBytes,
    fileCount: BigInt(descriptor.activeFiles.length),
    latestSnapshotVersion: descriptor.snapshotVersion,
    minReaderVersion: 1,
    minWriterVersion: 2,
    storageLocation: descriptor.tableUri,
  });
}

export async function buildPublicDeltaLogManifest(
  root: PublicObjectStorageTableRoot,
  options: PublicObjectStorageFetchOptions = {},
): Promise<PublicDeltaLogManifest> {
  const fetcher = options.fetch ?? globalThis.fetch;
  if (typeof fetcher !== 'function') {
    throw accessFailed('global fetch is not available for public object storage');
  }

  if (root.provider === 'r2') {
    return buildPublicR2DeltaLogManifest(root, fetcher, options);
  }

  const objects: PublicDeltaLogManifestObject[] = [];
  let continuationToken: string | undefined;
  let listRequestCount = 0;
  const listStartedAt = nowMs();

  do {
    throwIfPublicObjectStorageAborted(options.signal);
    listRequestCount += 1;
    const response = await fetcher(publicObjectStorageListUrl(root, continuationToken), {
      credentials: 'omit',
      signal: options.signal,
    });
    throwIfPublicObjectStorageAborted(options.signal);
    if (!response.ok) {
      throw accessFailed(
        `public object storage Delta log listing failed (HTTP ${response.status})`,
      );
    }

    const page = parseObjectStorageListResponse(await response.text());
    throwIfPublicObjectStorageAborted(options.signal);
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

async function buildPublicR2DeltaLogManifest(
  root: Extract<PublicObjectStorageTableRoot, { provider: 'r2' }>,
  fetcher: PublicObjectStorageFetch,
  options: PublicObjectStorageFetchOptions,
): Promise<PublicDeltaLogManifest> {
  throwIfPublicObjectStorageAborted(options.signal);
  const startedAt = nowMs();
  const indexUrl = publicObjectUrl(root, '_axon/public-delta-log-index.json');
  const deadline = publicR2IndexDeadline(
    options.signal,
    options.timeoutMs ?? DEFAULT_PUBLIC_R2_INDEX_TIMEOUT_MS,
  );
  try {
    const response = await fetcher(indexUrl, {
      credentials: 'omit',
      redirect: 'error',
      signal: deadline.signal,
    });
    throwIfPublicObjectStorageAborted(options.signal);
    rejectPublicR2Redirect(response, indexUrl);
    if (!response.ok) {
      if (response.status === 404) {
        throw accessFailed(
          'public R2 Delta log index _axon/public-delta-log-index.json was not found; publish the well-known index at the table root',
        );
      }
      throw accessFailed(`public R2 Delta log index request failed (HTTP ${response.status})`);
    }

    const declaredLength = response.headers.get('content-length');
    if (declaredLength !== null) {
      const bytes = Number(declaredLength);
      if (Number.isSafeInteger(bytes) && bytes > MAX_PUBLIC_R2_INDEX_BYTES) {
        throw accessFailed('public R2 Delta log index exceeded the maximum byte size');
      }
    }

    let value: unknown;
    try {
      value = JSON.parse(
        await readPublicR2IndexText(response, MAX_PUBLIC_R2_INDEX_BYTES, deadline.signal),
      );
    } catch (error) {
      if (deadline.timedOut()) throw error;
      throwIfPublicObjectStorageAborted(options.signal);
      if (error instanceof PublicObjectStorageError) throw error;
      throw accessFailed('public R2 Delta log index returned invalid JSON');
    }
    throwIfPublicObjectStorageAborted(options.signal);
    const objects = parsePublicDeltaLogIndexV1(value, root);
    if (objects.length === 0) {
      throw accessFailed('public R2 Delta log index did not contain any Delta log objects');
    }
    return {
      tableUri: root.tableUri,
      objects,
      list_request_count: 1,
      list_duration_ms: Math.round(nowMs() - startedAt),
    };
  } catch (error) {
    if (deadline.timedOut()) {
      throw accessFailed('public R2 Delta log index request timed out');
    }
    throwIfPublicObjectStorageAborted(options.signal);
    throw error;
  } finally {
    deadline.dispose();
  }
}

export function parsePublicDeltaLogIndexV1(
  value: unknown,
  root: PublicObjectStorageTableRoot,
): PublicDeltaLogManifestObject[] {
  if (root.provider !== 'r2') {
    throw accessFailed('PublicDeltaLogIndexV1 is only valid for public R2 table roots');
  }
  if (!isRecord(value) || !hasExactKeys(value, ['objects', 'schema_version', 'table_uri'])) {
    throw accessFailed('public R2 Delta log index envelope must use the closed v1 schema');
  }
  if (value.schema_version !== 1) {
    throw accessFailed('public R2 Delta log index schema_version must equal 1');
  }
  if (value.table_uri !== root.tableUri || containsSecretMaterial(String(value.table_uri ?? ''))) {
    throw accessFailed('public R2 Delta log index table_uri did not match the configured table');
  }
  if (!Array.isArray(value.objects) || value.objects.length === 0) {
    throw accessFailed('public R2 Delta log index objects must be a non-empty array');
  }
  if (value.objects.length > MAX_PUBLIC_R2_INDEX_OBJECTS) {
    throw accessFailed('public R2 Delta log index object count exceeded the browser limit');
  }

  const paths = new Set<string>();
  const objects = value.objects.map((candidate) => {
    if (
      !isRecord(candidate) ||
      !hasExactKeys(
        candidate,
        candidate.etag === undefined
          ? ['relative_path', 'size_bytes']
          : ['etag', 'relative_path', 'size_bytes'],
      )
    ) {
      throw accessFailed('public R2 Delta log index object must use the closed v1 schema');
    }
    const relativePath = candidate.relative_path;
    if (
      typeof relativePath !== 'string' ||
      !relativePath.startsWith('_delta_log/') ||
      relativePath.includes('\\') ||
      /%(?:2f|5c)/i.test(relativePath) ||
      hasUnsafeEncodedPathSegment(relativePath) ||
      containsSecretMaterial(relativePath)
    ) {
      throw accessFailed('public R2 Delta log index contained an invalid Delta log path');
    }
    let normalized: string;
    try {
      normalized = normalizeObjectPath(relativePath);
    } catch {
      throw accessFailed('public R2 Delta log index contained an unsafe Delta log path');
    }
    if (!normalized || normalized !== relativePath) {
      throw accessFailed('public R2 Delta log index contained an unsafe Delta log path');
    }
    if (paths.has(relativePath)) {
      throw accessFailed('public R2 Delta log index contained a duplicate Delta log path');
    }
    paths.add(relativePath);

    if (!Number.isSafeInteger(candidate.size_bytes) || Number(candidate.size_bytes) < 0) {
      throw accessFailed('public R2 Delta log index contained an unsafe object size');
    }
    const sizeBytes = Number(candidate.size_bytes);
    const object: PublicDeltaLogManifestObject = {
      relative_path: relativePath,
      url: publicObjectUrl(root, relativePath),
      size_bytes: sizeBytes,
    };
    if (candidate.etag !== undefined) {
      if (
        typeof candidate.etag !== 'string' ||
        strongObjectEtag(candidate.etag) !== candidate.etag ||
        containsSecretMaterial(candidate.etag)
      ) {
        throw accessFailed('public R2 Delta log index contained an invalid strong ETag');
      }
      object.etag = candidate.etag;
    }
    return object;
  });

  return objects.sort((left, right) =>
    left.relative_path < right.relative_path
      ? -1
      : left.relative_path > right.relative_path
        ? 1
        : 0,
  );
}

async function readPublicR2IndexText(
  response: Response,
  maxBytes: number,
  signal: AbortSignal | undefined,
): Promise<string> {
  if (!response.body) {
    const text = await response.text();
    if (new TextEncoder().encode(text).byteLength > maxBytes) {
      throw accessFailed('public R2 Delta log index exceeded the maximum byte size');
    }
    return text;
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let byteLength = 0;
  let text = '';
  try {
    while (true) {
      throwIfPublicObjectStorageAborted(signal);
      const { done, value } = await readPublicR2IndexChunk(reader, signal);
      if (done) break;
      byteLength += value.byteLength;
      if (byteLength > maxBytes) {
        await reader.cancel();
        throw accessFailed('public R2 Delta log index exceeded the maximum byte size');
      }
      text += decoder.decode(value, { stream: true });
    }
    return text + decoder.decode();
  } finally {
    reader.releaseLock();
  }
}

async function readPublicR2IndexChunk(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  signal: AbortSignal | undefined,
): Promise<ReadableStreamReadResult<Uint8Array>> {
  if (!signal) return await reader.read();
  throwIfPublicObjectStorageAborted(signal);
  return await new Promise((resolve, reject) => {
    let settled = false;
    const finish = (callback: () => void) => {
      if (settled) return;
      settled = true;
      signal.removeEventListener('abort', onAbort);
      callback();
    };
    const onAbort = () => {
      void reader.cancel().catch(() => undefined);
      finish(() => {
        const error = new Error('public object storage acquisition was cancelled');
        error.name = 'AbortError';
        reject(error);
      });
    };
    signal.addEventListener('abort', onAbort, { once: true });
    void reader.read().then(
      (result) => finish(() => resolve(result)),
      (error: unknown) => finish(() => reject(error)),
    );
  });
}

function rejectPublicR2Redirect(response: Response, requestedUrl: string): void {
  if (!response.url) {
    throw accessFailed('public R2 Delta log index returned an unverifiable response URL');
  }
  let responseOrigin: string;
  let requestedOrigin: string;
  try {
    responseOrigin = new URL(response.url).origin;
    requestedOrigin = new URL(requestedUrl).origin;
  } catch {
    throw accessFailed('public R2 Delta log index returned an invalid response URL');
  }
  if (response.url !== requestedUrl || response.redirected) {
    if (responseOrigin === requestedOrigin) {
      throw accessFailed('public R2 Delta log index rejected a redirect');
    }
    throw accessFailed('public R2 Delta log index rejected a cross-origin redirect');
  }
}

function publicR2IndexDeadline(
  parent: AbortSignal | undefined,
  timeoutMs: number,
): {
  signal: AbortSignal;
  timedOut: () => boolean;
  dispose: () => void;
} {
  const controller = new AbortController();
  let didTimeOut = false;
  const abortFromParent = () => controller.abort(parent?.reason);
  if (parent?.aborted) abortFromParent();
  else parent?.addEventListener('abort', abortFromParent, { once: true });
  const timer = setTimeout(() => {
    didTimeOut = true;
    controller.abort(new DOMException('public R2 index request timed out', 'TimeoutError'));
  }, timeoutMs);
  return {
    signal: controller.signal,
    timedOut: () => didTimeOut,
    dispose: () => {
      clearTimeout(timer);
      parent?.removeEventListener('abort', abortFromParent);
    },
  };
}

export async function resolvePublicObjectStorageDescriptor(input: {
  provider: PublicObjectStorageProvider;
  tableUri: string;
  region?: string;
  endpoint?: string;
  snapshotVersion?: number;
  resolveDeltaSnapshotFromManifest: (
    manifestJson: string,
    tableUri: string,
    snapshotVersion?: number,
  ) => Promise<string>;
  fetch?: PublicObjectStorageFetch;
  signal?: AbortSignal;
  onMetrics?: (metrics: PublicObjectStorageDescriptorResolutionMetrics) => void;
}): Promise<BrowserHttpSnapshotDescriptor> {
  if (
    input.snapshotVersion !== undefined &&
    (!Number.isSafeInteger(input.snapshotVersion) || input.snapshotVersion < 0)
  ) {
    throw accessFailed('public object storage snapshot version is invalid');
  }
  const root = parsePublicObjectStorageTableRoot({
    provider: input.provider,
    tableUri: input.tableUri,
    region: input.region,
    endpoint: input.endpoint,
  });
  throwIfPublicObjectStorageAborted(input.signal);
  const manifest = await buildPublicDeltaLogManifest(root, {
    fetch: input.fetch,
    signal: input.signal,
  });
  throwIfPublicObjectStorageAborted(input.signal);
  const snapshotResolveStartedAt = nowMs();
  let snapshot: ResolvedPublicSnapshot;
  try {
    snapshot = JSON.parse(
      await input.resolveDeltaSnapshotFromManifest(
        JSON.stringify({ objects: manifest.objects }),
        root.tableUri,
        input.snapshotVersion,
      ),
    ) as ResolvedPublicSnapshot;
  } catch (error) {
    throwIfPublicObjectStorageAborted(input.signal);
    if (error instanceof Error && error.name === 'AbortError') throw error;
    if (root.provider === 'r2') {
      if (isPublicR2StaleIndexError(error)) {
        throw accessFailed(
          'public R2 Delta log index is stale or incomplete for the requested snapshot; republish the table index after all Delta log objects',
        );
      }
      throw accessFailed(
        'public R2 Delta snapshot resolution failed; verify runtime compatibility and retry',
      );
    }
    throw error;
  }
  throwIfPublicObjectStorageAborted(input.signal);
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

  if (!Number.isSafeInteger(snapshot.snapshot_version) || snapshot.snapshot_version < 0) {
    throw accessFailed('public object storage snapshot resolver returned an invalid version');
  }

  return create(BrowserHttpSnapshotDescriptorSchema, {
    tableUri: root.tableUri,
    snapshotVersion: BigInt(snapshot.snapshot_version),
    partitionColumnTypes: generatedPartitionColumnTypes(snapshot.partition_column_types ?? {}),
    browserCompatibility: generatedCapabilityReport(snapshot.browser_compatibility),
    requiredCapabilities: generatedCapabilityReport(snapshot.required_capabilities),
    activeFiles: snapshot.active_files.map((file) =>
      create(BrowserHttpFileDescriptorSchema, {
        path: file.path,
        url: publicObjectUrl(root, file.path),
        sizeBytes: BigInt(validatedResolvedInteger(file.size_bytes, 'active file size')),
        partitionValues: Object.fromEntries(
          Object.entries(file.partition_values ?? {}).map(([name, value]) => [
            name,
            create(PartitionValueSchema, {
              value:
                value === null
                  ? { case: 'nullValue', value: NullValue.NULL_VALUE }
                  : { case: 'stringValue', value },
            }),
          ]),
        ),
        stats: file.stats,
      }),
    ),
  });
}

function isPublicR2StaleIndexError(error: unknown): boolean {
  const serialized =
    error instanceof Error ? error.message : typeof error === 'string' ? error : undefined;
  if (!serialized) return false;

  let structured: unknown;
  try {
    structured = JSON.parse(serialized);
  } catch {
    return false;
  }
  if (
    !isRecord(structured) ||
    typeof structured.code !== 'string' ||
    typeof structured.message !== 'string'
  ) {
    return false;
  }
  if (structured.code === 'object_not_found') return true;
  if (structured.code === 'object_store_protocol') {
    return /^Delta log object '.+' (?:size|identity) changed between manifest and read$/.test(
      structured.message,
    );
  }
  if (structured.code !== 'invalid_request') return false;
  return /^(?:delta log did not contain any commits or checkpoints|requested snapshot version \d+ exceeds the latest available version \d+|delta log replay expected commit file '.+'|missing checkpoint part '.+'|checkpoint sidecar '.+' referenced by '.+' was missing)$/.test(
    structured.message,
  );
}

export async function preflightPublicObjectStorageDescriptorRangeRead(input: {
  descriptor: BrowserHttpSnapshotDescriptor;
  preflightParquetMetadataForTargets: (targetsJson: string) => Promise<string>;
  signal?: AbortSignal;
}): Promise<PublicObjectStoragePreflightResult> {
  throwIfPublicObjectStorageAborted(input.signal);
  const target = input.descriptor.activeFiles[0];
  if (!target) return [];

  try {
    const result = await input.preflightParquetMetadataForTargets(
      JSON.stringify([
        {
          path: target.path,
          url: target.url,
          size_bytes: safeGeneratedInteger(target.sizeBytes, 'active file size'),
          partition_values: Object.fromEntries(
            Object.entries(target.partitionValues).map(([name, value]) => [
              name,
              value.value.case === 'stringValue' ? value.value.value : null,
            ]),
          ),
          ...(target.stats === undefined ? {} : { stats: target.stats }),
        },
      ]),
    );
    throwIfPublicObjectStorageAborted(input.signal);
    return parsePreflightResult(result);
  } catch (error) {
    throwIfPublicObjectStorageAborted(input.signal);
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
  endpoint?: string;
  snapshot: PublicObjectStorageRuntimeCacheSnapshot;
  descriptor: BrowserHttpSnapshotDescriptor;
  preflight: PublicObjectStoragePreflightResult;
  nowMs?: () => number;
  ttlMs?: number;
  signal?: AbortSignal;
}): boolean {
  if (input.signal?.aborted) return false;
  const root = parsePublicObjectStorageTableRoot({
    provider: input.provider,
    tableUri: input.tableUri,
    region: input.region,
    endpoint: input.endpoint,
  });
  if (input.descriptor.tableUri !== root.tableUri) return false;
  if (
    input.snapshot.kind === 'version' &&
    (!Number.isSafeInteger(input.snapshot.version) ||
      input.snapshot.version < 0 ||
      input.descriptor.snapshotVersion !== BigInt(input.snapshot.version))
  ) {
    return false;
  }

  const firstFile = input.descriptor.activeFiles[0];
  const firstPreflight = input.preflight[0];
  if (!firstFile || !firstPreflight || firstFile.path !== firstPreflight.path) return false;
  if (firstFile.url !== firstPreflight.url) return false;
  if (firstFile.sizeBytes !== BigInt(firstPreflight.size_bytes)) return false;

  const objectEtag = strongObjectEtag(firstPreflight.object_etag);
  if (!objectEtag) return false;
  const descriptor = cloneDescriptor(input.descriptor);
  descriptor.activeFiles = descriptor.activeFiles.map((file, index) =>
    index === 0 ? { ...file, objectEtag } : file,
  );

  publicObjectStorageRuntimeCache.set(
    publicObjectStorageRuntimeCacheKey(
      root.provider,
      root.tableUri,
      input.snapshot,
      root.region,
      root.provider === 'r2' ? root.endpoint : undefined,
    ),
    {
      descriptor,
      identity: {
        path: firstFile.path,
        size_bytes: safeGeneratedInteger(firstFile.sizeBytes, 'active file size'),
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
  endpoint?: string;
  snapshot: PublicObjectStorageRuntimeCacheSnapshot;
  expectedSnapshotVersion?: number;
  nowMs?: () => number;
}): PublicObjectStorageRuntimeCacheEntry | undefined {
  if (
    (input.snapshot.kind === 'version' &&
      (!Number.isSafeInteger(input.snapshot.version) || input.snapshot.version < 0)) ||
    (input.expectedSnapshotVersion !== undefined &&
      (!Number.isSafeInteger(input.expectedSnapshotVersion) || input.expectedSnapshotVersion < 0))
  ) {
    return undefined;
  }
  const root = parsePublicObjectStorageTableRoot({
    provider: input.provider,
    tableUri: input.tableUri,
    region: input.region,
    endpoint: input.endpoint,
  });
  const key = publicObjectStorageRuntimeCacheKey(
    root.provider,
    root.tableUri,
    input.snapshot,
    root.region,
    root.provider === 'r2' ? root.endpoint : undefined,
  );
  const entry = publicObjectStorageRuntimeCache.get(key);
  if (!entry) return undefined;

  if (entry.expiresAtEpochMs <= (input.nowMs ?? Date.now)()) {
    publicObjectStorageRuntimeCache.delete(key);
    return undefined;
  }
  if (
    input.expectedSnapshotVersion !== undefined &&
    entry.descriptor.snapshotVersion !== BigInt(input.expectedSnapshotVersion)
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

function throwIfPublicObjectStorageAborted(signal: AbortSignal | undefined): void {
  if (!signal?.aborted) return;
  const error = new Error('public object storage acquisition was cancelled');
  error.name = 'AbortError';
  throw error;
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
  switch (provider) {
    case 's3':
      return 'public object storage S3 table URI must look like s3://bucket/table';
    case 'gcs':
      return 'public object storage GCS table URI must look like gs://bucket/table';
    case 'r2':
      return 'public R2 object storage table URI must look like r2://bucket/table';
  }
}

function normalizePublicR2Endpoint(endpoint: string | undefined): string {
  const trimmed = endpoint?.trim();
  if (!trimmed || containsSecretMaterial(trimmed)) {
    throw invalidUri('public R2 object storage requires a credential-free HTTPS endpoint origin');
  }
  let parsed: URL;
  try {
    parsed = new URL(trimmed);
  } catch {
    throw invalidUri('public R2 object storage endpoint must be an HTTPS origin');
  }
  if (
    parsed.protocol !== 'https:' ||
    !parsed.hostname ||
    hasUserinfo(parsed) ||
    !hasOriginOnlyRawPath(trimmed, 'https') ||
    parsed.search ||
    parsed.hash ||
    parsed.hostname.toLowerCase().endsWith('.r2.cloudflarestorage.com')
  ) {
    throw invalidUri(
      'public R2 object storage endpoint must be a public HTTPS origin without a path, credentials, query, or fragment',
    );
  }
  return parsed.origin;
}

function validatePublicR2LogicalPath(uri: string): void {
  const rawPath = rawPathAfterAuthority(uri, 'r2');
  const rawSegments = rawPath?.startsWith('/') ? rawPath.slice(1).split('/') : [];
  if (
    !rawPath ||
    rawSegments.some((segment) => !segment) ||
    rawPath.includes('\\') ||
    /%(?:2f|5c)/i.test(rawPath) ||
    hasUnsafeEncodedPathSegment(rawPath)
  ) {
    throw invalidUri('public R2 object storage table URI contained an unsafe table path');
  }
}

function hasUnsafeEncodedPathSegment(path: string): boolean {
  return path.split('/').some((segment) => {
    try {
      const decoded = decodeURIComponent(segment);
      return decoded === '.' || decoded === '..' || decoded.includes('/') || decoded.includes('\\');
    } catch {
      return true;
    }
  });
}

function hasOriginOnlyRawPath(value: string, scheme: string): boolean {
  const rawPath = rawPathAfterAuthority(value, scheme);
  return rawPath === '' || rawPath === '/';
}

function rawPathAfterAuthority(value: string, scheme: string): string | undefined {
  return new RegExp(`^${scheme}:\\/\\/[^/?#]+([^?#]*)$`, 'i').exec(value)?.[1];
}

function normalizePublicR2Bucket(bucket: string, port: string): string {
  if (
    port ||
    bucket !== bucket.toLowerCase() ||
    !/^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/.test(bucket)
  ) {
    throw invalidUri('public R2 bucket must be a lowercase DNS-compatible bucket name');
  }
  return bucket;
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
  endpoint?: string,
): string {
  const snapshotKey = snapshot.kind === 'latest' ? 'latest' : `version:${snapshot.version}`;
  return endpoint === undefined
    ? `${provider}|${region ?? ''}|${tableUri}|${snapshotKey}`
    : `${provider}|${region ?? ''}|${endpoint}|${tableUri}|${snapshotKey}`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function hasExactKeys(value: Record<string, unknown>, keys: readonly string[]): boolean {
  const actual = Object.keys(value).sort();
  const expected = [...keys].sort();
  return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
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
  if (!/^"[\u0021\u0023-\u007e]*"$/.test(trimmed)) return undefined;
  return trimmed;
}

function generatedPartitionColumnTypes(
  values: Partial<Record<string, ResolvedPartitionColumnType>>,
): Record<string, PartitionColumnType> {
  return Object.fromEntries(
    Object.entries(values).map(([name, value]) => {
      switch (value) {
        case 'string':
          return [name, PartitionColumnType.STRING];
        case 'int64':
          return [name, PartitionColumnType.INT64];
        case 'boolean':
          return [name, PartitionColumnType.BOOLEAN];
        case 'unsupported':
          return [name, PartitionColumnType.UNSUPPORTED];
        default:
          throw accessFailed(
            `public object storage snapshot resolver returned invalid partition type '${String(
              value,
            )}'`,
          );
      }
    }),
  );
}

function generatedCapabilityReport(report: ResolvedCapabilityReport | undefined): CapabilityReport {
  return create(CapabilityReportSchema, {
    capabilities: Object.entries(report?.capabilities ?? {}).flatMap(([key, state]) => {
      const generatedKey = generatedCapabilityKey(key as ResolvedCapabilityKey);
      const generatedState = generatedCapabilityState(state);
      if (generatedKey === undefined || generatedState === undefined) {
        throw accessFailed('public object storage snapshot resolver returned invalid capabilities');
      }
      return [
        create(CapabilityEntrySchema, {
          key: generatedKey,
          state: generatedState,
        }),
      ];
    }),
  });
}

function generatedCapabilityKey(value: ResolvedCapabilityKey): CapabilityKey | undefined {
  switch (value) {
    case 'change_data_feed':
      return CapabilityKey.CHANGE_DATA_FEED;
    case 'column_mapping':
      return CapabilityKey.COLUMN_MAPPING;
    case 'deletion_vectors':
      return CapabilityKey.DELETION_VECTORS;
    case 'multi_partition_execution':
      return CapabilityKey.MULTI_PARTITION_EXECUTION;
    case 'proxy_access':
      return CapabilityKey.PROXY_ACCESS;
    case 'range_reads':
      return CapabilityKey.RANGE_READS;
    case 'signed_url_access':
      return CapabilityKey.SIGNED_URL_ACCESS;
    case 'time_travel':
      return CapabilityKey.TIME_TRAVEL;
    case 'timestamp_ntz':
      return CapabilityKey.TIMESTAMP_NTZ;
    case 'unknown_protocol_features':
      return CapabilityKey.UNKNOWN_PROTOCOL_FEATURES;
  }
}

function generatedCapabilityState(
  value: ResolvedCapabilityState | undefined,
): CapabilityState | undefined {
  switch (value) {
    case 'supported':
      return CapabilityState.SUPPORTED;
    case 'native_only':
      return CapabilityState.NATIVE_ONLY;
    case 'unsupported':
      return CapabilityState.UNSUPPORTED;
    case 'experimental':
      return CapabilityState.EXPERIMENTAL;
    case undefined:
      return undefined;
  }
}

function validatedResolvedInteger(value: number, field: string): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw accessFailed(`public object storage snapshot resolver returned an invalid ${field}`);
  }
  return value;
}

function safeGeneratedInteger(value: bigint, field: string): number {
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number < 0) {
    throw accessFailed(
      `public object storage descriptor ${field} is outside JavaScript-safe range`,
    );
  }
  return number;
}

function rowsFromDescriptorStats(stats: string | undefined): number | undefined {
  if (!stats) return undefined;
  try {
    const parsed = JSON.parse(stats) as unknown;
    if (
      typeof parsed === 'object' &&
      parsed !== null &&
      'numRecords' in parsed &&
      typeof parsed.numRecords === 'number'
    ) {
      return parsed.numRecords;
    }
  } catch {
    return undefined;
  }
  return undefined;
}

function cloneDescriptor(descriptor: BrowserHttpSnapshotDescriptor): BrowserHttpSnapshotDescriptor {
  return clone(BrowserHttpSnapshotDescriptorSchema, descriptor);
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
