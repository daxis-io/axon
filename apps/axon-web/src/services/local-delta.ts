import { create } from '@bufbuild/protobuf';
import { NullValue } from '@bufbuild/protobuf/wkt';
import init, { resolve_delta_snapshot_from_manifest } from '../wasm/axon_web_wasm.js';
import {
  BrowserHttpFileDescriptorSchema,
  BrowserHttpSnapshotDescriptorSchema,
  CapabilityReportSchema,
  PartitionColumnType,
  PartitionValueSchema,
  type BrowserHttpSnapshotDescriptor,
} from '../generated/contracts/protobuf/axon/dataaccess/v1/dataaccess_pb.ts';
import {
  ColumnNodeSchema,
  TableMetadataSchema,
  type TableMetadata,
} from '../generated/contracts/protobuf/axon/catalog/v1/catalog_pb.ts';
import {
  HandleStore,
  ensureDirectoryReadPermission,
  type LocalDeltaHandleFileRecord,
  type LocalDeltaHandleStoreRecord,
} from '../persistence/handle-store.ts';
import {
  hasLocalDeltaRuntime as hasMarkedLocalDeltaRuntime,
  markLocalDeltaRuntimeActive,
  markLocalDeltaRuntimeInactive,
} from './local-delta-session.ts';

export type LocalFileSystemFileHandle = {
  readonly kind: 'file';
  readonly name: string;
  getFile(): Promise<File>;
};

export type LocalFileSystemDirectoryHandle = {
  readonly kind: 'directory';
  readonly name: string;
  entries(): AsyncIterableIterator<
    [string, LocalFileSystemDirectoryHandle | LocalFileSystemFileHandle]
  >;
  queryPermission?(descriptor?: { mode?: 'read' }): Promise<PermissionState>;
  requestPermission?(descriptor?: { mode?: 'read' }): Promise<PermissionState>;
};

export type LocalDeltaErrorCode =
  | 'empty_selection'
  | 'invalid_path'
  | 'missing_delta_log'
  | 'missing_active_file'
  | 'unsupported_delta_feature'
  | 'invalid_delta_log'
  | 'registry_unavailable';

export class LocalDeltaError extends Error {
  readonly name = 'LocalDeltaError';

  constructor(
    readonly code: LocalDeltaErrorCode,
    message: string,
  ) {
    super(message);
  }
}

export type LocalDeltaRuntime = {
  kind: 'local_delta';
  registryId: string;
  persistence: LocalDeltaPersistenceMode;
  tableRootName: string;
  tableName: string;
  schemaName: string;
  storageLabel: string;
  descriptor: BrowserHttpSnapshotDescriptor;
  catalogMetadata: TableMetadata;
};

export type LocalDeltaPersistenceMode =
  | 'session_handles'
  | 'persisted_directory_handle'
  | 'metadata_only_reselect';

export type OpenLocalDeltaOptions = {
  schemaName?: string;
  tableName?: string;
  registryId?: string;
  snapshotVersion?: number;
  signal?: AbortSignal;
};

type ObjectKind = 'commit_json' | 'checkpoint_parquet' | 'last_checkpoint' | 'delta_log_object';

type LocalDeltaFileEntry = {
  file: File;
  browserPath: string;
  relativePath: string;
};

type LocalDeltaTableFiles = {
  registryId?: string;
  directoryHandle?: LocalFileSystemDirectoryHandle;
  persistenceMode?: LocalDeltaPersistenceMode;
  tableRootName: string;
  filesByRelativePath: Map<string, LocalDeltaFileEntry>;
  logEntries: LocalDeltaFileEntry[];
  dataEntries: LocalDeltaFileEntry[];
};

type LocalDeltaRegistryFileRecord = LocalDeltaHandleFileRecord;

type LocalDeltaRegistryRecord = LocalDeltaHandleStoreRecord<LocalFileSystemDirectoryHandle>;

type ResolvedSnapshot = {
  table_uri: string;
  snapshot_version: number;
  partition_column_types?: Partial<Record<string, ResolvedPartitionColumnType>>;
  active_files: Array<{
    path: string;
    size_bytes: number;
    partition_values: Record<string, string | null>;
    stats?: string;
  }>;
};

type ResolvedPartitionColumnType = 'string' | 'int64' | 'boolean' | 'unsupported';

type LocalLogFacts = {
  tableName?: string;
  minReaderVersion?: number;
  minWriterVersion?: number;
  schemaString?: string;
  partitionColumns: string[];
};

const LOCAL_DELTA_DB_NAME = 'axon-local-delta-registry';
const LOCAL_DELTA_DB_VERSION = 1;
const LOCAL_DELTA_STORE = 'tables';
const LOCAL_DELTA_ACTIVE_ID_KEY = 'axon-local-delta-active-id';

let wasmReady: Promise<unknown> | undefined;
const sessionLocalDeltaTables = new Map<string, LocalDeltaTableFiles>();
const localDeltaRuntimes = new Map<string, LocalDeltaRuntime>();
const localObjectUrlsByRegistryId = new Map<string, Set<string>>();
const localDeltaAcquisitionQueues = new Map<string, Promise<void>>();
const localDeltaHandleStore = new HandleStore<LocalFileSystemDirectoryHandle>({
  databaseName: LOCAL_DELTA_DB_NAME,
  version: LOCAL_DELTA_DB_VERSION,
  storeName: LOCAL_DELTA_STORE,
});

export async function openLocalDeltaTableFromFileList(
  files: FileList | File[] | null,
  options: OpenLocalDeltaOptions = {},
): Promise<LocalDeltaRuntime> {
  const table = collectLocalDeltaTableFiles(files);
  return openLocalDeltaRuntime(table, options);
}

export async function openLocalDeltaTableFromDirectoryHandle(
  directory: LocalFileSystemDirectoryHandle,
  options: OpenLocalDeltaOptions = {},
): Promise<LocalDeltaRuntime> {
  throwIfLocalDeltaAborted(options.signal);
  const entries = await collectLocalDeltaDirectoryEntries(directory, '', options.signal);
  const table = buildLocalDeltaTableFiles(directory.name || 'delta-table', entries, undefined, {
    directoryHandle: directory,
  });
  return openLocalDeltaRuntime(table, options);
}

export async function loadLocalDeltaRuntime(
  registryId: string,
  options: OpenLocalDeltaOptions = {},
): Promise<LocalDeltaRuntime> {
  throwIfLocalDeltaAborted(options.signal);
  let table = sessionLocalDeltaTables.get(registryId);
  if (table) {
    await revalidateLocalDeltaTableAccess(table, options.signal);
  } else {
    table = await loadLocalDeltaTable(registryId, options.signal);
  }
  if (!table) {
    throw new LocalDeltaError(
      'registry_unavailable',
      'Local Delta table registry entry could not be reopened. Select the folder again.',
    );
  }
  sessionLocalDeltaTables.set(registryId, table);
  markLocalDeltaRuntimeActive(registryId);
  const cacheKey = localDeltaRuntimeCacheKey(registryId, options);
  const cached = localDeltaRuntimes.get(cacheKey);
  if (cached) return cached;
  return withLocalDeltaAcquisition(registryId, async () => {
    throwIfLocalDeltaAborted(options.signal);
    const queuedCached = localDeltaRuntimes.get(cacheKey);
    if (queuedCached) return queuedCached;
    const ownedObjectUrls = new Set<string>();
    try {
      const runtime = await buildLocalDeltaRuntime(
        table,
        { ...options, registryId },
        table.persistenceMode ?? 'session_handles',
        ownedObjectUrls,
      );
      throwIfLocalDeltaAborted(options.signal);
      localDeltaRuntimes.set(cacheKey, runtime);
      return runtime;
    } catch (error) {
      releaseOwnedLocalDeltaObjectUrls(registryId, ownedObjectUrls);
      throw error;
    }
  });
}

export async function loadActiveLocalDeltaRuntime(
  options: OpenLocalDeltaOptions = {},
): Promise<LocalDeltaRuntime | undefined> {
  const activeId = activeLocalDeltaRegistryId();
  if (!activeId) return undefined;
  return loadLocalDeltaRuntime(activeId, options);
}

export function hasLocalDeltaRuntime(registryId?: string): boolean {
  return hasMarkedLocalDeltaRuntime(registryId);
}

export function clearActiveLocalDeltaRegistryId(): void {
  try {
    localStorage.removeItem(LOCAL_DELTA_ACTIVE_ID_KEY);
  } catch {
    // Best-effort cleanup only.
  }
}

export function releaseLocalDeltaObjectUrls(registryId?: string): void {
  if (registryId) {
    const urls = localObjectUrlsByRegistryId.get(registryId);
    deleteLocalDeltaRuntimeCache(registryId);
    if (urls) {
      for (const url of urls) URL.revokeObjectURL(url);
      localObjectUrlsByRegistryId.delete(registryId);
    }
    return;
  }

  for (const urls of localObjectUrlsByRegistryId.values()) {
    for (const url of urls) URL.revokeObjectURL(url);
  }
  localObjectUrlsByRegistryId.clear();
  localDeltaRuntimes.clear();
}

export function isCurrentLocalDeltaObjectUrl(registryId: string, url: string): boolean {
  return url.startsWith('blob:') && localObjectUrlsByRegistryId.get(registryId)?.has(url) === true;
}

export async function unregisterLocalDeltaRuntime(registryId: string): Promise<void> {
  sessionLocalDeltaTables.delete(registryId);
  deleteLocalDeltaRuntimeCache(registryId);
  markLocalDeltaRuntimeInactive(registryId);
  releaseLocalDeltaObjectUrls(registryId);

  if (activeLocalDeltaRegistryId() === registryId) {
    clearActiveLocalDeltaRegistryId();
  }

  try {
    await deleteLocalDeltaRegistryRecord(registryId);
  } catch {
    // Best-effort cleanup only.
  }
}

async function ensureWasm(): Promise<unknown> {
  if (!wasmReady) {
    wasmReady = init();
  }
  return wasmReady;
}

async function buildLocalDeltaRuntime(
  table: LocalDeltaTableFiles,
  options: OpenLocalDeltaOptions,
  persistence: LocalDeltaRuntime['persistence'],
  ownedObjectUrls?: Set<string>,
): Promise<LocalDeltaRuntime> {
  throwIfLocalDeltaAborted(options.signal);
  await ensureWasm();
  throwIfLocalDeltaAborted(options.signal);
  const facts = await readLocalLogFacts(table.logEntries, options.signal);
  throwIfLocalDeltaAborted(options.signal);
  const tableName = sanitizeSqlIdentifier(
    options.tableName ?? facts.tableName ?? table.tableRootName,
  );
  const schemaName = options.schemaName ?? 'default';
  const registryId =
    table.registryId ?? options.registryId ?? localDeltaRegistryId(table.tableRootName);
  const tableUri = localTableUri(table.tableRootName);
  if (
    options.snapshotVersion !== undefined &&
    (!Number.isSafeInteger(options.snapshotVersion) || options.snapshotVersion < 0)
  ) {
    throw new LocalDeltaError(
      'invalid_delta_log',
      'Requested Delta snapshot version must be a non-negative JavaScript-safe integer.',
    );
  }

  const logObjects = table.logEntries.map((entry) => ({
    relative_path: entry.relativePath,
    url: trackLocalObjectUrl(registryId, URL.createObjectURL(entry.file), ownedObjectUrls),
    size_bytes: entry.file.size,
    kind: classifyObject(entry.relativePath),
  }));
  const wasmManifest = {
    objects: logObjects.map((object) => ({
      relative_path: object.relative_path,
      url: object.url,
      size_bytes: object.size_bytes,
    })),
  };
  const snapshotJson = await resolve_delta_snapshot_from_manifest(
    JSON.stringify(wasmManifest),
    tableUri,
    options.snapshotVersion,
  );
  throwIfLocalDeltaAborted(options.signal);
  const snapshot = JSON.parse(snapshotJson) as ResolvedSnapshot;
  const partitionTypes =
    snapshot.partition_column_types ??
    partitionTypesFromSchema(facts.schemaString, facts.partitionColumns);
  const descriptor = create(BrowserHttpSnapshotDescriptorSchema, {
    tableUri: snapshot.table_uri,
    snapshotVersion: BigInt(snapshot.snapshot_version),
    partitionColumnTypes: generatedPartitionColumnTypes(partitionTypes),
    browserCompatibility: create(CapabilityReportSchema),
    requiredCapabilities: create(CapabilityReportSchema),
    activeFiles: snapshot.active_files.map((file) => {
      const entry = localFileForDeltaPath(table, file.path);
      if (!entry) {
        throw new LocalDeltaError(
          'missing_active_file',
          `Delta log references '${file.path}', but that file was not present in the selected folder.`,
        );
      }
      if (entry.file.size !== file.size_bytes) {
        throw new LocalDeltaError(
          'missing_active_file',
          `Active file '${file.path}' size ${entry.file.size} did not match Delta log size ${file.size_bytes}.`,
        );
      }
      return create(BrowserHttpFileDescriptorSchema, {
        path: file.path,
        url: trackLocalObjectUrl(registryId, URL.createObjectURL(entry.file), ownedObjectUrls),
        sizeBytes: BigInt(file.size_bytes),
        partitionValues: Object.fromEntries(
          Object.entries(file.partition_values).map(([name, value]) => [
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
      });
    }),
  });

  return {
    kind: 'local_delta',
    registryId,
    persistence,
    tableRootName: table.tableRootName,
    tableName,
    schemaName,
    storageLabel: `Local folder: ${table.tableRootName}`,
    descriptor,
    catalogMetadata: catalogMetadataFromRuntimeFacts(descriptor, facts),
  };
}

async function openLocalDeltaRuntime(
  table: LocalDeltaTableFiles,
  options: OpenLocalDeltaOptions,
): Promise<LocalDeltaRuntime> {
  const registryId =
    options.registryId ?? table.registryId ?? localDeltaRegistryId(table.tableRootName);
  return withLocalDeltaAcquisition(registryId, () =>
    openLocalDeltaRuntimeExclusive(table, options, registryId),
  );
}

async function openLocalDeltaRuntimeExclusive(
  table: LocalDeltaTableFiles,
  options: OpenLocalDeltaOptions,
  registryId: string,
): Promise<LocalDeltaRuntime> {
  const sessionTable = { ...table, registryId };
  const ownedObjectUrls = new Set<string>();
  let runtime: LocalDeltaRuntime;
  let persisted: LocalDeltaTableFiles | undefined;
  let previousRegistryRecord: LocalDeltaRegistryRecord | undefined;
  let registrySnapshotRead = false;
  try {
    throwIfLocalDeltaAborted(options.signal);
    runtime = await buildLocalDeltaRuntime(
      sessionTable,
      options,
      'session_handles',
      ownedObjectUrls,
    );
    throwIfLocalDeltaAborted(options.signal);
    const durableTable = durableLocalDeltaTableForRuntime(sessionTable, runtime);
    try {
      previousRegistryRecord = await localDeltaHandleStore.get(registryId);
      registrySnapshotRead = true;
    } catch {
      // Keep the working session runtime when the durable registry cannot be inspected safely.
    }
    if (registrySnapshotRead) {
      persisted = await tryPersistLocalDeltaTable(durableTable);
    }
    throwIfLocalDeltaAborted(options.signal);
  } catch (error) {
    releaseOwnedLocalDeltaObjectUrls(registryId, ownedObjectUrls);
    if (persisted && registrySnapshotRead) {
      await restoreLocalDeltaRegistryRecord(registryId, previousRegistryRecord);
    }
    throw error;
  }
  commitOwnedLocalDeltaObjectUrls(registryId, ownedObjectUrls);
  if (persisted) {
    const durableRuntime = {
      ...runtime,
      persistence: persisted.persistenceMode ?? runtime.persistence,
    };
    sessionLocalDeltaTables.set(registryId, persisted);
    localDeltaRuntimes.set(localDeltaRuntimeCacheKey(registryId, options), durableRuntime);
    markLocalDeltaRuntimeActive(registryId);
    setActiveLocalDeltaRegistryId(registryId);
    return durableRuntime;
  }
  sessionLocalDeltaTables.set(registryId, sessionTable);
  localDeltaRuntimes.set(localDeltaRuntimeCacheKey(registryId, options), runtime);
  markLocalDeltaRuntimeActive(registryId);
  return runtime;
}

async function withLocalDeltaAcquisition<T>(
  registryId: string,
  operation: () => Promise<T>,
): Promise<T> {
  const previous = localDeltaAcquisitionQueues.get(registryId) ?? Promise.resolve();
  let release!: () => void;
  const current = new Promise<void>((resolve) => {
    release = resolve;
  });
  const tail = previous.then(() => current);
  localDeltaAcquisitionQueues.set(registryId, tail);
  await previous;
  try {
    return await operation();
  } finally {
    release();
    if (localDeltaAcquisitionQueues.get(registryId) === tail) {
      localDeltaAcquisitionQueues.delete(registryId);
    }
  }
}

function throwIfLocalDeltaAborted(signal: AbortSignal | undefined): void {
  if (!signal?.aborted) return;
  const error = new Error('local Delta acquisition was cancelled');
  error.name = 'AbortError';
  throw error;
}

function collectLocalDeltaTableFiles(files: FileList | File[] | null): LocalDeltaTableFiles {
  const selectedFiles = Array.from(files ?? []);
  if (selectedFiles.length === 0) {
    throw new LocalDeltaError('empty_selection', 'Select a local Delta table directory first.');
  }

  const rawEntries = selectedFiles.map((file) => ({
    file,
    browserPath: normalizeBrowserFilePath(fileBrowserPath(file)),
  }));
  const rootPrefix = localDeltaRootPrefix(rawEntries.map((entry) => entry.browserPath));
  const entries: LocalDeltaFileEntry[] = [];
  for (const entry of rawEntries) {
    const relativePath = tableRelativePath(entry.browserPath, rootPrefix);
    if (
      relativePath === undefined ||
      relativePath.length === 0 ||
      isIgnoredLocalFile(relativePath)
    ) {
      continue;
    }
    entries.push({ ...entry, relativePath });
  }
  return buildLocalDeltaTableFiles(localTableRootName(rootPrefix), entries);
}

async function collectLocalDeltaDirectoryEntries(
  directory: LocalFileSystemDirectoryHandle,
  prefix = '',
  signal?: AbortSignal,
): Promise<LocalDeltaFileEntry[]> {
  throwIfLocalDeltaAborted(signal);
  const entries: LocalDeltaFileEntry[] = [];
  for await (const [name, handle] of directory.entries()) {
    throwIfLocalDeltaAborted(signal);
    const relativePath = prefix ? `${prefix}/${name}` : name;
    if (isIgnoredLocalFile(relativePath)) continue;
    if (handle.kind === 'directory') {
      entries.push(...(await collectLocalDeltaDirectoryEntries(handle, relativePath, signal)));
    } else {
      validateLocalRelativePath(relativePath);
      const file = await handle.getFile();
      throwIfLocalDeltaAborted(signal);
      entries.push({
        file,
        browserPath: `${directory.name}/${relativePath}`,
        relativePath,
      });
    }
  }
  return entries;
}

function buildLocalDeltaTableFiles(
  tableRootName: string,
  entries: LocalDeltaFileEntry[],
  registryId?: string,
  options: {
    directoryHandle?: LocalFileSystemDirectoryHandle;
    persistenceMode?: LocalDeltaPersistenceMode;
  } = {},
): LocalDeltaTableFiles {
  const filesByRelativePath = new Map<string, LocalDeltaFileEntry>();
  for (const entry of entries) {
    validateLocalRelativePath(entry.relativePath);
    if (filesByRelativePath.has(entry.relativePath)) {
      throw new LocalDeltaError(
        'invalid_path',
        `Selected folder contained duplicate path '${entry.relativePath}'.`,
      );
    }
    filesByRelativePath.set(entry.relativePath, entry);
  }

  const logEntries = entries
    .filter((entry) => entry.relativePath.startsWith('_delta_log/'))
    .sort(compareLocalDeltaEntries);
  if (logEntries.length === 0) {
    throw new LocalDeltaError(
      'missing_delta_log',
      'Selected folder is not a Delta table because it does not contain _delta_log/.',
    );
  }

  return {
    registryId,
    directoryHandle: options.directoryHandle,
    persistenceMode: options.persistenceMode,
    tableRootName,
    filesByRelativePath,
    logEntries,
    dataEntries: entries
      .filter((entry) => isParquetDataFile(entry.relativePath))
      .sort(compareLocalDeltaEntries),
  };
}

async function persistLocalDeltaTable(
  table: LocalDeltaTableFiles,
  requestedId?: string,
): Promise<LocalDeltaTableFiles> {
  const id = requestedId ?? table.registryId ?? localDeltaRegistryId(table.tableRootName);
  const baseRecord = {
    id,
    tableRootName: table.tableRootName,
    importedAtEpochMs: Date.now(),
  };
  const files = localDeltaMetadataRecords(table);

  if (table.directoryHandle) {
    try {
      await putLocalDeltaRegistryRecord({
        ...baseRecord,
        backend: 'directory_handle',
        files,
        directoryHandle: table.directoryHandle,
      });
      return { ...table, registryId: id, persistenceMode: 'persisted_directory_handle' };
    } catch (error) {
      console.warn(
        'local Delta directory handle could not be persisted; using metadata-only registry:',
        error,
      );
    }
  }

  await putLocalDeltaRegistryRecord({
    ...baseRecord,
    backend: 'metadata_only',
    files,
  });
  return { ...table, registryId: id, persistenceMode: 'metadata_only_reselect' };
}

async function tryPersistLocalDeltaTable(
  table: LocalDeltaTableFiles,
): Promise<LocalDeltaTableFiles | undefined> {
  try {
    return await persistLocalDeltaTable(table);
  } catch (error) {
    console.warn('local Delta table import could not be persisted:', error);
    return undefined;
  }
}

function durableLocalDeltaTableForRuntime(
  table: LocalDeltaTableFiles,
  runtime: LocalDeltaRuntime,
): LocalDeltaTableFiles {
  const entries = new Map<string, LocalDeltaFileEntry>();
  for (const entry of table.logEntries) entries.set(entry.relativePath, entry);
  for (const file of runtime.descriptor.activeFiles) {
    const entry = localFileForDeltaPath(table, file.path);
    if (entry) entries.set(entry.relativePath, entry);
  }
  return buildLocalDeltaTableFiles(table.tableRootName, [...entries.values()], runtime.registryId, {
    directoryHandle: table.directoryHandle,
  });
}

async function loadLocalDeltaTable(
  registryId: string,
  signal?: AbortSignal,
): Promise<LocalDeltaTableFiles | undefined> {
  throwIfLocalDeltaAborted(signal);
  const record = await localDeltaHandleStore.get(registryId);
  throwIfLocalDeltaAborted(signal);
  if (!record) return undefined;

  if (record.backend === 'metadata_only') {
    throw new LocalDeltaError(
      'registry_unavailable',
      'This local Delta table was saved as metadata only. Select the folder again to restore browser file access before querying.',
    );
  }

  if (record.backend === 'directory_handle') {
    return loadDirectoryHandleLocalDeltaTable(record, signal);
  }

  return undefined;
}

function localDeltaMetadataRecords(table: LocalDeltaTableFiles): LocalDeltaRegistryFileRecord[] {
  return [...table.filesByRelativePath.values()].map((entry) => localDeltaMetadataRecord(entry));
}

function localDeltaMetadataRecord(entry: LocalDeltaFileEntry): LocalDeltaRegistryFileRecord {
  return {
    relativePath: entry.relativePath,
    sizeBytes: entry.file.size,
    lastModified: entry.file.lastModified,
    mimeType: entry.file.type,
  };
}

async function loadDirectoryHandleLocalDeltaTable(
  record: LocalDeltaRegistryRecord,
  signal?: AbortSignal,
): Promise<LocalDeltaTableFiles | undefined> {
  const handle = record.directoryHandle;
  if (!handle) {
    throw new LocalDeltaError(
      'registry_unavailable',
      'Persisted local Delta directory handle was missing. Select the folder again.',
    );
  }

  throwIfLocalDeltaAborted(signal);
  const granted = await ensureDirectoryReadPermission(handle);
  throwIfLocalDeltaAborted(signal);
  if (!granted) {
    throw new LocalDeltaError(
      'registry_unavailable',
      'Browser permission for this local Delta folder expired. Select the folder again.',
    );
  }

  const entries = await collectLocalDeltaDirectoryEntries(handle, '', signal);
  const table = buildLocalDeltaTableFiles(record.tableRootName, entries, record.id, {
    directoryHandle: handle,
    persistenceMode: 'persisted_directory_handle',
  });
  validateLocalDeltaTableAgainstRecord(table, record);
  return table;
}

async function revalidateLocalDeltaTableAccess(
  table: LocalDeltaTableFiles,
  signal?: AbortSignal,
): Promise<void> {
  if (!table.directoryHandle) return;
  throwIfLocalDeltaAborted(signal);
  const granted = await ensureDirectoryReadPermission(table.directoryHandle);
  throwIfLocalDeltaAborted(signal);
  if (!granted) {
    throw new LocalDeltaError(
      'registry_unavailable',
      'Browser permission for this local Delta folder expired. Select the folder again.',
    );
  }
}

function validateLocalDeltaTableAgainstRecord(
  table: LocalDeltaTableFiles,
  record: LocalDeltaRegistryRecord,
): void {
  for (const fileRecord of record.files) {
    const entry = table.filesByRelativePath.get(fileRecord.relativePath);
    if (!entry) {
      throw new LocalDeltaError(
        'registry_unavailable',
        `Persisted local file '${fileRecord.relativePath}' was not present in the selected folder.`,
      );
    }
    if (entry.file.size !== fileRecord.sizeBytes) {
      throw new LocalDeltaError(
        'registry_unavailable',
        `Persisted local file '${fileRecord.relativePath}' size ${entry.file.size} did not match registry size ${fileRecord.sizeBytes}.`,
      );
    }
  }
}

async function readLocalLogFacts(
  logEntries: LocalDeltaFileEntry[],
  signal?: AbortSignal,
): Promise<LocalLogFacts> {
  const facts: LocalLogFacts = { partitionColumns: [] };
  const commitEntries = logEntries.filter((entry) =>
    /^_delta_log\/\d{20}\.json$/.test(entry.relativePath),
  );

  for (const entry of commitEntries) {
    throwIfLocalDeltaAborted(signal);
    const text = await entry.file.text();
    throwIfLocalDeltaAborted(signal);
    for (const [index, line] of text.split(/\r?\n/).entries()) {
      if (!line.trim()) continue;
      let action: unknown;
      try {
        action = JSON.parse(line) as unknown;
      } catch (error) {
        throw new LocalDeltaError(
          'invalid_delta_log',
          `Could not parse ${entry.relativePath} line ${index + 1}: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      }
      applyLocalLogAction(facts, action);
    }
  }

  return facts;
}

function applyLocalLogAction(facts: LocalLogFacts, action: unknown): void {
  if (!isRecord(action)) return;

  if (isRecord(action.protocol)) {
    const features = [
      ...stringArray(action.protocol.readerFeatures),
      ...stringArray(action.protocol.writerFeatures),
    ];
    if (features.length > 0) {
      throw new LocalDeltaError(
        'unsupported_delta_feature',
        `Selected local Delta table requires unsupported features: ${features.join(', ')}.`,
      );
    }
    facts.minReaderVersion = numberField(action.protocol, 'minReaderVersion');
    facts.minWriterVersion = numberField(action.protocol, 'minWriterVersion');
  }

  if (isRecord(action.metaData)) {
    const configuration = action.metaData.configuration;
    if (isRecord(configuration)) {
      const columnMappingMode = stringField(configuration, 'delta.columnMapping.mode');
      if (columnMappingMode && columnMappingMode !== 'none') {
        throw new LocalDeltaError(
          'unsupported_delta_feature',
          `Selected local Delta table uses unsupported column mapping mode '${columnMappingMode}'.`,
        );
      }
      const deletionVectorsEnabled = configuration['delta.enableDeletionVectors'];
      if (deletionVectorsEnabled === true || deletionVectorsEnabled === 'true') {
        throw new LocalDeltaError(
          'unsupported_delta_feature',
          'Selected local Delta table uses deletion vectors, which this browser runtime cannot apply.',
        );
      }
    }
    facts.tableName = stringField(action.metaData, 'name') ?? facts.tableName;
    facts.schemaString = stringField(action.metaData, 'schemaString') ?? facts.schemaString;
    if (Array.isArray(action.metaData.partitionColumns)) {
      facts.partitionColumns = action.metaData.partitionColumns.filter(
        (value): value is string => typeof value === 'string',
      );
    }
  }

  if (isRecord(action.add) && isRecord(action.add.deletionVector)) {
    throw new LocalDeltaError(
      'unsupported_delta_feature',
      'Selected local Delta table uses deletion vectors, which this browser runtime cannot apply.',
    );
  }
}

function catalogMetadataFromRuntimeFacts(
  descriptor: BrowserHttpSnapshotDescriptor,
  facts: LocalLogFacts,
): TableMetadata {
  const rows = descriptor.activeFiles.reduce((total, file) => {
    const statsRows = rowsFromStats(file.stats);
    return statsRows === undefined ? total : total + statsRows;
  }, 0);
  const sizeBytes = descriptor.activeFiles.reduce(
    (total, file) => total + BigInt(safeGeneratedInteger(file.sizeBytes, 'active file size')),
    0n,
  );
  if (!Number.isSafeInteger(rows) || rows < 0) {
    throw new LocalDeltaError('invalid_delta_log', 'Resolved Delta row count was invalid.');
  }

  return create(TableMetadataSchema, {
    columns: catalogColumnsFromSchema(facts.schemaString),
    partitionColumns: [...facts.partitionColumns],
    rowCount: BigInt(rows),
    sizeBytes,
    fileCount: BigInt(descriptor.activeFiles.length),
    latestSnapshotVersion: descriptor.snapshotVersion,
    minReaderVersion: facts.minReaderVersion,
    minWriterVersion: facts.minWriterVersion,
    storageLocation: descriptor.tableUri,
  });
}

function catalogColumnsFromSchema(schemaString: string | undefined) {
  if (!schemaString) return [];
  try {
    const schema = JSON.parse(schemaString) as unknown;
    if (!isRecord(schema) || !Array.isArray(schema.fields)) return [];
    return schema.fields.filter(isRecord).map((field) =>
      create(ColumnNodeSchema, {
        name: stringField(field, 'name') ?? 'column',
        type: typeof field.type === 'string' ? field.type : 'unknown',
        nullable: field.nullable !== false,
      }),
    );
  } catch {
    return [];
  }
}

function rowsFromStats(stats: string | undefined): number | undefined {
  if (!stats) return undefined;
  try {
    const parsed = JSON.parse(stats) as unknown;
    return isRecord(parsed) && typeof parsed.numRecords === 'number'
      ? parsed.numRecords
      : undefined;
  } catch {
    return undefined;
  }
}

function partitionTypesFromSchema(
  schemaString: string | undefined,
  partitionColumns: readonly string[],
): Partial<Record<string, ResolvedPartitionColumnType>> {
  if (!schemaString) {
    return Object.fromEntries(partitionColumns.map((name) => [name, 'string']));
  }

  try {
    const schema = JSON.parse(schemaString) as unknown;
    if (!isRecord(schema) || !Array.isArray(schema.fields)) return {};
    const fieldTypes = new Map<string, ResolvedPartitionColumnType>();
    for (const field of schema.fields) {
      if (!isRecord(field)) continue;
      const name = stringField(field, 'name');
      if (!name) continue;
      fieldTypes.set(name, partitionType(field.type));
    }
    return Object.fromEntries(
      partitionColumns.map((name) => [name, fieldTypes.get(name) ?? 'string']),
    );
  } catch {
    return Object.fromEntries(partitionColumns.map((name) => [name, 'string']));
  }
}

function partitionType(value: unknown): ResolvedPartitionColumnType {
  if (value === 'long' || value === 'integer' || value === 'short' || value === 'byte') {
    return 'int64';
  }
  if (value === 'boolean') return 'boolean';
  if (value === 'string' || value === undefined) return 'string';
  return 'unsupported';
}

function generatedPartitionColumnTypes(
  values: Partial<Record<string, ResolvedPartitionColumnType>>,
): Record<string, PartitionColumnType> {
  return Object.fromEntries(
    Object.entries(values).map(([name, value]) => [
      name,
      value === 'int64'
        ? PartitionColumnType.INT64
        : value === 'boolean'
          ? PartitionColumnType.BOOLEAN
          : value === 'unsupported'
            ? PartitionColumnType.UNSUPPORTED
            : PartitionColumnType.STRING,
    ]),
  );
}

function safeGeneratedInteger(value: bigint | undefined, field: string): number {
  if (value === undefined) {
    throw new LocalDeltaError('invalid_delta_log', `Resolved Delta ${field} was missing.`);
  }
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number < 0) {
    throw new LocalDeltaError(
      'invalid_delta_log',
      `Resolved Delta ${field} was outside the JavaScript-safe integer range.`,
    );
  }
  return number;
}

function localDeltaRuntimeCacheKey(registryId: string, options: OpenLocalDeltaOptions): string {
  return JSON.stringify([
    registryId,
    options.schemaName ?? 'default',
    options.tableName ?? '',
    options.snapshotVersion ?? null,
  ]);
}

function deleteLocalDeltaRuntimeCache(registryId: string): void {
  for (const key of localDeltaRuntimes.keys()) {
    const parsed = JSON.parse(key) as unknown;
    if (Array.isArray(parsed) && parsed[0] === registryId) {
      localDeltaRuntimes.delete(key);
    }
  }
}

function localDeltaRegistryId(tableRootName: string): string {
  const safeName = tableRootName.replace(/[^A-Za-z0-9._-]+/g, '-').replace(/^-+|-+$/g, '');
  return `${safeName || 'delta-table'}-${Date.now().toString(36)}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;
}

function setActiveLocalDeltaRegistryId(id: string): void {
  try {
    localStorage.setItem(LOCAL_DELTA_ACTIVE_ID_KEY, id);
  } catch {
    // Persistence is opportunistic; the selected table still works for this session.
  }
}

function activeLocalDeltaRegistryId(): string | undefined {
  try {
    const id = localStorage.getItem(LOCAL_DELTA_ACTIVE_ID_KEY)?.trim();
    return id ? id : undefined;
  } catch {
    return undefined;
  }
}

function validateLocalRelativePath(path: string): void {
  if (
    path.length === 0 ||
    path.startsWith('/') ||
    path.startsWith('\\') ||
    path.includes('\0') ||
    path.split('/').some((segment) => segment.length === 0 || segment === '.' || segment === '..')
  ) {
    throw new LocalDeltaError(
      'invalid_path',
      `Local Delta table path '${path}' must stay inside the selected table root.`,
    );
  }
}

async function putLocalDeltaRegistryRecord(record: LocalDeltaRegistryRecord): Promise<void> {
  return localDeltaHandleStore.put(record);
}

async function deleteLocalDeltaRegistryRecord(registryId: string): Promise<void> {
  return localDeltaHandleStore.delete(registryId);
}

async function restoreLocalDeltaRegistryRecord(
  registryId: string,
  previous: LocalDeltaRegistryRecord | undefined,
): Promise<void> {
  try {
    if (previous) {
      await putLocalDeltaRegistryRecord(previous);
    } else {
      await deleteLocalDeltaRegistryRecord(registryId);
    }
  } catch {
    // Best-effort rollback after cancellation only.
  }
}

function fileBrowserPath(file: File): string {
  return (file as File & { webkitRelativePath?: string }).webkitRelativePath || file.name;
}

function normalizeBrowserFilePath(path: string): string {
  return path.replaceAll('\\', '/').replace(/^\/+/, '').replace(/\/+/g, '/');
}

function localDeltaRootPrefix(paths: string[]): string {
  const prefixes = new Set<string>();
  for (const path of paths) {
    const segments = path.split('/');
    const deltaLogIndex = segments.indexOf('_delta_log');
    if (deltaLogIndex >= 0) prefixes.add(segments.slice(0, deltaLogIndex).join('/'));
  }

  if (prefixes.size === 0) {
    throw new LocalDeltaError(
      'missing_delta_log',
      'Select the Delta table directory containing _delta_log/.',
    );
  }
  if (prefixes.size > 1) {
    throw new LocalDeltaError(
      'invalid_path',
      'Selected files contain multiple Delta table roots. Select one table folder.',
    );
  }
  return [...prefixes][0];
}

function tableRelativePath(browserPath: string, rootPrefix: string): string | undefined {
  if (!rootPrefix) return browserPath;
  if (browserPath === rootPrefix) return '';
  const prefix = `${rootPrefix}/`;
  if (!browserPath.startsWith(prefix)) return undefined;
  return browserPath.slice(prefix.length);
}

function localTableRootName(rootPrefix: string): string {
  return rootPrefix.split('/').filter(Boolean).at(-1) ?? 'delta-table';
}

function localTableUri(rootName: string): string {
  return `browser-local://delta-table/${encodeURIComponent(rootName)}`;
}

function localFileForDeltaPath(
  localTable: LocalDeltaTableFiles,
  deltaPath: string,
): LocalDeltaFileEntry | undefined {
  return (
    localTable.filesByRelativePath.get(deltaPath) ??
    localTable.filesByRelativePath.get(decodeDeltaPath(deltaPath))
  );
}

function decodeDeltaPath(path: string): string {
  try {
    return decodeURIComponent(path);
  } catch {
    return path;
  }
}

function isIgnoredLocalFile(path: string): boolean {
  const filename = path.split('/').at(-1) ?? path;
  return filename === '.DS_Store' || filename.endsWith('.crc');
}

function isParquetDataFile(path: string): boolean {
  return !path.startsWith('_delta_log/') && path.endsWith('.parquet');
}

function compareLocalDeltaEntries(left: LocalDeltaFileEntry, right: LocalDeltaFileEntry): number {
  return left.relativePath.localeCompare(right.relativePath);
}

function classifyObject(path: string): ObjectKind {
  if (path === '_delta_log/_last_checkpoint') return 'last_checkpoint';
  if (path.endsWith('.checkpoint.parquet')) return 'checkpoint_parquet';
  if (path.endsWith('.json')) return 'commit_json';
  return 'delta_log_object';
}

function trackLocalObjectUrl(
  registryId: string,
  url: string,
  ownedObjectUrls?: Set<string>,
): string {
  const urls = localObjectUrlsByRegistryId.get(registryId) ?? new Set<string>();
  urls.add(url);
  ownedObjectUrls?.add(url);
  localObjectUrlsByRegistryId.set(registryId, urls);
  return url;
}

function releaseOwnedLocalDeltaObjectUrls(
  registryId: string,
  ownedObjectUrls: ReadonlySet<string>,
): void {
  const registeredUrls = localObjectUrlsByRegistryId.get(registryId);
  for (const url of ownedObjectUrls) {
    URL.revokeObjectURL(url);
    registeredUrls?.delete(url);
  }
  if (registeredUrls?.size === 0) {
    localObjectUrlsByRegistryId.delete(registryId);
  }
}

function commitOwnedLocalDeltaObjectUrls(
  registryId: string,
  ownedObjectUrls: ReadonlySet<string>,
): void {
  const registeredUrls = localObjectUrlsByRegistryId.get(registryId);
  for (const url of registeredUrls ?? []) {
    if (!ownedObjectUrls.has(url)) {
      URL.revokeObjectURL(url);
    }
  }
  localObjectUrlsByRegistryId.set(registryId, new Set(ownedObjectUrls));
  deleteLocalDeltaRuntimeCache(registryId);
}

function sanitizeSqlIdentifier(name: string): string {
  const sanitized = name
    .trim()
    .replace(/[^A-Za-z0-9_]+/g, '_')
    .replace(/^_+|_+$/g, '');
  return sanitized || 'local_delta_table';
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function stringField(value: Record<string, unknown>, key: string): string | undefined {
  return typeof value[key] === 'string' ? value[key] : undefined;
}

function numberField(value: Record<string, unknown>, key: string): number | undefined {
  return typeof value[key] === 'number' && Number.isFinite(value[key]) ? value[key] : undefined;
}

function stringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.filter((item): item is string => typeof item === 'string');
}
