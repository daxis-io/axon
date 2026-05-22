import init, { resolve_delta_snapshot_from_manifest } from '../wasm/axon_web_wasm.js';
import type { BrowserHttpSnapshotDescriptor, PartitionColumnType } from '../axon-browser-sdk.ts';

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

export type LocalDeltaDiscoveredTable = {
  name: string;
  snapshot: number;
  rows: number;
  files: number;
  size: string;
  protocol: string;
  features?: string[];
  uri?: string;
  columns?: { name: string; type: string; part?: boolean }[];
};

export type LocalDeltaDiscovery = {
  summary: string;
  schemas: Array<{
    name: string;
    tableCount: number;
    included: boolean;
    tables: LocalDeltaDiscoveredTable[];
  }>;
};

export type LocalDeltaRuntime = {
  kind: 'local_delta';
  registryId: string;
  persistence: LocalDeltaPersistenceMode;
  tableRootName: string;
  tableName: string;
  schemaName: string;
  storageLabel: string;
  descriptor: BrowserHttpSnapshotDescriptor;
  discovery: LocalDeltaDiscovery;
};

export type LocalDeltaPersistenceMode =
  | 'session_handles'
  | 'persisted_directory_handle'
  | 'metadata_only_reselect';

export type OpenLocalDeltaOptions = {
  schemaName?: string;
  tableName?: string;
  registryId?: string;
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

type LocalDeltaRegistryBackend = 'metadata_only' | 'directory_handle';

type LocalDeltaRegistryFileRecord = {
  relativePath: string;
  sizeBytes: number;
  lastModified?: number;
  mimeType?: string;
};

type LocalDeltaRegistryRecord = {
  id: string;
  tableRootName: string;
  importedAtEpochMs: number;
  backend: LocalDeltaRegistryBackend;
  files: LocalDeltaRegistryFileRecord[];
  directoryHandle?: LocalFileSystemDirectoryHandle;
};

type ResolvedSnapshot = {
  table_uri: string;
  snapshot_version: number;
  partition_column_types?: Partial<Record<string, PartitionColumnType>>;
  active_files: Array<{
    path: string;
    size_bytes: number;
    partition_values: Record<string, string | null>;
    stats?: string;
  }>;
};

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
const localObjectUrlsByRegistryId = new Map<string, Set<string>>();

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
  const entries = await collectLocalDeltaDirectoryEntries(directory);
  const table = buildLocalDeltaTableFiles(directory.name || 'delta-table', entries, undefined, {
    directoryHandle: directory,
  });
  return openLocalDeltaRuntime(table, options);
}

export async function loadLocalDeltaRuntime(
  registryId: string,
  options: OpenLocalDeltaOptions = {},
): Promise<LocalDeltaRuntime> {
  const table = sessionLocalDeltaTables.get(registryId) ?? (await loadLocalDeltaTable(registryId));
  if (!table) {
    throw new LocalDeltaError(
      'registry_unavailable',
      'Local Delta table registry entry could not be reopened. Select the folder again.',
    );
  }
  sessionLocalDeltaTables.set(registryId, table);
  return buildLocalDeltaRuntime(
    table,
    { ...options, registryId },
    table.persistenceMode ?? 'session_handles',
  );
}

export async function loadActiveLocalDeltaRuntime(
  options: OpenLocalDeltaOptions = {},
): Promise<LocalDeltaRuntime | undefined> {
  const activeId = activeLocalDeltaRegistryId();
  if (!activeId) return undefined;
  return loadLocalDeltaRuntime(activeId, options);
}

export function hasLocalDeltaRuntime(registryId?: string): boolean {
  return registryId ? sessionLocalDeltaTables.has(registryId) : false;
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
    if (!urls) return;
    for (const url of urls) URL.revokeObjectURL(url);
    localObjectUrlsByRegistryId.delete(registryId);
    return;
  }

  for (const urls of localObjectUrlsByRegistryId.values()) {
    for (const url of urls) URL.revokeObjectURL(url);
  }
  localObjectUrlsByRegistryId.clear();
}

export async function unregisterLocalDeltaRuntime(registryId: string): Promise<void> {
  sessionLocalDeltaTables.delete(registryId);
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
  wasmReady ??= init();
  return wasmReady;
}

async function buildLocalDeltaRuntime(
  table: LocalDeltaTableFiles,
  options: OpenLocalDeltaOptions,
  persistence: LocalDeltaRuntime['persistence'],
): Promise<LocalDeltaRuntime> {
  await ensureWasm();
  const facts = await readLocalLogFacts(table.logEntries);
  const tableName = sanitizeSqlIdentifier(
    options.tableName ?? facts.tableName ?? table.tableRootName,
  );
  const schemaName = options.schemaName ?? 'default';
  const registryId =
    table.registryId ?? options.registryId ?? localDeltaRegistryId(table.tableRootName);
  const tableUri = localTableUri(table.tableRootName);

  const logObjects = table.logEntries.map((entry) => ({
    relative_path: entry.relativePath,
    url: trackLocalObjectUrl(registryId, URL.createObjectURL(entry.file)),
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
  );
  const snapshot = JSON.parse(snapshotJson) as ResolvedSnapshot;
  const partitionTypes =
    snapshot.partition_column_types ??
    partitionTypesFromSchema(facts.schemaString, facts.partitionColumns);
  const descriptor: BrowserHttpSnapshotDescriptor = {
    table_uri: snapshot.table_uri,
    snapshot_version: snapshot.snapshot_version,
    partition_column_types: partitionTypes,
    browser_compatibility: { capabilities: {} },
    required_capabilities: { capabilities: {} },
    active_files: snapshot.active_files.map((file) => {
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
      return {
        path: file.path,
        url: trackLocalObjectUrl(registryId, URL.createObjectURL(entry.file)),
        size_bytes: file.size_bytes,
        partition_values: file.partition_values,
        stats: file.stats,
      };
    }),
  };

  return {
    kind: 'local_delta',
    registryId,
    persistence,
    tableRootName: table.tableRootName,
    tableName,
    schemaName,
    storageLabel: `Local folder: ${table.tableRootName}`,
    descriptor,
    discovery: discoveryFromRuntimeFacts(schemaName, tableName, descriptor, facts),
  };
}

async function openLocalDeltaRuntime(
  table: LocalDeltaTableFiles,
  options: OpenLocalDeltaOptions,
): Promise<LocalDeltaRuntime> {
  const registryId =
    options.registryId ?? table.registryId ?? localDeltaRegistryId(table.tableRootName);
  const sessionTable = { ...table, registryId };
  let runtime: LocalDeltaRuntime;
  try {
    runtime = await buildLocalDeltaRuntime(sessionTable, options, 'session_handles');
  } catch (error) {
    releaseLocalDeltaObjectUrls(registryId);
    throw error;
  }
  sessionLocalDeltaTables.set(registryId, sessionTable);

  const durableTable = durableLocalDeltaTableForRuntime(sessionTable, runtime);
  const persisted = await tryPersistLocalDeltaTable(durableTable);
  if (persisted) {
    return { ...runtime, persistence: persisted.persistenceMode ?? runtime.persistence };
  }
  return runtime;
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
): Promise<LocalDeltaFileEntry[]> {
  const entries: LocalDeltaFileEntry[] = [];
  for await (const [name, handle] of directory.entries()) {
    const relativePath = prefix ? `${prefix}/${name}` : name;
    if (isIgnoredLocalFile(relativePath)) continue;
    if (handle.kind === 'directory') {
      entries.push(...(await collectLocalDeltaDirectoryEntries(handle, relativePath)));
    } else {
      validateLocalRelativePath(relativePath);
      entries.push({
        file: await handle.getFile(),
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
    const persisted = await persistLocalDeltaTable(table);
    sessionLocalDeltaTables.set(
      persisted.registryId ?? table.registryId ?? table.tableRootName,
      persisted,
    );
    if (persisted.registryId) {
      setActiveLocalDeltaRegistryId(persisted.registryId);
    }
    return persisted;
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
  for (const file of runtime.descriptor.active_files) {
    const entry = localFileForDeltaPath(table, file.path);
    if (entry) entries.set(entry.relativePath, entry);
  }
  return buildLocalDeltaTableFiles(table.tableRootName, [...entries.values()], runtime.registryId, {
    directoryHandle: table.directoryHandle,
  });
}

async function loadLocalDeltaTable(registryId: string): Promise<LocalDeltaTableFiles | undefined> {
  const records = await getLocalDeltaRegistryRecords();
  const record = records.find((candidate) => candidate.id === registryId);
  if (!record) return undefined;

  if (record.backend === 'metadata_only') {
    throw new LocalDeltaError(
      'registry_unavailable',
      'This local Delta table was saved as metadata only. Select the folder again to restore browser file access before querying.',
    );
  }

  if (record.backend === 'directory_handle') {
    return loadDirectoryHandleLocalDeltaTable(record);
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
): Promise<LocalDeltaTableFiles | undefined> {
  const handle = record.directoryHandle;
  if (!handle) {
    throw new LocalDeltaError(
      'registry_unavailable',
      'Persisted local Delta directory handle was missing. Select the folder again.',
    );
  }

  const granted = await ensureDirectoryHandleReadPermission(handle);
  if (!granted) {
    throw new LocalDeltaError(
      'registry_unavailable',
      'Browser permission for this local Delta folder expired. Select the folder again.',
    );
  }

  const entries = await collectLocalDeltaDirectoryEntries(handle);
  const table = buildLocalDeltaTableFiles(record.tableRootName, entries, record.id, {
    directoryHandle: handle,
    persistenceMode: 'persisted_directory_handle',
  });
  validateLocalDeltaTableAgainstRecord(table, record);
  return table;
}

async function ensureDirectoryHandleReadPermission(
  handle: LocalFileSystemDirectoryHandle,
): Promise<boolean> {
  if (!handle.queryPermission) return true;
  const current = await handle.queryPermission({ mode: 'read' });
  if (current === 'granted') return true;
  if (!handle.requestPermission) return false;
  try {
    return (await handle.requestPermission({ mode: 'read' })) === 'granted';
  } catch {
    return false;
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

async function readLocalLogFacts(logEntries: LocalDeltaFileEntry[]): Promise<LocalLogFacts> {
  const facts: LocalLogFacts = { partitionColumns: [] };
  const commitEntries = logEntries.filter((entry) =>
    /^_delta_log\/\d{20}\.json$/.test(entry.relativePath),
  );

  for (const entry of commitEntries) {
    const text = await entry.file.text();
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

function discoveryFromRuntimeFacts(
  schemaName: string,
  tableName: string,
  descriptor: BrowserHttpSnapshotDescriptor,
  facts: LocalLogFacts,
): LocalDeltaDiscovery {
  const rows = descriptor.active_files.reduce((total, file) => {
    const statsRows = rowsFromStats(file.stats);
    return statsRows === undefined ? total : total + statsRows;
  }, 0);
  const sizeBytes = descriptor.active_files.reduce((total, file) => total + file.size_bytes, 0);
  const protocol =
    facts.minReaderVersion !== undefined && facts.minWriterVersion !== undefined
      ? `r${facts.minReaderVersion}/w${facts.minWriterVersion}`
      : 'json-log';

  return {
    summary: 'Detected 1 local Delta table',
    schemas: [
      {
        name: schemaName,
        tableCount: 1,
        included: true,
        tables: [
          {
            name: tableName,
            snapshot: descriptor.snapshot_version,
            rows,
            files: descriptor.active_files.length,
            size: formatBytes(sizeBytes),
            protocol,
            uri: descriptor.table_uri,
            columns: columnsFromSchema(facts.schemaString, facts.partitionColumns),
          },
        ],
      },
    ],
  };
}

function columnsFromSchema(
  schemaString: string | undefined,
  partitionColumns: readonly string[],
): { name: string; type: string; part?: boolean }[] | undefined {
  if (!schemaString) return undefined;
  try {
    const schema = JSON.parse(schemaString) as unknown;
    if (!isRecord(schema) || !Array.isArray(schema.fields)) return undefined;
    return schema.fields.filter(isRecord).map((field) => {
      const name = stringField(field, 'name') ?? 'column';
      return {
        name,
        type: typeof field.type === 'string' ? field.type : 'unknown',
        part: partitionColumns.includes(name) || undefined,
      };
    });
  } catch {
    return undefined;
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
): Partial<Record<string, PartitionColumnType>> {
  if (!schemaString) {
    return Object.fromEntries(partitionColumns.map((name) => [name, 'string']));
  }

  try {
    const schema = JSON.parse(schemaString) as unknown;
    if (!isRecord(schema) || !Array.isArray(schema.fields)) return {};
    const fieldTypes = new Map<string, PartitionColumnType>();
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

function partitionType(value: unknown): PartitionColumnType {
  if (value === 'long' || value === 'integer' || value === 'short' || value === 'byte') {
    return 'int64';
  }
  if (value === 'boolean') return 'boolean';
  if (value === 'string' || value === undefined) return 'string';
  return 'unsupported';
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

async function openLocalDeltaRegistryDb(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(LOCAL_DELTA_DB_NAME, LOCAL_DELTA_DB_VERSION);
    request.onerror = () => reject(request.error ?? new Error('local Delta registry open failed'));
    request.onsuccess = () => resolve(request.result);
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(LOCAL_DELTA_STORE)) {
        db.createObjectStore(LOCAL_DELTA_STORE, { keyPath: 'id' });
      }
    };
  });
}

async function putLocalDeltaRegistryRecord(record: LocalDeltaRegistryRecord): Promise<void> {
  const db = await openLocalDeltaRegistryDb();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(LOCAL_DELTA_STORE, 'readwrite');
    tx.objectStore(LOCAL_DELTA_STORE).put(record);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error ?? new Error('local Delta registry write failed'));
    tx.onabort = () => reject(tx.error ?? new Error('local Delta registry write aborted'));
  });
}

async function getLocalDeltaRegistryRecords(): Promise<LocalDeltaRegistryRecord[]> {
  const db = await openLocalDeltaRegistryDb();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(LOCAL_DELTA_STORE, 'readonly');
    const request = tx.objectStore(LOCAL_DELTA_STORE).getAll();
    request.onerror = () => reject(request.error ?? new Error('local Delta registry read failed'));
    request.onsuccess = () => resolve(request.result as LocalDeltaRegistryRecord[]);
  });
}

async function deleteLocalDeltaRegistryRecord(registryId: string): Promise<void> {
  const db = await openLocalDeltaRegistryDb();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(LOCAL_DELTA_STORE, 'readwrite');
    tx.objectStore(LOCAL_DELTA_STORE).delete(registryId);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error ?? new Error('local Delta registry delete failed'));
    tx.onabort = () => reject(tx.error ?? new Error('local Delta registry delete aborted'));
  });
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

function trackLocalObjectUrl(registryId: string, url: string): string {
  const urls = localObjectUrlsByRegistryId.get(registryId) ?? new Set<string>();
  urls.add(url);
  localObjectUrlsByRegistryId.set(registryId, urls);
  return url;
}

function sanitizeSqlIdentifier(name: string): string {
  const sanitized = name
    .trim()
    .replace(/[^A-Za-z0-9_]+/g, '_')
    .replace(/^_+|_+$/g, '');
  return sanitized || 'local_delta_table';
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} bytes`;
  const units = ['KB', 'MB', 'GB', 'TB'];
  let value = bytes / 1024;
  let unit = units[0];
  for (const candidate of units.slice(1)) {
    if (value < 1024) break;
    value /= 1024;
    unit = candidate;
  }
  return `${value.toFixed(value < 10 ? 1 : 0)} ${unit}`;
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
