import init, {
  preflight_parquet_metadata_for_targets,
  resolve_delta_snapshot_from_manifest,
} from './wasm/axon_web_wasm';
import { sql } from '@codemirror/lang-sql';
import { basicSetup, EditorView } from 'codemirror';
import {
  AxonWorkerError,
  createAxonBrowserClient,
  redactUrlSecrets,
  type AxonBrowserClient,
  type AxonQueryResult,
  type BrowserHttpSnapshotDescriptor,
  type BrowserWorkerEventContext,
  type BrowserWorkerEventEnvelope,
  type BrowserWorkerResultPreview,
  type DeltaObjectStoreProvider,
  type FallbackReason,
  type ParquetInspectionSummary,
  type PartitionColumnType,
  type QueryError,
} from './axon-browser-sdk';
import { SERVER_QUERY_FALLBACK_ENABLED } from './services/server-fallback.ts';
import './styles.css';

type FixtureManifest = {
  name?: string;
  table_uri: string;
  expected_latest_version?: number;
  checkpoint_version?: number;
  generated_steps?: Array<{
    version: number;
    label: string;
    detail: string;
  }>;
  objects: Array<{
    relative_path: string;
    url_path: string;
    kind?: ObjectKind;
    size_bytes?: number;
    etag?: string;
  }>;
  data_files?: BrowserDataFileInventory[];
};

type BrowserDataFileInventory = {
  relative_path: string;
  url_path: string;
  size_bytes: number;
  partition_values: Record<string, string>;
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

type ActiveDataFile = ResolvedSnapshot['active_files'][number] & {
  url_path: string;
  absolute_url: string;
};

type DecimalString = string;

type ParquetPreflightTarget = {
  path: string;
  url: string;
  size_bytes: number;
  partition_values: Record<string, string | null>;
  stats?: string;
};

type ParquetPreflightFile = {
  path: string;
  url: string;
  size_bytes: DecimalString;
  partition_values: Record<string, string | null>;
  delta_stats?: string;
  footer_length_bytes: DecimalString;
  row_group_count: DecimalString;
  row_count: DecimalString;
  fields: ParquetPreflightField[];
  field_stats: Record<string, ParquetPreflightFieldStats>;
};

type ParquetPreflightField = {
  name: string;
  physical_type: string;
  logical_type?: string;
  converted_type?: string;
  repetition: string;
  nullable: boolean;
};

type ParquetPreflightFieldStats = {
  min_i64?: DecimalString;
  max_i64?: DecimalString;
  null_count?: DecimalString;
};

type PruningPreflightFile = {
  path: string;
  status: 'touch' | 'skip';
  detail: string;
};

type PruningPreflightResult = {
  filter: string;
  files_touched: number;
  files_skipped: number;
  files: PruningPreflightFile[];
};

type ObjectKind = 'commit_json' | 'checkpoint_parquet' | 'last_checkpoint' | 'delta_log_object';

type LogObjectDetail = FixtureManifest['objects'][number] & {
  kind: ObjectKind;
  text?: string;
  actions: ParsedAction[];
};

type ParsedAction = {
  version: number | null;
  kind: string;
  path?: string;
  stats?: string;
  source: string;
};

type FixtureChoice = {
  manifestUrl: string;
  fallbackName?: string;
};

type SourceMode = 'fixture' | 'object_store' | 'local_files';

type QueryContext = {
  key: string;
  fixture: FixtureManifest;
  logObjectDetails: LogObjectDetail[];
  snapshot: ResolvedSnapshot;
  activeFiles: ActiveDataFile[];
};

type BrowserObjectStoreSource = {
  provider: DeltaObjectStoreProvider;
  tableUri: string;
  snapshotVersion?: number;
};

type LocalDeltaFileEntry = {
  file: File;
  browserPath: string;
  relativePath: string;
};

type LocalDeltaTableFiles = {
  registryId?: string;
  tableRootName: string;
  filesByRelativePath: Map<string, LocalDeltaFileEntry>;
  logEntries: LocalDeltaFileEntry[];
  dataEntries: LocalDeltaFileEntry[];
};

type LocalDeltaRegistryBackend = 'opfs' | 'indexeddb_blob';

type LocalDeltaRegistryFileRecord = {
  relativePath: string;
  sizeBytes: number;
  lastModified?: number;
  mimeType?: string;
  bytes?: ArrayBuffer;
};

type LocalDeltaRegistryRecord = {
  id: string;
  tableRootName: string;
  importedAtEpochMs: number;
  backend: LocalDeltaRegistryBackend;
  files: LocalDeltaRegistryFileRecord[];
};

const FIXTURES: Record<string, FixtureChoice> = {
  simple: {
    manifestUrl: '/fixtures/delta-log-manifest.json',
    fallbackName: 'Delta JSON log smoke fixture',
  },
  'prod-like': {
    manifestUrl: '/fixtures/prod-like/delta-log-manifest.json',
  },
};

const DEFAULT_FIXTURE_KEY = 'prod-like';
const QUERY_TABLE_NAME = 'axon_table';
const LOCAL_DELTA_DB_NAME = 'axon-local-delta-registry';
const LOCAL_DELTA_DB_VERSION = 1;
const LOCAL_DELTA_STORE = 'tables';
const LOCAL_DELTA_OPFS_ROOT = 'axon-local-delta-tables';
const SAMPLE_QUERIES = {
  count: 'SELECT COUNT(*) AS row_count FROM axon_table',
  category:
    'SELECT category, COUNT(*) AS rows, SUM(value) AS total_value FROM axon_table GROUP BY category ORDER BY category',
  'top-values':
    'SELECT id, category, value FROM axon_table WHERE value >= 90 ORDER BY value DESC LIMIT 20',
} as const;

const dataSourceSelect = requiredNode('#fixture-selector') as HTMLSelectElement;
const runSqlButton = requiredNode('#run-sql') as HTMLButtonElement;
const cancelSqlButton = requiredNode('#cancel-sql') as HTMLButtonElement;
const sqlEditorHost = requiredNode('#sql-editor');
const statusPill = document.querySelector<HTMLElement>('.status-pill');
const statusNode = requiredNode('[data-testid="status"]');
const fixtureNameNode = requiredNode('[data-testid="fixture-name"]');
const tableUriNode = requiredNode('[data-testid="table-uri"]');
const snapshotVersionNode = requiredNode('[data-testid="snapshot-version"]');
const checkpointVersionNode = requiredNode('[data-testid="checkpoint-version"]');
const fileCountNode = requiredNode('[data-testid="file-count"]');
const queryStatusNode = requiredNode('[data-testid="query-status"]');
const queryElapsedMsNode = requiredNode('[data-testid="query-elapsed-ms"]');
const queryExecutedOnNode = requiredNode('[data-testid="query-executed-on"]');
const queryFallbackReasonNode = requiredNode('[data-testid="query-fallback-reason"]');
const queryFallbackSummaryNode = document.querySelector<HTMLElement>(
  '[data-server-fallback-summary]',
);
const queryArrowIpcBytesNode = requiredNode('[data-testid="query-arrow-ipc-bytes"]');
const queryRowCountNode = requiredNode('[data-testid="query-row-count"]');
const queryMetricsNode = requiredNode('[data-testid="query-metrics"]');
const resultGridNode = requiredNode('[data-testid="result-grid"]');
const workerEventLogNode = requiredNode('[data-testid="worker-event-log"]');
const queryErrorNode = requiredNode('[data-testid="query-error"]');
const logObjectsNode = requiredNode('[data-testid="log-objects"]');
const commitActionsNode = requiredNode('[data-testid="commit-actions"]');
const dataFilesNode = requiredNode('[data-testid="data-files"]');
const activeFilesNode = requiredNode('[data-testid="active-files"]');
const activeDataFileUrlsNode = requiredNode('[data-testid="active-data-file-urls"]');
const browserSnapshotProviderNode = requiredNode('[data-testid="browser-snapshot-provider"]');
const browserSnapshotModeNode = requiredNode('[data-testid="browser-snapshot-mode"]');
const browserSnapshotUriNode = requiredNode('[data-testid="browser-snapshot-uri"]');

if (!SERVER_QUERY_FALLBACK_ENABLED) {
  queryFallbackSummaryNode?.remove();
}
const pruningPreflightNode = requiredNode('[data-testid="pruning-preflight"]');
const parquetPreflightNode = requiredNode('[data-testid="parquet-preflight"]');
const inputOutputMapNode = requiredNode('[data-testid="input-output-map"]');
const errorPanel = requiredNode('.error-banner') as HTMLElement;
const errorNode = requiredNode('[data-testid="error"]');
const objectStoreControls = requiredNode('[data-testid="object-store-controls"]') as HTMLElement;
const objectStoreProviderSelect = requiredNode('#object-store-provider') as HTMLSelectElement;
const objectStoreTableUriInput = requiredNode('#object-store-table-uri') as HTMLInputElement;
const objectStoreSnapshotInput = requiredNode('#object-store-snapshot') as HTMLInputElement;
const localTableControls = requiredNode('[data-testid="local-table-controls"]') as HTMLElement;
const localTableFilesInput = requiredNode('#local-table-files') as HTMLInputElement;

let wasmReady: Promise<void> | undefined;
let queryContext: QueryContext | undefined;
let queryClient: AxonBrowserClient | undefined;
let queryDescriptor: BrowserHttpSnapshotDescriptor | undefined;
let queryTableOpened = false;
let parquetPreflightContextKey: string | undefined;
let queryRequestCounter = 0;
let activeQuery: { token: number; requestId: string } | undefined;
let activeWorkerEventRequestIds: Set<string> | undefined;
let queryTokenCounter = 0;
let localObjectUrls = new Set<string>();
let lastLocalFileSelectionSignature: string | undefined;

dataSourceSelect.addEventListener('change', () => {
  resetResolvedContext();
  syncSourceControls();
  fixtureNameNode.textContent = selectedSourceLabel();
});

objectStoreProviderSelect.addEventListener('change', () => {
  syncObjectStoreDefaults();
  resetResolvedContext();
});

[objectStoreTableUriInput, objectStoreSnapshotInput].forEach((control) => {
  control.addEventListener('change', () => {
    resetResolvedContext();
  });
});

localTableFilesInput.addEventListener('change', () => {
  const signature = localFileSelectionSignature(localTableFilesInput.files);
  if (activeQuery || signature === lastLocalFileSelectionSignature) {
    return;
  }
  lastLocalFileSelectionSignature = signature;
  resetResolvedContext();
  fixtureNameNode.textContent = selectedSourceLabel();
});

runSqlButton.addEventListener('click', () => {
  runSql().catch((error: unknown) => {
    renderQueryError(error, elapsedLabel(0));
  });
});

cancelSqlButton.addEventListener('click', () => {
  cancelActiveQuery();
});

document.querySelectorAll<HTMLButtonElement>('[data-sample-query]').forEach((button) => {
  button.addEventListener('click', () => {
    const queryName = button.dataset.sampleQuery as keyof typeof SAMPLE_QUERIES;
    setSqlEditorValue(SAMPLE_QUERIES[queryName] ?? SAMPLE_QUERIES.count);
  });
});

initResultsTabs();
syncObjectStoreDefaults();
syncSourceControls();

const sqlEditor = createSqlEditor(SAMPLE_QUERIES.count);
attachEditorShortcuts(sqlEditor);
clearQueryOutput();

async function loadQueryContext(key: string, fixtureChoice: FixtureChoice): Promise<QueryContext> {
  setStatus('Resolving snapshot');
  errorPanel.hidden = true;
  clearDetails();
  await ensureWasm();

  const fixture = await fetchJson<FixtureManifest>(fixtureChoice.manifestUrl);
  const logObjectDetails = await loadLogObjectDetails(fixture);
  const wasmManifest = {
    objects: fixture.objects.map((object) => ({
      relative_path: object.relative_path,
      url: new URL(object.url_path, window.location.href).toString(),
      size_bytes: object.size_bytes,
      etag: object.etag,
    })),
  };
  const snapshotJson = await resolve_delta_snapshot_from_manifest(
    JSON.stringify(wasmManifest),
    fixture.table_uri,
  );
  const snapshot = JSON.parse(snapshotJson) as ResolvedSnapshot;
  const activeFiles = fixture.data_files === undefined ? [] : activeDataFiles(fixture, snapshot);
  const context = {
    key,
    fixture,
    logObjectDetails,
    snapshot,
    activeFiles,
  };
  renderSnapshot(fixture, logObjectDetails, snapshot, activeFiles, [], fixtureChoice.fallbackName);
  prepareQuerySandbox(snapshot, activeFiles);
  return context;
}

async function loadObjectStoreQueryContext(key: string): Promise<QueryContext> {
  setStatus('Resolving snapshot');
  errorPanel.hidden = true;
  clearDetails();
  await ensureWasm();

  const source = browserObjectStoreSource();
  const fixture = await fetchJson<FixtureManifest>(FIXTURES['prod-like'].manifestUrl);
  const logObjectDetails = await loadLogObjectDetails(fixture);
  const wasmManifest = {
    objects: fixture.objects.map((object) => ({
      relative_path: object.relative_path,
      url: new URL(object.url_path, window.location.href).toString(),
      size_bytes: object.size_bytes,
      etag: object.etag,
    })),
  };
  const snapshotJson = await resolve_delta_snapshot_from_manifest(
    JSON.stringify(wasmManifest),
    source.tableUri,
  );
  const snapshot = JSON.parse(snapshotJson) as ResolvedSnapshot;
  if (
    source.snapshotVersion !== undefined &&
    source.snapshotVersion !== snapshot.snapshot_version
  ) {
    throw new Error(`browser-local fixture snapshot is ${snapshot.snapshot_version}`);
  }
  const activeFiles = activeDataFiles(fixture, snapshot);
  const context = {
    key,
    fixture,
    logObjectDetails,
    snapshot,
    activeFiles,
  };
  renderSnapshot(fixture, logObjectDetails, snapshot, activeFiles, [], fixture.name);
  renderBrowserSnapshotSource(source.provider, source.tableUri);
  prepareQuerySandbox(snapshot, activeFiles);
  return context;
}

async function loadLocalFilesQueryContext(key: string): Promise<QueryContext> {
  setStatus('Resolving local snapshot');
  errorPanel.hidden = true;
  clearDetails();
  await ensureWasm();
  releaseLocalObjectUrls();

  const localTable = await localDeltaTableForCurrentSelection();
  const tableUri = localTableUri(localTable.tableRootName);
  const checkpointVersion = await localCheckpointVersion(localTable);
  const fixture: FixtureManifest = {
    name: 'Local Delta table',
    table_uri: tableUri,
    checkpoint_version: checkpointVersion,
    objects: localTable.logEntries.map((entry) => ({
      relative_path: entry.relativePath,
      url_path: trackLocalObjectUrl(URL.createObjectURL(entry.file)),
      kind: classifyObject(entry.relativePath),
      size_bytes: entry.file.size,
    })),
    data_files: localTable.dataEntries.map((entry) => ({
      relative_path: entry.relativePath,
      url_path: `local:${entry.relativePath}`,
      size_bytes: entry.file.size,
      partition_values: partitionValuesFromPath(entry.relativePath),
    })),
  };
  const logObjectDetails = await loadLogObjectDetails(fixture);
  const wasmManifest = {
    objects: fixture.objects.map((object) => ({
      relative_path: object.relative_path,
      url: object.url_path,
      size_bytes: object.size_bytes,
      etag: object.etag,
    })),
  };
  const snapshotJson = await resolve_delta_snapshot_from_manifest(
    JSON.stringify(wasmManifest),
    tableUri,
  );
  const snapshot = JSON.parse(snapshotJson) as ResolvedSnapshot;
  const activeFiles = localActiveDataFiles(localTable, snapshot);
  const context = {
    key,
    fixture,
    logObjectDetails,
    snapshot,
    activeFiles,
  };
  renderSnapshot(fixture, logObjectDetails, snapshot, activeFiles, [], fixture.name);
  renderBrowserSnapshotSource('local_files', tableUri);
  prepareQuerySandbox(snapshot, activeFiles);
  return context;
}

function renderSnapshot(
  fixture: FixtureManifest,
  logObjectDetails: LogObjectDetail[],
  snapshot: ResolvedSnapshot,
  activeFiles: ActiveDataFile[],
  parquetPreflight: ParquetPreflightFile[],
  fallbackName?: string,
): void {
  setStatus(snapshotStatus(fixture, snapshot));
  fixtureNameNode.textContent = fixture.name ?? fallbackName ?? 'Delta fixture';
  tableUriNode.textContent = snapshot.table_uri;
  snapshotVersionNode.textContent = String(snapshot.snapshot_version);
  checkpointVersionNode.textContent =
    fixture.checkpoint_version === undefined ? '-' : String(fixture.checkpoint_version);
  fileCountNode.textContent = `${snapshot.active_files.length} file${
    snapshot.active_files.length === 1 ? '' : 's'
  }`;
  renderLogObjects(logObjectDetails);
  renderCommitActions(logObjectDetails);
  renderDataFiles(fixture, snapshot);
  renderActiveDataFileUrls(activeFiles);
  renderPruningPreflight(planDeltaStatsPruning(activeFiles));
  renderParquetPreflight(parquetPreflight);
  activeFilesNode.replaceChildren(
    ...snapshot.active_files.map((file) => {
      const item = document.createElement('li');
      item.className = 'file-row active';
      const partitions = Object.entries(file.partition_values)
        .map(([key, value]) => `${key}=${value ?? 'null'}`)
        .join(', ');
      item.append(
        textBlock('path', file.path),
        textBlock('meta', `${formatBytes(file.size_bytes)}${partitions ? `, ${partitions}` : ''}`),
      );
      if (file.stats) {
        item.append(textBlock('stats', `stats ${formatStats(file.stats)}`));
      }
      return item;
    }),
  );
  renderInputOutputMap(fixture, logObjectDetails, snapshot);
}

function activeDataFiles(fixture: FixtureManifest, snapshot: ResolvedSnapshot): ActiveDataFile[] {
  const dataFilesByPath = new Map(
    (fixture.data_files ?? []).map((file) => [file.relative_path, file]),
  );

  return snapshot.active_files.map((file) => {
    const manifestFile = dataFilesByPath.get(file.path);
    if (!manifestFile) {
      throw new Error(`active file ${file.path} was missing from fixture data_files`);
    }

    return {
      ...file,
      url_path: manifestFile.url_path,
      absolute_url: new URL(manifestFile.url_path, window.location.href).toString(),
    };
  });
}

function localActiveDataFiles(
  localTable: LocalDeltaTableFiles,
  snapshot: ResolvedSnapshot,
): ActiveDataFile[] {
  return snapshot.active_files.map((file) => {
    const entry = localFileForDeltaPath(localTable, file.path);
    if (!entry) {
      throw new Error(`active file ${file.path} was missing from the selected local folder`);
    }
    if (entry.file.size !== file.size_bytes) {
      throw new Error(
        `active file ${file.path} size ${entry.file.size} did not match Delta log size ${file.size_bytes}`,
      );
    }

    return {
      ...file,
      url_path: `local:${entry.relativePath}`,
      absolute_url: trackLocalObjectUrl(URL.createObjectURL(entry.file)),
    };
  });
}

async function localDeltaTableForCurrentSelection(): Promise<LocalDeltaTableFiles> {
  const selectedFiles = Array.from(localTableFilesInput.files ?? []);
  if (selectedFiles.length > 0) {
    lastLocalFileSelectionSignature = localFileSelectionSignature(selectedFiles);
    const table = collectLocalDeltaTableFiles(selectedFiles);
    try {
      return await persistLocalDeltaTable(table);
    } catch (error) {
      console.warn('local Delta table import could not be persisted:', error);
      return table;
    }
  }

  const persisted = await loadLatestLocalDeltaTable();
  if (persisted) {
    return persisted;
  }

  throw new Error('select a local Delta table directory first');
}

function collectLocalDeltaTableFiles(files: FileList | File[] | null): LocalDeltaTableFiles {
  const selectedFiles = Array.from(files ?? []);
  if (selectedFiles.length === 0) {
    throw new Error('select a local Delta table directory first');
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

function localFileSelectionSignature(files: FileList | File[] | null): string {
  return Array.from(files ?? [])
    .map(
      (file) =>
        `${normalizeBrowserFilePath(fileBrowserPath(file))}:${file.size}:${file.lastModified}`,
    )
    .sort()
    .join('|');
}

function buildLocalDeltaTableFiles(
  tableRootName: string,
  entries: LocalDeltaFileEntry[],
  registryId?: string,
): LocalDeltaTableFiles {
  const filesByRelativePath = new Map<string, LocalDeltaFileEntry>();
  for (const entry of entries) {
    validateLocalRelativePath(entry.relativePath);
    if (filesByRelativePath.has(entry.relativePath)) {
      throw new Error(`selected folder contained duplicate path ${entry.relativePath}`);
    }
    filesByRelativePath.set(entry.relativePath, entry);
  }

  const logEntries = entries
    .filter((entry) => entry.relativePath.startsWith('_delta_log/'))
    .sort(compareLocalDeltaEntries);
  if (logEntries.length === 0) {
    throw new Error('selected folder does not contain a _delta_log directory');
  }

  return {
    registryId,
    tableRootName,
    filesByRelativePath,
    logEntries,
    dataEntries: entries
      .filter((entry) => isParquetDataFile(entry.relativePath))
      .sort(compareLocalDeltaEntries),
  };
}

async function persistLocalDeltaTable(table: LocalDeltaTableFiles): Promise<LocalDeltaTableFiles> {
  const id = table.registryId ?? localDeltaRegistryId(table.tableRootName);
  const recordFiles = await Promise.all(
    [...table.filesByRelativePath.values()].map(async (entry) => ({
      relativePath: entry.relativePath,
      sizeBytes: entry.file.size,
      lastModified: entry.file.lastModified,
      mimeType: entry.file.type,
      bytes: await entry.file.arrayBuffer(),
    })),
  );
  const baseRecord = {
    id,
    tableRootName: table.tableRootName,
    importedAtEpochMs: Date.now(),
    files: recordFiles,
  };

  if (await tryWriteLocalDeltaTableToOpfs(id, table)) {
    await putLocalDeltaRegistryRecord({ ...baseRecord, backend: 'opfs' });
    return { ...table, registryId: id };
  }

  await putLocalDeltaRegistryRecord({
    ...baseRecord,
    backend: 'indexeddb_blob',
  });
  return { ...table, registryId: id };
}

async function tryWriteLocalDeltaTableToOpfs(
  id: string,
  table: LocalDeltaTableFiles,
): Promise<boolean> {
  const root = await opfsLocalDeltaRoot();
  if (!root) {
    return false;
  }

  try {
    const tableDirectory = await root.getDirectoryHandle(id, { create: true });
    await Promise.all(
      [...table.filesByRelativePath.values()].map((entry) =>
        writeOpfsFile(tableDirectory, entry.relativePath, entry.file),
      ),
    );
    return true;
  } catch (error) {
    console.warn('OPFS local Delta table import failed; using IndexedDB blob registry:', error);
    try {
      await root.removeEntry(id, { recursive: true });
    } catch {
      // Best-effort cleanup only; stale partial imports are ignored by the registry.
    }
    return false;
  }
}

async function loadLatestLocalDeltaTable(): Promise<LocalDeltaTableFiles | undefined> {
  const records = await getLocalDeltaRegistryRecords();
  const latest = records.sort((a, b) => b.importedAtEpochMs - a.importedAtEpochMs)[0];
  if (!latest) {
    return undefined;
  }

  if (latest.backend === 'opfs') {
    const table = await loadOpfsLocalDeltaTable(latest);
    if (table) {
      return table;
    }
  }

  return loadIndexedDbBlobLocalDeltaTable(latest);
}

async function loadOpfsLocalDeltaTable(
  record: LocalDeltaRegistryRecord,
): Promise<LocalDeltaTableFiles | undefined> {
  const root = await opfsLocalDeltaRoot();
  if (!root) {
    return undefined;
  }

  try {
    const tableDirectory = await root.getDirectoryHandle(record.id);
    const entries = await Promise.all(
      record.files.map(async (fileRecord) => {
        const fileHandle = await getOpfsFileHandle(tableDirectory, fileRecord.relativePath);
        const file = await fileHandle.getFile();
        if (file.size !== fileRecord.sizeBytes) {
          throw new Error(
            `persisted local file ${fileRecord.relativePath} size ${file.size} did not match registry size ${fileRecord.sizeBytes}`,
          );
        }
        return localDeltaFileEntry(record.tableRootName, fileRecord.relativePath, file);
      }),
    );
    return buildLocalDeltaTableFiles(record.tableRootName, entries, record.id);
  } catch (error) {
    console.warn('persisted OPFS local Delta table could not be reopened:', error);
    return undefined;
  }
}

function loadIndexedDbBlobLocalDeltaTable(record: LocalDeltaRegistryRecord): LocalDeltaTableFiles {
  const entries = record.files.map((fileRecord) => {
    if (!fileRecord.bytes) {
      throw new Error(`persisted local file ${fileRecord.relativePath} was missing stored bytes`);
    }
    const file = new File([fileRecord.bytes], fileRecord.relativePath.split('/').at(-1) ?? 'file', {
      lastModified: fileRecord.lastModified,
      type: fileRecord.mimeType,
    });
    if (file.size !== fileRecord.sizeBytes) {
      throw new Error(
        `persisted local file ${fileRecord.relativePath} size ${file.size} did not match registry size ${fileRecord.sizeBytes}`,
      );
    }
    return localDeltaFileEntry(record.tableRootName, fileRecord.relativePath, file);
  });
  return buildLocalDeltaTableFiles(record.tableRootName, entries, record.id);
}

async function localCheckpointVersion(
  localTable: LocalDeltaTableFiles,
): Promise<number | undefined> {
  const hint = localTable.filesByRelativePath.get('_delta_log/_last_checkpoint');
  if (!hint) {
    return undefined;
  }

  try {
    const parsed = JSON.parse(await hint.file.text()) as { version?: unknown };
    return typeof parsed.version === 'number' ? parsed.version : undefined;
  } catch {
    return undefined;
  }
}

function localDeltaFileEntry(
  tableRootName: string,
  relativePath: string,
  file: File,
): LocalDeltaFileEntry {
  return {
    file,
    browserPath: `${tableRootName}/${relativePath}`,
    relativePath,
  };
}

function localDeltaRegistryId(tableRootName: string): string {
  const safeName = tableRootName.replace(/[^A-Za-z0-9._-]+/g, '-').replace(/^-+|-+$/g, '');
  return `${safeName || 'delta-table'}-${Date.now().toString(36)}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;
}

function validateLocalRelativePath(path: string): void {
  if (
    path.length === 0 ||
    path.startsWith('/') ||
    path.startsWith('\\') ||
    path.includes('\0') ||
    path.split('/').some((segment) => segment.length === 0 || segment === '.' || segment === '..')
  ) {
    throw new Error(`local Delta table path ${path} must stay inside the selected table root`);
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

async function opfsLocalDeltaRoot(): Promise<FileSystemDirectoryHandle | undefined> {
  try {
    const storage = navigator.storage;
    if (!storage || typeof storage.getDirectory !== 'function') {
      return undefined;
    }
    const root = await storage.getDirectory();
    return root.getDirectoryHandle(LOCAL_DELTA_OPFS_ROOT, { create: true });
  } catch {
    return undefined;
  }
}

async function writeOpfsFile(
  root: FileSystemDirectoryHandle,
  relativePath: string,
  file: File,
): Promise<void> {
  const fileHandle = await getOrCreateOpfsFileHandle(root, relativePath);
  const writable = await fileHandle.createWritable();
  try {
    await writable.write(file);
  } finally {
    await writable.close();
  }
}

async function getOrCreateOpfsFileHandle(
  root: FileSystemDirectoryHandle,
  relativePath: string,
): Promise<FileSystemFileHandle> {
  const segments = validateOpfsPathSegments(relativePath);
  const filename = segments.at(-1);
  if (!filename) {
    throw new Error(`local Delta table path ${relativePath} did not include a filename`);
  }

  let directory = root;
  for (const segment of segments.slice(0, -1)) {
    directory = await directory.getDirectoryHandle(segment, { create: true });
  }
  return directory.getFileHandle(filename, { create: true });
}

async function getOpfsFileHandle(
  root: FileSystemDirectoryHandle,
  relativePath: string,
): Promise<FileSystemFileHandle> {
  const segments = validateOpfsPathSegments(relativePath);
  const filename = segments.at(-1);
  if (!filename) {
    throw new Error(`local Delta table path ${relativePath} did not include a filename`);
  }

  let directory = root;
  for (const segment of segments.slice(0, -1)) {
    directory = await directory.getDirectoryHandle(segment);
  }
  return directory.getFileHandle(filename);
}

function validateOpfsPathSegments(relativePath: string): string[] {
  validateLocalRelativePath(relativePath);
  return relativePath.split('/').map((segment) => decodeDeltaPath(segment));
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
    if (deltaLogIndex >= 0) {
      prefixes.add(segments.slice(0, deltaLogIndex).join('/'));
    }
  }

  if (prefixes.size === 0) {
    throw new Error('select the Delta table directory containing _delta_log');
  }
  if (prefixes.size > 1) {
    throw new Error('selected files contain multiple Delta table roots');
  }
  return [...prefixes][0];
}

function tableRelativePath(browserPath: string, rootPrefix: string): string | undefined {
  if (!rootPrefix) {
    return browserPath;
  }
  if (browserPath === rootPrefix) {
    return '';
  }
  const prefix = `${rootPrefix}/`;
  if (!browserPath.startsWith(prefix)) {
    return undefined;
  }
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

function partitionValuesFromPath(path: string): Record<string, string> {
  const partitions: Record<string, string> = {};
  const segments = path.split('/').slice(0, -1);
  for (const segment of segments) {
    const [key, value] = segment.split('=', 2);
    if (key && value !== undefined) {
      partitions[decodeDeltaPath(key)] = decodeDeltaPath(value);
    }
  }
  return partitions;
}

function trackLocalObjectUrl(url: string): string {
  localObjectUrls.add(url);
  return url;
}

function prepareQuerySandbox(snapshot: ResolvedSnapshot, activeFiles: ActiveDataFile[]): void {
  resetQuerySession();
  queryDescriptor = browserSnapshotDescriptor(snapshot, activeFiles);
}

function browserSnapshotDescriptor(
  snapshot: ResolvedSnapshot,
  activeFiles: ActiveDataFile[],
): BrowserHttpSnapshotDescriptor {
  return {
    table_uri: snapshot.table_uri,
    snapshot_version: snapshot.snapshot_version,
    partition_column_types: snapshot.partition_column_types ?? {},
    browser_compatibility: { capabilities: {} },
    required_capabilities: { capabilities: {} },
    active_files: activeFiles.map((file) => ({
      path: file.path,
      url: file.absolute_url,
      size_bytes: file.size_bytes,
      partition_values: file.partition_values,
      stats: file.stats,
    })),
  };
}

async function runSql(): Promise<void> {
  const token = ++queryTokenCounter;
  const requestId = `sandbox-query-${++queryRequestCounter}`;
  activeQuery = { token, requestId };
  activeWorkerEventRequestIds = new Set([requestId]);
  runSqlButton.disabled = true;
  cancelSqlButton.disabled = false;
  setQueryStatus('Loading table');
  queryElapsedMsNode.textContent = '-';
  queryExecutedOnNode.textContent = '-';
  clearFallbackReason();
  queryArrowIpcBytesNode.textContent = '-';
  queryRowCountNode.textContent = '-';
  queryMetricsNode.textContent = '-';
  queryErrorNode.textContent = '-';
  resultGridNode.replaceChildren();
  workerEventLogNode.replaceChildren();
  const startedAt = performance.now();

  try {
    const context = await ensureQueryContext();
    if (!isActiveQuery(token)) {
      return;
    }
    if (context.activeFiles.length === 0) {
      throw new Error('selected data source does not include browser-readable data files');
    }

    const client = ensureQueryClient();
    setQueryStatus(queryTableOpened ? 'Running' : 'Opening table');
    await ensureQueryTableOpen(client, token);
    if (!isActiveQuery(token)) {
      return;
    }

    setQueryStatus('Running');
    const result = await client.query(QUERY_TABLE_NAME, sqlEditor.state.doc.toString(), {
      requestId,
      queryOptions: {
        collect_metrics: true,
        include_explain: false,
      },
    });
    if (!isActiveQuery(token)) {
      return;
    }

    renderQueryResult(result, elapsedLabel(performance.now() - startedAt));
    refreshParquetPreflight(context).catch((error: unknown) => {
      renderError(error);
    });
  } catch (error) {
    if (!isActiveQuery(token)) {
      return;
    }
    renderQueryError(error, elapsedLabel(performance.now() - startedAt));
  } finally {
    if (isActiveQuery(token)) {
      activeQuery = undefined;
      activeWorkerEventRequestIds = undefined;
      runSqlButton.disabled = false;
      cancelSqlButton.disabled = true;
    }
  }
}

async function ensureQueryContext(): Promise<QueryContext> {
  const key = selectedFixtureKey();
  if (queryContext?.key === key && queryDescriptor) {
    return queryContext;
  }

  const mode = selectedSourceMode();
  const context =
    mode === 'object_store'
      ? await loadObjectStoreQueryContext(key)
      : mode === 'local_files'
        ? await loadLocalFilesQueryContext(key)
        : await loadQueryContext(key, selectedFixture());
  queryContext = context;
  return context;
}

async function refreshParquetPreflight(context: QueryContext): Promise<void> {
  if (parquetPreflightContextKey === context.key || context.activeFiles.length === 0) {
    return;
  }

  const parquetPreflight = await preflightActiveParquetFiles(context.activeFiles);
  if (queryContext?.key !== context.key) {
    return;
  }

  parquetPreflightContextKey = context.key;
  renderParquetPreflight(parquetPreflight);

  const parquetInspection = queryTableOpened
    ? await inspectActiveParquetFiles(context.activeFiles)
    : new Map<string, ParquetInspectionSummary>();
  if (queryContext?.key !== context.key) {
    return;
  }

  renderParquetPreflight(parquetPreflight, parquetInspection);
}

function ensureQueryClient(): AxonBrowserClient {
  queryClient ??= createAxonBrowserClient({
    worker: () =>
      new Worker(new URL('./sandbox-query-worker.ts', import.meta.url), {
        type: 'module',
        name: 'axon-sandbox-query-worker',
      }),
    requestId: () => `sandbox-request-${++queryRequestCounter}`,
    onEvent: renderWorkerEvent,
  });

  return queryClient;
}

async function ensureQueryTableOpen(client: AxonBrowserClient, token: number): Promise<void> {
  if (queryTableOpened) {
    return;
  }
  if (!queryDescriptor) {
    throw new Error('no query descriptor has been resolved');
  }

  const requestId = `sandbox-open-${++queryRequestCounter}`;
  if (isActiveQuery(token)) {
    activeWorkerEventRequestIds?.add(requestId);
  }
  await client.openDeltaTable(QUERY_TABLE_NAME, queryDescriptor, { requestId });
  queryTableOpened = true;
}

function cancelActiveQuery(): void {
  const active = activeQuery;
  if (!active) {
    return;
  }

  activeQuery = undefined;
  runSqlButton.disabled = false;
  cancelSqlButton.disabled = true;
  setQueryStatus('Cancellation requested');
  const cancellationError: QueryError = SERVER_QUERY_FALLBACK_ENABLED
    ? {
        code: 'fallback_required',
        message:
          'UI cancellation requested; true worker abort is not available, so this request was superseded locally.',
        target: 'browser_wasm',
        fallback_reason: 'browser_runtime_constraint',
      }
    : {
        code: 'execution_failed',
        message:
          'UI cancellation requested; true worker abort is not available, so this request was superseded locally.',
        target: 'browser_wasm',
      };
  renderFallbackReason(cancellationError.fallback_reason);
  appendWorkerEvent({
    cancellation: {
      context: {
        phase: 'query',
        request_id: active.requestId,
        query_id: active.requestId,
        table_name: QUERY_TABLE_NAME,
      },
      error: cancellationError,
    },
  });
  activeWorkerEventRequestIds = undefined;
  closeQueryClient();
  queryErrorNode.textContent = JSON.stringify(cancellationError, null, 2);
}

function isActiveQuery(token: number): boolean {
  return activeQuery?.token === token;
}

function renderQueryResult(result: AxonQueryResult, elapsed: string): void {
  setQueryStatus('Finished');
  queryElapsedMsNode.textContent = elapsed;
  queryExecutedOnNode.textContent = result.response.executed_on;
  renderFallbackReason(result.response.fallback_reason);
  queryArrowIpcBytesNode.textContent = formatBytes(result.result.bytes.byteLength);
  queryRowCountNode.textContent = result.preview ? String(result.preview.row_count) : '-';
  queryMetricsNode.textContent = JSON.stringify(result.response.metrics, null, 2);
  queryErrorNode.textContent = '-';
  renderResultPreview(result.preview);
}

function renderQueryError(error: unknown, elapsed: string): void {
  const queryError = displayQueryError(errorToQueryError(error));
  setQueryStatus('Error');
  queryElapsedMsNode.textContent = elapsed;
  queryExecutedOnNode.textContent = queryError.target;
  renderFallbackReason(queryError.fallback_reason);
  queryErrorNode.textContent = JSON.stringify(queryError, null, 2);
  focusResultsTab('error');
}

function errorToQueryError(error: unknown): QueryError {
  if (error instanceof AxonWorkerError) {
    return redactQueryError(error.queryError);
  }
  if (isQueryError(error)) {
    return redactQueryError(error);
  }
  return {
    code: 'execution_failed',
    message: redactUrlSecrets(error instanceof Error ? error.message : String(error)),
    target: 'browser_wasm',
  };
}

function isQueryError(value: unknown): value is QueryError {
  if (!value || typeof value !== 'object') {
    return false;
  }
  const candidate = value as Partial<QueryError>;
  return (
    typeof candidate.code === 'string' &&
    typeof candidate.message === 'string' &&
    (candidate.target === 'browser_wasm' || candidate.target === 'native')
  );
}

function redactQueryError(error: QueryError): QueryError {
  return {
    ...error,
    message: redactUrlSecrets(error.message),
  };
}

function renderResultPreview(preview: BrowserWorkerResultPreview | undefined): void {
  if (!preview) {
    resultGridNode.textContent = 'No preview returned';
    return;
  }

  const table = document.createElement('table');
  const thead = document.createElement('thead');
  const headerRow = document.createElement('tr');
  for (const column of preview.columns) {
    const header = document.createElement('th');
    header.textContent = column;
    headerRow.append(header);
  }
  thead.append(headerRow);

  const tbody = document.createElement('tbody');
  for (const row of preview.rows) {
    const rowNode = document.createElement('tr');
    for (const value of row) {
      const cell = document.createElement('td');
      cell.textContent = value === null ? 'null' : String(value);
      rowNode.append(cell);
    }
    tbody.append(rowNode);
  }
  table.append(thead, tbody);

  const meta = document.createElement('p');
  meta.className = 'detail';
  meta.textContent = preview.truncated
    ? `Preview capped at ${preview.preview_row_limit} of ${preview.row_count} rows`
    : `Preview rows ${preview.row_count}`;

  resultGridNode.replaceChildren(table, meta);
}

function renderWorkerEvent(event: BrowserWorkerEventEnvelope): void {
  if (!shouldRenderWorkerEvent(event)) {
    return;
  }

  appendWorkerEvent(event);
}

function appendWorkerEvent(event: BrowserWorkerEventEnvelope): void {
  const item = document.createElement('li');
  item.textContent = workerEventLabel(event);
  workerEventLogNode.append(item);
}

function shouldRenderWorkerEvent(event: BrowserWorkerEventEnvelope): boolean {
  if (!SERVER_QUERY_FALLBACK_ENABLED && 'fallback' in event) {
    return false;
  }
  const requestId = workerEventContext(event).request_id;
  if (!requestId) {
    return activeWorkerEventRequestIds !== undefined;
  }

  return activeWorkerEventRequestIds?.has(requestId) ?? false;
}

function workerEventContext(event: BrowserWorkerEventEnvelope): BrowserWorkerEventContext {
  if ('progress' in event) {
    return event.progress.context;
  }
  if ('range_read_metrics' in event) {
    return event.range_read_metrics.context;
  }
  if ('cache_metrics' in event) {
    return event.cache_metrics.context;
  }
  if ('fallback' in event) {
    return event.fallback.context;
  }
  if ('cancellation' in event) {
    return event.cancellation.context;
  }
  if ('terminal_error' in event) {
    return event.terminal_error.context;
  }
  return event.log.context;
}

function workerEventLabel(event: BrowserWorkerEventEnvelope): string {
  if ('progress' in event) {
    return `${event.progress.context.phase} ${event.progress.stage}`;
  }
  if ('range_read_metrics' in event) {
    return `range_read_metrics files_touched ${event.range_read_metrics.files_touched}, bytes_fetched ${event.range_read_metrics.bytes_fetched}`;
  }
  if ('cache_metrics' in event) {
    return `cache_metrics session_tables ${event.cache_metrics.session_table_count}, cached ${formatBytes(event.cache_metrics.session_cached_bytes)}`;
  }
  if ('fallback' in event) {
    return `fallback ${fallbackReasonLabel(event.fallback.reason)}`;
  }
  if ('cancellation' in event) {
    return `cancellation ${event.cancellation.error.message}`;
  }
  if ('terminal_error' in event) {
    return `terminal_error ${event.terminal_error.error.code}: ${event.terminal_error.error.message}`;
  }
  return `${event.log.context.phase} ${event.log.level}: ${event.log.message}`;
}

function fallbackReasonLabel(reason: FallbackReason | undefined): string {
  if (reason === undefined) {
    return '-';
  }
  if (typeof reason === 'string') {
    return reason;
  }
  return `${reason.capability_gate.capability}:${reason.capability_gate.required_state}`;
}

function clearFallbackReason(): void {
  if (SERVER_QUERY_FALLBACK_ENABLED) {
    queryFallbackReasonNode.textContent = '-';
  }
}

function renderFallbackReason(reason: FallbackReason | undefined): void {
  if (SERVER_QUERY_FALLBACK_ENABLED) {
    queryFallbackReasonNode.textContent = fallbackReasonLabel(reason);
  }
}

function displayQueryError(error: QueryError): QueryError {
  if (SERVER_QUERY_FALLBACK_ENABLED || error.fallback_reason === undefined) {
    return error;
  }
  return {
    code: 'unsupported_feature',
    message: 'This browser build cannot execute that query locally.',
    target: error.target,
  };
}

function elapsedLabel(milliseconds: number): string {
  return `${Math.max(0, Math.round(milliseconds))} ms`;
}

function setQueryStatus(status: string): void {
  queryStatusNode.textContent = status;
  updateStatusPill(status);
}

function updateStatusPill(status: string): void {
  if (!statusPill) {
    return;
  }
  statusPill.dataset.state = statusPillState(status);
}

function statusPillState(status: string): 'ready' | 'running' | 'success' | 'error' {
  const lower = status.toLowerCase();
  if (lower === 'error' || lower.includes('cancel')) {
    return 'error';
  }
  if (lower === 'finished') {
    return 'success';
  }
  if (lower === 'ready') {
    return 'ready';
  }
  return 'running';
}

function initResultsTabs(): void {
  const tabs = Array.from(document.querySelectorAll<HTMLButtonElement>('.tab[data-tab]'));
  const panels = Array.from(document.querySelectorAll<HTMLElement>('.tab-panel[data-panel]'));
  if (tabs.length === 0 || panels.length === 0) {
    return;
  }
  const activate = (name: string): void => {
    for (const tab of tabs) {
      const selected = tab.dataset.tab === name;
      tab.setAttribute('aria-selected', selected ? 'true' : 'false');
    }
    for (const panel of panels) {
      panel.hidden = panel.dataset.panel !== name;
    }
  };
  for (const tab of tabs) {
    tab.addEventListener('click', () => {
      const target = tab.dataset.tab;
      if (target) {
        activate(target);
      }
    });
  }
}

function focusResultsTab(name: string): void {
  const tab = document.querySelector<HTMLButtonElement>(`.tab[data-tab="${name}"]`);
  if (tab) {
    tab.click();
  }
}

function attachEditorShortcuts(editor: EditorView): void {
  editor.dom.addEventListener('keydown', (event) => {
    if ((event.metaKey || event.ctrlKey) && event.key === 'Enter') {
      event.preventDefault();
      if (!runSqlButton.disabled) {
        runSqlButton.click();
      }
    }
  });
}

function clearQueryOutput(): void {
  setQueryStatus('Ready');
  queryElapsedMsNode.textContent = '-';
  queryExecutedOnNode.textContent = '-';
  clearFallbackReason();
  queryArrowIpcBytesNode.textContent = '-';
  queryRowCountNode.textContent = '-';
  queryMetricsNode.textContent = '-';
  queryErrorNode.textContent = '-';
  resultGridNode.replaceChildren();
  workerEventLogNode.replaceChildren();
  runSqlButton.disabled = false;
  cancelSqlButton.disabled = true;
}

function resetQueryClient(): void {
  activeQuery = undefined;
  activeWorkerEventRequestIds = undefined;
  resetQuerySession();
}

function resetResolvedContext(): void {
  resetQueryClient();
  releaseLocalObjectUrls();
  queryContext = undefined;
  parquetPreflightContextKey = undefined;
  clearDetails();
  clearQueryOutput();
  setStatus('Ready');
}

function resetQuerySession(): void {
  closeQueryClient();
  queryDescriptor = undefined;
}

function closeQueryClient(): void {
  queryTableOpened = false;
  if (queryClient) {
    queryClient.terminate();
    queryClient = undefined;
  }
}

function releaseLocalObjectUrls(): void {
  for (const url of localObjectUrls) {
    URL.revokeObjectURL(url);
  }
  localObjectUrls = new Set();
}

async function preflightActiveParquetFiles(
  activeFiles: ActiveDataFile[],
): Promise<ParquetPreflightFile[]> {
  if (activeFiles.length === 0) {
    return [];
  }

  const targets: ParquetPreflightTarget[] = activeFiles.map((file) => ({
    path: file.path,
    url: file.absolute_url,
    size_bytes: file.size_bytes,
    partition_values: file.partition_values,
    stats: file.stats,
  }));
  const preflightJson = await preflight_parquet_metadata_for_targets(JSON.stringify(targets));
  return JSON.parse(preflightJson) as ParquetPreflightFile[];
}

async function inspectActiveParquetFiles(
  activeFiles: ActiveDataFile[],
): Promise<Map<string, ParquetInspectionSummary>> {
  if (activeFiles.length === 0) {
    return new Map();
  }

  const client = ensureQueryClient();
  const inspections = await Promise.all(
    activeFiles.map(async (file) => {
      const summary = await client.inspectParquet(QUERY_TABLE_NAME, file.path, {
        requestId: `sandbox-inspect-${++queryRequestCounter}`,
      });
      return [file.path, summary] as const;
    }),
  );
  return new Map(inspections);
}

function snapshotStatus(fixture: FixtureManifest, snapshot: ResolvedSnapshot): string {
  if (fixture.checkpoint_version === undefined) {
    return `Snapshot ${snapshot.snapshot_version} resolved`;
  }
  const replayCommits = snapshot.snapshot_version - fixture.checkpoint_version;
  return `Snapshot ${snapshot.snapshot_version} resolved from checkpoint ${fixture.checkpoint_version} + ${replayCommits} replay commit${replayCommits === 1 ? '' : 's'}`;
}

async function loadLogObjectDetails(fixture: FixtureManifest): Promise<LogObjectDetail[]> {
  return Promise.all(
    fixture.objects.map(async (object) => {
      const kind = object.kind ?? classifyObject(object.relative_path);
      const detail: LogObjectDetail = { ...object, kind, actions: [] };
      if (kind === 'commit_json' || kind === 'last_checkpoint') {
        detail.text = await fetchText(object.url_path);
      }
      if (kind === 'commit_json' && detail.text) {
        detail.actions = parseCommitActions(object.relative_path, detail.text);
      }
      return detail;
    }),
  );
}

function renderLogObjects(objects: LogObjectDetail[]): void {
  logObjectsNode.replaceChildren(
    ...objects.map((object) => {
      const item = document.createElement('li');
      item.className = `object-row ${object.kind}`;
      item.append(
        textBlock('path', object.relative_path),
        textBlock('meta', `${kindLabel(object.kind)}, ${formatBytes(object.size_bytes ?? 0)}`),
      );
      if (object.kind === 'last_checkpoint' && object.text) {
        item.append(textBlock('detail', compactJson(object.text)));
      }
      return item;
    }),
  );
}

function renderCommitActions(objects: LogObjectDetail[]): void {
  const actions = objects.flatMap((object) => object.actions);
  commitActionsNode.replaceChildren(
    ...actions.map((action) => {
      const item = document.createElement('li');
      item.className = `action-row ${action.kind}`;
      item.append(
        textBlock(
          'badge',
          action.version === null ? action.kind : `v${action.version} ${action.kind}`,
        ),
      );
      if (action.path) {
        item.append(textBlock('path', action.path));
      }
      if (action.stats) {
        item.append(textBlock('stats', `stats ${formatStats(action.stats)}`));
      }
      return item;
    }),
  );
}

function renderDataFiles(fixture: FixtureManifest, snapshot: ResolvedSnapshot): void {
  const activePaths = new Set(snapshot.active_files.map((file) => file.path));
  dataFilesNode.replaceChildren(
    ...(fixture.data_files ?? []).map((file) => {
      const isActive = activePaths.has(file.relative_path);
      const item = document.createElement('li');
      item.className = `file-row ${isActive ? 'active' : 'inactive'}`;
      item.append(
        textBlock('status', isActive ? 'active' : 'inactive'),
        textBlock('path', file.relative_path),
        textBlock(
          'meta',
          `${formatBytes(file.size_bytes)}, ${formatPartitions(file.partition_values)}`,
        ),
      );
      return item;
    }),
  );
}

function renderActiveDataFileUrls(files: ActiveDataFile[]): void {
  activeDataFileUrlsNode.replaceChildren(
    ...files.map((file) => {
      const item = document.createElement('li');
      item.className = 'file-row active';
      item.append(
        textBlock('path', file.path),
        textBlock('detail', file.url_path),
        textBlock('meta', `absolute ${file.absolute_url}`),
        textBlock(
          'meta',
          `${formatBytes(file.size_bytes)}, ${formatPartitions(file.partition_values)}`,
        ),
      );
      if (file.stats) {
        item.append(textBlock('stats', `stats ${formatStats(file.stats)}`));
      }
      return item;
    }),
  );
}

function renderParquetPreflight(
  files: ParquetPreflightFile[],
  inspections: Map<string, ParquetInspectionSummary> = new Map(),
): void {
  parquetPreflightNode.replaceChildren(
    ...files.map((file) => {
      const item = document.createElement('li');
      item.className = 'file-row active';
      const inspection = inspections.get(file.path);
      item.append(
        textBlock('path', file.path),
        textBlock(
          'meta',
          `file ${formatBytes(file.size_bytes)}, footer ${formatBytes(file.footer_length_bytes)}, rows ${file.row_count}, row groups ${file.row_group_count}`,
        ),
        textBlock('detail', `partitions ${formatPartitions(file.partition_values)}`),
        textBlock('detail', `schema ${file.fields.map(formatParquetField).join(', ')}`),
      );
      const stats = formatParquetFieldStats(file.field_stats);
      if (stats) {
        item.append(textBlock('stats', `parquet stats ${stats}`));
      }
      if (file.delta_stats) {
        item.append(textBlock('stats', `delta stats ${formatStats(file.delta_stats)}`));
      }
      if (inspection) {
        item.append(
          textBlock(
            'detail',
            `created_by ${inspection.created_by ?? '-'}, version ${inspection.file_version}, metadata memory ${formatBytes(inspection.metadata_memory_size_bytes)}`,
          ),
          textBlock(
            'detail',
            `compression ${formatBytes(inspection.compression.compressed_size_bytes)} / ${formatBytes(inspection.compression.uncompressed_size_bytes)} (${formatBasisPoints(inspection.compression.ratio_basis_points)})`,
          ),
          textBlock('detail', formatParquetInspectionIndexes(inspection)),
          textBlock('detail', formatParquetInspectionColumnDetails(inspection)),
        );
      }
      return item;
    }),
  );
}

function formatParquetInspectionIndexes(inspection: ParquetInspectionSummary): string {
  const columnIndexCount = inspection.columns.filter((column) => column.has_column_index).length;
  const offsetIndexCount = inspection.columns.filter((column) => column.has_offset_index).length;
  const bloomFilterCount = inspection.columns.filter((column) => column.has_bloom_filter).length;
  return `indexes column ${columnIndexCount}/${inspection.column_count}, offset ${offsetIndexCount}/${inspection.column_count}, bloom ${bloomFilterCount}/${inspection.column_count}`;
}

function formatParquetInspectionColumnDetails(inspection: ParquetInspectionSummary): string {
  return inspection.columns
    .map((column) => {
      const encodings = column.encodings.length > 0 ? column.encodings.join('+') : '-';
      const compressions = column.compressions.length > 0 ? column.compressions.join('+') : '-';
      return `${column.name} ${compressions} ${encodings} compressed ${formatBytes(column.compressed_size_bytes)}`;
    })
    .join('; ');
}

function renderPruningPreflight(result: PruningPreflightResult): void {
  const summary = document.createElement('li');
  summary.className = 'file-row active';
  summary.append(
    textBlock('path', result.filter),
    textBlock(
      'meta',
      `files_touched ${result.files_touched}, files_skipped ${result.files_skipped}`,
    ),
  );

  pruningPreflightNode.replaceChildren(
    summary,
    ...result.files.map((file) => {
      const item = document.createElement('li');
      item.className = `file-row ${file.status === 'skip' ? 'inactive' : 'active'}`;
      item.append(
        textBlock('status', file.status === 'skip' ? 'skipped' : 'touched'),
        textBlock('path', file.path),
        textBlock('detail', file.detail),
      );
      return item;
    }),
  );
}

function planDeltaStatsPruning(files: ActiveDataFile[]): PruningPreflightResult {
  const column = 'value';
  const threshold = 90;
  const plannedFiles = files.map((file) => {
    const stats = parseDeltaStats(file.stats);
    const max = stats?.maxValues?.[column];
    if (typeof max === 'number' && max < threshold) {
      return {
        path: file.path,
        status: 'skip' as const,
        detail: `max ${column} ${max} < ${threshold}`,
      };
    }
    const min = stats?.minValues?.[column];
    return {
      path: file.path,
      status: 'touch' as const,
      detail:
        typeof min === 'number' && typeof max === 'number'
          ? `${column} range ${min}-${max}`
          : 'stats unavailable',
    };
  });

  return {
    filter: `${column} >= ${threshold}`,
    files_touched: plannedFiles.filter((file) => file.status === 'touch').length,
    files_skipped: plannedFiles.filter((file) => file.status === 'skip').length,
    files: plannedFiles,
  };
}

function renderInputOutputMap(
  fixture: FixtureManifest,
  objects: LogObjectDetail[],
  snapshot: ResolvedSnapshot,
): void {
  const activePaths = new Set(snapshot.active_files.map((file) => file.path));
  const actionsByPath = new Map<string, ParsedAction[]>();
  for (const action of objects.flatMap((object) => object.actions)) {
    if (!action.path) {
      continue;
    }
    const actions = actionsByPath.get(action.path) ?? [];
    actions.push(action);
    actionsByPath.set(action.path, actions);
  }

  const rows: HTMLElement[] = [];
  if (fixture.checkpoint_version !== undefined) {
    rows.push(
      mappingRow(
        'checkpoint seed',
        `version ${fixture.checkpoint_version} checkpoint seeds the replay state`,
      ),
    );
  }
  for (const step of fixture.generated_steps ?? []) {
    rows.push(mappingRow(`v${step.version} ${step.label}`, step.detail));
  }
  for (const file of fixture.data_files ?? []) {
    const actions = actionsByPath.get(file.relative_path) ?? [];
    const lastAdd = latestAction(actions, 'add');
    const lastRemove = latestAction(actions, 'remove');
    const status = activePaths.has(file.relative_path)
      ? activeOrigin(lastAdd, fixture.checkpoint_version)
      : inactiveOrigin(lastRemove, fixture.checkpoint_version);
    rows.push(mappingRow(file.relative_path, status));
  }
  inputOutputMapNode.replaceChildren(...rows);
}

function mappingRow(label: string, detail: string): HTMLElement {
  const item = document.createElement('li');
  item.className = 'mapping-row';
  item.append(textBlock('path', label), textBlock('detail', detail));
  return item;
}

function activeOrigin(
  action: ParsedAction | undefined,
  checkpointVersion: number | undefined,
): string {
  if (action?.version !== null && action?.version !== undefined) {
    if (checkpointVersion !== undefined && action.version > checkpointVersion) {
      return `active via replay commit ${action.version}`;
    }
  }
  return 'active via checkpoint seed';
}

function inactiveOrigin(
  action: ParsedAction | undefined,
  checkpointVersion: number | undefined,
): string {
  if (action?.version !== null && action?.version !== undefined) {
    if (checkpointVersion !== undefined && action.version > checkpointVersion) {
      return `inactive: checkpoint seed removed by replay commit ${action.version}`;
    }
    return `inactive: removed by commit ${action.version}`;
  }
  return 'inactive: not present in resolved output';
}

function latestAction(actions: ParsedAction[], kind: string): ParsedAction | undefined {
  return actions
    .filter((action) => action.kind === kind)
    .sort((left, right) => (right.version ?? -1) - (left.version ?? -1))[0];
}

function parseCommitActions(relativePath: string, text: string): ParsedAction[] {
  const version = versionFromPath(relativePath);
  return text
    .split('\n')
    .filter((line) => line.trim().length > 0)
    .map((line) => parseCommitAction(version, relativePath, line));
}

function parseCommitAction(version: number | null, source: string, line: string): ParsedAction {
  const action = JSON.parse(line) as Record<string, unknown>;
  if (isAction(action.add)) {
    return {
      version,
      source,
      kind: 'add',
      path: stringField(action.add, 'path'),
      stats: stringField(action.add, 'stats'),
    };
  }
  if (isAction(action.remove)) {
    return { version, source, kind: 'remove', path: stringField(action.remove, 'path') };
  }
  if (isAction(action.protocol)) {
    return { version, source, kind: 'protocol' };
  }
  if (isAction(action.metaData)) {
    return { version, source, kind: 'metaData' };
  }
  if (isAction(action.commitInfo)) {
    return { version, source, kind: 'commitInfo' };
  }
  return { version, source, kind: 'other' };
}

function isAction(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function stringField(value: Record<string, unknown>, field: string): string | undefined {
  const candidate = value[field];
  return typeof candidate === 'string' ? candidate : undefined;
}

function versionFromPath(path: string): number | null {
  const match = /_delta_log\/(\d{20})\.json$/.exec(path);
  return match ? Number.parseInt(match[1], 10) : null;
}

function classifyObject(path: string): ObjectKind {
  if (path === '_delta_log/_last_checkpoint') {
    return 'last_checkpoint';
  }
  if (path.endsWith('.checkpoint.parquet')) {
    return 'checkpoint_parquet';
  }
  if (path.endsWith('.json')) {
    return 'commit_json';
  }
  return 'delta_log_object';
}

function kindLabel(kind: ObjectKind): string {
  return kind.replaceAll('_', ' ');
}

function formatBytes(size: number | DecimalString): string {
  const bytes = BigInt(size);
  if (bytes < 1024n) {
    return `${bytes.toString()} bytes`;
  }
  const kibTenths = (bytes * 10n + 512n) / 1024n;
  return `${kibTenths / 10n}.${kibTenths % 10n} KB`;
}

function formatBasisPoints(value: number): string {
  const basisPoints = BigInt(value);
  const percentTenths = (basisPoints + 5n) / 10n;
  return `${percentTenths / 10n}.${percentTenths % 10n}%`;
}

function formatPartitions(partitions: Record<string, string | null>): string {
  const formatted = Object.entries(partitions)
    .map(([key, value]) => `${key}=${value ?? 'null'}`)
    .join(', ');
  return formatted || 'no partitions';
}

function formatStats(stats: string): string {
  try {
    const parsed = JSON.parse(stats) as { numRecords?: number };
    return parsed.numRecords === undefined ? compactJson(stats) : `${parsed.numRecords} rows`;
  } catch {
    return compactJson(stats);
  }
}

function parseDeltaStats(
  stats: string | undefined,
): { minValues?: Record<string, number>; maxValues?: Record<string, number> } | undefined {
  if (!stats) {
    return undefined;
  }
  try {
    return JSON.parse(stats) as {
      minValues?: Record<string, number>;
      maxValues?: Record<string, number>;
    };
  } catch {
    return undefined;
  }
}

function formatParquetField(field: ParquetPreflightField): string {
  const logicalType = field.logical_type ?? field.converted_type;
  const typeLabel = logicalType ? `${field.physical_type}/${logicalType}` : field.physical_type;
  return `${field.name}: ${typeLabel} ${field.repetition.toLowerCase()}`;
}

function formatParquetFieldStats(statsByField: Record<string, ParquetPreflightFieldStats>): string {
  return Object.entries(statsByField)
    .map(([field, stats]) => {
      const parts = [
        stats.min_i64 === undefined ? undefined : `min ${stats.min_i64}`,
        stats.max_i64 === undefined ? undefined : `max ${stats.max_i64}`,
        stats.null_count === undefined ? undefined : `nulls ${stats.null_count}`,
      ].filter((part): part is string => part !== undefined);
      return parts.length === 0 ? undefined : `${field} ${parts.join(' ')}`;
    })
    .filter((part): part is string => part !== undefined)
    .join('; ');
}

function compactJson(text: string): string {
  try {
    return JSON.stringify(JSON.parse(text));
  } catch {
    return text.trim();
  }
}

function textBlock(className: string, text: string): HTMLElement {
  const node = document.createElement('span');
  node.className = className;
  node.textContent = text;
  return node;
}

function createSqlEditor(initialDoc: string): EditorView {
  const editor = new EditorView({
    doc: initialDoc,
    extensions: [
      basicSetup,
      sql(),
      EditorView.lineWrapping,
      EditorView.theme({
        '&': {
          height: '100%',
        },
        '.cm-scroller': {
          fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Consolas, monospace',
          fontSize: '0.92rem',
          lineHeight: '1.55',
        },
      }),
    ],
    parent: sqlEditorHost,
  });
  editor.contentDOM.dataset.testid = 'sql-editor';
  editor.contentDOM.setAttribute('aria-label', 'SQL editor');
  return editor;
}

function setSqlEditorValue(sqlText: string): void {
  sqlEditor.dispatch({
    changes: {
      from: 0,
      to: sqlEditor.state.doc.length,
      insert: sqlText,
    },
  });
  sqlEditor.focus();
}

function clearDetails(): void {
  fixtureNameNode.textContent = '-';
  tableUriNode.textContent = '-';
  snapshotVersionNode.textContent = '-';
  checkpointVersionNode.textContent = '-';
  fileCountNode.textContent = '0 files';
  logObjectsNode.replaceChildren();
  commitActionsNode.replaceChildren();
  dataFilesNode.replaceChildren();
  activeFilesNode.replaceChildren();
  activeDataFileUrlsNode.replaceChildren();
  browserSnapshotProviderNode.textContent = '-';
  browserSnapshotModeNode.textContent = '-';
  browserSnapshotUriNode.textContent = '-';
  pruningPreflightNode.replaceChildren();
  parquetPreflightNode.replaceChildren();
  inputOutputMapNode.replaceChildren();
}

function renderBrowserSnapshotSource(provider: string, tableUri: string): void {
  browserSnapshotProviderNode.textContent = provider;
  browserSnapshotModeNode.textContent = 'browser_wasm';
  browserSnapshotUriNode.textContent = tableUri;
}

function selectedFixture(): FixtureChoice {
  return FIXTURES[selectedFixtureKey()] ?? FIXTURES[DEFAULT_FIXTURE_KEY];
}

function selectedFixtureKey(): string {
  return dataSourceSelect.value || DEFAULT_FIXTURE_KEY;
}

function selectedSourceMode(): SourceMode {
  const key = selectedFixtureKey();
  if (key === 'object-store') {
    return 'object_store';
  }
  if (key === 'local-files') {
    return 'local_files';
  }
  return 'fixture';
}

function selectedSourceLabel(): string {
  const mode = selectedSourceMode();
  if (mode === 'object_store') {
    return 'Browser object store';
  }
  if (mode === 'local_files') {
    return 'Local Delta table';
  }
  return selectedFixture().fallbackName ?? dataSourceSelect.selectedOptions[0].text;
}

function syncSourceControls(): void {
  syncObjectStoreControls();
  syncLocalTableControls();
}

function syncObjectStoreControls(): void {
  objectStoreControls.hidden = selectedSourceMode() !== 'object_store';
}

function syncLocalTableControls(): void {
  localTableControls.hidden = selectedSourceMode() !== 'local_files';
}

function syncObjectStoreDefaults(): void {
  const provider = objectStoreProviderSelect.value as DeltaObjectStoreProvider;
  const current = objectStoreTableUriInput.value.trim();
  const knownDefault =
    current === '' ||
    current === 's3://axon-fixtures/prod-like-events' ||
    current === 'gs://axon-fixtures/prod-like-events' ||
    current === 'az://axonaccount/tables/prod-like-events' ||
    current === 'abfs://tables@axonaccount.dfs.core.windows.net/prod-like-events';

  if (!knownDefault) {
    return;
  }

  objectStoreTableUriInput.value =
    provider === 's3'
      ? 's3://axon-fixtures/prod-like-events'
      : provider === 'gcs'
        ? 'gs://axon-fixtures/prod-like-events'
        : 'az://axonaccount/tables/prod-like-events';
}

function browserObjectStoreSource(): BrowserObjectStoreSource {
  const snapshotText = objectStoreSnapshotInput.value.trim();
  const source: BrowserObjectStoreSource = {
    provider: objectStoreProviderSelect.value as DeltaObjectStoreProvider,
    tableUri: objectStoreTableUriInput.value.trim(),
  };

  validateBrowserObjectStoreSource(source);

  if (snapshotText !== '') {
    const snapshotVersion = Number(snapshotText);
    if (!Number.isSafeInteger(snapshotVersion) || snapshotVersion < 0) {
      throw new Error('snapshot version must be a non-negative integer');
    }
    source.snapshotVersion = snapshotVersion;
  }

  return source;
}

function validateBrowserObjectStoreSource(source: BrowserObjectStoreSource): void {
  if (!source.tableUri) {
    throw new Error('object store table URI is required');
  }

  const validScheme =
    source.provider === 's3'
      ? source.tableUri.startsWith('s3://')
      : source.provider === 'gcs'
        ? source.tableUri.startsWith('gs://')
        : source.tableUri.startsWith('az://') || source.tableUri.startsWith('abfs://');
  if (!validScheme) {
    throw new Error(`table URI does not match ${source.provider} object-store scheme`);
  }
}

function renderError(error: unknown): void {
  setStatus('Error');
  errorPanel.hidden = false;
  errorNode.textContent = error instanceof Error ? error.message : String(error);
}

function setStatus(status: string): void {
  statusNode.textContent = status;
}

async function ensureWasm(): Promise<void> {
  wasmReady ??= init().then(() => undefined);
  await wasmReady;
}

async function fetchJson<T>(url: string): Promise<T> {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status} while fetching ${url}`);
  }
  return (await response.json()) as T;
}

async function fetchText(url: string): Promise<string> {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status} while fetching ${url}`);
  }
  return response.text();
}

function requiredNode(selector: string): HTMLElement {
  const node = document.querySelector<HTMLElement>(selector);
  if (!node) {
    throw new Error(`missing element for selector ${selector}`);
  }
  return node;
}
