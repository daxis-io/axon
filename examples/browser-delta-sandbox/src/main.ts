import init, {
  preflight_parquet_metadata_for_targets,
  resolve_delta_snapshot_from_manifest,
} from './wasm/browser_delta_sandbox';
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
  type FallbackReason,
  type PartitionColumnType,
  type QueryError,
} from './axon-browser-sdk';
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

type ObjectKind = 'commit_json' | 'checkpoint_parquet' | 'last_checkpoint';

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

const FIXTURES: Record<string, FixtureChoice> = {
  simple: {
    manifestUrl: '/fixtures/delta-log-manifest.json',
    fallbackName: 'Delta JSON log smoke fixture',
  },
  'prod-like': {
    manifestUrl: '/fixtures/prod-like/delta-log-manifest.json',
  },
};

const QUERY_TABLE_NAME = 'axon_table';
const SAMPLE_QUERIES = {
  count: 'SELECT COUNT(*) AS row_count FROM axon_table',
  category:
    'SELECT category, COUNT(*) AS rows, SUM(value) AS total_value FROM axon_table GROUP BY category ORDER BY category',
  'top-values':
    'SELECT id, category, value FROM axon_table WHERE value >= 90 ORDER BY value DESC LIMIT 20',
} as const;

const resolveButton = document.querySelector<HTMLButtonElement>('#resolve-snapshot');
const runSqlButton = requiredNode('#run-sql') as HTMLButtonElement;
const cancelSqlButton = requiredNode('#cancel-sql') as HTMLButtonElement;
const sqlEditor = requiredNode('#sql-editor') as HTMLTextAreaElement;
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
const pruningPreflightNode = requiredNode('[data-testid="pruning-preflight"]');
const parquetPreflightNode = requiredNode('[data-testid="parquet-preflight"]');
const inputOutputMapNode = requiredNode('[data-testid="input-output-map"]');
const errorPanel = requiredNode('.error-panel') as HTMLElement;
const errorNode = requiredNode('[data-testid="error"]');

let wasmReady: Promise<void> | undefined;
let queryClient: AxonBrowserClient | undefined;
let queryDescriptor: BrowserHttpSnapshotDescriptor | undefined;
let queryTableOpened = false;
let queryRequestCounter = 0;
let activeQuery: { token: number; requestId: string } | undefined;
let activeWorkerEventRequestIds: Set<string> | undefined;
let queryTokenCounter = 0;

resolveButton?.addEventListener('click', () => {
  const fixture = selectedFixture();
  resolveSnapshot(fixture.manifestUrl, fixture.fallbackName).catch((error: unknown) => {
    renderError(error);
  });
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
    sqlEditor.value = SAMPLE_QUERIES[queryName] ?? SAMPLE_QUERIES.count;
  });
});

sqlEditor.value = SAMPLE_QUERIES.count;
clearQueryOutput();

async function resolveSnapshot(manifestUrl: string, fallbackName?: string): Promise<void> {
  setStatus('Resolving');
  errorPanel.hidden = true;
  clearDetails();
  await ensureWasm();

  const fixture = await fetchJson<FixtureManifest>(manifestUrl);
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
  setStatus('Preflighting Parquet');
  const parquetPreflight = await preflightActiveParquetFiles(activeFiles);
  renderSnapshot(fixture, logObjectDetails, snapshot, activeFiles, parquetPreflight, fallbackName);
  prepareQuerySandbox(snapshot, activeFiles);
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
  fileCountNode.textContent = String(snapshot.active_files.length);
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

function prepareQuerySandbox(snapshot: ResolvedSnapshot, activeFiles: ActiveDataFile[]): void {
  resetQueryClient();
  clearQueryOutput();
  queryDescriptor = browserSnapshotDescriptor(snapshot, activeFiles);
  if (activeFiles.length === 0) {
    setQueryStatus('Resolve the prod-like fixture to enable SQL');
    runSqlButton.disabled = true;
    return;
  }

  setQueryStatus('Ready');
  runSqlButton.disabled = false;
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
  if (!queryDescriptor) {
    throw new Error('resolve the prod-like fixture before running SQL');
  }

  const token = ++queryTokenCounter;
  const requestId = `sandbox-query-${++queryRequestCounter}`;
  activeQuery = { token, requestId };
  activeWorkerEventRequestIds = new Set([requestId]);
  cancelSqlButton.disabled = false;
  setQueryStatus('Running');
  queryElapsedMsNode.textContent = '-';
  queryExecutedOnNode.textContent = '-';
  queryFallbackReasonNode.textContent = '-';
  queryArrowIpcBytesNode.textContent = '-';
  queryRowCountNode.textContent = '-';
  queryMetricsNode.textContent = '-';
  queryErrorNode.textContent = '-';
  resultGridNode.replaceChildren();
  workerEventLogNode.replaceChildren();
  const startedAt = performance.now();

  try {
    const client = ensureQueryClient();
    await ensureQueryTableOpen(client, token);
    if (!isActiveQuery(token)) {
      return;
    }

    const result = await client.query(QUERY_TABLE_NAME, sqlEditor.value, {
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
  } catch (error) {
    if (!isActiveQuery(token)) {
      return;
    }
    renderQueryError(error, elapsedLabel(performance.now() - startedAt));
  } finally {
    if (isActiveQuery(token)) {
      activeQuery = undefined;
      activeWorkerEventRequestIds = undefined;
      cancelSqlButton.disabled = true;
    }
  }
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
  cancelSqlButton.disabled = true;
  setQueryStatus('Cancellation requested');
  queryFallbackReasonNode.textContent = 'browser_runtime_constraint';
  const cancellationError: QueryError = {
    code: 'fallback_required',
    message:
      'UI cancellation requested; true worker abort is not available, so this request was superseded locally.',
    target: 'browser_wasm',
    fallback_reason: 'browser_runtime_constraint',
  };
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
  queryErrorNode.textContent = JSON.stringify(cancellationError, null, 2);
}

function isActiveQuery(token: number): boolean {
  return activeQuery?.token === token;
}

function renderQueryResult(result: AxonQueryResult, elapsed: string): void {
  setQueryStatus('Finished');
  queryElapsedMsNode.textContent = elapsed;
  queryExecutedOnNode.textContent = result.response.executed_on;
  queryFallbackReasonNode.textContent = fallbackReasonLabel(result.response.fallback_reason);
  queryArrowIpcBytesNode.textContent = formatBytes(result.result.bytes.byteLength);
  queryRowCountNode.textContent = result.preview ? String(result.preview.row_count) : '-';
  queryMetricsNode.textContent = JSON.stringify(result.response.metrics, null, 2);
  queryErrorNode.textContent = '-';
  renderResultPreview(result.preview);
}

function renderQueryError(error: unknown, elapsed: string): void {
  const queryError = errorToQueryError(error);
  setQueryStatus('Error');
  queryElapsedMsNode.textContent = elapsed;
  queryExecutedOnNode.textContent = queryError.target;
  queryFallbackReasonNode.textContent = fallbackReasonLabel(queryError.fallback_reason);
  queryErrorNode.textContent = JSON.stringify(queryError, null, 2);
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

function elapsedLabel(milliseconds: number): string {
  return `${Math.max(0, Math.round(milliseconds))} ms`;
}

function setQueryStatus(status: string): void {
  queryStatusNode.textContent = status;
}

function clearQueryOutput(): void {
  setQueryStatus('No query');
  queryElapsedMsNode.textContent = '-';
  queryExecutedOnNode.textContent = '-';
  queryFallbackReasonNode.textContent = '-';
  queryArrowIpcBytesNode.textContent = '-';
  queryRowCountNode.textContent = '-';
  queryMetricsNode.textContent = '-';
  queryErrorNode.textContent = '-';
  resultGridNode.replaceChildren();
  workerEventLogNode.replaceChildren();
  runSqlButton.disabled = true;
  cancelSqlButton.disabled = true;
}

function resetQueryClient(): void {
  activeQuery = undefined;
  activeWorkerEventRequestIds = undefined;
  queryTableOpened = false;
  queryDescriptor = undefined;
  if (queryClient) {
    queryClient.terminate();
    queryClient = undefined;
  }
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

function renderParquetPreflight(files: ParquetPreflightFile[]): void {
  parquetPreflightNode.replaceChildren(
    ...files.map((file) => {
      const item = document.createElement('li');
      item.className = 'file-row active';
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
      return item;
    }),
  );
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
  return 'commit_json';
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

function clearDetails(): void {
  fixtureNameNode.textContent = '-';
  tableUriNode.textContent = '-';
  snapshotVersionNode.textContent = '-';
  checkpointVersionNode.textContent = '-';
  fileCountNode.textContent = '0';
  logObjectsNode.replaceChildren();
  commitActionsNode.replaceChildren();
  dataFilesNode.replaceChildren();
  activeFilesNode.replaceChildren();
  activeDataFileUrlsNode.replaceChildren();
  pruningPreflightNode.replaceChildren();
  parquetPreflightNode.replaceChildren();
  inputOutputMapNode.replaceChildren();
  resetQueryClient();
  clearQueryOutput();
}

function selectedFixture(): FixtureChoice {
  const checked = document.querySelector<HTMLInputElement>('input[name="fixture"]:checked');
  return FIXTURES[checked?.value ?? 'simple'] ?? FIXTURES.simple;
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
