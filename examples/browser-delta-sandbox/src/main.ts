import init, { resolve_delta_snapshot_from_manifest } from './wasm/browser_delta_sandbox';
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
  data_files?: Array<{
    relative_path: string;
    url_path: string;
    size_bytes: number;
    partition_values: Record<string, string>;
  }>;
};

type ResolvedSnapshot = {
  table_uri: string;
  snapshot_version: number;
  partition_column_types?: Record<string, string>;
  active_files: Array<{
    path: string;
    size_bytes: number;
    partition_values: Record<string, string | null>;
    stats?: string;
  }>;
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

const resolveButton = document.querySelector<HTMLButtonElement>('#resolve-snapshot');
const resolveProdButton = document.querySelector<HTMLButtonElement>('#resolve-prod-snapshot');
const statusNode = requiredNode('[data-testid="status"]');
const fixtureNameNode = requiredNode('[data-testid="fixture-name"]');
const tableUriNode = requiredNode('[data-testid="table-uri"]');
const snapshotVersionNode = requiredNode('[data-testid="snapshot-version"]');
const checkpointVersionNode = requiredNode('[data-testid="checkpoint-version"]');
const fileCountNode = requiredNode('[data-testid="file-count"]');
const logObjectsNode = requiredNode('[data-testid="log-objects"]');
const commitActionsNode = requiredNode('[data-testid="commit-actions"]');
const dataFilesNode = requiredNode('[data-testid="data-files"]');
const activeFilesNode = requiredNode('[data-testid="active-files"]');
const inputOutputMapNode = requiredNode('[data-testid="input-output-map"]');
const errorPanel = requiredNode('.error-panel') as HTMLElement;
const errorNode = requiredNode('[data-testid="error"]');

let wasmReady: Promise<void> | undefined;

resolveButton?.addEventListener('click', () => {
  resolveSnapshot('/fixtures/delta-log-manifest.json', 'Delta JSON log smoke fixture').catch((error: unknown) => {
    renderError(error);
  });
});

resolveProdButton?.addEventListener('click', () => {
  resolveSnapshot('/fixtures/prod-like/delta-log-manifest.json').catch((error: unknown) => {
    renderError(error);
  });
});

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
  renderSnapshot(fixture, logObjectDetails, JSON.parse(snapshotJson) as ResolvedSnapshot, fallbackName);
}

function renderSnapshot(
  fixture: FixtureManifest,
  logObjectDetails: LogObjectDetail[],
  snapshot: ResolvedSnapshot,
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
      item.append(textBlock('badge', action.version === null ? action.kind : `v${action.version} ${action.kind}`));
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
        textBlock('meta', `${formatBytes(file.size_bytes)}, ${formatPartitions(file.partition_values)}`),
      );
      return item;
    }),
  );
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
    rows.push(mappingRow('checkpoint seed', `version ${fixture.checkpoint_version} checkpoint seeds the replay state`));
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

function activeOrigin(action: ParsedAction | undefined, checkpointVersion: number | undefined): string {
  if (action?.version !== null && action?.version !== undefined) {
    if (checkpointVersion !== undefined && action.version > checkpointVersion) {
      return `active via replay commit ${action.version}`;
    }
  }
  return 'active via checkpoint seed';
}

function inactiveOrigin(action: ParsedAction | undefined, checkpointVersion: number | undefined): string {
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

function formatBytes(size: number): string {
  if (size < 1024) {
    return `${size} bytes`;
  }
  return `${(size / 1024).toFixed(1)} KB`;
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
  inputOutputMapNode.replaceChildren();
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
