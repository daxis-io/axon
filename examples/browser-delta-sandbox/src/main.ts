import init, { resolve_delta_snapshot_from_manifest } from './wasm/browser_delta_sandbox';
import './styles.css';

type FixtureManifest = {
  table_uri: string;
  objects: Array<{
    relative_path: string;
    url_path: string;
  }>;
};

type ResolvedSnapshot = {
  table_uri: string;
  snapshot_version: number;
  active_files: Array<{
    path: string;
    size_bytes: number;
    partition_values: Record<string, string | null>;
  }>;
};

const resolveButton = document.querySelector<HTMLButtonElement>('#resolve-snapshot');
const statusNode = requiredNode('[data-testid="status"]');
const tableUriNode = requiredNode('[data-testid="table-uri"]');
const snapshotVersionNode = requiredNode('[data-testid="snapshot-version"]');
const fileCountNode = requiredNode('[data-testid="file-count"]');
const activeFilesNode = requiredNode('[data-testid="active-files"]');
const errorPanel = requiredNode('.error-panel') as HTMLElement;
const errorNode = requiredNode('[data-testid="error"]');

let wasmReady: Promise<void> | undefined;

resolveButton?.addEventListener('click', () => {
  resolveSnapshot().catch((error: unknown) => {
    renderError(error);
  });
});

async function resolveSnapshot(): Promise<void> {
  setStatus('Resolving');
  errorPanel.hidden = true;
  await ensureWasm();

  const fixture = await fetchJson<FixtureManifest>('/fixtures/delta-log-manifest.json');
  const wasmManifest = {
    objects: fixture.objects.map((object) => ({
      relative_path: object.relative_path,
      url: new URL(object.url_path, window.location.href).toString(),
    })),
  };
  const snapshotJson = await resolve_delta_snapshot_from_manifest(
    JSON.stringify(wasmManifest),
    fixture.table_uri,
  );
  renderSnapshot(JSON.parse(snapshotJson) as ResolvedSnapshot);
}

function renderSnapshot(snapshot: ResolvedSnapshot): void {
  setStatus(`Snapshot ${snapshot.snapshot_version} resolved`);
  tableUriNode.textContent = snapshot.table_uri;
  snapshotVersionNode.textContent = String(snapshot.snapshot_version);
  fileCountNode.textContent = String(snapshot.active_files.length);
  activeFilesNode.replaceChildren(
    ...snapshot.active_files.map((file) => {
      const item = document.createElement('li');
      const partitions = Object.entries(file.partition_values)
        .map(([key, value]) => `${key}=${value ?? 'null'}`)
        .join(', ');
      item.textContent = `${file.path} (${file.size_bytes} bytes${partitions ? `, ${partitions}` : ''})`;
      return item;
    }),
  );
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

function requiredNode(selector: string): HTMLElement {
  const node = document.querySelector<HTMLElement>(selector);
  if (!node) {
    throw new Error(`missing element for selector ${selector}`);
  }
  return node;
}
