// Drives the generated wasm glue under Node through the same SandboxQuerySession calls the private
// sandbox child makes, against the bundled prod-like fixtures. Files are exposed as blob: URLs like
// the local Delta folder path; fetches for them are answered from memory with browser Range
// semantics because Node's ranged blob: fetch is not byte-exact.
import { readFileSync } from 'node:fs';
import {
  BROWSER_SAFE_ARROW_IPC_BYTES,
  BROWSER_SAFE_PREVIEW_STRING_BYTES,
  BROWSER_SAFE_RESULT_ROW_LIMIT,
} from './axon-browser-sdk.ts';
import { initSync, SandboxQuerySession } from './wasm/axon_web_wasm.js';

// Matches QUERY_PREVIEW_LIMIT in sandbox-query-child-worker.ts.
export const CHILD_QUERY_PREVIEW_LIMIT = 501;

const PROD_LIKE_FIXTURE = new URL('../public/fixtures/prod-like/', import.meta.url);
const PROD_LIKE_SNAPSHOT_VERSION = 3;
const objectBodies = new Map<string, Uint8Array>();
let glueLoaded = false;

export type WasmPreview = {
  columns: string[];
  rows: unknown[][];
  row_count: string;
  preview_row_limit: number;
  truncated: boolean;
  [key: string]: unknown;
};

export type WasmTerminal = {
  status: string;
  error?: { code: string; message: string };
  preview?: WasmPreview;
  response?: { metrics?: Record<string, unknown> };
};

export type WasmTable = { name: string; tableUri: string; snapshotVersion?: number };

export function loadWasmQueryGlue(): void {
  if (glueLoaded) return;
  initSync({ module: readFileSync(new URL('./wasm/axon_web_wasm_bg.wasm', import.meta.url)) });
  const realFetch = globalThis.fetch.bind(globalThis);
  globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
    const body = objectBodies.get(url);
    if (!body) return realFetch(input, init);
    const headers = new Headers(init?.headers ?? (input instanceof Request ? input.headers : {}));
    return Promise.resolve(rangeResponse(url, body, headers.get('range')));
  }) as typeof fetch;
  glueLoaded = true;
}

export async function openProdLikeEvents(
  session: SandboxQuerySession,
  name = 'events',
): Promise<WasmTable> {
  const tableUri = 'local-delta://prod-like-events';
  const activeFiles = prodLikeActiveAdds().map((add) => {
    const bytes = readFileSync(new URL(add.path, new URL('table/', PROD_LIKE_FIXTURE)));
    return {
      path: add.path,
      url: blobUrl(bytes),
      size_bytes: bytes.length,
      partition_values: add.partitionValues,
      stats: add.stats,
    };
  });
  await session.open_delta_table(
    name,
    JSON.stringify({
      table_uri: tableUri,
      snapshot_version: PROD_LIKE_SNAPSHOT_VERSION,
      partition_column_types: { category: 'string' },
      active_files: activeFiles,
    }),
  );
  return { name, tableUri, snapshotVersion: PROD_LIKE_SNAPSHOT_VERSION };
}

// 65,536 rows with a unique event_id column, in one Parquet file.
export async function openEventIds(
  session: SandboxQuerySession,
  name = 'event_ids',
): Promise<WasmTable> {
  const tableUri = 'https://example.invalid/fixtures/event-id';
  const bytes = readFileSync(new URL('page-index-ab/event-id.parquet', PROD_LIKE_FIXTURE));
  await session.open_parquet_dataset(
    name,
    JSON.stringify({
      table_uri: tableUri,
      files: [
        {
          path: 'event-id.parquet',
          url: blobUrl(bytes),
          size_bytes: bytes.length,
          partition_values: {},
        },
      ],
    }),
  );
  return { name, tableUri };
}

export async function runSqlStream(
  session: SandboxQuerySession,
  table: WasmTable,
  sql: string,
  resultPage: { limit: number; offset: number } = {
    limit: BROWSER_SAFE_RESULT_ROW_LIMIT,
    offset: 0,
  },
): Promise<WasmTerminal> {
  const request = {
    table_uri: table.tableUri,
    snapshot_version: table.snapshotVersion,
    sql,
    preferred_target: 'browser_wasm',
    options: {
      collect_metrics: true,
      include_explain: false,
      result_page: resultPage,
      runtime_limits: {
        max_result_rows: BROWSER_SAFE_RESULT_ROW_LIMIT,
        max_arrow_ipc_bytes: BROWSER_SAFE_ARROW_IPC_BYTES,
        max_preview_string_bytes: BROWSER_SAFE_PREVIEW_STRING_BYTES,
      },
    },
  };
  let stream;
  try {
    stream = await session.start_sql_stream(
      table.name,
      JSON.stringify(request),
      CHILD_QUERY_PREVIEW_LIMIT,
    );
  } catch (error) {
    return { status: 'failed', error: startError(error) };
  }
  try {
    for (;;) {
      const item = (await stream.next()) as null | { terminal: { metadata_json: string } } | object;
      if (item === null) throw new Error('Arrow IPC stream closed without a terminal outcome');
      if ('terminal' in item) return JSON.parse(item.terminal.metadata_json) as WasmTerminal;
    }
  } finally {
    await stream.close();
  }
}

export function previewRows(terminal: WasmTerminal): unknown[][] {
  if (terminal.status !== 'succeeded' || !terminal.preview) {
    throw new Error(`query did not succeed: ${JSON.stringify(terminal.error ?? terminal.status)}`);
  }
  return terminal.preview.rows;
}

function prodLikeActiveAdds(): Array<{
  path: string;
  partitionValues: Record<string, string>;
  stats: string;
}> {
  const active = new Map<
    string,
    { path: string; partitionValues: Record<string, string>; stats: string }
  >();
  for (let version = 0; version <= PROD_LIKE_SNAPSHOT_VERSION; version += 1) {
    const commit = readFileSync(
      new URL(`table/_delta_log/${String(version).padStart(20, '0')}.json`, PROD_LIKE_FIXTURE),
      'utf8',
    );
    for (const line of commit.split('\n')) {
      if (!line.trim()) continue;
      const action = JSON.parse(line) as {
        add?: { path: string; partitionValues: Record<string, string>; stats: string };
        remove?: { path: string };
      };
      if (action.add) active.set(action.add.path, action.add);
      if (action.remove) active.delete(action.remove.path);
    }
  }
  return [...active.values()];
}

function blobUrl(bytes: Uint8Array): string {
  const url = URL.createObjectURL(new Blob([new Uint8Array(bytes)]));
  objectBodies.set(url, bytes);
  return url;
}

function rangeResponse(url: string, body: Uint8Array, range: string | null): Response {
  const match = range ? /^bytes=(\d*)-(\d*)$/.exec(range) : null;
  let response: Response;
  if (!match) {
    response = new Response(new Uint8Array(body), {
      status: 200,
      headers: { 'content-length': String(body.length) },
    });
  } else {
    const start = match[1] === '' ? Math.max(0, body.length - Number(match[2])) : Number(match[1]);
    const end =
      match[1] === '' || match[2] === ''
        ? body.length - 1
        : Math.min(Number(match[2]), body.length - 1);
    response = new Response(new Uint8Array(body.subarray(start, end + 1)), {
      status: 206,
      headers: {
        'content-length': String(end - start + 1),
        'content-range': `bytes ${start}-${end}/${body.length}`,
      },
    });
  }
  Object.defineProperty(response, 'url', { value: url });
  return response;
}

function startError(error: unknown): { code: string; message: string } {
  if (error && typeof error === 'object' && 'code' in error && 'message' in error) {
    return { code: String(error.code), message: String(error.message) };
  }
  if (typeof error === 'string') {
    try {
      const parsed = JSON.parse(error) as { code?: unknown; message?: unknown };
      return { code: String(parsed.code), message: String(parsed.message) };
    } catch {
      return { code: 'unknown', message: error };
    }
  }
  return { code: 'unknown', message: String(error) };
}
