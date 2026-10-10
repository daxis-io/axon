// The editor sends its SQL text unchanged and asks the shipping wasm session for one result page
// at a time. These tests run the generated glue through start_sql_stream with the editor's page
// requests and fold the pages with the app's own pagination helpers, against the bundled
// 65,536-row event_id Parquet file (event_id is unique).
import { beforeAll, describe, expect, test } from 'vitest';
import { SandboxQuerySession } from './wasm/axon_web_wasm.js';
import {
  appendResultPage,
  defaultQueryPage,
  queryResultPageRequest,
  resultPageFromPreview,
} from './services/query-pagination.ts';
import type { QueryPageRequest, QueryResultData } from './services/types.ts';
import {
  loadWasmQueryGlue,
  openEventIds,
  runSqlStream,
  type WasmTable,
} from './wasm-query-session-test-harness.ts';

let session: SandboxQuerySession;
let eventIds: WasmTable;

beforeAll(async () => {
  loadWasmQueryGlue();
  session = new SandboxQuerySession();
  eventIds = await openEventIds(session);
}, 60_000);

async function runPage(sql: string, page: QueryPageRequest): Promise<QueryResultData> {
  const resultPage = queryResultPageRequest(page);
  const terminal = await runSqlStream(session, eventIds, sql, {
    limit: Number(resultPage.limit),
    offset: Number(resultPage.offset),
  });
  if (terminal.status !== 'succeeded' || !terminal.preview) {
    throw new Error(`${sql}: ${JSON.stringify(terminal.error ?? terminal.status)}`);
  }
  return resultPageFromPreview(
    { ...terminal.preview, row_count: Number(terminal.preview.row_count) } as Parameters<
      typeof resultPageFromPreview
    >[0],
    page,
  );
}

// Run, then Load more until the result reports no more rows (bounded).
async function runAndLoadAll(sql: string, maxPages: number): Promise<QueryResultData> {
  let result = await runPage(sql, defaultQueryPage());
  for (let loads = 1; loads < maxPages && result.page?.has_more; loads += 1) {
    const page = result.page;
    result = appendResultPage(
      result,
      await runPage(sql, { offset: page.next_offset ?? 0, size: page.size }),
    );
  }
  return result;
}

function eventIdsOf(result: QueryResultData): number[] {
  return result.rows.map((row) => Number(row[0]));
}

describe('result pages for SQL as typed in the editor', () => {
  test('a trailing comment or semicolon does not drop the page clause', async () => {
    for (const sql of [
      'SELECT event_id FROM event_ids WHERE event_id < 600 -- check',
      'SELECT event_id\nFROM event_ids\nWHERE event_id < 600\n-- AND event_id % 2 = 0\n',
      'SELECT event_id FROM event_ids WHERE event_id < 600; -- done',
      'SELECT event_id FROM event_ids WHERE event_id < 600 /* note */;',
    ]) {
      const result = await runPage(sql, defaultQueryPage());
      expect(result.rows, sql).toHaveLength(500);
      expect(result.page?.has_more, sql).toBe(true);
    }
  }, 60_000);

  test('exactly one row past the first page loads once and stops', async () => {
    const result = await runAndLoadAll(
      'SELECT event_id FROM event_ids WHERE event_id < 501 -- c',
      4,
    );

    expect(result.page?.has_more).toBe(false);
    expect(eventIdsOf(result).sort((a, b) => a - b)).toEqual(
      Array.from({ length: 501 }, (_, id) => id),
    );
  }, 60_000);

  test('a top-level LIMIT followed by a comment still pages', async () => {
    const result = await runPage(
      'SELECT event_id FROM event_ids WHERE event_id < 10 LIMIT 3 -- sample',
      defaultQueryPage(),
    );

    expect(result.rows).toHaveLength(3);
    expect(result.page?.has_more).toBe(false);
  }, 60_000);
});
