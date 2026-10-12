// The editor sends its SQL text unchanged and asks the shipping wasm session for one result page
// at a time; Load more re-runs the query with the next OFFSET. These tests run the generated glue
// through start_sql_stream with the editor's page requests and fold the pages with the app's own
// pagination helpers, against the bundled 65,536-row event_id Parquet file (event_id is unique
// and stored in ascending order).
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
    terminal.result_page,
  );
}

// Run, then Load more while the result offers it (the editor's loadMoreRows guard), bounded.
async function runAndLoadAll(sql: string, maxPages: number): Promise<QueryResultData> {
  let result = await runPage(sql, defaultQueryPage());
  for (let loads = 1; loads < maxPages; loads += 1) {
    const page = result.page;
    if (!page?.has_more || page.next_offset == null) break;
    result = appendResultPage(
      result,
      await runPage(sql, { offset: page.next_offset, size: page.size }),
    );
  }
  return result;
}

function eventIdsOf(result: QueryResultData): number[] {
  return result.rows.map((row) => Number(row[0]));
}

function idRange(first: number, step: number, count: number): number[] {
  return Array.from({ length: count }, (_, index) => first + index * step);
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

describe('Load more across re-run pages', () => {
  // Each Load more re-runs the query with a deeper top-K sort, which can order tied rows
  // differently, so following it would repeat some rows and skip others.
  test('Load more is not offered when the next row may tie on the ORDER BY key', async () => {
    for (const sql of [
      'SELECT event_id, event_id % 4 AS bucket FROM event_ids ORDER BY bucket',
      // The leading key is not an output column, so the boundary rows cannot be compared.
      'SELECT event_id, event_id % 4 AS bucket FROM event_ids ORDER BY event_id % 4, event_id',
    ]) {
      const result = await runAndLoadAll(sql, 4);
      const ids = eventIdsOf(result);

      expect(new Set(ids).size, sql).toBe(ids.length);
      expect(result.rows, sql).toHaveLength(500);
      expect(result.page?.has_more, sql).toBe(true);
      expect(result.page?.next_offset, sql).toBeUndefined();
    }
  }, 60_000);

  test('a unique ORDER BY key pages through every row without gaps', async () => {
    const byBucket = await runAndLoadAll(
      'SELECT event_id, event_id % 4 AS bucket FROM event_ids ORDER BY bucket, event_id',
      4,
    );
    expect(eventIdsOf(byBucket)).toEqual(idRange(0, 4, 2000));
    expect(byBucket.page?.next_offset).toBe(2000);

    const descending = await runAndLoadAll(
      'SELECT event_id FROM event_ids ORDER BY 1 DESC -- newest first',
      3,
    );
    expect(eventIdsOf(descending)).toEqual(idRange(65535, -1, 1500));
  }, 60_000);

  test('unordered pages continue the same scan order', async () => {
    const filtered = await runAndLoadAll(
      'SELECT event_id FROM event_ids WHERE event_id % 3 = 1',
      4,
    );
    expect(eventIdsOf(filtered)).toEqual(idRange(1, 3, 2000));

    const grouped = await runAndLoadAll(
      'SELECT event_id % 1600 AS k, COUNT(*) AS n FROM event_ids GROUP BY event_id % 1600',
      5,
    );
    const keys = eventIdsOf(grouped);
    expect(grouped.page?.has_more).toBe(false);
    expect(keys.sort((a, b) => a - b)).toEqual(idRange(0, 1, 1600));

    const sql = 'SELECT DISTINCT event_id % 1600 AS k FROM event_ids';
    const second = await runPage(sql, { offset: 500, size: 500 });
    expect((await runPage(sql, { offset: 500, size: 500 })).rows).toEqual(second.rows);
  }, 60_000);
});
