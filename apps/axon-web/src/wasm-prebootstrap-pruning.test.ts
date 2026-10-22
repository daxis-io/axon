// Prebootstrap pruning registers only the selected files under the table name, so any part of the
// statement that reads the table outside the top-level WHERE, or that renames its columns, must
// fall back to the full file set. Runs the generated wasm glue through start_sql_stream against
// the bundled prod-like Delta table at v3: category=B holds ids 7-8 (values 70-80) and
// category=D holds ids 9-10 (values 90-100).
import { beforeAll, describe, expect, test } from 'vitest';
import { SandboxQuerySession } from './wasm/axon_web_wasm.js';
import {
  loadWasmQueryGlue,
  openProdLikeEvents,
  previewRows,
  runSqlStream,
  type WasmTable,
  type WasmTerminal,
} from './wasm-query-session-test-harness.ts';

let session: SandboxQuerySession;
let events: WasmTable;

beforeAll(async () => {
  loadWasmQueryGlue();
  session = new SandboxQuerySession();
  events = await openProdLikeEvents(session);
}, 60_000);

function filesPruned(terminal: WasmTerminal): number {
  return Number(terminal.response?.metrics?.prebootstrap_files_pruned);
}

describe('prebootstrap pruning on the shipping wasm session', () => {
  test('a subquery inside TRIM sees every partition', async () => {
    for (const sql of [
      "SELECT id, TRIM(BOTH (SELECT MAX(category) FROM events) FROM category) AS t FROM events WHERE category = 'B' ORDER BY id",
      "SELECT id, TRIM(category, (SELECT MAX(category) FROM events)) AS t FROM events WHERE category = 'B' ORDER BY id",
    ]) {
      const terminal = await runSqlStream(session, events, sql);
      expect(previewRows(terminal), sql).toEqual([
        [7, 'B'],
        [8, 'B'],
      ]);
      expect(filesPruned(terminal), sql).toBe(0);
    }
  }, 60_000);

  test('a table alias column list does not reuse physical column statistics', async () => {
    for (const sql of [
      'SELECT COUNT(*) AS c FROM events AS e(value, id, category) WHERE id > 8',
      'SELECT COUNT(*) AS c FROM events AS e(value, id, category) WHERE value < 85',
    ]) {
      const terminal = await runSqlStream(session, events, sql);
      expect(
        previewRows(terminal).map((row) => Number(row[0])),
        sql,
      ).toEqual([4]);
      expect(filesPruned(terminal), sql).toBe(0);
    }
  }, 60_000);

  test('single-reference filters still prune before bootstrap', async () => {
    const partition = await runSqlStream(
      session,
      events,
      "SELECT id FROM events WHERE category = 'B' ORDER BY id",
    );
    expect(previewRows(partition)).toEqual([[7], [8]]);
    expect(filesPruned(partition)).toBeGreaterThan(0);

    const stats = await runSqlStream(
      session,
      events,
      'SELECT id FROM events WHERE id > 8 ORDER BY id',
    );
    expect(previewRows(stats)).toEqual([[9], [10]]);
    expect(filesPruned(stats)).toBeGreaterThan(0);
  }, 60_000);
});
