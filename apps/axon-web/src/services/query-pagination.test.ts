import { create } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';
import {
  PreviewCellSchema,
  QueryExecutionOptionsSchema,
  QueryRequestSchema,
  QueryRuntimeLimitsSchema,
  ResultPreviewRowSchema,
  ResultPreviewSchema,
  ExecutionTarget,
} from '../generated/contracts/protobuf/axon/exec/v1/exec_pb.ts';
import {
  SAMPLE_QUERY_SOURCE,
  SAMPLE_QUERY_SOURCE_REF,
  type AvailableQuerySourceSelection,
} from './query-source.ts';
import { canonicalTableForSelection } from './browser-read-resolution.ts';
import {
  QUERY_RESULT_PAGE_SIZE,
  appendResultPage,
  canLoadMoreResultRows,
  queryPageFromRequest,
  queryResultPageRequest,
  queryResultPageRun,
  queryResultPageRunRequest,
  resultPageFromContractPreview,
  resultPageFromPreview,
  sameQueryResultPageRun,
} from './query-pagination.ts';
import type { BrowserWorkerResultPreview } from '../axon-browser-sdk.ts';

const selection: AvailableQuerySourceSelection = {
  kind: 'sample',
  ref: SAMPLE_QUERY_SOURCE_REF,
  source: SAMPLE_QUERY_SOURCE,
};

function request(sql = 'select * from events') {
  return create(QueryRequestSchema, {
    sql,
    preferredTarget: ExecutionTarget.BROWSER_WASM,
    options: create(QueryExecutionOptionsSchema, {
      collectMetrics: true,
      resultPage: queryResultPageRequest({ offset: 0, size: QUERY_RESULT_PAGE_SIZE }),
      runtimeLimits: create(QueryRuntimeLimitsSchema, {
        maxResultRows: 501n,
        maxArrowIpcBytes: 8n * 1024n * 1024n,
        maxPreviewStringBytes: 256n * 1024n,
      }),
    }),
  });
}

describe('generated query pagination identity', () => {
  it('carries the sentinel page in the generated query contract', () => {
    const page = queryResultPageRequest({ offset: 500, size: QUERY_RESULT_PAGE_SIZE });

    expect(page).toMatchObject({ limit: 501n, offset: 500n });
  });

  it('clones the generated table/query and replaces only the page for pagination', () => {
    const table = canonicalTableForSelection(selection);
    const query = request();
    const run = queryResultPageRun(table, query, selection, 3);
    table.name = 'mutated';
    query.sql = 'mutated';

    const next = queryResultPageRunRequest(run, { offset: 500, size: 250 });

    expect(run.table.name).toBe('events');
    expect(run.query.sql).toBe('select * from events');
    expect(next).toMatchObject({
      sql: 'select * from events',
      options: { resultPage: { limit: 251n, offset: 500n } },
    });
    expect(queryPageFromRequest(next)).toEqual({ offset: 500, size: 250 });
  });

  it('uses canonical table and generated query equality instead of source transport fields', () => {
    const original = queryResultPageRun(
      canonicalTableForSelection(selection),
      request(),
      selection,
      3,
    );
    const displayOnlyChange = queryResultPageRun(
      canonicalTableForSelection(selection),
      request(),
      {
        ...selection,
        source: { ...selection.source, rows: 999, files: 22 },
      },
      3,
    );
    const edited = queryResultPageRun(
      canonicalTableForSelection(selection),
      request('select id from events'),
      selection,
      3,
    );

    expect(sameQueryResultPageRun(original, displayOnlyChange)).toBe(true);
    expect(sameQueryResultPageRun(original, edited)).toBe(false);
  });

  it('projects every generated preview cell variant into the bounded UI page', () => {
    const preview = create(ResultPreviewSchema, {
      columns: ['text', 'number', 'flag', 'missing'],
      rows: [
        create(ResultPreviewRowSchema, {
          cells: [
            create(PreviewCellSchema, {
              value: { case: 'stringValue', value: 'alpha' },
            }),
            create(PreviewCellSchema, {
              value: { case: 'numberValue', value: 3.5 },
            }),
            create(PreviewCellSchema, {
              value: { case: 'boolValue', value: false },
            }),
            create(PreviewCellSchema, {
              value: { case: 'nullValue', value: 0 },
            }),
          ],
        }),
      ],
      rowCount: 2n,
      previewRowLimit: 1n,
      truncated: true,
    });

    expect(
      resultPageFromContractPreview(
        preview,
        { offset: 10, size: 1 },
        { ordered: false, order_key_columns: [] },
      ),
    ).toEqual({
      columns: [
        { name: 'text', type: 'string' },
        { name: 'number', type: 'double' },
        { name: 'flag', type: 'boolean' },
        { name: 'missing', type: 'string' },
      ],
      rows: [['alpha', 3.5, false, null]],
      row_count: 11,
      truncated: true,
      page: {
        offset: 10,
        size: 1,
        returned_rows: 1,
        loaded_rows: 11,
        has_more: true,
        next_offset: 11,
      },
    });
  });
});

describe('Load more over re-run result pages', () => {
  // Two page rows plus the sentinel row the page request asks for past the page.
  function preview(rows: BrowserWorkerResultPreview['rows']): BrowserWorkerResultPreview {
    return {
      columns: ['bucket', 'id'],
      rows,
      row_count: rows.length,
      preview_row_limit: 3,
      truncated: false,
    };
  }
  const page = { offset: 0, size: 2 };
  const tiedAtBoundary = preview([
    [0, 1],
    [0, 2],
    [0, 3],
  ]);

  it('offers Load more for unsorted rows', () => {
    const result = resultPageFromPreview(tiedAtBoundary, page, {
      ordered: false,
      order_key_columns: [],
    });

    expect(result.page).toEqual({
      offset: 0,
      size: 2,
      returned_rows: 2,
      loaded_rows: 2,
      has_more: true,
      next_offset: 2,
    });
    expect(canLoadMoreResultRows(result.page)).toBe(true);
  });

  it('offers Load more for sorted rows only past a boundary a leading ORDER BY column splits', () => {
    for (const [rows, orderKeyColumns, blocked] of [
      [tiedAtBoundary.rows, [0], true],
      [tiedAtBoundary.rows, [0, 1], false],
      [tiedAtBoundary.rows, [1], false],
      [
        [
          [0, 1],
          [0, 2],
          [1, 2],
        ],
        [0],
        false,
      ],
      [
        [
          [0, null],
          [0, null],
          [0, null],
        ],
        [0, 1],
        true,
      ],
    ] as const) {
      const result = resultPageFromPreview(preview(rows.map((row) => [...row])), page, {
        ordered: true,
        order_key_columns: [...orderKeyColumns],
      });

      expect(result.page?.has_more).toBe(true);
      expect(canLoadMoreResultRows(result.page), JSON.stringify({ rows, orderKeyColumns })).toBe(
        !blocked,
      );
      expect(result.page?.next_offset).toBe(blocked ? undefined : 2);
      expect(result.page?.load_more_blocked).toBe(blocked ? 'order_by_tie' : undefined);
    }
  });

  it('does not offer Load more for sorted rows that no reported ORDER BY column can split', () => {
    // An empty key list means the runtime found no ORDER BY key the page rows can be compared on
    // (an expression or qualified leading key, a sorting window, DISTINCT ON or a nested ORDER BY),
    // which is not a tie at the boundary: adding a unique ORDER BY column would not change it.
    for (const rows of [
      tiedAtBoundary.rows,
      [
        [0, 1],
        [0, 2],
        [1, 3],
      ],
    ]) {
      const result = resultPageFromPreview(preview(rows.map((row) => [...row])), page, {
        ordered: true,
        order_key_columns: [],
      });

      expect(result.page?.has_more).toBe(true);
      expect(result.page?.next_offset).toBeUndefined();
      expect(result.page?.load_more_blocked).toBe('unverifiable_order');
      expect(canLoadMoreResultRows(result.page)).toBe(false);
    }
  });

  it('does not offer Load more for sorted rows when the row past the page is not in the preview', () => {
    const result = resultPageFromPreview(
      { ...preview(tiedAtBoundary.rows.slice(0, 2)), row_count: 3 },
      page,
      { ordered: true, order_key_columns: [1] },
    );

    expect(result.page?.has_more).toBe(true);
    expect(result.page?.load_more_blocked).toBe('order_by_tie');
    expect(canLoadMoreResultRows(result.page)).toBe(false);
  });

  it('does not offer Load more when the runtime did not report the row order', () => {
    const result = resultPageFromPreview(tiedAtBoundary, page);

    expect(result.page?.has_more).toBe(true);
    expect(result.page?.next_offset).toBeUndefined();
    expect(result.page?.load_more_blocked).toBe('unknown_order');
    expect(canLoadMoreResultRows(result.page)).toBe(false);
    expect(canLoadMoreResultRows(undefined)).toBe(false);
  });

  it('takes the Load more decision of the newest page when appending', () => {
    const first = resultPageFromPreview(tiedAtBoundary, page, {
      ordered: true,
      order_key_columns: [1],
    });
    const second = resultPageFromPreview(
      tiedAtBoundary,
      { offset: 2, size: 2 },
      {
        ordered: true,
        order_key_columns: [0],
      },
    );

    const appended = appendResultPage(first, second);

    expect(appended.rows).toHaveLength(4);
    expect(appended.page).toEqual({
      offset: 0,
      size: 2,
      returned_rows: 2,
      loaded_rows: 4,
      has_more: true,
      load_more_blocked: 'order_by_tie',
    });
    expect(canLoadMoreResultRows(appended.page)).toBe(false);
    expect(appendResultPage(second, first).page?.next_offset).toBe(4);
  });
});
