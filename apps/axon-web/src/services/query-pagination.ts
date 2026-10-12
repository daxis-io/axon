import { clone, create, equals } from '@bufbuild/protobuf';
import {
  BROWSER_SAFE_ARROW_IPC_BYTES,
  BROWSER_SAFE_PREVIEW_STRING_BYTES,
  BROWSER_SAFE_RESULT_ROW_LIMIT,
  type BrowserWorkerResultPage,
  type BrowserWorkerResultPreview,
} from '../axon-browser-sdk.ts';
import {
  ExecutionTarget,
  QueryExecutionOptionsSchema,
  QueryRequestSchema,
  QueryResultPageSchema,
  QueryRuntimeLimitsSchema,
  type QueryRequest,
  type QueryResultPage,
  type ResultPreview,
} from '../generated/contracts/protobuf/axon/exec/v1/exec_pb.ts';
import {
  TableNodeSchema,
  type TableNode,
} from '../generated/contracts/protobuf/axon/catalog/v1/catalog_pb.ts';
import type {
  QueryPageRequest,
  QueryResultData,
  QueryResultPageInfo,
  ResultCell,
  ResultColumn,
} from './types.ts';
import type { AvailableQuerySourceSelection } from './query-source.ts';

export const QUERY_RESULT_PAGE_SIZE = 500;
export const MAX_QUERY_RESULT_PAGE_LIMIT = QUERY_RESULT_PAGE_SIZE + 1;

export type QueryResultPageRun = Readonly<{
  table: TableNode;
  query: QueryRequest;
  selection: AvailableQuerySourceSelection;
  snapshotVersion?: number;
}>;

export function queryResultPageRun(
  table: TableNode,
  query: QueryRequest,
  selection: AvailableQuerySourceSelection,
  snapshotVersion?: number,
): QueryResultPageRun {
  return {
    table: clone(TableNodeSchema, table),
    query: clone(QueryRequestSchema, query),
    selection,
    snapshotVersion,
  };
}

export function sameQueryResultPageRun(
  left: QueryResultPageRun,
  right: QueryResultPageRun,
): boolean {
  return (
    equals(TableNodeSchema, left.table, right.table) &&
    equals(QueryRequestSchema, left.query, right.query) &&
    Object.is(left.snapshotVersion, right.snapshotVersion)
  );
}

export function queryResultPageRunRequest(
  run: QueryResultPageRun,
  page: QueryPageRequest,
): QueryRequest {
  const query = clone(QueryRequestSchema, run.query);
  query.options ??= create(QueryExecutionOptionsSchema);
  query.options.resultPage = queryResultPageRequest(page);
  return query;
}

export function defaultQueryPage(): QueryPageRequest {
  return { offset: 0, size: QUERY_RESULT_PAGE_SIZE };
}

export function browserQueryRequest(input: {
  sql: string;
  preferredTarget: ExecutionTarget;
  page?: QueryPageRequest;
}): QueryRequest {
  return create(QueryRequestSchema, {
    sql: input.sql,
    preferredTarget: input.preferredTarget,
    options: create(QueryExecutionOptionsSchema, {
      collectMetrics: true,
      includeExplain: true,
      resultPage: queryResultPageRequest(input.page ?? defaultQueryPage()),
      runtimeLimits: create(QueryRuntimeLimitsSchema, {
        maxResultRows: BigInt(BROWSER_SAFE_RESULT_ROW_LIMIT),
        maxArrowIpcBytes: BigInt(BROWSER_SAFE_ARROW_IPC_BYTES),
        maxPreviewStringBytes: BigInt(BROWSER_SAFE_PREVIEW_STRING_BYTES),
      }),
    }),
  });
}

export function queryResultPageRequest(page: QueryPageRequest): QueryResultPage {
  const offset = nonNegativeInteger(page.offset, 'query result page offset');
  const size = positiveInteger(page.size, 'query result page size');
  if (size >= Number.MAX_SAFE_INTEGER) {
    throw new Error('query result page size is too large to request a sentinel row');
  }
  if (size + 1 > MAX_QUERY_RESULT_PAGE_LIMIT) {
    throw new Error(`query result page size ${size} exceeds maximum ${QUERY_RESULT_PAGE_SIZE}`);
  }
  return create(QueryResultPageSchema, {
    limit: BigInt(size + 1),
    offset: BigInt(offset),
  });
}

export function queryPageFromRequest(request: QueryRequest): QueryPageRequest {
  const page = request.options?.resultPage;
  if (!page?.limit || page.limit < 2n) {
    throw new Error('generated query request requires a sentinel result-page limit');
  }
  const size = safeInteger(page.limit - 1n, 'query result page size');
  const offset = safeInteger(page.offset ?? 0n, 'query result page offset');
  if (size > QUERY_RESULT_PAGE_SIZE) {
    throw new Error(`query result page size ${size} exceeds maximum ${QUERY_RESULT_PAGE_SIZE}`);
  }
  return { offset, size };
}

export function resultPageFromPreview(
  preview: BrowserWorkerResultPreview | undefined,
  page: QueryPageRequest,
  order?: BrowserWorkerResultPage,
): QueryResultData {
  if (!preview) {
    return emptyPageResult(page);
  }

  const size = positiveInteger(page.size, 'query result page size');
  const offset = nonNegativeInteger(page.offset, 'query result page offset');
  const columns: ResultColumn[] = preview.columns.map((name, idx) => ({
    name,
    type: inferTypeFromColumn(preview.rows, idx),
  }));
  const rows = preview.rows.slice(0, size).map((row) => row as ResultCell[]);
  const hasMore = preview.row_count > rows.length || preview.rows.length > size;
  const loadedRows = offset + rows.length;
  const loadMoreBlocked = hasMore ? loadMoreBlockedReason(preview.rows, size, order) : undefined;

  return {
    columns,
    rows,
    row_count: loadedRows,
    truncated: hasMore,
    page: {
      offset,
      size,
      returned_rows: rows.length,
      loaded_rows: loadedRows,
      has_more: hasMore,
      next_offset: hasMore && !loadMoreBlocked ? loadedRows : undefined,
      ...(loadMoreBlocked ? { load_more_blocked: loadMoreBlocked } : {}),
    },
  };
}

export function canLoadMoreResultRows(page: QueryResultPageInfo | undefined): boolean {
  return (
    page?.has_more === true && page.load_more_blocked === undefined && page.next_offset != null
  );
}

// Load more re-runs the query with the next OFFSET. Unsorted rows come back in the same scan
// order every time. Sorted rows only continue the loaded ones when the last page row and the row
// after it (the sentinel past the page) differ in a leading ORDER BY column: then every loaded row
// sorts strictly before every later row, so a deeper re-run cannot move a row across the boundary.
// Rows that tie there can come back in a different order and repeat or skip rows.
function loadMoreBlockedReason(
  rows: BrowserWorkerResultPreview['rows'],
  size: number,
  order: BrowserWorkerResultPage | undefined,
): QueryResultPageInfo['load_more_blocked'] {
  if (!order) return 'unknown_order';
  if (!order.ordered) return undefined;
  const last = rows[size - 1];
  const next = rows[size];
  if (last && next && order.order_key_columns.some((column) => last[column] !== next[column])) {
    return undefined;
  }
  return 'order_by_tie';
}

export function resultPageFromContractPreview(
  preview: ResultPreview | undefined,
  page: QueryPageRequest,
  order?: BrowserWorkerResultPage,
): QueryResultData {
  if (!preview) return emptyPageResult(page);
  return resultPageFromPreview(
    {
      columns: preview.columns,
      rows: preview.rows.map((row) =>
        row.cells.map((cell) => {
          switch (cell.value.case) {
            case 'stringValue':
            case 'numberValue':
            case 'boolValue':
              return cell.value.value;
            case 'nullValue':
            case undefined:
              return null;
          }
        }),
      ),
      row_count:
        preview.rowCount === undefined
          ? preview.rows.length
          : safeInteger(preview.rowCount, 'result preview row count'),
      preview_row_limit:
        preview.previewRowLimit === undefined
          ? preview.rows.length
          : safeInteger(preview.previewRowLimit, 'result preview row limit'),
      truncated: preview.truncated ?? false,
    },
    page,
    order,
  );
}

export function appendResultPage(current: QueryResultData, next: QueryResultData): QueryResultData {
  if (!sameColumns(current.columns, next.columns)) {
    return next;
  }

  const rows = [...current.rows, ...next.rows];
  const loadedRows = rows.length;
  const hasMore = next.page?.has_more === true;
  const loadMoreBlocked = hasMore ? next.page?.load_more_blocked : undefined;
  const pageSize = next.page?.size ?? current.page?.size ?? QUERY_RESULT_PAGE_SIZE;

  return {
    columns: current.columns,
    rows,
    row_count: loadedRows,
    truncated: hasMore,
    page: {
      offset: current.page?.offset ?? 0,
      size: pageSize,
      returned_rows: next.page?.returned_rows ?? next.rows.length,
      loaded_rows: loadedRows,
      has_more: hasMore,
      next_offset: hasMore && !loadMoreBlocked ? loadedRows : undefined,
      ...(loadMoreBlocked ? { load_more_blocked: loadMoreBlocked } : {}),
    },
  };
}

function emptyPageResult(page: QueryPageRequest): QueryResultData {
  return {
    columns: [],
    rows: [],
    row_count: page.offset,
    truncated: false,
    page: {
      offset: page.offset,
      size: page.size,
      returned_rows: 0,
      loaded_rows: page.offset,
      has_more: false,
    },
  };
}

function inferTypeFromColumn(rows: BrowserWorkerResultPreview['rows'], idx: number): string {
  for (const row of rows) {
    const value = row[idx];
    if (value == null) continue;
    if (typeof value === 'number') return Number.isInteger(value) ? 'integer' : 'double';
    if (typeof value === 'boolean') return 'boolean';
    return 'string';
  }
  return 'string';
}

function sameColumns(left: ResultColumn[], right: ResultColumn[]): boolean {
  return (
    left.length === right.length &&
    left.every((column, index) => column.name === right[index]?.name)
  );
}

function positiveInteger(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new Error(`${label} must be a positive safe integer`);
  }
  return value;
}

function nonNegativeInteger(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${label} must be a non-negative safe integer`);
  }
  return value;
}

function safeInteger(value: bigint, label: string): number {
  if (value < 0n || value > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new Error(`${label} must be a safe integer`);
  }
  return Number(value);
}
