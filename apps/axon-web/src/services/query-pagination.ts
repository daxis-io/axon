import type { BrowserWorkerResultPreview, QueryResultPageRequest } from '../axon-browser-sdk.ts';
import type {
  QueryExecRequest,
  QueryPageRequest,
  QueryResultData,
  ResultCell,
  ResultColumn,
} from './types.ts';
import { sameQuerySource, type QueryTableSource } from './query-source.ts';

export const QUERY_RESULT_PAGE_SIZE = 500;
export const MAX_QUERY_RESULT_PAGE_LIMIT = QUERY_RESULT_PAGE_SIZE + 1;

export type QueryResultPageRun = {
  request: Omit<QueryExecRequest, 'page'>;
  source: QueryTableSource;
};

export function queryResultPageRun(
  request: QueryExecRequest,
  source: QueryTableSource,
): QueryResultPageRun {
  return {
    request: {
      sql: request.sql,
      table_name: request.table_name,
      preferred_target: request.preferred_target,
      snapshot_version: request.snapshot_version,
    },
    source,
  };
}

export function sameQueryResultPageRun(
  left: QueryResultPageRun,
  right: QueryResultPageRun,
): boolean {
  return (
    sameQuerySource(left.source, right.source) &&
    left.request.sql === right.request.sql &&
    left.request.table_name === right.request.table_name &&
    left.request.preferred_target === right.request.preferred_target &&
    (left.request.snapshot_version ?? null) === (right.request.snapshot_version ?? null)
  );
}

export function queryResultPageRunRequest(
  run: QueryResultPageRun,
  page: QueryPageRequest,
): QueryExecRequest {
  return { ...run.request, page };
}

export function defaultQueryPage(): QueryPageRequest {
  return { offset: 0, size: QUERY_RESULT_PAGE_SIZE };
}

export function queryResultPageRequest(page: QueryPageRequest): QueryResultPageRequest {
  const offset = nonNegativeInteger(page.offset, 'query result page offset');
  const size = positiveInteger(page.size, 'query result page size');
  if (size >= Number.MAX_SAFE_INTEGER) {
    throw new Error('query result page size is too large to request a sentinel row');
  }
  if (size + 1 > MAX_QUERY_RESULT_PAGE_LIMIT) {
    throw new Error(`query result page size ${size} exceeds maximum ${QUERY_RESULT_PAGE_SIZE}`);
  }
  return { limit: size + 1, offset };
}

export function resultPageFromPreview(
  preview: BrowserWorkerResultPreview | undefined,
  page: QueryPageRequest,
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
      next_offset: hasMore ? loadedRows : undefined,
    },
  };
}

export function appendResultPage(current: QueryResultData, next: QueryResultData): QueryResultData {
  if (!sameColumns(current.columns, next.columns)) {
    return next;
  }

  const rows = [...current.rows, ...next.rows];
  const loadedRows = rows.length;
  const hasMore = next.page?.has_more === true;
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
      next_offset: hasMore ? loadedRows : undefined,
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
