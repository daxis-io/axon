import { create } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';
import { BrowserHttpSnapshotDescriptorSchema } from '../generated/contracts/protobuf/axon/dataaccess/v1/dataaccess_pb.ts';
import {
  ExecuteRequestSchema,
  ExecutionRejectionReason,
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
  browserReadSnapshotVersion,
  queryPageFromRequest,
  queryResultPageRequest,
  queryResultPageRun,
  queryResultPageRunRequest,
  resolveNextResultPage,
  resultPageFromContractPreview,
  sameQueryResultPageRun,
} from './query-pagination.ts';

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

function resolvedRequest(snapshotVersion: bigint) {
  return create(ExecuteRequestSchema, {
    executionId: 'execution-1',
    binding: {
      case: 'browserRead',
      value: {
        descriptor: {
          descriptor: {
            case: 'snapshot',
            value: create(BrowserHttpSnapshotDescriptorSchema, {
              tableUri: 'gs://public-bucket/events',
              snapshotVersion,
            }),
          },
        },
      },
    },
  });
}

describe('result page snapshot', () => {
  it('reads the snapshot version the first page was resolved at', () => {
    expect(browserReadSnapshotVersion(resolvedRequest(14n))).toBe(14);
  });

  function pageRun(snapshotVersion: number | undefined, readSnapshotVersion?: number) {
    return queryResultPageRun(
      canonicalTableForSelection(selection),
      request(),
      selection,
      snapshotVersion,
      readSnapshotVersion,
    );
  }

  function resolverAt(...versions: bigint[]) {
    const requested: Array<number | undefined> = [];
    const resolve = async (snapshotVersion: number | undefined) => {
      requested.push(snapshotVersion);
      const version = versions[requested.length - 1];
      if (version === undefined) throw new Error('unexpected resolution');
      return { request: resolvedRequest(version) };
    };
    return { requested, resolve };
  }

  it('reuses the latest resolution while latest is still the version the rows came from', async () => {
    const { requested, resolve } = resolverAt(14n);

    const input = await resolveNextResultPage(pageRun(undefined, 14), resolve);

    expect(requested).toEqual([undefined]);
    expect(browserReadSnapshotVersion(input.request)).toBe(14);
  });

  it('requests the rows snapshot explicitly once latest has moved on', async () => {
    const { requested, resolve } = resolverAt(15n, 14n);

    const input = await resolveNextResultPage(pageRun(undefined, 14), resolve);

    expect(requested).toEqual([undefined, 14]);
    expect(browserReadSnapshotVersion(input.request)).toBe(14);
  });

  it('still reads the rows snapshot when latest cannot be resolved', async () => {
    const requested: Array<number | undefined> = [];
    const input = await resolveNextResultPage(pageRun(undefined, 14), async (snapshotVersion) => {
      requested.push(snapshotVersion);
      if (snapshotVersion === undefined) throw new Error('missing checkpoint part');
      return { request: resolvedRequest(14n) };
    });

    expect(requested).toEqual([undefined, 14]);
    expect(browserReadSnapshotVersion(input.request)).toBe(14);
  });

  it('stops at a cancelled or expired latest resolution', async () => {
    for (const error of [
      new DOMException('cancelled', 'AbortError'),
      Object.assign(new Error('deadline'), {
        rejectionReason: ExecutionRejectionReason.DEADLINE_EXPIRED,
      }),
      Object.assign(new Error('cancelled'), {
        rejectionReason: ExecutionRejectionReason.CANCELLED,
      }),
    ]) {
      const requested: Array<number | undefined> = [];
      await expect(
        resolveNextResultPage(pageRun(undefined, 14), async (snapshotVersion) => {
          requested.push(snapshotVersion);
          throw error;
        }),
      ).rejects.toBe(error);
      expect(requested).toEqual([undefined]);
    }
  });

  it('requests a pinned run at its pin directly', async () => {
    const { requested, resolve } = resolverAt(3n);

    await resolveNextResultPage(pageRun(3, 3), resolve);

    expect(requested).toEqual([3]);
  });

  it('fails closed instead of reading another snapshot', async () => {
    const moved = resolverAt(15n, 15n);
    await expect(resolveNextResultPage(pageRun(undefined, 14), moved.resolve)).rejects.toThrow(
      'Snapshot 14, which the loaded rows came from, could not be read again.',
    );

    const unrecorded = resolverAt(3n);
    await expect(resolveNextResultPage(pageRun(3), unrecorded.resolve)).rejects.toThrow(
      'The loaded rows did not record their snapshot. Run the query again.',
    );
    expect(unrecorded.requested).toEqual([]);
  });

  it('reports no version unless the request resolved one exact snapshot', () => {
    expect(browserReadSnapshotVersion(create(ExecuteRequestSchema, {}))).toBeUndefined();
    expect(
      browserReadSnapshotVersion(resolvedRequest(BigInt(Number.MAX_SAFE_INTEGER) + 1n)),
    ).toBeUndefined();
  });
});

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

  it('keeps an unpinned run on the snapshot its first page read', () => {
    const table = canonicalTableForSelection(selection);
    const firstRead = queryResultPageRun(table, request(), selection, undefined, 14);
    const sameRead = queryResultPageRun(table, request(), selection, undefined, 14);
    const newerRead = queryResultPageRun(table, request(), selection, undefined, 15);

    expect(firstRead).toMatchObject({ snapshotVersion: undefined, readSnapshotVersion: 14 });
    expect(sameQueryResultPageRun(firstRead, sameRead)).toBe(true);
    expect(sameQueryResultPageRun(firstRead, newerRead)).toBe(false);
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

    expect(resultPageFromContractPreview(preview, { offset: 10, size: 1 })).toEqual({
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
