import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';
import type { QueryResultPageInfo } from '../../services/types.ts';
import * as ResultsModule from './Results.tsx';
import { Results, resultPageNotice } from './Results.tsx';

type ExternalMemoryPresentation = {
  label: string;
  value: string;
  sub: string;
  title: string;
};

type ResultsTelemetryModule = {
  externalMemoryReservationPresentation?: (
    peakReservationBytes: number,
    workingSetLimitBytes: number,
  ) => ExternalMemoryPresentation;
};

describe('external-memory telemetry presentation', () => {
  it('distinguishes operator reservations from total browser memory', () => {
    const presentation = (ResultsModule as ResultsTelemetryModule)
      .externalMemoryReservationPresentation;

    expect(presentation).toEqual(expect.any(Function));
    if (!presentation) return;

    expect(presentation(120 * 1024 * 1024, 128 * 1024 * 1024)).toEqual({
      label: 'Peak operator reservations',
      value: '120.0 MB',
      sub: '128.0 MB spill watermark · includes spill headroom',
      title:
        'DataFusion-accounted operator reservations. Includes aggregate spill-sort headroom and does not represent total browser memory.',
    });
  });
});

describe('result page notices', () => {
  const pageInfo = (overrides: Partial<QueryResultPageInfo> = {}): QueryResultPageInfo => ({
    offset: 0,
    size: 2,
    returned_rows: 2,
    loaded_rows: 2,
    has_more: true,
    next_offset: 2,
    ...overrides,
  });
  const blocked = (reason: QueryResultPageInfo['load_more_blocked']) =>
    pageInfo({ next_offset: undefined, load_more_blocked: reason });

  // Render the completed Results panel for a two-row page and return its notices.
  function renderedNotices(page: QueryResultPageInfo | undefined) {
    const markup = renderToStaticMarkup(
      createElement(Results, {
        runState: {
          status: 'completed',
          executionId: 'exec-1',
          target: 'browser_wasm',
          ms: 1,
          rows: 2,
        },
        resultData: {
          columns: [{ name: 'id', type: 'int64' }],
          rows: [[1], [2]],
          row_count: 2,
          truncated: false,
          page,
        },
        metrics: undefined,
        events: [],
        history: [],
        serverFallbackEnabled: false,
        plan: undefined,
        commits: [],
        snapshotPin: null,
        tableSnapshot: undefined,
        tableFileCount: undefined,
        tableUri: undefined,
        protocolVersion: undefined,
      }),
    );
    return [
      ...markup.matchAll(
        /<div class="result-pagination-notice" data-testid="([^"]+)">([^<]*)<\/div>/g,
      ),
    ].map(([, testId, text]) => ({ testId, text: text.replaceAll('&#x27;', "'") }));
  }

  it('describes when Load more is offered without contradicting a disabled Load more', () => {
    const notice = resultPageNotice(pageInfo());

    expect(notice?.testId).toBe('result-pagination-notice');
    expect(notice?.text).toContain(
      'Loading more recomputes the original query until streaming results are available.',
    );
    expect(notice?.text).toContain('only when the next page is known to continue the loaded rows');
    expect(notice?.text).not.toContain('Use an explicit ORDER BY');
    expect(resultPageNotice(pageInfo({ has_more: false, next_offset: undefined }))).toBeUndefined();
    expect(
      resultPageNotice(pageInfo({ loaded_rows: 4, has_more: false, next_offset: undefined })),
    ).toEqual(notice);
    expect(resultPageNotice(undefined)).toBeUndefined();
  });

  it('explains a tie at the page boundary apart from an order the page cannot check', () => {
    const tie = resultPageNotice(blocked('order_by_tie'));
    const unverifiable = resultPageNotice(blocked('unverifiable_order'));
    const unknown = resultPageNotice(blocked('unknown_order'));

    for (const notice of [tie, unverifiable, unknown]) {
      expect(notice?.testId).toBe('result-load-more-blocked');
      expect(notice?.text).toMatch(/^Load more is off/);
    }
    expect(tie?.text).toContain('the next row may tie with the last loaded row');
    expect(tie?.text).toContain(
      'Add a unique column to the end of ORDER BY to page through every row',
    );

    expect(unverifiable?.text).not.toMatch(/\btie/);
    expect(unverifiable?.text).not.toContain('Add a unique column to the end of ORDER BY');
    expect(unverifiable?.text).toContain('cannot check across re-run pages');
    expect(unverifiable?.text).toContain('SELECT-list column names or positions');
    expect(unverifiable?.text).toContain('qualified names such as e.id');
    expect(unverifiable?.text).toContain('no window with PARTITION BY or ORDER BY');
    expect(unverifiable?.text).toContain('no DISTINCT ON');
    expect(unverifiable?.text).toContain('no ORDER BY in a subquery');
    expect(unverifiable?.text).toContain('LIMIT and OFFSET in SQL');

    expect(unknown?.text).toContain('the runtime did not report how these rows are ordered');
    expect(new Set([tie?.text, unverifiable?.text, unknown?.text]).size).toBe(3);

    // A block only applies while more rows exist.
    expect(
      resultPageNotice({ ...blocked('order_by_tie'), loaded_rows: 4, has_more: false }),
    ).toEqual(resultPageNotice(pageInfo()));
  });

  it('shows one notice at a time in the results panel', () => {
    expect(renderedNotices(pageInfo())).toEqual([resultPageNotice(pageInfo())]);
    for (const reason of ['order_by_tie', 'unverifiable_order', 'unknown_order'] as const) {
      expect(renderedNotices(blocked(reason)), reason).toEqual([resultPageNotice(blocked(reason))]);
      expect(
        renderedNotices({ ...blocked(reason), loaded_rows: 4, offset: 0 }),
        `${reason} after Load more`,
      ).toEqual([resultPageNotice(blocked(reason))]);
    }
    expect(renderedNotices(pageInfo({ has_more: false, next_offset: undefined }))).toEqual([]);
    expect(renderedNotices(undefined)).toEqual([]);
  });
});
