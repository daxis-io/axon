import { expect, test, type Browser, type Response } from '@playwright/test';
import { createHash } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';

const BASE_SHA = 'd1a31ec22479bb7d2fb380bfd61e00fd2f7881e8';
const SQL =
  'SELECT COUNT(*) AS row_count, SUM(event_id) AS event_id_sum, ' +
  'SUM(LENGTH(payload)) AS payload_length_sum ' +
  'FROM page_index_events WHERE event_id >= 63488';
const ORDER = ['skip', 'predicate', 'predicate', 'skip', 'skip', 'predicate'] as const;
const JAVASCRIPT_MAX_SAFE_INTEGER = Number.MAX_SAFE_INTEGER;

type Arm = (typeof ORDER)[number];

type Extent = {
  offset_bytes: number;
  length_bytes: number;
};

type ColumnExtent = Extent & {
  column: string;
};

type DataPageExtent = ColumnExtent & {
  page_index: number;
  first_row_index: number;
  row_count: number;
  predicate_match: boolean;
};

type FixtureManifest = {
  schema_version: number;
  fixture_revision: string;
  url_path: string;
  size_bytes: number;
  seed: string;
  row_count: number;
  row_group_count: number;
  page_row_count_limit: number;
  predicate: string;
  expected_row_count: number;
  expected_event_id_sum: number;
  expected_payload_length_sum: number;
  expected_pages_selected: number;
  expected_pages_skipped: number;
  footer_extent: Extent;
  column_index_extents: ColumnExtent[];
  offset_index_extents: ColumnExtent[];
  data_page_extents: DataPageExtent[];
  sha256: string;
};

type PhysicalRequest = {
  range: string | null;
  offset_bytes: number;
  length_bytes: number;
  status: number;
  content_range: string;
  content_length: number;
};

type RawRun = {
  repetition: number;
  order_index: number;
  arm: Arm;
  policy: {
    column_index: 'skip' | 'optional';
    offset_index: 'skip' | 'optional';
    row_selection: 'disabled' | 'predicate';
  };
  result: {
    schema: string[];
    row_count: number;
    event_id_sum: number;
    payload_length_sum: number;
    checksum: string;
  };
  runtime: {
    executed_on: string;
    fallback_reason: string | null;
    ipc_bytes: number;
    ipc_chunks: number;
    coordinator_reserved_bytes: number;
    coordinator_limit_bytes: number;
    coordinator_staged_bytes: number;
    coordinator_peak_reserved_bytes: number;
    coordinator_peak_staged_bytes: number;
    datafusion_reserved_bytes: number;
    datafusion_limit_bytes: number;
    datafusion_peak_bytes: number;
  };
  telemetry: Record<string, number>;
  physical: {
    total_bytes: number;
    total_requests: number;
    footer_bytes: number;
    footer_requests: number;
    page_index_bytes: number;
    page_index_requests: number;
    column_index_bytes: number;
    offset_index_bytes: number;
    scan_data_bytes: number;
    scan_data_requests: number;
    unclassified_bytes: number;
    pages_selected: number;
    pages_skipped: number;
    pages_touched: number;
    files_selected: number;
    files_skipped: number;
    row_groups_selected: number;
    row_groups_skipped: number;
    cache_disabled: boolean;
    service_workers_blocked: boolean;
    cache_served_responses: number;
    requests: PhysicalRequest[];
  };
};

test('records real browser-Wasm page-index byte-savings A/B evidence', async ({
  browser,
  baseURL,
}) => {
  if (!baseURL) throw new Error('page-index A/B requires a Playwright base URL');
  const manifest = JSON.parse(
    await readFile(resolve('public/fixtures/prod-like/page-index-ab/manifest.json'), 'utf8'),
  ) as FixtureManifest;
  validateManifest(manifest);
  const fixtureBytes = await readFile(
    resolve('public/fixtures/prod-like/page-index-ab/event-id.parquet'),
  );
  const fixtureSha256 = createHash('sha256').update(fixtureBytes).digest('hex');
  expect(fixtureBytes.byteLength).toBe(manifest.size_bytes);
  expect(fixtureSha256).toBe(manifest.sha256);

  const runs: RawRun[] = [];
  const repetitions: Record<Arm, number> = { skip: 0, predicate: 0 };
  for (const [orderIndex, arm] of ORDER.entries()) {
    repetitions[arm] += 1;
    runs.push(await runArm(browser, baseURL, manifest, arm, repetitions[arm], orderIndex));
  }

  const skipRuns = runs.filter((run) => run.arm === 'skip');
  const predicateRuns = runs.filter((run) => run.arm === 'predicate');
  for (const run of runs) validateRun(run, manifest);
  expect(new Set(runs.map((run) => run.result.checksum))).toEqual(
    new Set([runs[0]?.result.checksum]),
  );

  const grossDataPageBytesAvoided =
    Math.min(...skipRuns.map((run) => run.physical.scan_data_bytes)) -
    Math.max(...predicateRuns.map((run) => run.physical.scan_data_bytes));
  const netPhysicalSavings =
    Math.min(...skipRuns.map((run) => run.physical.total_bytes)) -
    Math.max(...predicateRuns.map((run) => run.physical.total_bytes));
  const maxIndexOverhead = Math.max(...predicateRuns.map((run) => run.physical.page_index_bytes));
  const gates = {
    exact_result_parity: new Set(runs.map((run) => run.result.checksum)).size === 1,
    browser_wasm_no_fallback: runs.every(
      (run) => run.runtime.executed_on === 'browser_wasm' && run.runtime.fallback_reason === null,
    ),
    terminal_ownership_zero: runs.every(
      (run) =>
        run.runtime.coordinator_reserved_bytes === 0 &&
        run.runtime.coordinator_staged_bytes === 0 &&
        run.runtime.datafusion_reserved_bytes === 0,
    ),
    owned_memory_within_limits: runs.every(memoryWithinLimits),
    cursor_within_limits: runs.every(
      (run) =>
        run.telemetry.cursor_peak_pending_encoded_bytes <=
          run.telemetry.coordinator_staging_limit_bytes &&
        run.telemetry.cursor_peak_transport_chunk_bytes <= 1_048_576,
    ),
    cold_cache_confirmed: runs.every(
      (run) =>
        run.physical.cache_disabled &&
        run.physical.service_workers_blocked &&
        run.physical.cache_served_responses === 0 &&
        run.telemetry.footer_cache_hits === 0 &&
        run.telemetry.range_cache_hits === 0 &&
        run.telemetry.range_cache_bytes_reused === 0 &&
        run.telemetry.range_cache_bytes_stored === 0,
    ),
    no_readahead_or_overfetch: runs.every(
      (run) =>
        run.telemetry.scan_overfetch_bytes === 0 &&
        run.telemetry.range_readahead_requests === 0 &&
        run.telemetry.range_readahead_bytes_fetched === 0 &&
        run.telemetry.range_readahead_bytes_used === 0 &&
        run.telemetry.range_readahead_wasted_bytes === 0,
    ),
    no_coalescing_gap: runs.every((run) => run.telemetry.coalesced_gap_bytes_fetched === 0),
    page_selection_observed: predicateRuns.every(
      (run) => run.physical.pages_selected > 0 && run.physical.pages_skipped > 0,
    ),
    response_grounded_bytes: runs.every((run) =>
      run.physical.requests.every(
        (request) =>
          request.status === 206 &&
          request.content_length === request.length_bytes &&
          request.content_range.length > 0,
      ),
    ),
  };
  const positive =
    Object.values(gates).every(Boolean) &&
    grossDataPageBytesAvoided > 0 &&
    netPhysicalSavings > 0 &&
    predicateRuns.every(
      (run) =>
        run.physical.pages_selected > 0 &&
        run.physical.pages_skipped > 0 &&
        run.physical.scan_data_bytes <
          Math.min(...skipRuns.map((skipRun) => skipRun.physical.scan_data_bytes)) &&
        run.physical.total_bytes <
          Math.min(...skipRuns.map((skipRun) => skipRun.physical.total_bytes)),
    );
  const decision = positive
    ? 'positive_local_browser_byte_savings_keep_default_off'
    : 'nonpositive_or_missed_workload_keep_default_off';
  const evidence = {
    schema_version: 1,
    generated_at: new Date().toISOString(),
    base_sha: BASE_SHA,
    scope: 'local_deterministic_browser_wasm',
    fixture: {
      revision: manifest.fixture_revision,
      sha256: fixtureSha256,
      size_bytes: manifest.size_bytes,
      seed: manifest.seed,
      row_count: manifest.row_count,
      row_group_count: manifest.row_group_count,
      page_row_count_limit: manifest.page_row_count_limit,
      column_indexes: manifest.column_index_extents.length,
      offset_indexes: manifest.offset_index_extents.length,
    },
    sql: SQL,
    execution_order: ORDER,
    policies: {
      skip: {
        column_index: 'skip',
        offset_index: 'skip',
        row_selection: 'disabled',
      },
      predicate: {
        column_index: 'optional',
        offset_index: 'optional',
        row_selection: 'predicate',
      },
      cache: 'fresh_browser_context_and_worker_per_run',
      browser_http_cache: 'disabled_via_cdp_and_verified_no_cache_served_responses',
      service_workers: 'blocked',
      coalescing: 'unchanged_production_policy',
      readahead: 'unchanged_production_policy',
    },
    raw_runs: runs,
    aggregates: {
      skip: aggregate(skipRuns),
      predicate: aggregate(predicateRuns),
      conservative_gross_data_page_bytes_avoided: grossDataPageBytesAvoided,
      conservative_max_page_index_overhead_bytes: maxIndexOverhead,
      conservative_net_physical_savings_bytes: netPhysicalSavings,
    },
    decision: {
      outcome: decision,
      enable_by_default: false,
      public_s3_evidence: false,
      gates,
      rationale: positive
        ? 'Every fresh predicate run preserved exact results, skipped pages, avoided data bytes, and remained below every skip run after index overhead.'
        : 'The local browser experiment did not satisfy every positive byte-savings gate.',
    },
  };
  const serialized = `${JSON.stringify(evidence, null, 2)}\n`;
  expect(evidenceContainsSecret(serialized)).toBe(false);
  const evidencePath = resolve('../../target/perf/page-index-byte-savings-ab-evidence.json');
  await mkdir(dirname(evidencePath), { recursive: true });
  await writeFile(evidencePath, serialized, 'utf8');
  const evidenceSha256 = createHash('sha256').update(serialized).digest('hex');
  await writeFile(`${evidencePath}.sha256`, `${evidenceSha256}  ${evidencePath}\n`, 'utf8');
  await test.info().attach('page-index-byte-savings-ab-evidence', {
    body: serialized,
    contentType: 'application/json',
  });

  expect(grossDataPageBytesAvoided).toBeGreaterThan(0);
  expect(maxIndexOverhead).toBeGreaterThan(0);
  expect(netPhysicalSavings).toBeGreaterThan(0);
  expect(decision).toBe('positive_local_browser_byte_savings_keep_default_off');
});

test('rejects invalid byte ranges and credential-shaped evidence', () => {
  expect(parseRangeHeader('bytes=0-9', 100)).toEqual({
    range: 'bytes=0-9',
    offset_bytes: 0,
    length_bytes: 10,
  });
  expect(parseRangeHeader('bytes=90-', 100).length_bytes).toBe(10);
  expect(parseRangeHeader('bytes=-10', 100).offset_bytes).toBe(90);
  for (const range of ['bytes=10-9', 'bytes=100-', 'bytes=-0', 'bytes=-101', 'items=0-9']) {
    expect(() => parseRangeHeader(range, 100)).toThrow();
  }
  expect(evidenceContainsSecret('https://user:password@example.invalid/file')).toBe(true);
  expect(evidenceContainsSecret('key=AKIAABCDEFGHIJKLMNOP')).toBe(true);
  expect(evidenceContainsSecret('key=ASIAABCDEFGHIJKLMNOP')).toBe(true);
  expect(evidenceContainsSecret('{"scope":"local_deterministic_browser_wasm"}')).toBe(false);
});

async function runArm(
  browser: Browser,
  baseURL: string,
  manifest: FixtureManifest,
  arm: Arm,
  repetition: number,
  orderIndex: number,
): Promise<RawRun> {
  const context = await browser.newContext({
    ignoreHTTPSErrors: true,
    serviceWorkers: 'block',
  });
  const page = await context.newPage();
  const cdp = await context.newCDPSession(page);
  await cdp.send('Network.enable');
  await cdp.send('Network.setCacheDisabled', { cacheDisabled: true });
  const fixtureRequestIds = new Set<string>();
  const cacheServedRequestIds = new Set<string>();
  cdp.on('Network.requestWillBeSent', (event) => {
    if (new URL(event.request.url).pathname === manifest.url_path) {
      fixtureRequestIds.add(event.requestId);
    }
  });
  cdp.on('Network.requestServedFromCache', (event) => {
    cacheServedRequestIds.add(event.requestId);
  });
  const responseCaptures: Array<Promise<PhysicalRequest | undefined>> = [];
  page.on('response', (response) => {
    responseCaptures.push(captureFixtureResponse(response, manifest));
  });
  try {
    await page.goto(baseURL);
    const observation = await page.evaluate(
      async ({ arm, manifest, sql, repetition }) => {
        const sdk = await import(new URL('/src/axon-browser-sdk.ts', location.href).href);
        const runtimeEvents: Array<{
          owned_memory_metrics?: {
            context?: { request_id?: string };
            coordinator?: Record<string, number>;
            datafusion?: Record<string, number>;
          };
        }> = [];
        const workerUrl = new URL('/src/sandbox-query-worker.ts', location.href);
        if (arm === 'predicate') workerUrl.searchParams.set('page_index_policy', 'predicate');
        const worker = new Worker(workerUrl, {
          type: 'module',
          name: `page-index-ab-${arm}-${repetition}`,
        });
        const client = sdk.createAxonBrowserClient({
          worker,
          onEvent: (event: unknown) => runtimeEvents.push(event as (typeof runtimeEvents)[number]),
        });
        try {
          await client.openParquetDataset(
            'page_index_events',
            {
              table_uri: new URL('/fixtures/prod-like/page-index-ab', location.href).href,
              partition_column_types: {},
              browser_compatibility: { capabilities: {} },
              required_capabilities: { capabilities: {} },
              files: [
                {
                  path: 'event-id.parquet',
                  url: new URL(manifest.url_path, location.href).href,
                  size_bytes: manifest.size_bytes,
                  partition_values: {},
                },
              ],
            },
            { requestId: `page-index-open-${arm}-${repetition}` },
          );
          const requestId = `page-index-query-${arm}-${repetition}`;
          const result = await client.query('page_index_events', sql, {
            requestId,
            preferredTarget: 'browser_wasm',
            queryOptions: { collect_metrics: true },
          });
          const memory = runtimeEvents.findLast(
            (event) => event.owned_memory_metrics?.context?.request_id === requestId,
          )?.owned_memory_metrics;
          if (!memory?.coordinator || !memory.datafusion) {
            throw new Error(`run ${arm}/${repetition} omitted owned-memory telemetry`);
          }
          if (!result.preview || result.preview.rows.length !== 1) {
            throw new Error(`run ${arm}/${repetition} omitted its scalar preview`);
          }
          const [rowCount, eventIdSum, payloadLengthSum] = result.preview.rows[0] ?? [];
          return {
            schema: result.preview.columns,
            values: [rowCount, eventIdSum, payloadLengthSum],
            executedOn: result.response.executed_on,
            fallbackReason: result.response.fallback_reason ?? result.fallbackReason ?? null,
            ipcBytes: result.result.byte_length,
            ipcChunks: result.result.chunk_count,
            metrics: result.response.metrics,
            memory: {
              coordinatorReservedBytes: memory.coordinator.reserved_bytes,
              coordinatorLimitBytes: memory.coordinator.limit_bytes,
              coordinatorStagedBytes: memory.coordinator.staged_bytes,
              coordinatorPeakReservedBytes: memory.coordinator.peak_reserved_bytes,
              coordinatorPeakStagedBytes: memory.coordinator.peak_staged_bytes,
              datafusionReservedBytes: memory.datafusion.reserved_bytes,
              datafusionLimitBytes: memory.datafusion.limit_bytes,
              datafusionPeakBytes: memory.datafusion.peak_bytes,
            },
          };
        } finally {
          client.terminate();
        }
      },
      { arm, manifest, sql: SQL, repetition },
    );
    const [rowCount, eventIdSum, payloadLengthSum] = observation.values.map((value) =>
      requireResultSafeInteger(value, 'result scalar'),
    );
    const resultShape = {
      schema: observation.schema,
      row_count: rowCount,
      event_id_sum: eventIdSum,
      payload_length_sum: payloadLengthSum,
    };
    await page.waitForTimeout(0);
    const physicalRequests = (await Promise.all(responseCaptures)).filter(
      (request): request is PhysicalRequest => request !== undefined,
    );
    const cacheServedResponses = [...cacheServedRequestIds].filter((requestId) =>
      fixtureRequestIds.has(requestId),
    ).length;
    const classified = classifyPhysicalRequests(physicalRequests, manifest, arm);
    const telemetry = numericTelemetry(observation.metrics);
    return {
      repetition,
      order_index: orderIndex,
      arm,
      policy:
        arm === 'skip'
          ? {
              column_index: 'skip',
              offset_index: 'skip',
              row_selection: 'disabled',
            }
          : {
              column_index: 'optional',
              offset_index: 'optional',
              row_selection: 'predicate',
            },
      result: {
        ...resultShape,
        checksum: createHash('sha256').update(JSON.stringify(resultShape)).digest('hex'),
      },
      runtime: {
        executed_on: observation.executedOn,
        fallback_reason: observation.fallbackReason,
        ipc_bytes: requireSafeInteger(observation.ipcBytes, 'ipc_bytes'),
        ipc_chunks: requireSafeInteger(observation.ipcChunks, 'ipc_chunks'),
        coordinator_reserved_bytes: requireSafeInteger(
          observation.memory.coordinatorReservedBytes,
          'coordinator_reserved_bytes',
        ),
        coordinator_limit_bytes: requireSafeInteger(
          observation.memory.coordinatorLimitBytes,
          'coordinator_limit_bytes',
        ),
        coordinator_staged_bytes: requireSafeInteger(
          observation.memory.coordinatorStagedBytes,
          'coordinator_staged_bytes',
        ),
        coordinator_peak_reserved_bytes: requireSafeInteger(
          observation.memory.coordinatorPeakReservedBytes,
          'coordinator_peak_reserved_bytes',
        ),
        coordinator_peak_staged_bytes: requireSafeInteger(
          observation.memory.coordinatorPeakStagedBytes,
          'coordinator_peak_staged_bytes',
        ),
        datafusion_reserved_bytes: requireSafeInteger(
          observation.memory.datafusionReservedBytes,
          'datafusion_reserved_bytes',
        ),
        datafusion_limit_bytes: requireSafeInteger(
          observation.memory.datafusionLimitBytes,
          'datafusion_limit_bytes',
        ),
        datafusion_peak_bytes: requireSafeInteger(
          observation.memory.datafusionPeakBytes,
          'datafusion_peak_bytes',
        ),
      },
      telemetry,
      physical: {
        ...classified,
        cache_disabled: true,
        service_workers_blocked: true,
        cache_served_responses: cacheServedResponses,
      },
    };
  } finally {
    await cdp.detach();
    await context.close();
  }
}

async function captureFixtureResponse(
  response: Response,
  manifest: FixtureManifest,
): Promise<PhysicalRequest | undefined> {
  const request = response.request();
  if (request.method() !== 'GET' || new URL(request.url()).pathname !== manifest.url_path) {
    return undefined;
  }
  const responseError = await response.finished();
  if (responseError) {
    throw new Error(`fixture response failed: ${responseError}`);
  }
  expect(response.status()).toBe(206);
  const range = request.headers().range ?? null;
  const parsed = parseRangeHeader(range, manifest.size_bytes);
  const expectedEnd = parsed.offset_bytes + parsed.length_bytes - 1;
  const expectedContentRange = `bytes ${parsed.offset_bytes}-${expectedEnd}/${manifest.size_bytes}`;
  const headers = response.headers();
  expect(headers['content-range']).toBe(expectedContentRange);
  expect(requireSafeInteger(Number(headers['content-length']), 'Content-Length')).toBe(
    parsed.length_bytes,
  );
  return {
    ...parsed,
    status: response.status(),
    content_range: expectedContentRange,
    content_length: parsed.length_bytes,
  };
}

function parseRangeHeader(
  range: string | null,
  objectSize: number,
): Pick<PhysicalRequest, 'range' | 'offset_bytes' | 'length_bytes'> {
  requireSafeInteger(objectSize, 'object size');
  if (objectSize === 0) throw new Error('object size must be positive');
  if (range === null) {
    return { range, offset_bytes: 0, length_bytes: objectSize };
  }
  const match = /^bytes=(\d*)-(\d*)$/.exec(range);
  if (!match) throw new Error(`unsupported fixture Range header '${range}'`);
  let start: number;
  let end: number;
  if (match[1] === '') {
    const suffix = requireSafeInteger(Number(match[2]), 'suffix range');
    if (suffix === 0 || suffix > objectSize) {
      throw new Error(`suffix range '${range}' exceeds object size ${objectSize}`);
    }
    start = objectSize - suffix;
    end = objectSize - 1;
  } else {
    start = requireSafeInteger(Number(match[1]), 'range start');
    end = match[2] === '' ? objectSize - 1 : requireSafeInteger(Number(match[2]), 'range end');
  }
  if (start >= objectSize || end >= objectSize || end < start) {
    throw new Error(`range '${range}' is invalid for object size ${objectSize}`);
  }
  return {
    range,
    offset_bytes: start,
    length_bytes: end - start + 1,
  };
}

function classifyPhysicalRequests(
  requests: PhysicalRequest[],
  manifest: FixtureManifest,
  arm: Arm,
): Omit<
  RawRun['physical'],
  'cache_disabled' | 'service_workers_blocked' | 'cache_served_responses'
> {
  const indexExtents = [...manifest.column_index_extents, ...manifest.offset_index_extents];
  const eventPages = manifest.data_page_extents.filter((page) => page.column === 'event_id');
  const sumIntersections = (extents: Extent[]) =>
    requests.reduce((total, request) => total + unionIntersectionBytes(request, extents), 0);
  const requestCount = (extents: Extent[]) =>
    requests.filter((request) => unionIntersectionBytes(request, extents) > 0).length;
  const totalBytes = requests.reduce((total, request) => total + request.length_bytes, 0);
  const footerBytes = sumIntersections([manifest.footer_extent]);
  const columnIndexBytes = sumIntersections(manifest.column_index_extents);
  const offsetIndexBytes = sumIntersections(manifest.offset_index_extents);
  const scanDataBytes = sumIntersections(manifest.data_page_extents);
  const touchedEventPages = eventPages.filter((page) =>
    requests.some((request) => unionIntersectionBytes(request, [page]) > 0),
  );
  const observedPredicateSelection =
    arm === 'predicate' &&
    touchedEventPages.length > 0 &&
    touchedEventPages.every((page) => page.predicate_match);
  return {
    total_bytes: totalBytes,
    total_requests: requests.length,
    footer_bytes: footerBytes,
    footer_requests: requestCount([manifest.footer_extent]),
    page_index_bytes: sumIntersections(indexExtents),
    page_index_requests: requestCount(indexExtents),
    column_index_bytes: columnIndexBytes,
    offset_index_bytes: offsetIndexBytes,
    scan_data_bytes: scanDataBytes,
    scan_data_requests: requestCount(manifest.data_page_extents),
    unclassified_bytes:
      totalBytes - footerBytes - columnIndexBytes - offsetIndexBytes - scanDataBytes,
    pages_selected: observedPredicateSelection ? touchedEventPages.length : 0,
    pages_skipped: observedPredicateSelection ? eventPages.length - touchedEventPages.length : 0,
    pages_touched: touchedEventPages.length,
    files_selected: 1,
    files_skipped: 0,
    row_groups_selected: 1,
    row_groups_skipped: 0,
    requests,
  };
}

function unionIntersectionBytes(request: Extent, extents: Extent[]): number {
  const requestStart = request.offset_bytes;
  const requestEnd = requestStart + request.length_bytes;
  const intersections = extents
    .map(
      (extent) =>
        [
          Math.max(requestStart, extent.offset_bytes),
          Math.min(requestEnd, extent.offset_bytes + extent.length_bytes),
        ] as const,
    )
    .filter(([start, end]) => end > start)
    .sort(([left], [right]) => left - right);
  let total = 0;
  let cursorStart = -1;
  let cursorEnd = -1;
  for (const [start, end] of intersections) {
    if (start > cursorEnd) {
      total += Math.max(0, cursorEnd - cursorStart);
      cursorStart = start;
      cursorEnd = end;
    } else {
      cursorEnd = Math.max(cursorEnd, end);
    }
  }
  return total + Math.max(0, cursorEnd - cursorStart);
}

function validateManifest(manifest: FixtureManifest): void {
  expect(manifest.schema_version).toBe(1);
  expect(manifest.fixture_revision).toBe('local-page-index-ab-v1');
  expect(manifest.row_group_count).toBe(1);
  expect(manifest.column_index_extents).toHaveLength(2);
  expect(manifest.offset_index_extents).toHaveLength(2);
  expect(manifest.expected_pages_selected).toBeGreaterThan(0);
  expect(manifest.expected_pages_skipped).toBeGreaterThan(0);
  for (const value of [
    manifest.size_bytes,
    manifest.row_count,
    manifest.row_group_count,
    manifest.page_row_count_limit,
    manifest.expected_row_count,
    manifest.expected_event_id_sum,
    manifest.expected_payload_length_sum,
  ]) {
    requireSafeInteger(value, 'fixture manifest');
  }
}

function validateRun(run: RawRun, manifest: FixtureManifest): void {
  expect(run.result.schema).toEqual(['row_count', 'event_id_sum', 'payload_length_sum']);
  expect(run.result.row_count).toBe(manifest.expected_row_count);
  expect(run.result.event_id_sum).toBe(manifest.expected_event_id_sum);
  expect(run.result.payload_length_sum).toBe(manifest.expected_payload_length_sum);
  expect(run.runtime.executed_on).toBe('browser_wasm');
  expect(run.runtime.fallback_reason).toBeNull();
  expect(run.runtime.coordinator_reserved_bytes).toBe(0);
  expect(run.runtime.coordinator_staged_bytes).toBe(0);
  expect(run.runtime.datafusion_reserved_bytes).toBe(0);
  expect(memoryWithinLimits(run)).toBe(true);
  expect(run.physical.cache_disabled).toBe(true);
  expect(run.physical.service_workers_blocked).toBe(true);
  expect(run.physical.cache_served_responses).toBe(0);
  expect(run.telemetry.footer_cache_hits).toBe(0);
  expect(run.telemetry.range_cache_hits).toBe(0);
  expect(run.telemetry.range_cache_bytes_reused).toBe(0);
  expect(run.telemetry.range_cache_bytes_stored).toBe(0);
  expect(run.telemetry.scan_overfetch_bytes).toBe(0);
  expect(run.telemetry.coalesced_gap_bytes_fetched).toBe(0);
  expect(run.telemetry.range_readahead_requests).toBe(0);
  expect(run.telemetry.range_readahead_bytes_fetched).toBe(0);
  expect(run.telemetry.range_readahead_wasted_bytes).toBe(0);
  expect(run.physical.total_requests).toBeGreaterThan(0);
  expect(run.physical.footer_bytes).toBeGreaterThan(0);
  expect(run.physical.scan_data_bytes).toBeGreaterThan(0);
  if (run.arm === 'predicate') {
    expect(run.physical.page_index_bytes).toBeGreaterThan(0);
    expect(run.physical.pages_selected).toBe(manifest.expected_pages_selected);
    expect(run.physical.pages_skipped).toBe(manifest.expected_pages_skipped);
    expect(run.physical.pages_touched).toBe(manifest.expected_pages_selected);
  } else {
    expect(run.physical.page_index_bytes).toBe(0);
    expect(run.physical.pages_touched).toBe(
      manifest.expected_pages_selected + manifest.expected_pages_skipped,
    );
  }
}

function memoryWithinLimits(run: RawRun): boolean {
  return (
    run.runtime.coordinator_peak_reserved_bytes <= run.runtime.coordinator_limit_bytes &&
    run.runtime.coordinator_peak_staged_bytes <= run.runtime.coordinator_limit_bytes &&
    run.runtime.coordinator_peak_staged_bytes <= run.telemetry.coordinator_staging_limit_bytes &&
    run.runtime.datafusion_peak_bytes <= run.runtime.datafusion_limit_bytes
  );
}

function evidenceContainsSecret(serialized: string): boolean {
  return (
    /(?:X-Amz-|AWSAccessKeyId|aws_secret_access_key|session[_-]?token|authorization["':\s]|[?&](?:token|signature|credential)=)/i.test(
      serialized,
    ) ||
    /\b(?:AKIA|ASIA)[A-Z0-9]{16}\b/.test(serialized) ||
    /https?:\/\/[^/\s:@]+:[^@\s/]+@/i.test(serialized)
  );
}

function numericTelemetry(metrics: Record<string, unknown>): Record<string, number> {
  const required = [
    'bytes_fetched',
    'footer_reads',
    'bootstrap_footer_range_reads',
    'scan_footer_range_reads',
    'scan_data_range_reads',
    'duplicate_range_reads',
    'coalesced_range_reads',
    'coalesced_gap_bytes_fetched',
    'scan_overfetch_bytes',
    'footer_cache_hits',
    'footer_cache_misses',
    'footer_range_reads_avoided',
    'range_cache_hits',
    'range_cache_misses',
    'range_cache_bytes_reused',
    'range_cache_bytes_stored',
    'range_readahead_requests',
    'range_readahead_bytes_fetched',
    'range_readahead_bytes_used',
    'range_readahead_wasted_bytes',
    'rows_emitted',
    'arrow_ipc_bytes',
    'arrow_ipc_chunk_count',
    'coordinator_peak_staged_bytes',
    'coordinator_staging_limit_bytes',
    'cursor_peak_pending_encoded_bytes',
    'cursor_peak_transport_chunk_bytes',
  ];
  return Object.fromEntries(
    required.map((field) => [field, requireSafeInteger(metrics[field], `metrics.${field}`)]),
  );
}

function requireSafeInteger(value: unknown, field: string): number {
  if (
    typeof value !== 'number' ||
    !Number.isFinite(value) ||
    !Number.isSafeInteger(value) ||
    value < 0 ||
    value > JAVASCRIPT_MAX_SAFE_INTEGER
  ) {
    throw new Error(`${field} was not a finite nonnegative safe integer: ${String(value)}`);
  }
  return value;
}

function requireResultSafeInteger(value: unknown, field: string): number {
  if (typeof value === 'string' && /^\d+$/.test(value)) {
    return requireSafeInteger(Number(value), field);
  }
  return requireSafeInteger(value, field);
}

function aggregate(runs: RawRun[]) {
  const metrics = [
    'total_bytes',
    'total_requests',
    'footer_bytes',
    'footer_requests',
    'page_index_bytes',
    'page_index_requests',
    'scan_data_bytes',
    'scan_data_requests',
    'pages_selected',
    'pages_skipped',
    'pages_touched',
  ] as const;
  return Object.fromEntries(
    metrics.map((metric) => {
      const values = runs.map((run) => run.physical[metric]);
      return [
        metric,
        {
          min: Math.min(...values),
          max: Math.max(...values),
          mean: values.reduce((total, value) => total + value, 0) / values.length,
        },
      ];
    }),
  );
}
