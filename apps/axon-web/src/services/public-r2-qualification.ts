export type PublicR2PerformanceQualification = {
  row_count: number;
  first_event_id: string;
  last_event_id: string;
  result_sha256: string;
  columns: string[];
};

export type PublicR2OnboardingQualification = {
  row_count: number;
  result_sha256: string;
  columns: string[];
};

export type PublicR2BrowserQueryEvidence = {
  metrics: {
    bytes_fetched: number;
    scan_data_range_reads: number;
    rows_emitted: number;
    arrow_ipc_bytes: number;
    coordinator_peak_staged_bytes: number;
    coordinator_staging_limit_bytes: number;
    cursor_peak_pending_encoded_bytes: number;
    cursor_peak_transport_chunk_bytes: number;
  };
  owned_memory: {
    coordinator: {
      limit_bytes: number;
      reserved_bytes: number;
      staged_bytes: number;
      peak_reserved_bytes: number;
      peak_staged_bytes: number;
    };
    datafusion: {
      limit_bytes: number;
      reserved_bytes: number;
      peak_bytes: number;
    };
  };
  execution: {
    executed_on: string;
    fallback_event_observed: boolean;
    response_fallback_reason: unknown;
  };
};

export function validatePublicR2BrowserQueryEvidence(evidence: PublicR2BrowserQueryEvidence): void {
  if (
    evidence.execution.executed_on !== 'browser_wasm' ||
    evidence.execution.fallback_event_observed !== false ||
    evidence.execution.response_fallback_reason !== null
  ) {
    throw new Error('public R2 qualification requires browser WASM execution without fallback');
  }

  const metrics = evidence.metrics;
  for (const [field, value] of Object.entries(metrics)) {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new Error(`public R2 qualification metric '${field}' was not a safe integer`);
    }
  }
  for (const field of [
    'bytes_fetched',
    'scan_data_range_reads',
    'rows_emitted',
    'arrow_ipc_bytes',
  ] as const) {
    if (metrics[field] === 0) {
      throw new Error(`public R2 qualification metric '${field}' must be positive`);
    }
  }
  if (metrics.coordinator_peak_staged_bytes > metrics.coordinator_staging_limit_bytes) {
    throw new Error('public R2 qualification coordinator staging exceeded its limit');
  }
  if (metrics.cursor_peak_pending_encoded_bytes > 8 * 1024 * 1024) {
    throw new Error('public R2 qualification pending IPC exceeded 8 MiB');
  }
  if (metrics.cursor_peak_transport_chunk_bytes > 1024 * 1024) {
    throw new Error('public R2 qualification IPC transport chunk exceeded 1 MiB');
  }

  const coordinator = evidence.owned_memory.coordinator;
  const datafusion = evidence.owned_memory.datafusion;
  for (const [field, value] of Object.entries({
    ...Object.fromEntries(
      Object.entries(coordinator).map(([field, value]) => [`coordinator.${field}`, value]),
    ),
    ...Object.fromEntries(
      Object.entries(datafusion).map(([field, value]) => [`datafusion.${field}`, value]),
    ),
  })) {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new Error(`public R2 qualification memory '${field}' was not a safe integer`);
    }
  }
  if (
    coordinator.reserved_bytes !== 0 ||
    coordinator.staged_bytes !== 0 ||
    datafusion.reserved_bytes !== 0
  ) {
    throw new Error('public R2 qualification owned memory must be zero at terminal');
  }
  if (
    coordinator.peak_reserved_bytes > coordinator.limit_bytes ||
    coordinator.peak_staged_bytes > coordinator.limit_bytes ||
    datafusion.peak_bytes > datafusion.limit_bytes
  ) {
    throw new Error('public R2 qualification owned-memory peak exceeded its limit');
  }
}

type PublicR2PerformanceRow = {
  eventId: bigint;
  eventTimestampMs: number;
  region: string;
  customerId: string;
  amount: number;
  status: string;
};

const EXPECTED_HEADER = ['event_id', 'event_ts', 'region', 'customer_id', 'amount', 'status'];
const EXPECTED_ROW_COUNT = 1000;
const ACTIVE_EVENT_ID_MIN = 20_000_000_000n;
const ACTIVE_EVENT_ID_MAX = 20_001_048_575n;
const ACTIVE_REGIONS = new Set(['us-east', 'us-west', 'eu-west', 'ap-south']);
const QUALIFYING_STATUSES = new Set(['paid', 'shipped']);
const EXPECTED_RESULT_SHA256 = '761cc331ddf6de4d7b59ec96a756e5b36f47c03932d6dd3f3f23257762b89cd2';
const EXPECTED_ONBOARDING_RESULT_SHA256 =
  'ef601f704c2fc8130c76c92d4fc65f0a0c21b2c4d6cfd20cd273d393d6d8ce8e';
const FIRST_EXPECTED = {
  eventId: 20_000_086_400n,
  eventTimestampMs: Date.UTC(2026, 0, 1, 0, 0, 0),
  region: 'us-east',
  customerId: 'cust-762a90e5-00015180',
  amount: 282.66,
  status: 'paid',
};
const LAST_EXPECTED = {
  eventId: 20_000_393_402n,
  eventTimestampMs: Date.UTC(2026, 0, 1, 0, 3, 6),
  region: 'ap-south',
  customerId: 'cust-bbee1215-000000ba',
  amount: 156.85,
  status: 'shipped',
};

export async function validatePublicR2PerformanceCsv(
  csv: string,
): Promise<PublicR2PerformanceQualification> {
  const lines = csv.trimEnd().split(/\r?\n/);
  const header = lines.shift()?.split(',');
  if (
    !header ||
    header.length !== EXPECTED_HEADER.length ||
    !header.every((v, i) => v === EXPECTED_HEADER[i])
  ) {
    throw new Error('public R2 performance result columns did not match the pinned projection');
  }
  if (lines.length !== EXPECTED_ROW_COUNT) {
    throw new Error(`public R2 performance result row count did not match ${EXPECTED_ROW_COUNT}`);
  }

  const rows = lines.map(parsePerformanceRow);
  const eventIds = new Set<bigint>();
  let previous: PublicR2PerformanceRow | undefined;
  for (const row of rows) {
    if (
      row.eventId < ACTIVE_EVENT_ID_MIN ||
      row.eventId > ACTIVE_EVENT_ID_MAX ||
      !ACTIVE_REGIONS.has(row.region) ||
      !/^cust-[0-9a-f]{8}-[0-9a-f]{8}$/.test(row.customerId) ||
      !QUALIFYING_STATUSES.has(row.status) ||
      !(row.amount > 100)
    ) {
      throw new Error('public R2 performance result violated its pinned predicate');
    }
    if (eventIds.has(row.eventId)) {
      throw new Error('public R2 performance result contained a duplicate event_id');
    }
    eventIds.add(row.eventId);
    if (
      previous &&
      (row.eventTimestampMs < previous.eventTimestampMs ||
        (row.eventTimestampMs === previous.eventTimestampMs && row.eventId <= previous.eventId))
    ) {
      throw new Error('public R2 performance result violated its deterministic ordering');
    }
    previous = row;
  }

  if (!matchesBoundary(rows[0]!, FIRST_EXPECTED) || !matchesBoundary(rows.at(-1)!, LAST_EXPECTED)) {
    throw new Error('public R2 performance result did not match its pinned boundary rows');
  }

  const resultSha256 = await sha256Hex(
    rows
      .map(
        (row) =>
          `${row.eventId},${row.eventTimestampMs},${row.region},${row.customerId},${row.amount.toFixed(2)},${row.status}`,
      )
      .join('\n'),
  );
  if (resultSha256 !== EXPECTED_RESULT_SHA256) {
    throw new Error('public R2 performance result did not match the exact pinned result');
  }

  return {
    row_count: rows.length,
    first_event_id: rows[0]!.eventId.toString(),
    last_event_id: rows.at(-1)!.eventId.toString(),
    result_sha256: resultSha256,
    columns: [...EXPECTED_HEADER],
  };
}

export async function validatePublicR2OnboardingCsv(
  csv: string,
): Promise<PublicR2OnboardingQualification> {
  const lines = csv.trimEnd().split(/\r?\n/);
  if (lines.shift() !== 'id,category,value' || lines.length !== 4) {
    throw new Error('public R2 onboarding result did not match its pinned projection');
  }
  const rows = lines.map((line) => line.split(','));
  if (
    rows.some(
      (row) =>
        row.length !== 3 ||
        !/^\d+$/.test(row[0]!) ||
        !/^[A-Z]$/.test(row[1]!) ||
        !/^\d+$/.test(row[2]!),
    )
  ) {
    throw new Error('public R2 onboarding result contained an invalid row');
  }
  const resultSha256 = await sha256Hex(rows.map((row) => row.join(',')).join('\n'));
  if (resultSha256 !== EXPECTED_ONBOARDING_RESULT_SHA256) {
    throw new Error('public R2 onboarding result did not match the exact pinned result');
  }
  return {
    row_count: rows.length,
    result_sha256: resultSha256,
    columns: ['id', 'category', 'value'],
  };
}

function parsePerformanceRow(line: string): PublicR2PerformanceRow {
  const values = line.split(',');
  if (values.length !== EXPECTED_HEADER.length) {
    throw new Error('public R2 performance result row did not match the pinned projection');
  }
  const [eventId, eventTs, region, customerId, amount, status] = values as [
    string,
    string,
    string,
    string,
    string,
    string,
  ];
  if (!/^\d+$/.test(eventId)) {
    throw new Error('public R2 performance result event_id was invalid');
  }
  const eventTimestampMs = utcTimestampMs(eventTs);
  if (!/^-?(?:0|[1-9]\d*)(?:\.\d{1,2})?$/.test(amount)) {
    throw new Error('public R2 performance result amount was invalid');
  }
  const numericAmount = Number(amount);
  if (!Number.isFinite(numericAmount)) {
    throw new Error('public R2 performance result amount was invalid');
  }
  return {
    eventId: BigInt(eventId),
    eventTimestampMs,
    region,
    customerId,
    amount: numericAmount,
    status,
  };
}

async function sha256Hex(value: string): Promise<string> {
  const bytes = await globalThis.crypto.subtle.digest('SHA-256', new TextEncoder().encode(value));
  return Array.from(new Uint8Array(bytes), (byte) => byte.toString(16).padStart(2, '0')).join('');
}

function utcTimestampMs(value: string): number {
  const match = value.match(
    /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(?:Z|\+00:00)?$/,
  );
  if (!match) throw new Error('public R2 performance result event_ts was invalid');
  const [, year, month, day, hour, minute, second, fraction = ''] = match;
  return Date.UTC(
    Number(year),
    Number(month) - 1,
    Number(day),
    Number(hour),
    Number(minute),
    Number(second),
    Number(fraction.padEnd(3, '0').slice(0, 3)),
  );
}

function matchesBoundary(
  actual: PublicR2PerformanceRow,
  expected: PublicR2PerformanceRow,
): boolean {
  return (
    actual.eventId === expected.eventId &&
    actual.eventTimestampMs === expected.eventTimestampMs &&
    actual.region === expected.region &&
    actual.customerId === expected.customerId &&
    Math.abs(actual.amount - expected.amount) < 1e-9 &&
    actual.status === expected.status
  );
}
