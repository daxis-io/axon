import { describe, expect, it } from 'vitest';

import {
  validatePublicR2BrowserQueryEvidence,
  validatePublicR2OnboardingCsv,
  validatePublicR2PerformanceCsv,
} from './public-r2-qualification.ts';

const HEADER = 'event_id,event_ts,region,customer_id,amount,status';
const EXPECTED_RESULT_SHA256 = '761cc331ddf6de4d7b59ec96a756e5b36f47c03932d6dd3f3f23257762b89cd2';
const MASK_64 = (1n << 64n) - 1n;
const FIXTURE_SEED = 0xa5015eedda7a2026n;
const ACTIVE_REGIONS = ['us-east', 'us-west', 'eu-west', 'ap-south'] as const;

function validPinnedRowsCsv(): string {
  const rows = pinnedPerformanceRows().map(
    (row) =>
      `${row.eventId},${new Date(row.eventTimestampMs).toISOString()},${row.region},${row.customerId},${row.amount.toFixed(2)},${row.status}`,
  );
  return [HEADER, ...rows].join('\n');
}

describe('public R2 pinned performance result qualification', () => {
  it('accepts only the exact ordered result from the pinned fixture', async () => {
    await expect(validatePublicR2PerformanceCsv(validPinnedRowsCsv())).resolves.toEqual({
      row_count: 1000,
      first_event_id: '20000086400',
      last_event_id: '20000393402',
      result_sha256: EXPECTED_RESULT_SHA256,
      columns: ['event_id', 'event_ts', 'region', 'customer_id', 'amount', 'status'],
    });
  });

  it('rejects a nonempty result with correct columns but wrong broad semantics', async () => {
    const wrongPredicate = validPinnedRowsCsv().replace(',282.66,paid', ',99,pending');
    await expect(validatePublicR2PerformanceCsv(wrongPredicate)).rejects.toThrow(/predicate/i);

    const wrongBoundary = validPinnedRowsCsv().replace('cust-bbee1215', 'cust-deadbeef');
    await expect(validatePublicR2PerformanceCsv(wrongBoundary)).rejects.toThrow(/boundary/i);
  });

  it('rejects one altered interior value that still satisfies predicates and ordering', async () => {
    const lines = validPinnedRowsCsv().split('\n');
    const interior = lines[500]!.split(',');
    interior[4] = (Number(interior[4]) + 0.01).toFixed(2);
    lines[500] = interior.join(',');

    await expect(validatePublicR2PerformanceCsv(lines.join('\n'))).rejects.toThrow(
      /exact pinned result/i,
    );
  });
});

describe('public R2 pinned onboarding result qualification', () => {
  const csv = 'id,category,value\n7,B,70\n8,B,80\n9,D,90\n10,D,100';

  it('accepts the exact four ordered rows', async () => {
    await expect(validatePublicR2OnboardingCsv(csv)).resolves.toEqual({
      row_count: 4,
      result_sha256: 'ef601f704c2fc8130c76c92d4fc65f0a0c21b2c4d6cfd20cd273d393d6d8ce8e',
      columns: ['id', 'category', 'value'],
    });
  });

  it('rejects a different four-row snapshot', async () => {
    await expect(validatePublicR2OnboardingCsv(csv.replace('8,B,80', '8,B,81'))).rejects.toThrow(
      /exact pinned result/i,
    );
  });
});

describe('public R2 browser query evidence qualification', () => {
  const evidence = {
    metrics: {
      bytes_fetched: 4096,
      scan_data_range_reads: 2,
      rows_emitted: 1,
      arrow_ipc_bytes: 128,
      coordinator_peak_staged_bytes: 1024,
      coordinator_staging_limit_bytes: 2048,
      cursor_peak_pending_encoded_bytes: 1024,
      cursor_peak_transport_chunk_bytes: 512,
    },
    owned_memory: {
      coordinator: {
        limit_bytes: 2048,
        reserved_bytes: 0,
        staged_bytes: 0,
        peak_reserved_bytes: 1024,
        peak_staged_bytes: 1024,
      },
      datafusion: {
        limit_bytes: 4096,
        reserved_bytes: 0,
        peak_bytes: 1024,
      },
    },
    execution: {
      executed_on: 'browser_wasm',
      fallback_event_observed: false,
      response_fallback_reason: null,
    },
  };

  it('requires zero terminal owned memory while retaining bounded peaks', () => {
    expect(() => validatePublicR2BrowserQueryEvidence(evidence)).not.toThrow();

    for (const mutate of [
      (value: typeof evidence) => (value.owned_memory.coordinator.reserved_bytes = 1),
      (value: typeof evidence) => (value.owned_memory.coordinator.staged_bytes = 1),
      (value: typeof evidence) => (value.owned_memory.datafusion.reserved_bytes = 1),
    ]) {
      const mutated = structuredClone(evidence);
      mutate(mutated);
      expect(() => validatePublicR2BrowserQueryEvidence(mutated)).toThrow(/zero at terminal/i);
    }
  });
});

type PinnedRow = {
  eventId: number;
  eventTimestampMs: number;
  region: string;
  customerId: string;
  amount: number;
  status: string;
};

function pinnedPerformanceRows(): PinnedRow[] {
  const rows: PinnedRow[] = [];
  for (let fileIndex = 0; fileIndex < ACTIVE_REGIONS.length; fileIndex += 1) {
    const rng = {
      state: FIXTURE_SEED ^ (BigInt(fileIndex + 1) * 0x9e3779b9n),
    };
    for (let rowIndex = 0; rowIndex < 131_072; rowIndex += 1) {
      const skew = nextU32(rng) % 100;
      const status = statusFor(skew, rowIndex);
      const amount = amountFor(rng, status, rowIndex);
      if (rowIndex % 11 !== 0) nextU32(rng);
      nextU32(rng);
      const customerId = `cust-${nextU32(rng).toString(16).padStart(8, '0')}-${rowIndex.toString(16).padStart(8, '0')}`;
      nextU32(rng);
      nextU32(rng);
      nextU32(rng);
      if (rowIndex % 13 !== 0) nextU32(rng);
      nextU32(rng);
      nextU32(rng);
      if (rowIndex % 7 !== 0) {
        nextU32(rng);
        nextU32(rng);
        nextU32(rng);
      }
      if (amount > 100 && (status === 'paid' || status === 'shipped')) {
        rows.push({
          eventId: 20_000_000_000 + fileIndex * 131_072 + rowIndex,
          eventTimestampMs: Date.UTC(2026, 0, 1, 0, 0, rowIndex % 86_400),
          region: ACTIVE_REGIONS[fileIndex]!,
          customerId,
          amount,
          status,
        });
      }
    }
  }
  return rows
    .sort(
      (left, right) =>
        left.eventTimestampMs - right.eventTimestampMs || left.eventId - right.eventId,
    )
    .slice(0, 1000);
}

function nextU32(rng: { state: bigint }): number {
  rng.state = (rng.state * 6_364_136_223_846_793_005n + 1_442_695_040_888_963_407n) & MASK_64;
  return Number((rng.state >> 32n) & 0xffff_ffffn);
}

function statusFor(skew: number, rowIndex: number): string {
  if (rowIndex % 97 === 0) return 'refunded';
  if (skew < 46) return 'paid';
  if (skew < 68) return 'shipped';
  if (skew < 86) return 'pending';
  return 'failed';
}

function amountFor(rng: { state: bigint }, status: string, rowIndex: number): number {
  const base =
    status === 'paid'
      ? 110
      : status === 'shipped'
        ? 145
        : status === 'pending'
          ? 45
          : status === 'failed'
            ? 15
            : 70;
  return base + (rowIndex % 251 === 0 ? 450 : 0) + (nextU32(rng) % 20_000) / 100;
}
