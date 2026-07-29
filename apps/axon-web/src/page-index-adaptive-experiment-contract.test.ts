import { describe, expect, it } from 'vitest';

import {
  LATIN_SQUARE_ARM_ORDERS,
  PAGE_INDEX_CALIBRATION_BLOCKS,
  auditPageIndexDecisionSummary,
  buildPinnedCoveringArrays,
  calibrationArmOrder,
  calibrationP95AbsoluteErrorUs,
  cellId,
  evaluateControlledHoldout,
  freezePageIndexModel,
  holdoutArmOrder,
  missingPairKeys,
  validateNetworkPreflight,
} from '../tests/support/page-index-adaptive-experiment.ts';

describe('adaptive page-index experiment contract', () => {
  it('pins disjoint deterministic pairwise calibration and holdout arrays', () => {
    const first = buildPinnedCoveringArrays();
    const second = buildPinnedCoveringArrays();
    expect(first).toEqual(second);
    expect(missingPairKeys(first.calibration.cells)).toEqual([]);
    expect(missingPairKeys(first.holdout.cells)).toEqual([]);
    const calibrationIds = new Set(first.calibration.cells.map(cellId));
    expect(first.holdout.cells.every((cell) => !calibrationIds.has(cellId(cell)))).toBe(true);
    expect(first.calibration.cells).toHaveLength(29);
    expect(first.calibration.hash).toBe(
      'ce84a0b18c33e30233a91ec1ed8a1ec45d3d92b008b201a51f6f2f3aeadca163',
    );
    expect(first.holdout.cells).toHaveLength(28);
    expect(first.holdout.hash).toBe(
      '1d790d47d7b86513d2612194374b0e8df1a8b271db02ff4e8d802e95992bea1c',
    );
  });

  it('interleaves calibration pairs and cycles every Latin-square arm permutation', () => {
    const cell = buildPinnedCoveringArrays().calibration.cells[0]!;
    const calibrationOrders = Array.from({ length: PAGE_INDEX_CALIBRATION_BLOCKS }, (_, block) =>
      calibrationArmOrder(cell, block).join(','),
    );
    expect(new Set(calibrationOrders)).toEqual(new Set(['skip,predicate', 'predicate,skip']));
    const holdoutOrders = Array.from({ length: 6 }, (_, block) =>
      holdoutArmOrder(cell, block).join(','),
    );
    expect(new Set(holdoutOrders)).toEqual(
      new Set(LATIN_SQUARE_ARM_ORDERS.map((order) => order.join(','))),
    );
  });

  it('freezes and hashes the p95 calibration margin before holdout', () => {
    const error = calibrationP95AbsoluteErrorUs([
      { predicted_us: 90, actual_us: 100 },
      { predicted_us: 100, actual_us: 102 },
      { predicted_us: 110, actual_us: 103 },
      { predicted_us: 120, actual_us: 100 },
    ]);
    expect(error).toBe(20);
    const frozen = freezePageIndexModel(error, {
      max_parallel_requests: 6,
      browser: 'chromium',
    });
    expect(frozen.calibration_error_margin_us).toBe(20);
    expect(frozen.hash).toMatch(/^[0-9a-f]{64}$/);
    expect(freezePageIndexModel(error, frozen.browser_constants)).toEqual(frozen);
  });

  it('invalidates shaped cells when worker requests do not observe the profile', () => {
    expect(
      validateNetworkPreflight({
        profile: '25mbps_60ms',
        measured_throughput_mbps: 25.2,
        measured_added_latency_ms: 61,
        worker_request_count: 4,
      }),
    ).toEqual({ valid: true, reasons: [] });
    const mismatch = validateNetworkPreflight({
      profile: '10mbps_150ms',
      measured_throughput_mbps: 80,
      measured_added_latency_ms: 5,
      worker_request_count: 0,
    });
    expect(mismatch.valid).toBe(false);
    expect(mismatch.reasons).toHaveLength(3);
  });

  it('accepts only the redacted decision-summary field envelope', () => {
    const safe = {
      requested_mode: 'adaptive',
      chosen_plan: 'predicate',
      decision_reason: 'predicted_predicate_faster',
      model_version: 'adaptive_page_index_v1',
      decision_duration_us: 42,
      range_sample_count: 5,
      decode_sample_count: 3,
      confidence_eligible: true,
      predicted_skip_time_us: 120_000,
      predicted_predicate_time_us: 40_000,
      index_bytes: 8_192,
      index_requests: 2,
      pages_selected: 4,
      pages_skipped: 60,
      pages_touched: 4,
    };
    expect(auditPageIndexDecisionSummary(safe)).toEqual([]);
    expect(auditPageIndexDecisionSummary({ ...safe, table_uri: 's3://secret/path' })).toEqual([
      'secret',
      'table_uri',
    ]);
  });

  it('evaluates the frozen controlled-holdout gates from completed observations', () => {
    const rows = Array.from({ length: 30 }, (_, block) =>
      [
        {
          cell_id: 'cell-1',
          block,
          arm: 'skip' as const,
          duration_ms: 100,
          throughput_queries_per_second: 10,
          engine_time_us: 100_000,
          decision_duration_us: 50,
          chosen_plan: 'skip' as const,
          physical_bytes: 1_000,
          index_bytes: 0,
          index_requests: 0,
          completed_range_responses_valid: true,
          result_checksum: 'same',
          executed_on: 'browser_wasm' as const,
          fallback: false,
          terminal_owned_bytes: 0,
          retained_heap_bytes: 0,
          failed: false,
          cancelled: false,
        },
        {
          cell_id: 'cell-1',
          block,
          arm: 'predicate' as const,
          duration_ms: 50,
          throughput_queries_per_second: 20,
          engine_time_us: 50_000,
          decision_duration_us: 50,
          chosen_plan: 'predicate' as const,
          physical_bytes: 500,
          index_bytes: 50,
          index_requests: 1,
          completed_range_responses_valid: true,
          result_checksum: 'same',
          executed_on: 'browser_wasm' as const,
          fallback: false,
          terminal_owned_bytes: 0,
          retained_heap_bytes: 0,
          failed: false,
          cancelled: false,
        },
        {
          cell_id: 'cell-1',
          block,
          arm: 'adaptive' as const,
          duration_ms: 50,
          throughput_queries_per_second: 20,
          engine_time_us: 50_000,
          decision_duration_us: 50,
          chosen_plan: 'predicate' as const,
          physical_bytes: 500,
          index_bytes: 50,
          index_requests: 1,
          completed_range_responses_valid: true,
          result_checksum: 'same',
          executed_on: 'browser_wasm' as const,
          fallback: false,
          terminal_owned_bytes: 0,
          retained_heap_bytes: 0,
          failed: false,
          cancelled: false,
        },
      ].flat(),
    ).flat();
    expect(evaluateControlledHoldout(rows)).toMatchObject({ passed: true });
    const slowDecision = rows.map((row, index) =>
      index === 2 ? { ...row, decision_duration_us: 300 } : row,
    );
    expect(evaluateControlledHoldout(slowDecision)).toMatchObject({
      passed: false,
      gates: { decision_budget: false },
    });
  });
});
