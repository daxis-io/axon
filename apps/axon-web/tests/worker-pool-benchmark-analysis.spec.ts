import { describe, expect, it } from 'vitest';

import {
  bootstrapMedianInterval,
  evaluatePilotGates,
  mergeAggregateRows,
  pairedRatios,
  roundRobinShards,
  summarize,
  type AggregateRow,
  type PilotGateInput,
} from './worker-pool-benchmark-analysis.ts';

describe('roundRobinShards', () => {
  it('covers every item exactly once while preserving order within balanced shards', () => {
    const items = ['a', 'b', 'c', 'd', 'e', 'f', 'g'];

    const shards = roundRobinShards(items, 3);

    expect(shards).toEqual([
      ['a', 'd', 'g'],
      ['b', 'e'],
      ['c', 'f'],
    ]);
    expect(shards.flat().toSorted()).toEqual(items.toSorted());
    expect(Math.max(...shards.map(({ length }) => length))).toBe(
      Math.min(...shards.map(({ length }) => length)) + 1,
    );
  });

  it('does not mutate the input and returns independent shard arrays', () => {
    const items = Object.freeze([1, 2, 3, 4]);

    const shards = roundRobinShards(items, 2);
    shards[0]?.push(5);

    expect(items).toEqual([1, 2, 3, 4]);
    expect(shards[1]).toEqual([2, 4]);
  });

  it.each([0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY])(
    'rejects invalid shard count %s',
    (shardCount) => {
      expect(() => roundRobinShards([1], shardCount)).toThrow(/shardCount/i);
    },
  );

  it('rejects an empty item list', () => {
    expect(() => roundRobinShards([], 1)).toThrow(/items/i);
  });
});

describe('mergeAggregateRows', () => {
  it('exactly sums the three integer aggregate fields', () => {
    const rows: AggregateRow[] = [
      { row_count: 12, quantity_sum: 42, mobile_rows: 7 },
      { row_count: 8, quantity_sum: -2, mobile_rows: 3 },
      { row_count: 0, quantity_sum: 0, mobile_rows: 0 },
    ];

    expect(mergeAggregateRows(rows)).toEqual({
      row_count: 20,
      quantity_sum: 40,
      mobile_rows: 10,
    });
  });

  it('rejects an empty row list', () => {
    expect(() => mergeAggregateRows([])).toThrow(/rows/i);
  });

  it.each([
    null,
    [],
    { row_count: 1, quantity_sum: 2 },
    { row_count: 1, quantity_sum: 2, mobile_rows: 3, other: 4 },
  ])('rejects an unexpected aggregate row shape: %j', (row) => {
    const mergeUnknown = mergeAggregateRows as (rows: readonly unknown[]) => AggregateRow;

    expect(() => mergeUnknown([row])).toThrow(/row/i);
  });

  it.each([
    { row_count: Number.NaN, quantity_sum: 2, mobile_rows: 3 },
    { row_count: 1.5, quantity_sum: 2, mobile_rows: 3 },
    { row_count: 1, quantity_sum: Number.POSITIVE_INFINITY, mobile_rows: 3 },
    { row_count: 1, quantity_sum: 2, mobile_rows: '3' },
  ])('rejects non-finite, non-integer, and non-number values: %j', (row) => {
    const mergeUnknown = mergeAggregateRows as (rows: readonly unknown[]) => AggregateRow;

    expect(() => mergeUnknown([row])).toThrow(/integer/i);
  });

  it('rejects unsafe source integers and unsafe addition', () => {
    expect(() =>
      mergeAggregateRows([
        { row_count: Number.MAX_SAFE_INTEGER + 1, quantity_sum: 0, mobile_rows: 0 },
      ]),
    ).toThrow(/safe integer/i);

    expect(() =>
      mergeAggregateRows([
        { row_count: Number.MAX_SAFE_INTEGER, quantity_sum: 0, mobile_rows: 0 },
        { row_count: 1, quantity_sum: 0, mobile_rows: 0 },
      ]),
    ).toThrow(/overflow/i);
  });
});

describe('summarize', () => {
  it('uses linear interpolation for percentiles and reports IQR and median absolute deviation', () => {
    const values = Object.freeze([50, 10, 40, 20, 30]);

    expect(summarize(values)).toEqual({
      count: 5,
      min: 10,
      p10: 14,
      q1: 20,
      median: 30,
      q3: 40,
      p90: 46,
      max: 50,
      iqr: 20,
      mad: 10,
    });
    expect(values).toEqual([50, 10, 40, 20, 30]);
  });

  it('interpolates even-sized distributions deterministically', () => {
    expect(summarize([4, 1, 3, 2])).toMatchObject({
      p10: 1.3,
      q1: 1.75,
      median: 2.5,
      q3: 3.25,
      p90: 3.7,
      iqr: 1.5,
      mad: 1,
    });
  });

  it('accepts zero latency', () => {
    expect(summarize([0])).toEqual({
      count: 1,
      min: 0,
      p10: 0,
      q1: 0,
      median: 0,
      q3: 0,
      p90: 0,
      max: 0,
      iqr: 0,
      mad: 0,
    });
  });

  it.each([
    { values: [] },
    { values: [-1] },
    { values: [Number.NaN] },
    { values: [Number.POSITIVE_INFINITY] },
  ])('rejects invalid latency values: %j', ({ values }) => {
    expect(() => summarize(values)).toThrow(/values|latency/i);
  });
});

describe('pairedRatios', () => {
  it('preserves pair order when computing pool over single ratios', () => {
    expect(pairedRatios([100, 80, 200], [50, 60, 220])).toEqual([0.5, 0.75, 1.1]);
  });

  it('rejects empty and mismatched samples', () => {
    expect(() => pairedRatios([], [])).toThrow(/empty/i);
    expect(() => pairedRatios([1], [1, 2])).toThrow(/same length/i);
  });

  it.each([
    [[0], [1]],
    [[1], [0]],
    [[-1], [1]],
    [[1], [Number.NaN]],
    [[Number.POSITIVE_INFINITY], [1]],
  ] as const)('rejects non-positive or non-finite timings: %j', (singleMs, poolMs) => {
    expect(() => pairedRatios(singleMs, poolMs)).toThrow(/timing/i);
  });
});

describe('bootstrapMedianInterval', () => {
  it('is stable for a seed and reports reproducibility metadata', () => {
    const values = Object.freeze([0.45, 0.62, 0.71, 0.78, 0.9, 1.02, 1.1]);
    const options = { iterations: 1_000, confidence: 0.9, seed: 0xdecafbad };

    const first = bootstrapMedianInterval(values, options);
    const second = bootstrapMedianInterval(values, options);

    expect(first).toEqual(second);
    expect(first).toMatchObject({
      lower: 0.62,
      upper: 1.02,
      confidence: 0.9,
      iterations: 1_000,
      seed: 0xdecafbad,
      sampleCount: values.length,
    });
    expect(first.lower).toBeGreaterThanOrEqual(Math.min(...values));
    expect(first.upper).toBeLessThanOrEqual(Math.max(...values));
    expect(first.lower).toBeLessThanOrEqual(first.upper);
    expect(values).toEqual([0.45, 0.62, 0.71, 0.78, 0.9, 1.02, 1.1]);
  });

  it('uses every source position when resampling a single-value sample', () => {
    expect(bootstrapMedianInterval([0.75], { iterations: 25, confidence: 0.8, seed: 7 })).toEqual({
      lower: 0.75,
      upper: 0.75,
      confidence: 0.8,
      iterations: 25,
      seed: 7,
      sampleCount: 1,
    });
  });

  it.each([
    { values: [], options: { iterations: 10, confidence: 0.9, seed: 1 } },
    { values: [Number.NaN], options: { iterations: 10, confidence: 0.9, seed: 1 } },
    { values: [1], options: { iterations: 0, confidence: 0.9, seed: 1 } },
    { values: [1], options: { iterations: 1.5, confidence: 0.9, seed: 1 } },
    { values: [1], options: { iterations: 10, confidence: 0, seed: 1 } },
    { values: [1], options: { iterations: 10, confidence: 1, seed: 1 } },
    { values: [1], options: { iterations: 10, confidence: 0.9, seed: -1 } },
    { values: [1], options: { iterations: 10, confidence: 0.9, seed: 1.5 } },
  ])('rejects invalid bootstrap input: %j', ({ values, options }) => {
    expect(() => bootstrapMedianInterval(values, options)).toThrow();
  });
});

describe('evaluatePilotGates', () => {
  const researchGo: PilotGateInput = {
    correctness: true,
    hiddenFallback: false,
    bytesRatio: 1.05,
    p90Ratio: 1.1,
    medianPairedRatio: 0.75,
    medianRatioInterval: { lower: 0.62, upper: 0.899 },
  };

  it('continues research without declaring an overall go while production gates are missing', () => {
    const result = evaluatePilotGates(researchGo);

    expect(result.researchDecision).toBe('continue');
    expect(result.decision).toBe('inconclusive');
    expect(result.reasons).toContainEqual(
      expect.objectContaining({ code: 'research_speedup_supported', kind: 'evidence' }),
    );
    expect(result.reasons).toContainEqual(
      expect.objectContaining({ code: 'credible_memory_missing', kind: 'production_gap' }),
    );
    expect(result.reasons).toContainEqual(
      expect.objectContaining({ code: 'startup_amortization_missing', kind: 'production_gap' }),
    );
    expect(result.scope).toEqual({
      credibleMemoryAvailable: false,
      startupAmortizationAvailable: false,
      wcrpcEvaluated: false,
    });
  });

  it.each([
    {
      patch: { correctness: false },
      code: 'correctness_failed',
    },
    {
      patch: { hiddenFallback: true },
      code: 'hidden_fallback_observed',
    },
    {
      patch: { bytesRatio: 1.050_001 },
      code: 'bytes_regression',
    },
    {
      patch: { p90Ratio: 1.100_001 },
      code: 'p90_regression',
    },
  ])('classifies failed guard $code as no_go', ({ patch, code }) => {
    const result = evaluatePilotGates({ ...researchGo, ...patch });

    expect(result.decision).toBe('no_go');
    expect(result.researchDecision).toBe('defer');
    expect(result.reasons).toContainEqual(expect.objectContaining({ code, kind: 'guard' }));
  });

  it('allows byte and p90 ratios equal to their guard limits', () => {
    expect(evaluatePilotGates(researchGo)).toMatchObject({
      decision: 'inconclusive',
      researchDecision: 'continue',
    });
  });

  it('classifies evidence that rules out even a ten-percent gain as no_go', () => {
    const result = evaluatePilotGates({
      ...researchGo,
      medianPairedRatio: 0.91,
      medianRatioInterval: { lower: 0.901, upper: 1.02 },
    });

    expect(result.decision).toBe('no_go');
    expect(result.researchDecision).toBe('defer');
    expect(result.reasons).toContainEqual(
      expect.objectContaining({ code: 'meaningful_speedup_ruled_out', kind: 'evidence' }),
    );
  });

  it.each([
    {
      medianPairedRatio: 0.751,
      medianRatioInterval: { lower: 0.6, upper: 0.85 },
    },
    {
      medianPairedRatio: 0.7,
      medianRatioInterval: { lower: 0.6, upper: 0.9 },
    },
    {
      medianPairedRatio: 0.91,
      medianRatioInterval: { lower: 0.89, upper: 1.01 },
    },
  ])('classifies unresolved speed evidence as inconclusive: %j', (patch) => {
    const result = evaluatePilotGates({ ...researchGo, ...patch });

    expect(result.decision).toBe('inconclusive');
    expect(result.researchDecision).toBe('inconclusive');
    expect(result.reasons).toContainEqual(
      expect.objectContaining({ code: 'speedup_evidence_inconclusive', kind: 'evidence' }),
    );
  });

  it('uses strict greater-than rules at the no-go evidence threshold', () => {
    const result = evaluatePilotGates({
      ...researchGo,
      medianPairedRatio: 0.9,
      medianRatioInterval: { lower: 0.9, upper: 0.95 },
    });

    expect(result.decision).toBe('inconclusive');
    expect(result.researchDecision).toBe('inconclusive');
  });

  it.each([
    { bytesRatio: 0 },
    { p90Ratio: Number.NaN },
    { medianPairedRatio: Number.POSITIVE_INFINITY },
    { medianRatioInterval: { lower: 1, upper: 0.5 } },
  ])('rejects malformed gate input: %j', (patch) => {
    expect(() => evaluatePilotGates({ ...researchGo, ...patch })).toThrow();
  });
});
