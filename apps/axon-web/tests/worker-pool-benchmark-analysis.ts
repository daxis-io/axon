export type AggregateRow = {
  row_count: number;
  quantity_sum: number;
  mobile_rows: number;
};

export type DistributionSummary = {
  count: number;
  min: number;
  p10: number;
  q1: number;
  median: number;
  q3: number;
  p90: number;
  max: number;
  iqr: number;
  mad: number;
};

export type BootstrapOptions = {
  iterations: number;
  confidence: number;
  seed: number;
};

export type ConfidenceInterval = {
  lower: number;
  upper: number;
  confidence: number;
  iterations: number;
  seed: number;
  sampleCount: number;
};

export type PilotGateInput = {
  correctness: boolean;
  hiddenFallback: boolean;
  bytesRatio: number;
  p90Ratio: number;
  medianPairedRatio: number;
  medianRatioInterval: Pick<ConfidenceInterval, 'lower' | 'upper'>;
};

export type PilotGateDecision = 'go' | 'inconclusive' | 'no_go';
export type ResearchDecision = 'continue' | 'inconclusive' | 'defer';

export type PilotGateReason = {
  code:
    | 'correctness_failed'
    | 'hidden_fallback_observed'
    | 'bytes_regression'
    | 'p90_regression'
    | 'research_speedup_supported'
    | 'meaningful_speedup_ruled_out'
    | 'speedup_evidence_inconclusive'
    | 'credible_memory_missing'
    | 'startup_amortization_missing';
  kind: 'guard' | 'evidence' | 'production_gap';
  message: string;
};

export type PilotGateResult = {
  decision: PilotGateDecision;
  researchDecision: ResearchDecision;
  reasons: PilotGateReason[];
  scope: {
    credibleMemoryAvailable: false;
    startupAmortizationAvailable: false;
    wcrpcEvaluated: false;
  };
};

const AGGREGATE_KEYS = ['mobile_rows', 'quantity_sum', 'row_count'] as const;
const MAX_UINT32 = 0xffff_ffff;

export function roundRobinShards<T>(items: readonly T[], shardCount: number): T[][] {
  if (!Number.isInteger(shardCount) || shardCount <= 0) {
    throw new RangeError('shardCount must be a positive integer');
  }
  if (items.length === 0) {
    throw new RangeError('items must not be empty');
  }

  const shards = Array.from({ length: shardCount }, () => [] as T[]);
  items.forEach((item, index) => {
    shards[index % shardCount]?.push(item);
  });
  return shards;
}

export function mergeAggregateRows(rows: readonly AggregateRow[]): AggregateRow {
  if (rows.length === 0) {
    throw new RangeError('rows must not be empty');
  }

  return rows.reduce<AggregateRow>(
    (total, candidate, index) => {
      const row = checkedAggregateRow(candidate, index);
      return {
        row_count: addSafeIntegers(total.row_count, row.row_count, 'row_count'),
        quantity_sum: addSafeIntegers(total.quantity_sum, row.quantity_sum, 'quantity_sum'),
        mobile_rows: addSafeIntegers(total.mobile_rows, row.mobile_rows, 'mobile_rows'),
      };
    },
    { row_count: 0, quantity_sum: 0, mobile_rows: 0 },
  );
}

/**
 * Summarizes latencies with linear percentiles: for sorted values and percentile p,
 * interpolate between indexes floor((n - 1) * p) and ceil((n - 1) * p).
 * MAD is the median absolute deviation from the interpolated median.
 */
export function summarize(values: readonly number[]): DistributionSummary {
  assertFiniteValues(values, { allowZero: true, label: 'latency values' });

  const sorted = [...values].sort((left, right) => left - right);
  const q1 = linearPercentile(sorted, 0.25);
  const median = linearPercentile(sorted, 0.5);
  const q3 = linearPercentile(sorted, 0.75);
  const deviations = sorted
    .map((value) => Math.abs(value - median))
    .sort((left, right) => left - right);

  return {
    count: sorted.length,
    min: sorted[0] as number,
    p10: linearPercentile(sorted, 0.1),
    q1,
    median,
    q3,
    p90: linearPercentile(sorted, 0.9),
    max: sorted.at(-1) as number,
    iqr: q3 - q1,
    mad: linearPercentile(deviations, 0.5),
  };
}

export function pairedRatios(singleMs: readonly number[], poolMs: readonly number[]): number[] {
  if (singleMs.length === 0 || poolMs.length === 0) {
    throw new RangeError('timing samples must not be empty');
  }
  if (singleMs.length !== poolMs.length) {
    throw new RangeError('timing samples must have the same length');
  }
  assertFiniteValues(singleMs, { allowZero: false, label: 'single timing samples' });
  assertFiniteValues(poolMs, { allowZero: false, label: 'pool timing samples' });

  return singleMs.map((single, index) => (poolMs[index] as number) / single);
}

export function bootstrapMedianInterval(
  values: readonly number[],
  options: BootstrapOptions,
): ConfidenceInterval {
  assertFiniteValues(values, { allowZero: true, allowNegative: true, label: 'bootstrap values' });
  if (!Number.isInteger(options.iterations) || options.iterations <= 0) {
    throw new RangeError('iterations must be a positive integer');
  }
  if (!Number.isFinite(options.confidence) || options.confidence <= 0 || options.confidence >= 1) {
    throw new RangeError('confidence must be greater than zero and less than one');
  }
  if (!Number.isInteger(options.seed) || options.seed < 0 || options.seed > MAX_UINT32) {
    throw new RangeError('seed must be an unsigned 32-bit integer');
  }

  const source = [...values];
  const random = mulberry32(options.seed);
  const medians: number[] = [];
  for (let iteration = 0; iteration < options.iterations; iteration += 1) {
    const sample = Array.from(
      { length: source.length },
      () => source[Math.floor(random() * source.length)] as number,
    );
    sample.sort((left, right) => left - right);
    medians.push(linearPercentile(sample, 0.5));
  }
  medians.sort((left, right) => left - right);

  const tail = (1 - options.confidence) / 2;
  return {
    lower: linearPercentile(medians, tail),
    upper: linearPercentile(medians, 1 - tail),
    confidence: options.confidence,
    iterations: options.iterations,
    seed: options.seed,
    sampleCount: source.length,
  };
}

export function evaluatePilotGates(input: PilotGateInput): PilotGateResult {
  if (typeof input.correctness !== 'boolean' || typeof input.hiddenFallback !== 'boolean') {
    throw new TypeError('correctness and hiddenFallback must be boolean values');
  }
  assertPositiveRatio(input.bytesRatio, 'bytesRatio');
  assertPositiveRatio(input.p90Ratio, 'p90Ratio');
  assertPositiveRatio(input.medianPairedRatio, 'medianPairedRatio');
  assertPositiveRatio(input.medianRatioInterval.lower, 'medianRatioInterval.lower');
  assertPositiveRatio(input.medianRatioInterval.upper, 'medianRatioInterval.upper');
  if (input.medianRatioInterval.lower > input.medianRatioInterval.upper) {
    throw new RangeError('medianRatioInterval lower bound must not exceed its upper bound');
  }

  const reasons: PilotGateReason[] = [];
  if (!input.correctness) {
    reasons.push({
      code: 'correctness_failed',
      kind: 'guard',
      message: 'The pooled result did not match the single-worker result.',
    });
  }
  if (input.hiddenFallback) {
    reasons.push({
      code: 'hidden_fallback_observed',
      kind: 'guard',
      message: 'The pooled path used a hidden single-worker fallback.',
    });
  }
  if (input.bytesRatio > 1.05) {
    reasons.push({
      code: 'bytes_regression',
      kind: 'guard',
      message: 'The pooled path transferred more than 105% of the single-worker bytes.',
    });
  }
  if (input.p90Ratio > 1.1) {
    reasons.push({
      code: 'p90_regression',
      kind: 'guard',
      message: 'The pooled p90 exceeded 110% of the single-worker p90.',
    });
  }

  let decision: PilotGateDecision;
  let researchDecision: ResearchDecision;
  if (reasons.some(({ kind }) => kind === 'guard')) {
    decision = 'no_go';
    researchDecision = 'defer';
  } else if (input.medianPairedRatio <= 0.75 && input.medianRatioInterval.upper < 0.9) {
    decision = 'inconclusive';
    researchDecision = 'continue';
    reasons.push({
      code: 'research_speedup_supported',
      kind: 'evidence',
      message: 'The median paired ratio and confidence interval clear the research speedup gate.',
    });
  } else if (input.medianPairedRatio > 0.9 && input.medianRatioInterval.lower > 0.9) {
    decision = 'no_go';
    researchDecision = 'defer';
    reasons.push({
      code: 'meaningful_speedup_ruled_out',
      kind: 'evidence',
      message: 'The evidence rules out even a ten-percent median gain.',
    });
  } else {
    decision = 'inconclusive';
    researchDecision = 'inconclusive';
    reasons.push({
      code: 'speedup_evidence_inconclusive',
      kind: 'evidence',
      message: 'The pilot does not yet support or rule out the target speedup.',
    });
  }

  reasons.push({
    code: 'credible_memory_missing',
    kind: 'production_gap',
    message: 'The pilot lacks a credible production memory measurement.',
  });
  reasons.push({
    code: 'startup_amortization_missing',
    kind: 'production_gap',
    message: 'The pilot has not established how worker startup cost is amortized.',
  });

  return {
    decision,
    researchDecision,
    reasons,
    scope: {
      credibleMemoryAvailable: false,
      startupAmortizationAvailable: false,
      wcrpcEvaluated: false,
    },
  };
}

function checkedAggregateRow(candidate: unknown, index: number): AggregateRow {
  if (typeof candidate !== 'object' || candidate === null || Array.isArray(candidate)) {
    throw new TypeError(`row ${index} must be an aggregate object`);
  }
  const keys = Reflect.ownKeys(candidate).toSorted();
  if (
    keys.length !== AGGREGATE_KEYS.length ||
    keys.some((key, keyIndex) => key !== AGGREGATE_KEYS[keyIndex])
  ) {
    throw new TypeError(`row ${index} must contain exactly ${AGGREGATE_KEYS.join(', ')}`);
  }

  const row = candidate as Record<(typeof AGGREGATE_KEYS)[number], unknown>;
  return {
    row_count: checkedSafeInteger(row.row_count, `row ${index}.row_count`),
    quantity_sum: checkedSafeInteger(row.quantity_sum, `row ${index}.quantity_sum`),
    mobile_rows: checkedSafeInteger(row.mobile_rows, `row ${index}.mobile_rows`),
  };
}

function checkedSafeInteger(value: unknown, label: string): number {
  if (typeof value !== 'number' || !Number.isInteger(value)) {
    throw new TypeError(`${label} must be an integer`);
  }
  if (!Number.isSafeInteger(value)) {
    throw new RangeError(`${label} must be a safe integer`);
  }
  return value;
}

function addSafeIntegers(left: number, right: number, label: string): number {
  const sum = left + right;
  if (!Number.isSafeInteger(sum)) {
    throw new RangeError(`${label} aggregate overflow`);
  }
  return sum;
}

function assertFiniteValues(
  values: readonly number[],
  options: { allowZero: boolean; allowNegative?: boolean; label: string },
): void {
  if (values.length === 0) {
    throw new RangeError(`${options.label} must not be empty`);
  }
  values.forEach((value, index) => {
    if (!Number.isFinite(value)) {
      throw new TypeError(`${options.label}[${index}] must be finite`);
    }
    if (!options.allowNegative && value < 0) {
      throw new RangeError(`${options.label}[${index}] must not be negative`);
    }
    if (!options.allowZero && value === 0) {
      throw new RangeError(`${options.label}[${index}] must be a positive timing`);
    }
  });
}

function linearPercentile(sorted: readonly number[], percentile: number): number {
  const position = (sorted.length - 1) * percentile;
  const lowerIndex = Math.floor(position);
  const upperIndex = Math.ceil(position);
  const lower = sorted[lowerIndex] as number;
  const upper = sorted[upperIndex] as number;
  return lower + (upper - lower) * (position - lowerIndex);
}

function mulberry32(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let value = state;
    value = Math.imul(value ^ (value >>> 15), value | 1);
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
    return ((value ^ (value >>> 14)) >>> 0) / 0x1_0000_0000;
  };
}

function assertPositiveRatio(value: number, label: string): void {
  if (!Number.isFinite(value) || value <= 0) {
    throw new RangeError(`${label} must be a positive finite ratio`);
  }
}
