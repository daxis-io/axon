import { createHash } from 'node:crypto';

export const PAGE_INDEX_MODEL_FORMULA =
  'request_queue_rounds + transferred_bytes / throughput + decode_filter_cpu';
export const PAGE_INDEX_MODEL_VERSION = 'adaptive_page_index_v1';
export const PAGE_INDEX_EWMA_COEFFICIENT = 0.25;
export const PAGE_INDEX_MIN_RANGE_SAMPLES = 5;
export const PAGE_INDEX_MIN_DECODE_SAMPLES = 3;
export const PAGE_INDEX_CALIBRATION_BLOCKS = 12;
export const PAGE_INDEX_HOLDOUT_BLOCKS = 30;
export const PAGE_INDEX_BOUNDARY_HOLDOUT_BLOCKS = 102;

export const PAGE_INDEX_DIMENSIONS = {
  layout: ['ordered', 'eight_page_clusters', 'fully_shuffled'],
  selectivity_basis_points: [0, 10, 100, 500, 2_000, 5_000, 10_000],
  geometry: ['few_large', 'many_small'],
  projection: ['narrow', 'wide'],
  connection: ['cold', 'warm'],
  metadata_cache: ['cold', 'warm'],
  concurrency: [1, 2, 4],
  network_profile: ['public_s3_unshaped', '100mbps_20ms', '25mbps_60ms', '10mbps_150ms'],
} as const;

type DimensionValues = typeof PAGE_INDEX_DIMENSIONS;
export type PageIndexExperimentCell = {
  [Key in keyof DimensionValues]: DimensionValues[Key][number];
};

export type PageIndexExperimentArm = 'skip' | 'predicate' | 'adaptive';

export type CoveringArray = {
  seed: string;
  cells: PageIndexExperimentCell[];
  hash: string;
};

export type FrozenPageIndexModel = {
  schema_version: 1;
  model_version: typeof PAGE_INDEX_MODEL_VERSION;
  formula: typeof PAGE_INDEX_MODEL_FORMULA;
  ewma_coefficient: typeof PAGE_INDEX_EWMA_COEFFICIENT;
  minimum_uncached_range_samples: typeof PAGE_INDEX_MIN_RANGE_SAMPLES;
  minimum_decode_samples: typeof PAGE_INDEX_MIN_DECODE_SAMPLES;
  no_artificial_probes: true;
  session_persistence: false;
  calibration_error_margin_us: number;
  browser_constants: Record<string, number | string>;
  hash: string;
};

export type NetworkPreflightObservation = {
  profile: PageIndexExperimentCell['network_profile'];
  measured_throughput_mbps: number;
  measured_added_latency_ms: number;
  worker_request_count: number;
};

export type NetworkPreflightResult = {
  valid: boolean;
  reasons: string[];
};

export type CompletedHoldoutObservation = {
  cell_id: string;
  block: number;
  arm: PageIndexExperimentArm;
  duration_ms: number;
  throughput_queries_per_second: number;
  engine_time_us: number;
  decision_duration_us: number;
  chosen_plan: 'skip' | 'predicate' | 'mixed';
  physical_bytes: number;
  index_bytes: number;
  index_requests: number;
  completed_range_responses_valid: boolean;
  result_checksum: string;
  executed_on: 'browser_wasm' | string;
  fallback: boolean;
  terminal_owned_bytes: number;
  retained_heap_bytes: number;
  failed: boolean;
  cancelled: boolean;
};

export type ControlledHoldoutGateReport = {
  passed: boolean;
  gates: {
    correctness_and_ownership: boolean;
    decision_budget: boolean;
    skip_zero_index_io: boolean;
    adaptive_choice_accuracy: boolean;
    regret: boolean;
    cell_regression: boolean;
    holdout_sample_count: boolean;
    concurrency_throughput: boolean;
    aggregate_bytes: boolean;
    retained_heap_and_failures: boolean;
  };
  reasons: string[];
};

const DIMENSION_NAMES = Object.keys(PAGE_INDEX_DIMENSIONS) as (keyof DimensionValues)[];

export function buildPinnedCoveringArrays(
  calibrationSeed = 'axon-page-index-calibration-v1',
  holdoutSeed = 'axon-page-index-holdout-v1',
): { calibration: CoveringArray; holdout: CoveringArray } {
  const calibrationCells = greedyPairwiseCover(calibrationSeed);
  const calibrationIds = new Set(calibrationCells.map(cellId));
  const holdoutCells = greedyPairwiseCover(holdoutSeed, calibrationIds);
  return {
    calibration: coveringArray(calibrationSeed, calibrationCells),
    holdout: coveringArray(holdoutSeed, holdoutCells),
  };
}

export function greedyPairwiseCover(
  seed: string,
  excludedCellIds: ReadonlySet<string> = new Set(),
): PageIndexExperimentCell[] {
  const candidates = allCells()
    .map((cell) => {
      const id = cellId(cell);
      return {
        cell,
        id,
        pairs: cellPairKeys(cell),
        rank: sha256(`${seed}:${id}`),
      };
    })
    .filter((candidate) => !excludedCellIds.has(candidate.id));
  const uncovered = allPairKeys();
  const selected: PageIndexExperimentCell[] = [];
  const selectedIds = new Set<string>();
  while (uncovered.size > 0) {
    let best: (typeof candidates)[number] | undefined;
    let bestScore = -1;
    for (const candidate of candidates) {
      if (selectedIds.has(candidate.id)) continue;
      let score = 0;
      for (const pair of candidate.pairs) {
        if (uncovered.has(pair)) score += 1;
      }
      if (
        score > bestScore ||
        (score === bestScore && (best === undefined || candidate.rank < best.rank))
      ) {
        best = candidate;
        bestScore = score;
      }
    }
    if (!best || bestScore <= 0) {
      throw new Error(`pairwise covering array could not cover ${uncovered.size} remaining pairs`);
    }
    selected.push(best.cell);
    selectedIds.add(best.id);
    for (const pair of best.pairs) uncovered.delete(pair);
  }
  return selected;
}

export function missingPairKeys(cells: readonly PageIndexExperimentCell[]): string[] {
  const missing = allPairKeys();
  for (const cell of cells) {
    for (const pair of cellPairKeys(cell)) missing.delete(pair);
  }
  return [...missing].sort();
}

export function calibrationArmOrder(
  cell: PageIndexExperimentCell,
  block: number,
): ['skip', 'predicate'] | ['predicate', 'skip'] {
  if (!Number.isInteger(block) || block < 0 || block >= PAGE_INDEX_CALIBRATION_BLOCKS) {
    throw new TypeError(
      `calibration block must be between 0 and ${PAGE_INDEX_CALIBRATION_BLOCKS - 1}`,
    );
  }
  return Number.parseInt(sha256(`calibration:${cellId(cell)}:${block}`).slice(0, 2), 16) % 2 === 0
    ? ['skip', 'predicate']
    : ['predicate', 'skip'];
}

export const LATIN_SQUARE_ARM_ORDERS: readonly (readonly PageIndexExperimentArm[])[] = [
  ['skip', 'predicate', 'adaptive'],
  ['skip', 'adaptive', 'predicate'],
  ['predicate', 'skip', 'adaptive'],
  ['predicate', 'adaptive', 'skip'],
  ['adaptive', 'skip', 'predicate'],
  ['adaptive', 'predicate', 'skip'],
];

export function holdoutArmOrder(
  cell: PageIndexExperimentCell,
  block: number,
): readonly PageIndexExperimentArm[] {
  if (!Number.isInteger(block) || block < 0)
    throw new TypeError('holdout block must be non-negative');
  const cycle = Math.floor(block / LATIN_SQUARE_ARM_ORDERS.length);
  const ranked = LATIN_SQUARE_ARM_ORDERS.map((order, index) => ({
    order,
    rank: sha256(`holdout:${cellId(cell)}:${cycle}:${index}`),
  })).sort((left, right) => left.rank.localeCompare(right.rank));
  return ranked[block % LATIN_SQUARE_ARM_ORDERS.length]!.order;
}

export function calibrationP95AbsoluteErrorUs(
  observations: readonly { predicted_us: number; actual_us: number }[],
): number {
  if (observations.length === 0) throw new TypeError('calibration requires completed observations');
  const errors = observations.map(({ predicted_us, actual_us }) => {
    requireNonNegativeFinite(predicted_us, 'predicted_us');
    requireNonNegativeFinite(actual_us, 'actual_us');
    return Math.abs(predicted_us - actual_us);
  });
  return percentile(errors, 0.95);
}

export function freezePageIndexModel(
  calibrationErrorMarginUs: number,
  browserConstants: Record<string, number | string>,
): FrozenPageIndexModel {
  requireNonNegativeFinite(calibrationErrorMarginUs, 'calibration_error_margin_us');
  const body: Omit<FrozenPageIndexModel, 'hash'> = {
    schema_version: 1 as const,
    model_version: PAGE_INDEX_MODEL_VERSION,
    formula: PAGE_INDEX_MODEL_FORMULA,
    ewma_coefficient: PAGE_INDEX_EWMA_COEFFICIENT,
    minimum_uncached_range_samples: PAGE_INDEX_MIN_RANGE_SAMPLES,
    minimum_decode_samples: PAGE_INDEX_MIN_DECODE_SAMPLES,
    no_artificial_probes: true as const,
    session_persistence: false as const,
    calibration_error_margin_us: Math.ceil(calibrationErrorMarginUs),
    browser_constants: browserConstants,
  };
  return { ...body, hash: sha256(stableJson(body)) };
}

export function validateNetworkPreflight(
  observation: NetworkPreflightObservation,
): NetworkPreflightResult {
  const reasons: string[] = [];
  requireNonNegativeFinite(observation.measured_throughput_mbps, 'measured_throughput_mbps');
  requireNonNegativeFinite(observation.measured_added_latency_ms, 'measured_added_latency_ms');
  if (!Number.isInteger(observation.worker_request_count) || observation.worker_request_count < 1) {
    reasons.push('worker requests did not observe the preflight');
  }
  const expected = {
    public_s3_unshaped: undefined,
    '100mbps_20ms': { throughput: 100, latency: 20 },
    '25mbps_60ms': { throughput: 25, latency: 60 },
    '10mbps_150ms': { throughput: 10, latency: 150 },
  }[observation.profile];
  if (expected) {
    const minimumThroughput = expected.throughput * 0.7;
    const maximumThroughput = expected.throughput * 1.1;
    if (
      observation.measured_throughput_mbps < minimumThroughput ||
      observation.measured_throughput_mbps > maximumThroughput
    ) {
      reasons.push(
        `throughput ${observation.measured_throughput_mbps} Mbps did not confirm ${expected.throughput} Mbps shaping`,
      );
    }
    const latencyTolerance = Math.max(10, expected.latency * 0.2);
    if (Math.abs(observation.measured_added_latency_ms - expected.latency) > latencyTolerance) {
      reasons.push(
        `added latency ${observation.measured_added_latency_ms} ms did not confirm ${expected.latency} ms shaping`,
      );
    }
  }
  return { valid: reasons.length === 0, reasons };
}

export function auditPageIndexDecisionSummary(value: unknown): string[] {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return ['decision summary is not an object'];
  }
  const allowed = new Set([
    'requested_mode',
    'chosen_plan',
    'decision_reason',
    'model_version',
    'decision_duration_us',
    'range_sample_count',
    'decode_sample_count',
    'confidence_eligible',
    'predicted_skip_time_us',
    'predicted_predicate_time_us',
    'index_bytes',
    'index_requests',
    'pages_selected',
    'pages_skipped',
    'pages_touched',
  ]);
  const forbidden = Object.keys(value).filter((key) => !allowed.has(key));
  const serialized = JSON.stringify(value).toLowerCase();
  for (const token of [
    'raw_sql',
    'predicate_literal',
    'table_uri',
    'object_uri',
    'object_path',
    'credential',
    'authorization',
    'secret',
  ]) {
    if (serialized.includes(token) && !forbidden.includes(token)) forbidden.push(token);
  }
  return forbidden.sort();
}

export function evaluateControlledHoldout(
  observations: readonly CompletedHoldoutObservation[],
): ControlledHoldoutGateReport {
  if (observations.length === 0) throw new TypeError('holdout observations must not be empty');
  for (const observation of observations) {
    requireNonNegativeFinite(observation.duration_ms, 'duration_ms');
    requireNonNegativeFinite(
      observation.throughput_queries_per_second,
      'throughput_queries_per_second',
    );
    if (observation.throughput_queries_per_second === 0) {
      throw new TypeError('throughput_queries_per_second must be positive');
    }
  }
  const reasons: string[] = [];
  const groups = Map.groupBy(observations, (observation) => observation.cell_id);
  const correctnessAndOwnership = [...groups.entries()].every(([cell, rows]) => {
    const checksums = new Set(rows.map((row) => row.result_checksum));
    const passed =
      checksums.size === 1 &&
      rows.every(
        (row) =>
          row.executed_on === 'browser_wasm' &&
          !row.fallback &&
          !row.failed &&
          row.completed_range_responses_valid &&
          row.terminal_owned_bytes === 0,
      );
    if (!passed) reasons.push(`${cell}: correctness, range validation, or ownership failed`);
    return passed;
  });
  const adaptive = observations.filter((row) => row.arm === 'adaptive');
  const decisionBudget =
    percentile(
      adaptive.map((row) => row.decision_duration_us),
      0.99,
    ) <= 250 &&
    adaptive.every(
      (row) => row.engine_time_us > 0 && row.decision_duration_us / row.engine_time_us <= 0.01,
    );
  if (!decisionBudget) reasons.push('decision-only p99 or engine-time share exceeded its gate');

  const skipZeroIndexIo = observations
    .filter((row) => row.chosen_plan === 'skip')
    .every((row) => row.index_bytes === 0 && row.index_requests === 0);
  if (!skipZeroIndexIo) reasons.push('a Skip decision performed page-index I/O');

  let separatedChoices = 0;
  let correctSeparatedChoices = 0;
  let regretPassed = true;
  let cellRegressionPassed = true;
  let holdoutSampleCountPassed = true;
  let concurrencyThroughputPassed = true;
  let perCellBytesPassed = true;
  for (const [cell, rows] of groups) {
    const skip = rows.filter((row) => row.arm === 'skip');
    const predicate = rows.filter((row) => row.arm === 'predicate');
    const adaptiveRows = rows.filter((row) => row.arm === 'adaptive');
    if (skip.length === 0 || predicate.length === 0 || adaptiveRows.length === 0) {
      reasons.push(`${cell}: missing a fixed or adaptive arm`);
      regretPassed = false;
      continue;
    }
    const skipMedian = percentile(
      skip.map((row) => row.duration_ms),
      0.5,
    );
    const predicateMedian = percentile(
      predicate.map((row) => row.duration_ms),
      0.5,
    );
    const fasterPlan = predicateMedian < skipMedian ? 'predicate' : 'skip';
    const separation = Math.abs(skipMedian - predicateMedian);
    const boundary = separation <= Math.max(Math.min(skipMedian, predicateMedian) * 0.05, 10);
    const byBlock = Map.groupBy(rows, (row) => row.block);
    const completedBlocks = [...byBlock.values()].filter(
      (blockRows) =>
        blockRows.some((row) => row.arm === 'skip') &&
        blockRows.some((row) => row.arm === 'predicate') &&
        blockRows.some((row) => row.arm === 'adaptive'),
    );
    const requiredBlocks = boundary
      ? PAGE_INDEX_BOUNDARY_HOLDOUT_BLOCKS
      : PAGE_INDEX_HOLDOUT_BLOCKS;
    if (completedBlocks.length < requiredBlocks) {
      holdoutSampleCountPassed = false;
      reasons.push(
        `${cell}: ${completedBlocks.length} completed blocks did not meet the ${requiredBlocks}-block holdout requirement`,
      );
    }
    if (separation > Math.max(Math.min(skipMedian, predicateMedian) * 0.05, 10)) {
      for (const row of adaptiveRows) {
        separatedChoices += 1;
        if (row.chosen_plan === fasterPlan) correctSeparatedChoices += 1;
      }
    }

    const regrets: { ms: number; baseline: number }[] = [];
    for (const blockRows of byBlock.values()) {
      const skipRow = blockRows.find((row) => row.arm === 'skip');
      const predicateRow = blockRows.find((row) => row.arm === 'predicate');
      const adaptiveRow = blockRows.find((row) => row.arm === 'adaptive');
      if (!skipRow || !predicateRow || !adaptiveRow) continue;
      const baseline = Math.min(skipRow.duration_ms, predicateRow.duration_ms);
      regrets.push({ ms: adaptiveRow.duration_ms - baseline, baseline });
    }
    for (const quantile of [0.5, 0.95]) {
      const upperIndex = upperQuantileConfidenceIndex(regrets.length, quantile, 0.95);
      const upper = [...regrets].sort((left, right) => left.ms - right.ms)[upperIndex];
      if (!upper || upper.ms > Math.max(10, upper.baseline * 0.05)) {
        regretPassed = false;
        reasons.push(`${cell}: adaptive p${quantile * 100} regret exceeded its upper bound`);
      }
    }

    const adaptiveP95 = percentile(
      adaptiveRows.map((row) => row.duration_ms),
      0.95,
    );
    const skipP95 = percentile(
      skip.map((row) => row.duration_ms),
      0.95,
    );
    if (adaptiveP95 - skipP95 > 10 && adaptiveP95 / skipP95 > 1.05) {
      cellRegressionPassed = false;
      reasons.push(`${cell}: adaptive p95 regressed against Skip`);
    }
    const adaptiveP99 = percentile(
      adaptiveRows.map((row) => row.duration_ms),
      0.99,
    );
    const skipP99 = percentile(
      skip.map((row) => row.duration_ms),
      0.99,
    );
    if (boundary && adaptiveP99 - skipP99 > 25 && adaptiveP99 / skipP99 > 1.1) {
      cellRegressionPassed = false;
      reasons.push(`${cell}: boundary-cell adaptive p99 regressed against Skip`);
    }

    const throughput = (armRows: readonly CompletedHoldoutObservation[]) =>
      percentile(
        armRows.map((row) => row.throughput_queries_per_second),
        0.5,
      );
    const adaptiveThroughput = throughput(adaptiveRows);
    const skipThroughput = throughput(skip);
    const predicateThroughput = throughput(predicate);
    if (
      adaptiveThroughput < Math.max(skipThroughput, predicateThroughput) * 0.98 ||
      adaptiveThroughput < skipThroughput * 0.98
    ) {
      concurrencyThroughputPassed = false;
      reasons.push(`${cell}: adaptive throughput missed the fixed-arm gate`);
    }

    for (const blockRows of completedBlocks) {
      const skipRow = blockRows.find((row) => row.arm === 'skip')!;
      const adaptiveRow = blockRows.find((row) => row.arm === 'adaptive')!;
      const amplification = adaptiveRow.physical_bytes - skipRow.physical_bytes;
      if (amplification <= 0) continue;
      const amplificationCap = Math.max(64 * 1_024, skipRow.physical_bytes * 0.02);
      const latencyImproved = adaptiveRow.duration_ms <= skipRow.duration_ms * 0.95;
      const throughputImproved =
        adaptiveRow.throughput_queries_per_second >= skipRow.throughput_queries_per_second * 1.05;
      if (amplification > amplificationCap || (!latencyImproved && !throughputImproved)) {
        perCellBytesPassed = false;
        reasons.push(`${cell}: adaptive byte amplification exceeded its per-query allowance`);
        break;
      }
    }
  }
  const adaptiveChoiceAccuracy =
    separatedChoices === 0 || correctSeparatedChoices / separatedChoices >= 0.95;
  if (!adaptiveChoiceAccuracy) reasons.push('adaptive fixed-arm choice accuracy was below 95%');

  const totalBytes = (arm: PageIndexExperimentArm) =>
    observations.filter((row) => row.arm === arm).reduce((sum, row) => sum + row.physical_bytes, 0);
  const skipBytes = totalBytes('skip');
  const adaptiveBytes = totalBytes('adaptive');
  const aggregateBytes = adaptiveBytes <= skipBytes * 1.02 && perCellBytesPassed;
  if (!aggregateBytes) reasons.push('aggregate adaptive physical bytes exceeded Skip by 2%');

  const retainedHeapAndFailures = [...groups.entries()].every(([cell, rows]) => {
    const skipRows = rows.filter((row) => row.arm === 'skip');
    const adaptiveRows = rows.filter((row) => row.arm === 'adaptive');
    const skipHeap = percentile(
      skipRows.map((row) => row.retained_heap_bytes),
      0.95,
    );
    const adaptiveHeap = percentile(
      adaptiveRows.map((row) => row.retained_heap_bytes),
      0.95,
    );
    const passed =
      adaptiveRows.every((row) => !row.failed && !row.cancelled) && adaptiveHeap <= skipHeap;
    if (!passed) reasons.push(`${cell}: retained heap, failure, or cancellation regressed`);
    return passed;
  });

  const gates = {
    correctness_and_ownership: correctnessAndOwnership,
    decision_budget: decisionBudget,
    skip_zero_index_io: skipZeroIndexIo,
    adaptive_choice_accuracy: adaptiveChoiceAccuracy,
    regret: regretPassed,
    cell_regression: cellRegressionPassed,
    holdout_sample_count: holdoutSampleCountPassed,
    concurrency_throughput: concurrencyThroughputPassed,
    aggregate_bytes: aggregateBytes,
    retained_heap_and_failures: retainedHeapAndFailures,
  };
  return { passed: Object.values(gates).every(Boolean), gates, reasons };
}

export function cellId(cell: PageIndexExperimentCell): string {
  return sha256(stableJson(cell)).slice(0, 24);
}

function coveringArray(seed: string, cells: PageIndexExperimentCell[]): CoveringArray {
  const body = { seed, cells };
  return { ...body, hash: sha256(stableJson(body)) };
}

function allCells(): PageIndexExperimentCell[] {
  const cells: PageIndexExperimentCell[] = [];
  for (const layout of PAGE_INDEX_DIMENSIONS.layout) {
    for (const selectivity_basis_points of PAGE_INDEX_DIMENSIONS.selectivity_basis_points) {
      for (const geometry of PAGE_INDEX_DIMENSIONS.geometry) {
        for (const projection of PAGE_INDEX_DIMENSIONS.projection) {
          for (const connection of PAGE_INDEX_DIMENSIONS.connection) {
            for (const metadata_cache of PAGE_INDEX_DIMENSIONS.metadata_cache) {
              for (const concurrency of PAGE_INDEX_DIMENSIONS.concurrency) {
                for (const network_profile of PAGE_INDEX_DIMENSIONS.network_profile) {
                  cells.push({
                    layout,
                    selectivity_basis_points,
                    geometry,
                    projection,
                    connection,
                    metadata_cache,
                    concurrency,
                    network_profile,
                  });
                }
              }
            }
          }
        }
      }
    }
  }
  return cells;
}

function allPairKeys(): Set<string> {
  const pairs = new Set<string>();
  for (let leftIndex = 0; leftIndex < DIMENSION_NAMES.length; leftIndex += 1) {
    for (let rightIndex = leftIndex + 1; rightIndex < DIMENSION_NAMES.length; rightIndex += 1) {
      const leftName = DIMENSION_NAMES[leftIndex]!;
      const rightName = DIMENSION_NAMES[rightIndex]!;
      for (const leftValue of PAGE_INDEX_DIMENSIONS[leftName]) {
        for (const rightValue of PAGE_INDEX_DIMENSIONS[rightName]) {
          pairs.add(pairKey(leftName, leftValue, rightName, rightValue));
        }
      }
    }
  }
  return pairs;
}

function cellPairKeys(cell: PageIndexExperimentCell): string[] {
  const pairs: string[] = [];
  for (let leftIndex = 0; leftIndex < DIMENSION_NAMES.length; leftIndex += 1) {
    for (let rightIndex = leftIndex + 1; rightIndex < DIMENSION_NAMES.length; rightIndex += 1) {
      const leftName = DIMENSION_NAMES[leftIndex]!;
      const rightName = DIMENSION_NAMES[rightIndex]!;
      pairs.push(pairKey(leftName, cell[leftName], rightName, cell[rightName]));
    }
  }
  return pairs;
}

function pairKey(
  leftName: keyof DimensionValues,
  leftValue: unknown,
  rightName: keyof DimensionValues,
  rightValue: unknown,
): string {
  return `${leftName}=${String(leftValue)}|${rightName}=${String(rightValue)}`;
}

function percentile(values: readonly number[], fraction: number): number {
  if (values.length === 0) return Number.POSITIVE_INFINITY;
  const sorted = [...values].sort((left, right) => left - right);
  const index = Math.max(0, Math.ceil(sorted.length * fraction) - 1);
  return sorted[index]!;
}

function upperQuantileConfidenceIndex(
  sampleCount: number,
  quantile: number,
  confidence: number,
): number {
  if (sampleCount <= 0) return 0;
  const z = confidence === 0.95 ? 1.645 : 1.96;
  const estimate = sampleCount * quantile + z * Math.sqrt(sampleCount * quantile * (1 - quantile));
  return Math.min(sampleCount - 1, Math.max(0, Math.ceil(estimate) - 1));
}

function stableJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(',')}]`;
  if (value && typeof value === 'object') {
    const object = value as Record<string, unknown>;
    return `{${Object.keys(object)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${stableJson(object[key])}`)
      .join(',')}}`;
  }
  return JSON.stringify(value);
}

function sha256(value: string): string {
  return createHash('sha256').update(value).digest('hex');
}

function requireNonNegativeFinite(value: number, name: string): void {
  if (!Number.isFinite(value) || value < 0) {
    throw new TypeError(`${name} must be a non-negative finite number`);
  }
}
