import { createHash } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';

import {
  LATIN_SQUARE_ARM_ORDERS,
  PAGE_INDEX_BOUNDARY_HOLDOUT_BLOCKS,
  PAGE_INDEX_CALIBRATION_BLOCKS,
  PAGE_INDEX_EWMA_COEFFICIENT,
  PAGE_INDEX_HOLDOUT_BLOCKS,
  PAGE_INDEX_MIN_DECODE_SAMPLES,
  PAGE_INDEX_MIN_RANGE_SAMPLES,
  PAGE_INDEX_MODEL_FORMULA,
  PAGE_INDEX_MODEL_VERSION,
  buildPinnedCoveringArrays,
} from '../tests/support/page-index-adaptive-experiment.ts';

const outputPath = resolve(
  process.env.AXON_PAGE_INDEX_EXPERIMENT_PLAN_PATH ??
    '../../target/page-index-adaptive/experiment-plan.json',
);
const arrays = buildPinnedCoveringArrays();
const artifact = {
  schema_version: 1,
  fixture_revision: 's3-browser-perf-page-index-v2',
  immutable_prefix: 'fixtures/s3-browser-perf-page-index-v2',
  phase: 'pre_calibration',
  model: {
    version: PAGE_INDEX_MODEL_VERSION,
    formula: PAGE_INDEX_MODEL_FORMULA,
    ewma_coefficient: PAGE_INDEX_EWMA_COEFFICIENT,
    minimum_uncached_range_samples: PAGE_INDEX_MIN_RANGE_SAMPLES,
    minimum_decode_samples: PAGE_INDEX_MIN_DECODE_SAMPLES,
    no_artificial_probes: true,
    session_persistence: false,
    calibration_error_margin_us: null,
    frozen_model_hash: null,
  },
  calibration: {
    paired_blocks_per_cell: PAGE_INDEX_CALIBRATION_BLOCKS,
    covering_array: arrays.calibration,
  },
  holdout: {
    ordinary_blocks_per_cell: PAGE_INDEX_HOLDOUT_BLOCKS,
    boundary_blocks_per_cell: PAGE_INDEX_BOUNDARY_HOLDOUT_BLOCKS,
    latin_square_orders: LATIN_SQUARE_ARM_ORDERS,
    covering_array: arrays.holdout,
    readable_before_model_freeze: false,
  },
  browsers: {
    chromium: 'controlled shaped matrix',
    firefox: 'correctness and unshaped public-S3 holdout',
    webkit: 'correctness and unshaped public-S3 holdout',
    independent_enablement: true,
  },
  stopped_gates: {
    fixture_upload: 'requires separate explicit cloud authorization',
    shaped_preflight: 'requires a validated external network shaper',
    public_s3_holdout: 'requires the immutable uploaded fixture and completed preflight',
    canary: 'requires a consenting trusted host and signed hash-pinned telemetry artifact',
  },
};
const serialized = `${JSON.stringify(artifact, null, 2)}\n`;
const sha256 = createHash('sha256').update(serialized).digest('hex');
await mkdir(dirname(outputPath), { recursive: true });
await writeFile(outputPath, serialized, 'utf8');
await writeFile(`${outputPath}.sha256`, `${sha256}  ${outputPath}\n`, 'utf8');
console.log(`Wrote pinned adaptive page-index experiment plan: ${outputPath}`);
console.log(`SHA-256: ${sha256}`);
