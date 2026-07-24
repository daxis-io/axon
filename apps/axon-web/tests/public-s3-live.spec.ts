import { readFile, writeFile } from 'node:fs/promises';

import { expect, test, type APIRequestContext, type Page } from '@playwright/test';

import {
  buildPublicDeltaLogManifest,
  parsePublicObjectStorageTableRoot,
  publicObjectUrl,
} from '../src/services/object-storage.ts';

const liveTableUri = process.env.AXON_LIVE_PUBLIC_S3_TABLE_URI;
const liveRegion = process.env.AXON_LIVE_PUBLIC_S3_REGION;
const liveOrigin =
  process.env.AXON_LIVE_PUBLIC_S3_ORIGIN ??
  new URL(process.env.PLAYWRIGHT_BASE_URL ?? 'https://127.0.0.1:5173').origin;
const queryEvidenceCaptureKey = '__AXON_PUBLIC_S3_QUERY_EVIDENCE__';
const browserSafeCursorPendingEncodedBytes = 8 * 1024 * 1024;
const browserSafeCursorTransportChunkBytes = 1024 * 1024;
const requiredLiveMetricKeys = [
  'bytes_fetched',
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
  'identity_present_range_reads',
  'identity_missing_range_reads',
  'range_cache_hits',
  'range_cache_misses',
  'range_cache_bytes_reused',
  'range_cache_bytes_stored',
  'range_cache_validation_misses',
  'range_cache_degraded_identity_reads',
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
] as const;
const comparisonMetricKeys = requiredLiveMetricKeys;

type RequiredLiveMetricKey = (typeof requiredLiveMetricKeys)[number];
type ComparisonMetricKey = (typeof comparisonMetricKeys)[number];
type LiveMetricsInput = Partial<Record<RequiredLiveMetricKey, number>>;
type ComparisonMetrics = Record<ComparisonMetricKey, number | null>;
type ProjectedOwnedMemory = {
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
type ProjectedExecution = {
  executed_on: 'browser_wasm';
  fallback_event_observed: false;
  response_fallback_reason: null;
};
type CapturedQueryEvidence = {
  metrics: LiveMetricsInput;
  ownedMemory: unknown;
  execution: unknown;
};
const currentMainBaseCommit = 'ee6a430afe99144c5e5780952b45a335d15e89c3';
const verified20260716ArtifactSha256 =
  '0dbda0ae8f7018f739fbaf57897aebc1dfa5083927c8bc6691f9a494424a7152';
const pinnedFixtureRowCount = 1_048_576;
const preCacheComparison: ComparisonMetrics = {
  bytes_fetched: 22_677_645,
  bootstrap_footer_range_reads: null,
  scan_footer_range_reads: null,
  scan_data_range_reads: 160,
  duplicate_range_reads: null,
  coalesced_range_reads: 32,
  coalesced_gap_bytes_fetched: null,
  scan_overfetch_bytes: null,
  footer_cache_hits: null,
  footer_cache_misses: null,
  footer_range_reads_avoided: null,
  identity_present_range_reads: null,
  identity_missing_range_reads: null,
  range_cache_hits: null,
  range_cache_misses: null,
  range_cache_bytes_reused: null,
  range_cache_bytes_stored: null,
  range_cache_validation_misses: null,
  range_cache_degraded_identity_reads: null,
  range_readahead_requests: null,
  range_readahead_bytes_fetched: null,
  range_readahead_bytes_used: null,
  range_readahead_wasted_bytes: null,
  rows_emitted: 1_048_576,
  arrow_ipc_bytes: 36_744,
  arrow_ipc_chunk_count: null,
  coordinator_peak_staged_bytes: null,
  coordinator_staging_limit_bytes: null,
  cursor_peak_pending_encoded_bytes: null,
  cursor_peak_transport_chunk_bytes: null,
};
const verified20260716Comparison: ComparisonMetrics = {
  bytes_fetched: 22_677_645,
  bootstrap_footer_range_reads: 16,
  scan_footer_range_reads: 0,
  scan_data_range_reads: 160,
  duplicate_range_reads: 0,
  coalesced_range_reads: 32,
  coalesced_gap_bytes_fetched: 0,
  scan_overfetch_bytes: null,
  footer_cache_hits: 8,
  footer_cache_misses: 8,
  footer_range_reads_avoided: 16,
  identity_present_range_reads: 160,
  identity_missing_range_reads: 16,
  range_cache_hits: 0,
  range_cache_misses: 128,
  range_cache_bytes_reused: 0,
  range_cache_bytes_stored: 22_677_645,
  range_cache_validation_misses: 0,
  range_cache_degraded_identity_reads: 0,
  range_readahead_requests: 0,
  range_readahead_bytes_fetched: 0,
  range_readahead_bytes_used: 0,
  range_readahead_wasted_bytes: 0,
  rows_emitted: 1_048_576,
  arrow_ipc_bytes: 36_744,
  arrow_ipc_chunk_count: 1,
  coordinator_peak_staged_bytes: null,
  coordinator_staging_limit_bytes: null,
  cursor_peak_pending_encoded_bytes: null,
  cursor_peak_transport_chunk_bytes: null,
};
type PublicS3LiveRunEvidence = {
  run: number;
  scalar_result: string;
  metrics: ProjectedLiveMetrics;
  owned_memory: ProjectedOwnedMemory;
  execution: ProjectedExecution;
};
type ProjectedLiveMetrics = Record<RequiredLiveMetricKey, number>;
type PublicS3EvidenceBase = {
  table_uri: string;
  table_name: string;
  browser_name: string;
  base_url: string;
  region: string;
  fixture_provenance?: PublicS3FixtureProvenance;
};
type PublicS3FixtureProvenance = {
  fixture_revision: string;
  table_uri: string;
  region: string;
  manifest_sha256: string;
  object_checksums_sha256: string;
  required_object_count: number;
  active_file_count: number;
  active_data_bytes: number;
};
type PublicS3PerformanceEvidence = PublicS3EvidenceBase & {
  metrics: ProjectedLiveMetrics;
  owned_memory: ProjectedOwnedMemory;
  execution: ProjectedExecution;
  comparison: {
    pre_cache: {
      metrics: ComparisonMetrics;
      owned_memory: null;
    };
    verified_2026_07_16: {
      artifact_sha256: string;
      metrics: ComparisonMetrics;
      owned_memory: null;
    };
    current_main: {
      base_commit_sha: string;
      metrics: Record<ComparisonMetricKey, number>;
      owned_memory: ProjectedOwnedMemory;
    };
  };
};
type PublicS3RepeatEvidence = PublicS3EvidenceBase & {
  repeat_count: number;
  runs: PublicS3LiveRunEvidence[];
};

const pinnedFixtureProvenance: PublicS3FixtureProvenance = {
  fixture_revision: 's3-browser-perf-v1',
  table_uri: 's3://axon-public-s3-fixture-452456948477/fixtures/s3-browser-perf/table',
  region: 'us-east-2',
  manifest_sha256: '18d1c4c3b5e1ce78ce156ce51247a94a46e44401cad9688ec0d14ceaa01b6ab3',
  object_checksums_sha256: '05f6c5823a88c49559eef70072165b584dfe3c320ae8a435c6f6f82f30d719a9',
  required_object_count: 21,
  active_file_count: 8,
  active_data_bytes: 82_057_700,
};

const exampleLiveMetrics: LiveMetricsInput = {
  ...completeLiveMetrics(),
  bytes_fetched: 42,
  bootstrap_footer_range_reads: 1,
  scan_footer_range_reads: 2,
  scan_data_range_reads: 3,
  duplicate_range_reads: 4,
  coalesced_range_reads: 5,
  coalesced_gap_bytes_fetched: 6,
  footer_cache_hits: 7,
  footer_cache_misses: 8,
  footer_range_reads_avoided: 9,
  identity_present_range_reads: 10,
  identity_missing_range_reads: 11,
  rows_emitted: 12,
  arrow_ipc_bytes: 13,
  arrow_ipc_chunk_count: 14,
  range_cache_hits: 15,
  range_cache_misses: 16,
  range_cache_bytes_reused: 17,
  range_cache_bytes_stored: 18,
  range_cache_validation_misses: 19,
  range_cache_degraded_identity_reads: 20,
  range_readahead_requests: 21,
  range_readahead_bytes_fetched: 22,
  range_readahead_bytes_used: 23,
  range_readahead_wasted_bytes: 24,
  scan_overfetch_bytes: 25,
  coordinator_peak_staged_bytes: 26,
  coordinator_staging_limit_bytes: 260,
  cursor_peak_pending_encoded_bytes: 28,
  cursor_peak_transport_chunk_bytes: 29,
};
const exampleOwnedMemory = {
  coordinator: {
    limit_bytes: 1_000,
    reserved_bytes: 0,
    staged_bytes: 0,
    peak_reserved_bytes: 100,
    peak_staged_bytes: 200,
  },
  datafusion: {
    limit_bytes: 2_000,
    reserved_bytes: 0,
    peak_bytes: 500,
  },
};
const exampleExecution = {
  executed_on: 'browser_wasm',
  fallback_event_observed: false,
  response_fallback_reason: null,
};

test('public S3 repeat evidence redacts URI secrets and preserves per-run metrics', () => {
  const evidence = buildPublicS3RepeatEvidence({
    tableUri:
      's3://embedded-user:embedded-password@live-bucket/customer/path/table?X-Amz-Signature=signed-secret&token=query-secret#fragment-secret',
    tableName: 'table',
    browserName: 'chromium',
    baseURL: 'https://127.0.0.1:5173',
    region: 'us-east-2',
    runs: [
      {
        run: 1,
        scalar_result: '42',
        metrics: exampleLiveMetrics,
        ownedMemory: exampleOwnedMemory,
        execution: exampleExecution,
      },
      {
        run: 2,
        scalar_result: '42',
        metrics: {
          ...completeLiveMetrics(),
          bytes_fetched: 24,
          bootstrap_footer_range_reads: 1,
          scan_footer_range_reads: 1,
          scan_data_range_reads: 1,
          duplicate_range_reads: 0,
          coalesced_range_reads: 0,
          coalesced_gap_bytes_fetched: 0,
          footer_cache_hits: 0,
          footer_cache_misses: 1,
          footer_range_reads_avoided: 0,
          identity_present_range_reads: 2,
          identity_missing_range_reads: 0,
          rows_emitted: 12,
        },
        ownedMemory: exampleOwnedMemory,
        execution: exampleExecution,
      },
    ],
  });

  expect(evidence.table_uri).toBe('s3://live-bucket/customer/path/table');
  expect(evidence.table_name).toBe('table');
  expect(evidence.browser_name).toBe('chromium');
  expect(evidence.base_url).toBe('https://127.0.0.1:5173');
  expect(evidence.region).toBe('us-east-2');
  expect(evidence.repeat_count).toBe(2);
  expect(evidence.runs).toHaveLength(2);
  expect(evidence.runs[0]).toEqual({
    run: 1,
    scalar_result: '42',
    metrics: {
      bytes_fetched: 42,
      bootstrap_footer_range_reads: 1,
      scan_footer_range_reads: 2,
      scan_data_range_reads: 3,
      duplicate_range_reads: 4,
      coalesced_range_reads: 5,
      coalesced_gap_bytes_fetched: 6,
      footer_cache_hits: 7,
      footer_cache_misses: 8,
      footer_range_reads_avoided: 9,
      identity_present_range_reads: 10,
      identity_missing_range_reads: 11,
      range_cache_hits: 15,
      range_cache_misses: 16,
      range_cache_bytes_reused: 17,
      range_cache_bytes_stored: 18,
      range_cache_validation_misses: 19,
      range_cache_degraded_identity_reads: 20,
      range_readahead_requests: 21,
      range_readahead_bytes_fetched: 22,
      range_readahead_bytes_used: 23,
      range_readahead_wasted_bytes: 24,
      scan_overfetch_bytes: 25,
      rows_emitted: 12,
      arrow_ipc_bytes: 13,
      arrow_ipc_chunk_count: 14,
      coordinator_peak_staged_bytes: 26,
      coordinator_staging_limit_bytes: 260,
      cursor_peak_pending_encoded_bytes: 28,
      cursor_peak_transport_chunk_bytes: 29,
    },
    owned_memory: exampleOwnedMemory,
    execution: exampleExecution,
  });
  expect(evidence.runs[1]).toMatchObject({
    run: 2,
    scalar_result: '42',
    metrics: { bytes_fetched: 24, rows_emitted: 12 },
    owned_memory: exampleOwnedMemory,
    execution: exampleExecution,
  });
  const serializedEvidence = JSON.stringify(evidence);
  expect(serializedEvidence).not.toContain('embedded-user');
  expect(serializedEvidence).not.toContain('embedded-password');
  expect(serializedEvidence).not.toContain('X-Amz-Signature');
  expect(serializedEvidence).not.toContain('signed-secret');
  expect(serializedEvidence).not.toContain('query-secret');
  expect(serializedEvidence).not.toContain('fragment-secret');
});

test('public S3 performance evidence preserves comparison metrics', () => {
  const evidence = buildPublicS3PerformanceEvidence({
    tableUri: 's3://live-bucket/customer/path/table',
    tableName: 'table',
    browserName: 'chromium',
    baseURL: 'https://127.0.0.1:5173',
    region: 'us-east-2',
    metrics: exampleLiveMetrics,
    ownedMemory: exampleOwnedMemory,
    execution: exampleExecution,
  });

  expect(evidence.metrics).toEqual({
    bytes_fetched: 42,
    bootstrap_footer_range_reads: 1,
    scan_footer_range_reads: 2,
    scan_data_range_reads: 3,
    duplicate_range_reads: 4,
    coalesced_range_reads: 5,
    coalesced_gap_bytes_fetched: 6,
    footer_cache_hits: 7,
    footer_cache_misses: 8,
    footer_range_reads_avoided: 9,
    identity_present_range_reads: 10,
    identity_missing_range_reads: 11,
    range_cache_hits: 15,
    range_cache_misses: 16,
    range_cache_bytes_reused: 17,
    range_cache_bytes_stored: 18,
    range_cache_validation_misses: 19,
    range_cache_degraded_identity_reads: 20,
    range_readahead_requests: 21,
    range_readahead_bytes_fetched: 22,
    range_readahead_bytes_used: 23,
    range_readahead_wasted_bytes: 24,
    scan_overfetch_bytes: 25,
    rows_emitted: 12,
    arrow_ipc_bytes: 13,
    arrow_ipc_chunk_count: 14,
    coordinator_peak_staged_bytes: 26,
    coordinator_staging_limit_bytes: 260,
    cursor_peak_pending_encoded_bytes: 28,
    cursor_peak_transport_chunk_bytes: 29,
  });
  expect(evidence.comparison).toEqual({
    pre_cache: {
      metrics: {
        bytes_fetched: 22_677_645,
        bootstrap_footer_range_reads: null,
        scan_footer_range_reads: null,
        scan_data_range_reads: 160,
        duplicate_range_reads: null,
        coalesced_range_reads: 32,
        coalesced_gap_bytes_fetched: null,
        scan_overfetch_bytes: null,
        footer_cache_hits: null,
        footer_cache_misses: null,
        footer_range_reads_avoided: null,
        identity_present_range_reads: null,
        identity_missing_range_reads: null,
        range_cache_hits: null,
        range_cache_misses: null,
        range_cache_bytes_reused: null,
        range_cache_bytes_stored: null,
        range_cache_validation_misses: null,
        range_cache_degraded_identity_reads: null,
        range_readahead_requests: null,
        range_readahead_bytes_fetched: null,
        range_readahead_bytes_used: null,
        range_readahead_wasted_bytes: null,
        rows_emitted: 1_048_576,
        arrow_ipc_bytes: 36_744,
        arrow_ipc_chunk_count: null,
        coordinator_peak_staged_bytes: null,
        coordinator_staging_limit_bytes: null,
        cursor_peak_pending_encoded_bytes: null,
        cursor_peak_transport_chunk_bytes: null,
      },
      owned_memory: null,
    },
    verified_2026_07_16: {
      artifact_sha256: '0dbda0ae8f7018f739fbaf57897aebc1dfa5083927c8bc6691f9a494424a7152',
      metrics: {
        bytes_fetched: 22_677_645,
        bootstrap_footer_range_reads: 16,
        scan_footer_range_reads: 0,
        scan_data_range_reads: 160,
        duplicate_range_reads: 0,
        coalesced_range_reads: 32,
        coalesced_gap_bytes_fetched: 0,
        scan_overfetch_bytes: null,
        footer_cache_hits: 8,
        footer_cache_misses: 8,
        footer_range_reads_avoided: 16,
        identity_present_range_reads: 160,
        identity_missing_range_reads: 16,
        range_cache_hits: 0,
        range_cache_misses: 128,
        range_cache_bytes_reused: 0,
        range_cache_bytes_stored: 22_677_645,
        range_cache_validation_misses: 0,
        range_cache_degraded_identity_reads: 0,
        range_readahead_requests: 0,
        range_readahead_bytes_fetched: 0,
        range_readahead_bytes_used: 0,
        range_readahead_wasted_bytes: 0,
        rows_emitted: 1_048_576,
        arrow_ipc_bytes: 36_744,
        arrow_ipc_chunk_count: 1,
        coordinator_peak_staged_bytes: null,
        coordinator_staging_limit_bytes: null,
        cursor_peak_pending_encoded_bytes: null,
        cursor_peak_transport_chunk_bytes: null,
      },
      owned_memory: null,
    },
    current_main: {
      base_commit_sha: 'ee6a430afe99144c5e5780952b45a335d15e89c3',
      metrics: {
        bytes_fetched: 42,
        bootstrap_footer_range_reads: 1,
        scan_footer_range_reads: 2,
        scan_data_range_reads: 3,
        duplicate_range_reads: 4,
        coalesced_range_reads: 5,
        coalesced_gap_bytes_fetched: 6,
        scan_overfetch_bytes: 25,
        footer_cache_hits: 7,
        footer_cache_misses: 8,
        footer_range_reads_avoided: 9,
        identity_present_range_reads: 10,
        identity_missing_range_reads: 11,
        range_cache_hits: 15,
        range_cache_misses: 16,
        range_cache_bytes_reused: 17,
        range_cache_bytes_stored: 18,
        range_cache_validation_misses: 19,
        range_cache_degraded_identity_reads: 20,
        range_readahead_requests: 21,
        range_readahead_bytes_fetched: 22,
        range_readahead_bytes_used: 23,
        range_readahead_wasted_bytes: 24,
        rows_emitted: 12,
        arrow_ipc_bytes: 13,
        arrow_ipc_chunk_count: 14,
        coordinator_peak_staged_bytes: 26,
        coordinator_staging_limit_bytes: 260,
        cursor_peak_pending_encoded_bytes: 28,
        cursor_peak_transport_chunk_bytes: 29,
      },
      owned_memory: exampleOwnedMemory,
    },
  });
});

test('pinned public S3 repeat evidence requires the exact fixture row count', () => {
  const input = {
    tableUri: pinnedFixtureProvenance.table_uri,
    tableName: 'table',
    browserName: 'chromium',
    baseURL: 'https://127.0.0.1:5173',
    region: pinnedFixtureProvenance.region,
    runs: [
      {
        run: 1,
        scalar_result: '1048575',
        metrics: exampleLiveMetrics,
        ownedMemory: exampleOwnedMemory,
        execution: exampleExecution,
      },
    ],
  };

  expect(() => buildPublicS3RepeatEvidence(input)).toThrow('1,048,576');
  expect(
    buildPublicS3RepeatEvidence({
      ...input,
      runs: [
        {
          run: 1,
          scalar_result: '1048576',
          metrics: exampleLiveMetrics,
          ownedMemory: exampleOwnedMemory,
          execution: exampleExecution,
        },
      ],
    }).runs[0]?.scalar_result,
  ).toBe('1048576');
});

test('public S3 evidence rejects serialized AWS credential and signed-query material', () => {
  expect(() =>
    buildPublicS3PerformanceEvidence({
      tableUri: pinnedFixtureProvenance.table_uri,
      tableName: 'table',
      browserName: 'chromium',
      baseURL:
        'https://127.0.0.1:5173/?X-Amz-Credential=AKIAIOSFODNN7EXAMPLE&X-Amz-Signature=signed-secret',
      region: pinnedFixtureProvenance.region,
      metrics: exampleLiveMetrics,
      ownedMemory: exampleOwnedMemory,
      execution: exampleExecution,
    }),
  ).toThrow(/credential|signed-query/i);
  expect(() =>
    buildPublicS3PerformanceEvidence({
      tableUri: pinnedFixtureProvenance.table_uri,
      tableName: 'table',
      browserName: 'chromium',
      baseURL: 'https://127.0.0.1:5173/?X-Amz-Unmodeled-Signer-Field=secret',
      region: pinnedFixtureProvenance.region,
      metrics: exampleLiveMetrics,
      ownedMemory: exampleOwnedMemory,
      execution: exampleExecution,
    }),
  ).toThrow(/credential|signed-query/i);
});

test('public S3 evidence requires zero terminal owned memory with bounded peaks', () => {
  const buildWithOwnedMemory = (ownedMemory: unknown) =>
    buildPublicS3PerformanceEvidence({
      tableUri: pinnedFixtureProvenance.table_uri,
      tableName: 'table',
      browserName: 'chromium',
      baseURL: 'https://127.0.0.1:5173',
      region: pinnedFixtureProvenance.region,
      metrics: exampleLiveMetrics,
      ownedMemory,
      execution: exampleExecution,
    });

  const evidence = buildWithOwnedMemory(exampleOwnedMemory);
  expect(evidence.owned_memory).toEqual(exampleOwnedMemory);
  expect(evidence.comparison.pre_cache.owned_memory).toBeNull();
  expect(evidence.comparison.verified_2026_07_16.owned_memory).toBeNull();
  expect(evidence.comparison.current_main.owned_memory).toEqual(exampleOwnedMemory);

  const numericCases = [
    {
      key: 'coordinator.limit_bytes',
      withValue: (value: number) => ({
        ...exampleOwnedMemory,
        coordinator: { ...exampleOwnedMemory.coordinator, limit_bytes: value },
      }),
    },
    {
      key: 'coordinator.reserved_bytes',
      withValue: (value: number) => ({
        ...exampleOwnedMemory,
        coordinator: { ...exampleOwnedMemory.coordinator, reserved_bytes: value },
      }),
    },
    {
      key: 'coordinator.staged_bytes',
      withValue: (value: number) => ({
        ...exampleOwnedMemory,
        coordinator: { ...exampleOwnedMemory.coordinator, staged_bytes: value },
      }),
    },
    {
      key: 'coordinator.peak_reserved_bytes',
      withValue: (value: number) => ({
        ...exampleOwnedMemory,
        coordinator: { ...exampleOwnedMemory.coordinator, peak_reserved_bytes: value },
      }),
    },
    {
      key: 'coordinator.peak_staged_bytes',
      withValue: (value: number) => ({
        ...exampleOwnedMemory,
        coordinator: { ...exampleOwnedMemory.coordinator, peak_staged_bytes: value },
      }),
    },
    {
      key: 'datafusion.limit_bytes',
      withValue: (value: number) => ({
        ...exampleOwnedMemory,
        datafusion: { ...exampleOwnedMemory.datafusion, limit_bytes: value },
      }),
    },
    {
      key: 'datafusion.reserved_bytes',
      withValue: (value: number) => ({
        ...exampleOwnedMemory,
        datafusion: { ...exampleOwnedMemory.datafusion, reserved_bytes: value },
      }),
    },
    {
      key: 'datafusion.peak_bytes',
      withValue: (value: number) => ({
        ...exampleOwnedMemory,
        datafusion: { ...exampleOwnedMemory.datafusion, peak_bytes: value },
      }),
    },
  ];
  for (const { key, withValue } of numericCases) {
    for (const invalidValue of [
      -1,
      1.5,
      Number.MAX_SAFE_INTEGER + 1,
      Number.NaN,
      Number.POSITIVE_INFINITY,
    ]) {
      expect(() => buildWithOwnedMemory(withValue(invalidValue))).toThrow(key);
    }
  }

  expect(() =>
    buildWithOwnedMemory({
      ...exampleOwnedMemory,
      coordinator: { ...exampleOwnedMemory.coordinator, reserved_bytes: 1 },
    }),
  ).toThrow(/coordinator.*reserved_bytes.*zero/i);
  expect(() =>
    buildWithOwnedMemory({
      ...exampleOwnedMemory,
      coordinator: { ...exampleOwnedMemory.coordinator, staged_bytes: 1 },
    }),
  ).toThrow(/coordinator.*staged_bytes.*zero/i);
  expect(() =>
    buildWithOwnedMemory({
      ...exampleOwnedMemory,
      datafusion: { ...exampleOwnedMemory.datafusion, reserved_bytes: 1 },
    }),
  ).toThrow(/datafusion.*reserved_bytes.*zero/i);
  expect(() =>
    buildWithOwnedMemory({
      ...exampleOwnedMemory,
      coordinator: {
        ...exampleOwnedMemory.coordinator,
        peak_reserved_bytes: exampleOwnedMemory.coordinator.limit_bytes + 1,
      },
    }),
  ).toThrow(/peak_reserved_bytes.*limit_bytes/i);
  expect(() =>
    buildWithOwnedMemory({
      ...exampleOwnedMemory,
      coordinator: {
        ...exampleOwnedMemory.coordinator,
        peak_staged_bytes: exampleOwnedMemory.coordinator.limit_bytes + 1,
      },
    }),
  ).toThrow(/peak_staged_bytes.*limit_bytes/i);
  expect(() =>
    buildWithOwnedMemory({
      ...exampleOwnedMemory,
      datafusion: {
        ...exampleOwnedMemory.datafusion,
        peak_bytes: exampleOwnedMemory.datafusion.limit_bytes + 1,
      },
    }),
  ).toThrow(/datafusion.*peak_bytes.*limit_bytes/i);
  expect(() =>
    buildWithOwnedMemory({
      coordinator: exampleOwnedMemory.coordinator,
    }),
  ).toThrow('datafusion');
});

test('public S3 evidence requires successful browser WASM execution without fallback', () => {
  const buildWithExecution = (execution: unknown) =>
    buildPublicS3PerformanceEvidence({
      tableUri: pinnedFixtureProvenance.table_uri,
      tableName: 'table',
      browserName: 'chromium',
      baseURL: 'https://127.0.0.1:5173',
      region: pinnedFixtureProvenance.region,
      metrics: exampleLiveMetrics,
      ownedMemory: exampleOwnedMemory,
      execution,
    });

  expect(buildWithExecution(exampleExecution).execution).toEqual(exampleExecution);
  expect(() => buildWithExecution({ ...exampleExecution, executed_on: 'native' })).toThrow(
    'browser_wasm',
  );
  expect(() => buildWithExecution({ ...exampleExecution, fallback_event_observed: true })).toThrow(
    /fallback event/i,
  );
  expect(() =>
    buildWithExecution({
      ...exampleExecution,
      response_fallback_reason: { code: 'browser_runtime_constraint' },
    }),
  ).toThrow(/fallback reason/i);
});

test('public S3 performance evidence identifies the pinned fixture revision', async () => {
  const evidence = buildPublicS3PerformanceEvidence({
    tableUri: `${pinnedFixtureProvenance.table_uri}?X-Amz-Signature=secret`,
    tableName: 'table',
    browserName: 'chromium',
    baseURL: 'https://127.0.0.1:5173',
    region: pinnedFixtureProvenance.region,
    metrics: exampleLiveMetrics,
    ownedMemory: exampleOwnedMemory,
    execution: exampleExecution,
  });
  const trackedProvenance = JSON.parse(
    await readFile(
      new URL('../public/fixtures/s3-perf/s3-perf-provenance.json', import.meta.url),
      'utf8',
    ),
  ) as PublicS3FixtureProvenance;

  expect(evidence.fixture_provenance).toEqual(pinnedFixtureProvenance);
  expect(trackedProvenance).toMatchObject(pinnedFixtureProvenance);
  expect(JSON.stringify(evidence)).not.toContain('X-Amz-Signature');
  expect(JSON.stringify(evidence)).not.toContain('secret');
});

test('public S3 performance evidence requires the exact pinned fixture URI and region', () => {
  expect(
    isPinnedPerformanceFixture(
      `${pinnedFixtureProvenance.table_uri}?X-Amz-Signature=redacted`,
      pinnedFixtureProvenance.region,
    ),
  ).toBe(true);
  expect(
    isPinnedPerformanceFixture(
      's3://lookalike-bucket/fixtures/s3-browser-perf/table',
      pinnedFixtureProvenance.region,
    ),
  ).toBe(false);
  expect(isPinnedPerformanceFixture(pinnedFixtureProvenance.table_uri, 'us-east-1')).toBe(false);
});

test('public S3 query evidence correlates the latest success without inheriting fallback', () => {
  const requestAMetrics = { context: { request_id: 'request-a' }, bytes_fetched: 1 };
  const requestAOwnedMemory = { context: { request_id: 'request-a' }, marker: 'a' };
  const requestBMetrics = { context: { request_id: 'request-b' }, bytes_fetched: 2 };
  const requestBOwnedMemory = { context: { request_id: 'request-b' }, marker: 'b' };

  expect(
    correlateCapturedQueryEvidence([
      { kind: 'range_read_metrics', value: requestAMetrics },
      { kind: 'owned_memory_metrics', value: requestAOwnedMemory },
      {
        kind: 'success',
        request_id: 'request-a',
        executed_on: 'browser_wasm',
        response_fallback_reason: null,
      },
      { kind: 'fallback', request_id: 'request-a' },
      { kind: 'range_read_metrics', value: requestBMetrics },
      { kind: 'owned_memory_metrics', value: requestBOwnedMemory },
      {
        kind: 'success',
        request_id: 'request-b',
        executed_on: 'browser_wasm',
        response_fallback_reason: null,
      },
    ]),
  ).toEqual({
    metrics: requestBMetrics,
    ownedMemory: requestBOwnedMemory,
    execution: {
      executed_on: 'browser_wasm',
      fallback_event_observed: false,
      response_fallback_reason: null,
    },
  });
});

test('public S3 live evidence requires finite nonnegative cache, readahead, and IPC metrics', () => {
  const requiredComparisonMetricKeys = [
    'range_cache_hits',
    'range_cache_misses',
    'range_cache_bytes_reused',
    'range_cache_bytes_stored',
    'range_cache_validation_misses',
    'range_cache_degraded_identity_reads',
    'range_readahead_requests',
    'range_readahead_bytes_fetched',
    'range_readahead_bytes_used',
    'range_readahead_wasted_bytes',
    'scan_overfetch_bytes',
    'arrow_ipc_bytes',
    'arrow_ipc_chunk_count',
    'coordinator_peak_staged_bytes',
    'coordinator_staging_limit_bytes',
    'cursor_peak_pending_encoded_bytes',
    'cursor_peak_transport_chunk_bytes',
  ] as const satisfies readonly RequiredLiveMetricKey[];

  for (const key of requiredComparisonMetricKeys) {
    const missing = completeLiveMetrics();
    delete missing[key];
    expect(() => buildEvidenceWithMetrics(missing)).toThrow(key);

    for (const invalidValue of [
      -1,
      1.5,
      Number.MAX_SAFE_INTEGER + 1,
      Number.NaN,
      Number.POSITIVE_INFINITY,
    ]) {
      expect(() =>
        buildEvidenceWithMetrics({ ...completeLiveMetrics(), [key]: invalidValue }),
      ).toThrow(key);
    }
  }
});

test('public S3 live evidence rejects a coordinator peak above its staging limit', () => {
  expect(() =>
    buildEvidenceWithMetrics({
      ...completeLiveMetrics(),
      coordinator_peak_staged_bytes: 65,
      coordinator_staging_limit_bytes: 64,
    }),
  ).toThrow(/coordinator_peak_staged_bytes.*coordinator_staging_limit_bytes/i);
});

test('public S3 live evidence rejects a cursor pending peak above 8 MiB', () => {
  expect(() =>
    buildEvidenceWithMetrics({
      ...completeLiveMetrics(),
      cursor_peak_pending_encoded_bytes: browserSafeCursorPendingEncodedBytes + 1,
    }),
  ).toThrow(/cursor_peak_pending_encoded_bytes.*8 MiB/i);
});

test('public S3 live evidence rejects a cursor transport chunk peak above 1 MiB', () => {
  expect(() =>
    buildEvidenceWithMetrics({
      ...completeLiveMetrics(),
      cursor_peak_transport_chunk_bytes: browserSafeCursorTransportChunkBytes + 1,
    }),
  ).toThrow(/cursor_peak_transport_chunk_bytes.*1 MiB/i);
});

test.describe('public S3 live smoke', () => {
  test.skip(
    !liveTableUri || !liveRegion,
    'set AXON_LIVE_PUBLIC_S3_TABLE_URI and AXON_LIVE_PUBLIC_S3_REGION to run live public S3 smoke',
  );

  test('public S3 Delta table root supports anonymous list, log read, and range read', async ({
    request,
  }) => {
    const root = parsePublicObjectStorageTableRoot({
      provider: 's3',
      tableUri: liveTableUri!,
      region: liveRegion!,
    });
    const manifest = await buildPublicDeltaLogManifest(root);
    expect(manifest.objects.length).toBeGreaterThan(0);

    const addPath = await firstAddPathFromDeltaLogs(request, manifest.objects, liveOrigin);
    const dataResponse = await request.get(publicObjectUrl(root, addPath), {
      headers: {
        Origin: liveOrigin,
        Range: 'bytes=0-15',
      },
    });
    expect(dataResponse.status()).toBe(206);
    expect(dataResponse.headers()['content-range']).toContain('bytes 0-15/');
    expectCorsAllowsOrigin(dataResponse.headers(), liveOrigin);
    expect(
      Buffer.from(await dataResponse.body())
        .subarray(0, 4)
        .toString('utf8'),
    ).toBe('PAR1');
  });

  test('app repeats a live public S3 query across fresh browser runtimes', async ({
    page,
    browserName,
    baseURL,
  }, testInfo) => {
    testInfo.setTimeout(240_000);
    const tableName = tableNameFromTableUri(liveTableUri!);
    const repeatedQuery = `SELECT COUNT(*) AS row_count FROM ${quoteSqlIdentifier(tableName)}`;
    const runtimeErrors = captureRuntimeErrors(page);
    const runs: Array<
      CapturedQueryEvidence & {
        run: number;
        scalar_result: string;
      }
    > = [];
    let expectedScalarResult: string | undefined;

    await installQueryEvidenceCapture(page);
    await connectPublicS3Table(page);
    for (let run = 1; run <= 3; run += 1) {
      if (run > 1) await page.reload();

      await selectPersistedPublicTable(page, tableName);
      const runtimeErrorStart = runtimeErrors.length;
      await page.locator('.code-input').fill(repeatedQuery);
      await page.locator('.btn.primary', { hasText: 'Run' }).click();

      await expect(page.locator('.res-meta')).toContainText(/browser · wasm/i, {
        timeout: 60_000,
      });
      await expect(page.locator('table.grid')).toContainText('row_count');
      const scalarResult = (
        await page.locator('table.grid tbody tr').first().locator('td').last().innerText()
      ).trim();
      expect(scalarResult, `run ${run} returned a scalar COUNT(*) result`).not.toBe('');
      expectedScalarResult ??= scalarResult;
      expect(scalarResult, `run ${run} matched the first COUNT(*) result`).toBe(
        expectedScalarResult,
      );
      if (redactTableUri(liveTableUri!) === pinnedFixtureProvenance.table_uri) {
        expect(scalarResult, `run ${run} matched the pinned fixture row count`).toBe(
          String(pinnedFixtureRowCount),
        );
      }
      await expect(page.locator('.results')).not.toContainText(
        /(?:parquet|decode|worker).*(?:error|failed)|(?:error|failed).*(?:parquet|decode|worker)/i,
      );
      expect(
        runtimeErrors
          .slice(runtimeErrorStart)
          .filter((message) => /parquet|decode|worker/i.test(message)),
        `run ${run} emitted no Parquet, decode, or worker errors`,
      ).toEqual([]);

      const queryEvidence = await latestCapturedQueryEvidence(page);
      runs.push({
        run,
        scalar_result: scalarResult,
        ...queryEvidence,
      });
    }

    const evidence = buildPublicS3RepeatEvidence({
      tableUri: liveTableUri!,
      tableName,
      browserName,
      baseURL: baseURL ?? liveOrigin,
      region: liveRegion!,
      runs,
    });
    const artifactPath = testInfo.outputPath('public-s3-repeat-query-evidence.json');
    await writeFile(artifactPath, `${JSON.stringify(evidence, null, 2)}\n`, 'utf8');
    await testInfo.attach('public-s3-repeat-query-evidence', {
      path: artifactPath,
      contentType: 'application/json',
    });
  });

  test('performance fixture records cache and readahead comparison evidence', async ({
    page,
    browserName,
    baseURL,
  }, testInfo) => {
    test.skip(
      !isPinnedPerformanceFixture(liveTableUri!, liveRegion!),
      'set the exact pinned public S3 performance fixture URI and region for performance evidence',
    );
    testInfo.setTimeout(240_000);
    const tableName = tableNameFromTableUri(liveTableUri!);

    await installQueryEvidenceCapture(page);
    await connectPublicS3Table(page);
    await page.locator('.code-input').fill(`
SELECT event_id, event_ts, region, customer_id, amount, status
FROM ${quoteSqlIdentifier(tableName)}
WHERE amount > 100 AND status IN ('paid', 'shipped')
ORDER BY event_ts
LIMIT 1000
`);
    await page.locator('.btn.primary', { hasText: 'Run' }).click();

    await expect(page.locator('.res-meta')).toContainText(/browser · wasm/i, {
      timeout: 90_000,
    });
    await expect(page.locator('table.grid')).toContainText('event_id');
    await expect(page.locator('table.grid')).toContainText('amount');

    const queryEvidence = await latestCapturedQueryEvidence(page);
    const evidence = buildPublicS3PerformanceEvidence({
      tableUri: liveTableUri!,
      tableName,
      browserName,
      baseURL: baseURL ?? liveOrigin,
      region: liveRegion!,
      ...queryEvidence,
    });
    expect(evidence.fixture_provenance).toEqual(pinnedFixtureProvenance);
    const artifactPath = testInfo.outputPath('public-s3-live-uat-evidence.json');
    await writeFile(artifactPath, `${JSON.stringify(evidence, null, 2)}\n`, 'utf8');
    await testInfo.attach('public-s3-live-uat-evidence', {
      path: artifactPath,
      contentType: 'application/json',
    });
    expect(evidence.metrics.bytes_fetched).toBeGreaterThan(0);
    expect(evidence.metrics.scan_data_range_reads).toBeGreaterThan(0);
    expect(evidence.metrics.rows_emitted).toBeGreaterThan(0);
    expect(
      evidence.metrics.range_readahead_wasted_bytes,
      'readahead waste must not exceed bytes subsequently used',
    ).toBeLessThanOrEqual(evidence.metrics.range_readahead_bytes_used);
  });
});

function captureRuntimeErrors(page: Page): string[] {
  const errors: string[] = [];
  page.on('pageerror', (error) => errors.push(error.message));
  page.on('console', (message) => {
    if (message.type() === 'error') errors.push(message.text());
  });
  return errors;
}

async function connectPublicS3Table(page: Page): Promise<void> {
  await page.goto('/');
  await page.getByRole('button', { name: /^Connect$/ }).click();
  const sourceDialog = page.getByRole('dialog', { name: 'Connect a Delta source' });
  await sourceDialog.locator('.cc-source-row', { hasText: 'Object storage' }).click();
  await sourceDialog.getByRole('button', { name: /Continue/ }).click();

  const configDialog = page.getByRole('dialog', { name: 'Connect to object storage' });
  await configDialog.getByRole('button', { name: /AWS S3/ }).click();
  await configDialog
    .locator('.cc-input.mono.has-prefix')
    .fill(liveTableUri!.replace(/^s3:\/\//, ''));
  await configDialog.locator('select.cc-select').selectOption(liveRegion!);
  await configDialog.getByRole('button', { name: 'Test connection' }).click();
  await expect(configDialog).toContainText(/source check passed/i, { timeout: 60_000 });
  await configDialog.getByRole('button', { name: /Discover tables/ }).click();

  const reviewDialog = page.getByRole('dialog', { name: 'Review & name catalog' });
  const recommended = reviewDialog.getByLabel('Use recommended organization');
  if (await recommended.isChecked()) await recommended.uncheck();
  await reviewDialog.getByLabel('Catalog alias').fill('live-public-s3');
  await reviewDialog.getByRole('button', { name: /Connect catalog/ }).click();

  await expect(page.locator('.conn-pill')).toContainText('live-public-s3', {
    timeout: 30_000,
  });
}

async function selectPersistedPublicTable(page: Page, tableName: string): Promise<void> {
  await expect(page.locator('.conn-pill')).toContainText('live-public-s3', {
    timeout: 30_000,
  });
  await page.locator('.conn-pill').click();
  const panel = page.getByRole('dialog', { name: 'Connected catalogs' });
  const activateTable = panel.getByRole('button', {
    name: `Activate live-public-s3 default ${tableName}`,
  });
  if (!(await activateTable.isVisible())) {
    await panel.getByRole('button', { name: 'Expand live-public-s3' }).click();
  }
  await expect(activateTable).toBeEnabled();
  await activateTable.click();
  await expect(page.locator('.queryref-bar .qref')).toContainText(tableName);
}

async function installQueryEvidenceCapture(page: Page): Promise<void> {
  await page.addInitScript((captureKey) => {
    const scope = window as typeof window & Record<string, unknown>;
    const captured: unknown[] = [];
    Object.defineProperty(scope, captureKey, {
      value: captured,
      configurable: true,
    });

    const OriginalWorker = window.Worker;
    class InstrumentedWorker extends OriginalWorker {
      constructor(scriptURL: string | URL, options?: WorkerOptions) {
        super(scriptURL, options);
        this.addEventListener('message', (event: MessageEvent<unknown>) => {
          const data = event.data;
          if (!data || typeof data !== 'object') return;
          if (
            'range_read_metrics' in data &&
            data.range_read_metrics &&
            typeof data.range_read_metrics === 'object'
          ) {
            captured.push({ kind: 'range_read_metrics', value: data.range_read_metrics });
          }
          if (
            'owned_memory_metrics' in data &&
            data.owned_memory_metrics &&
            typeof data.owned_memory_metrics === 'object'
          ) {
            captured.push({ kind: 'owned_memory_metrics', value: data.owned_memory_metrics });
          }
          if ('fallback' in data && data.fallback && typeof data.fallback === 'object') {
            const context =
              'context' in data.fallback &&
              data.fallback.context &&
              typeof data.fallback.context === 'object'
                ? data.fallback.context
                : undefined;
            captured.push({
              kind: 'fallback',
              request_id: context && 'request_id' in context ? context.request_id : undefined,
            });
          }
          if ('success' in data && data.success && typeof data.success === 'object') {
            const response =
              'response' in data.success &&
              data.success.response &&
              typeof data.success.response === 'object'
                ? data.success.response
                : undefined;
            captured.push({
              kind: 'success',
              request_id: 'request_id' in data.success ? data.success.request_id : undefined,
              executed_on: response && 'executed_on' in response ? response.executed_on : undefined,
              response_fallback_reason:
                response && 'fallback_reason' in response
                  ? (response.fallback_reason ?? null)
                  : null,
            });
          }
        });
      }
    }

    Object.defineProperty(window, 'Worker', {
      value: InstrumentedWorker,
      configurable: true,
      writable: true,
    });
  }, queryEvidenceCaptureKey);
}

async function latestCapturedQueryEvidence(page: Page): Promise<CapturedQueryEvidence> {
  await page.waitForFunction(
    (captureKey) => {
      const captured = (window as typeof window & Record<string, unknown>)[captureKey];
      if (!Array.isArray(captured)) return false;
      const records = captured.filter(
        (value): value is Record<string, unknown> =>
          Boolean(value) && typeof value === 'object' && !Array.isArray(value),
      );
      const success = records.filter((record) => record.kind === 'success').at(-1);
      const requestId = success?.request_id;
      if (typeof requestId !== 'string') return false;
      const matchesRequest = (record: Record<string, unknown>) => {
        const value = record.value;
        if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
        const context = (value as Record<string, unknown>).context;
        return (
          Boolean(context) &&
          typeof context === 'object' &&
          !Array.isArray(context) &&
          (context as Record<string, unknown>).request_id === requestId
        );
      };
      return (
        records.some((record) => record.kind === 'range_read_metrics' && matchesRequest(record)) &&
        records.some((record) => record.kind === 'owned_memory_metrics' && matchesRequest(record))
      );
    },
    queryEvidenceCaptureKey,
    { timeout: 5_000 },
  );
  const captured = await page.evaluate(
    (captureKey) => (window as typeof window & Record<string, unknown>)[captureKey],
    queryEvidenceCaptureKey,
  );
  const queryEvidence = correlateCapturedQueryEvidence(captured);
  expect(
    queryEvidence,
    'browser worker emitted correlated range, owned-memory, and success evidence for the live query',
  ).toBeTruthy();
  return queryEvidence as CapturedQueryEvidence;
}

function correlateCapturedQueryEvidence(captured: unknown): CapturedQueryEvidence | null {
  if (!Array.isArray(captured)) return null;
  const records = captured.filter(
    (value): value is Record<string, unknown> =>
      Boolean(value) && typeof value === 'object' && !Array.isArray(value),
  );
  const success = records.filter((record) => record.kind === 'success').at(-1);
  const requestId = success?.request_id;
  if (!success || typeof requestId !== 'string') return null;
  const matchesRequest = (record: Record<string, unknown>) => {
    const value = record.value;
    if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
    const context = (value as Record<string, unknown>).context;
    return (
      Boolean(context) &&
      typeof context === 'object' &&
      !Array.isArray(context) &&
      (context as Record<string, unknown>).request_id === requestId
    );
  };
  const metrics = records
    .filter((record) => record.kind === 'range_read_metrics' && matchesRequest(record))
    .at(-1)?.value;
  const ownedMemory = records
    .filter((record) => record.kind === 'owned_memory_metrics' && matchesRequest(record))
    .at(-1)?.value;
  if (!metrics || !ownedMemory) return null;
  return {
    metrics: metrics as LiveMetricsInput,
    ownedMemory,
    execution: {
      executed_on: success.executed_on,
      fallback_event_observed: records.some(
        (record) => record.kind === 'fallback' && record.request_id === requestId,
      ),
      response_fallback_reason: success.response_fallback_reason,
    },
  };
}

function buildPublicS3PerformanceEvidence(input: {
  tableUri: string;
  tableName: string;
  browserName: string;
  baseURL: string;
  region: string;
  metrics: LiveMetricsInput;
  ownedMemory: unknown;
  execution: unknown;
}): PublicS3PerformanceEvidence {
  const metrics = projectLiveMetrics(input.metrics);
  const ownedMemory = projectOwnedMemory(input.ownedMemory);
  const execution = projectExecution(input.execution);
  const currentComparison = Object.fromEntries(
    comparisonMetricKeys.map((key) => [key, metrics[key]]),
  ) as Record<ComparisonMetricKey, number>;
  const evidence: PublicS3PerformanceEvidence = {
    ...buildEvidenceBase(input),
    metrics,
    owned_memory: ownedMemory,
    execution,
    comparison: {
      pre_cache: {
        metrics: preCacheComparison,
        owned_memory: null,
      },
      verified_2026_07_16: {
        artifact_sha256: verified20260716ArtifactSha256,
        metrics: verified20260716Comparison,
        owned_memory: null,
      },
      current_main: {
        base_commit_sha: currentMainBaseCommit,
        metrics: currentComparison,
        owned_memory: ownedMemory,
      },
    },
  };
  assertArtifactContainsNoSecrets(evidence);
  return evidence;
}

function buildPublicS3RepeatEvidence(input: {
  tableUri: string;
  tableName: string;
  browserName: string;
  baseURL: string;
  region: string;
  runs: Array<{
    run: number;
    scalar_result: string;
    metrics: LiveMetricsInput;
    ownedMemory: unknown;
    execution: unknown;
  }>;
}): PublicS3RepeatEvidence {
  const evidence: PublicS3RepeatEvidence = {
    ...buildEvidenceBase(input),
    repeat_count: input.runs.length,
    runs: input.runs.map((run) => ({
      run: run.run,
      scalar_result: run.scalar_result,
      metrics: projectLiveMetrics(run.metrics),
      owned_memory: projectOwnedMemory(run.ownedMemory),
      execution: projectExecution(run.execution),
    })),
  };
  if (evidence.fixture_provenance) {
    for (const run of evidence.runs) {
      if (run.scalar_result !== String(pinnedFixtureRowCount)) {
        throw new Error(
          `pinned public S3 fixture COUNT(*) must equal 1,048,576; run ${run.run} returned ${run.scalar_result}`,
        );
      }
    }
  }
  assertArtifactContainsNoSecrets(evidence);
  return evidence;
}

function buildEvidenceBase(input: {
  tableUri: string;
  tableName: string;
  browserName: string;
  baseURL: string;
  region: string;
}): PublicS3EvidenceBase {
  const tableUri = redactTableUri(input.tableUri);
  const fixtureProvenance = isPinnedPerformanceFixture(tableUri, input.region)
    ? pinnedFixtureProvenance
    : undefined;
  return {
    table_uri: tableUri,
    table_name: input.tableName,
    browser_name: input.browserName,
    base_url: input.baseURL,
    region: input.region,
    ...(fixtureProvenance === undefined ? {} : { fixture_provenance: fixtureProvenance }),
  };
}

function projectLiveMetrics(metrics: LiveMetricsInput): PublicS3LiveRunEvidence['metrics'] {
  const projected = Object.fromEntries(
    requiredLiveMetricKeys.map((key) => [key, requiredMetric(metrics, key)]),
  ) as PublicS3LiveRunEvidence['metrics'];
  assertLiveMetricBounds(projected);
  return projected;
}

function requiredMetric(metrics: LiveMetricsInput, key: RequiredLiveMetricKey): number {
  const value = metrics[key];
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) {
    throw new Error(
      `live public S3 evidence requires finite nonnegative safe-integer metric '${key}'`,
    );
  }
  return value;
}

function assertLiveMetricBounds(metrics: PublicS3LiveRunEvidence['metrics']): void {
  if (metrics.coordinator_peak_staged_bytes > metrics.coordinator_staging_limit_bytes) {
    throw new Error(
      "live public S3 evidence requires 'coordinator_peak_staged_bytes' to not exceed 'coordinator_staging_limit_bytes'",
    );
  }
  if (metrics.cursor_peak_pending_encoded_bytes > browserSafeCursorPendingEncodedBytes) {
    throw new Error(
      "live public S3 evidence requires 'cursor_peak_pending_encoded_bytes' to not exceed 8 MiB",
    );
  }
  if (metrics.cursor_peak_transport_chunk_bytes > browserSafeCursorTransportChunkBytes) {
    throw new Error(
      "live public S3 evidence requires 'cursor_peak_transport_chunk_bytes' to not exceed 1 MiB",
    );
  }
}

function projectOwnedMemory(input: unknown): ProjectedOwnedMemory {
  const ownedMemory = requiredObject(input, 'owned_memory');
  const coordinatorInput = requiredObject(ownedMemory.coordinator, 'owned_memory.coordinator');
  const datafusionInput = requiredObject(ownedMemory.datafusion, 'owned_memory.datafusion');
  const coordinator = {
    limit_bytes: requiredSafeInteger(
      coordinatorInput.limit_bytes,
      'owned_memory.coordinator.limit_bytes',
    ),
    reserved_bytes: requiredSafeInteger(
      coordinatorInput.reserved_bytes,
      'owned_memory.coordinator.reserved_bytes',
    ),
    staged_bytes: requiredSafeInteger(
      coordinatorInput.staged_bytes,
      'owned_memory.coordinator.staged_bytes',
    ),
    peak_reserved_bytes: requiredSafeInteger(
      coordinatorInput.peak_reserved_bytes,
      'owned_memory.coordinator.peak_reserved_bytes',
    ),
    peak_staged_bytes: requiredSafeInteger(
      coordinatorInput.peak_staged_bytes,
      'owned_memory.coordinator.peak_staged_bytes',
    ),
  };
  const datafusion = {
    limit_bytes: requiredSafeInteger(
      datafusionInput.limit_bytes,
      'owned_memory.datafusion.limit_bytes',
    ),
    reserved_bytes: requiredSafeInteger(
      datafusionInput.reserved_bytes,
      'owned_memory.datafusion.reserved_bytes',
    ),
    peak_bytes: requiredSafeInteger(
      datafusionInput.peak_bytes,
      'owned_memory.datafusion.peak_bytes',
    ),
  };

  if (coordinator.reserved_bytes !== 0) {
    throw new Error('owned_memory.coordinator.reserved_bytes must be zero at terminal');
  }
  if (coordinator.staged_bytes !== 0) {
    throw new Error('owned_memory.coordinator.staged_bytes must be zero at terminal');
  }
  if (datafusion.reserved_bytes !== 0) {
    throw new Error('owned_memory.datafusion.reserved_bytes must be zero at terminal');
  }
  if (coordinator.peak_reserved_bytes > coordinator.limit_bytes) {
    throw new Error('owned_memory.coordinator.peak_reserved_bytes must not exceed limit_bytes');
  }
  if (coordinator.peak_staged_bytes > coordinator.limit_bytes) {
    throw new Error('owned_memory.coordinator.peak_staged_bytes must not exceed limit_bytes');
  }
  if (datafusion.peak_bytes > datafusion.limit_bytes) {
    throw new Error('owned_memory.datafusion.peak_bytes must not exceed limit_bytes');
  }

  return { coordinator, datafusion };
}

function projectExecution(input: unknown): ProjectedExecution {
  const execution = requiredObject(input, 'execution');
  if (execution.executed_on !== 'browser_wasm') {
    throw new Error("live public S3 evidence requires executed_on 'browser_wasm'");
  }
  if (execution.fallback_event_observed !== false) {
    throw new Error('live public S3 evidence requires no fallback event');
  }
  if (
    execution.response_fallback_reason !== null &&
    execution.response_fallback_reason !== undefined
  ) {
    throw new Error('live public S3 evidence requires no response fallback reason');
  }
  return {
    executed_on: 'browser_wasm',
    fallback_event_observed: false,
    response_fallback_reason: null,
  };
}

function requiredObject(value: unknown, path: string): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(`${path} must be an object`);
  }
  return value as Record<string, unknown>;
}

function requiredSafeInteger(value: unknown, path: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${path} must be a finite nonnegative safe integer`);
  }
  return value;
}

function assertArtifactContainsNoSecrets(
  evidence: PublicS3PerformanceEvidence | PublicS3RepeatEvidence,
): void {
  const serialized = JSON.stringify(evidence);
  const forbiddenPatterns = [
    /:\/\/[^/"\s]+:[^/@\s]+@/,
    /\b(?:AKIA|ASIA)[0-9A-Z]{16}\b/,
    /aws_(?:access_key_id|secret_access_key|session_token)/i,
    /x-amz-[a-z0-9-]+/i,
    /(?:^|[?&])(?:credential|security-token|signature|token)=/i,
  ];
  if (forbiddenPatterns.some((pattern) => pattern.test(serialized))) {
    throw new Error('serialized public S3 evidence contains credential or signed-query material');
  }
}

function completeLiveMetrics(): LiveMetricsInput {
  return Object.fromEntries(requiredLiveMetricKeys.map((key) => [key, 0])) as LiveMetricsInput;
}

function buildEvidenceWithMetrics(metrics: LiveMetricsInput): PublicS3PerformanceEvidence {
  return buildPublicS3PerformanceEvidence({
    tableUri: 's3://live-bucket/customer/path/table',
    tableName: 'table',
    browserName: 'chromium',
    baseURL: 'https://127.0.0.1:5173',
    region: 'us-east-2',
    metrics,
    ownedMemory: exampleOwnedMemory,
    execution: exampleExecution,
  });
}

function redactTableUri(tableUri: string): string {
  try {
    const url = new URL(tableUri);
    url.username = '';
    url.password = '';
    url.search = '';
    url.hash = '';
    return url.toString();
  } catch {
    return tableUri.split(/[?#]/, 1)[0];
  }
}

function isPinnedPerformanceFixture(tableUri: string, region: string): boolean {
  return (
    redactTableUri(tableUri) === pinnedFixtureProvenance.table_uri &&
    region === pinnedFixtureProvenance.region
  );
}

function tableNameFromTableUri(tableUri: string): string {
  try {
    const url = new URL(redactTableUri(tableUri));
    return url.pathname.split('/').filter(Boolean).at(-1) ?? 'public_table';
  } catch {
    return redactTableUri(tableUri).split('/').filter(Boolean).at(-1) ?? 'public_table';
  }
}

async function firstAddPathFromDeltaLogs(
  request: APIRequestContext,
  objects: Array<{ relative_path: string; url: string }>,
  origin: string,
): Promise<string> {
  const jsonLogs = objects.filter((object) => object.relative_path.endsWith('.json'));
  expect(
    jsonLogs.length,
    'live table must expose at least one JSON Delta log object',
  ).toBeGreaterThan(0);

  for (const logObject of jsonLogs) {
    const logResponse = await request.get(logObject.url, {
      headers: { Origin: origin },
    });
    expect(logResponse.status()).toBe(200);
    expectCorsAllowsOrigin(logResponse.headers(), origin);
    const addPath = addPathFromDeltaLog(await logResponse.text());
    if (addPath) return addPath;
  }

  throw new Error('Delta log objects did not contain an add action');
}

function addPathFromDeltaLog(log: string): string | undefined {
  for (const line of log.split('\n')) {
    if (!line.trim()) continue;
    const action = JSON.parse(line) as { add?: { path?: unknown } };
    if (typeof action.add?.path === 'string') return action.add.path;
  }
  return undefined;
}

function expectCorsAllowsOrigin(headers: Record<string, string>, origin: string): void {
  expect([origin, '*']).toContain(headers['access-control-allow-origin']);
}

function quoteSqlIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}
