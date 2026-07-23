import { Buffer } from 'node:buffer';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import {
  fromJson,
  toJson,
  type DescField,
  type DescMessage,
  type JsonValue,
} from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';
import type {
  BrowserHttpFileDescriptor,
  BrowserHttpParquetDatasetDescriptor,
  BrowserHttpSnapshotDescriptor,
  BrowserWorkerCommand,
  BrowserWorkerDisposeCommand,
  BrowserWorkerEventContext,
  BrowserWorkerEventEnvelope,
  BrowserWorkerOpenParquetDatasetCommand,
  BrowserWorkerOpenTableCommand,
  BrowserWorkerRangeReadMetricsEvent,
  BrowserWorkerResultPreview,
  BrowserWorkerSqlCommand,
  CapabilityReport,
  FallbackReason,
  ParquetInspectionColumn,
  ParquetInspectionColumnChunk,
  ParquetInspectionRowGroup,
  ParquetInspectionSummary,
  QueryError,
  QueryMetricsSummary,
  QueryRequest,
  QueryResponse,
  WireArrowIpcResult,
  WireBrowserWorkerResponseEnvelope,
} from '../../axon-browser-sdk.ts';
import {
  BrowserHttpFileDescriptorSchema,
  BrowserHttpParquetDatasetDescriptorSchema,
  BrowserHttpSnapshotDescriptorSchema,
} from './protobuf/axon/dataaccess/v1/dataaccess_pb.ts';
import {
  ArrowIpcResultSchema,
  BrowserWorkerCommandSchema,
  BrowserWorkerEventEnvelopeSchema,
  BrowserWorkerResponseEnvelopeSchema,
  CancelResponseSchema,
  ExecuteRequestSchema,
  ExecuteResponseSchema,
  ExecutionTarget,
  OpenableDescriptorSchema,
  PreviewRequestSchema,
  PreviewResponseSchema,
  PreviewCellSchema,
  QueryEngine,
  QueryRequestSchema,
  ResultPreviewSchema,
  file_axon_exec_v1_exec,
} from './protobuf/axon/exec/v1/exec_pb.ts';

type JsonObject = { [key: string]: JsonValue };

type LegacySqlCommand = Omit<BrowserWorkerSqlCommand, 'query'> & {
  query?: QueryRequest;
  request?: QueryRequest;
};

type LegacyWorkerCommand = BrowserWorkerCommand | { sql: LegacySqlCommand };

type ContractQueryRequest = Omit<QueryRequest, 'preferred_target'> & {
  preferred_target: QueryRequest['preferred_target'] | 'remote_service';
};

const HANDOFF_EXAMPLE_DIR = resolve(
  '../../docs/program/browser-lakehouse-release-handoff-examples',
);

const QUERY_METRIC_UINT64_FIELDS = [
  'bytes_fetched',
  'duration_ms',
  'files_touched',
  'files_skipped',
  'prebootstrap_fail_open_count',
  'prebootstrap_files_pruned',
  'footer_reads_avoided',
  'prebootstrap_candidate_files',
  'row_groups_touched',
  'row_groups_skipped',
  'footer_reads',
  'bootstrap_footer_range_reads',
  'scan_footer_range_reads',
  'scan_data_range_reads',
  'duplicate_range_reads',
  'coalesced_range_reads',
  'coalesced_gap_bytes_fetched',
  'scan_overfetch_bytes',
  'coordinator_peak_staged_bytes',
  'coordinator_staging_limit_bytes',
  'cursor_peak_pending_encoded_bytes',
  'cursor_peak_transport_chunk_bytes',
  'footer_cache_hits',
  'footer_cache_misses',
  'footer_range_reads_avoided',
  'footer_cache_degraded_identity_reads',
  'identity_present_range_reads',
  'identity_missing_range_reads',
  'descriptor_resolution_count',
  'delta_log_manifest_list_count',
  'delta_log_manifest_list_duration_ms',
  'snapshot_resolve_count',
  'snapshot_resolve_duration_ms',
  'descriptor_cache_hit',
  'session_reuse_count',
  'opened_table_reuse_count',
  'identity_refresh_count',
  'access_envelope_refresh_count',
  'rows_emitted',
  'snapshot_bootstrap_duration_ms',
  'arrow_ipc_bytes',
  'arrow_ipc_chunk_count',
  'preview_rows',
  'preview_string_bytes',
  'planning_duration_ms',
  'arrow_ipc_encode_duration_ms',
  'preview_duration_ms',
] as const;

describe('execution contract codegen', () => {
  it('normalizes all seven legacy worker command arms through the protobuf oneof', () => {
    const openTable = readExample<{ open_table: BrowserWorkerOpenTableCommand }>(
      'browser-worker-command.open-table.json',
    );
    const openParquet = readExample<{
      open_parquet_dataset: BrowserWorkerOpenParquetDatasetCommand;
    }>('browser-worker-command.open-parquet-dataset.json');
    const sql = readExample<{ sql: LegacySqlCommand }>('browser-worker-request.supported.json');
    const dispose = readExample<{ dispose: BrowserWorkerDisposeCommand }>(
      'browser-worker-command.dispose.json',
    );
    const commands: Array<{
      expectedCase: string;
      expectedJsonName: string;
      legacy: LegacyWorkerCommand;
    }> = [
      { expectedCase: 'openTable', expectedJsonName: 'open_table', legacy: openTable },
      {
        expectedCase: 'openDeltaTable',
        expectedJsonName: 'open_delta_table',
        legacy: {
          open_delta_table: {
            request_id: 'req-open-delta-001',
            name: 'supported_table',
            snapshot: openTable.open_table.snapshot,
          },
        },
      },
      {
        expectedCase: 'openParquetDataset',
        expectedJsonName: 'open_parquet_dataset',
        legacy: openParquet,
      },
      {
        expectedCase: 'inspectParquet',
        expectedJsonName: 'inspect_parquet',
        legacy: {
          inspect_parquet: {
            request_id: 'req-inspect-001',
            name: 'supported_table',
            path: 'part-000.parquet',
          },
        },
      },
      { expectedCase: 'sql', expectedJsonName: 'sql', legacy: sql },
      {
        expectedCase: 'cancel',
        expectedJsonName: 'cancel',
        legacy: {
          cancel: {
            request_id: 'req-cancel-001',
            query_id: 'req-supported-001',
          },
        },
      },
      { expectedCase: 'dispose', expectedJsonName: 'dispose', legacy: dispose },
    ];

    for (const { expectedCase, expectedJsonName, legacy } of commands) {
      const normalized = normalizeWorkerCommand(legacy);
      const decoded = fromJson(BrowserWorkerCommandSchema, normalized);
      const protobufJson = toJson(BrowserWorkerCommandSchema, decoded);

      expect(decoded.command.case).toBe(expectedCase);
      expect(protobufJson).toHaveProperty(expectedJsonName);
      expect(Object.keys(requiredJsonObject(protobufJson))).toEqual([expectedJsonName]);
    }
  });

  it('uses canonical query plus legacy worker defaults for the checked-in SQL handoff', () => {
    const legacy = readExample<{ sql: LegacySqlCommand }>('browser-worker-request.supported.json');
    const decoded = fromJson(BrowserWorkerCommandSchema, normalizeWorkerCommand(legacy));
    const protobufJson = toJson(BrowserWorkerCommandSchema, decoded);

    expect(protobufJson).toMatchObject({
      sql: {
        request_id: 'req-supported-001',
        query: {
          preferred_target: 'EXECUTION_TARGET_BROWSER_WASM',
          options: {
            include_explain: false,
            collect_metrics: true,
          },
        },
        output: 'BROWSER_WORKER_SQL_OUTPUT_ARROW_IPC_STREAM',
        delivery: 'BROWSER_WORKER_SQL_DELIVERY_SINGLE_BUFFER',
        browser_safe_defaults: false,
      },
    });
    expect(protobufJson).not.toHaveProperty('sql.request');
  });

  it('preserves pagination, execution-option defaults, explicit zeros, and runtime-limit absence', () => {
    const withoutRuntimeLimits: QueryRequest = {
      table_uri: 'gs://axon-fixtures/partitioned-table',
      snapshot_version: 0,
      sql: 'SELECT * FROM axon_table',
      preferred_target: 'browser_wasm',
      options: {
        include_explain: false,
        collect_metrics: false,
        result_page: { limit: 0, offset: 0 },
      },
    };
    const decodedWithoutRuntimeLimits = fromJson(
      QueryRequestSchema,
      normalizeQueryRequest(withoutRuntimeLimits),
    );
    const jsonWithoutRuntimeLimits = toJson(QueryRequestSchema, decodedWithoutRuntimeLimits);

    expect(decodedWithoutRuntimeLimits.snapshotVersion).toBe(0n);
    expect(decodedWithoutRuntimeLimits.options?.includeExplain).toBe(false);
    expect(decodedWithoutRuntimeLimits.options?.collectMetrics).toBe(false);
    expect(decodedWithoutRuntimeLimits.options?.resultPage?.limit).toBe(0n);
    expect(decodedWithoutRuntimeLimits.options?.runtimeLimits).toBeUndefined();
    expect(jsonWithoutRuntimeLimits).toMatchObject({
      snapshot_version: '0',
      options: {
        include_explain: false,
        collect_metrics: false,
        result_page: { limit: '0', offset: '0' },
      },
    });
    expect(jsonWithoutRuntimeLimits).not.toHaveProperty('options.runtime_limits');

    const withExplicitZeroLimits: QueryRequest = {
      ...withoutRuntimeLimits,
      options: {
        ...withoutRuntimeLimits.options,
        runtime_limits: {
          max_result_rows: 0,
          max_arrow_ipc_bytes: 0,
          max_preview_string_bytes: 0,
          max_scan_bytes: 0,
          max_scan_overfetch_bytes: 0,
        },
      },
    };
    const zeroJson = toJson(
      QueryRequestSchema,
      fromJson(QueryRequestSchema, normalizeQueryRequest(withExplicitZeroLimits)),
    );

    expect(zeroJson).toMatchObject({
      options: {
        runtime_limits: {
          max_result_rows: '0',
          max_arrow_ipc_bytes: '0',
          max_preview_string_bytes: '0',
          max_scan_bytes: '0',
          max_scan_overfetch_bytes: '0',
        },
      },
    });
  });

  it('round-trips the reserved remote-service execution target', () => {
    const query: ContractQueryRequest = {
      table_uri: 's3://axon-fixtures/remote-table',
      sql: 'SELECT 1',
      preferred_target: 'remote_service',
    };
    const decoded = fromJson(QueryRequestSchema, normalizeQueryRequest(query));
    const protobufJson = toJson(QueryRequestSchema, decoded);

    expect(ExecutionTarget.REMOTE_SERVICE).toBe(3);
    expect(decoded.preferredTarget).toBe(ExecutionTarget.REMOTE_SERVICE);
    expect(protobufJson).toHaveProperty(
      'preferred_target',
      'EXECUTION_TARGET_REMOTE_SERVICE',
    );
  });

  it('normalizes all five worker response arms and both successful and failed handoffs', () => {
    const opened = readExample<WireBrowserWorkerResponseEnvelope>(
      'browser-worker-response.opened.json',
    );
    const success = readExample<WireBrowserWorkerResponseEnvelope>(
      'browser-worker-response.supported.json',
    );
    const disposed = readExample<WireBrowserWorkerResponseEnvelope>(
      'browser-worker-response.disposed.json',
    );
    const error = readExample<WireBrowserWorkerResponseEnvelope>(
      'browser-worker-response.hard-fail.json',
    );
    const inspection: WireBrowserWorkerResponseEnvelope = {
      parquet_inspection: {
        request_id: 'req-inspect-001',
        summary: representativeParquetInspection(),
      },
    };
    const responses = [
      { expectedCase: 'opened', expectedJsonName: 'opened', legacy: opened },
      { expectedCase: 'success', expectedJsonName: 'success', legacy: success },
      {
        expectedCase: 'parquetInspection',
        expectedJsonName: 'parquet_inspection',
        legacy: inspection,
      },
      { expectedCase: 'disposed', expectedJsonName: 'disposed', legacy: disposed },
      { expectedCase: 'error', expectedJsonName: 'error', legacy: error },
    ];

    for (const { expectedCase, expectedJsonName, legacy } of responses) {
      const decoded = fromJson(
        BrowserWorkerResponseEnvelopeSchema,
        normalizeWorkerResponse(legacy),
      );
      const protobufJson = toJson(BrowserWorkerResponseEnvelopeSchema, decoded);

      expect(decoded.response.case).toBe(expectedCase);
      expect(protobufJson).toHaveProperty(expectedJsonName);
    }

    const successJson = toJson(
      BrowserWorkerResponseEnvelopeSchema,
      fromJson(BrowserWorkerResponseEnvelopeSchema, normalizeWorkerResponse(success)),
    );
    expect(successJson).toMatchObject({
      success: {
        request_id: 'req-supported-001',
        response: {
          executed_on: 'EXECUTION_TARGET_BROWSER_WASM',
          metrics: {
            bytes_fetched: '2048',
            files_skipped: '1',
          },
        },
        result: {
          format: 'ARROW_IPC_FORMAT_STREAM',
          delivery: 'ARROW_IPC_DELIVERY_SINGLE_BUFFER',
          bytes: 'AQIDBA==',
          byte_length: '4',
          chunk_count: '1',
        },
      },
    });
  });

  it('normalizes structured fallback reasons and preserves explicit zero metrics', () => {
    const legacy = readExample<WireBrowserWorkerResponseEnvelope>(
      'browser-worker-response.native-fallback.json',
    );
    const decoded = fromJson(
      BrowserWorkerResponseEnvelopeSchema,
      normalizeWorkerResponse(legacy),
    );
    const protobufJson = toJson(BrowserWorkerResponseEnvelopeSchema, decoded);

    expect(protobufJson).toMatchObject({
      success: {
        response: {
          executed_on: 'EXECUTION_TARGET_NATIVE',
          fallback_reason: {
            capability_gate: {
              capability: 'CAPABILITY_KEY_MULTI_PARTITION_EXECUTION',
              required_state: 'CAPABILITY_STATE_NATIVE_ONLY',
            },
          },
          metrics: {
            row_groups_touched: '0',
            row_groups_skipped: '0',
            rows_emitted: '0',
          },
        },
        result: {
          format: 'ARROW_IPC_FORMAT_FILE',
          bytes: 'CQgHBg==',
          byte_length: '4',
          chunk_count: '1',
        },
      },
    });
  });

  it('keeps Arrow IPC opaque, base64-encoded, and explicit about delivery metadata', () => {
    const singleBuffer: WireArrowIpcResult = {
      format: 'stream',
      content_type: 'application/vnd.apache.arrow.stream',
      bytes: new Uint8Array([1, 2, 3, 4]),
    };
    const singleBufferJson = toJson(
      ArrowIpcResultSchema,
      fromJson(ArrowIpcResultSchema, normalizeArrowIpcResult(singleBuffer)),
    );

    expect(singleBufferJson).toEqual({
      format: 'ARROW_IPC_FORMAT_STREAM',
      content_type: 'application/vnd.apache.arrow.stream',
      delivery: 'ARROW_IPC_DELIVERY_SINGLE_BUFFER',
      bytes: 'AQIDBA==',
      byte_length: '4',
      chunk_count: '1',
    });

    const chunked: WireArrowIpcResult = {
      format: 'stream',
      content_type: 'application/vnd.apache.arrow.stream',
      delivery: 'chunked_buffers',
      byte_length: 0,
      chunk_count: 0,
    };
    const chunkedJson = toJson(
      ArrowIpcResultSchema,
      fromJson(ArrowIpcResultSchema, normalizeArrowIpcResult(chunked)),
    );

    expect(chunkedJson).toEqual({
      format: 'ARROW_IPC_FORMAT_STREAM',
      content_type: 'application/vnd.apache.arrow.stream',
      delivery: 'ARROW_IPC_DELIVERY_CHUNKED_BUFFERS',
      byte_length: '0',
      chunk_count: '0',
    });
    expect(chunkedJson).not.toHaveProperty('rows');
  });

  it('normalizes every preview cell variant and preserves preview zero counts', () => {
    const preview: BrowserWorkerResultPreview = {
      columns: ['text', 'number', 'flag', 'missing'],
      rows: [['alpha', 3.5, false, null]],
      row_count: 0,
      preview_row_limit: 0,
      truncated: false,
    };
    const decoded = fromJson(ResultPreviewSchema, normalizeResultPreview(preview));
    const protobufJson = toJson(ResultPreviewSchema, decoded);

    expect(protobufJson).toEqual({
      columns: ['text', 'number', 'flag', 'missing'],
      rows: [
        {
          cells: [
            { string_value: 'alpha' },
            { number_value: 3.5 },
            { bool_value: false },
            { null_value: null },
          ],
        },
      ],
      row_count: '0',
      preview_row_limit: '0',
      truncated: false,
    });
    expect(
      decoded.rows[0]?.cells.map(
        (cell: { value: { case: string | undefined } }) => cell.value.case,
      ),
    ).toEqual([
      'stringValue',
      'numberValue',
      'boolValue',
      'nullValue',
    ]);

    const legacySuccess = readExample<WireBrowserWorkerResponseEnvelope>(
      'browser-worker-response.supported.json',
    );
    if (!('success' in legacySuccess)) {
      throw new Error('supported handoff fixture is not a success response');
    }
    legacySuccess.success.preview = preview;
    const successJson = toJson(
      BrowserWorkerResponseEnvelopeSchema,
      fromJson(BrowserWorkerResponseEnvelopeSchema, normalizeWorkerResponse(legacySuccess)),
    );

    expect(successJson).toHaveProperty('success.preview', protobufJson);
  });

  it('preserves nullable Parquet zero counts while accepting implicit false booleans', () => {
    const response: WireBrowserWorkerResponseEnvelope = {
      parquet_inspection: {
        request_id: 'req-inspect-zero-counts',
        summary: representativeParquetInspection(),
      },
    };
    const decoded = fromJson(
      BrowserWorkerResponseEnvelopeSchema,
      normalizeWorkerResponse(response),
    );
    const protobufJson = toJson(BrowserWorkerResponseEnvelopeSchema, decoded);

    expect(protobufJson).toMatchObject({
      parquet_inspection: {
        request_id: 'req-inspect-zero-counts',
        summary: {
          object_size_bytes: '128',
          metadata_memory_size_bytes: '512',
          row_count: '3',
          columns: [{ null_count: '0' }],
          row_groups: [{ columns: [{ null_count: '0' }] }],
        },
      },
    });
    expect(protobufJson).not.toHaveProperty(
      'parquet_inspection.summary.columns.0.has_bloom_filter',
    );
  });

  it('normalizes all eight worker event arms and every query metric field', () => {
    const context: BrowserWorkerEventContext = {
      phase: 'query',
      request_id: 'req-query-001',
      query_id: 'req-query-001',
      table_name: 'supported_table',
    };
    const queryError: QueryError = {
      code: 'fallback_required',
      message: 'browser query exceeded runtime budget',
      target: 'browser_wasm',
      fallback_reason: 'browser_runtime_constraint',
    };
    const events: Array<{
      expectedCase: string;
      expectedJsonName: string;
      legacy: BrowserWorkerEventEnvelope;
    }> = [
      {
        expectedCase: 'progress',
        expectedJsonName: 'progress',
        legacy: { progress: { context, stage: 'planning' } },
      },
      {
        expectedCase: 'log',
        expectedJsonName: 'log',
        legacy: { log: { context, level: 'warn', message: 'query budget nearly exhausted' } },
      },
      {
        expectedCase: 'rangeReadMetrics',
        expectedJsonName: 'range_read_metrics',
        legacy: {
          range_read_metrics: {
            context,
            ...representativeRangeReadMetrics(),
          },
        },
      },
      {
        expectedCase: 'cacheMetrics',
        expectedJsonName: 'cache_metrics',
        legacy: {
          cache_metrics: {
            context,
            session_cached_bytes: 0,
            session_table_count: 0,
            max_session_cached_bytes: 67_108_864,
            transport: {
              bytes_reused: 0,
              validation_misses: 1,
              persistent_cache_errors: 0,
            },
          },
        },
      },
      {
        expectedCase: 'fallback',
        expectedJsonName: 'fallback',
        legacy: { fallback: { context, reason: 'browser_runtime_constraint' } },
      },
      {
        expectedCase: 'cancellation',
        expectedJsonName: 'cancellation',
        legacy: { cancellation: { context, error: queryError } },
      },
      {
        expectedCase: 'terminalError',
        expectedJsonName: 'terminal_error',
        legacy: { terminal_error: { context, error: queryError } },
      },
      {
        expectedCase: 'arrowIpcChunk',
        expectedJsonName: 'arrow_ipc_chunk',
        legacy: {
          arrow_ipc_chunk: {
            context,
            request_id: 'req-query-001',
            sequence: 0,
            byte_offset: 0,
            byte_length: 2,
            bytes: new Uint8Array([9, 8]),
          },
        },
      },
    ];

    for (const { expectedCase, expectedJsonName, legacy } of events) {
      const decoded = fromJson(BrowserWorkerEventEnvelopeSchema, normalizeWorkerEvent(legacy));
      const protobufJson = toJson(BrowserWorkerEventEnvelopeSchema, decoded);

      expect(decoded.event.case).toBe(expectedCase);
      expect(protobufJson).toHaveProperty(expectedJsonName);
    }

    const rangeEvent = events[2];
    if (!rangeEvent) {
      throw new Error('range metrics fixture is missing');
    }
    const rangeJson = toJson(
      BrowserWorkerEventEnvelopeSchema,
      fromJson(BrowserWorkerEventEnvelopeSchema, normalizeWorkerEvent(rangeEvent.legacy)),
    );
    const legacyRange = representativeRangeReadMetrics() as unknown as Record<string, unknown>;
    for (const key of QUERY_METRIC_UINT64_FIELDS.filter((field) => field !== 'duration_ms')) {
      const value = legacyRange[key];
      if (value !== undefined) {
        expect(rangeJson).toHaveProperty(`range_read_metrics.${key}`, String(value));
      }
    }
    expect(rangeJson).toHaveProperty(
      'range_read_metrics.access_mode',
      'BROWSER_ACCESS_MODE_BROWSER_SAFE_HTTP',
    );

    const cacheJson = toJson(
      BrowserWorkerEventEnvelopeSchema,
      fromJson(BrowserWorkerEventEnvelopeSchema, normalizeWorkerEvent(events[3]!.legacy)),
    );
    expect(cacheJson).toMatchObject({
      cache_metrics: {
        session_cached_bytes: '0',
        session_table_count: '0',
        max_session_cached_bytes: '67108864',
        transport: {
          bytes_reused: '0',
          validation_misses: '1',
          persistent_cache_errors: '0',
        },
      },
    });

    const chunkJson = toJson(
      BrowserWorkerEventEnvelopeSchema,
      fromJson(BrowserWorkerEventEnvelopeSchema, normalizeWorkerEvent(events[7]!.legacy)),
    );
    expect(chunkJson).toMatchObject({
      arrow_ipc_chunk: {
        request_id: 'req-query-001',
        sequence: '0',
        byte_offset: '0',
        byte_length: '2',
        bytes: 'CQg=',
      },
    });
  });

  it('retains legacy snake_case JSON names for reusable descriptors', () => {
    const descriptorJsonNames = (fields: readonly DescField[]) =>
      Object.fromEntries(fields.map((field) => [field.name, field.jsonName]));

    expect(descriptorJsonNames(BrowserHttpFileDescriptorSchema.fields)).toMatchObject({
      size_bytes: 'size_bytes',
      partition_values: 'partition_values',
      object_etag: 'object_etag',
    });
    expect(descriptorJsonNames(BrowserHttpSnapshotDescriptorSchema.fields)).toMatchObject({
      table_uri: 'table_uri',
      snapshot_version: 'snapshot_version',
      partition_column_types: 'partition_column_types',
      browser_compatibility: 'browser_compatibility',
      required_capabilities: 'required_capabilities',
      active_files: 'active_files',
    });
    expect(
      descriptorJsonNames(BrowserHttpParquetDatasetDescriptorSchema.fields),
    ).toMatchObject({
      table_uri: 'table_uri',
      partition_column_types: 'partition_column_types',
      browser_compatibility: 'browser_compatibility',
      required_capabilities: 'required_capabilities',
    });

    const openTable = readExample<{ open_table: BrowserWorkerOpenTableCommand }>(
      'browser-worker-command.open-table.json',
    );
    const openParquet = readExample<{
      open_parquet_dataset: BrowserWorkerOpenParquetDatasetCommand;
    }>('browser-worker-command.open-parquet-dataset.json');
    const tableJson = toJson(
      BrowserWorkerCommandSchema,
      fromJson(BrowserWorkerCommandSchema, normalizeWorkerCommand(openTable)),
    );
    const parquetJson = toJson(
      BrowserWorkerCommandSchema,
      fromJson(BrowserWorkerCommandSchema, normalizeWorkerCommand(openParquet)),
    );
    const versionZeroOpenTable: { open_table: BrowserWorkerOpenTableCommand } = {
      open_table: {
        ...openTable.open_table,
        snapshot: {
          ...openTable.open_table.snapshot,
          snapshot_version: 0,
        },
      },
    };
    const versionZeroJson = toJson(
      BrowserWorkerCommandSchema,
      fromJson(BrowserWorkerCommandSchema, normalizeWorkerCommand(versionZeroOpenTable)),
    );

    expect(tableJson).toMatchObject({
      open_table: {
        request_id: 'req-open-001',
        snapshot: {
          table_uri: 'gs://axon-fixtures/partitioned-table',
          snapshot_version: '7',
          active_files: [
            {
              size_bytes: '128',
            },
          ],
        },
      },
    });
    expect(tableJson).not.toHaveProperty('open_table.requestId');
    expect(tableJson).not.toHaveProperty('open_table.snapshot.tableUri');
    expect(tableJson).not.toHaveProperty(
      'open_table.snapshot.active_files.0.partition_values',
    );
    expect(versionZeroJson).toHaveProperty('open_table.snapshot.snapshot_version', '0');
    expect(parquetJson).toMatchObject({
      open_parquet_dataset: {
        request_id: 'req-open-parquet-001',
        dataset: {
          table_uri: 'https://storage.example.invalid/datasets/plain-parquet-events',
          required_capabilities: {
            capabilities: {
              range_reads: 'CAPABILITY_STATE_SUPPORTED',
            },
          },
          files: [{ size_bytes: '1048576' }],
        },
      },
    });
  });

  it('keeps row-like protobuf messages confined to result previews', () => {
    const messageNames = file_axon_exec_v1_exec.messages.map(
      (message: DescMessage) => message.name,
    );
    const forbiddenRowMessages = messageNames.filter(
      (name: string) =>
        name !== 'ResultPreviewRow' && /(Arrow|Query|Result).*(Row|Record|Batch)/.test(name),
    );

    expect(forbiddenRowMessages).toEqual([]);
    expect(ArrowIpcResultSchema.fields.map((field: DescField) => field.name)).toContain('bytes');
    expect(ArrowIpcResultSchema.fields.map((field: DescField) => field.name)).not.toContain('rows');
    expect(PreviewCellSchema.oneofs).toHaveLength(1);
    expect(PreviewCellSchema.oneofs[0]?.fields.map((field: DescField) => field.name)).toEqual([
      'string_value',
      'number_value',
      'bool_value',
      'null_value',
    ]);
  });

  it('restricts execution and preview requests to directly openable descriptors', () => {
    for (const requestSchema of [ExecuteRequestSchema, PreviewRequestSchema]) {
      expect(messageFieldTypeName(requestSchema.fields, 'descriptor')).toBe(
        'axon.exec.v1.OpenableDescriptor',
      );
      expect(requestSchema.fields.map((field: DescField) => field.name)).not.toContain(
        'table_read_resolution',
      );
    }

    expect(
      OpenableDescriptorSchema.oneofs[0]?.fields.map((field: DescField) => [
        field.name,
        messageFieldTypeName(OpenableDescriptorSchema.fields, field.name),
      ]),
    ).toEqual([
      ['snapshot', 'axon.dataaccess.v1.BrowserHttpSnapshotDescriptor'],
      ['parquet_dataset', 'axon.dataaccess.v1.BrowserHttpParquetDatasetDescriptor'],
    ]);
    expect(ExecuteResponseSchema.oneofs[0]?.fields.map((field: DescField) => field.name)).toEqual([
      'event',
      'response',
    ]);
    expect(PreviewResponseSchema.oneofs[0]?.fields.map((field: DescField) => field.name)).toEqual([
      'success',
      'error',
    ]);

    const previewRequest = toJson(
      PreviewRequestSchema,
      fromJson(PreviewRequestSchema, {
        request_id: 'preview-control-001',
        preferred_target: 'EXECUTION_TARGET_BROWSER_WASM',
        limit: '0',
      }),
    );
    const cancelResponse = toJson(
      CancelResponseSchema,
      fromJson(CancelResponseSchema, {
        request_id: 'cancel-control-001',
        query_id: 'query-001',
        accepted: false,
      }),
    );

    expect(previewRequest).toHaveProperty('limit', '0');
    expect(cancelResponse).toEqual({
      request_id: 'cancel-control-001',
      query_id: 'query-001',
      accepted: false,
    });
  });

  it('describes Execute as server-streaming and Preview and Cancel as unary', () => {
    expect(QueryEngine.method.execute.methodKind).toBe('server_streaming');
    expect(QueryEngine.method.preview.methodKind).toBe('unary');
    expect(QueryEngine.method.cancel.methodKind).toBe('unary');
    expect({
      execute: [QueryEngine.method.execute.input.name, QueryEngine.method.execute.output.name],
      preview: [QueryEngine.method.preview.input.name, QueryEngine.method.preview.output.name],
      cancel: [QueryEngine.method.cancel.input.name, QueryEngine.method.cancel.output.name],
    }).toEqual({
      execute: ['ExecuteRequest', 'ExecuteResponse'],
      preview: ['PreviewRequest', 'PreviewResponse'],
      cancel: ['CancelRequest', 'CancelResponse'],
    });
  });
});

function readExample<T>(fileName: string): T {
  return JSON.parse(readFileSync(resolve(HANDOFF_EXAMPLE_DIR, fileName), 'utf8')) as T;
}

function compact(entries: Record<string, JsonValue | undefined>): JsonObject {
  return Object.fromEntries(
    Object.entries(entries).filter((entry): entry is [string, JsonValue] => entry[1] !== undefined),
  );
}

function requiredJsonObject(value: JsonValue): JsonObject {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new TypeError('expected protobuf JSON object');
  }
  return value;
}

function messageFieldTypeName(fields: readonly DescField[], fieldName: string): string {
  const field = fields.find((candidate) => candidate.name === fieldName);
  if (!field || field.fieldKind !== 'message') {
    throw new TypeError(`expected ${fieldName} to be a message field`);
  }
  return field.message.typeName;
}

function enumSymbol(prefix: string, value: string): string {
  return `${prefix}_${value.toUpperCase()}`;
}

function decimal(value: number | undefined): string | undefined {
  return value === undefined ? undefined : String(value);
}

function implicitBoolean(value: boolean): true | undefined {
  return value ? true : undefined;
}

function normalizeCapabilityReport(report: CapabilityReport | undefined): JsonObject | undefined {
  if (!report) {
    return undefined;
  }
  const capabilities: JsonObject = {};
  for (const [key, value] of Object.entries(report.capabilities)) {
    if (value !== undefined) {
      capabilities[key] = enumSymbol('CAPABILITY_STATE', value);
    }
  }
  return {
    capabilities,
  };
}

function normalizePartitionColumnTypes(
  values: BrowserHttpSnapshotDescriptor['partition_column_types'],
): JsonObject | undefined {
  if (!values) {
    return undefined;
  }
  const normalized: JsonObject = {};
  for (const [key, value] of Object.entries(values)) {
    if (value !== undefined) {
      normalized[key] = enumSymbol('PARTITION_COLUMN_TYPE', value);
    }
  }
  return normalized;
}

function normalizeFileDescriptor(file: BrowserHttpFileDescriptor): JsonObject {
  const partitionValues: JsonObject = {};
  for (const [key, value] of Object.entries(file.partition_values)) {
    partitionValues[key] = value === null ? { nullValue: null } : { stringValue: value };
  }
  return compact({
    path: file.path,
    url: file.url,
    size_bytes: decimal(file.size_bytes),
    partition_values: partitionValues,
    stats: file.stats,
    object_etag: file.object_etag,
  });
}

function normalizeSnapshotDescriptor(snapshot: BrowserHttpSnapshotDescriptor): JsonObject {
  return compact({
    table_uri: snapshot.table_uri,
    snapshot_version: decimal(snapshot.snapshot_version),
    partition_column_types: normalizePartitionColumnTypes(snapshot.partition_column_types),
    browser_compatibility: normalizeCapabilityReport(snapshot.browser_compatibility),
    required_capabilities: normalizeCapabilityReport(snapshot.required_capabilities),
    active_files: snapshot.active_files.map(normalizeFileDescriptor),
  });
}

function normalizeParquetDatasetDescriptor(
  dataset: BrowserHttpParquetDatasetDescriptor,
): JsonObject {
  return compact({
    table_uri: dataset.table_uri,
    partition_column_types: normalizePartitionColumnTypes(dataset.partition_column_types),
    browser_compatibility: normalizeCapabilityReport(dataset.browser_compatibility),
    required_capabilities: normalizeCapabilityReport(dataset.required_capabilities),
    files: dataset.files.map(normalizeFileDescriptor),
  });
}

function normalizeQueryRequest(request: ContractQueryRequest): JsonObject {
  const options = request.options ?? {};
  return compact({
    table_uri: request.table_uri,
    snapshot_version: decimal(request.snapshot_version),
    sql: request.sql,
    preferred_target: enumSymbol('EXECUTION_TARGET', request.preferred_target),
    options: compact({
      include_explain: options.include_explain ?? false,
      collect_metrics: options.collect_metrics ?? true,
      result_page: options.result_page
        ? compact({
            limit: decimal(options.result_page.limit),
            offset: decimal(options.result_page.offset),
          })
        : undefined,
      runtime_limits: options.runtime_limits
        ? compact({
            max_result_rows: decimal(options.runtime_limits.max_result_rows),
            max_arrow_ipc_bytes: decimal(options.runtime_limits.max_arrow_ipc_bytes),
            max_preview_string_bytes: decimal(options.runtime_limits.max_preview_string_bytes),
            max_scan_bytes: decimal(options.runtime_limits.max_scan_bytes),
            max_scan_overfetch_bytes: decimal(
              options.runtime_limits.max_scan_overfetch_bytes,
            ),
          })
        : undefined,
    }),
  });
}

function normalizeFallbackReason(reason: FallbackReason): JsonObject {
  if (typeof reason === 'string') {
    return { [reason]: {} };
  }
  return {
    capability_gate: {
      capability: enumSymbol('CAPABILITY_KEY', reason.capability_gate.capability),
      required_state: enumSymbol(
        'CAPABILITY_STATE',
        reason.capability_gate.required_state,
      ),
    },
  };
}

function normalizeQueryError(error: QueryError): JsonObject {
  return compact({
    code: enumSymbol('QUERY_ERROR_CODE', error.code),
    message: error.message,
    target: enumSymbol('EXECUTION_TARGET', error.target),
    fallback_reason: error.fallback_reason
      ? normalizeFallbackReason(error.fallback_reason)
      : undefined,
  });
}

function normalizeMetricFields(metrics: object): JsonObject {
  const source = metrics as unknown as Record<string, unknown>;
  const normalized: JsonObject = {};
  for (const key of QUERY_METRIC_UINT64_FIELDS) {
    const value = source[key];
    if (typeof value === 'number') {
      normalized[key] = String(value);
    }
  }
  if (typeof source.access_mode === 'string') {
    normalized.access_mode = enumSymbol('BROWSER_ACCESS_MODE', source.access_mode);
  }
  return normalized;
}

function normalizeQueryResponse(response: QueryResponse): JsonObject {
  return compact({
    executed_on: enumSymbol('EXECUTION_TARGET', response.executed_on),
    capabilities: normalizeCapabilityReport(response.capabilities),
    fallback_reason: response.fallback_reason
      ? normalizeFallbackReason(response.fallback_reason)
      : undefined,
    metrics: normalizeMetricFields(response.metrics),
    explain: response.explain,
  });
}

function normalizeBytes(
  value: number[] | ArrayBuffer | Uint8Array | undefined,
): Uint8Array | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (value instanceof Uint8Array) {
    return value;
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  return Uint8Array.from(value);
}

function normalizeArrowIpcResult(result: WireArrowIpcResult): JsonObject {
  const bytes = normalizeBytes(result.bytes);
  const delivery = result.delivery ?? 'single_buffer';
  const byteLength = result.byte_length ?? bytes?.byteLength;
  const chunkCount =
    result.chunk_count ?? (delivery === 'single_buffer' ? (byteLength === 0 ? 0 : 1) : undefined);
  return compact({
    format: enumSymbol('ARROW_IPC_FORMAT', result.format),
    content_type: result.content_type,
    delivery: enumSymbol('ARROW_IPC_DELIVERY', delivery),
    bytes: bytes ? Buffer.from(bytes).toString('base64') : undefined,
    byte_length: decimal(byteLength),
    chunk_count: decimal(chunkCount),
  });
}

function normalizePreviewCell(
  cell: BrowserWorkerResultPreview['rows'][number][number],
): JsonObject {
  if (cell === null) {
    return { null_value: null };
  }
  switch (typeof cell) {
    case 'string':
      return { string_value: cell };
    case 'number':
      return { number_value: cell };
    case 'boolean':
      return { bool_value: cell };
  }
  throw new TypeError('unsupported result preview cell');
}

function normalizeResultPreview(preview: BrowserWorkerResultPreview): JsonObject {
  return {
    columns: preview.columns,
    rows: preview.rows.map((row) => ({ cells: row.map(normalizePreviewCell) })),
    row_count: String(preview.row_count),
    preview_row_limit: String(preview.preview_row_limit),
    truncated: preview.truncated,
  };
}

function normalizeParquetColumnChunk(chunk: ParquetInspectionColumnChunk): JsonObject {
  return compact({
    column_name: chunk.column_name,
    compression: chunk.compression,
    encodings: chunk.encodings,
    compressed_size_bytes: decimal(chunk.compressed_size_bytes),
    uncompressed_size_bytes: decimal(chunk.uncompressed_size_bytes),
    null_count: decimal(chunk.null_count),
    has_statistics: implicitBoolean(chunk.has_statistics),
    has_column_index: implicitBoolean(chunk.has_column_index),
    has_offset_index: implicitBoolean(chunk.has_offset_index),
    has_bloom_filter: implicitBoolean(chunk.has_bloom_filter),
  });
}

function normalizeParquetRowGroup(rowGroup: ParquetInspectionRowGroup): JsonObject {
  return {
    index: String(rowGroup.index),
    row_count: String(rowGroup.row_count),
    compressed_size_bytes: String(rowGroup.compressed_size_bytes),
    uncompressed_size_bytes: String(rowGroup.uncompressed_size_bytes),
    columns: rowGroup.columns.map(normalizeParquetColumnChunk),
  };
}

function normalizeParquetColumn(column: ParquetInspectionColumn): JsonObject {
  return compact({
    name: column.name,
    physical_type: column.physical_type,
    logical_type: column.logical_type,
    converted_type: column.converted_type,
    repetition: column.repetition,
    nullable: implicitBoolean(column.nullable),
    compressed_size_bytes: decimal(column.compressed_size_bytes),
    uncompressed_size_bytes: decimal(column.uncompressed_size_bytes),
    null_count: decimal(column.null_count),
    encodings: column.encodings,
    compressions: column.compressions,
    has_statistics: implicitBoolean(column.has_statistics),
    has_column_index: implicitBoolean(column.has_column_index),
    has_offset_index: implicitBoolean(column.has_offset_index),
    has_bloom_filter: implicitBoolean(column.has_bloom_filter),
  });
}

function normalizeParquetInspection(summary: ParquetInspectionSummary): JsonObject {
  return compact({
    path: summary.path,
    object_size_bytes: decimal(summary.object_size_bytes),
    footer_length_bytes: summary.footer_length_bytes,
    metadata_memory_size_bytes: decimal(summary.metadata_memory_size_bytes),
    created_by: summary.created_by,
    file_version: summary.file_version,
    row_group_count: decimal(summary.row_group_count),
    row_count: decimal(summary.row_count),
    column_count: decimal(summary.column_count),
    compression: {
      compressed_size_bytes: String(summary.compression.compressed_size_bytes),
      uncompressed_size_bytes: String(summary.compression.uncompressed_size_bytes),
      ratio_basis_points: String(summary.compression.ratio_basis_points),
    },
    columns: summary.columns.map(normalizeParquetColumn),
    row_groups: summary.row_groups.map(normalizeParquetRowGroup),
  });
}

function normalizeWorkerCommand(command: LegacyWorkerCommand): JsonObject {
  if ('open_table' in command) {
    return {
      open_table: compact({
        request_id: command.open_table.request_id,
        name: command.open_table.name,
        snapshot: normalizeSnapshotDescriptor(command.open_table.snapshot),
      }),
    };
  }
  if ('open_delta_table' in command) {
    return {
      open_delta_table: compact({
        request_id: command.open_delta_table.request_id,
        name: command.open_delta_table.name,
        snapshot: normalizeSnapshotDescriptor(command.open_delta_table.snapshot),
      }),
    };
  }
  if ('open_parquet_dataset' in command) {
    return {
      open_parquet_dataset: compact({
        request_id: command.open_parquet_dataset.request_id,
        name: command.open_parquet_dataset.name,
        dataset: normalizeParquetDatasetDescriptor(command.open_parquet_dataset.dataset),
      }),
    };
  }
  if ('inspect_parquet' in command) {
    return {
      inspect_parquet: {
        request_id: command.inspect_parquet.request_id,
        name: command.inspect_parquet.name,
        path: command.inspect_parquet.path,
      },
    };
  }
  if ('sql' in command) {
    const sql = command.sql as LegacySqlCommand;
    const query = sql.query ?? sql.request;
    if (!query) {
      throw new TypeError('legacy SQL worker command is missing query/request');
    }
    return {
      sql: {
        request_id: sql.request_id,
        name: sql.name,
        query: normalizeQueryRequest(query),
        output: enumSymbol('BROWSER_WORKER_SQL_OUTPUT', sql.output ?? 'arrow_ipc_stream'),
        delivery: enumSymbol(
          'BROWSER_WORKER_SQL_DELIVERY',
          sql.delivery ?? 'single_buffer',
        ),
        browser_safe_defaults: sql.browser_safe_defaults ?? false,
      },
    };
  }
  if ('cancel' in command) {
    return {
      cancel: compact({
        request_id: command.cancel.request_id,
        query_id: command.cancel.query_id,
      }),
    };
  }
  return {
    dispose: {
      request_id: command.dispose.request_id,
      name: command.dispose.name,
    },
  };
}

function normalizeWorkerResponse(response: WireBrowserWorkerResponseEnvelope): JsonObject {
  if ('opened' in response) {
    return { opened: response.opened };
  }
  if ('success' in response) {
    return {
      success: compact({
        request_id: response.success.request_id,
        response: normalizeQueryResponse(response.success.response),
        result: normalizeArrowIpcResult(response.success.result),
        preview: response.success.preview
          ? normalizeResultPreview(response.success.preview)
          : undefined,
      }),
    };
  }
  if ('parquet_inspection' in response) {
    return {
      parquet_inspection: {
        request_id: response.parquet_inspection.request_id,
        summary: normalizeParquetInspection(response.parquet_inspection.summary),
      },
    };
  }
  if ('disposed' in response) {
    return { disposed: response.disposed };
  }
  return {
    error: {
      request_id: response.error.request_id,
      error: normalizeQueryError(response.error.error),
    },
  };
}

function normalizeEventContext(context: BrowserWorkerEventContext): JsonObject {
  return compact({
    phase: enumSymbol('BROWSER_WORKER_EVENT_PHASE', context.phase),
    request_id: context.request_id,
    query_id: context.query_id,
    table_name: context.table_name,
  });
}

function normalizeWorkerEvent(event: BrowserWorkerEventEnvelope): JsonObject {
  if ('progress' in event) {
    return {
      progress: {
        context: normalizeEventContext(event.progress.context),
        stage: enumSymbol('BROWSER_WORKER_PROGRESS_STAGE', event.progress.stage),
      },
    };
  }
  if ('log' in event) {
    return {
      log: {
        context: normalizeEventContext(event.log.context),
        level: enumSymbol('BROWSER_WORKER_LOG_LEVEL', event.log.level),
        message: event.log.message,
      },
    };
  }
  if ('range_read_metrics' in event) {
    return {
      range_read_metrics: {
        context: normalizeEventContext(event.range_read_metrics.context),
        ...normalizeMetricFields(event.range_read_metrics),
      },
    };
  }
  if ('cache_metrics' in event) {
    const transport = event.cache_metrics.transport;
    return {
      cache_metrics: compact({
        context: normalizeEventContext(event.cache_metrics.context),
        session_cached_bytes: decimal(event.cache_metrics.session_cached_bytes),
        session_table_count: decimal(event.cache_metrics.session_table_count),
        max_session_cached_bytes: decimal(event.cache_metrics.max_session_cached_bytes),
        transport: transport
          ? compact({
              bytes_reused: decimal(transport.bytes_reused),
              validation_misses: decimal(transport.validation_misses),
              persistent_cache_errors: decimal(transport.persistent_cache_errors),
            })
          : undefined,
      }),
    };
  }
  if ('fallback' in event) {
    return {
      fallback: {
        context: normalizeEventContext(event.fallback.context),
        reason: normalizeFallbackReason(event.fallback.reason),
      },
    };
  }
  if ('cancellation' in event) {
    return {
      cancellation: {
        context: normalizeEventContext(event.cancellation.context),
        error: normalizeQueryError(event.cancellation.error),
      },
    };
  }
  if ('terminal_error' in event) {
    return {
      terminal_error: {
        context: normalizeEventContext(event.terminal_error.context),
        error: normalizeQueryError(event.terminal_error.error),
      },
    };
  }
  return {
    arrow_ipc_chunk: compact({
      context: normalizeEventContext(event.arrow_ipc_chunk.context),
      request_id: event.arrow_ipc_chunk.request_id,
      sequence: decimal(event.arrow_ipc_chunk.sequence),
      byte_offset: decimal(event.arrow_ipc_chunk.byte_offset),
      byte_length: decimal(event.arrow_ipc_chunk.byte_length),
      bytes: Buffer.from(event.arrow_ipc_chunk.bytes).toString('base64'),
    }),
  };
}

function representativeMetrics(): QueryMetricsSummary {
  return {
    bytes_fetched: 4_096,
    duration_ms: 11,
    files_touched: 2,
    files_skipped: 1,
    prebootstrap_fail_open_count: 1,
    prebootstrap_files_pruned: 0,
    footer_reads_avoided: 0,
    prebootstrap_candidate_files: 2,
    row_groups_touched: 4,
    row_groups_skipped: 3,
    footer_reads: 2,
    bootstrap_footer_range_reads: 4,
    scan_footer_range_reads: 2,
    scan_data_range_reads: 6,
    duplicate_range_reads: 2,
    coalesced_range_reads: 1,
    coalesced_gap_bytes_fetched: 12_288,
    scan_overfetch_bytes: 13_312,
    coordinator_peak_staged_bytes: 4_096,
    coordinator_staging_limit_bytes: 8_388_608,
    cursor_peak_pending_encoded_bytes: 2_048,
    cursor_peak_transport_chunk_bytes: 1_048_576,
    footer_cache_hits: 1,
    footer_cache_misses: 3,
    footer_range_reads_avoided: 2,
    footer_cache_degraded_identity_reads: 1,
    identity_present_range_reads: 5,
    identity_missing_range_reads: 7,
    range_cache_hits: 1,
    range_cache_misses: 2,
    range_cache_bytes_reused: 800,
    range_cache_bytes_stored: 2_560,
    range_cache_validation_misses: 3,
    range_cache_degraded_identity_reads: 4,
    range_readahead_requests: 5,
    range_readahead_bytes_fetched: 4_096,
    range_readahead_bytes_used: 3_072,
    range_readahead_wasted_bytes: 1_024,
    descriptor_resolution_count: 1,
    delta_log_manifest_list_count: 1,
    delta_log_manifest_list_duration_ms: 3,
    snapshot_resolve_count: 1,
    snapshot_resolve_duration_ms: 6,
    descriptor_cache_hit: 1,
    session_reuse_count: 2,
    opened_table_reuse_count: 3,
    identity_refresh_count: 4,
    access_envelope_refresh_count: 5,
    rows_emitted: 25,
    snapshot_bootstrap_duration_ms: 7,
    access_mode: 'browser_safe_http',
    arrow_ipc_bytes: 1_024,
    arrow_ipc_chunk_count: 4,
    preview_rows: 25,
    preview_string_bytes: 128,
    planning_duration_ms: 2,
    arrow_ipc_encode_duration_ms: 3,
    preview_duration_ms: 1,
  };
}

function representativeRangeReadMetrics(): Omit<BrowserWorkerRangeReadMetricsEvent, 'context'> {
  const metrics = representativeMetrics();
  return Object.fromEntries(
    Object.entries(metrics).filter(([key]) => key !== 'duration_ms'),
  ) as unknown as Omit<BrowserWorkerRangeReadMetricsEvent, 'context'>;
}

function representativeParquetInspection(): ParquetInspectionSummary {
  return {
    path: 'part-000.parquet',
    object_size_bytes: 128,
    footer_length_bytes: 32,
    metadata_memory_size_bytes: 512,
    created_by: 'axon test',
    file_version: 2,
    row_group_count: 1,
    row_count: 3,
    column_count: 1,
    compression: {
      compressed_size_bytes: 64,
      uncompressed_size_bytes: 128,
      ratio_basis_points: 5_000,
    },
    columns: [
      {
        name: 'id',
        physical_type: 'INT64',
        repetition: 'REQUIRED',
        nullable: false,
        compressed_size_bytes: 64,
        uncompressed_size_bytes: 128,
        null_count: 0,
        encodings: ['PLAIN'],
        compressions: ['UNCOMPRESSED'],
        has_statistics: true,
        has_column_index: true,
        has_offset_index: true,
        has_bloom_filter: false,
      },
    ],
    row_groups: [
      {
        index: 0,
        row_count: 3,
        compressed_size_bytes: 64,
        uncompressed_size_bytes: 128,
        columns: [
          {
            column_name: 'id',
            compression: 'UNCOMPRESSED',
            encodings: ['PLAIN'],
            compressed_size_bytes: 64,
            uncompressed_size_bytes: 128,
            null_count: 0,
            has_statistics: true,
            has_column_index: true,
            has_offset_index: true,
            has_bloom_filter: false,
          },
        ],
      },
    ],
  };
}
