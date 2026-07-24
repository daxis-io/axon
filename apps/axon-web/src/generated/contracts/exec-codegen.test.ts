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
  ExecuteResponseSchema,
  ExecutionTarget,
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
        execution_id: 'req-supported-001',
        query: {
          preferred_target: 'EXECUTION_TARGET_BROWSER_WASM',
          options: {
            include_explain: false,
            collect_metrics: true,
          },
        },
        output: 'BROWSER_WORKER_SQL_OUTPUT_ARROW_IPC_STREAM',
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

    expect(decodedWithoutRuntimeLimits.options?.includeExplain).toBe(false);
    expect(decodedWithoutRuntimeLimits.options?.collectMetrics).toBe(false);
    expect(decodedWithoutRuntimeLimits.options?.resultPage?.limit).toBe(0n);
    expect(decodedWithoutRuntimeLimits.options?.runtimeLimits).toBeUndefined();
    expect(jsonWithoutRuntimeLimits).toMatchObject({
      options: {
        include_explain: false,
        collect_metrics: false,
        result_page: { limit: '0', offset: '0' },
      },
    });
    expect(jsonWithoutRuntimeLimits).not.toHaveProperty('table_uri');
    expect(jsonWithoutRuntimeLimits).not.toHaveProperty('snapshot_version');
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
          bytes: 'AQIDBA==',
          byte_length: '4',
        },
      },
    });
  });

  it('drops legacy fallback metadata and preserves explicit zero metrics', () => {
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
        },
      },
    });
    expect(protobufJson).not.toHaveProperty('success.response.fallback_reason');
  });

  it('keeps Arrow IPC opaque, base64-encoded, and single-buffer only', () => {
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
      bytes: 'AQIDBA==',
      byte_length: '4',
    });

    const chunked: WireArrowIpcResult = {
      format: 'stream',
      content_type: 'application/vnd.apache.arrow.stream',
      delivery: 'chunked_buffers',
      byte_length: 0,
      chunk_count: 0,
    };
    expect(() => normalizeArrowIpcResult(chunked)).toThrow(
      'corrected protobuf contract requires one Arrow IPC buffer',
    );
    expect(singleBufferJson).not.toHaveProperty('rows');
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

  it('normalizes the four nonterminal worker event arms and every retained metric field', () => {
    const context: BrowserWorkerEventContext = {
      phase: 'query',
      request_id: 'req-query-001',
      query_id: 'req-query-001',
      table_name: 'supported_table',
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
            capabilities: [
              {
                key: 'CAPABILITY_KEY_RANGE_READS',
                state: 'CAPABILITY_STATE_SUPPORTED',
              },
            ],
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

  it('restricts preview to one self-identifying browser binding', () => {
    expect(messageFieldTypeName(PreviewRequestSchema.fields, 'browser_read')).toBe(
      'axon.dataaccess.v1.ResolvedBrowserRead',
    );
    expect(PreviewRequestSchema.fields.map((field: DescField) => field.name)).not.toEqual(
      expect.arrayContaining(['request_id', 'table_ref', 'descriptor']),
    );
    expect(PreviewResponseSchema.oneofs[0]?.fields.map((field: DescField) => field.name)).toEqual([
      'success',
      'error',
    ]);

    const previewRequest = toJson(
      PreviewRequestSchema,
      fromJson(PreviewRequestSchema, {
        execution_id: 'execution-preview-001',
        preferred_target: 'EXECUTION_TARGET_BROWSER_WASM',
        limit: '0',
      }),
    );
    const cancelResponse = toJson(
      CancelResponseSchema,
      fromJson(CancelResponseSchema, {
        execution_id: 'execution-001',
        state: 'EXECUTION_LIFECYCLE_STATE_CANCEL_REQUESTED',
      }),
    );

    expect(previewRequest).toHaveProperty('limit', '0');
    expect(cancelResponse).toEqual({
      execution_id: 'execution-001',
      state: 'EXECUTION_LIFECYCLE_STATE_CANCEL_REQUESTED',
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

  it('defines one caller-owned execution identity and one binding arm', () => {
    const executeRequest = contractMessage('ExecuteRequest');
    const queryRequest = contractMessage('QueryRequest');
    const runtimeLimits = contractMessage('QueryRuntimeLimits');

    expect(executeRequest.fields.map((field: DescField) => field.name)).toEqual([
      'execution_id',
      'browser_read',
      'logical_resource',
      'query',
      'deadline',
    ]);
    expect(executeRequest.oneofs).toHaveLength(1);
    expect(executeRequest.oneofs[0]?.fields.map((field: DescField) => field.name)).toEqual([
      'browser_read',
      'logical_resource',
    ]);
    expect(messageFieldTypeName(executeRequest.fields, 'browser_read')).toBe(
      'axon.dataaccess.v1.ResolvedBrowserRead',
    );
    expect(messageFieldTypeName(executeRequest.fields, 'logical_resource')).toBe(
      'axon.common.v1.CanonicalResourceRef',
    );
    expect(messageFieldTypeName(executeRequest.fields, 'deadline')).toBe(
      'google.protobuf.Timestamp',
    );
    expect(queryRequest.fields.map((field: DescField) => field.name)).toEqual([
      'sql',
      'preferred_target',
      'options',
    ]);
    expect(runtimeLimits.fields.map((field: DescField) => field.name)).toEqual([
      'max_result_rows',
      'max_arrow_ipc_bytes',
      'max_preview_string_bytes',
      'max_scan_bytes',
      'max_scan_overfetch_bytes',
    ]);
    expect(
      runtimeLimits.fields.every((field: DescField) => field.proto.proto3Optional === true),
    ).toBe(true);
  });

  it('closes admission, lifecycle, terminal delivery, cancellation, and Arrow buffering', () => {
    const lifecycleStates = contractEnumValues('ExecutionLifecycleState');
    expect(lifecycleStates).toEqual([
      'EXECUTION_LIFECYCLE_STATE_UNSPECIFIED',
      'EXECUTION_LIFECYCLE_STATE_CREATED',
      'EXECUTION_LIFECYCLE_STATE_RUNNING',
      'EXECUTION_LIFECYCLE_STATE_CANCEL_REQUESTED',
      'EXECUTION_LIFECYCLE_STATE_REJECTED',
      'EXECUTION_LIFECYCLE_STATE_COMPLETED',
      'EXECUTION_LIFECYCLE_STATE_FAILED',
      'EXECUTION_LIFECYCLE_STATE_CANCELLED',
    ]);
    expect(contractMessage('ExecutionAdmission').oneofs[0]?.fields.map((field) => field.name)).toEqual(
      ['accepted', 'rejected'],
    );
    expect(
      contractMessage('ExecutionTerminalState').oneofs[0]?.fields.map((field) => field.name),
    ).toEqual(['completed', 'failed', 'cancelled']);
    expect(ExecuteResponseSchema.oneofs[0]?.fields.map((field: DescField) => field.name)).toEqual([
      'admission',
      'event',
      'terminal',
    ]);
    expect(contractMessage('CancelRequest').fields.map((field) => field.name)).toEqual([
      'execution_id',
    ]);
    expect(contractMessage('CancelResponse').fields.map((field) => field.name)).toEqual([
      'execution_id',
      'state',
    ]);
    expect(BrowserWorkerEventEnvelopeSchema.oneofs[0]?.fields.map((field) => field.name)).toEqual([
      'progress',
      'log',
      'range_read_metrics',
      'cache_metrics',
    ]);
    expect(ArrowIpcResultSchema.fields.map((field: DescField) => field.name)).toEqual([
      'format',
      'content_type',
      'bytes',
      'byte_length',
    ]);

    const admission = contractMessage('ExecutionAdmission');
    const accepted = fromJson(admission, {
      accepted: {
        execution_id: 'execution-accepted',
        state: 'EXECUTION_LIFECYCLE_STATE_RUNNING',
        launch: true,
      },
    }) as unknown as { outcome: { case?: string } };
    const rejected = fromJson(admission, {
      rejected: {
        execution_id: 'execution-rejected',
        reason: 'EXECUTION_REJECTION_REASON_INVALID_REQUEST',
        message: 'invalid request',
      },
    }) as unknown as { outcome: { case?: string } };
    expect(accepted.outcome.case).toBe('accepted');
    expect(rejected.outcome.case).toBe('rejected');

    const arrow = fromJson(ArrowIpcResultSchema, {
      format: 'ARROW_IPC_FORMAT_STREAM',
      content_type: 'application/vnd.apache.arrow.stream',
      bytes: 'AQIDBA==',
      byte_length: '4',
    });
    expect(validateArrowBuffer(arrow, 4n)).toBe(arrow);
    expect(() => validateArrowBuffer(arrow, 3n)).toThrow('Arrow IPC byte budget');
  });

  it('rejects invalid admission identity, binding, deadlines, budgets, and ID reuse', () => {
    const executeRequest = contractMessage('ExecuteRequest');
    const validJson = validExecuteRequestJson();
    const valid = fromJson(executeRequest, validJson);
    const withoutBinding = { ...validJson };
    delete withoutBinding.browser_read;

    expect(validateExecuteAdmission(valid, Date.parse('2026-07-23T12:00:00Z'))).toEqual(valid);
    expect(() =>
      validateExecuteAdmission(
        fromJson(executeRequest, { ...validJson, execution_id: '' }),
        Date.parse('2026-07-23T12:00:00Z'),
      ),
    ).toThrow('execution_id');
    expect(() =>
      validateExecuteAdmission(
        fromJson(executeRequest, withoutBinding),
        Date.parse('2026-07-23T12:00:00Z'),
      ),
    ).toThrow('binding');
    const withoutNestedResource = structuredClone(validJson);
    delete requiredJsonObject(withoutNestedResource.browser_read).resource;
    expect(() =>
      validateExecuteAdmission(
        fromJson(executeRequest, withoutNestedResource),
        Date.parse('2026-07-23T12:00:00Z'),
      ),
    ).toThrow('browser_read resource');
    const withoutNestedDescriptor = structuredClone(validJson);
    delete requiredJsonObject(withoutNestedDescriptor.browser_read).descriptor;
    expect(() =>
      validateExecuteAdmission(
        fromJson(executeRequest, withoutNestedDescriptor),
        Date.parse('2026-07-23T12:00:00Z'),
      ),
    ).toThrow('browser_read descriptor');
    expect(() =>
      fromJson(executeRequest, {
        ...validJson,
        logical_resource: {
          connection_id: 'connection-001',
          provider_namespace: 'axon.public-gcs/v1',
          kind: 'RESOURCE_KIND_TABLE',
          canonical_locator: 'gs://axon-fixtures/tables/events',
        },
      }),
    ).toThrow();
    expect(() =>
      validateExecuteAdmission(
        fromJson(executeRequest, {
          ...validJson,
          query: {
            ...requiredJsonObject(validJson.query),
            preferred_target: 'EXECUTION_TARGET_NATIVE',
          },
        }),
        Date.parse('2026-07-23T12:00:00Z'),
      ),
    ).toThrow('browser_read requires browser_wasm');
    const logicalBinding = { ...validJson };
    delete logicalBinding.browser_read;
    logicalBinding.logical_resource = {
      connection_id: 'connection-001',
      provider_namespace: 'axon.public-gcs/v1',
      kind: 'RESOURCE_KIND_TABLE',
      canonical_locator: 'gs://axon-fixtures/tables/events',
    };
    expect(() =>
      validateExecuteAdmission(
        fromJson(executeRequest, logicalBinding),
        Date.parse('2026-07-23T12:00:00Z'),
      ),
    ).toThrow('logical_resource requires non-browser target');
    expect(() =>
      validateExecuteAdmission(
        fromJson(executeRequest, {
          ...validJson,
          deadline: '2026-07-23T12:00:00Z',
        }),
        Date.parse('2026-07-23T12:00:00Z'),
      ),
    ).toThrow('deadline');
    expect(() =>
      validateExecuteAdmission(
        fromJson(executeRequest, {
          ...validJson,
          query: {
            ...requiredJsonObject(validJson.query),
            options: {
              runtime_limits: {
                max_result_rows: '0',
                max_arrow_ipc_bytes: '0',
                max_preview_string_bytes: '0',
                max_scan_bytes: '0',
              },
            },
          },
        }),
        Date.parse('2026-07-23T12:00:00Z'),
      ),
    ).toThrow('positive budget');
    expect(() =>
      validateExecuteAdmission(
        fromJson(executeRequest, {
          ...validJson,
          query: {
            ...requiredJsonObject(validJson.query),
            options: {
              runtime_limits: {
                max_result_rows: '502',
                max_arrow_ipc_bytes: '8388609',
                max_preview_string_bytes: '262145',
                max_scan_bytes: '67108864',
              },
            },
          },
        }),
        Date.parse('2026-07-23T12:00:00Z'),
      ),
    ).toThrow('browser-safe budget');

    const ledger = new AdmissionLedger();
    expect(ledger.admit(valid)).toBe(true);
    expect(ledger.admit(fromJson(executeRequest, validJson))).toBe(false);
    expect(() =>
      ledger.admit(
        fromJson(executeRequest, {
          ...validJson,
          query: {
            ...requiredJsonObject(validJson.query),
            sql: 'select 2',
          },
        }),
      ),
    ).toThrow('mismatched execution_id reuse');
  });

  it('rejects events after terminal and duplicate terminal delivery', () => {
    expect(() =>
      validateExecuteResponseOrder([
        { item: { case: 'admission', value: { outcome: { case: 'accepted' } } } },
        { item: { case: 'event' } },
        { item: { case: 'terminal' } },
      ]),
    ).not.toThrow();
    expect(() =>
      validateExecuteResponseOrder([
        { item: { case: 'admission', value: { outcome: { case: 'accepted' } } } },
        { item: { case: 'terminal' } },
        { item: { case: 'event' } },
      ]),
    ).toThrow('event after terminal');
    expect(() =>
      validateExecuteResponseOrder([
        { item: { case: 'admission', value: { outcome: { case: 'accepted' } } } },
        { item: { case: 'terminal' } },
        { item: { case: 'terminal' } },
      ]),
    ).toThrow('duplicate terminal');
    expect(() =>
      validateExecuteResponseOrder([
        {
          item: {
            case: 'admission',
            value: { outcome: { case: 'rejected' } },
          },
        },
        { item: { case: 'event' } },
      ]),
    ).toThrow('event after rejected admission');
    expect(() =>
      validateExecuteResponseOrder([
        {
          item: {
            case: 'admission',
            value: { outcome: { case: 'rejected' } },
          },
        },
        { item: { case: 'terminal' } },
      ]),
    ).toThrow('terminal after rejected admission');
    expect(() => validateExecuteResponseOrder([{ item: {} }])).toThrow(
      'execution response item is required',
    );
    expect(() =>
      validateExecuteResponseOrder([
        {
          item: {
            case: 'admission',
            value: { outcome: {} },
          },
        },
      ]),
    ).toThrow('admission outcome is required');
  });

  it('removes legacy identity, fallback, chunk, and descriptor declarations', () => {
    const messageNames = file_axon_exec_v1_exec.messages.map(
      (message: DescMessage) => message.name,
    );
    const enumNames = file_axon_exec_v1_exec.enums.map((value) => value.name);

    expect(messageNames).not.toEqual(
      expect.arrayContaining([
        'OpenableDescriptor',
        'FallbackReasonMarker',
        'CapabilityGateFallbackReason',
        'FallbackReason',
        'BrowserWorkerFallbackEvent',
        'BrowserWorkerCancellationEvent',
        'BrowserWorkerTerminalErrorEvent',
        'BrowserWorkerArrowIpcChunkEvent',
      ]),
    );
    expect(enumNames).not.toEqual(
      expect.arrayContaining(['CapabilityKey', 'ArrowIpcDelivery', 'BrowserWorkerSqlDelivery']),
    );
    expect(contractEnumValues('QueryErrorCode')).not.toContain(
      'QUERY_ERROR_CODE_FALLBACK_REQUIRED',
    );
    expect(contractMessage('QueryMetricsSummary').fields.map((field) => field.name)).not.toContain(
      'arrow_ipc_chunk_count',
    );
  });
});

function contractMessage(name: string): DescMessage {
  const descriptor = file_axon_exec_v1_exec.messages.find((message) => message.name === name);
  if (!descriptor) throw new TypeError(`missing corrected execution message ${name}`);
  return descriptor;
}

function contractEnumValues(name: string): string[] {
  const descriptor = file_axon_exec_v1_exec.enums.find((value) => value.name === name);
  if (!descriptor) throw new TypeError(`missing corrected execution enum ${name}`);
  return descriptor.values.map((value) => value.name);
}

function validExecuteRequestJson(): JsonObject {
  return {
    execution_id: 'execution-001',
    browser_read: {
      resource: {
        connection_id: 'connection-001',
        provider_namespace: 'axon.public-gcs/v1',
        kind: 'RESOURCE_KIND_TABLE',
        canonical_locator: 'gs://axon-fixtures/tables/events',
      },
      descriptor: {
        snapshot: {
          table_uri: 'gs://axon-fixtures/tables/events',
        },
      },
      access_class: 'BROWSER_ACCESS_CLASS_PUBLIC',
      correlation_id: 'correlation-001',
      provenance: {
        resolver_id: 'public-object-storage',
        resolution_id: 'resolution-001',
      },
    },
    query: {
      sql: 'select * from events',
      preferred_target: 'EXECUTION_TARGET_BROWSER_WASM',
      options: {
        runtime_limits: {
          max_result_rows: '501',
          max_arrow_ipc_bytes: '8388608',
          max_preview_string_bytes: '262144',
          max_scan_bytes: '67108864',
        },
      },
    },
    deadline: '2026-07-23T12:02:00Z',
  };
}

function validateExecuteAdmission<T>(request: T, nowMs: number): T {
  const value = request as {
    executionId?: string;
    binding?: {
      case?: string;
      value?: {
        connectionId?: string;
        providerNamespace?: string;
        kind?: number;
        identity?: { case?: string; value?: string };
        resource?: {
          connectionId?: string;
          providerNamespace?: string;
          kind?: number;
          identity?: { case?: string; value?: string };
        };
        descriptor?: { descriptor?: { case?: string } };
        accessClass?: number;
        correlationId?: string;
        provenance?: { resolverId?: string; resolutionId?: string };
      };
    };
    query?: {
      sql?: string;
      preferredTarget?: number;
      options?: {
        runtimeLimits?: {
          maxResultRows?: bigint;
          maxArrowIpcBytes?: bigint;
          maxPreviewStringBytes?: bigint;
          maxScanBytes?: bigint;
        };
      };
    };
    deadline?: { seconds?: bigint; nanos?: number };
  };
  if (!value.executionId) throw new TypeError('execution_id must not be empty');
  if (!value.binding?.case) throw new TypeError('exactly one binding is required');
  validateExecuteBinding(value.binding);
  if (!value.query?.sql?.trim()) throw new TypeError('SQL must not be empty');
  if (!value.query.preferredTarget) throw new TypeError('execution target must be specified');
  if (value.binding.case === 'browserRead' && value.query.preferredTarget !== 1) {
    throw new TypeError('browser_read requires browser_wasm');
  }
  if (value.binding.case === 'logicalResource' && value.query.preferredTarget === 1) {
    throw new TypeError('logical_resource requires non-browser target');
  }
  if (!value.deadline) throw new TypeError('absolute deadline is required');
  const deadlineMs = Number(value.deadline.seconds ?? 0n) * 1_000 + (value.deadline.nanos ?? 0) / 1e6;
  if (!Number.isSafeInteger(deadlineMs) || deadlineMs <= nowMs) {
    throw new TypeError('deadline must be finite and in the future');
  }
  const limits = value.query.options?.runtimeLimits;
  const budgets = [
    limits?.maxResultRows,
    limits?.maxArrowIpcBytes,
    limits?.maxPreviewStringBytes,
    limits?.maxScanBytes,
  ];
  if (budgets.some((budget) => budget === undefined || budget <= 0n)) {
    throw new TypeError('every execution budget must be a positive budget');
  }
  if (
    value.binding.case === 'browserRead' &&
    (limits!.maxResultRows! > 501n ||
      limits!.maxArrowIpcBytes! > 8n * 1024n * 1024n ||
      limits!.maxPreviewStringBytes! > 256n * 1024n)
  ) {
    throw new TypeError('browser execution exceeded a browser-safe budget');
  }
  return request;
}

function validateExecuteBinding(binding: {
  case?: string;
  value?: {
    connectionId?: string;
    providerNamespace?: string;
    kind?: number;
    identity?: { case?: string; value?: string };
    resource?: {
      connectionId?: string;
      providerNamespace?: string;
      kind?: number;
      identity?: { case?: string; value?: string };
    };
    descriptor?: { descriptor?: { case?: string } };
    accessClass?: number;
    correlationId?: string;
    provenance?: { resolverId?: string; resolutionId?: string };
  };
}): void {
  if (binding.case === 'logicalResource') {
    validateCanonicalResourceValue(binding.value, 'logical_resource');
    return;
  }
  if (binding.case !== 'browserRead') {
    throw new TypeError('exactly one supported binding is required');
  }
  const read = binding.value;
  if (!read?.resource) throw new TypeError('browser_read resource is required');
  validateCanonicalResourceValue(read.resource, 'browser_read resource');
  if (!read.descriptor?.descriptor?.case) {
    throw new TypeError('browser_read descriptor arm is required');
  }
  if (!read.accessClass) throw new TypeError('browser_read access_class is required');
  if (!read.correlationId?.trim()) throw new TypeError('browser_read correlation_id is required');
  if (!read.provenance?.resolverId?.trim() || !read.provenance.resolutionId?.trim()) {
    throw new TypeError('browser_read provenance is required');
  }
}

function validateCanonicalResourceValue(
  resource:
    | {
        connectionId?: string;
        providerNamespace?: string;
        kind?: number;
        identity?: { case?: string; value?: string };
      }
    | undefined,
  label: string,
): void {
  if (
    !resource?.connectionId?.trim() ||
    !resource.providerNamespace?.trim() ||
    !resource.kind ||
    !resource.identity?.case ||
    !resource.identity.value?.trim()
  ) {
    throw new TypeError(`${label} must contain one complete canonical resource tuple`);
  }
}

function validateArrowBuffer<T>(
  result: T,
  maxArrowIpcBytes: bigint,
): T {
  const value = result as { bytes?: Uint8Array; byteLength?: bigint };
  if (!(value.bytes instanceof Uint8Array)) {
    throw new TypeError('Arrow IPC result must contain one byte buffer');
  }
  if (value.byteLength !== BigInt(value.bytes.byteLength)) {
    throw new TypeError('Arrow IPC byte length did not match its buffer');
  }
  if (value.byteLength > maxArrowIpcBytes) {
    throw new TypeError('Arrow IPC byte budget exceeded');
  }
  return result;
}

class AdmissionLedger {
  readonly #inputs = new Map<string, string>();

  admit(request: unknown): boolean {
    const executionId = (request as { executionId?: string }).executionId;
    if (!executionId) throw new TypeError('execution_id must not be empty');
    const encoded = stableAdmissionJson(request);
    const existing = this.#inputs.get(executionId);
    if (existing === undefined) {
      this.#inputs.set(executionId, encoded);
      return true;
    }
    if (existing !== encoded) throw new TypeError('mismatched execution_id reuse');
    return false;
  }
}

function stableAdmissionJson(value: unknown): string {
  return JSON.stringify(value, (_key, item) =>
    typeof item === 'bigint' ? item.toString() : item,
  );
}

function validateExecuteResponseOrder(
  responses: Array<{
    item?: {
      case?: string;
      value?: { outcome?: { case?: string } };
    };
  }>,
): void {
  let admissionSeen = false;
  let rejected = false;
  let terminalSeen = false;
  for (const response of responses) {
    const item = response.item?.case;
    if (!item) throw new TypeError('execution response item is required');
    if (item === 'admission') {
      if (admissionSeen || terminalSeen) throw new TypeError('duplicate or late admission');
      if (!response.item?.value?.outcome?.case) {
        throw new TypeError('admission outcome is required');
      }
      admissionSeen = true;
      rejected = response.item?.value?.outcome?.case === 'rejected';
      continue;
    }
    if (!admissionSeen) throw new TypeError('execution response preceded admission');
    if (rejected) throw new TypeError(`${item} after rejected admission`);
    if (item === 'terminal') {
      if (terminalSeen) throw new TypeError('duplicate terminal');
      terminalSeen = true;
      continue;
    }
    if (item === 'event' && terminalSeen) throw new TypeError('event after terminal');
    if (item !== 'event') throw new TypeError(`unknown execution response item '${item}'`);
  }
}

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
  const capabilities: JsonObject[] = [];
  for (const [key, value] of Object.entries(report.capabilities)) {
    if (value !== undefined) {
      capabilities.push({
        key: enumSymbol('CAPABILITY_KEY', key),
        state: enumSymbol('CAPABILITY_STATE', value),
      });
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

function normalizeQueryError(error: QueryError): JsonObject {
  return compact({
    code: enumSymbol('QUERY_ERROR_CODE', error.code),
    message: error.message,
    target: enumSymbol('EXECUTION_TARGET', error.target),
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
  if (result.delivery === 'chunked_buffers' || !bytes) {
    throw new TypeError('corrected protobuf contract requires one Arrow IPC buffer');
  }
  const byteLength = result.byte_length ?? bytes?.byteLength;
  return compact({
    format: enumSymbol('ARROW_IPC_FORMAT', result.format),
    content_type: result.content_type,
    bytes: bytes ? Buffer.from(bytes).toString('base64') : undefined,
    byte_length: decimal(byteLength),
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
        execution_id: sql.request_id,
        name: sql.name,
        query: normalizeQueryRequest(query),
        output: enumSymbol('BROWSER_WORKER_SQL_OUTPUT', sql.output ?? 'arrow_ipc_stream'),
        browser_safe_defaults: sql.browser_safe_defaults ?? false,
      },
    };
  }
  if ('cancel' in command) {
    return {
      cancel: compact({
        request_id: command.cancel.request_id,
        execution_id: command.cancel.query_id ?? command.cancel.request_id,
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
    execution_id: context.query_id ?? context.request_id,
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
  throw new TypeError('corrected protobuf contract accepts only nonterminal worker events');
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
