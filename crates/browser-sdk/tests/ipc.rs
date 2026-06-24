use std::collections::BTreeMap;

use browser_sdk::{
    ArrowIpcFormat, ArrowIpcResultEnvelope, BrowserWorkerCommand, BrowserWorkerEventContext,
    BrowserWorkerEventEnvelope, BrowserWorkerEventPhase, BrowserWorkerLogLevel,
    BrowserWorkerProgressStage, BrowserWorkerResponseEnvelope, BrowserWorkerSqlOutput,
    BrowserWorkerTransportCacheMetrics,
};
use query_contract::{
    BrowserAccessMode, BrowserHttpFileDescriptor, BrowserHttpParquetDatasetDescriptor,
    BrowserHttpSnapshotDescriptor, CapabilityKey, CapabilityReport, CapabilityState,
    ExecutionTarget, FallbackReason, ParquetCompressionSummary, ParquetInspectionColumn,
    ParquetInspectionColumnChunk, ParquetInspectionRowGroup, ParquetInspectionSummary, QueryError,
    QueryErrorCode, QueryMetricsSummary, QueryRequest, QueryResponse,
};

#[test]
fn browser_sdk_round_trips_open_table_query_and_dispose_commands() {
    let snapshot = sample_snapshot_descriptor();
    let open_table = BrowserWorkerCommand::open_table("req-open", "events", snapshot.clone());
    let sql = BrowserWorkerCommand::sql(
        "req-sql",
        "events",
        QueryRequest::new(
            snapshot.table_uri.clone(),
            "select count(*) from axon_table",
            ExecutionTarget::BrowserWasm,
        ),
    );
    let dispose = BrowserWorkerCommand::dispose("req-dispose", "events");

    let round_tripped_open: BrowserWorkerCommand =
        serde_json::from_value(serde_json::to_value(&open_table).expect("open table serializes"))
            .expect("open table deserializes");
    let round_tripped_sql: BrowserWorkerCommand =
        serde_json::from_value(serde_json::to_value(&sql).expect("sql serializes"))
            .expect("sql deserializes");
    let round_tripped_dispose: BrowserWorkerCommand =
        serde_json::from_value(serde_json::to_value(&dispose).expect("dispose serializes"))
            .expect("dispose deserializes");

    match round_tripped_open {
        BrowserWorkerCommand::OpenTable(command) => {
            assert_eq!(command.request_id, "req-open");
            assert_eq!(command.name, "events");
            assert_eq!(command.snapshot, snapshot);
        }
        _ => panic!("expected open_table command"),
    }

    match round_tripped_sql {
        BrowserWorkerCommand::Sql(command) => {
            assert_eq!(command.request_id, "req-sql");
            assert_eq!(command.name, "events");
            assert_eq!(command.request.sql, "select count(*) from axon_table");
        }
        _ => panic!("expected sql command"),
    }

    match round_tripped_dispose {
        BrowserWorkerCommand::Dispose(command) => {
            assert_eq!(command.request_id, "req-dispose");
            assert_eq!(command.name, "events");
        }
        _ => panic!("expected dispose command"),
    }
}

#[test]
fn browser_sdk_round_trips_open_delta_table_and_arrow_ipc_sql_output() {
    let snapshot = sample_snapshot_descriptor();
    let open_delta_table =
        BrowserWorkerCommand::open_delta_table("req-open-delta", "events", snapshot.clone());
    let sql = BrowserWorkerCommand::sql(
        "req-sql-ipc",
        "events",
        QueryRequest::new(
            snapshot.table_uri.clone(),
            "select count(*) as rows from events",
            ExecutionTarget::BrowserWasm,
        ),
    );

    let serialized_open =
        serde_json::to_value(&open_delta_table).expect("open delta table serializes");
    assert!(
        serialized_open.get("open_delta_table").is_some(),
        "OpenDeltaTable should use the browser open_delta_table command tag"
    );

    let round_tripped_open: BrowserWorkerCommand =
        serde_json::from_value(serialized_open).expect("open delta table deserializes");
    match round_tripped_open {
        BrowserWorkerCommand::OpenDeltaTable(command) => {
            assert_eq!(command.request_id, "req-open-delta");
            assert_eq!(command.name, "events");
            assert_eq!(command.snapshot, snapshot);
        }
        _ => panic!("expected open_delta_table command"),
    }

    let serialized_sql = serde_json::to_value(&sql).expect("sql serializes");
    assert_eq!(
        serialized_sql["sql"]["output"],
        serde_json::json!("arrow_ipc_stream")
    );
    assert_eq!(
        serialized_sql["sql"]["query"]["sql"],
        serde_json::json!("select count(*) as rows from events")
    );
    assert!(serialized_sql["sql"].get("request").is_none());

    let round_tripped_sql: BrowserWorkerCommand =
        serde_json::from_value(serialized_sql).expect("sql deserializes");
    match round_tripped_sql {
        BrowserWorkerCommand::Sql(command) => {
            assert_eq!(command.request_id, "req-sql-ipc");
            assert_eq!(command.name, "events");
            assert_eq!(command.output, BrowserWorkerSqlOutput::ArrowIpcStream);
            assert_eq!(command.request.sql, "select count(*) as rows from events");
        }
        _ => panic!("expected sql command"),
    }
}

#[test]
fn browser_sdk_round_trips_open_parquet_dataset_command() {
    let dataset = sample_parquet_dataset_descriptor();
    let open_parquet_dataset =
        BrowserWorkerCommand::open_parquet_dataset("req-open-parquet", "events", dataset.clone());

    let serialized_open =
        serde_json::to_value(&open_parquet_dataset).expect("open parquet dataset serializes");
    assert!(
        serialized_open.get("open_parquet_dataset").is_some(),
        "OpenParquetDataset should use the browser open_parquet_dataset command tag"
    );
    assert!(serialized_open["open_parquet_dataset"]["dataset"]
        .get("snapshot_version")
        .is_none());
    assert!(serialized_open["open_parquet_dataset"]["dataset"]
        .get("active_files")
        .is_none());

    let round_tripped_open: BrowserWorkerCommand =
        serde_json::from_value(serialized_open).expect("open parquet dataset deserializes");
    match round_tripped_open {
        BrowserWorkerCommand::OpenParquetDataset(command) => {
            assert_eq!(command.request_id, "req-open-parquet");
            assert_eq!(command.name, "events");
            assert_eq!(command.dataset, dataset);
        }
        _ => panic!("expected open_parquet_dataset command"),
    }
}

#[test]
fn browser_sdk_round_trips_parquet_inspection_command_and_response() {
    let command = BrowserWorkerCommand::inspect_parquet(
        "req-inspect",
        "events",
        "event_date=2026-01-01/part-000.parquet",
    );
    let serialized_command = serde_json::to_value(&command).expect("inspect command serializes");
    assert_eq!(
        serialized_command["inspect_parquet"]["path"],
        serde_json::json!("event_date=2026-01-01/part-000.parquet")
    );

    let round_tripped_command: BrowserWorkerCommand =
        serde_json::from_value(serialized_command).expect("inspect command deserializes");
    match round_tripped_command {
        BrowserWorkerCommand::InspectParquet(command) => {
            assert_eq!(command.request_id, "req-inspect");
            assert_eq!(command.name, "events");
            assert_eq!(command.path, "event_date=2026-01-01/part-000.parquet");
        }
        _ => panic!("expected inspect_parquet command"),
    }

    let response = BrowserWorkerResponseEnvelope::parquet_inspection(
        "req-inspect",
        sample_parquet_inspection(),
    );
    let serialized_response =
        serde_json::to_value(&response).expect("inspection response serializes");
    assert_eq!(
        serialized_response["parquet_inspection"]["summary"]["path"],
        serde_json::json!("event_date=2026-01-01/part-000.parquet")
    );
    assert_eq!(
        serialized_response["parquet_inspection"]["summary"]["columns"][0]["has_offset_index"],
        serde_json::json!(true)
    );

    let round_tripped_response: BrowserWorkerResponseEnvelope =
        serde_json::from_value(serialized_response).expect("inspection response deserializes");
    let envelope = round_tripped_response
        .parquet_inspection_envelope()
        .expect("inspection envelope should be present");
    assert_eq!(envelope.request_id, "req-inspect");
    assert_eq!(
        envelope.summary.path,
        "event_date=2026-01-01/part-000.parquet"
    );
    assert_eq!(envelope.summary.columns[0].encodings, vec!["PLAIN"]);
}

#[test]
fn browser_sdk_exposes_arrow_ipc_result_envelope() {
    let response = BrowserWorkerResponseEnvelope::success(
        "req-1",
        sample_query_response(ExecutionTarget::BrowserWasm, None),
        ArrowIpcResultEnvelope::new(ArrowIpcFormat::Stream, vec![0xff, 0xff, 0x00, 0x01]),
    );

    let serialized = serde_json::to_value(&response).expect("response serializes");
    assert_eq!(
        serialized["success"]["result"]["content_type"],
        "application/vnd.apache.arrow.stream"
    );
    assert_eq!(serialized["success"]["result"]["format"], "stream");
    assert!(serialized["success"]["result"].get("rows").is_none());

    let round_tripped: BrowserWorkerResponseEnvelope =
        serde_json::from_value(serialized).expect("response deserializes");
    let success = round_tripped.success_envelope().expect("success envelope");
    assert_eq!(success.request_id, "req-1");
    assert_eq!(success.result.format, ArrowIpcFormat::Stream);
    assert_eq!(
        success.result.content_type,
        "application/vnd.apache.arrow.stream"
    );
    assert_eq!(success.result.bytes, vec![0xff, 0xff, 0x00, 0x01]);
}

fn sample_parquet_inspection() -> ParquetInspectionSummary {
    ParquetInspectionSummary {
        path: "event_date=2026-01-01/part-000.parquet".to_string(),
        object_size_bytes: 128,
        footer_length_bytes: 32,
        metadata_memory_size_bytes: 512,
        created_by: Some("axon test".to_string()),
        file_version: 2,
        row_group_count: 1,
        row_count: 3,
        column_count: 1,
        compression: ParquetCompressionSummary {
            compressed_size_bytes: 64,
            uncompressed_size_bytes: 128,
            ratio_basis_points: 5000,
        },
        columns: vec![ParquetInspectionColumn {
            name: "id".to_string(),
            physical_type: "Int64".to_string(),
            logical_type: None,
            converted_type: None,
            repetition: "Required".to_string(),
            nullable: false,
            compressed_size_bytes: 64,
            uncompressed_size_bytes: 128,
            null_count: Some(0),
            encodings: vec!["PLAIN".to_string()],
            compressions: vec!["UNCOMPRESSED".to_string()],
            has_statistics: true,
            has_column_index: true,
            has_offset_index: true,
            has_bloom_filter: false,
        }],
        row_groups: vec![ParquetInspectionRowGroup {
            index: 0,
            row_count: 3,
            compressed_size_bytes: 64,
            uncompressed_size_bytes: 128,
            columns: vec![ParquetInspectionColumnChunk {
                column_name: "id".to_string(),
                compression: "UNCOMPRESSED".to_string(),
                encodings: vec!["PLAIN".to_string()],
                compressed_size_bytes: 64,
                uncompressed_size_bytes: 128,
                null_count: Some(0),
                has_statistics: true,
                has_column_index: true,
                has_offset_index: true,
                has_bloom_filter: false,
            }],
        }],
    }
}

#[test]
fn browser_sdk_preserves_structured_fallback_reason() {
    let fallback_reason = FallbackReason::CapabilityGate {
        capability: CapabilityKey::TimeTravel,
        required_state: CapabilityState::NativeOnly,
    };
    let response = BrowserWorkerResponseEnvelope::success(
        "req-2",
        sample_query_response(ExecutionTarget::Native, Some(fallback_reason.clone())),
        ArrowIpcResultEnvelope::new(ArrowIpcFormat::File, vec![1, 2, 3]),
    );

    let round_tripped: BrowserWorkerResponseEnvelope =
        serde_json::from_value(serde_json::to_value(&response).expect("response serializes"))
            .expect("response deserializes");
    let preserved = round_tripped
        .fallback_reason()
        .expect("fallback reason survives worker envelope");
    assert_eq!(preserved, &fallback_reason);
}

#[test]
fn browser_sdk_round_trips_opened_and_disposed_responses() {
    let opened = BrowserWorkerResponseEnvelope::opened("req-open", "events");
    let disposed = BrowserWorkerResponseEnvelope::disposed("req-dispose", "events");

    let round_tripped_opened: BrowserWorkerResponseEnvelope =
        serde_json::from_value(serde_json::to_value(&opened).expect("opened serializes"))
            .expect("opened deserializes");
    let round_tripped_disposed: BrowserWorkerResponseEnvelope =
        serde_json::from_value(serde_json::to_value(&disposed).expect("disposed serializes"))
            .expect("disposed deserializes");

    assert_eq!(
        round_tripped_opened
            .opened_envelope()
            .expect("opened envelope should exist")
            .name,
        "events"
    );
    assert_eq!(
        round_tripped_disposed
            .disposed_envelope()
            .expect("disposed envelope should exist")
            .name,
        "events"
    );
    assert_eq!(round_tripped_opened.fallback_reason(), None);
    assert_eq!(round_tripped_disposed.fallback_reason(), None);
}

#[test]
fn browser_sdk_round_trips_browser_telemetry_fields() {
    let response = BrowserWorkerResponseEnvelope::success(
        "req-telemetry",
        QueryResponse {
            executed_on: ExecutionTarget::BrowserWasm,
            capabilities: CapabilityReport::from_pairs([(
                CapabilityKey::RangeReads,
                CapabilityState::Supported,
            )]),
            fallback_reason: None,
            metrics: QueryMetricsSummary {
                bytes_fetched: 2048,
                duration_ms: 15,
                files_touched: 2,
                files_skipped: 1,
                row_groups_touched: 2,
                row_groups_skipped: 1,
                footer_reads: Some(2),
                bootstrap_footer_range_reads: Some(4),
                scan_footer_range_reads: Some(2),
                scan_data_range_reads: Some(6),
                duplicate_range_reads: Some(2),
                descriptor_resolution_count: Some(1),
                delta_log_manifest_list_count: Some(1),
                delta_log_manifest_list_duration_ms: Some(3),
                snapshot_resolve_count: Some(1),
                snapshot_resolve_duration_ms: Some(6),
                rows_emitted: 3,
                snapshot_bootstrap_duration_ms: Some(6),
                access_mode: Some(BrowserAccessMode::BrowserSafeHttp),
            },
            explain: None,
        },
        ArrowIpcResultEnvelope::new(ArrowIpcFormat::Stream, vec![9, 8, 7, 6]),
    );

    let round_tripped: BrowserWorkerResponseEnvelope =
        serde_json::from_value(serde_json::to_value(&response).expect("response serializes"))
            .expect("response deserializes");
    let metrics = &round_tripped
        .success_envelope()
        .expect("success envelope should be present")
        .response
        .metrics;

    assert_eq!(metrics.footer_reads, Some(2));
    assert_eq!(metrics.bootstrap_footer_range_reads, Some(4));
    assert_eq!(metrics.scan_footer_range_reads, Some(2));
    assert_eq!(metrics.scan_data_range_reads, Some(6));
    assert_eq!(metrics.duplicate_range_reads, Some(2));
    assert_eq!(metrics.descriptor_resolution_count, Some(1));
    assert_eq!(metrics.delta_log_manifest_list_count, Some(1));
    assert_eq!(metrics.snapshot_resolve_count, Some(1));
    assert_eq!(metrics.snapshot_bootstrap_duration_ms, Some(6));
    assert_eq!(
        metrics.access_mode,
        Some(BrowserAccessMode::BrowserSafeHttp)
    );
}

#[test]
fn browser_sdk_round_trips_typed_worker_runtime_events() {
    let context = BrowserWorkerEventContext::query("req-query-42", "events");
    let progress =
        BrowserWorkerEventEnvelope::progress(context.clone(), BrowserWorkerProgressStage::Started);
    let log = BrowserWorkerEventEnvelope::log(
        context.clone(),
        BrowserWorkerLogLevel::Info,
        "query accepted by browser worker",
    );
    let range_metrics = BrowserWorkerEventEnvelope::range_read_metrics(
        context.clone(),
        QueryMetricsSummary {
            bytes_fetched: 4096,
            duration_ms: 11,
            files_touched: 2,
            files_skipped: 1,
            row_groups_touched: 4,
            row_groups_skipped: 3,
            footer_reads: Some(2),
            bootstrap_footer_range_reads: Some(4),
            scan_footer_range_reads: Some(2),
            scan_data_range_reads: Some(6),
            duplicate_range_reads: Some(2),
            descriptor_resolution_count: Some(1),
            delta_log_manifest_list_count: Some(1),
            delta_log_manifest_list_duration_ms: Some(3),
            snapshot_resolve_count: Some(1),
            snapshot_resolve_duration_ms: Some(6),
            rows_emitted: 25,
            snapshot_bootstrap_duration_ms: Some(7),
            access_mode: Some(BrowserAccessMode::BrowserSafeHttp),
        },
    );

    let serialized_progress = serde_json::to_value(&progress).expect("progress event serializes");
    assert_eq!(
        serialized_progress["progress"]["context"]["phase"],
        serde_json::json!("query")
    );
    assert_eq!(
        serialized_progress["progress"]["context"]["request_id"],
        serde_json::json!("req-query-42")
    );
    assert_eq!(
        serialized_progress["progress"]["context"]["query_id"],
        serde_json::json!("req-query-42")
    );
    assert_eq!(
        serialized_progress["progress"]["context"]["table_name"],
        serde_json::json!("events")
    );
    assert_eq!(
        serialized_progress["progress"]["stage"],
        serde_json::json!("started")
    );

    let round_tripped_progress: BrowserWorkerEventEnvelope =
        serde_json::from_value(serialized_progress).expect("progress event deserializes");
    assert_eq!(round_tripped_progress.context(), &context);

    let round_tripped_log: BrowserWorkerEventEnvelope =
        serde_json::from_value(serde_json::to_value(&log).expect("log event serializes"))
            .expect("log event deserializes");
    assert_eq!(round_tripped_log.context(), &context);

    let round_tripped_range_metrics: BrowserWorkerEventEnvelope = serde_json::from_value(
        serde_json::to_value(&range_metrics).expect("range metrics event serializes"),
    )
    .expect("range metrics event deserializes");
    match round_tripped_range_metrics {
        BrowserWorkerEventEnvelope::RangeReadMetrics(event) => {
            assert_eq!(event.context.phase, BrowserWorkerEventPhase::Query);
            assert_eq!(event.context.request_id.as_deref(), Some("req-query-42"));
            assert_eq!(event.context.query_id.as_deref(), Some("req-query-42"));
            assert_eq!(event.bytes_fetched, 4096);
            assert_eq!(event.files_touched, 2);
            assert_eq!(event.files_skipped, 1);
            assert_eq!(event.row_groups_touched, 4);
            assert_eq!(event.row_groups_skipped, 3);
            assert_eq!(event.footer_reads, Some(2));
            assert_eq!(event.bootstrap_footer_range_reads, Some(4));
            assert_eq!(event.scan_footer_range_reads, Some(2));
            assert_eq!(event.scan_data_range_reads, Some(6));
            assert_eq!(event.duplicate_range_reads, Some(2));
            assert_eq!(event.descriptor_resolution_count, Some(1));
            assert_eq!(event.delta_log_manifest_list_count, Some(1));
            assert_eq!(event.snapshot_resolve_count, Some(1));
            assert_eq!(event.rows_emitted, 25);
            assert_eq!(event.snapshot_bootstrap_duration_ms, Some(7));
            assert_eq!(event.access_mode, Some(BrowserAccessMode::BrowserSafeHttp));
        }
        other => panic!("expected range_read_metrics event, got {other:?}"),
    }
}

#[test]
fn browser_sdk_round_trips_worker_fallback_cache_cancellation_and_error_events() {
    let context = BrowserWorkerEventContext::query("req-query-43", "events");
    let fallback = BrowserWorkerEventEnvelope::fallback(
        context.clone(),
        FallbackReason::BrowserRuntimeConstraint,
    );
    let cache = BrowserWorkerEventEnvelope::cache_metrics(
        context.clone(),
        8192,
        2,
        65_536,
        Some(BrowserWorkerTransportCacheMetrics {
            bytes_reused: 1024,
            validation_misses: 1,
            persistent_cache_errors: 0,
        }),
    );
    let cancellation_error = QueryError::new(
        QueryErrorCode::ExecutionFailed,
        "experimental browser DataFusion query cancelled",
        ExecutionTarget::BrowserWasm,
    );
    let cancellation =
        BrowserWorkerEventEnvelope::cancellation(context.clone(), cancellation_error.clone());
    let terminal_error = BrowserWorkerEventEnvelope::terminal_error(
        context.clone(),
        QueryError::new(
            QueryErrorCode::FallbackRequired,
            "browser query exceeded runtime budget",
            ExecutionTarget::BrowserWasm,
        )
        .with_fallback_reason(FallbackReason::BrowserRuntimeConstraint),
    );

    let round_tripped_fallback: BrowserWorkerEventEnvelope =
        serde_json::from_value(serde_json::to_value(&fallback).expect("fallback serializes"))
            .expect("fallback deserializes");
    assert_eq!(round_tripped_fallback.context(), &context);

    let round_tripped_cache: BrowserWorkerEventEnvelope =
        serde_json::from_value(serde_json::to_value(&cache).expect("cache serializes"))
            .expect("cache deserializes");
    match round_tripped_cache {
        BrowserWorkerEventEnvelope::CacheMetrics(event) => {
            assert_eq!(event.context, context);
            assert_eq!(event.session_cached_bytes, 8192);
            assert_eq!(event.session_table_count, 2);
            assert_eq!(event.max_session_cached_bytes, 65_536);
            let transport = event.transport.expect("transport metrics should survive");
            assert_eq!(transport.bytes_reused, 1024);
            assert_eq!(transport.validation_misses, 1);
            assert_eq!(transport.persistent_cache_errors, 0);
        }
        other => panic!("expected cache_metrics event, got {other:?}"),
    }

    let round_tripped_cancellation: BrowserWorkerEventEnvelope = serde_json::from_value(
        serde_json::to_value(&cancellation).expect("cancellation serializes"),
    )
    .expect("cancellation deserializes");
    match round_tripped_cancellation {
        BrowserWorkerEventEnvelope::Cancellation(event) => {
            assert_eq!(event.context.request_id.as_deref(), Some("req-query-43"));
            assert_eq!(event.error, cancellation_error);
        }
        other => panic!("expected cancellation event, got {other:?}"),
    }

    let round_tripped_terminal_error: BrowserWorkerEventEnvelope = serde_json::from_value(
        serde_json::to_value(&terminal_error).expect("terminal error serializes"),
    )
    .expect("terminal error deserializes");
    match round_tripped_terminal_error {
        BrowserWorkerEventEnvelope::TerminalError(event) => {
            assert_eq!(
                event.error.fallback_reason,
                Some(FallbackReason::BrowserRuntimeConstraint)
            );
        }
        other => panic!("expected terminal_error event, got {other:?}"),
    }
}

#[test]
fn browser_sdk_rejects_mismatched_arrow_ipc_content_type() {
    let invalid = serde_json::json!({
        "format": "stream",
        "content_type": "application/vnd.apache.arrow.file",
        "bytes": [255, 255, 0, 1]
    });

    let error = serde_json::from_value::<ArrowIpcResultEnvelope>(invalid)
        .expect_err("mismatched format/content_type must fail");
    assert!(
        error
            .to_string()
            .contains("does not match expected content type"),
        "unexpected error: {error}"
    );
}

#[test]
fn browser_sdk_rejects_legacy_row_payloads_in_success_envelope() {
    let invalid = serde_json::json!({
        "success": {
            "request_id": "req-legacy-rows",
            "response": {
                "executed_on": "browser_wasm",
                "capabilities": {
                    "capabilities": {}
                },
                "metrics": {
                    "bytes_fetched": 128,
                    "duration_ms": 4,
                    "files_touched": 1,
                    "files_skipped": 0
                }
            },
            "result": {
                "format": "stream",
                "content_type": "application/vnd.apache.arrow.stream",
                "bytes": [255, 255, 0, 1],
                "rows": [
                    {"row_count": 1}
                ]
            }
        }
    });

    let error = serde_json::from_value::<BrowserWorkerResponseEnvelope>(invalid)
        .expect_err("legacy row payloads must be rejected at the worker boundary");
    assert!(
        error.to_string().contains("unknown field `rows`"),
        "unexpected error: {error}"
    );
}

#[test]
fn browser_sdk_round_trips_error_envelope_with_fallback_reason() {
    let fallback_reason = FallbackReason::NetworkFailure;
    let response = BrowserWorkerResponseEnvelope::error(
        "req-3",
        QueryError::new(
            QueryErrorCode::FallbackRequired,
            "browser worker lost connectivity",
            ExecutionTarget::BrowserWasm,
        )
        .with_fallback_reason(fallback_reason.clone()),
    );

    let serialized = serde_json::to_value(&response).expect("error response serializes");
    let round_tripped: BrowserWorkerResponseEnvelope =
        serde_json::from_value(serialized).expect("error response deserializes");

    assert!(round_tripped.success_envelope().is_none());
    let preserved = round_tripped
        .fallback_reason()
        .expect("error fallback reason survives worker envelope");
    assert_eq!(preserved, &fallback_reason);
}

#[test]
fn browser_sdk_preserves_terminal_error_metadata_without_result_payload() {
    let response = BrowserWorkerResponseEnvelope::error(
        "req-hard-fail",
        QueryError::new(
            QueryErrorCode::UnsupportedFeature,
            "unknown Delta protocol feature",
            ExecutionTarget::Native,
        ),
    );

    let round_tripped: BrowserWorkerResponseEnvelope =
        serde_json::from_value(serde_json::to_value(&response).expect("error response serializes"))
            .expect("error response deserializes");

    assert!(round_tripped.success_envelope().is_none());
    assert_eq!(round_tripped.fallback_reason(), None);
    assert_eq!(round_tripped.request_id(), "req-hard-fail");

    match round_tripped {
        BrowserWorkerResponseEnvelope::Error(error) => {
            assert_eq!(error.request_id, "req-hard-fail");
            assert_eq!(error.error.code, QueryErrorCode::UnsupportedFeature);
            assert_eq!(error.error.target, ExecutionTarget::Native);
            assert_eq!(error.error.message, "unknown Delta protocol feature");
        }
        _ => panic!("hard-fail response must remain an error envelope"),
    }
}

fn sample_snapshot_descriptor() -> BrowserHttpSnapshotDescriptor {
    BrowserHttpSnapshotDescriptor {
        table_uri: "gs://axon-fixtures/partitioned-table".to_string(),
        snapshot_version: 7,
        partition_column_types: BTreeMap::new(),
        browser_compatibility: CapabilityReport::default(),
        required_capabilities: CapabilityReport::default(),
        active_files: vec![BrowserHttpFileDescriptor {
            path: "part-000.parquet".to_string(),
            url: "https://example.invalid/part-000.parquet".to_string(),
            size_bytes: 128,
            partition_values: BTreeMap::new(),
            stats: None,
        }],
    }
}

fn sample_parquet_dataset_descriptor() -> BrowserHttpParquetDatasetDescriptor {
    BrowserHttpParquetDatasetDescriptor {
        table_uri: "https://example.invalid/datasets/events".to_string(),
        partition_column_types: BTreeMap::from([(
            "event_date".to_string(),
            query_contract::PartitionColumnType::String,
        )]),
        browser_compatibility: CapabilityReport::default(),
        required_capabilities: CapabilityReport::from_pairs([(
            CapabilityKey::RangeReads,
            CapabilityState::Supported,
        )]),
        files: vec![BrowserHttpFileDescriptor {
            path: "event_date=2026-06-01/part-000.parquet".to_string(),
            url: "https://storage.example.invalid/events/part-000.parquet".to_string(),
            size_bytes: 1234,
            partition_values: BTreeMap::from([(
                "event_date".to_string(),
                Some("2026-06-01".to_string()),
            )]),
            stats: None,
        }],
    }
}

fn sample_query_response(
    executed_on: ExecutionTarget,
    fallback_reason: Option<FallbackReason>,
) -> QueryResponse {
    QueryResponse {
        executed_on,
        capabilities: CapabilityReport::default(),
        fallback_reason,
        metrics: QueryMetricsSummary {
            bytes_fetched: 128,
            duration_ms: 4,
            files_touched: 1,
            files_skipped: 0,
            row_groups_touched: 1,
            row_groups_skipped: 0,
            footer_reads: None,
            bootstrap_footer_range_reads: None,
            scan_footer_range_reads: None,
            scan_data_range_reads: None,
            duplicate_range_reads: None,
            descriptor_resolution_count: None,
            delta_log_manifest_list_count: None,
            delta_log_manifest_list_duration_ms: None,
            snapshot_resolve_count: None,
            snapshot_resolve_duration_ms: None,
            rows_emitted: 1,
            snapshot_bootstrap_duration_ms: None,
            access_mode: None,
        },
        explain: None,
    }
}
