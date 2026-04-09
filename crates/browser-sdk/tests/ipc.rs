use std::collections::BTreeMap;

use browser_sdk::{
    ArrowIpcFormat, ArrowIpcResultEnvelope, BrowserWorkerCommand, BrowserWorkerResponseEnvelope,
};
use query_contract::{
    BrowserAccessMode, BrowserHttpFileDescriptor, BrowserHttpSnapshotDescriptor, CapabilityKey,
    CapabilityReport, CapabilityState, ExecutionTarget, FallbackReason, QueryError, QueryErrorCode,
    QueryMetricsSummary, QueryRequest, QueryResponse,
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
                footer_reads: Some(2),
                snapshot_bootstrap_duration_ms: Some(6),
                access_mode: Some(BrowserAccessMode::BrowserSafeHttp),
            },
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
    assert_eq!(metrics.snapshot_bootstrap_duration_ms, Some(6));
    assert_eq!(
        metrics.access_mode,
        Some(BrowserAccessMode::BrowserSafeHttp)
    );
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
            footer_reads: None,
            snapshot_bootstrap_duration_ms: None,
            access_mode: None,
        },
    }
}
