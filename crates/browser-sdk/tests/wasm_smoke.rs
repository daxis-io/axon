#![cfg(target_arch = "wasm32")]

use std::collections::BTreeMap;

use browser_sdk::{
    preferred_target, ArrowIpcFormat, ArrowIpcResultEnvelope, BrowserWorkerCommand,
    BrowserWorkerResponseEnvelope,
};
use query_contract::{
    BrowserAccessMode, BrowserHttpFileDescriptor, BrowserHttpSnapshotDescriptor, CapabilityKey,
    CapabilityReport, CapabilityState, ExecutionTarget, FallbackReason, QueryError, QueryErrorCode,
    QueryMetricsSummary, QueryRequest, QueryResponse,
};
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen_test]
fn browser_sdk_round_trips_worker_commands_in_wasm() {
    let snapshot = sample_snapshot_descriptor();
    let open_table = BrowserWorkerCommand::open_table("req-open", "events", snapshot.clone());
    let sql = BrowserWorkerCommand::sql(
        "req-sql",
        "events",
        QueryRequest::new(
            snapshot.table_uri.clone(),
            "SELECT id FROM axon_table ORDER BY id",
            ExecutionTarget::BrowserWasm,
        ),
    );
    let dispose = BrowserWorkerCommand::dispose("req-dispose", "events");

    let round_tripped_open: BrowserWorkerCommand =
        serde_json::from_value(serde_json::to_value(&open_table).expect("open table serializes"))
            .expect("open table deserializes in wasm");
    let round_tripped_sql: BrowserWorkerCommand =
        serde_json::from_value(serde_json::to_value(&sql).expect("sql serializes"))
            .expect("sql deserializes in wasm");
    let round_tripped_dispose: BrowserWorkerCommand =
        serde_json::from_value(serde_json::to_value(&dispose).expect("dispose serializes"))
            .expect("dispose deserializes in wasm");

    assert_eq!(preferred_target(), ExecutionTarget::BrowserWasm);
    assert_eq!(round_tripped_open.request_id(), "req-open");
    assert_eq!(round_tripped_open.table_name(), "events");
    assert_eq!(round_tripped_sql.request_id(), "req-sql");
    assert_eq!(round_tripped_sql.table_name(), "events");
    assert_eq!(round_tripped_dispose.request_id(), "req-dispose");
    assert_eq!(round_tripped_dispose.table_name(), "events");
}

#[wasm_bindgen_test]
fn browser_sdk_round_trips_worker_responses_in_wasm() {
    let opened = BrowserWorkerResponseEnvelope::opened("req-open", "events");
    let success = BrowserWorkerResponseEnvelope::success(
        "req-sql",
        QueryResponse {
            executed_on: ExecutionTarget::BrowserWasm,
            capabilities: CapabilityReport::from_pairs([(
                CapabilityKey::RangeReads,
                CapabilityState::Supported,
            )]),
            fallback_reason: None,
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
                footer_cache_hits: None,
                footer_cache_misses: None,
                footer_range_reads_avoided: None,
                footer_cache_degraded_identity_reads: None,
                identity_present_range_reads: None,
                identity_missing_range_reads: None,
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
        },
        ArrowIpcResultEnvelope::new(ArrowIpcFormat::Stream, vec![1, 2, 3, 4]),
    );
    let disposed = BrowserWorkerResponseEnvelope::disposed("req-dispose", "events");

    let round_tripped_opened: BrowserWorkerResponseEnvelope =
        serde_json::from_value(serde_json::to_value(&opened).expect("opened serializes"))
            .expect("opened deserializes in wasm");
    let round_tripped_success: BrowserWorkerResponseEnvelope =
        serde_json::from_value(serde_json::to_value(&success).expect("success serializes"))
            .expect("success deserializes in wasm");
    let round_tripped_disposed: BrowserWorkerResponseEnvelope =
        serde_json::from_value(serde_json::to_value(&disposed).expect("disposed serializes"))
            .expect("disposed deserializes in wasm");

    assert_eq!(
        round_tripped_opened
            .opened_envelope()
            .expect("opened envelope should be present")
            .name,
        "events"
    );
    assert_eq!(
        round_tripped_success
            .success_envelope()
            .expect("success envelope should be present")
            .result
            .content_type,
        ArrowIpcFormat::Stream.content_type()
    );
    assert_eq!(
        round_tripped_disposed
            .disposed_envelope()
            .expect("disposed envelope should be present")
            .name,
        "events"
    );
}

#[wasm_bindgen_test]
fn browser_sdk_preserves_structured_fallbacks_and_content_type_validation_in_wasm() {
    let fallback_reason = FallbackReason::CapabilityGate {
        capability: CapabilityKey::DeletionVectors,
        required_state: CapabilityState::NativeOnly,
    };
    let response = BrowserWorkerResponseEnvelope::error(
        "req-2",
        QueryError::new(
            QueryErrorCode::FallbackRequired,
            "native-only browser capability",
            ExecutionTarget::BrowserWasm,
        )
        .with_fallback_reason(fallback_reason.clone()),
    );

    let round_tripped: BrowserWorkerResponseEnvelope =
        serde_json::from_value(serde_json::to_value(&response).expect("response serializes"))
            .expect("response deserializes in wasm");
    let invalid_result = serde_json::json!({
        "format": "stream",
        "content_type": "application/vnd.apache.arrow.file",
        "bytes": [1, 2, 3, 4]
    });
    let error = serde_json::from_value::<ArrowIpcResultEnvelope>(invalid_result)
        .expect_err("mismatched Arrow IPC content types must fail in wasm");

    assert_eq!(round_tripped.fallback_reason(), Some(&fallback_reason));
    assert!(error
        .to_string()
        .contains("does not match expected content type"));
}

#[wasm_bindgen_test]
fn browser_sdk_rejects_legacy_row_payloads_in_wasm() {
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
                "bytes": [1, 2, 3, 4],
                "rows": [
                    {"row_count": 1}
                ]
            }
        }
    });

    let error = serde_json::from_value::<BrowserWorkerResponseEnvelope>(invalid)
        .expect_err("legacy row payloads must fail in wasm");
    assert!(error.to_string().contains("unknown field `rows`"));
}

#[wasm_bindgen_test]
fn browser_sdk_round_trips_browser_telemetry_fields_in_wasm() {
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
                footer_cache_hits: Some(1),
                footer_cache_misses: Some(3),
                footer_range_reads_avoided: Some(2),
                footer_cache_degraded_identity_reads: Some(1),
                identity_present_range_reads: Some(5),
                identity_missing_range_reads: Some(7),
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
            .expect("response deserializes in wasm");
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

fn sample_snapshot_descriptor() -> BrowserHttpSnapshotDescriptor {
    BrowserHttpSnapshotDescriptor {
        table_uri: "gs://axon-fixtures/sample_table".to_string(),
        snapshot_version: 1,
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
