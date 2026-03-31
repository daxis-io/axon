#![cfg(target_arch = "wasm32")]

use browser_sdk::{
    preferred_target, ArrowIpcFormat, ArrowIpcResultEnvelope, BrowserWorkerRequestEnvelope,
    BrowserWorkerResponseEnvelope,
};
use query_contract::{
    BrowserAccessMode, CapabilityKey, CapabilityReport, CapabilityState, ExecutionTarget,
    FallbackReason, QueryError, QueryErrorCode, QueryMetricsSummary, QueryRequest, QueryResponse,
};
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen_test]
fn browser_sdk_round_trips_worker_success_envelopes_in_wasm() {
    let request = BrowserWorkerRequestEnvelope::new(
        "req-1",
        QueryRequest::new(
            "gs://axon-fixtures/sample_table",
            "SELECT id FROM axon_table ORDER BY id",
            ExecutionTarget::BrowserWasm,
        ),
    );
    let response = BrowserWorkerResponseEnvelope::success(
        "req-1",
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
                footer_reads: None,
                snapshot_bootstrap_duration_ms: None,
                access_mode: None,
            },
        },
        ArrowIpcResultEnvelope::new(ArrowIpcFormat::Stream, vec![1, 2, 3, 4]),
    );

    let round_tripped_request: BrowserWorkerRequestEnvelope =
        serde_json::from_value(serde_json::to_value(&request).expect("request serializes"))
            .expect("request deserializes in wasm");
    let round_tripped_response: BrowserWorkerResponseEnvelope =
        serde_json::from_value(serde_json::to_value(&response).expect("response serializes"))
            .expect("response deserializes in wasm");

    assert_eq!(preferred_target(), ExecutionTarget::BrowserWasm);
    assert_eq!(round_tripped_request.request_id, "req-1");
    assert_eq!(round_tripped_request.request.sql, request.request.sql);
    assert_eq!(
        round_tripped_response
            .success_envelope()
            .expect("success envelope should be present")
            .result
            .content_type,
        ArrowIpcFormat::Stream.content_type()
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
                footer_reads: Some(2),
                snapshot_bootstrap_duration_ms: Some(6),
                access_mode: Some(BrowserAccessMode::BrowserSafeHttp),
            },
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
