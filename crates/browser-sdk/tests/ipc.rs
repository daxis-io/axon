use browser_sdk::{
    ArrowIpcFormat, ArrowIpcResultEnvelope, BrowserWorkerRequestEnvelope,
    BrowserWorkerResponseEnvelope,
};
use query_contract::{
    CapabilityKey, CapabilityReport, CapabilityState, ExecutionTarget, FallbackReason,
    QueryMetricsSummary, QueryRequest, QueryResponse,
};

#[test]
fn browser_sdk_exposes_arrow_ipc_result_envelope() {
    let request = BrowserWorkerRequestEnvelope::new(
        "req-1",
        QueryRequest::new(
            "s3://bucket/table",
            "select count(*) from axon_table",
            ExecutionTarget::BrowserWasm,
        ),
    );

    let round_tripped_request: BrowserWorkerRequestEnvelope =
        serde_json::from_value(serde_json::to_value(&request).expect("request serializes"))
            .expect("request deserializes");
    assert_eq!(round_tripped_request.request_id, "req-1");
    assert_eq!(
        round_tripped_request.request.sql,
        "select count(*) from axon_table"
    );

    let response = BrowserWorkerResponseEnvelope::success(
        "req-1",
        QueryResponse {
            executed_on: ExecutionTarget::BrowserWasm,
            capabilities: CapabilityReport::default(),
            fallback_reason: None,
            metrics: QueryMetricsSummary {
                bytes_fetched: 128,
                duration_ms: 4,
                files_touched: 1,
                files_skipped: 0,
            },
        },
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
        QueryResponse {
            executed_on: ExecutionTarget::Native,
            capabilities: CapabilityReport::from_pairs([(
                CapabilityKey::TimeTravel,
                CapabilityState::NativeOnly,
            )]),
            fallback_reason: Some(fallback_reason.clone()),
            metrics: QueryMetricsSummary {
                bytes_fetched: 512,
                duration_ms: 12,
                files_touched: 2,
                files_skipped: 3,
            },
        },
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
