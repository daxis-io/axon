use query_contract::{
    CapabilityKey, CapabilityReport, CapabilityState, ExecutionTarget, FallbackReason, QueryError,
    QueryErrorCode, QueryMetricsSummary, QueryResponse,
};

#[test]
fn capability_report_serializes_with_snake_case_keys_and_states() {
    let report = CapabilityReport::from_pairs([
        (CapabilityKey::DeletionVectors, CapabilityState::NativeOnly),
        (CapabilityKey::RangeReads, CapabilityState::Supported),
    ]);

    let json = serde_json::to_value(&report).expect("capability report should serialize");

    assert_eq!(
        json,
        serde_json::json!({
            "capabilities": {
                "deletion_vectors": "native_only",
                "range_reads": "supported"
            }
        })
    );
}

#[test]
fn query_error_reports_when_fallback_is_required() {
    let error = QueryError::new(
        QueryErrorCode::UnsupportedFeature,
        "deletion vectors require the native runtime",
        ExecutionTarget::BrowserWasm,
    )
    .with_fallback_reason(FallbackReason::CapabilityGate {
        capability: CapabilityKey::DeletionVectors,
        required_state: CapabilityState::NativeOnly,
    });

    assert!(error.requires_fallback());
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
    assert_eq!(error.code, QueryErrorCode::UnsupportedFeature);
}

#[test]
fn query_error_serializes_without_absent_fallback_reason() {
    let error = QueryError::new(
        QueryErrorCode::InvalidRequest,
        "sql must not be empty",
        ExecutionTarget::BrowserWasm,
    );

    let json = serde_json::to_value(&error).expect("query error should serialize");

    assert_eq!(
        json,
        serde_json::json!({
            "code": "invalid_request",
            "message": "sql must not be empty",
            "target": "browser_wasm"
        })
    );
}

#[test]
fn query_error_serializes_capability_gate_fallback_reason() {
    let error = QueryError::new(
        QueryErrorCode::FallbackRequired,
        "browser runtime cannot execute deletion vectors",
        ExecutionTarget::BrowserWasm,
    )
    .with_fallback_reason(FallbackReason::CapabilityGate {
        capability: CapabilityKey::DeletionVectors,
        required_state: CapabilityState::NativeOnly,
    });

    let json = serde_json::to_value(&error).expect("query error should serialize");

    assert_eq!(
        json,
        serde_json::json!({
            "code": "fallback_required",
            "message": "browser runtime cannot execute deletion vectors",
            "target": "browser_wasm",
            "fallback_reason": {
                "capability_gate": {
                    "capability": "deletion_vectors",
                    "required_state": "native_only"
                }
            }
        })
    );
}

#[test]
fn query_response_serializes_without_absent_fallback_reason() {
    let response = QueryResponse {
        executed_on: ExecutionTarget::Native,
        capabilities: CapabilityReport::from_pairs([
            (CapabilityKey::DeletionVectors, CapabilityState::NativeOnly),
            (CapabilityKey::RangeReads, CapabilityState::Supported),
        ]),
        fallback_reason: None,
        metrics: QueryMetricsSummary {
            bytes_fetched: 4096,
            files_touched: 3,
            files_skipped: 1,
        },
    };

    let json = serde_json::to_value(&response).expect("query response should serialize");

    assert_eq!(
        json,
        serde_json::json!({
            "executed_on": "native",
            "capabilities": {
                "capabilities": {
                    "deletion_vectors": "native_only",
                    "range_reads": "supported"
                }
            },
            "metrics": {
                "bytes_fetched": 4096,
                "files_touched": 3,
                "files_skipped": 1
            }
        })
    );
}
