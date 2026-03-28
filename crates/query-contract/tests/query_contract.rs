use query_contract::{
    validate_browser_object_url, BrowserHttpFileDescriptor, BrowserHttpSnapshotDescriptor,
    BrowserObjectUrlPolicy, CapabilityKey, CapabilityReport, CapabilityState, ExecutionTarget,
    FallbackReason, PartitionColumnType, QueryError, QueryErrorCode, QueryExecutionOptions,
    QueryMetricsSummary, QueryRequest, QueryResponse, ResolvedFileDescriptor,
    ResolvedSnapshotDescriptor, SnapshotResolutionRequest,
};
use serde_json::json;

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
            duration_ms: 12,
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
                "duration_ms": 12,
                "files_touched": 3,
                "files_skipped": 1
            }
        })
    );
}

#[test]
fn query_request_serializes_single_table_locator_and_execution_options() {
    let mut request = QueryRequest::new(
        "gs://axon-fixtures/sample_table",
        "SELECT id FROM axon_table LIMIT 5",
        ExecutionTarget::Native,
    )
    .with_options(QueryExecutionOptions {
        include_explain: true,
        collect_metrics: false,
    });
    request.snapshot_version = Some(3);

    let json = serde_json::to_value(&request).expect("query request should serialize");

    assert_eq!(
        json,
        json!({
            "table_uri": "gs://axon-fixtures/sample_table",
            "snapshot_version": 3,
            "sql": "SELECT id FROM axon_table LIMIT 5",
            "preferred_target": "native",
            "options": {
                "include_explain": true,
                "collect_metrics": false
            }
        })
    );
}

#[test]
fn query_request_defaults_execution_options_when_omitted() {
    let request: QueryRequest = serde_json::from_value(json!({
        "table_uri": "gs://axon-fixtures/sample_table",
        "sql": "SELECT count(*) FROM axon_table",
        "preferred_target": "native"
    }))
    .expect("query request should deserialize");

    assert_eq!(request.table_uri, "gs://axon-fixtures/sample_table");
    assert_eq!(request.snapshot_version, None);
    assert_eq!(request.options, QueryExecutionOptions::default());
    assert!(!request.options.include_explain);
    assert!(request.options.collect_metrics);
}

#[test]
fn query_request_deserializes_explicit_snapshot_version() {
    let request: QueryRequest = serde_json::from_value(json!({
        "table_uri": "gs://axon-fixtures/sample_table",
        "snapshot_version": 7,
        "sql": "SELECT count(*) FROM axon_table",
        "preferred_target": "native"
    }))
    .expect("query request should deserialize");

    assert_eq!(request.snapshot_version, Some(7));
    assert_eq!(request.options, QueryExecutionOptions::default());
}

#[test]
fn snapshot_resolution_request_serializes_optional_snapshot_version() {
    let request = SnapshotResolutionRequest {
        table_uri: "gs://axon-fixtures/sample_table".to_string(),
        snapshot_version: Some(9),
    };

    let json =
        serde_json::to_value(&request).expect("snapshot resolution request should serialize");

    assert_eq!(
        json,
        json!({
            "table_uri": "gs://axon-fixtures/sample_table",
            "snapshot_version": 9
        })
    );
}

#[test]
fn snapshot_resolution_request_defaults_snapshot_version_when_omitted() {
    let request: SnapshotResolutionRequest = serde_json::from_value(json!({
        "table_uri": "gs://axon-fixtures/sample_table"
    }))
    .expect("snapshot resolution request should deserialize");

    assert_eq!(request.table_uri, "gs://axon-fixtures/sample_table");
    assert_eq!(request.snapshot_version, None);
}

#[test]
fn resolved_snapshot_descriptor_serializes_metadata_only_file_descriptors() {
    let descriptor = ResolvedSnapshotDescriptor {
        table_uri: "gs://axon-fixtures/sample_table".to_string(),
        snapshot_version: 12,
        partition_column_types: std::collections::BTreeMap::from([
            ("category".to_string(), PartitionColumnType::String),
            ("year".to_string(), PartitionColumnType::Int64),
        ]),
        active_files: vec![
            ResolvedFileDescriptor {
                path: "category=A/part-000.parquet".to_string(),
                size_bytes: 128,
                partition_values: std::collections::BTreeMap::from([
                    ("category".to_string(), Some("A".to_string())),
                    ("region".to_string(), None),
                ]),
            },
            ResolvedFileDescriptor {
                path: "category=B/part-001.parquet".to_string(),
                size_bytes: 256,
                partition_values: std::collections::BTreeMap::new(),
            },
        ],
    };

    let json =
        serde_json::to_value(&descriptor).expect("resolved snapshot descriptor should serialize");

    assert_eq!(
        json,
        json!({
            "table_uri": "gs://axon-fixtures/sample_table",
            "snapshot_version": 12,
            "partition_column_types": {
                "category": "string",
                "year": "int64"
            },
            "active_files": [
                {
                    "path": "category=A/part-000.parquet",
                    "size_bytes": 128,
                    "partition_values": {
                        "category": "A",
                        "region": null
                    }
                },
                {
                    "path": "category=B/part-001.parquet",
                    "size_bytes": 256,
                    "partition_values": {}
                }
            ]
        })
    );
}

#[test]
fn browser_http_snapshot_descriptor_serializes_browser_safe_file_urls() {
    let descriptor = BrowserHttpSnapshotDescriptor {
        table_uri: "gs://axon-fixtures/sample_table".to_string(),
        snapshot_version: 12,
        partition_column_types: std::collections::BTreeMap::from([
            ("category".to_string(), PartitionColumnType::String),
            ("year".to_string(), PartitionColumnType::Int64),
        ]),
        active_files: vec![
            BrowserHttpFileDescriptor {
                path: "category=A/part-000.parquet".to_string(),
                url: "https://signed.example.test/category=A/part-000.parquet?X-Goog-Signature=super-secret#fragment".to_string(),
                size_bytes: 128,
                partition_values: std::collections::BTreeMap::from([
                    ("category".to_string(), Some("A".to_string())),
                    ("region".to_string(), None),
                ]),
            },
            BrowserHttpFileDescriptor {
                path: "category=B/part-001.parquet".to_string(),
                url: "https://proxy.example.test/read/category=B/part-001.parquet".to_string(),
                size_bytes: 256,
                partition_values: std::collections::BTreeMap::new(),
            },
        ],
    };

    let json = serde_json::to_value(&descriptor)
        .expect("browser http snapshot descriptor should serialize");

    assert_eq!(
        json,
        json!({
            "table_uri": "gs://axon-fixtures/sample_table",
            "snapshot_version": 12,
            "partition_column_types": {
                "category": "string",
                "year": "int64"
            },
            "active_files": [
                {
                    "path": "category=A/part-000.parquet",
                    "url": "https://signed.example.test/category=A/part-000.parquet?X-Goog-Signature=super-secret#fragment",
                    "size_bytes": 128,
                    "partition_values": {
                        "category": "A",
                        "region": null
                    }
                },
                {
                    "path": "category=B/part-001.parquet",
                    "url": "https://proxy.example.test/read/category=B/part-001.parquet",
                    "size_bytes": 256,
                    "partition_values": {}
                }
            ]
        })
    );
}

#[test]
fn snapshot_descriptors_default_partition_column_types_when_omitted() {
    let resolved: ResolvedSnapshotDescriptor = serde_json::from_value(json!({
        "table_uri": "gs://axon-fixtures/sample_table",
        "snapshot_version": 12,
        "active_files": []
    }))
    .expect("resolved snapshot descriptor should deserialize");
    let browser: BrowserHttpSnapshotDescriptor = serde_json::from_value(json!({
        "table_uri": "gs://axon-fixtures/sample_table",
        "snapshot_version": 12,
        "active_files": []
    }))
    .expect("browser http snapshot descriptor should deserialize");

    assert!(resolved.partition_column_types.is_empty());
    assert!(browser.partition_column_types.is_empty());
}

#[test]
fn browser_object_url_validation_allows_https_for_browser_contracts() {
    let url = validate_browser_object_url(
        "https://signed.example.test/object?sig=secret#fragment",
        ExecutionTarget::Native,
        BrowserObjectUrlPolicy::HttpsOnly,
        "browser object URL",
    )
    .expect("https urls should be accepted");

    assert_eq!(
        url.as_str(),
        "https://signed.example.test/object?sig=secret#fragment"
    );
}

#[test]
fn browser_object_url_validation_rejects_loopback_http_for_https_only_policy() {
    let error = validate_browser_object_url(
        "http://127.0.0.1:8787/object?sig=secret#fragment",
        ExecutionTarget::Native,
        BrowserObjectUrlPolicy::HttpsOnly,
        "browser object URL",
    )
    .expect_err("https-only policy should reject plain HTTP");

    assert_eq!(error.code, QueryErrorCode::SecurityPolicyViolation);
    assert!(error.message.contains("http://127.0.0.1:8787/object"));
    assert!(!error.message.contains("secret"));
    assert!(!error.message.contains("fragment"));
}

#[test]
fn browser_object_url_validation_allows_loopback_http_only_for_host_test_policy() {
    let url = validate_browser_object_url(
        "http://127.0.0.1:8787/object",
        ExecutionTarget::Native,
        BrowserObjectUrlPolicy::HttpsOrLoopbackHttpForHostTests,
        "browser object URL",
    )
    .expect("host-test policy should allow loopback HTTP on native targets");

    assert_eq!(url.as_str(), "http://127.0.0.1:8787/object");
}
