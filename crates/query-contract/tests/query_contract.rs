use query_contract::{
    delta_protocol_feature, delta_protocol_feature_names, validate_browser_object_url,
    validate_delta_location_resolve_request, validate_delta_location_resolve_response,
    BrowserAccessMode, BrowserHttpFileDescriptor, BrowserHttpSnapshotDescriptor,
    BrowserObjectUrlPolicy, CapabilityKey, CapabilityReport, CapabilityState, CredentialProfileRef,
    DeltaLocationRefresh, DeltaLocationResolveRequest, DeltaLocationResolveResponse,
    DeltaLocationResolverError, DeltaLocationResolverErrorCode, DeltaObjectStoreProvider,
    DeltaProtocolFeatureClass, DeltaProtocolFeatureEnablement, DeltaProtocolFeatureKind,
    ExecutionTarget, FallbackReason, PartitionColumnType, QueryError, QueryErrorCode,
    QueryExecutionOptions, QueryMetricsSummary, QueryRequest, QueryResponse,
    ResolvedFileDescriptor, ResolvedSnapshotDescriptor, ResolverActualAccessMode,
    ResolverRequestedAccessMode, SnapshotResolutionRequest,
};
use serde_json::json;

#[test]
fn delta_protocol_feature_catalog_covers_browser_routing_matrix() {
    assert_eq!(
        delta_protocol_feature("appendOnly").map(|feature| feature.class),
        Some(DeltaProtocolFeatureClass::TerminalUnsupported)
    );
    assert_eq!(
        delta_protocol_feature("changeDataFeed").map(|feature| feature.class),
        Some(DeltaProtocolFeatureClass::NativeOnly)
    );
    assert_eq!(
        delta_protocol_feature("v2Checkpoint").map(|feature| feature.class),
        Some(DeltaProtocolFeatureClass::SupportedInBrowser)
    );
    assert_eq!(
        delta_protocol_feature("checkConstraints").map(|feature| feature.enablement),
        Some(DeltaProtocolFeatureEnablement::ConfigurationPrefix(
            "delta.constraints."
        ))
    );
    assert_eq!(
        delta_protocol_feature("catalogManaged").map(|feature| feature.kind),
        Some(DeltaProtocolFeatureKind::ReaderWriter)
    );
}

#[test]
fn delta_protocol_feature_catalog_reserves_unknown_for_unrecognized_features() {
    let names = delta_protocol_feature_names().collect::<Vec<_>>();

    assert!(names.contains(&"catalogManaged"));
    assert!(names.contains(&"catalogOwned-preview"));
    assert!(names.contains(&"clustering"));
    assert!(names.contains(&"materializePartitionColumns"));
    assert!(names.contains(&"variantType"));
    assert!(names.contains(&"variantType-preview"));
    assert!(names.contains(&"variantShredding-preview"));
    assert!(delta_protocol_feature("mysteryFeature").is_none());
}

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
            row_groups_touched: 5,
            row_groups_skipped: 2,
            footer_reads: None,
            rows_emitted: 7,
            snapshot_bootstrap_duration_ms: None,
            access_mode: None,
        },
        explain: None,
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
                "files_skipped": 1,
                "row_groups_touched": 5,
                "row_groups_skipped": 2,
                "rows_emitted": 7
            }
        })
    );
}

#[test]
fn query_response_serializes_browser_telemetry_when_present() {
    let response = QueryResponse {
        executed_on: ExecutionTarget::BrowserWasm,
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
            row_groups_touched: 5,
            row_groups_skipped: 2,
            footer_reads: Some(3),
            rows_emitted: 7,
            snapshot_bootstrap_duration_ms: Some(8),
            access_mode: Some(BrowserAccessMode::BrowserSafeHttp),
        },
        explain: None,
    };

    let json = serde_json::to_value(&response).expect("query response should serialize");

    assert_eq!(
        json,
        serde_json::json!({
            "executed_on": "browser_wasm",
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
                "files_skipped": 1,
                "row_groups_touched": 5,
                "row_groups_skipped": 2,
                "footer_reads": 3,
                "rows_emitted": 7,
                "snapshot_bootstrap_duration_ms": 8,
                "access_mode": "browser_safe_http"
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
fn delta_location_resolve_request_round_trips_provider_profile_and_auto_mode() {
    let request = DeltaLocationResolveRequest {
        provider: DeltaObjectStoreProvider::Gcs,
        table_uri: "gs://axon-fixtures/sample_table".to_string(),
        credential_profile: CredentialProfileRef {
            id: "prod-readonly".to_string(),
            display_name: Some("Production readonly".to_string()),
        },
        requested_access_mode: ResolverRequestedAccessMode::Auto,
        snapshot_version: Some(12),
    };

    let json =
        serde_json::to_value(&request).expect("delta location resolve request should serialize");

    assert_eq!(
        json,
        json!({
            "provider": "gcs",
            "table_uri": "gs://axon-fixtures/sample_table",
            "credential_profile": {
                "id": "prod-readonly",
                "display_name": "Production readonly"
            },
            "requested_access_mode": "auto",
            "snapshot_version": 12
        })
    );

    let decoded: DeltaLocationResolveRequest =
        serde_json::from_value(json).expect("delta location resolve request should deserialize");
    assert_eq!(decoded, request);
    validate_delta_location_resolve_request(&decoded).expect("request should validate");
}

#[test]
fn delta_location_resolve_response_round_trips_access_mode_expiry_and_refresh() {
    let response = DeltaLocationResolveResponse {
        descriptor: BrowserHttpSnapshotDescriptor {
            table_uri: "axon-resolved://session/events/v12".to_string(),
            snapshot_version: 12,
            partition_column_types: std::collections::BTreeMap::new(),
            browser_compatibility: CapabilityReport::default(),
            required_capabilities: CapabilityReport::default(),
            active_files: vec![BrowserHttpFileDescriptor {
                path: "part-000.parquet".to_string(),
                url: "https://signed.example.test/part-000.parquet?X-Goog-Signature=secret"
                    .to_string(),
                size_bytes: 128,
                partition_values: std::collections::BTreeMap::new(),
                stats: None,
            }],
        },
        provider: DeltaObjectStoreProvider::Gcs,
        table_uri: "gs://axon-fixtures/sample_table".to_string(),
        requested_snapshot_version: Some(12),
        resolved_snapshot_version: 12,
        requested_access_mode: Some(ResolverRequestedAccessMode::Auto),
        actual_access_mode: ResolverActualAccessMode::Proxy,
        expires_at_epoch_ms: 4_102_444_800_000,
        correlation_id: Some("corr-123".to_string()),
        warnings: vec!["signed URL mode unavailable; resolver selected proxy".to_string()],
        refresh: Some(DeltaLocationRefresh {
            refresh_url: Some("https://resolver.example.test/refresh/corr-123".to_string()),
            refresh_after_epoch_ms: Some(4_102_444_740_000),
            same_snapshot_required: true,
        }),
    };

    let json =
        serde_json::to_value(&response).expect("delta location resolve response should serialize");

    assert_eq!(json["provider"], "gcs");
    assert_eq!(json["requested_access_mode"], "auto");
    assert_eq!(json["actual_access_mode"], "proxy");
    assert_eq!(json["resolved_snapshot_version"], 12);
    assert_eq!(json["expires_at_epoch_ms"], 4_102_444_800_000_u64);
    assert_eq!(json["refresh"]["same_snapshot_required"], true);

    let decoded: DeltaLocationResolveResponse =
        serde_json::from_value(json).expect("delta location resolve response should deserialize");
    assert_eq!(decoded, response);
    validate_delta_location_resolve_response(&decoded, 4_102_444_700_000)
        .expect("response should validate before expiry");
}

#[test]
fn delta_location_resolve_request_validation_rejects_secret_bearing_inputs() {
    let valid = DeltaLocationResolveRequest {
        provider: DeltaObjectStoreProvider::S3,
        table_uri: "s3://axon-fixtures/sample_table".to_string(),
        credential_profile: CredentialProfileRef {
            id: "prod-readonly".to_string(),
            display_name: None,
        },
        requested_access_mode: ResolverRequestedAccessMode::Auto,
        snapshot_version: None,
    };

    let cases = [
        (
            "s3://user:pass@axon-fixtures/sample_table",
            "prod-readonly",
            DeltaLocationResolverErrorCode::InvalidTableUri,
        ),
        (
            "https://bucket.s3.amazonaws.com/table?X-Amz-Signature=secret",
            "prod-readonly",
            DeltaLocationResolverErrorCode::InvalidTableUri,
        ),
        (
            "s3://axon-fixtures/sample_table?X-Amz-Signature=secret",
            "prod-readonly",
            DeltaLocationResolverErrorCode::InvalidTableUri,
        ),
        (
            "s3://axon-fixtures/sample_table",
            "AKIAIOSFODNN7EXAMPLE",
            DeltaLocationResolverErrorCode::PolicyBlocked,
        ),
        (
            "s3://axon-fixtures/sample_table",
            r#"{"type":"service_account","private_key":"secret"}"#,
            DeltaLocationResolverErrorCode::PolicyBlocked,
        ),
    ];

    for (table_uri, profile_id, expected_code) in cases {
        let mut request = valid.clone();
        request.table_uri = table_uri.to_string();
        request.credential_profile.id = profile_id.to_string();

        let error = validate_delta_location_resolve_request(&request)
            .expect_err("secret-bearing inputs should be rejected");

        assert_eq!(error.code, expected_code);
        assert!(!error.message.contains("secret"));
        assert!(!error.message.contains("AKIAIOSFODNN7EXAMPLE"));
        assert!(!error.message.contains("private_key"));
        assert!(!error.message.contains("X-Amz-Signature=secret"));
    }
}

#[test]
fn delta_location_resolve_request_validation_rejects_invalid_snapshot_versions() {
    let mut request = DeltaLocationResolveRequest {
        provider: DeltaObjectStoreProvider::Gcs,
        table_uri: "gs://axon-fixtures/sample_table".to_string(),
        credential_profile: CredentialProfileRef {
            id: "prod-readonly".to_string(),
            display_name: None,
        },
        requested_access_mode: ResolverRequestedAccessMode::Auto,
        snapshot_version: Some(-1),
    };

    let error = validate_delta_location_resolve_request(&request)
        .expect_err("negative snapshot versions should be rejected");
    assert_eq!(
        error.code,
        DeltaLocationResolverErrorCode::InvalidSnapshotVersion
    );

    request.snapshot_version = Some(0);
    validate_delta_location_resolve_request(&request).expect("version zero should remain valid");
}

#[test]
fn delta_location_resolve_request_validation_accepts_logical_object_store_uris() {
    let cases = [
        (DeltaObjectStoreProvider::S3, "s3://bucket/table"),
        (DeltaObjectStoreProvider::Gcs, "gs://bucket/table"),
        (
            DeltaObjectStoreProvider::AzureBlob,
            "az://account/container/table",
        ),
        (
            DeltaObjectStoreProvider::AzureBlob,
            "abfs://container@account.dfs.core.windows.net/table",
        ),
    ];

    for (provider, table_uri) in cases {
        let request = DeltaLocationResolveRequest {
            provider,
            table_uri: table_uri.to_string(),
            credential_profile: CredentialProfileRef {
                id: "prod-readonly".to_string(),
                display_name: None,
            },
            requested_access_mode: ResolverRequestedAccessMode::Auto,
            snapshot_version: None,
        };

        validate_delta_location_resolve_request(&request)
            .unwrap_or_else(|error| panic!("{table_uri} should validate: {error:?}"));
    }
}

#[test]
fn delta_location_resolve_response_validation_requires_same_snapshot_refresh() {
    let mut response = DeltaLocationResolveResponse {
        descriptor: BrowserHttpSnapshotDescriptor {
            table_uri: "axon-resolved://session/events/v12".to_string(),
            snapshot_version: 12,
            partition_column_types: std::collections::BTreeMap::new(),
            browser_compatibility: CapabilityReport::default(),
            required_capabilities: CapabilityReport::default(),
            active_files: Vec::new(),
        },
        provider: DeltaObjectStoreProvider::Gcs,
        table_uri: "gs://axon-fixtures/sample_table".to_string(),
        requested_snapshot_version: Some(12),
        resolved_snapshot_version: 12,
        requested_access_mode: Some(ResolverRequestedAccessMode::Auto),
        actual_access_mode: ResolverActualAccessMode::SignedUrl,
        expires_at_epoch_ms: 4_102_444_800_000,
        correlation_id: None,
        warnings: Vec::new(),
        refresh: Some(DeltaLocationRefresh {
            refresh_url: None,
            refresh_after_epoch_ms: Some(4_102_444_740_000),
            same_snapshot_required: false,
        }),
    };

    let error = validate_delta_location_resolve_response(&response, 4_102_444_700_000)
        .expect_err("refresh must require the same resolved snapshot");
    assert_eq!(error.code, DeltaLocationResolverErrorCode::PolicyBlocked);

    response.refresh.as_mut().unwrap().same_snapshot_required = true;
    validate_delta_location_resolve_response(&response, 4_102_444_700_000)
        .expect("same-snapshot refresh should validate");
}

#[test]
fn delta_location_resolve_response_validation_rejects_negative_snapshot_versions() {
    let mut response = DeltaLocationResolveResponse {
        descriptor: BrowserHttpSnapshotDescriptor {
            table_uri: "axon-resolved://session/events/v12".to_string(),
            snapshot_version: -1,
            partition_column_types: std::collections::BTreeMap::new(),
            browser_compatibility: CapabilityReport::default(),
            required_capabilities: CapabilityReport::default(),
            active_files: Vec::new(),
        },
        provider: DeltaObjectStoreProvider::Gcs,
        table_uri: "gs://axon-fixtures/sample_table".to_string(),
        requested_snapshot_version: None,
        resolved_snapshot_version: -1,
        requested_access_mode: Some(ResolverRequestedAccessMode::Auto),
        actual_access_mode: ResolverActualAccessMode::SignedUrl,
        expires_at_epoch_ms: 4_102_444_800_000,
        correlation_id: None,
        warnings: Vec::new(),
        refresh: None,
    };

    let error = validate_delta_location_resolve_response(&response, 4_102_444_700_000)
        .expect_err("negative resolved snapshot versions should be rejected");
    assert_eq!(
        error.code,
        DeltaLocationResolverErrorCode::InvalidSnapshotVersion
    );

    response.descriptor.snapshot_version = 12;
    response.resolved_snapshot_version = 12;
    validate_delta_location_resolve_response(&response, 4_102_444_700_000)
        .expect("non-negative snapshot versions should validate");
}

#[test]
fn delta_location_resolver_errors_redact_bearer_token_values() {
    let error = DeltaLocationResolverError::new(
        DeltaLocationResolverErrorCode::ResolverUnavailable,
        "resolver failed with Authorization: Bearer abc123 and Authorization: Basic basic-secret and https://signed.example.test/file.parquet?X-Amz-Signature=secret",
    );

    assert!(!error.message.contains("abc123"));
    assert!(!error.message.contains("Bearer abc123"));
    assert!(!error.message.contains("basic-secret"));
    assert!(!error.message.contains("X-Amz-Signature=secret"));
    assert!(error.message.contains("[REDACTED]"));
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
        browser_compatibility: CapabilityReport::from_pairs([
            (CapabilityKey::DeletionVectors, CapabilityState::NativeOnly),
            (CapabilityKey::RangeReads, CapabilityState::Supported),
        ]),
        required_capabilities: CapabilityReport::from_pairs([
            (CapabilityKey::DeletionVectors, CapabilityState::NativeOnly),
            (CapabilityKey::RangeReads, CapabilityState::Supported),
        ]),
        active_files: vec![
            ResolvedFileDescriptor {
                path: "category=A/part-000.parquet".to_string(),
                size_bytes: 128,
                partition_values: std::collections::BTreeMap::from([
                    ("category".to_string(), Some("A".to_string())),
                    ("region".to_string(), None),
                ]),
                stats: None,
            },
            ResolvedFileDescriptor {
                path: "category=B/part-001.parquet".to_string(),
                size_bytes: 256,
                partition_values: std::collections::BTreeMap::new(),
                stats: None,
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
            "browser_compatibility": {
                "capabilities": {
                    "deletion_vectors": "native_only",
                    "range_reads": "supported"
                }
            },
            "required_capabilities": {
                "capabilities": {
                    "deletion_vectors": "native_only",
                    "range_reads": "supported"
                }
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
        browser_compatibility: CapabilityReport::from_pairs([
            (CapabilityKey::DeletionVectors, CapabilityState::NativeOnly),
            (CapabilityKey::RangeReads, CapabilityState::Supported),
        ]),
        required_capabilities: CapabilityReport::from_pairs([
            (CapabilityKey::DeletionVectors, CapabilityState::NativeOnly),
            (CapabilityKey::RangeReads, CapabilityState::Supported),
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
                stats: None,
            },
            BrowserHttpFileDescriptor {
                path: "category=B/part-001.parquet".to_string(),
                url: "https://proxy.example.test/read/category=B/part-001.parquet".to_string(),
                size_bytes: 256,
                partition_values: std::collections::BTreeMap::new(),
                stats: None,
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
            "browser_compatibility": {
                "capabilities": {
                    "deletion_vectors": "native_only",
                    "range_reads": "supported"
                }
            },
            "required_capabilities": {
                "capabilities": {
                    "deletion_vectors": "native_only",
                    "range_reads": "supported"
                }
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
    assert_eq!(resolved.browser_compatibility, CapabilityReport::default());
    assert_eq!(resolved.required_capabilities, CapabilityReport::default());
    assert!(browser.partition_column_types.is_empty());
    assert_eq!(browser.browser_compatibility, CapabilityReport::default());
    assert_eq!(browser.required_capabilities, CapabilityReport::default());
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
