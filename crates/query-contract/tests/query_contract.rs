use std::collections::BTreeMap;

use query_contract::{
    delta_protocol_feature, delta_protocol_feature_names, validate_browser_object_url,
    validate_daxis_approved_axon_read_descriptor, validate_daxis_result_envelope,
    validate_delta_location_resolve_exchange, validate_delta_location_resolve_request,
    validate_delta_location_resolve_response, BrokeredDeltaAccessMode, BrokeredDeltaReadPlan,
    BrokeredObjectAccess, BrokeredPolicyAuthority, BrowserAccessMode, BrowserHttpFileDescriptor,
    BrowserHttpParquetDatasetDescriptor, BrowserHttpSnapshotDescriptor, BrowserObjectUrlPolicy,
    CapabilityKey, CapabilityReport, CapabilityState, CredentialProfileRef, DeltaLocationRefresh,
    DeltaLocationResolveRequest, DeltaLocationResolveResponse, DeltaLocationResolverError,
    DeltaLocationResolverErrorCode, DeltaObjectStoreProvider, DeltaProtocolFeatureClass,
    DeltaProtocolFeatureEnablement, DeltaProtocolFeatureKind, DirectExternalEngineReadSupport,
    ExecutionTarget, FallbackReason, ObjectGrantBatchSignRequest, ObjectGrantHeadRequest,
    ObjectGrantListRequest, PartitionColumnType, PolicyAuthorityKind, QueryError, QueryErrorCode,
    QueryExecutionOptions, QueryMetricsSummary, QueryRequest, QueryResponse, QueryResultPage,
    ReadAccessPlan, ReadAccessPlanReason, ResolvedFileDescriptor, ResolvedSnapshotDescriptor,
    ResolverActualAccessMode, ResolverRequestedAccessMode, SnapshotResolutionRequest,
    SqlFallbackRequiredPlan,
};
use schemars::schema_for;
use serde_json::{json, Value};

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
            bootstrap_footer_range_reads: None,
            scan_footer_range_reads: None,
            scan_data_range_reads: None,
            duplicate_range_reads: None,
            identity_present_range_reads: None,
            identity_missing_range_reads: None,
            descriptor_resolution_count: None,
            delta_log_manifest_list_count: None,
            delta_log_manifest_list_duration_ms: None,
            snapshot_resolve_count: None,
            snapshot_resolve_duration_ms: None,
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
fn browser_http_parquet_dataset_descriptor_serializes_without_delta_snapshot_fields() {
    let descriptor = BrowserHttpParquetDatasetDescriptor {
        table_uri: "https://example.invalid/datasets/events".to_string(),
        partition_column_types: BTreeMap::from([(
            "event_date".to_string(),
            PartitionColumnType::String,
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
    };

    let json = serde_json::to_value(&descriptor).expect("Parquet descriptor should serialize");

    assert_eq!(
        json,
        serde_json::json!({
            "table_uri": "https://example.invalid/datasets/events",
            "partition_column_types": {
                "event_date": "string"
            },
            "required_capabilities": {
                "capabilities": {
                    "range_reads": "supported"
                }
            },
            "files": [
                {
                    "path": "event_date=2026-06-01/part-000.parquet",
                    "url": "https://storage.example.invalid/events/part-000.parquet",
                    "size_bytes": 1234,
                    "partition_values": {
                        "event_date": "2026-06-01"
                    }
                }
            ]
        })
    );
    assert!(json.get("snapshot_version").is_none());
    assert!(json.get("active_files").is_none());

    let round_tripped: BrowserHttpParquetDatasetDescriptor =
        serde_json::from_value(json).expect("Parquet descriptor should deserialize");
    assert_eq!(round_tripped, descriptor);
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
            bootstrap_footer_range_reads: Some(6),
            scan_footer_range_reads: Some(2),
            scan_data_range_reads: Some(4),
            duplicate_range_reads: Some(2),
            identity_present_range_reads: Some(5),
            identity_missing_range_reads: Some(7),
            descriptor_resolution_count: Some(1),
            delta_log_manifest_list_count: Some(1),
            delta_log_manifest_list_duration_ms: Some(4),
            snapshot_resolve_count: Some(1),
            snapshot_resolve_duration_ms: Some(8),
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
                "bootstrap_footer_range_reads": 6,
                "scan_footer_range_reads": 2,
                "scan_data_range_reads": 4,
                "duplicate_range_reads": 2,
                "identity_present_range_reads": 5,
                "identity_missing_range_reads": 7,
                "descriptor_resolution_count": 1,
                "delta_log_manifest_list_count": 1,
                "delta_log_manifest_list_duration_ms": 4,
                "snapshot_resolve_count": 1,
                "snapshot_resolve_duration_ms": 8,
                "rows_emitted": 7,
                "snapshot_bootstrap_duration_ms": 8,
                "access_mode": "browser_safe_http"
            }
        })
    );
}

#[test]
fn read_access_plan_serializes_brokered_delta_without_cloud_credentials() {
    let plan = ReadAccessPlan::BrokeredDelta(BrokeredDeltaReadPlan {
        table_id: "tbl-123".to_string(),
        full_name: "main.sales.orders".to_string(),
        table_root: "s3://prod-bucket/tables/orders".to_string(),
        grant_id: "grant-456".to_string(),
        expires_at_epoch_ms: 1_800_000_000_000,
        delta_access_mode: BrokeredDeltaAccessMode::DeltaLog,
        policy_authority: BrokeredPolicyAuthority {
            authority: PolicyAuthorityKind::UnityCatalog,
            direct_external_engine_read: DirectExternalEngineReadSupport::Confirmed,
        },
        object_access: BrokeredObjectAccess {
            list: true,
            head: true,
            get: false,
            range_get: true,
            batch_sign: true,
            proxy_range: true,
        },
    });

    let json = serde_json::to_value(&plan).expect("read access plan should serialize");

    assert_eq!(
        json,
        json!({
            "plan_type": "brokered_delta",
            "tableId": "tbl-123",
            "fullName": "main.sales.orders",
            "tableRoot": "s3://prod-bucket/tables/orders",
            "grantId": "grant-456",
            "expiresAtEpochMs": 1_800_000_000_000_u64,
            "deltaAccessMode": "delta_log",
            "policyAuthority": {
                "authority": "unity_catalog",
                "directExternalEngineRead": "confirmed"
            },
            "objectAccess": {
                "list": true,
                "head": true,
                "get": false,
                "rangeGet": true,
                "batchSign": true,
                "proxyRange": true
            }
        })
    );

    let rendered = serde_json::to_string(&json).expect("json should render");
    for forbidden in [
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        "azure_sas",
        "gcp_oauth_token",
        "gcp_hmac_secret",
        "databricks_bearer_token",
        "refresh_token",
        "pat",
    ] {
        assert!(
            !rendered.contains(forbidden),
            "production browser contracts must not expose {forbidden}"
        );
    }
}

#[test]
fn read_access_plan_deserializes_all_full_mvp_variants() {
    let delta_sharing: ReadAccessPlan = serde_json::from_value(json!({
        "plan_type": "delta_sharing",
        "tableId": "tbl-share",
        "fullName": "main.analytics.events",
        "sharingEndpoint": "https://sharing.example.test/delta-sharing",
        "expiresAtEpochMs": 1_800_000_000_000_u64,
        "files": [
            {
                "path": "part-000.parquet",
                "url": "https://signed.example.test/part-000.parquet?sig=redacted-at-log-boundary",
                "size_bytes": 512,
                "partition_values": {}
            }
        ]
    }))
    .expect("delta sharing plan should deserialize");

    let sql_fallback: ReadAccessPlan = serde_json::from_value(json!({
        "plan_type": "sql_fallback_required",
        "tableId": "tbl-governed",
        "fullName": "main.secure.payments",
        "reason": "row_filter",
        "message": "governed table requires service-side SQL execution",
        "statementEndpoint": "https://dbc.example.com/api/2.0/sql/statements",
        "warehouseRequired": true
    }))
    .expect("sql fallback plan should deserialize");

    let blocked: ReadAccessPlan = serde_json::from_value(json!({
        "plan_type": "blocked",
        "tableId": "tbl-blocked",
        "fullName": "main.secure.audit",
        "reason": "unknown_policy_state",
        "message": "direct browser reads are blocked by policy"
    }))
    .expect("blocked plan should deserialize");

    assert!(matches!(delta_sharing, ReadAccessPlan::DeltaSharing(_)));
    assert!(matches!(
        sql_fallback,
        ReadAccessPlan::SqlFallbackRequired(_)
    ));
    assert!(matches!(blocked, ReadAccessPlan::Blocked(_)));
}

#[test]
fn read_access_plan_rejects_sql_fallback_when_warehouse_is_not_required() {
    let err = serde_json::from_value::<ReadAccessPlan>(json!({
        "plan_type": "sql_fallback_required",
        "tableId": "tbl-governed",
        "fullName": "main.secure.payments",
        "reason": "row_filter",
        "message": "governed table requires service-side SQL execution",
        "statementEndpoint": "https://dbc.example.com/api/2.0/sql/statements",
        "warehouseRequired": false
    }))
    .expect_err("sql fallback plans must require a server-side warehouse");

    assert!(
        err.to_string().contains("warehouseRequired must be true"),
        "unexpected error: {err}"
    );
}

#[test]
fn read_access_plan_refuses_to_serialize_sql_fallback_without_required_warehouse() {
    let plan = ReadAccessPlan::SqlFallbackRequired(SqlFallbackRequiredPlan {
        table_id: "tbl-governed".into(),
        full_name: "main.secure.payments".into(),
        reason: ReadAccessPlanReason::RowFilter,
        message: "governed table requires service-side SQL execution".into(),
        statement_endpoint: "https://dbc.example.com/api/2.0/sql/statements".into(),
        warehouse_required: false,
    });

    let err = serde_json::to_value(plan)
        .expect_err("sql fallback plans must not serialize warehouseRequired=false");

    assert!(
        err.to_string().contains("warehouseRequired must be true"),
        "unexpected error: {err}"
    );
}

#[test]
fn read_access_plan_denies_unknown_and_raw_credential_fields() {
    let with_unknown = json!({
        "plan_type": "brokered_delta",
        "tableId": "tbl-123",
        "fullName": "main.sales.orders",
        "tableRoot": "s3://prod-bucket/tables/orders",
        "grantId": "grant-456",
        "expiresAtEpochMs": 1_800_000_000_000_u64,
        "deltaAccessMode": "delta_log",
        "policyAuthority": {
            "authority": "unity_catalog",
            "directExternalEngineRead": "confirmed"
        },
        "objectAccess": {
            "list": true,
            "head": true,
            "get": false,
            "rangeGet": true,
            "batchSign": true,
            "proxyRange": true
        },
        "aws_sts": {
            "access_key_id": "AKIA...",
            "secret_access_key": "secret",
            "session_token": "session"
        }
    });

    let error = serde_json::from_value::<ReadAccessPlan>(with_unknown)
        .expect_err("raw cloud credentials must not be accepted by production browser contracts");
    assert!(
        error.to_string().contains("unknown field"),
        "deny_unknown_fields should reject credential-shaped extensions: {error}"
    );
}

#[test]
fn delta_sharing_plan_denies_unknown_file_fields() {
    let error = serde_json::from_value::<ReadAccessPlan>(json!({
        "plan_type": "delta_sharing",
        "tableId": "tbl-share",
        "fullName": "main.analytics.events",
        "sharingEndpoint": "https://sharing.example.test/delta-sharing",
        "expiresAtEpochMs": 1_800_000_000_000_u64,
        "files": [
            {
                "path": "part-000.parquet",
                "url": "https://signed.example.test/part-000.parquet?sig=redacted-at-log-boundary",
                "size_bytes": 512,
                "partition_values": {},
                "databricks_bearer_token": "dapi..."
            }
        ]
    }))
    .expect_err("Delta Sharing file descriptors should reject credential-shaped extensions");
    assert!(
        error.to_string().contains("unknown field"),
        "nested file descriptors should be closed: {error}"
    );
}

#[test]
fn object_grant_route_payloads_deny_unknown_fields() {
    let list_error = serde_json::from_value::<ObjectGrantListRequest>(json!({
        "prefix": "_delta_log/",
        "aws_secret_access_key": "secret"
    }))
    .expect_err("list requests should reject raw credentials");
    assert!(list_error.to_string().contains("unknown field"));

    let head_error = serde_json::from_value::<ObjectGrantHeadRequest>(json!({
        "path": "part-000.parquet",
        "databricks_bearer_token": "dapi..."
    }))
    .expect_err("head requests should reject bearer tokens");
    assert!(head_error.to_string().contains("unknown field"));

    let sign_error = serde_json::from_value::<ObjectGrantBatchSignRequest>(json!({
        "paths": ["part-000.parquet"],
        "refresh_token": "refresh"
    }))
    .expect_err("batch sign requests should reject refresh tokens");
    assert!(sign_error.to_string().contains("unknown field"));
}

#[test]
fn read_access_plan_reason_taxonomy_is_explicit_and_closed() {
    for reason in [
        "row_filter",
        "column_mask",
        "view",
        "unknown_policy_state",
        "no_direct_external_engine_read_support",
        "unsupported_table_type",
        "grant_expired",
        "storage_cors_blocked",
        "broker_unavailable",
    ] {
        let fallback: ReadAccessPlan = serde_json::from_value(json!({
            "plan_type": "sql_fallback_required",
            "tableId": "tbl-governed",
            "fullName": "main.secure.payments",
            "reason": reason,
            "message": "governed table requires service-side SQL execution",
            "statementEndpoint": "https://dbc.example.com/api/2.0/sql/statements",
            "warehouseRequired": true
        }))
        .expect("required fallback reason should deserialize");
        assert!(matches!(fallback, ReadAccessPlan::SqlFallbackRequired(_)));
    }

    let unknown = serde_json::from_value::<ReadAccessPlan>(json!({
        "plan_type": "blocked",
        "tableId": "tbl-blocked",
        "fullName": "main.secure.audit",
        "reason": "security_policy",
        "message": "old generic query fallback reason should not be accepted here"
    }))
    .expect_err("read access reasons should stay distinct from generic query fallbacks");
    assert!(unknown.to_string().contains("unknown variant"));

    assert_eq!(
        serde_json::to_value(ReadAccessPlanReason::NoDirectExternalEngineReadSupport)
            .expect("reason should serialize"),
        json!("no_direct_external_engine_read_support")
    );
}

#[test]
fn read_access_plan_json_schema_covers_browser_facing_variants() {
    let schema = schema_for!(ReadAccessPlan);
    let rendered =
        serde_json::to_string(&schema).expect("read access plan schema should serialize");

    for expected in [
        "brokered_delta",
        "delta_sharing",
        "sql_fallback_required",
        "blocked",
        "tableId",
        "fullName",
        "tableRoot",
        "grantId",
        "statementEndpoint",
        "warehouseRequired",
        "deltaAccessMode",
        "policyAuthority",
        "objectAccess",
        "rangeGet",
        "batchSign",
        "proxyRange",
        "BrokeredObjectAccess",
    ] {
        assert!(
            rendered.contains(expected),
            "schema should include {expected}"
        );
    }

    let schema_value = serde_json::to_value(&schema).expect("schema should convert to JSON");
    assert!(
        schema_has_warehouse_required_const_true(&schema_value),
        "schema should require warehouseRequired=true"
    );
}

fn schema_has_warehouse_required_const_true(value: &Value) -> bool {
    match value {
        Value::Object(object) => {
            object
                .get("warehouseRequired")
                .and_then(Value::as_object)
                .and_then(|warehouse_required| warehouse_required.get("const"))
                == Some(&Value::Bool(true))
                || object
                    .values()
                    .any(schema_has_warehouse_required_const_true)
        }
        Value::Array(values) => values.iter().any(schema_has_warehouse_required_const_true),
        _ => false,
    }
}

#[test]
fn read_access_plan_schema_fixture_is_checked_in_for_ts_consumers() {
    let fixture = include_str!("../schemas/read-access-plan.schema.json");
    let schema: serde_json::Value = serde_json::from_str(fixture)
        .expect("read access plan schema fixture should be valid JSON");
    let rendered = serde_json::to_string(&schema).expect("schema should render");

    for expected in [
        "ReadAccessPlan",
        "brokered_delta",
        "delta_sharing",
        "sql_fallback_required",
        "blocked",
        "tableId",
        "fullName",
        "statementEndpoint",
        "warehouseRequired",
        "objectAccess",
        "additionalProperties",
    ] {
        assert!(
            rendered.contains(expected),
            "schema fixture should include {expected}"
        );
    }
}

#[test]
fn read_access_plan_schema_fixture_restricts_partition_values_to_strings_or_null() {
    let fixture = include_str!("../schemas/read-access-plan.schema.json");
    let schema: serde_json::Value = serde_json::from_str(fixture)
        .expect("read access plan schema fixture should be valid JSON");

    assert_eq!(
        schema["$defs"]["BrowserHttpFileDescriptor"]["properties"]["partition_values"]
            ["additionalProperties"],
        json!({
            "type": ["string", "null"]
        })
    );
}

#[test]
fn read_access_plan_schema_fixture_declares_each_closed_variant() {
    let fixture = include_str!("../schemas/read-access-plan.schema.json");
    let schema: serde_json::Value = serde_json::from_str(fixture)
        .expect("read access plan schema fixture should be valid JSON");
    let one_of = schema
        .get("oneOf")
        .and_then(serde_json::Value::as_array)
        .expect("read access plan schema fixture should declare oneOf variants");
    let refs = one_of
        .iter()
        .map(|entry| {
            entry
                .get("$ref")
                .and_then(serde_json::Value::as_str)
                .expect("oneOf entries should be schema refs")
        })
        .collect::<Vec<_>>();

    assert_eq!(
        refs,
        vec![
            "#/$defs/BrokeredDeltaReadAccessPlan",
            "#/$defs/DeltaSharingReadAccessPlan",
            "#/$defs/SqlFallbackRequiredReadAccessPlan",
            "#/$defs/BlockedReadAccessPlan",
        ]
    );

    let defs = schema
        .get("$defs")
        .and_then(serde_json::Value::as_object)
        .expect("read access plan schema fixture should declare definitions");
    for variant in refs {
        let name = variant
            .strip_prefix("#/$defs/")
            .expect("variant ref should point into $defs");
        assert_eq!(
            defs.get(name)
                .and_then(|definition| definition.get("additionalProperties"))
                .and_then(serde_json::Value::as_bool),
            Some(false),
            "{name} should reject unknown fields"
        );
    }
}

#[test]
fn brokered_delta_plan_adapts_resolved_snapshot_to_browser_http_descriptor() {
    let plan = brokered_delta_plan();
    let resolved = ResolvedSnapshotDescriptor {
        table_uri: "s3://prod-bucket/tables/orders".to_string(),
        snapshot_version: 7,
        partition_column_types: Default::default(),
        browser_compatibility: signed_url_capabilities(),
        required_capabilities: signed_url_capabilities(),
        active_files: vec![ResolvedFileDescriptor {
            path: "part-000.parquet".to_string(),
            size_bytes: 512,
            partition_values: Default::default(),
            stats: Some("{\"numRecords\":2}".to_string()),
        }],
    };
    let signed_urls = BTreeMap::from([(
        "part-000.parquet".to_string(),
        "https://signed.example.test/part-000.parquet?sig=redacted".to_string(),
    )]);

    let descriptor = plan
        .to_browser_http_snapshot_descriptor(resolved, &signed_urls)
        .expect("brokered plan should adapt into the existing descriptor path");

    assert_eq!(descriptor.table_uri, "s3://prod-bucket/tables/orders");
    assert_eq!(descriptor.snapshot_version, 7);
    assert_eq!(descriptor.active_files[0].path, "part-000.parquet");
    assert_eq!(
        descriptor.active_files[0].url,
        "https://signed.example.test/part-000.parquet?sig=redacted"
    );
    assert_eq!(
        descriptor.active_files[0].stats.as_deref(),
        Some("{\"numRecords\":2}")
    );
}

#[test]
fn brokered_delta_adapter_rejects_unconfirmed_policy_or_url_coverage_drift() {
    let mut unconfirmed = brokered_delta_plan();
    unconfirmed.policy_authority.direct_external_engine_read =
        DirectExternalEngineReadSupport::NotConfirmed;
    let error = unconfirmed
        .to_browser_http_snapshot_descriptor(empty_resolved_snapshot(), &BTreeMap::new())
        .expect_err("direct reads require confirmed external-engine support");
    assert_eq!(error.code, QueryErrorCode::FallbackRequired);

    let plan = brokered_delta_plan();
    let missing_url_error = plan
        .to_browser_http_snapshot_descriptor(one_file_resolved_snapshot(), &BTreeMap::new())
        .expect_err("signed URL coverage must match resolved files");
    assert_eq!(missing_url_error.code, QueryErrorCode::InvalidRequest);
    assert!(missing_url_error.message.contains("missing URLs"));

    let mut without_batch_sign = brokered_delta_plan();
    without_batch_sign.object_access.batch_sign = false;
    let missing_capability = without_batch_sign
        .to_browser_http_snapshot_descriptor(empty_resolved_snapshot(), &BTreeMap::new())
        .expect_err("descriptor adapter needs batch signed object URLs");
    assert_eq!(missing_capability.code, QueryErrorCode::FallbackRequired);
}

fn brokered_delta_plan() -> BrokeredDeltaReadPlan {
    BrokeredDeltaReadPlan {
        table_id: "tbl-123".to_string(),
        full_name: "main.sales.orders".to_string(),
        table_root: "s3://prod-bucket/tables/orders".to_string(),
        grant_id: "grant-456".to_string(),
        expires_at_epoch_ms: 1_800_000_000_000,
        delta_access_mode: BrokeredDeltaAccessMode::DeltaLog,
        policy_authority: BrokeredPolicyAuthority {
            authority: PolicyAuthorityKind::UnityCatalog,
            direct_external_engine_read: DirectExternalEngineReadSupport::Confirmed,
        },
        object_access: BrokeredObjectAccess {
            list: true,
            head: true,
            get: true,
            range_get: true,
            batch_sign: true,
            proxy_range: true,
        },
    }
}

fn signed_url_capabilities() -> CapabilityReport {
    CapabilityReport::from_pairs([
        (CapabilityKey::RangeReads, CapabilityState::Supported),
        (CapabilityKey::SignedUrlAccess, CapabilityState::Supported),
    ])
}

fn empty_resolved_snapshot() -> ResolvedSnapshotDescriptor {
    ResolvedSnapshotDescriptor {
        table_uri: "s3://prod-bucket/tables/orders".to_string(),
        snapshot_version: 7,
        partition_column_types: BTreeMap::new(),
        browser_compatibility: signed_url_capabilities(),
        required_capabilities: signed_url_capabilities(),
        active_files: Vec::new(),
    }
}

fn one_file_resolved_snapshot() -> ResolvedSnapshotDescriptor {
    let mut snapshot = empty_resolved_snapshot();
    snapshot.active_files.push(ResolvedFileDescriptor {
        path: "part-000.parquet".to_string(),
        size_bytes: 512,
        partition_values: BTreeMap::new(),
        stats: None,
    });
    snapshot
}

#[test]
fn object_grant_openapi_fixture_documents_browser_routes() {
    let fixture = include_str!("../schemas/object-grants.openapi.json");
    let openapi: serde_json::Value =
        serde_json::from_str(fixture).expect("object grant OpenAPI fixture should be valid JSON");

    let paths = openapi
        .get("paths")
        .and_then(serde_json::Value::as_object)
        .expect("OpenAPI fixture should contain paths");

    for route in [
        "/object-grants/{grantId}/list",
        "/object-grants/{grantId}/head",
        "/object-grants/{grantId}/batch-sign",
        "/object-grants/{grantId}/range",
    ] {
        assert!(paths.contains_key(route), "missing OpenAPI route {route}");
    }

    let rendered = fixture.to_ascii_lowercase();
    for forbidden in [
        "aws_secret_access_key",
        "aws_session_token",
        "azure_sas",
        "gcp_oauth",
        "gcp_hmac",
        "databricks_bearer",
        "refresh_token",
        "personal_access_token",
    ] {
        assert!(
            !rendered.contains(forbidden),
            "object grant fixture must not document raw credential field {forbidden}"
        );
    }
}

#[test]
fn object_grant_openapi_fixture_documents_audit_event_schema() {
    let fixture = include_str!("../schemas/object-grants.openapi.json");
    let openapi: serde_json::Value =
        serde_json::from_str(fixture).expect("object grant OpenAPI fixture should be valid JSON");

    let schemas = openapi["components"]["schemas"]
        .as_object()
        .expect("OpenAPI fixture should contain component schemas");
    let audit_event = schemas
        .get("ObjectGrantAuditEvent")
        .expect("OpenAPI fixture should document object-grant audit events");

    assert_eq!(audit_event["additionalProperties"], json!(false));
    assert_eq!(
        audit_event["required"],
        json!([
            "eventId",
            "eventType",
            "occurredAtEpochMs",
            "tenantId",
            "workspaceId",
            "userSubject",
            "tableId",
            "fullName",
            "grantId",
            "queryId",
            "requestId",
            "correlationId",
            "action",
            "objectPath",
            "outcome"
        ])
    );
    assert_eq!(
        audit_event["properties"]["eventType"]["const"],
        json!("object_grant_access")
    );
    assert_eq!(
        audit_event["properties"]["action"]["enum"],
        json!(["list", "head", "batch_sign", "range"])
    );
    assert_eq!(
        audit_event["properties"]["outcome"]["enum"],
        json!(["allowed", "denied"])
    );
    assert_eq!(
        audit_event["properties"]["range"]["$ref"],
        json!("#/components/schemas/ObjectGrantAuditRange")
    );
    assert_eq!(
        schemas["ObjectGrantAuditRange"]["additionalProperties"],
        json!(false)
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
        result_page: None,
        runtime_limits: None,
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
fn query_request_serializes_result_page_execution_options() {
    let request = QueryRequest::new(
        "gs://axon-fixtures/sample_table",
        "SELECT * FROM axon_table",
        ExecutionTarget::BrowserWasm,
    )
    .with_options(QueryExecutionOptions {
        include_explain: true,
        collect_metrics: true,
        result_page: Some(QueryResultPage {
            limit: 501,
            offset: 500,
        }),
        runtime_limits: None,
    });

    let json = serde_json::to_value(&request).expect("query request should serialize");

    assert_eq!(
        json["options"]["result_page"],
        json!({
            "limit": 501,
            "offset": 500
        })
    );
}

#[test]
fn query_request_serializes_runtime_execution_limits() {
    let request = QueryRequest::new(
        "gs://axon-fixtures/sample_table",
        "SELECT * FROM axon_table",
        ExecutionTarget::BrowserWasm,
    )
    .with_options(QueryExecutionOptions {
        include_explain: false,
        collect_metrics: true,
        result_page: Some(QueryResultPage {
            limit: 500,
            offset: 0,
        }),
        runtime_limits: Some(query_contract::QueryRuntimeLimits {
            max_result_rows: Some(500),
            max_arrow_ipc_bytes: Some(1_048_576),
            max_scan_bytes: Some(10_485_760),
        }),
    });

    let json = serde_json::to_value(&request).expect("query request should serialize");

    assert_eq!(
        json["options"]["runtime_limits"],
        json!({
            "max_result_rows": 500,
            "max_arrow_ipc_bytes": 1048576,
            "max_scan_bytes": 10485760
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
fn delta_location_resolve_exchange_validation_rejects_responses_for_the_wrong_request() {
    let request = DeltaLocationResolveRequest {
        provider: DeltaObjectStoreProvider::Gcs,
        table_uri: "gs://axon-fixtures/sample_table".to_string(),
        credential_profile: CredentialProfileRef {
            id: "prod-readonly".to_string(),
            display_name: None,
        },
        requested_access_mode: ResolverRequestedAccessMode::SignedUrl,
        snapshot_version: Some(12),
    };
    let mut response = DeltaLocationResolveResponse {
        descriptor: BrowserHttpSnapshotDescriptor {
            table_uri: "axon-resolved://session/events/v12".to_string(),
            snapshot_version: 12,
            partition_column_types: BTreeMap::new(),
            browser_compatibility: CapabilityReport::default(),
            required_capabilities: CapabilityReport::default(),
            active_files: Vec::new(),
        },
        provider: DeltaObjectStoreProvider::Gcs,
        table_uri: "gs://axon-fixtures/sample_table".to_string(),
        requested_snapshot_version: Some(12),
        resolved_snapshot_version: 12,
        requested_access_mode: Some(ResolverRequestedAccessMode::SignedUrl),
        actual_access_mode: ResolverActualAccessMode::SignedUrl,
        expires_at_epoch_ms: 4_102_444_800_000,
        correlation_id: Some("corr-request-match".to_string()),
        warnings: Vec::new(),
        refresh: None,
    };

    validate_delta_location_resolve_exchange(&request, &response, 4_102_444_700_000)
        .expect("matching resolver exchange should validate");

    response.table_uri = "gs://axon-fixtures/other_table".to_string();
    let error = validate_delta_location_resolve_exchange(&request, &response, 4_102_444_700_000)
        .expect_err("resolver responses must match the requested table URI");
    assert_eq!(error.code, DeltaLocationResolverErrorCode::PolicyBlocked);
    assert_eq!(error.correlation_id.as_deref(), Some("corr-request-match"));

    response.table_uri = request.table_uri.clone();
    response.resolved_snapshot_version = 11;
    response.descriptor.snapshot_version = 11;
    let error = validate_delta_location_resolve_exchange(&request, &response, 4_102_444_700_000)
        .expect_err("resolver responses must match the requested snapshot");
    assert_eq!(
        error.code,
        DeltaLocationResolverErrorCode::SnapshotVersionNotFound
    );

    response.resolved_snapshot_version = 12;
    response.descriptor.snapshot_version = 12;
    response.actual_access_mode = ResolverActualAccessMode::Proxy;
    let error = validate_delta_location_resolve_exchange(&request, &response, 4_102_444_700_000)
        .expect_err("resolver responses must satisfy the requested access mode");
    assert_eq!(
        error.code,
        DeltaLocationResolverErrorCode::AccessModeNotSupported
    );
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

#[test]
fn browser_object_url_validation_allows_blob_only_for_browser_local_policy() {
    let url = validate_browser_object_url(
        "blob:https://127.0.0.1:5173/2cf91b7c-8f64-4d42-b85d-4f517db2ef21",
        ExecutionTarget::BrowserWasm,
        BrowserObjectUrlPolicy::HttpsOrBrowserLocalBlob,
        "browser object URL",
    )
    .expect("browser-local policy should allow blob URLs");

    assert_eq!(url.scheme(), "blob");

    let error = validate_browser_object_url(
        "blob:https://127.0.0.1:5173/2cf91b7c-8f64-4d42-b85d-4f517db2ef21",
        ExecutionTarget::BrowserWasm,
        BrowserObjectUrlPolicy::HttpsOnly,
        "browser object URL",
    )
    .expect_err("https-only policy should reject blob URLs");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("unsupported scheme 'blob'"));
}

#[test]
fn browser_object_url_validation_combined_policy_allows_loopback_and_blob() {
    validate_browser_object_url(
        "http://127.0.0.1:8787/object",
        ExecutionTarget::Native,
        BrowserObjectUrlPolicy::HttpsOrLoopbackHttpForHostTestsOrBrowserLocalBlob,
        "browser object URL",
    )
    .expect("combined host-test policy should allow loopback HTTP on native targets");

    let blob_url = validate_browser_object_url(
        "blob:https://127.0.0.1:5173/2cf91b7c-8f64-4d42-b85d-4f517db2ef21",
        ExecutionTarget::BrowserWasm,
        BrowserObjectUrlPolicy::HttpsOrLoopbackHttpForHostTestsOrBrowserLocalBlob,
        "browser object URL",
    )
    .expect("combined host-test policy should allow browser-local blob URLs");

    assert_eq!(blob_url.scheme(), "blob");
}

#[test]
fn daxis_result_envelope_accepts_executed_axon_browser_result_shape() {
    let envelope = json!({
        "schema_version": "daxis.query_result.v1",
        "status": "executed",
        "execution_engine": "axon_browser",
        "result_transport": "arrow_ipc",
        "surface_kind": "saved_query",
        "intent_kind": "sql",
        "input_artifact_kind": "saved_sql",
        "compiled_artifact_kind": "validated_sql",
        "request_id": "req-01JHEADLESS",
        "correlation_id": "corr-01JHEADLESS",
        "query_id": "qry-01JHEADLESS",
        "execution_id": "exec-01JHEADLESS",
        "workspace_id": "ws-01JHEADLESS",
        "metrics": {
            "rows_returned": 2,
            "arrow_ipc_bytes": 640,
            "scan_bytes": 4096,
            "duration_ms": 18
        },
        "diagnostics": {
            "worker_artifact_id": "axon-web-worker@sha256:abc123",
            "sql_fingerprint": "sqlfp_8cc7e5"
        }
    });

    validate_daxis_result_envelope(&envelope).expect("executed envelope should validate");
}

#[test]
fn daxis_result_envelope_rejects_conflicting_fallback_and_block_reasons() {
    let envelope = json!({
        "schema_version": "daxis.query_result.v1",
        "status": "fallback",
        "execution_engine": null,
        "result_transport": null,
        "fallback_reason": "unsupported_sql",
        "block_reason": "policy_denied",
        "surface_kind": "api",
        "intent_kind": "sql",
        "input_artifact_kind": "raw_sql",
        "compiled_artifact_kind": "validated_sql",
        "request_id": "req-conflict",
        "correlation_id": "corr-conflict",
        "query_id": "qry-conflict",
        "execution_id": "exec-conflict",
        "workspace_id": "ws-conflict",
        "metrics": {},
        "diagnostics": {}
    });

    let error = validate_daxis_result_envelope(&envelope)
        .expect_err("fallback and block reasons should be mutually exclusive");

    assert!(error.message.contains("fallback_reason and block_reason"));
}

#[test]
fn daxis_result_envelope_serializes_null_terminal_metadata() {
    let envelope = query_contract::DaxisResultEnvelope {
        schema_version: "daxis.query_result.v1".to_string(),
        status: query_contract::DaxisQueryStatus::Rejected,
        execution_engine: None,
        result_transport: None,
        fallback_reason: None,
        block_reason: Some(query_contract::DaxisQueryBlockReason::PolicyDenied),
        surface_kind: query_contract::DaxisSurfaceKind::Api,
        intent_kind: query_contract::DaxisIntentKind::Sql,
        input_artifact_kind: query_contract::DaxisInputArtifactKind::RawSql,
        compiled_artifact_kind: query_contract::DaxisCompiledArtifactKind::ValidatedSql,
        request_id: "req-null-metadata".to_string(),
        correlation_id: "corr-null-metadata".to_string(),
        query_id: "qry-null-metadata".to_string(),
        execution_id: "exec-null-metadata".to_string(),
        workspace_id: "ws-null-metadata".to_string(),
        metrics: query_contract::DaxisQueryMetrics::default(),
        diagnostics: Default::default(),
    };

    let json = serde_json::to_value(&envelope).expect("envelope should serialize");

    assert_eq!(json["execution_engine"], serde_json::Value::Null);
    assert_eq!(json["result_transport"], serde_json::Value::Null);
    assert_eq!(json["fallback_reason"], serde_json::Value::Null);
}

#[test]
fn daxis_approved_axon_read_descriptor_accepts_validated_read_only_sql() {
    let descriptor = daxis_approved_axon_read_descriptor_json();

    validate_daxis_approved_axon_read_descriptor(&descriptor, 1_800_000_000_000)
        .expect("approved Axon descriptor should validate");
}

#[test]
fn daxis_approved_axon_read_descriptor_rejects_unvalidated_sql() {
    let mut descriptor = daxis_approved_axon_read_descriptor_json();
    descriptor["validated_sql"]["validation_id"] = json!("");

    let error = validate_daxis_approved_axon_read_descriptor(&descriptor, 1_800_000_000_000)
        .expect_err("validation id must be present before Axon execution");

    assert!(error.message.contains("validation_id"));
}

#[test]
fn daxis_approved_axon_read_descriptor_rejects_multiple_table_descriptors() {
    let mut descriptor = daxis_approved_axon_read_descriptor_json();
    let second_table = descriptor["tables"][0].clone();
    descriptor["tables"]
        .as_array_mut()
        .unwrap()
        .push(second_table);

    let error = validate_daxis_approved_axon_read_descriptor(&descriptor, 1_800_000_000_000)
        .expect_err("v1 approved Axon descriptors should stay single-table");

    assert!(error.message.contains("exactly one table descriptor"));
}

#[test]
fn daxis_approved_axon_read_descriptor_rejects_table_descriptor_mismatch() {
    let mut descriptor = daxis_approved_axon_read_descriptor_json();
    descriptor["tables"][0]["descriptor"]["table_uri"] =
        json!("gs://daxis-prod-lakehouse/sales/customers");

    let error = validate_daxis_approved_axon_read_descriptor(&descriptor, 1_800_000_000_000)
        .expect_err("table URI mismatch should fail closed");

    assert!(error.message.contains("table_uri"));
}

fn daxis_approved_axon_read_descriptor_json() -> Value {
    json!({
        "schema_version": "daxis.approved_axon_read.v1",
        "request_id": "req-01JHEADLESS",
        "correlation_id": "corr-01JHEADLESS",
        "query_id": "qry-01JHEADLESS",
        "execution_id": "exec-01JHEADLESS",
        "workspace_id": "ws-01JHEADLESS",
        "surface_kind": "saved_query",
        "intent_kind": "sql",
        "input_artifact_kind": "saved_sql",
        "compiled_artifact_kind": "validated_sql",
        "validated_sql": {
            "sql": "select order_id, total from orders where order_date >= DATE '2026-05-01'",
            "dialect": "daxis_sql_v1",
            "fingerprint": "sqlfp_8cc7e5",
            "validation_id": "sqlval-01JHEADLESS",
            "read_only": true
        },
        "tables": [
            {
                "catalog_id": "cat-prod",
                "table_id": "tbl-orders",
                "descriptor_id": "desc-orders-v42",
                "table_uri": "gs://daxis-prod-lakehouse/sales/orders",
                "full_name": "main.sales.orders",
                "snapshot_version": 42,
                "schema_hash": "sha256:orders-schema",
                "descriptor_schema_hash": "sha256:orders-schema",
                "descriptor": {
                    "table_uri": "gs://daxis-prod-lakehouse/sales/orders",
                    "snapshot_version": 42,
                    "partition_column_types": {
                        "order_date": "string"
                    },
                    "active_files": [
                        {
                            "path": "order_date=2026-05-30/part-00000.parquet",
                            "url": "https://storage.googleapis.com/daxis-prod-lakehouse/sales/orders/order_date=2026-05-30/part-00000.parquet?X-Goog-Signature=redacted",
                            "size_bytes": 4096,
                            "partition_values": {
                                "order_date": "2026-05-30"
                            }
                        }
                    ]
                }
            }
        ],
        "access_proof": {
            "policy_decision_id": "pol-01JHEADLESS",
            "read_access_decision": "approved",
            "expires_at_epoch_ms": 1_800_000_060_000_u64
        },
        "limits": {
            "max_result_rows": 500,
            "max_arrow_ipc_bytes": 1048576,
            "max_scan_bytes": 10485760,
            "timeout_ms": 15000,
            "cancellation_deadline_epoch_ms": 1_800_000_015_000_u64
        },
        "runtime_preference": {
            "preferred_engine": "axon_browser",
            "allow_remote_fallback": true
        }
    })
}
