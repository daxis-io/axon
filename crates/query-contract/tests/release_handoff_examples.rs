use std::path::PathBuf;

use query_contract::{
    validate_daxis_approved_axon_read_descriptor, validate_daxis_result_envelope,
    validate_delta_location_resolve_exchange, validate_delta_location_resolve_request,
    BrokeredDeltaAccessMode, BrowserHttpSnapshotDescriptor, CapabilityKey, CapabilityState,
    DaxisQueryStatus, DeltaLocationResolveRequest, DeltaLocationResolveResponse,
    DeltaLocationResolverErrorCode, DeltaObjectStoreProvider, DirectExternalEngineReadSupport,
    ExecutionTarget, ObjectGrantAuditAction, ObjectGrantAuditEvent, ObjectGrantAuditOutcome,
    ObjectGrantBatchSignRequest, ObjectGrantBatchSignResponse, ObjectGrantHeadRequest,
    ObjectGrantListRequest, ObjectGrantListResponse, ObjectGrantObject, ObjectGrantRangeRequest,
    PolicyAuthorityKind, ReadAccessPlan, ReadAccessPlanReason, ResolverActualAccessMode,
    ResolverRequestedAccessMode, SnapshotResolutionRequest,
};

fn example_path(file_name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/program/browser-lakehouse-release-handoff-examples")
        .join(file_name)
}

fn read_example(file_name: &str) -> String {
    std::fs::read_to_string(example_path(file_name))
        .unwrap_or_else(|error| panic!("failed to read example '{}': {error}", file_name))
}

fn daxis_example_path(file_name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/integrations/daxis/daxis-first-class-integration-examples")
        .join(file_name)
}

fn read_daxis_example(file_name: &str) -> String {
    std::fs::read_to_string(daxis_example_path(file_name))
        .unwrap_or_else(|error| panic!("failed to read Daxis example '{}': {error}", file_name))
}

#[test]
fn latest_snapshot_resolution_request_example_matches_contract() {
    let request: SnapshotResolutionRequest =
        serde_json::from_str(&read_example("snapshot-resolution-request.latest.json"))
            .expect("snapshot resolution request example should deserialize");

    assert_eq!(request.table_uri, "gs://axon-fixtures/partitioned-table");
    assert_eq!(request.snapshot_version, None);
}

#[test]
fn resolved_snapshot_descriptor_example_matches_supported_contract() {
    let descriptor: query_contract::ResolvedSnapshotDescriptor =
        serde_json::from_str(&read_example("resolved-snapshot-descriptor.supported.json"))
            .expect("resolved snapshot descriptor example should deserialize");

    assert_eq!(descriptor.table_uri, "gs://axon-fixtures/partitioned-table");
    assert_eq!(descriptor.snapshot_version, 12);
    assert!(descriptor.required_capabilities.capabilities.is_empty());
    assert!(descriptor.browser_compatibility.capabilities.is_empty());
    assert_eq!(descriptor.active_files.len(), 2);
}

#[test]
fn browser_http_snapshot_descriptor_example_matches_native_only_contract() {
    let descriptor: BrowserHttpSnapshotDescriptor = serde_json::from_str(&read_example(
        "browser-http-snapshot-descriptor.native-only.json",
    ))
    .expect("browser http snapshot descriptor example should deserialize");

    assert_eq!(
        descriptor.table_uri,
        "gs://axon-fixtures/change-data-feed-table"
    );
    assert_eq!(descriptor.snapshot_version, 7);
    assert_eq!(
        descriptor
            .required_capabilities
            .state(CapabilityKey::ChangeDataFeed),
        Some(CapabilityState::NativeOnly)
    );
    assert_eq!(
        descriptor
            .browser_compatibility
            .state(CapabilityKey::ChangeDataFeed),
        Some(CapabilityState::NativeOnly)
    );
    assert!(descriptor
        .active_files
        .iter()
        .all(|file| file.url.starts_with("https://")));
}

#[test]
fn resolved_snapshot_descriptor_hard_fail_example_is_represented_as_query_error_not_descriptor() {
    let error: query_contract::QueryError =
        serde_json::from_str(&read_example("query-error.unknown-protocol-feature.json"))
            .expect("unknown protocol feature example should deserialize");

    assert_eq!(error.target, ExecutionTarget::Native);
    assert_eq!(
        error.code,
        query_contract::QueryErrorCode::UnsupportedFeature
    );
    assert_eq!(error.fallback_reason, None);
}

#[test]
fn daxis_snapshot_descriptor_request_example_matches_contract() {
    let request: DeltaLocationResolveRequest = serde_json::from_str(&read_daxis_example(
        "snapshot-descriptor-request.latest.json",
    ))
    .expect("Daxis snapshot descriptor request should deserialize");

    assert_eq!(request.provider, DeltaObjectStoreProvider::Gcs);
    assert_eq!(request.table_uri, "gs://daxis-prod-lakehouse/sales/orders");
    assert_eq!(request.credential_profile.id, "daxis-workspace-prod-read");
    assert_eq!(
        request.requested_access_mode,
        ResolverRequestedAccessMode::Auto
    );
    assert_eq!(request.snapshot_version, None);
    validate_delta_location_resolve_request(&request)
        .expect("Daxis snapshot descriptor request should validate");
}

#[test]
fn daxis_snapshot_descriptor_response_example_matches_contract() {
    let request: DeltaLocationResolveRequest = serde_json::from_str(&read_daxis_example(
        "snapshot-descriptor-request.latest.json",
    ))
    .expect("Daxis snapshot descriptor request should deserialize");
    let response: DeltaLocationResolveResponse = serde_json::from_str(&read_daxis_example(
        "snapshot-descriptor-response.signed-url.json",
    ))
    .expect("Daxis snapshot descriptor response should deserialize");

    assert_eq!(response.provider, DeltaObjectStoreProvider::Gcs);
    assert_eq!(response.resolved_snapshot_version, 42);
    assert_eq!(
        response.actual_access_mode,
        ResolverActualAccessMode::SignedUrl
    );
    assert_eq!(
        response.correlation_id.as_deref(),
        Some("daxis-req-01JABCDEF")
    );
    assert_eq!(response.descriptor.snapshot_version, 42);
    assert_eq!(response.descriptor.active_files.len(), 2);
    assert!(response
        .descriptor
        .active_files
        .iter()
        .all(|file| file.url.starts_with("https://storage.googleapis.com/")));
    validate_delta_location_resolve_exchange(&request, &response, 1_800_000_000_000)
        .expect("Daxis snapshot descriptor response should validate before expiry");
    validate_daxis_signed_url_path_scope(&response)
        .expect("Daxis signed URLs should stay scoped to table root and active file paths");
}

#[test]
fn daxis_snapshot_descriptor_negative_examples_fail_closed() {
    let latest_request: DeltaLocationResolveRequest = serde_json::from_str(&read_daxis_example(
        "snapshot-descriptor-request.latest.json",
    ))
    .expect("Daxis latest snapshot request should deserialize");
    let pinned_request: DeltaLocationResolveRequest = serde_json::from_str(&read_daxis_example(
        "snapshot-descriptor-request.version-42.json",
    ))
    .expect("Daxis pinned snapshot request should deserialize");

    let expired: DeltaLocationResolveResponse = serde_json::from_str(&read_daxis_example(
        "snapshot-descriptor-response.expired.json",
    ))
    .expect("expired Daxis descriptor fixture should deserialize");
    let error =
        validate_delta_location_resolve_exchange(&latest_request, &expired, 1_800_000_000_000)
            .expect_err("expired Daxis descriptor should fail closed");
    assert_eq!(
        error.code,
        DeltaLocationResolverErrorCode::DescriptorExpired
    );
    assert_eq!(
        error.correlation_id.as_deref(),
        Some("daxis-req-01JEXPIRED")
    );

    let snapshot_mismatch: DeltaLocationResolveResponse = serde_json::from_str(
        &read_daxis_example("snapshot-descriptor-response.snapshot-version-mismatch.json"),
    )
    .expect("snapshot mismatch Daxis descriptor fixture should deserialize");
    let error = validate_delta_location_resolve_exchange(
        &pinned_request,
        &snapshot_mismatch,
        1_800_000_000_000,
    )
    .expect_err("Daxis descriptor for a different snapshot should fail closed");
    assert_eq!(
        error.code,
        DeltaLocationResolverErrorCode::SnapshotVersionNotFound
    );
    assert_eq!(
        error.correlation_id.as_deref(),
        Some("daxis-req-01JVERSION")
    );

    let path_escape: DeltaLocationResolveResponse = serde_json::from_str(&read_daxis_example(
        "snapshot-descriptor-response.path-escape.json",
    ))
    .expect("path escape Daxis descriptor fixture should deserialize");
    validate_delta_location_resolve_exchange(&latest_request, &path_escape, 1_800_000_000_000)
        .expect("path escape fixture should otherwise match the resolver envelope");
    let error = validate_daxis_signed_url_path_scope(&path_escape)
        .expect_err("Daxis descriptor URLs must stay scoped to table root and file path");
    assert!(error.contains("escaped table root"), "{error}");

    let invalid_descriptor =
        read_daxis_example("snapshot-descriptor-response.invalid-descriptor.json");
    let error = serde_json::from_str::<DeltaLocationResolveResponse>(&invalid_descriptor)
        .expect_err("invalid descriptor fixture should not deserialize");
    assert!(
        error.to_string().contains("url") || error.to_string().contains("active_files"),
        "{error}"
    );
}

#[test]
fn daxis_read_access_plan_examples_match_contract() {
    let brokered: ReadAccessPlan =
        serde_json::from_str(&read_daxis_example("read-access-plan.brokered-delta.json"))
            .expect("Daxis brokered Delta plan should deserialize");
    let ReadAccessPlan::BrokeredDelta(plan) = brokered else {
        panic!("expected brokered_delta read access plan")
    };
    assert_eq!(plan.table_id, "tbl_daxis_sales_orders");
    assert_eq!(plan.full_name, "main.sales.orders");
    assert_eq!(plan.delta_access_mode, BrokeredDeltaAccessMode::DeltaLog);
    assert_eq!(
        plan.policy_authority.authority,
        PolicyAuthorityKind::UnityCatalog
    );
    assert_eq!(
        plan.policy_authority.direct_external_engine_read,
        DirectExternalEngineReadSupport::Confirmed
    );
    assert!(plan.object_access.list);
    assert!(plan.object_access.head);
    assert!(plan.object_access.range_get);
    assert!(plan.object_access.batch_sign);

    let delta_sharing: ReadAccessPlan =
        serde_json::from_str(&read_daxis_example("read-access-plan.delta-sharing.json"))
            .expect("Daxis Delta Sharing plan should deserialize");
    assert!(matches!(delta_sharing, ReadAccessPlan::DeltaSharing(_)));

    let fallback: ReadAccessPlan = serde_json::from_str(&read_daxis_example(
        "read-access-plan.sql-fallback-required.json",
    ))
    .expect("Daxis SQL fallback plan should deserialize");
    let ReadAccessPlan::SqlFallbackRequired(plan) = fallback else {
        panic!("expected sql_fallback_required read access plan")
    };
    assert_eq!(plan.reason, ReadAccessPlanReason::RowFilter);
    assert!(plan.warehouse_required);
    assert!(plan.statement_endpoint.starts_with("https://"));

    let blocked: ReadAccessPlan =
        serde_json::from_str(&read_daxis_example("read-access-plan.blocked.json"))
            .expect("Daxis blocked plan should deserialize");
    let ReadAccessPlan::Blocked(plan) = blocked else {
        panic!("expected blocked read access plan")
    };
    assert_eq!(plan.reason, ReadAccessPlanReason::UnknownPolicyState);
}

#[test]
fn daxis_object_grant_route_examples_match_contract() {
    let list_request: ObjectGrantListRequest =
        serde_json::from_str(&read_daxis_example("object-grants.list-request.json"))
            .expect("Daxis object grant list request should deserialize");
    assert_eq!(list_request.prefix, "order_date=2026-05-30/");

    let list_response: ObjectGrantListResponse =
        serde_json::from_str(&read_daxis_example("object-grants.list-response.json"))
            .expect("Daxis object grant list response should deserialize");
    assert_eq!(list_response.objects.len(), 1);
    assert_eq!(
        list_response.objects[0].path,
        "order_date=2026-05-30/part-00000.parquet"
    );

    let head_request: ObjectGrantHeadRequest =
        serde_json::from_str(&read_daxis_example("object-grants.head-request.json"))
            .expect("Daxis object grant head request should deserialize");
    let head_response: ObjectGrantObject =
        serde_json::from_str(&read_daxis_example("object-grants.head-response.json"))
            .expect("Daxis object grant head response should deserialize");
    assert_eq!(head_response.path, head_request.path);
    assert_eq!(head_response.size_bytes, 32768);

    let batch_sign_request: ObjectGrantBatchSignRequest =
        serde_json::from_str(&read_daxis_example("object-grants.batch-sign-request.json"))
            .expect("Daxis object grant batch-sign request should deserialize");
    let batch_sign_response: ObjectGrantBatchSignResponse = serde_json::from_str(
        &read_daxis_example("object-grants.batch-sign-response.json"),
    )
    .expect("Daxis object grant batch-sign response should deserialize");
    assert_eq!(batch_sign_request.paths.len(), 1);
    assert_eq!(batch_sign_response.signed_urls.len(), 1);
    assert_eq!(
        batch_sign_response.signed_urls[0].path,
        batch_sign_request.paths[0]
    );
    assert!(batch_sign_response.signed_urls[0]
        .url
        .starts_with("https://storage.googleapis.com/"));
    assert!(batch_sign_response.signed_urls[0].expires_at_epoch_ms > 1_800_000_000_000);

    let range_request: ObjectGrantRangeRequest =
        serde_json::from_str(&read_daxis_example("object-grants.range-request.json"))
            .expect("Daxis object grant range request should deserialize");
    assert_eq!(range_request.path, batch_sign_request.paths[0]);
    assert!(range_request.start < range_request.end);
}

#[test]
fn daxis_object_grant_audit_example_ties_access_to_query_context() {
    let event: ObjectGrantAuditEvent =
        serde_json::from_str(&read_daxis_example("object-grants.audit-event.range.json"))
            .expect("Daxis object grant audit event should deserialize");

    assert_eq!(event.event_type, "object_grant_access");
    assert_eq!(event.tenant_id, "tenant_daxis_prod");
    assert_eq!(event.workspace_id, "workspace_sales");
    assert_eq!(event.user_subject, "user:analyst@example.test");
    assert_eq!(event.table_id, "tbl_daxis_sales_orders");
    assert_eq!(event.full_name, "main.sales.orders");
    assert_eq!(event.grant_id, "grant_daxis_sales_orders_01JABCDEF");
    assert_eq!(event.query_id, "axon-query-01JABCDEF");
    assert_eq!(event.request_id, "req-daxis-object-read");
    assert_eq!(event.correlation_id, "daxis-corr-01JABCDEF");
    assert_eq!(event.action, ObjectGrantAuditAction::Range);
    assert_eq!(
        event.object_path,
        "order_date=2026-05-30/part-00000.parquet"
    );
    assert_eq!(
        event
            .range
            .expect("range action should carry byte range")
            .start,
        0
    );
    assert_eq!(event.outcome, ObjectGrantAuditOutcome::Allowed);
}

#[test]
fn daxis_approved_axon_read_descriptor_example_matches_contract() {
    let descriptor: serde_json::Value = serde_json::from_str(&read_daxis_example(
        "approved-axon-read-descriptor.saved-query.json",
    ))
    .expect("Daxis-approved Axon read descriptor example should be valid JSON");

    let descriptor = validate_daxis_approved_axon_read_descriptor(&descriptor, 1_800_000_000_000)
        .expect("Daxis-approved Axon read descriptor example should validate");

    assert_eq!(descriptor.request_id, "req-daxis-headless-saved-query");
    assert_eq!(
        descriptor.validated_sql.dialect,
        query_contract::DaxisSqlDialect::DaxisSqlV1
    );
    assert!(descriptor.validated_sql.read_only);
    assert_eq!(descriptor.tables.len(), 1);
    assert_eq!(descriptor.tables[0].table_id, "tbl_daxis_sales_orders");
    assert_eq!(descriptor.tables[0].descriptor.snapshot_version, 42);
}

#[test]
fn daxis_surface_result_envelope_examples_share_one_contract_shape() {
    for (fixture, expected_surface) in [
        ("query-result.agent.executed.json", "agent"),
        (
            "query-result.dashboard-tile.executed.json",
            "dashboard_tile",
        ),
        ("query-result.builder.executed.json", "builder"),
        ("query-result.saved-query.executed.json", "saved_query"),
        ("query-result.api.executed.json", "api"),
    ] {
        let envelope: serde_json::Value = serde_json::from_str(&read_daxis_example(fixture))
            .unwrap_or_else(|error| {
                panic!("Daxis result envelope fixture {fixture} should be valid JSON: {error}")
            });

        assert_eq!(envelope["surface_kind"], expected_surface);
        let envelope = validate_daxis_result_envelope(&envelope).unwrap_or_else(|error| {
            panic!(
                "Daxis result envelope fixture {fixture} should validate: {}",
                error.message
            )
        });

        assert_eq!(envelope.status, DaxisQueryStatus::Executed);
        assert_eq!(
            envelope.execution_engine,
            Some(query_contract::DaxisExecutionEngine::AxonBrowser)
        );
        assert_eq!(
            envelope.result_transport,
            Some(query_contract::DaxisResultTransport::ArrowIpc)
        );
        assert!(envelope.fallback_reason.is_none());
        assert!(envelope.block_reason.is_none());
    }
}

#[test]
fn daxis_negative_result_envelope_examples_preserve_structured_reasons() {
    for (fixture, expected_status, expected_reason_field) in [
        (
            "query-result.policy-denied.rejected.json",
            DaxisQueryStatus::Rejected,
            "block_reason",
        ),
        (
            "query-result.unsupported-sql.fallback.json",
            DaxisQueryStatus::Fallback,
            "fallback_reason",
        ),
        (
            "query-result.runtime-budget-overflow.fallback.json",
            DaxisQueryStatus::Fallback,
            "fallback_reason",
        ),
    ] {
        let envelope: serde_json::Value = serde_json::from_str(&read_daxis_example(fixture))
            .unwrap_or_else(|error| {
                panic!("Daxis result envelope fixture {fixture} should be valid JSON: {error}")
            });

        assert!(envelope.get(expected_reason_field).is_some());
        let envelope = validate_daxis_result_envelope(&envelope).unwrap_or_else(|error| {
            panic!(
                "Daxis result envelope fixture {fixture} should validate: {}",
                error.message
            )
        });
        assert_eq!(envelope.status, expected_status);
    }
}

fn validate_daxis_signed_url_path_scope(
    response: &DeltaLocationResolveResponse,
) -> Result<(), String> {
    if response.provider != DeltaObjectStoreProvider::Gcs
        || response.actual_access_mode != ResolverActualAccessMode::SignedUrl
    {
        return Ok(());
    }

    let table_uri = url::Url::parse(&response.table_uri)
        .map_err(|error| format!("invalid Daxis table URI: {error}"))?;
    let bucket = table_uri
        .host_str()
        .ok_or_else(|| "Daxis GCS table URI must include a bucket".to_string())?;
    let table_root = table_uri.path().trim_matches('/');

    for file in &response.descriptor.active_files {
        if file.path.is_empty()
            || file.path.starts_with('/')
            || file.path.split('/').any(|segment| segment == "..")
        {
            return Err(format!(
                "Daxis descriptor active file path '{}' escaped table root",
                file.path
            ));
        }

        let signed_url = url::Url::parse(&file.url).map_err(|error| {
            format!(
                "Daxis descriptor active file URL '{}' was invalid: {error}",
                file.url
            )
        })?;
        if signed_url.host_str() == Some("storage.googleapis.com") {
            let expected_path = format!("/{bucket}/{table_root}/{}", file.path);
            if signed_url.path() != expected_path {
                return Err(format!(
                    "Daxis descriptor active file URL '{}' escaped table root '{}'",
                    signed_url.path(),
                    expected_path
                ));
            }
        }
    }

    Ok(())
}
