use axon_contract_proto::{axon, axon_exec_v1};
use buffa::{Message, MessageName};
use buffa_types::google::protobuf::Timestamp;

#[test]
fn exec_messages_are_exported_through_nested_and_compatibility_modules() {
    assert_eq!(
        <axon::exec::v1::ExecuteRequest as MessageName>::FULL_NAME,
        "axon.exec.v1.ExecuteRequest"
    );
    assert_eq!(
        <axon_exec_v1::ExecuteRequest as MessageName>::FULL_NAME,
        "axon.exec.v1.ExecuteRequest"
    );
    assert_eq!(
        <axon_exec_v1::BrowserWorkerCommand as MessageName>::FULL_NAME,
        "axon.exec.v1.BrowserWorkerCommand"
    );
    assert_eq!(
        <axon_exec_v1::ExecutionAdmission as MessageName>::FULL_NAME,
        "axon.exec.v1.ExecutionAdmission"
    );
    assert_eq!(
        <axon_exec_v1::ExecutionTerminalState as MessageName>::FULL_NAME,
        "axon.exec.v1.ExecutionTerminalState"
    );
}

#[test]
fn execution_binding_admission_terminal_and_cancellation_round_trip() {
    let command = axon_exec_v1::BrowserWorkerCommand {
        command: axon_exec_v1::BrowserWorkerCancelCommand {
            request_id: "cancel-control-001".into(),
            execution_id: "execution-001".into(),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    };
    let decoded_command =
        axon_exec_v1::BrowserWorkerCommand::decode_from_slice(&command.encode_to_vec())
            .expect("worker command should decode");

    match decoded_command.command {
        Some(axon_exec_v1::browser_worker_command::Command::Cancel(cancel)) => {
            assert_eq!(cancel.request_id, "cancel-control-001");
            assert_eq!(cancel.execution_id, "execution-001");
        }
        other => panic!("expected cancel command, got {other:?}"),
    }

    let request = axon_exec_v1::ExecuteRequest {
        execution_id: "execution-001".into(),
        binding: axon::common::v1::CanonicalResourceRef {
            connection_id: "connection-public-gcs".into(),
            provider_namespace: "axon.public-gcs/v1".into(),
            kind: axon::common::v1::ResourceKind::Table.into(),
            identity: Some(
                axon::common::v1::canonical_resource_ref::Identity::CanonicalLocator(
                    "gs://axon-fixtures/tables/events".into(),
                ),
            ),
            ..Default::default()
        }
        .into(),
        query: axon_exec_v1::QueryRequest {
            sql: "select * from events".into(),
            preferred_target: axon_exec_v1::ExecutionTarget::Native.into(),
            options: axon_exec_v1::QueryExecutionOptions {
                runtime_limits: axon_exec_v1::QueryRuntimeLimits {
                    max_result_rows: Some(501),
                    max_arrow_ipc_bytes: Some(8 * 1024 * 1024),
                    max_preview_string_bytes: Some(256 * 1024),
                    max_scan_bytes: Some(64 * 1024 * 1024),
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }
        .into(),
        deadline: Timestamp {
            seconds: 1_800_000_120,
            ..Default::default()
        }
        .into(),
        ..Default::default()
    };
    let decoded_request = axon_exec_v1::ExecuteRequest::decode_from_slice(&request.encode_to_vec())
        .expect("execute request should decode");
    assert_eq!(decoded_request.execution_id, "execution-001");
    assert!(matches!(
        decoded_request.binding,
        Some(axon_exec_v1::execute_request::Binding::LogicalResource(_))
    ));
    assert_eq!(
        decoded_request
            .deadline
            .as_option()
            .expect("deadline should be present")
            .seconds,
        1_800_000_120
    );

    let admission = axon_exec_v1::ExecutionAdmission {
        outcome: axon_exec_v1::ExecutionAccepted {
            execution_id: "execution-001".into(),
            state: axon_exec_v1::ExecutionLifecycleState::Running.into(),
            launch: true,
            ..Default::default()
        }
        .into(),
        ..Default::default()
    };
    let admission_item = axon_exec_v1::ExecuteResponse {
        item: admission.into(),
        ..Default::default()
    };
    let decoded_admission =
        axon_exec_v1::ExecuteResponse::decode_from_slice(&admission_item.encode_to_vec())
            .expect("execute response should decode");
    assert!(matches!(
        decoded_admission.item,
        Some(axon_exec_v1::execute_response::Item::Admission(_))
    ));

    let terminal_item = axon_exec_v1::ExecuteResponse {
        item: axon_exec_v1::ExecutionTerminalFrame {
            execution_id: "execution-001".into(),
            sequence: 1,
            state: axon_exec_v1::ExecutionTerminalState {
                outcome: axon_exec_v1::ExecutionCancelled::default().into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    };
    let decoded_terminal =
        axon_exec_v1::ExecuteResponse::decode_from_slice(&terminal_item.encode_to_vec())
            .expect("terminal response should decode");
    assert!(matches!(
        decoded_terminal.item,
        Some(axon_exec_v1::execute_response::Item::Terminal(_))
    ));

    let cancel = axon_exec_v1::CancelResponse {
        execution_id: "execution-001".into(),
        state: axon_exec_v1::ExecutionLifecycleState::CancelRequested.into(),
        ..Default::default()
    };
    let decoded_cancel = axon_exec_v1::CancelResponse::decode_from_slice(&cancel.encode_to_vec())
        .expect("cancel response should decode");
    assert_eq!(decoded_cancel.execution_id, "execution-001");
    assert_eq!(
        decoded_cancel.state,
        axon_exec_v1::ExecutionLifecycleState::CancelRequested
    );
}

#[test]
fn resolved_browser_read_and_typed_capabilities_round_trip() {
    let resource = axon::common::v1::CanonicalResourceRef {
        connection_id: "connection-public-gcs".into(),
        provider_namespace: "axon.public-gcs/v1".into(),
        kind: axon::common::v1::ResourceKind::Table.into(),
        identity: Some(
            axon::common::v1::canonical_resource_ref::Identity::CanonicalLocator(
                "gs://axon-fixtures/tables/events".into(),
            ),
        ),
        ..Default::default()
    };
    let capabilities = axon::dataaccess::v1::CapabilityReport {
        capabilities: vec![axon::dataaccess::v1::CapabilityEntry {
            key: axon::dataaccess::v1::CapabilityKey::RangeReads.into(),
            state: axon::dataaccess::v1::CapabilityState::Supported.into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let binding = axon::dataaccess::v1::ResolvedBrowserRead {
        resource: resource.into(),
        descriptor: axon::dataaccess::v1::BrowserReadDescriptor {
            descriptor: axon::dataaccess::v1::BrowserHttpSnapshotDescriptor {
                table_uri: "gs://axon-fixtures/tables/events".into(),
                browser_compatibility: capabilities.into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }
        .into(),
        access_class: axon::dataaccess::v1::BrowserAccessClass::Public.into(),
        correlation_id: "correlation-001".into(),
        provenance: axon::dataaccess::v1::ResolutionProvenance {
            resolver_id: "public-object-storage".into(),
            resolution_id: "resolution-001".into(),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    };
    let resolution = axon::dataaccess::v1::ReadResolution {
        outcome: binding.into(),
        ..Default::default()
    };
    let decoded =
        axon::dataaccess::v1::ReadResolution::decode_from_slice(&resolution.encode_to_vec())
            .expect("read resolution should decode");

    match decoded.outcome {
        Some(axon::dataaccess::v1::read_resolution::Outcome::BrowserRead(read)) => {
            let resource = read
                .resource
                .as_option()
                .expect("canonical resource should be present");
            assert_eq!(resource.provider_namespace, "axon.public-gcs/v1");
            let descriptor = read
                .descriptor
                .as_option()
                .expect("browser descriptor should be present");
            assert!(matches!(
                descriptor.descriptor,
                Some(axon::dataaccess::v1::browser_read_descriptor::Descriptor::Snapshot(_))
            ));
        }
        other => panic!("expected browser-read resolution, got {other:?}"),
    }
}

#[test]
fn arrow_bytes_and_explicit_zero_presence_survive_binary_round_trips() {
    let arrow = axon_exec_v1::ArrowIpcResult {
        format: axon_exec_v1::ArrowIpcFormat::Stream.into(),
        content_type: "application/vnd.apache.arrow.stream".into(),
        bytes: Some(vec![1, 2, 3, 4]),
        byte_length: Some(4),
        ..Default::default()
    };
    let decoded_arrow = axon_exec_v1::ArrowIpcResult::decode_from_slice(&arrow.encode_to_vec())
        .expect("Arrow IPC envelope should decode");

    assert_eq!(
        decoded_arrow.bytes.as_deref(),
        Some([1, 2, 3, 4].as_slice())
    );
    assert_eq!(decoded_arrow.byte_length, Some(4));

    let page = axon_exec_v1::QueryResultPage {
        limit: Some(0),
        offset: Some(0),
        ..Default::default()
    };
    let decoded_page = axon_exec_v1::QueryResultPage::decode_from_slice(&page.encode_to_vec())
        .expect("query result page should decode");

    assert_eq!(decoded_page.limit, Some(0));
    assert_eq!(decoded_page.offset, Some(0));

    let descriptor = axon::dataaccess::v1::BrowserHttpSnapshotDescriptor {
        table_uri: "https://storage.example.test/table".into(),
        snapshot_version: Some(0),
        ..Default::default()
    };
    let decoded_descriptor =
        axon::dataaccess::v1::BrowserHttpSnapshotDescriptor::decode_from_slice(
            &descriptor.encode_to_vec(),
        )
        .expect("snapshot descriptor should decode");

    assert_eq!(decoded_descriptor.snapshot_version, Some(0));

    let metrics = axon_exec_v1::QueryMetricsSummary {
        range_cache_hits: Some(1),
        range_cache_misses: Some(0),
        range_cache_bytes_reused: Some(800),
        range_cache_bytes_stored: Some(2_560),
        range_cache_validation_misses: Some(0),
        range_cache_degraded_identity_reads: Some(0),
        range_readahead_requests: Some(0),
        range_readahead_bytes_fetched: Some(0),
        range_readahead_bytes_used: Some(0),
        range_readahead_wasted_bytes: Some(0),
        ..Default::default()
    };
    let decoded_metrics =
        axon_exec_v1::QueryMetricsSummary::decode_from_slice(&metrics.encode_to_vec())
            .expect("query metrics should decode");
    assert_eq!(decoded_metrics.range_cache_hits, Some(1));
    assert_eq!(decoded_metrics.range_cache_bytes_reused, Some(800));
    assert_eq!(decoded_metrics.range_readahead_wasted_bytes, Some(0));

    let worker_metrics = axon_exec_v1::BrowserWorkerRangeReadMetricsEvent {
        range_cache_hits: Some(1),
        range_cache_bytes_reused: Some(800),
        range_readahead_requests: Some(0),
        range_readahead_wasted_bytes: Some(0),
        ..Default::default()
    };
    let decoded_worker_metrics =
        axon_exec_v1::BrowserWorkerRangeReadMetricsEvent::decode_from_slice(
            &worker_metrics.encode_to_vec(),
        )
        .expect("worker metrics should decode");
    assert_eq!(decoded_worker_metrics.range_cache_hits, Some(1));
    assert_eq!(decoded_worker_metrics.range_readahead_requests, Some(0));
}

#[test]
fn semantic_negative_fixtures_reject_incomplete_contracts() {
    let resource = valid_resource();
    assert_eq!(validate_canonical_resource(&resource), Ok(()));

    let mut missing_connection = resource.clone();
    missing_connection.connection_id.clear();
    assert_eq!(
        validate_canonical_resource(&missing_connection),
        Err("canonical resource connection_id is required")
    );

    let mut unspecified_kind = resource.clone();
    unspecified_kind.kind = axon::common::v1::ResourceKind::Unspecified.into();
    assert_eq!(
        validate_canonical_resource(&unspecified_kind),
        Err("canonical resource kind is required")
    );

    let mut read = valid_browser_read();
    read.descriptor = Default::default();
    assert_eq!(
        validate_resolved_browser_read(&read),
        Err("browser_read descriptor is required")
    );

    let mut read = valid_browser_read();
    read.access_class = axon::dataaccess::v1::BrowserAccessClass::Unspecified.into();
    assert_eq!(
        validate_resolved_browser_read(&read),
        Err("browser_read access_class is required")
    );

    let mut read = valid_browser_read();
    read.provenance = Default::default();
    assert_eq!(
        validate_resolved_browser_read(&read),
        Err("browser_read provenance is required")
    );

    let mut read = valid_browser_read();
    read.correlation_id.clear();
    assert_eq!(
        validate_resolved_browser_read(&read),
        Err("browser_read correlation_id is required")
    );

    let mut read = valid_browser_read();
    let descriptor = read
        .descriptor
        .as_option_mut()
        .expect("valid fixture descriptor");
    let snapshot = match descriptor.descriptor.as_mut() {
        Some(axon::dataaccess::v1::browser_read_descriptor::Descriptor::Snapshot(snapshot)) => {
            snapshot
        }
        other => panic!("expected snapshot fixture, got {other:?}"),
    };
    snapshot.browser_compatibility = axon::dataaccess::v1::CapabilityReport {
        capabilities: vec![
            axon::dataaccess::v1::CapabilityEntry {
                key: axon::dataaccess::v1::CapabilityKey::RangeReads.into(),
                state: axon::dataaccess::v1::CapabilityState::Supported.into(),
                ..Default::default()
            },
            axon::dataaccess::v1::CapabilityEntry {
                key: axon::dataaccess::v1::CapabilityKey::RangeReads.into(),
                state: axon::dataaccess::v1::CapabilityState::NativeOnly.into(),
                ..Default::default()
            },
        ],
        ..Default::default()
    }
    .into();
    assert_eq!(
        validate_resolved_browser_read(&read),
        Err("duplicate capability key")
    );

    let mut request = valid_execute_request();
    request.binding = Some(axon_exec_v1::execute_request::Binding::BrowserRead(
        Box::default(),
    ));
    assert_eq!(
        validate_execute_request(&request),
        Err("browser_read resource is required")
    );
}

#[test]
fn semantic_response_order_rejects_unset_items_and_terminal_after_rejection() {
    let rejected = axon_exec_v1::ExecuteResponse {
        item: axon_exec_v1::ExecutionAdmission {
            outcome: axon_exec_v1::ExecutionRejected {
                execution_id: "execution-rejected".into(),
                reason: axon_exec_v1::ExecutionRejectionReason::InvalidRequest.into(),
                message: "invalid request".into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    };
    let terminal = axon_exec_v1::ExecuteResponse {
        item: axon_exec_v1::ExecutionTerminalFrame {
            execution_id: "execution-rejected".into(),
            sequence: 1,
            state: axon_exec_v1::ExecutionTerminalState {
                outcome: axon_exec_v1::ExecutionFailed::default().into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    };

    assert_eq!(
        validate_execute_response_order(&[rejected, terminal]),
        Err("terminal after rejected admission")
    );
    assert_eq!(
        validate_execute_response_order(&[axon_exec_v1::ExecuteResponse::default()]),
        Err("execution response item is required")
    );
}

fn valid_resource() -> axon::common::v1::CanonicalResourceRef {
    axon::common::v1::CanonicalResourceRef {
        connection_id: "connection-public-gcs".into(),
        provider_namespace: "axon.public-gcs/v1".into(),
        kind: axon::common::v1::ResourceKind::Table.into(),
        identity: Some(
            axon::common::v1::canonical_resource_ref::Identity::CanonicalLocator(
                "gs://axon-fixtures/tables/events".into(),
            ),
        ),
        ..Default::default()
    }
}

fn valid_browser_read() -> axon::dataaccess::v1::ResolvedBrowserRead {
    axon::dataaccess::v1::ResolvedBrowserRead {
        resource: valid_resource().into(),
        descriptor: axon::dataaccess::v1::BrowserReadDescriptor {
            descriptor: axon::dataaccess::v1::BrowserHttpSnapshotDescriptor {
                table_uri: "gs://axon-fixtures/tables/events".into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }
        .into(),
        access_class: axon::dataaccess::v1::BrowserAccessClass::Public.into(),
        correlation_id: "correlation-001".into(),
        provenance: axon::dataaccess::v1::ResolutionProvenance {
            resolver_id: "public-object-storage".into(),
            resolution_id: "resolution-001".into(),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

fn valid_execute_request() -> axon_exec_v1::ExecuteRequest {
    axon_exec_v1::ExecuteRequest {
        execution_id: "execution-001".into(),
        binding: valid_browser_read().into(),
        query: axon_exec_v1::QueryRequest {
            sql: "select * from events".into(),
            preferred_target: axon_exec_v1::ExecutionTarget::BrowserWasm.into(),
            options: axon_exec_v1::QueryExecutionOptions {
                runtime_limits: axon_exec_v1::QueryRuntimeLimits {
                    max_result_rows: Some(501),
                    max_arrow_ipc_bytes: Some(8 * 1024 * 1024),
                    max_preview_string_bytes: Some(256 * 1024),
                    max_scan_bytes: Some(64 * 1024 * 1024),
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }
        .into(),
        deadline: Timestamp {
            seconds: 1_800_000_120,
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

fn validate_canonical_resource(
    resource: &axon::common::v1::CanonicalResourceRef,
) -> Result<(), &'static str> {
    if resource.connection_id.trim().is_empty() {
        return Err("canonical resource connection_id is required");
    }
    if resource.provider_namespace.trim().is_empty() {
        return Err("canonical resource provider_namespace is required");
    }
    if !matches!(
        resource.kind.as_known(),
        Some(axon::common::v1::ResourceKind::Table | axon::common::v1::ResourceKind::Volume)
    ) {
        return Err("canonical resource kind is required");
    }
    let identity = match resource.identity.as_ref() {
        Some(axon::common::v1::canonical_resource_ref::Identity::ProviderObjectId(value))
        | Some(axon::common::v1::canonical_resource_ref::Identity::CanonicalLocator(value)) => {
            value
        }
        None => return Err("canonical resource identity is required"),
    };
    if identity.trim().is_empty() {
        return Err("canonical resource identity is required");
    }
    Ok(())
}

fn validate_resolved_browser_read(
    read: &axon::dataaccess::v1::ResolvedBrowserRead,
) -> Result<(), &'static str> {
    let resource = read
        .resource
        .as_option()
        .ok_or("browser_read resource is required")?;
    validate_canonical_resource(resource)?;

    let descriptor = read
        .descriptor
        .as_option()
        .ok_or("browser_read descriptor is required")?;
    match descriptor.descriptor.as_ref() {
        Some(axon::dataaccess::v1::browser_read_descriptor::Descriptor::Snapshot(snapshot)) => {
            if snapshot.table_uri.trim().is_empty() {
                return Err("browser_read descriptor table_uri is required");
            }
            validate_capability_report(snapshot.browser_compatibility.as_option())?;
            validate_capability_report(snapshot.required_capabilities.as_option())?;
        }
        Some(axon::dataaccess::v1::browser_read_descriptor::Descriptor::ParquetDataset(
            dataset,
        )) => {
            if dataset.table_uri.trim().is_empty() {
                return Err("browser_read descriptor table_uri is required");
            }
            validate_capability_report(dataset.browser_compatibility.as_option())?;
            validate_capability_report(dataset.required_capabilities.as_option())?;
        }
        None => return Err("browser_read descriptor arm is required"),
    }

    let access_class = read
        .access_class
        .as_known()
        .filter(|value| *value != axon::dataaccess::v1::BrowserAccessClass::Unspecified)
        .ok_or("browser_read access_class is required")?;
    if access_class != axon::dataaccess::v1::BrowserAccessClass::Public
        && read.not_after.as_option().is_none()
    {
        return Err("capability-bearing browser access requires not_after");
    }
    if read.correlation_id.trim().is_empty() {
        return Err("browser_read correlation_id is required");
    }
    let provenance = read
        .provenance
        .as_option()
        .ok_or("browser_read provenance is required")?;
    if provenance.resolver_id.trim().is_empty() {
        return Err("browser_read provenance resolver_id is required");
    }
    if provenance.resolution_id.trim().is_empty() {
        return Err("browser_read provenance resolution_id is required");
    }
    Ok(())
}

fn validate_capability_report(
    report: Option<&axon::dataaccess::v1::CapabilityReport>,
) -> Result<(), &'static str> {
    let Some(report) = report else {
        return Ok(());
    };
    let mut keys = std::collections::HashSet::new();
    for capability in &report.capabilities {
        let key = capability
            .key
            .as_known()
            .filter(|value| *value != axon::dataaccess::v1::CapabilityKey::Unspecified)
            .ok_or("unspecified capability key")?;
        capability
            .state
            .as_known()
            .filter(|value| *value != axon::dataaccess::v1::CapabilityState::Unspecified)
            .ok_or("unspecified capability state")?;
        if !keys.insert(key) {
            return Err("duplicate capability key");
        }
    }
    Ok(())
}

fn validate_execute_request(request: &axon_exec_v1::ExecuteRequest) -> Result<(), &'static str> {
    if request.execution_id.trim().is_empty() {
        return Err("execution_id is required");
    }
    match request.binding.as_ref() {
        Some(axon_exec_v1::execute_request::Binding::BrowserRead(read)) => {
            validate_resolved_browser_read(read)?;
        }
        Some(axon_exec_v1::execute_request::Binding::LogicalResource(resource)) => {
            validate_canonical_resource(resource)?;
        }
        None => return Err("execution binding is required"),
    }
    let query = request
        .query
        .as_option()
        .ok_or("execution query is required")?;
    if query.sql.trim().is_empty() {
        return Err("execution SQL is required");
    }
    if query.preferred_target.as_known().is_none()
        || query.preferred_target == axon_exec_v1::ExecutionTarget::Unspecified
    {
        return Err("execution target is required");
    }
    let limits = query
        .options
        .as_option()
        .and_then(|options| options.runtime_limits.as_option())
        .ok_or("execution budgets are required")?;
    if [
        limits.max_result_rows,
        limits.max_arrow_ipc_bytes,
        limits.max_preview_string_bytes,
        limits.max_scan_bytes,
    ]
    .iter()
    .any(|value| value.is_none_or(|value| value == 0))
    {
        return Err("execution budgets must be positive");
    }
    if request.deadline.as_option().is_none() {
        return Err("execution deadline is required");
    }
    Ok(())
}

fn validate_execute_response_order(
    responses: &[axon_exec_v1::ExecuteResponse],
) -> Result<(), &'static str> {
    let mut admission_seen = false;
    let mut rejected = false;
    let mut terminal_seen = false;

    for response in responses {
        let item = response
            .item
            .as_ref()
            .ok_or("execution response item is required")?;
        match item {
            axon_exec_v1::execute_response::Item::Admission(admission) => {
                if admission_seen || terminal_seen {
                    return Err("duplicate or late admission");
                }
                rejected = match admission.outcome.as_ref() {
                    Some(axon_exec_v1::execution_admission::Outcome::Accepted(_)) => false,
                    Some(axon_exec_v1::execution_admission::Outcome::Rejected(_)) => true,
                    None => return Err("admission outcome is required"),
                };
                admission_seen = true;
            }
            axon_exec_v1::execute_response::Item::Event(_) => {
                if !admission_seen {
                    return Err("execution response preceded admission");
                }
                if rejected {
                    return Err("event after rejected admission");
                }
                if terminal_seen {
                    return Err("event after terminal");
                }
            }
            axon_exec_v1::execute_response::Item::Terminal(_) => {
                if !admission_seen {
                    return Err("execution response preceded admission");
                }
                if rejected {
                    return Err("terminal after rejected admission");
                }
                if terminal_seen {
                    return Err("duplicate terminal");
                }
                terminal_seen = true;
            }
        }
    }
    Ok(())
}
