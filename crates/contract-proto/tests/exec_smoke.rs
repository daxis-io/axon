use axon_contract_proto::{axon, axon_exec_v1};
use buffa::{Message, MessageName};

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
}

#[test]
fn worker_and_service_oneofs_construct_and_round_trip() {
    let command = axon_exec_v1::BrowserWorkerCommand {
        command: axon_exec_v1::BrowserWorkerCancelCommand {
            request_id: "cancel-control-001".into(),
            query_id: Some("query-001".into()),
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
            assert_eq!(cancel.query_id.as_deref(), Some("query-001"));
        }
        other => panic!("expected cancel command, got {other:?}"),
    }

    let terminal_response = axon_exec_v1::BrowserWorkerResponseEnvelope {
        response: axon_exec_v1::BrowserWorkerOpenedEnvelope {
            request_id: "open-control-001".into(),
            name: "events".into(),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    };
    let stream_item = axon_exec_v1::ExecuteResponse {
        item: terminal_response.into(),
        ..Default::default()
    };
    let decoded_item =
        axon_exec_v1::ExecuteResponse::decode_from_slice(&stream_item.encode_to_vec())
            .expect("execute response should decode");

    assert!(matches!(
        decoded_item.item,
        Some(axon_exec_v1::execute_response::Item::Response(_))
    ));
}

#[test]
fn arrow_bytes_and_explicit_zero_presence_survive_binary_round_trips() {
    let arrow = axon_exec_v1::ArrowIpcResult {
        format: axon_exec_v1::ArrowIpcFormat::Stream.into(),
        content_type: "application/vnd.apache.arrow.stream".into(),
        delivery: Some(axon_exec_v1::ArrowIpcDelivery::SingleBuffer.into()),
        bytes: Some(vec![1, 2, 3, 4]),
        byte_length: Some(4),
        chunk_count: Some(1),
        ..Default::default()
    };
    let decoded_arrow = axon_exec_v1::ArrowIpcResult::decode_from_slice(&arrow.encode_to_vec())
        .expect("Arrow IPC envelope should decode");

    assert_eq!(
        decoded_arrow.bytes.as_deref(),
        Some([1, 2, 3, 4].as_slice())
    );
    assert_eq!(decoded_arrow.byte_length, Some(4));
    assert_eq!(decoded_arrow.chunk_count, Some(1));

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
