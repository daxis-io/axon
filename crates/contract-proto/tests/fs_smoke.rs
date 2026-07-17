use axon_contract_proto::{axon, axon_fs_v1};
use buffa::{Message, MessageName};
use buffa_types::google::protobuf::Timestamp;

#[test]
fn filesystem_messages_are_exported_through_nested_and_compatibility_modules() {
    assert_eq!(
        <axon::fs::v1::FsRootRef as MessageName>::FULL_NAME,
        "axon.fs.v1.FsRootRef"
    );
    assert_eq!(
        <axon_fs_v1::FsEntry as MessageName>::FULL_NAME,
        "axon.fs.v1.FsEntry"
    );
    assert_eq!(
        <axon_fs_v1::ObjectReadResolution as MessageName>::FULL_NAME,
        "axon.fs.v1.ObjectReadResolution"
    );
}

#[test]
fn entries_pagination_and_timestamp_survive_binary_round_trips() {
    let root = axon_fs_v1::FsRootRef {
        provider_id: "provider-1".into(),
        root_id: "root-opaque-1".into(),
        backend_kind: axon_fs_v1::FsBackendKind::ObjectStorePrefix.into(),
        ..Default::default()
    };
    let file = axon_fs_v1::FsEntry {
        name: "empty.parquet".into(),
        path: "reports/empty.parquet".into(),
        kind: axon_fs_v1::FsEntryKind::File.into(),
        size_bytes: Some(0),
        modified_at: Timestamp {
            seconds: 1_752_589_800,
            nanos: 123_000_000,
            ..Default::default()
        }
        .into(),
        content_type: Some("application/vnd.apache.parquet".into()),
        ..Default::default()
    };
    let directory = axon_fs_v1::FsEntry {
        name: "archive".into(),
        path: "reports/archive".into(),
        kind: axon_fs_v1::FsEntryKind::Directory.into(),
        ..Default::default()
    };
    let request = axon_fs_v1::ListDirectoryRequest {
        root: root.into(),
        path: "reports".into(),
        page: axon::common::v1::PageRequest {
            cursor: "opaque-page-1".into(),
            page_size: 25,
            ..Default::default()
        }
        .into(),
        ..Default::default()
    };
    let response = axon_fs_v1::ListDirectoryResponse {
        entries: vec![file.clone(), directory],
        page: axon::common::v1::PageInfo {
            next_cursor: Some("opaque-page-2".into()),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    };

    let decoded_request =
        axon_fs_v1::ListDirectoryRequest::decode_from_slice(&request.encode_to_vec())
            .expect("list request should decode");
    let decoded_response =
        axon_fs_v1::ListDirectoryResponse::decode_from_slice(&response.encode_to_vec())
            .expect("list response should decode");

    assert_eq!(decoded_request.path, "reports");
    let decoded_page = decoded_request
        .page
        .as_option()
        .expect("request page should be present");
    assert_eq!(decoded_page.cursor, "opaque-page-1");
    assert_eq!(decoded_page.page_size, 25);

    assert_eq!(decoded_response.entries.len(), 2);
    assert_eq!(
        decoded_response.entries[0].kind,
        axon_fs_v1::FsEntryKind::File
    );
    assert_eq!(decoded_response.entries[0].size_bytes, Some(0));
    let modified_at = decoded_response.entries[0]
        .modified_at
        .as_option()
        .expect("file timestamp should be present");
    assert_eq!(modified_at.seconds, 1_752_589_800);
    assert_eq!(modified_at.nanos, 123_000_000);
    assert_eq!(
        decoded_response.entries[1].kind,
        axon_fs_v1::FsEntryKind::Directory
    );
    assert_eq!(decoded_response.entries[1].size_bytes, None);
    let decoded_page = decoded_response
        .page
        .as_option()
        .expect("response page should be present");
    assert_eq!(decoded_page.next_cursor.as_deref(), Some("opaque-page-2"));

    let stat = axon_fs_v1::StatResponse {
        entry: file.into(),
        ..Default::default()
    };
    let decoded_stat = axon_fs_v1::StatResponse::decode_from_slice(&stat.encode_to_vec())
        .expect("stat response should decode");
    assert_eq!(
        decoded_stat
            .entry
            .as_option()
            .expect("stat entry should be present")
            .path,
        "reports/empty.parquet"
    );
}

#[test]
fn read_requests_and_all_resolution_arms_survive_binary_round_trips() {
    let request = axon_fs_v1::ResolveReadRequest {
        root: axon_fs_v1::FsRootRef {
            provider_id: "brokered-files".into(),
            root_id: "volume-1".into(),
            backend_kind: axon_fs_v1::FsBackendKind::UnityCatalogVolume.into(),
            ..Default::default()
        }
        .into(),
        path: "reports/summary.pdf".into(),
        start: Some(0),
        end: Some(16),
        ..Default::default()
    };
    let decoded_request =
        axon_fs_v1::ResolveReadRequest::decode_from_slice(&request.encode_to_vec())
            .expect("read request should decode");
    assert_eq!(decoded_request.start, Some(0));
    assert_eq!(decoded_request.end, Some(16));

    let signed = axon_fs_v1::ObjectReadResolution {
        resolution: axon::dataaccess::v1::ObjectGrantSignedUrl {
            path: "reports/summary.pdf".into(),
            url: "https://signed.example.test/summary.pdf?sig=redacted".into(),
            expires_at_epoch_ms: 1_800_000_000_000,
            ..Default::default()
        }
        .into(),
        ..Default::default()
    };
    let decoded_signed =
        axon_fs_v1::ObjectReadResolution::decode_from_slice(&signed.encode_to_vec())
            .expect("signed resolution should decode");
    match decoded_signed.resolution {
        Some(axon_fs_v1::object_read_resolution::Resolution::SignedUrl(grant)) => {
            assert_eq!(grant.path, "reports/summary.pdf");
            assert_eq!(grant.expires_at_epoch_ms, 1_800_000_000_000);
        }
        other => panic!("expected signed URL resolution, got {other:?}"),
    }

    let proxy = axon_fs_v1::ObjectReadResolution {
        resolution: axon::dataaccess::v1::ObjectGrantRangeRequest {
            path: "reports/summary.pdf".into(),
            start: Some(0),
            end: Some(16),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    };
    let decoded_proxy = axon_fs_v1::ObjectReadResolution::decode_from_slice(&proxy.encode_to_vec())
        .expect("proxy resolution should decode");
    match decoded_proxy.resolution {
        Some(axon_fs_v1::object_read_resolution::Resolution::ProxyRange(range)) => {
            assert_eq!(range.start, Some(0));
            assert_eq!(range.end, Some(16));
        }
        other => panic!("expected proxy range resolution, got {other:?}"),
    }

    let denied = axon_fs_v1::ObjectReadResolution {
        resolution: axon::common::v1::ProviderError {
            code: axon::common::v1::ProviderErrorCode::Invalid.into(),
            message: "read ranges require both start and end".into(),
            correlation_id: "corr-fs-123".into(),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    };
    let decoded_denied =
        axon_fs_v1::ObjectReadResolution::decode_from_slice(&denied.encode_to_vec())
            .expect("denied resolution should decode");
    match decoded_denied.resolution {
        Some(axon_fs_v1::object_read_resolution::Resolution::Denied(error)) => {
            assert_eq!(error.code, axon::common::v1::ProviderErrorCode::Invalid);
            assert_eq!(error.message, "read ranges require both start and end");
            assert_eq!(error.correlation_id, "corr-fs-123");
        }
        other => panic!("expected denied resolution, got {other:?}"),
    }
}
