use std::path::PathBuf;

use query_contract::{
    BrowserHttpSnapshotDescriptor, CapabilityKey, CapabilityState, ExecutionTarget,
    SnapshotResolutionRequest,
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
