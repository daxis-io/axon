mod support;

use std::path::PathBuf;

use delta_control_plane::resolve_snapshot;
use query_contract::{QueryErrorCode, SnapshotResolutionRequest};
use support::TestTableFixture;

#[test]
fn resolve_snapshot_returns_latest_descriptor_with_sorted_active_files() {
    let fixture = TestTableFixture::create_partitioned();

    let descriptor = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri.clone(),
        snapshot_version: None,
    })
    .expect("latest snapshot should resolve");

    assert_eq!(descriptor.table_uri, fixture.table_uri);
    assert_eq!(descriptor.snapshot_version, 1);
    assert!(!descriptor.active_files.is_empty());
    assert_eq!(
        descriptor.active_files.len(),
        fixture.data_file_paths().len()
    );

    let actual_paths: Vec<_> = descriptor
        .active_files
        .iter()
        .map(|file| file.path.clone())
        .collect();
    let mut expected_paths = actual_paths.clone();
    expected_paths.sort();

    assert_eq!(actual_paths, expected_paths);
    assert!(descriptor
        .active_files
        .iter()
        .all(|file| file.size_bytes > 0 && file.partition_values.contains_key("category")));
}

#[test]
fn resolve_snapshot_honors_explicit_snapshot_version() {
    let fixture = TestTableFixture::create_multi_version();

    let descriptor = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri,
        snapshot_version: Some(1),
    })
    .expect("historical snapshot should resolve");

    assert_eq!(descriptor.snapshot_version, 1);
    assert_eq!(descriptor.active_files.len(), 1);
}

#[test]
fn resolve_snapshot_rejects_negative_snapshot_versions() {
    let fixture = TestTableFixture::create_multi_version();

    let error = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri,
        snapshot_version: Some(-1),
    })
    .expect_err("negative versions should be rejected");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("snapshot_version"));
}

#[test]
fn resolve_snapshot_rejects_unknown_snapshot_versions() {
    let fixture = TestTableFixture::create_multi_version();

    let error = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri,
        snapshot_version: Some(99),
    })
    .expect_err("unknown versions should fail deterministically");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("snapshot"));
}

#[test]
fn resolve_snapshot_rejects_invalid_table_locations() {
    let missing_table = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("does-not-exist")
        .display()
        .to_string();

    let error = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: missing_table,
        snapshot_version: None,
    })
    .expect_err("invalid table locations should fail");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("table"));
}
