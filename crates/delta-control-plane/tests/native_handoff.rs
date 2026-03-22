mod support;

use delta_control_plane::resolve_snapshot;
use deltalake::arrow::util::pretty::pretty_format_batches;
use native_query_runtime::{bootstrap_table, execute_query, DEFAULT_TABLE_NAME};
use query_contract::{ExecutionTarget, QueryErrorCode, QueryRequest, SnapshotResolutionRequest};
use support::TestTableFixture;

#[test]
fn resolved_latest_snapshot_descriptor_matches_fixture_files_and_drives_native_query_execution() {
    let fixture = TestTableFixture::create_partitioned();

    let descriptor = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri.clone(),
        snapshot_version: None,
    })
    .expect("latest snapshot should resolve");
    let bootstrap =
        bootstrap_table(&descriptor.table_uri).expect("native bootstrap should load latest");

    assert_eq!(descriptor.snapshot_version, bootstrap.version);
    assert_eq!(descriptor.active_files.len() as u64, bootstrap.active_files);
    assert_eq!(descriptor.active_files, fixture.expected_active_files(None));

    let result = execute_query(QueryRequest {
        snapshot_version: Some(descriptor.snapshot_version),
        ..QueryRequest::new(
            descriptor.table_uri,
            format!("SELECT COUNT(*) AS row_count FROM {DEFAULT_TABLE_NAME}"),
            ExecutionTarget::Native,
        )
    })
    .expect("native query should execute with the resolved latest snapshot");

    assert_eq!(
        pretty_format_batches(&result.batches)
            .expect("batches should format")
            .to_string(),
        "+-----------+\n| row_count |\n+-----------+\n| 6         |\n+-----------+"
    );
}

#[test]
fn resolved_historical_snapshot_descriptor_matches_fixture_files_and_drives_native_query_execution()
{
    let fixture = TestTableFixture::create_multi_version();

    let descriptor = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri.clone(),
        snapshot_version: Some(1),
    })
    .expect("historical snapshot should resolve");
    assert_eq!(
        descriptor.active_files,
        fixture.expected_active_files(Some(1))
    );

    let result = execute_query(QueryRequest {
        snapshot_version: Some(descriptor.snapshot_version),
        ..QueryRequest::new(
            descriptor.table_uri,
            format!("SELECT COUNT(*) AS row_count FROM {DEFAULT_TABLE_NAME}"),
            ExecutionTarget::Native,
        )
    })
    .expect("native query should execute with the resolved historical snapshot");

    assert_eq!(descriptor.snapshot_version, 1);
    assert_eq!(descriptor.active_files.len(), 1);
    assert!(descriptor
        .active_files
        .iter()
        .all(|file| file.partition_values.is_empty()));
    assert_eq!(
        pretty_format_batches(&result.batches)
            .expect("batches should format")
            .to_string(),
        "+-----------+\n| row_count |\n+-----------+\n| 3         |\n+-----------+"
    );
}

#[test]
fn unavailable_snapshot_versions_map_to_invalid_request_in_control_plane_and_native_runtime() {
    let fixture = TestTableFixture::create_multi_version();

    let control_plane_error = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri.clone(),
        snapshot_version: Some(99),
    })
    .expect_err("control-plane should reject unavailable snapshots");
    let native_error = execute_query(QueryRequest {
        snapshot_version: Some(99),
        ..QueryRequest::new(
            fixture.table_uri,
            format!("SELECT COUNT(*) AS row_count FROM {DEFAULT_TABLE_NAME}"),
            ExecutionTarget::Native,
        )
    })
    .expect_err("native runtime should reject unavailable snapshots");

    assert_eq!(control_plane_error.code, QueryErrorCode::InvalidRequest);
    assert_eq!(native_error.code, QueryErrorCode::InvalidRequest);
}
