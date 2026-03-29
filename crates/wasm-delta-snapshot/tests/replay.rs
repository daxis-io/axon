#[path = "../../delta-control-plane/tests/support/mod.rs"]
mod support;

use std::collections::BTreeMap;

use delta_control_plane::resolve_snapshot as resolve_control_plane_snapshot;
use query_contract::{ResolvedFileDescriptor, SnapshotResolutionRequest};
use support::TestTableFixture;
use wasm_delta_snapshot::{
    DefaultJsonHandler, DefaultParquetHandler, LocalFileStorageHandler, SnapshotResolver,
};

#[test]
fn reconstructs_active_files_with_paths_sizes_and_partition_values() {
    let fixture = TestTableFixture::create_partitioned();
    let resolver = SnapshotResolver::new(
        LocalFileStorageHandler::default(),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );

    let snapshot = tokio::runtime::Runtime::new()
        .expect("runtime should be created")
        .block_on(async {
            resolver
                .resolve_snapshot(SnapshotResolutionRequest {
                    table_uri: fixture.raw_table_path(),
                    snapshot_version: None,
                })
                .await
        })
        .expect("snapshot should resolve");

    assert_eq!(snapshot.snapshot_version, 1);
    assert_eq!(snapshot.partition_column_types, BTreeMap::new());
    assert_eq!(snapshot.active_files, fixture.expected_active_files(None));
}

#[test]
fn snapshot_reconstruction_matches_control_plane_for_latest_snapshot() {
    let fixture = TestTableFixture::create_partitioned();
    let resolver = SnapshotResolver::new(
        LocalFileStorageHandler::default(),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );

    let browser_snapshot = tokio::runtime::Runtime::new()
        .expect("runtime should be created")
        .block_on(async {
            resolver
                .resolve_snapshot(SnapshotResolutionRequest {
                    table_uri: fixture.raw_table_path(),
                    snapshot_version: None,
                })
                .await
        })
        .expect("browser snapshot should resolve");
    let native_snapshot = resolve_control_plane_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri.clone(),
        snapshot_version: None,
    })
    .expect("native snapshot should resolve");

    assert_eq!(
        browser_snapshot.snapshot_version,
        native_snapshot.snapshot_version
    );
    assert_eq!(browser_snapshot.active_files, native_snapshot.active_files);
}

#[test]
fn snapshot_reconstruction_matches_control_plane_for_historical_snapshot() {
    let fixture = TestTableFixture::create_multi_version();
    let resolver = SnapshotResolver::new(
        LocalFileStorageHandler::default(),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );

    let browser_snapshot = tokio::runtime::Runtime::new()
        .expect("runtime should be created")
        .block_on(async {
            resolver
                .resolve_snapshot(SnapshotResolutionRequest {
                    table_uri: fixture.raw_table_path(),
                    snapshot_version: Some(1),
                })
                .await
        })
        .expect("browser snapshot should resolve");
    let native_snapshot = resolve_control_plane_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri.clone(),
        snapshot_version: Some(1),
    })
    .expect("native snapshot should resolve");

    assert_eq!(
        browser_snapshot.snapshot_version,
        native_snapshot.snapshot_version
    );
    assert_eq!(browser_snapshot.active_files, native_snapshot.active_files);
}

#[test]
fn expected_descriptor_shape_matches_latest_fixture_projection() {
    let fixture = TestTableFixture::create_partitioned();

    assert_eq!(
        fixture.expected_active_files(None),
        fixture
            .expected_active_files(None)
            .into_iter()
            .collect::<Vec<ResolvedFileDescriptor>>()
    );
}
