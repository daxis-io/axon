#[path = "../../delta-control-plane/tests/support/mod.rs"]
mod support;

use delta_control_plane::resolve_snapshot as resolve_control_plane_snapshot;
use deltalake::kernel::{Action, DataType, PrimitiveType, Protocol, StructField};
use deltalake::DeltaTable;
use query_contract::{CapabilityReport, ResolvedFileDescriptor, SnapshotResolutionRequest};
use serde_json::json;
use support::TestTableFixture;
use tempfile::TempDir;
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
    let native_snapshot = resolve_control_plane_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri.clone(),
        snapshot_version: None,
    })
    .expect("native snapshot should resolve");

    assert_eq!(snapshot.snapshot_version, 1);
    assert_eq!(
        snapshot.partition_column_types,
        native_snapshot.partition_column_types
    );
    assert_eq!(
        snapshot.required_capabilities,
        native_snapshot.required_capabilities
    );
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
    assert_eq!(
        browser_snapshot.partition_column_types,
        native_snapshot.partition_column_types
    );
    assert_eq!(
        browser_snapshot.required_capabilities,
        native_snapshot.required_capabilities
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
    assert_eq!(
        browser_snapshot.partition_column_types,
        native_snapshot.partition_column_types
    );
    assert_eq!(
        browser_snapshot.required_capabilities,
        native_snapshot.required_capabilities
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

#[test]
fn snapshot_reconstruction_matches_control_plane_for_protocol_only_deletion_vector_tables() {
    let fixture = TempDir::new().expect("tempdir should be created");
    let table_uri = deltalake::ensure_table_uri(fixture.path().to_string_lossy())
        .expect("table uri should normalize")
        .to_string();
    tokio::runtime::Runtime::new()
        .expect("runtime should be created")
        .block_on(async {
            let table = DeltaTable::try_from_url(
                deltalake::ensure_table_uri(&table_uri).expect("table uri should parse"),
            )
            .await
            .expect("table handle should be created");

            table
                .create()
                .with_columns(vec![StructField::new(
                    "id".to_string(),
                    DataType::Primitive(PrimitiveType::Integer),
                    false,
                )])
                .with_table_name("axon_fixture")
                .with_actions(vec![deletion_vector_protocol_action()])
                .await
                .expect("table should be created");
        });

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
                    table_uri: fixture.path().display().to_string(),
                    snapshot_version: None,
                })
                .await
        })
        .expect("snapshot should resolve");
    let native_snapshot = resolve_control_plane_snapshot(SnapshotResolutionRequest {
        table_uri,
        snapshot_version: None,
    })
    .expect("native snapshot should resolve");

    assert!(snapshot.active_files.is_empty());
    assert_eq!(snapshot.required_capabilities, CapabilityReport::default());
    assert_eq!(
        snapshot.required_capabilities,
        native_snapshot.required_capabilities
    );
}

fn deletion_vector_protocol_action() -> Action {
    let protocol: Protocol = serde_json::from_value(json!({
        "minReaderVersion": 3,
        "minWriterVersion": 7,
        "readerFeatures": ["deletionVectors"],
        "writerFeatures": ["deletionVectors"],
    }))
    .expect("protocol should deserialize");

    Action::Protocol(protocol)
}
