mod support;

use delta_control_plane::resolve_snapshot;
use deltalake::arrow::array::{Int64Array, UInt64Array};
use native_query_runtime::{execute_query, DEFAULT_TABLE_NAME};
use query_contract::{ExecutionTarget, QueryErrorCode, QueryRequest, SnapshotResolutionRequest};
use support::{LoopbackObjectServer, TestTableFixture};
use wasm_query_runtime::{
    BrowserObjectSource, BrowserRuntimeConfig, BrowserRuntimeSession, MaterializedBrowserFile,
    MaterializedBrowserSnapshot,
};

#[test]
fn latest_resolved_snapshot_bootstraps_browser_preflight_and_matches_native_row_count() {
    let fixture = TestTableFixture::create_partitioned();
    let resolved_snapshot = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri.clone(),
        snapshot_version: None,
    })
    .expect("latest snapshot should resolve");
    let server = LoopbackObjectServer::from_fixture_paths(
        &fixture,
        resolved_snapshot
            .active_files
            .iter()
            .map(|file| file.path.clone()),
    );
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default browser runtime config should be supported");
    let materialized = materialize_loopback_snapshot(&resolved_snapshot, &server);

    let bootstrapped = runtime()
        .block_on(session.bootstrap_snapshot_metadata(&materialized))
        .expect("browser snapshot metadata bootstrap should succeed");
    let summary = bootstrapped
        .summarize()
        .expect("uniform fixture schemas should summarize");
    let native_row_count = query_row_count(
        &resolved_snapshot.table_uri,
        resolved_snapshot.snapshot_version,
    );

    assert_eq!(summary.table_uri, resolved_snapshot.table_uri);
    assert_eq!(summary.snapshot_version, resolved_snapshot.snapshot_version);
    assert_eq!(
        summary.file_count,
        resolved_snapshot.active_files.len() as u64
    );
    assert_eq!(
        summary.total_bytes,
        resolved_snapshot
            .active_files
            .iter()
            .map(|file| file.size_bytes)
            .sum::<u64>()
    );
    assert_eq!(summary.total_rows, native_row_count);
    assert_eq!(summary.total_row_groups, 3);
    assert_eq!(
        summary.schema.fields,
        vec![
            wasm_query_runtime::BrowserParquetField {
                name: "id".to_string(),
                physical_type: "INT32".to_string(),
                logical_type: None,
                converted_type: None,
                repetition: "REQUIRED".to_string(),
                nullable: false,
                max_definition_level: 0,
                max_repetition_level: 0,
                type_length: None,
                precision: None,
                scale: None,
            },
            wasm_query_runtime::BrowserParquetField {
                name: "value".to_string(),
                physical_type: "INT32".to_string(),
                logical_type: None,
                converted_type: None,
                repetition: "REQUIRED".to_string(),
                nullable: false,
                max_definition_level: 0,
                max_repetition_level: 0,
                type_length: None,
                precision: None,
                scale: None,
            },
        ]
    );
    assert_eq!(
        summary.schema.partition_columns,
        vec!["category".to_string()]
    );

    let first_file = &bootstrapped.active_files[0];
    assert_eq!(
        first_file.partition_values.get("category"),
        Some(&Some("A".to_string()))
    );
    assert_eq!(first_file.metadata.row_group_count, 1);
    assert_eq!(first_file.metadata.row_count, 2);
}

#[test]
fn historical_resolved_snapshot_bootstraps_browser_preflight_and_matches_native_row_count() {
    let fixture = TestTableFixture::create_multi_version();
    let resolved_snapshot = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri.clone(),
        snapshot_version: Some(1),
    })
    .expect("historical snapshot should resolve");
    let server = LoopbackObjectServer::from_fixture_paths(
        &fixture,
        resolved_snapshot
            .active_files
            .iter()
            .map(|file| file.path.clone()),
    );
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default browser runtime config should be supported");
    let materialized = materialize_loopback_snapshot(&resolved_snapshot, &server);

    let bootstrapped = runtime()
        .block_on(session.bootstrap_snapshot_metadata(&materialized))
        .expect("historical browser snapshot metadata bootstrap should succeed");
    let summary = bootstrapped
        .summarize()
        .expect("uniform historical fixture schemas should summarize");
    let native_row_count = query_row_count(
        &resolved_snapshot.table_uri,
        resolved_snapshot.snapshot_version,
    );

    assert_eq!(summary.snapshot_version, 1);
    assert_eq!(summary.file_count, 1);
    assert_eq!(summary.total_rows, native_row_count);
    assert_eq!(summary.total_rows, 3);
    assert_eq!(summary.total_row_groups, 1);
    assert_eq!(
        summary.schema.fields,
        vec![
            wasm_query_runtime::BrowserParquetField {
                name: "id".to_string(),
                physical_type: "INT32".to_string(),
                logical_type: None,
                converted_type: None,
                repetition: "REQUIRED".to_string(),
                nullable: false,
                max_definition_level: 0,
                max_repetition_level: 0,
                type_length: None,
                precision: None,
                scale: None,
            },
            wasm_query_runtime::BrowserParquetField {
                name: "category".to_string(),
                physical_type: "BYTE_ARRAY".to_string(),
                logical_type: Some("String".to_string()),
                converted_type: Some("UTF8".to_string()),
                repetition: "REQUIRED".to_string(),
                nullable: false,
                max_definition_level: 0,
                max_repetition_level: 0,
                type_length: None,
                precision: None,
                scale: None,
            },
            wasm_query_runtime::BrowserParquetField {
                name: "value".to_string(),
                physical_type: "INT32".to_string(),
                logical_type: None,
                converted_type: None,
                repetition: "REQUIRED".to_string(),
                nullable: false,
                max_definition_level: 0,
                max_repetition_level: 0,
                type_length: None,
                precision: None,
                scale: None,
            },
        ]
    );
    assert!(summary.schema.partition_columns.is_empty());
    assert!(bootstrapped.active_files[0].partition_values.is_empty());
}

#[test]
fn bootstrap_snapshot_metadata_rejects_descriptor_size_mismatches_for_real_fixture_files() {
    let fixture = TestTableFixture::create_partitioned();
    let resolved_snapshot = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri.clone(),
        snapshot_version: None,
    })
    .expect("latest snapshot should resolve");
    let server = LoopbackObjectServer::from_fixture_paths(
        &fixture,
        resolved_snapshot
            .active_files
            .iter()
            .map(|file| file.path.clone()),
    );
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default browser runtime config should be supported");
    let mut files = resolved_snapshot
        .active_files
        .iter()
        .map(|file| {
            MaterializedBrowserFile::new(
                file.path.clone(),
                file.size_bytes,
                file.partition_values.clone(),
                BrowserObjectSource::from_url(server.url_for_path(&file.path))
                    .expect("loopback HTTP should be allowed in host tests"),
            )
        })
        .collect::<Vec<_>>();
    files[0] = MaterializedBrowserFile::new(
        resolved_snapshot.active_files[0].path.clone(),
        resolved_snapshot.active_files[0].size_bytes + 1,
        resolved_snapshot.active_files[0].partition_values.clone(),
        BrowserObjectSource::from_url(server.url_for_path(&resolved_snapshot.active_files[0].path))
            .expect("loopback HTTP should be allowed in host tests"),
    );
    let materialized = MaterializedBrowserSnapshot::new(
        resolved_snapshot.table_uri,
        resolved_snapshot.snapshot_version,
        files,
    )
    .expect("duplicate-free snapshots should construct");

    let error = runtime()
        .block_on(session.bootstrap_snapshot_metadata(&materialized))
        .expect_err("descriptor size mismatches should fail");

    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert!(error
        .message
        .contains(materialized.active_files()[0].path()));
}

fn materialize_loopback_snapshot(
    resolved_snapshot: &query_contract::ResolvedSnapshotDescriptor,
    server: &LoopbackObjectServer,
) -> MaterializedBrowserSnapshot {
    let files = resolved_snapshot
        .active_files
        .iter()
        .map(|file| {
            MaterializedBrowserFile::new(
                file.path.clone(),
                file.size_bytes,
                file.partition_values.clone(),
                BrowserObjectSource::from_url(server.url_for_path(&file.path))
                    .expect("loopback HTTP should be allowed in host tests"),
            )
        })
        .collect::<Vec<_>>();

    MaterializedBrowserSnapshot::new(
        resolved_snapshot.table_uri.clone(),
        resolved_snapshot.snapshot_version,
        files,
    )
    .expect("duplicate-free snapshots should construct")
}

fn query_row_count(table_uri: &str, snapshot_version: i64) -> u64 {
    let result = execute_query(QueryRequest {
        snapshot_version: Some(snapshot_version),
        ..QueryRequest::new(
            table_uri.to_string(),
            format!("SELECT COUNT(*) AS row_count FROM {DEFAULT_TABLE_NAME}"),
            ExecutionTarget::Native,
        )
    })
    .expect("native query should execute");
    let batch = result
        .batches
        .first()
        .expect("native query should return one batch");
    let column = batch.column(0);

    if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
        return u64::try_from(values.value(0)).expect("row counts should be non-negative");
    }

    if let Some(values) = column.as_any().downcast_ref::<UInt64Array>() {
        return values.value(0);
    }

    panic!("COUNT(*) should return an Int64Array or UInt64Array");
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("tokio runtime should be created")
}
