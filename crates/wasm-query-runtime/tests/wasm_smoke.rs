#![cfg(target_arch = "wasm32")]

use query_contract::{
    BrowserHttpFileDescriptor, BrowserHttpSnapshotDescriptor, ExecutionTarget, QueryErrorCode,
    QueryRequest,
};
use wasm_bindgen_test::wasm_bindgen_test;
use wasm_query_runtime::{
    runtime_target, BootstrappedBrowserFile, BootstrappedBrowserSnapshot, BrowserObjectSource,
    BrowserParquetField, BrowserParquetFileMetadata, BrowserParquetPhysicalType,
    BrowserParquetRepetition, BrowserRuntimeConfig, BrowserRuntimeSession, MaterializedBrowserFile,
    MaterializedBrowserSnapshot,
};

#[wasm_bindgen_test]
fn browser_runtime_session_and_object_source_construct_in_wasm() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported in wasm");
    let source = BrowserObjectSource::from_url("https://example.com/object")
        .expect("https object sources should be supported in wasm");

    assert_eq!(runtime_target(), ExecutionTarget::BrowserWasm);
    assert_eq!(session.config(), &BrowserRuntimeConfig::default());
    assert_eq!(source.url(), "https://example.com/object");
}

#[wasm_bindgen_test]
fn plain_http_sources_are_rejected_in_wasm() {
    let error = BrowserObjectSource::from_url("http://127.0.0.1:8080/object")
        .expect_err("plain HTTP should be rejected in wasm");

    assert_eq!(error.code, QueryErrorCode::SecurityPolicyViolation);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[wasm_bindgen_test]
fn parquet_footer_api_surface_constructs_a_future_in_wasm() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported in wasm");
    let source = BrowserObjectSource::from_url("https://example.com/object")
        .expect("https object sources should be supported in wasm");

    let footer_read = session.read_parquet_footer(&source);
    drop(footer_read);
}

#[wasm_bindgen_test]
fn preflight_api_surface_constructs_futures_and_summaries_in_wasm() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported in wasm");
    let source = BrowserObjectSource::from_url("https://example.com/object")
        .expect("https object sources should be supported in wasm");
    let file = MaterializedBrowserFile::new(
        "part-000.parquet",
        128,
        std::collections::BTreeMap::new(),
        source,
    );
    let snapshot =
        MaterializedBrowserSnapshot::new("gs://axon-fixtures/sample_table", 4, vec![file.clone()])
            .expect("duplicate-free snapshots should construct in wasm");
    let metadata_read = session.read_parquet_metadata_for_file(&file);
    let snapshot_bootstrap = session.bootstrap_snapshot_metadata(&snapshot);
    let summarized = BootstrappedBrowserSnapshot::new(
        snapshot.table_uri(),
        snapshot.snapshot_version(),
        vec![BootstrappedBrowserFile::new(
            file.path(),
            file.size_bytes(),
            file.partition_values().clone(),
            BrowserParquetFileMetadata {
                object_size_bytes: 128,
                footer_length_bytes: 16,
                row_group_count: 0,
                row_count: 0,
                fields: vec![BrowserParquetField {
                    name: "id".to_string(),
                    physical_type: BrowserParquetPhysicalType::Int32,
                    logical_type: None,
                    converted_type: None,
                    repetition: BrowserParquetRepetition::Required,
                    nullable: false,
                    max_definition_level: 0,
                    max_repetition_level: 0,
                    type_length: None,
                    precision: None,
                    scale: None,
                }],
                field_stats: std::collections::BTreeMap::new(),
            },
        )
        .expect("valid bootstrapped files should construct in wasm")],
    )
    .expect("duplicate-free snapshots should construct in wasm")
    .summarize()
    .expect("uniform synthetic snapshots should summarize in wasm");

    drop(metadata_read);
    drop(snapshot_bootstrap);
    assert_eq!(summarized.file_count, 1);
    assert!(summarized.schema.partition_columns.is_empty());
}

#[wasm_bindgen_test]
fn browser_runtime_materializes_https_descriptors_in_wasm() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported in wasm");
    let descriptor = BrowserHttpSnapshotDescriptor {
        table_uri: "gs://axon-fixtures/sample_table".to_string(),
        snapshot_version: 4,
        active_files: vec![BrowserHttpFileDescriptor {
            path: "part-000.parquet".to_string(),
            url: "https://example.com/object".to_string(),
            size_bytes: 128,
            partition_values: std::collections::BTreeMap::new(),
        }],
    };

    let materialized = session
        .materialize_snapshot(&descriptor)
        .expect("https descriptors should materialize in wasm");

    assert_eq!(materialized.table_uri(), descriptor.table_uri);
    assert_eq!(materialized.snapshot_version(), descriptor.snapshot_version);
    assert_eq!(materialized.active_files().len(), 1);
    assert_eq!(
        materialized.active_files()[0].object_source().url(),
        descriptor.active_files[0].url
    );
}

#[wasm_bindgen_test]
fn browser_runtime_analysis_and_planning_apis_construct_in_wasm() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported in wasm");
    let shape = session
        .analyze_query_shape("SELECT id FROM axon_table ORDER BY id LIMIT 1")
        .expect("supported browser queries should analyze in wasm");
    let snapshot = BootstrappedBrowserSnapshot::new(
        "gs://axon-fixtures/sample_table",
        4,
        vec![BootstrappedBrowserFile::new(
            "part-000.parquet",
            128,
            std::collections::BTreeMap::new(),
            BrowserParquetFileMetadata {
                object_size_bytes: 128,
                footer_length_bytes: 16,
                row_group_count: 0,
                row_count: 0,
                fields: vec![BrowserParquetField {
                    name: "id".to_string(),
                    physical_type: BrowserParquetPhysicalType::Int32,
                    logical_type: None,
                    converted_type: None,
                    repetition: BrowserParquetRepetition::Required,
                    nullable: false,
                    max_definition_level: 0,
                    max_repetition_level: 0,
                    type_length: None,
                    precision: None,
                    scale: None,
                }],
                field_stats: std::collections::BTreeMap::new(),
            },
        )
        .expect("valid bootstrapped files should construct in wasm")],
    )
    .expect("valid bootstrapped snapshots should construct in wasm");

    let planned = session
        .plan_query(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id FROM axon_table ORDER BY id LIMIT 1",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect("matching requests should plan in wasm");

    assert_eq!(shape.referenced_columns, vec!["id"]);
    assert_eq!(planned.candidate_file_count, 1);
    assert_eq!(planned.query_shape.limit, Some(1));
}

#[wasm_bindgen_test]
fn browser_runtime_execution_plan_api_constructs_in_wasm() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported in wasm");
    let snapshot = BootstrappedBrowserSnapshot::new(
        "gs://axon-fixtures/sample_table",
        4,
        vec![BootstrappedBrowserFile::new(
            "part-000.parquet",
            128,
            std::collections::BTreeMap::new(),
            BrowserParquetFileMetadata {
                object_size_bytes: 128,
                footer_length_bytes: 16,
                row_group_count: 0,
                row_count: 0,
                fields: vec![BrowserParquetField {
                    name: "id".to_string(),
                    physical_type: BrowserParquetPhysicalType::Int32,
                    logical_type: None,
                    converted_type: None,
                    repetition: BrowserParquetRepetition::Required,
                    nullable: false,
                    max_definition_level: 0,
                    max_repetition_level: 0,
                    type_length: None,
                    precision: None,
                    scale: None,
                }],
                field_stats: std::collections::BTreeMap::new(),
            },
        )
        .expect("valid bootstrapped files should construct in wasm")],
    )
    .expect("valid bootstrapped snapshots should construct in wasm");

    let plan = session
        .build_execution_plan(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id FROM axon_table",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect("direct projection execution plans should build in wasm");

    assert_eq!(plan.scan().candidate_file_count(), 1);
    assert_eq!(plan.outputs().len(), 1);
    assert!(plan.aggregation().is_none());
    assert!(plan.order_by().is_empty());
}

#[wasm_bindgen_test]
fn browser_runtime_execute_plan_api_constructs_in_wasm() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported in wasm");
    let snapshot = BootstrappedBrowserSnapshot::new(
        "gs://axon-fixtures/sample_table",
        4,
        vec![BootstrappedBrowserFile::new(
            "part-000.parquet",
            128,
            std::collections::BTreeMap::new(),
            BrowserParquetFileMetadata {
                object_size_bytes: 128,
                footer_length_bytes: 16,
                row_group_count: 0,
                row_count: 0,
                fields: vec![BrowserParquetField {
                    name: "id".to_string(),
                    physical_type: BrowserParquetPhysicalType::Int32,
                    logical_type: None,
                    converted_type: None,
                    repetition: BrowserParquetRepetition::Required,
                    nullable: false,
                    max_definition_level: 0,
                    max_repetition_level: 0,
                    type_length: None,
                    precision: None,
                    scale: None,
                }],
                field_stats: std::collections::BTreeMap::new(),
            },
        )
        .expect("valid bootstrapped files should construct in wasm")],
    )
    .expect("valid bootstrapped snapshots should construct in wasm");
    let materialized = MaterializedBrowserSnapshot::new(
        snapshot.table_uri(),
        snapshot.snapshot_version(),
        vec![MaterializedBrowserFile::new(
            "part-000.parquet",
            128,
            std::collections::BTreeMap::new(),
            BrowserObjectSource::from_url("https://example.com/object")
                .expect("https object sources should be supported in wasm"),
        )],
    )
    .expect("duplicate-free snapshots should construct in wasm");
    let plan = session
        .build_execution_plan(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id FROM axon_table",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect("direct projection execution plans should build in wasm");

    let execution = session.execute_plan(&materialized, &plan);
    drop(execution);
}

#[wasm_bindgen_test]
fn browser_runtime_aggregate_execution_plan_api_constructs_in_wasm() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported in wasm");
    let snapshot = BootstrappedBrowserSnapshot::new(
        "gs://axon-fixtures/sample_table",
        4,
        vec![BootstrappedBrowserFile::new(
            "part-000.parquet",
            128,
            std::collections::BTreeMap::new(),
            BrowserParquetFileMetadata {
                object_size_bytes: 128,
                footer_length_bytes: 16,
                row_group_count: 0,
                row_count: 0,
                fields: vec![BrowserParquetField {
                    name: "value".to_string(),
                    physical_type: BrowserParquetPhysicalType::Int32,
                    logical_type: None,
                    converted_type: None,
                    repetition: BrowserParquetRepetition::Required,
                    nullable: false,
                    max_definition_level: 0,
                    max_repetition_level: 0,
                    type_length: None,
                    precision: None,
                    scale: None,
                }],
                field_stats: std::collections::BTreeMap::new(),
            },
        )
        .expect("valid bootstrapped files should construct in wasm")],
    )
    .expect("valid bootstrapped snapshots should construct in wasm");

    let plan = session
        .build_execution_plan(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT COUNT(*) AS row_count FROM axon_table",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect("aggregate execution plans should build in wasm");

    assert_eq!(plan.outputs().len(), 1);
    assert!(plan.aggregation().is_some());
    assert!(plan.order_by().is_empty());
}

#[wasm_bindgen_test]
fn browser_runtime_grouped_order_limit_execution_plan_constructs_in_wasm() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported in wasm");
    let snapshot = BootstrappedBrowserSnapshot::new(
        "gs://axon-fixtures/sample_table",
        4,
        vec![BootstrappedBrowserFile::new(
            "category=A/part-000.parquet",
            128,
            std::collections::BTreeMap::from([("category".to_string(), Some("A".to_string()))]),
            BrowserParquetFileMetadata {
                object_size_bytes: 128,
                footer_length_bytes: 16,
                row_group_count: 0,
                row_count: 0,
                fields: vec![BrowserParquetField {
                    name: "value".to_string(),
                    physical_type: BrowserParquetPhysicalType::Int32,
                    logical_type: None,
                    converted_type: None,
                    repetition: BrowserParquetRepetition::Required,
                    nullable: false,
                    max_definition_level: 0,
                    max_repetition_level: 0,
                    type_length: None,
                    precision: None,
                    scale: None,
                }],
                field_stats: std::collections::BTreeMap::new(),
            },
        )
        .expect("valid bootstrapped files should construct in wasm")],
    )
    .expect("valid bootstrapped snapshots should construct in wasm");

    let plan = session
        .build_execution_plan(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT category, SUM(value) AS total_value \
                 FROM axon_table \
                 GROUP BY category \
                 ORDER BY category \
                 LIMIT 1",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect("grouped execution plans should build in wasm");

    assert_eq!(plan.outputs().len(), 2);
    assert_eq!(plan.order_by().len(), 1);
    assert_eq!(plan.limit(), Some(1));
}

#[wasm_bindgen_test]
fn materialize_snapshot_rejects_loopback_http_in_wasm() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported in wasm");
    let descriptor = BrowserHttpSnapshotDescriptor {
        table_uri: "gs://axon-fixtures/sample_table".to_string(),
        snapshot_version: 4,
        active_files: vec![BrowserHttpFileDescriptor {
            path: "part-000.parquet".to_string(),
            url: "http://127.0.0.1:8080/object".to_string(),
            size_bytes: 128,
            partition_values: std::collections::BTreeMap::new(),
        }],
    };

    let error = session
        .materialize_snapshot(&descriptor)
        .expect_err("loopback HTTP should be rejected in wasm");

    assert_eq!(error.code, QueryErrorCode::SecurityPolicyViolation);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}
