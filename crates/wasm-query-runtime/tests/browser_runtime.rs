#![cfg(not(target_arch = "wasm32"))]

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use query_contract::{
    BrowserHttpFileDescriptor, BrowserHttpSnapshotDescriptor, CapabilityKey, CapabilityState,
    ExecutionTarget, FallbackReason, PartitionColumnType, QueryErrorCode, QueryRequest,
};
use serde::Deserialize;
use wasm_http_object_store::{HttpByteRange, HttpRangeReader};
use wasm_query_runtime::{
    runtime_target, BootstrappedBrowserFile, BootstrappedBrowserSnapshot, BrowserAggregateFunction,
    BrowserComparisonOp, BrowserExecutionOutputKind, BrowserFilterExpr, BrowserObjectAccessMode,
    BrowserObjectSource, BrowserParquetConvertedType, BrowserParquetField,
    BrowserParquetFieldStats, BrowserParquetFileMetadata, BrowserParquetLogicalType,
    BrowserParquetPhysicalType, BrowserParquetRepetition, BrowserRuntimeConfig,
    BrowserRuntimeSession, BrowserScalarValue, MaterializedBrowserFile,
    MaterializedBrowserSnapshot,
};

const PARQUET_MAGIC: &[u8; 4] = b"PAR1";

#[derive(Debug, Deserialize)]
struct ExecutionPlanCorpusCase {
    name: String,
    snapshot: String,
    sql: String,
    expected_candidate_paths: Vec<String>,
    expected_candidate_bytes: u64,
    expected_candidate_rows: u64,
    expected_partition_columns: Vec<String>,
    expected_pruning: ExpectedPruningSummary,
    expected_outputs: Vec<ExpectedExecutionOutput>,
    expected_group_by_columns: Vec<String>,
    expected_measures: Vec<ExpectedAggregateMeasure>,
    expected_order_by: Vec<ExpectedSortKey>,
    expected_limit: Option<u64>,
    #[serde(default)]
    expected_required_columns: Option<Vec<String>>,
    #[serde(default)]
    expected_filter: Option<ExpectedFilterExpr>,
}

#[derive(Debug, Deserialize)]
struct ExpectedPruningSummary {
    used_partition_pruning: bool,
    used_file_stats_pruning: bool,
    files_retained: u64,
    files_pruned: u64,
}

#[derive(Debug, Deserialize)]
struct ExpectedExecutionOutput {
    output_name: String,
    kind: String,
    source_column: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ExpectedAggregateMeasure {
    output_name: String,
    function: String,
    source_column: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ExpectedSortKey {
    output_name: String,
    descending: bool,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ExpectedFilterExpr {
    And {
        children: Vec<ExpectedFilterExpr>,
    },
    Compare {
        source_column: String,
        comparison: String,
        literal: ExpectedScalarValue,
    },
    InList {
        source_column: String,
        literals: Vec<ExpectedScalarValue>,
    },
    IsNull {
        source_column: String,
    },
    IsNotNull {
        source_column: String,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ExpectedScalarValue {
    Int64 { value: i64 },
    String { value: String },
    Boolean { value: bool },
    Null,
}

#[test]
fn default_config_constructs_a_browser_runtime_session() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    assert_eq!(runtime_target(), ExecutionTarget::BrowserWasm);
    assert_eq!(session.config(), &BrowserRuntimeConfig::default());
}

#[test]
fn https_object_sources_are_constructible() {
    let source = BrowserObjectSource::from_url("https://example.com/object")
        .expect("https object sources should be supported");

    assert_eq!(source.url(), "https://example.com/object");
}

#[test]
fn analyze_query_shape_extracts_columns_and_flags_supported_queries() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let shape = session
        .analyze_query_shape(
            "SELECT category, SUM(value) AS total_value \
             FROM axon_table \
             WHERE value >= 40 \
             GROUP BY category \
             ORDER BY category \
             LIMIT 2",
        )
        .expect("supported single-table browser queries should analyze");

    assert_eq!(shape.referenced_columns, vec!["category", "value"]);
    assert_eq!(shape.filter_columns, vec!["value"]);
    assert_eq!(shape.projection_columns, vec!["category", "value"]);
    assert!(shape.has_aggregation);
    assert!(shape.has_order_by);
    assert_eq!(shape.limit, Some(2));
}

#[test]
fn analyze_query_shape_allows_non_recursive_ctes_built_from_axon_table() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let shape = session
        .analyze_query_shape(
            "WITH filtered AS (SELECT id, value FROM axon_table WHERE value >= 20) \
             SELECT id FROM filtered ORDER BY id",
        )
        .expect("non-recursive ctes over axon_table should analyze");

    assert_eq!(shape.referenced_columns, vec!["id", "value"]);
    assert_eq!(shape.filter_columns, vec!["value"]);
    assert_eq!(shape.projection_columns, vec!["id"]);
    assert!(!shape.has_aggregation);
    assert!(shape.has_order_by);
    assert_eq!(shape.limit, None);
}

#[test]
fn analyze_query_shape_rejects_constant_queries_without_axon_table() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let error = session
        .analyze_query_shape("SELECT 1")
        .expect_err("constant queries should be rejected");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("axon_table"));
}

#[test]
fn analyze_query_shape_rejects_multiple_statements() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let error = session
        .analyze_query_shape("SELECT * FROM axon_table; SELECT * FROM axon_table")
        .expect_err("multiple statements should be rejected");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("exactly one"));
}

#[test]
fn analyze_query_shape_rejects_recursive_ctes_and_table_functions() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let recursive_error = session
        .analyze_query_shape(
            "WITH RECURSIVE looped AS (SELECT * FROM axon_table) SELECT * FROM looped",
        )
        .expect_err("recursive ctes should be rejected");
    assert_eq!(recursive_error.code, QueryErrorCode::InvalidRequest);
    assert!(recursive_error.message.contains("recursive"));

    let table_function_error = session
        .analyze_query_shape("SELECT * FROM generate_series(1, 2)")
        .expect_err("table functions should be rejected");
    assert_eq!(table_function_error.code, QueryErrorCode::InvalidRequest);
    assert!(table_function_error.message.contains("axon_table"));
}

#[test]
fn analyze_query_shape_rejects_distinct_queries() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let error = session
        .analyze_query_shape("SELECT DISTINCT id FROM axon_table")
        .expect_err("distinct queries should be rejected");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("DISTINCT") || error.message.contains("supported"));
}

#[test]
fn plan_query_returns_full_scan_totals_for_matching_snapshot_requests() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let snapshot = sample_bootstrapped_snapshot();

    let planned = session
        .plan_query(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id FROM axon_table ORDER BY id",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect("matching requests should plan against bootstrapped snapshots");

    assert_eq!(planned.table_uri, snapshot.table_uri());
    assert_eq!(planned.snapshot_version, snapshot.snapshot_version());
    assert_eq!(
        planned.candidate_paths,
        vec![
            "category=A/part-000.parquet".to_string(),
            "category=B/part-001.parquet".to_string(),
            "category=C/part-002.parquet".to_string(),
        ]
    );
    assert_eq!(planned.candidate_file_count, 3);
    assert_eq!(planned.candidate_bytes, 360);
    assert_eq!(planned.candidate_rows, 9);
    assert_eq!(planned.partition_columns, vec!["category"]);
    assert!(!planned.pruning.used_partition_pruning);
    assert!(!planned.pruning.used_file_stats_pruning);
    assert_eq!(planned.pruning.files_retained, 3);
    assert_eq!(planned.pruning.files_pruned, 0);
}

#[test]
fn build_execution_plan_matches_direct_projection_corpus() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    for case in load_execution_plan_corpus() {
        let snapshot = match case.snapshot.as_str() {
            "sample" => sample_bootstrapped_snapshot(),
            "stats" => stats_bootstrapped_snapshot(),
            other => panic!("unsupported test snapshot '{other}'"),
        };

        let plan = session
            .build_execution_plan(
                &snapshot,
                &QueryRequest::new(
                    snapshot.table_uri(),
                    &case.sql,
                    ExecutionTarget::BrowserWasm,
                ),
            )
            .unwrap_or_else(|error| {
                panic!(
                    "case '{}': execution-plan build should succeed, got {:?}: {}",
                    case.name, error.code, error.message
                )
            });

        assert_eq!(
            plan.scan().candidate_paths(),
            case.expected_candidate_paths.as_slice(),
            "case '{}': candidate paths should match",
            case.name
        );
        assert_eq!(
            plan.scan().candidate_bytes(),
            case.expected_candidate_bytes,
            "case '{}': candidate bytes should match",
            case.name
        );
        assert_eq!(
            plan.scan().candidate_rows(),
            case.expected_candidate_rows,
            "case '{}': candidate rows should match",
            case.name
        );
        assert_eq!(
            plan.scan().partition_columns(),
            case.expected_partition_columns.as_slice(),
            "case '{}': partition columns should match",
            case.name
        );
        if let Some(expected_required_columns) = &case.expected_required_columns {
            assert_eq!(
                plan.scan().required_columns(),
                expected_required_columns.as_slice(),
                "case '{}': required scan columns should match",
                case.name
            );
        }
        assert_eq!(
            plan.pruning().used_partition_pruning,
            case.expected_pruning.used_partition_pruning,
            "case '{}': partition pruning usage should match",
            case.name
        );
        assert_eq!(
            plan.pruning().used_file_stats_pruning,
            case.expected_pruning.used_file_stats_pruning,
            "case '{}': file-stats pruning usage should match",
            case.name
        );
        assert_eq!(
            plan.pruning().files_retained,
            case.expected_pruning.files_retained,
            "case '{}': retained-file count should match",
            case.name
        );
        assert_eq!(
            plan.pruning().files_pruned,
            case.expected_pruning.files_pruned,
            "case '{}': pruned-file count should match",
            case.name
        );
        assert_eq!(
            plan.limit(),
            case.expected_limit,
            "case '{}': limit should match",
            case.name
        );
        match (plan.filter(), case.expected_filter.as_ref()) {
            (Some(actual), Some(expected)) => {
                assert_filter_expr_matches(actual, expected, &case.name);
            }
            (None, None) => {}
            (Some(actual), None) => {
                panic!(
                    "case '{}': execution plan unexpectedly lowered a filter: {:?}",
                    case.name, actual
                );
            }
            (None, Some(expected)) => {
                panic!(
                    "case '{}': execution plan did not lower expected filter {:?}",
                    case.name, expected
                );
            }
        }
        let aggregation = plan.aggregation();
        assert_eq!(
            aggregation
                .map(|aggregation| aggregation.group_by_columns().to_vec())
                .unwrap_or_default(),
            case.expected_group_by_columns,
            "case '{}': group-by columns should match",
            case.name
        );
        assert_eq!(
            aggregation
                .map(|aggregation| aggregation.measures().len())
                .unwrap_or_default(),
            case.expected_measures.len(),
            "case '{}': aggregate-measure count should match",
            case.name
        );
        if let Some(aggregation) = aggregation {
            for (measure, expected) in aggregation
                .measures()
                .iter()
                .zip(case.expected_measures.iter())
            {
                assert_eq!(
                    measure.output_name(),
                    expected.output_name,
                    "case '{}': aggregate output name should match",
                    case.name
                );
                assert_eq!(
                    measure.source_column(),
                    expected.source_column.as_deref(),
                    "case '{}': aggregate source column should match",
                    case.name
                );
                assert_eq!(
                    measure.function(),
                    &expected_aggregate_function(&expected.function),
                    "case '{}': aggregate function should match",
                    case.name
                );
            }
        }
        assert_eq!(
            plan.order_by().len(),
            case.expected_order_by.len(),
            "case '{}': sort-key count should match",
            case.name
        );
        for (sort_key, expected) in plan.order_by().iter().zip(case.expected_order_by.iter()) {
            assert_eq!(
                sort_key.output_name(),
                expected.output_name,
                "case '{}': sort-key output should match",
                case.name
            );
            assert_eq!(
                sort_key.descending(),
                expected.descending,
                "case '{}': sort-key direction should match",
                case.name
            );
        }
        assert_eq!(
            plan.outputs().len(),
            case.expected_outputs.len(),
            "case '{}': output count should match",
            case.name
        );

        for (output, expected) in plan.outputs().iter().zip(case.expected_outputs.iter()) {
            assert_eq!(
                output.output_name(),
                expected.output_name,
                "case '{}': output name should match",
                case.name
            );
            match output.kind() {
                BrowserExecutionOutputKind::Passthrough { source_column } => {
                    assert_eq!(expected.kind, "passthrough");
                    assert_eq!(
                        expected.source_column.as_deref(),
                        Some(source_column.as_str())
                    );
                }
                BrowserExecutionOutputKind::Aggregate {
                    function,
                    source_column,
                } => {
                    assert_eq!(expected.kind, "aggregate");
                    assert_eq!(expected.source_column.as_deref(), source_column.as_deref());
                    let function_name = format!("{function:?}");
                    assert!(
                        matches!(
                            function_name.as_str(),
                            "Avg"
                                | "ArrayAgg"
                                | "BoolAnd"
                                | "BoolOr"
                                | "CountStar"
                                | "Count"
                                | "Sum"
                                | "Min"
                                | "Max"
                        ),
                        "case '{}': aggregate outputs should use a supported function",
                        case.name
                    );
                }
            }
        }
    }
}

#[test]
fn build_execution_plan_rejects_table_and_snapshot_mismatches() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let snapshot = sample_bootstrapped_snapshot();

    let table_mismatch_error = session
        .build_execution_plan(
            &snapshot,
            &QueryRequest::new(
                "gs://axon-fixtures/other_table",
                "SELECT id FROM axon_table",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect_err("table-uri mismatches should be rejected");
    assert_eq!(table_mismatch_error.code, QueryErrorCode::InvalidRequest);
    assert!(table_mismatch_error.message.contains("table_uri"));

    let version_mismatch_error = session
        .build_execution_plan(
            &snapshot,
            &QueryRequest {
                snapshot_version: Some(snapshot.snapshot_version() + 1),
                ..QueryRequest::new(
                    snapshot.table_uri(),
                    "SELECT id FROM axon_table",
                    ExecutionTarget::BrowserWasm,
                )
            },
        )
        .expect_err("snapshot-version mismatches should be rejected");
    assert_eq!(version_mismatch_error.code, QueryErrorCode::InvalidRequest);
    assert!(version_mismatch_error.message.contains("snapshot_version"));
}

#[test]
fn build_execution_plan_rejects_non_lowerable_filters() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let snapshot = sample_bootstrapped_snapshot();
    let error = session
        .build_execution_plan(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id FROM axon_table WHERE value + 1 > 10 ORDER BY id",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect_err("non-lowerable filters should be rejected");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(
        error
            .message
            .contains("WHERE filters must be lowerable into browser execution plans"),
        "unexpected error message: {}",
        error.message
    );
}

#[test]
fn build_execution_plan_rejects_null_compare_literals() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let snapshot = sample_bootstrapped_snapshot();

    let error = session
        .build_execution_plan(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id FROM axon_table WHERE value = NULL ORDER BY id",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect_err("null compare literals should remain outside the browser execution envelope");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("WHERE filters"));
}

#[test]
fn build_execution_plan_rejects_null_in_list_literals() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let snapshot = sample_bootstrapped_snapshot();

    let error = session
        .build_execution_plan(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id FROM axon_table WHERE value IN (NULL) ORDER BY id",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect_err("null in-list literals should remain outside the browser execution envelope");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("WHERE filters"));
}

#[test]
fn execute_plan_rejects_snapshot_identity_mismatches() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let snapshot = sample_bootstrapped_snapshot();
    let plan = session
        .build_execution_plan(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id FROM axon_table ORDER BY id",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect("direct projection execution plans should build");
    let materialized = MaterializedBrowserSnapshot::new(
        "gs://axon-fixtures/other_table",
        snapshot.snapshot_version(),
        vec![MaterializedBrowserFile::new(
            "category=A/part-000.parquet",
            100,
            BTreeMap::from([("category".to_string(), Some("A".to_string()))]),
            BrowserObjectSource::from_url("http://127.0.0.1:8787/object")
                .expect("loopback HTTP should be allowed in host tests"),
        )],
    )
    .expect("duplicate-free snapshots should construct");

    let error = runtime()
        .block_on(session.execute_plan(&materialized, &plan))
        .expect_err("snapshot identity mismatches should fail before execution");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("table uri"));
}

#[test]
fn execute_plan_rejects_missing_candidate_paths_in_materialized_snapshots() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let snapshot = sample_bootstrapped_snapshot();
    let plan = session
        .build_execution_plan(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id FROM axon_table ORDER BY id",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect("direct projection execution plans should build");
    let materialized = MaterializedBrowserSnapshot::new(
        snapshot.table_uri(),
        snapshot.snapshot_version(),
        vec![MaterializedBrowserFile::new(
            "category=A/part-000.parquet",
            100,
            BTreeMap::from([("category".to_string(), Some("A".to_string()))]),
            BrowserObjectSource::from_url("http://127.0.0.1:8787/object")
                .expect("loopback HTTP should be allowed in host tests"),
        )],
    )
    .expect("duplicate-free snapshots should construct");

    let error = runtime()
        .block_on(session.execute_plan(&materialized, &plan))
        .expect_err("candidate paths absent from the materialized snapshot should fail");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("category=B/part-001.parquet"));
}

#[test]
fn execute_plan_enforces_the_execution_deadline() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig {
        request_timeout_ms: 200,
        execution_timeout_ms: 25,
        ..BrowserRuntimeConfig::default()
    })
    .expect("short execution timeout configuration should be supported");
    let snapshot = sample_bootstrapped_snapshot();
    let plan = session
        .build_execution_plan(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id FROM axon_table WHERE category = 'A' ORDER BY id",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect("single-file execution plans should build");
    let (url, server) = spawn_stalling_server(Duration::from_millis(70));
    let materialized = MaterializedBrowserSnapshot::new_with_partition_column_types(
        snapshot.table_uri(),
        snapshot.snapshot_version(),
        vec![MaterializedBrowserFile::new(
            "category=A/part-000.parquet",
            100,
            BTreeMap::from([("category".to_string(), Some("A".to_string()))]),
            BrowserObjectSource::from_url(&url)
                .expect("loopback HTTP should be allowed in host tests"),
        )],
        BTreeMap::from([("category".to_string(), PartitionColumnType::String)]),
    )
    .expect("duplicate-free snapshots should construct");

    let error = runtime()
        .block_on(session.execute_plan(&materialized, &plan))
        .expect_err("query execution should fail once the deadline is exceeded");

    server.join().expect("test server should shut down cleanly");
    assert_eq!(error.code, QueryErrorCode::ExecutionFailed);
    assert!(error.message.contains("browser execution"));
}

#[test]
fn execute_plan_rejects_entity_tag_mismatches_between_bootstrap_and_execution() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let snapshot = BootstrappedBrowserSnapshot::new(
        "gs://axon-fixtures/sample_table",
        7,
        vec![bootstrapped_file_with_etag(
            "part-000.parquet",
            4,
            BTreeMap::new(),
            BrowserParquetFileMetadata {
                object_size_bytes: 4,
                footer_length_bytes: 2,
                row_group_count: 1,
                row_count: 1,
                fields: vec![required_field("id", BrowserParquetPhysicalType::Int32)],
                field_stats: BTreeMap::new(),
            },
            Some("\"v1\""),
        )],
    )
    .expect("single-file snapshots should construct");
    let plan = session
        .build_execution_plan(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id FROM axon_table",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect("direct projection execution plans should build");
    let (url, requests, server) = spawn_test_server(|request| {
        assert!(!request.headers.contains_key("range"));
        TestResponse {
            status_line: "200 OK",
            headers: vec![
                ("Content-Length".to_string(), "4".to_string()),
                ("ETag".to_string(), "\"v2\"".to_string()),
            ],
            body: b"ABCD".to_vec(),
        }
    });
    let materialized = MaterializedBrowserSnapshot::new(
        snapshot.table_uri(),
        snapshot.snapshot_version(),
        vec![MaterializedBrowserFile::new(
            "part-000.parquet",
            4,
            BTreeMap::new(),
            BrowserObjectSource::from_url(&url)
                .expect("loopback HTTP should be allowed in host tests"),
        )],
    )
    .expect("duplicate-free snapshots should construct");

    let error = runtime()
        .block_on(session.execute_plan(&materialized, &plan))
        .expect_err("bootstrap-to-execution entity-tag drift should fail");

    let request = finish_request(server, requests);
    assert!(!request.headers.contains_key("range"));
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert!(error.message.contains("entity tag"));
}

#[test]
fn build_execution_plan_requires_bootstrapped_entity_tags_for_candidate_files() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let snapshot = BootstrappedBrowserSnapshot::new(
        "gs://axon-fixtures/sample_table",
        7,
        vec![bootstrapped_file_without_etag(
            "part-000.parquet",
            4,
            BTreeMap::new(),
            BrowserParquetFileMetadata {
                object_size_bytes: 4,
                footer_length_bytes: 2,
                row_group_count: 1,
                row_count: 1,
                fields: vec![required_field("id", BrowserParquetPhysicalType::Int32)],
                field_stats: BTreeMap::new(),
            },
        )],
    )
    .expect("single-file snapshots should construct");

    let error = session
        .build_execution_plan(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id FROM axon_table",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect_err("candidate files without entity tags should require native fallback");

    assert_eq!(error.code, QueryErrorCode::FallbackRequired);
    assert!(error.message.contains("entity tag"));
}

#[test]
fn build_execution_plan_rejects_having_clauses() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let snapshot = sample_bootstrapped_snapshot();

    let error = session
        .build_execution_plan(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT category, SUM(value) AS total_value \
                 FROM axon_table \
                 GROUP BY category \
                 HAVING SUM(value) >= 60",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect_err("having should remain unsupported for execution-plan lowering");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("HAVING") || error.message.contains("execution plans"));
}

#[test]
fn build_execution_plan_rejects_non_projected_cte_columns() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let snapshot = sample_bootstrapped_snapshot();

    let error = session
        .build_execution_plan(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "WITH filtered AS (SELECT value AS total FROM axon_table) \
                 SELECT id FROM filtered",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect_err("cte outputs should not leak base-table columns");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("projection") || error.message.contains("passthrough"));
}

#[test]
fn build_execution_plan_rejects_duplicate_output_names() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let snapshot = sample_bootstrapped_snapshot();

    let error = session
        .build_execution_plan(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id AS duplicate_name, value AS duplicate_name \
                 FROM axon_table \
                 ORDER BY duplicate_name",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect_err("duplicate output names should be rejected");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("duplicate"));
}

#[test]
fn plan_query_rejects_unknown_columns_and_snapshot_mismatches() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let snapshot = sample_bootstrapped_snapshot();

    let unknown_column_error = session
        .plan_query(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT missing_column FROM axon_table",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect_err("unknown columns should be rejected");
    assert_eq!(unknown_column_error.code, QueryErrorCode::InvalidRequest);
    assert!(unknown_column_error.message.contains("missing_column"));

    let table_mismatch_error = session
        .plan_query(
            &snapshot,
            &QueryRequest::new(
                "gs://axon-fixtures/other_table",
                "SELECT id FROM axon_table",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect_err("table-uri mismatches should be rejected");
    assert_eq!(table_mismatch_error.code, QueryErrorCode::InvalidRequest);
    assert!(table_mismatch_error.message.contains("table_uri"));

    let version_mismatch_error = session
        .plan_query(
            &snapshot,
            &QueryRequest {
                snapshot_version: Some(snapshot.snapshot_version() + 1),
                ..QueryRequest::new(
                    snapshot.table_uri(),
                    "SELECT id FROM axon_table",
                    ExecutionTarget::BrowserWasm,
                )
            },
        )
        .expect_err("snapshot-version mismatches should be rejected");
    assert_eq!(version_mismatch_error.code, QueryErrorCode::InvalidRequest);
    assert!(version_mismatch_error.message.contains("snapshot_version"));
}

#[test]
fn plan_query_accepts_cte_output_aliases_without_requiring_snapshot_columns() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let snapshot = sample_bootstrapped_snapshot();

    let planned = session
        .plan_query(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "WITH filtered AS (SELECT value AS total FROM axon_table) \
                 SELECT total FROM filtered ORDER BY total",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect("cte output aliases should not be rejected as missing snapshot columns");

    assert_eq!(planned.candidate_file_count, 3);
    assert_eq!(planned.candidate_bytes, 360);
    assert_eq!(planned.candidate_rows, 9);
    assert!(!planned.pruning.used_partition_pruning);
    assert!(!planned.pruning.used_file_stats_pruning);
}

#[test]
fn plan_query_prunes_candidate_files_from_partition_values() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let snapshot = sample_bootstrapped_snapshot();

    let planned = session
        .plan_query(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id FROM axon_table WHERE category = 'C' ORDER BY id",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect("partition predicates should plan");

    assert_eq!(planned.candidate_paths, vec!["category=C/part-002.parquet"]);
    assert_eq!(planned.candidate_file_count, 1);
    assert_eq!(planned.candidate_bytes, 140);
    assert_eq!(planned.candidate_rows, 4);
    assert!(planned.pruning.used_partition_pruning);
    assert!(!planned.pruning.used_file_stats_pruning);
    assert_eq!(planned.pruning.files_retained, 1);
    assert_eq!(planned.pruning.files_pruned, 2);
}

#[test]
fn plan_query_prunes_candidate_files_from_partition_in_lists() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let snapshot = sample_bootstrapped_snapshot();

    let planned = session
        .plan_query(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id FROM axon_table WHERE category IN ('A', 'B') ORDER BY id",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect("supported partition IN predicates should plan");

    assert_eq!(
        planned.candidate_paths,
        vec![
            "category=A/part-000.parquet".to_string(),
            "category=B/part-001.parquet".to_string(),
        ]
    );
    assert_eq!(planned.candidate_file_count, 2);
    assert_eq!(planned.candidate_bytes, 220);
    assert_eq!(planned.candidate_rows, 5);
    assert!(planned.pruning.used_partition_pruning);
    assert_eq!(planned.pruning.files_retained, 2);
    assert_eq!(planned.pruning.files_pruned, 1);
}

#[test]
fn plan_query_can_prune_all_files_when_partition_values_do_not_match() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let snapshot = sample_bootstrapped_snapshot();

    let planned = session
        .plan_query(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id FROM axon_table WHERE category = 'Z' ORDER BY id",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect("supported partition equality predicates should plan");

    assert!(planned.candidate_paths.is_empty());
    assert_eq!(planned.candidate_file_count, 0);
    assert_eq!(planned.candidate_bytes, 0);
    assert_eq!(planned.candidate_rows, 0);
    assert!(planned.pruning.used_partition_pruning);
    assert_eq!(planned.pruning.files_retained, 0);
    assert_eq!(planned.pruning.files_pruned, 3);
}

#[test]
fn plan_query_leaves_full_scan_when_partition_predicates_are_not_lossless() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let snapshot = sample_bootstrapped_snapshot();

    let planned = session
        .plan_query(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id FROM axon_table WHERE category = 'A' OR category = 'C' ORDER BY id",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect("unsupported partition predicates should still plan");

    assert_eq!(planned.candidate_file_count, 3);
    assert_eq!(planned.candidate_bytes, 360);
    assert_eq!(planned.candidate_rows, 9);
    assert!(!planned.pruning.used_partition_pruning);
    assert!(!planned.pruning.used_file_stats_pruning);
    assert_eq!(planned.pruning.files_retained, 3);
    assert_eq!(planned.pruning.files_pruned, 0);
}

#[test]
fn plan_query_keeps_lossless_partition_pruning_with_unsupported_sibling_predicates() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let snapshot = sample_bootstrapped_snapshot();

    let planned = session
        .plan_query(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id FROM axon_table WHERE category = 'C' AND value <> 0 ORDER BY id",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect("lossless partition pruning should survive unsupported sibling predicates");

    assert_eq!(planned.candidate_paths, vec!["category=C/part-002.parquet"]);
    assert_eq!(planned.candidate_file_count, 1);
    assert_eq!(planned.candidate_bytes, 140);
    assert_eq!(planned.candidate_rows, 4);
    assert!(planned.pruning.used_partition_pruning);
    assert!(!planned.pruning.used_file_stats_pruning);
    assert_eq!(planned.pruning.files_retained, 1);
    assert_eq!(planned.pruning.files_pruned, 2);
}

#[test]
fn plan_query_supports_partition_null_filters() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let snapshot = null_partition_bootstrapped_snapshot();

    let is_null = session
        .plan_query(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id FROM axon_table WHERE region IS NULL ORDER BY id",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect("partition IS NULL predicates should plan");
    assert_eq!(
        is_null.candidate_paths,
        vec!["region=NULL/part-000.parquet"]
    );
    assert_eq!(is_null.candidate_file_count, 1);
    assert_eq!(is_null.candidate_bytes, 80);
    assert_eq!(is_null.candidate_rows, 2);
    assert!(is_null.pruning.used_partition_pruning);
    assert_eq!(is_null.pruning.files_retained, 1);
    assert_eq!(is_null.pruning.files_pruned, 1);

    let is_not_null = session
        .plan_query(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id FROM axon_table WHERE region IS NOT NULL ORDER BY id",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect("partition IS NOT NULL predicates should plan");
    assert_eq!(
        is_not_null.candidate_paths,
        vec!["region=us-east/part-001.parquet"]
    );
    assert_eq!(is_not_null.candidate_file_count, 1);
    assert_eq!(is_not_null.candidate_bytes, 90);
    assert_eq!(is_not_null.candidate_rows, 3);
    assert!(is_not_null.pruning.used_partition_pruning);
    assert_eq!(is_not_null.pruning.files_retained, 1);
    assert_eq!(is_not_null.pruning.files_pruned, 1);
}

#[test]
fn plan_query_prunes_candidate_files_from_integer_footer_stats() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let snapshot = stats_bootstrapped_snapshot();

    let planned = session
        .plan_query(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id FROM axon_table WHERE value >= 40 ORDER BY id",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect("supported integer footer-stat predicates should plan");

    assert_eq!(
        planned.candidate_paths,
        vec![
            "category=B/part-001.parquet".to_string(),
            "category=C/part-002.parquet".to_string(),
        ]
    );
    assert_eq!(planned.candidate_file_count, 2);
    assert_eq!(planned.candidate_bytes, 260);
    assert_eq!(planned.candidate_rows, 7);
    assert!(!planned.pruning.used_partition_pruning);
    assert!(planned.pruning.used_file_stats_pruning);
    assert_eq!(planned.pruning.files_retained, 2);
    assert_eq!(planned.pruning.files_pruned, 1);
}

#[test]
fn plan_query_keeps_file_stats_pruning_with_unsupported_sibling_predicates() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let snapshot = stats_bootstrapped_snapshot();

    let planned = session
        .plan_query(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id FROM axon_table WHERE value >= 40 AND category <> 'A' ORDER BY id",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect("lossless stats pruning should survive unsupported sibling predicates");

    assert_eq!(
        planned.candidate_paths,
        vec![
            "category=B/part-001.parquet".to_string(),
            "category=C/part-002.parquet".to_string(),
        ]
    );
    assert_eq!(planned.candidate_file_count, 2);
    assert_eq!(planned.candidate_bytes, 260);
    assert_eq!(planned.candidate_rows, 7);
    assert!(!planned.pruning.used_partition_pruning);
    assert!(planned.pruning.used_file_stats_pruning);
    assert_eq!(planned.pruning.files_retained, 2);
    assert_eq!(planned.pruning.files_pruned, 1);
}

#[test]
fn plan_query_combines_partition_and_integer_footer_stats_pruning() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let snapshot = stats_bootstrapped_snapshot();

    let planned = session
        .plan_query(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id FROM axon_table WHERE category IN ('A', 'B') AND value >= 40 ORDER BY id",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect("combined partition and footer-stat predicates should plan");

    assert_eq!(planned.candidate_paths, vec!["category=B/part-001.parquet"]);
    assert_eq!(planned.candidate_file_count, 1);
    assert_eq!(planned.candidate_bytes, 120);
    assert_eq!(planned.candidate_rows, 3);
    assert!(planned.pruning.used_partition_pruning);
    assert!(planned.pruning.used_file_stats_pruning);
    assert_eq!(planned.pruning.files_retained, 1);
    assert_eq!(planned.pruning.files_pruned, 2);
}

#[test]
fn materialize_snapshot_preserves_descriptor_metadata_and_file_order() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let descriptor = BrowserHttpSnapshotDescriptor {
        table_uri: "gs://axon-fixtures/sample_table".to_string(),
        snapshot_version: 7,
        partition_column_types: BTreeMap::from([
            ("category".to_string(), PartitionColumnType::String),
            ("region".to_string(), PartitionColumnType::String),
        ]),
        active_files: vec![
            BrowserHttpFileDescriptor {
                path: "z-last.parquet".to_string(),
                url: "https://example.com/z-last.parquet".to_string(),
                size_bytes: 333,
                partition_values: BTreeMap::from([("category".to_string(), Some("z".to_string()))]),
            },
            BrowserHttpFileDescriptor {
                path: "a-first.parquet".to_string(),
                url: "https://example.com/a-first.parquet".to_string(),
                size_bytes: 111,
                partition_values: BTreeMap::from([
                    ("category".to_string(), Some("a".to_string())),
                    ("region".to_string(), None),
                ]),
            },
        ],
    };

    let materialized = session
        .materialize_snapshot(&descriptor)
        .expect("https descriptors should materialize");

    assert_eq!(materialized.table_uri(), descriptor.table_uri);
    assert_eq!(materialized.snapshot_version(), descriptor.snapshot_version);
    assert_eq!(
        materialized.partition_column_types(),
        &descriptor.partition_column_types
    );
    assert_eq!(
        materialized.active_files().len(),
        descriptor.active_files.len()
    );
    for (materialized_file, descriptor_file) in materialized
        .active_files()
        .iter()
        .zip(descriptor.active_files.iter())
    {
        assert_eq!(materialized_file.path(), descriptor_file.path);
        assert_eq!(materialized_file.size_bytes(), descriptor_file.size_bytes);
        assert_eq!(
            materialized_file.partition_values(),
            &descriptor_file.partition_values
        );
        assert_eq!(materialized_file.object_source().url(), descriptor_file.url);
    }
}

#[test]
fn materialize_snapshot_allows_empty_active_file_lists() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let descriptor = BrowserHttpSnapshotDescriptor {
        table_uri: "gs://axon-fixtures/empty_table".to_string(),
        snapshot_version: 3,
        partition_column_types: BTreeMap::new(),
        active_files: Vec::new(),
    };

    let materialized = session
        .materialize_snapshot(&descriptor)
        .expect("empty descriptors should materialize");

    assert_eq!(materialized.table_uri(), descriptor.table_uri);
    assert_eq!(materialized.snapshot_version(), descriptor.snapshot_version);
    assert!(materialized.active_files().is_empty());
}

#[test]
fn multi_partition_configs_require_native_fallback() {
    let error = BrowserRuntimeSession::new(BrowserRuntimeConfig {
        target_partitions: 2,
        ..BrowserRuntimeConfig::default()
    })
    .expect_err("multi-partition configs should be rejected");

    assert_eq!(error.code, QueryErrorCode::FallbackRequired);
    assert_eq!(
        error.fallback_reason,
        Some(FallbackReason::CapabilityGate {
            capability: CapabilityKey::MultiPartitionExecution,
            required_state: CapabilityState::NativeOnly,
        })
    );
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn non_http_object_access_modes_fail_with_browser_runtime_constraint() {
    let error = BrowserRuntimeSession::new(BrowserRuntimeConfig {
        object_access_mode: BrowserObjectAccessMode::CloudObjectStore,
        ..BrowserRuntimeConfig::default()
    })
    .expect_err("cloud object store access should be rejected");

    assert_eq!(error.code, QueryErrorCode::FallbackRequired);
    assert_eq!(
        error.fallback_reason,
        Some(FallbackReason::BrowserRuntimeConstraint)
    );
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn cloud_credentials_are_rejected_as_a_security_policy_violation() {
    let error = BrowserRuntimeSession::new(BrowserRuntimeConfig {
        allow_cloud_credentials: true,
        ..BrowserRuntimeConfig::default()
    })
    .expect_err("cloud credentials should be rejected");

    assert_eq!(error.code, QueryErrorCode::SecurityPolicyViolation);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn request_timeouts_must_be_positive() {
    let error = BrowserRuntimeSession::new(BrowserRuntimeConfig {
        request_timeout_ms: 0,
        ..BrowserRuntimeConfig::default()
    })
    .expect_err("zero timeouts should be rejected");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn execution_timeouts_must_be_positive() {
    let error = BrowserRuntimeSession::new(BrowserRuntimeConfig {
        execution_timeout_ms: 0,
        ..BrowserRuntimeConfig::default()
    })
    .expect_err("zero execution timeouts should be rejected");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn snapshot_preflight_limits_must_be_positive() {
    let concurrency_error = BrowserRuntimeSession::new(BrowserRuntimeConfig {
        snapshot_preflight_max_concurrency: 0,
        ..BrowserRuntimeConfig::default()
    })
    .expect_err("zero snapshot preflight concurrency should be rejected");
    assert_eq!(concurrency_error.code, QueryErrorCode::InvalidRequest);
    assert_eq!(concurrency_error.fallback_reason, None);
    assert_eq!(concurrency_error.target, ExecutionTarget::BrowserWasm);

    let timeout_error = BrowserRuntimeSession::new(BrowserRuntimeConfig {
        snapshot_preflight_timeout_ms: 0,
        ..BrowserRuntimeConfig::default()
    })
    .expect_err("zero snapshot preflight timeout should be rejected");
    assert_eq!(timeout_error.code, QueryErrorCode::InvalidRequest);
    assert_eq!(timeout_error.fallback_reason, None);
    assert_eq!(timeout_error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn non_loopback_plain_http_sources_are_rejected() {
    let error = BrowserObjectSource::from_url("http://0.0.0.0:1/object")
        .expect_err("non-loopback plain HTTP should be rejected");

    assert_eq!(error.code, QueryErrorCode::SecurityPolicyViolation);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn unsupported_object_source_schemes_are_rejected() {
    for url in ["file:///tmp/object", "s3://bucket/object"] {
        let error =
            BrowserObjectSource::from_url(url).expect_err("unsupported schemes should be rejected");

        assert_eq!(error.code, QueryErrorCode::InvalidRequest, "url: {url}");
        assert_eq!(error.fallback_reason, None, "url: {url}");
        assert_eq!(error.target, ExecutionTarget::BrowserWasm, "url: {url}");
    }
}

#[test]
fn materialize_snapshot_rejects_duplicate_paths() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let duplicate_path = "category=A/part-000.parquet".to_string();
    let descriptor = BrowserHttpSnapshotDescriptor {
        table_uri: "gs://axon-fixtures/sample_table".to_string(),
        snapshot_version: 12,
        partition_column_types: BTreeMap::new(),
        active_files: vec![
            BrowserHttpFileDescriptor {
                path: duplicate_path.clone(),
                url: "https://example.com/one.parquet".to_string(),
                size_bytes: 128,
                partition_values: BTreeMap::new(),
            },
            BrowserHttpFileDescriptor {
                path: duplicate_path.clone(),
                url: "https://example.com/two.parquet".to_string(),
                size_bytes: 256,
                partition_values: BTreeMap::new(),
            },
        ],
    };

    let error = session
        .materialize_snapshot(&descriptor)
        .expect_err("duplicate paths should fail");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains(&duplicate_path));
}

#[test]
fn materialize_snapshot_rejects_invalid_url_syntax_without_leaking_query_or_fragment() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let descriptor = BrowserHttpSnapshotDescriptor {
        table_uri: "gs://axon-fixtures/sample_table".to_string(),
        snapshot_version: 1,
        partition_column_types: BTreeMap::new(),
        active_files: vec![BrowserHttpFileDescriptor {
            path: "part-000.parquet".to_string(),
            url: "https://signed example.test/object?sig=super-secret#fragment".to_string(),
            size_bytes: 128,
            partition_values: BTreeMap::new(),
        }],
    };

    let error = session
        .materialize_snapshot(&descriptor)
        .expect_err("invalid URL syntax should fail");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("https://signed example.test/object"));
    assert!(!error.message.contains("super-secret"));
    assert!(!error.message.contains("fragment"));
}

#[test]
fn materialize_snapshot_rejects_unsupported_schemes_without_leaking_query_or_fragment() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let descriptor = BrowserHttpSnapshotDescriptor {
        table_uri: "gs://axon-fixtures/sample_table".to_string(),
        snapshot_version: 1,
        partition_column_types: BTreeMap::new(),
        active_files: vec![BrowserHttpFileDescriptor {
            path: "part-000.parquet".to_string(),
            url: "ftp://signed.example.test/object?sig=super-secret#fragment".to_string(),
            size_bytes: 128,
            partition_values: BTreeMap::new(),
        }],
    };

    let error = session
        .materialize_snapshot(&descriptor)
        .expect_err("unsupported schemes should fail");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("ftp://signed.example.test/object"));
    assert!(!error.message.contains("super-secret"));
    assert!(!error.message.contains("fragment"));
}

#[test]
fn materialize_snapshot_rejects_non_loopback_plain_http_urls() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let descriptor = BrowserHttpSnapshotDescriptor {
        table_uri: "gs://axon-fixtures/sample_table".to_string(),
        snapshot_version: 1,
        partition_column_types: BTreeMap::new(),
        active_files: vec![BrowserHttpFileDescriptor {
            path: "part-000.parquet".to_string(),
            url: "http://example.com/object?sig=super-secret#fragment".to_string(),
            size_bytes: 128,
            partition_values: BTreeMap::new(),
        }],
    };

    let error = session
        .materialize_snapshot(&descriptor)
        .expect_err("non-loopback plain HTTP should fail");

    assert_eq!(error.code, QueryErrorCode::SecurityPolicyViolation);
    assert!(error.message.contains("http://example.com/object"));
    assert!(!error.message.contains("super-secret"));
    assert!(!error.message.contains("fragment"));
}

#[test]
fn sessions_can_probe_loopback_http_sources_in_host_tests_through_an_injected_range_reader() {
    let (url, requests, server) =
        spawn_test_server(|request| full_or_ranged_response(request, b"abcdefghij"));
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session =
        BrowserRuntimeSession::with_reader(BrowserRuntimeConfig::default(), HttpRangeReader::new())
            .expect("default config should be supported");

    let result = runtime()
        .block_on(session.probe(
            &source,
            HttpByteRange::Bounded {
                offset: 2,
                length: 4,
            },
        ))
        .expect("probe should succeed");

    let request = finish_request(server, requests);
    assert_eq!(request.headers.get("range"), Some(&"bytes=2-5".to_string()));
    assert_eq!(result.metadata.url, url);
    assert_eq!(result.metadata.size_bytes, Some(10));
    assert_eq!(result.bytes.as_ref(), b"cdef");
}

#[test]
fn materialize_snapshot_rejects_loopback_http_even_in_host_tests() {
    let descriptor = BrowserHttpSnapshotDescriptor {
        table_uri: "gs://axon-fixtures/sample_table".to_string(),
        snapshot_version: 9,
        partition_column_types: BTreeMap::new(),
        active_files: vec![BrowserHttpFileDescriptor {
            path: "part-000.parquet".to_string(),
            url: "http://127.0.0.1:8787/object?sig=super-secret#fragment".to_string(),
            size_bytes: 10,
            partition_values: BTreeMap::from([("category".to_string(), Some("A".to_string()))]),
        }],
    };
    let session =
        BrowserRuntimeSession::with_reader(BrowserRuntimeConfig::default(), HttpRangeReader::new())
            .expect("default config should be supported");

    let error = session
        .materialize_snapshot(&descriptor)
        .expect_err("browser-facing descriptors should remain HTTPS-only");

    assert_eq!(error.code, QueryErrorCode::SecurityPolicyViolation);
    assert!(error.message.contains("http://127.0.0.1:8787/object"));
    assert!(!error.message.contains("super-secret"));
    assert!(!error.message.contains("fragment"));
}

#[test]
fn new_sessions_apply_request_timeouts_to_probe_requests() {
    let (url, server) = spawn_stalling_server(Duration::from_millis(250));
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig {
        request_timeout_ms: 25,
        ..BrowserRuntimeConfig::default()
    })
    .expect("short timeouts should be supported");

    let error = runtime()
        .block_on(session.probe(&source, HttpByteRange::Full))
        .expect_err("stalled probes should time out");

    server
        .join()
        .expect("stalling test server should shut down cleanly");
    assert_eq!(error.code, QueryErrorCode::ExecutionFailed);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn read_parquet_footer_applies_request_timeouts_to_footer_reads() {
    let footer = b"serialized-parquet-footer";
    let prefix = b"row-group-bytes";
    let object = parquet_like_object(prefix, footer);
    let trailer = object[object.len() - 8..].to_vec();
    let footer_offset = prefix.len() as u64;
    let expected_range = format!(
        "bytes={footer_offset}-{}",
        footer_offset + footer.len() as u64 - 1
    );
    let (url, requests, server) =
        spawn_stalling_footer_read_server(Duration::from_millis(250), trailer, object.len());
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig {
        request_timeout_ms: 25,
        ..BrowserRuntimeConfig::default()
    })
    .expect("short timeouts should be supported");

    let error = runtime()
        .block_on(session.read_parquet_footer(&source))
        .expect_err("stalled footer reads should time out");

    let requests = finish_requests(server, requests, 2);
    assert_eq!(
        requests[0].headers.get("range"),
        Some(&"bytes=-8".to_string())
    );
    assert_eq!(requests[1].headers.get("range"), Some(&expected_range));
    assert_eq!(error.code, QueryErrorCode::ExecutionFailed);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn probe_preserves_http_range_reader_errors() {
    let (url, _, server) = spawn_test_server(|_| TestResponse {
        status_line: "401 Unauthorized",
        headers: vec![("Content-Length".to_string(), "0".to_string())],
        body: Vec::new(),
    });
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let error = runtime()
        .block_on(session.probe(&source, HttpByteRange::Full))
        .expect_err("unauthorized probes should fail");

    server.join().expect("test server should shut down cleanly");
    assert_eq!(error.code, QueryErrorCode::AccessDenied);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn read_parquet_footer_bootstraps_raw_footer_bytes_from_loopback_http() {
    let footer = b"serialized-parquet-footer";
    let prefix = b"row-group-bytes";
    let object = parquet_like_object(prefix, footer);
    let footer_offset = prefix.len() as u64;
    let expected_range = format!(
        "bytes={footer_offset}-{}",
        footer_offset + footer.len() as u64 - 1
    );
    let (url, requests, server) = spawn_multi_request_server(2, move |request, _| {
        full_or_ranged_response(request, &object)
    });
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let result = runtime()
        .block_on(session.read_parquet_footer(&source))
        .expect("footer bootstrap should succeed");

    let requests = finish_requests(server, requests, 2);
    assert_eq!(
        requests[0].headers.get("range"),
        Some(&"bytes=-8".to_string())
    );
    assert_eq!(requests[1].headers.get("range"), Some(&expected_range));
    assert_eq!(
        result.object_size_bytes(),
        (prefix.len() + footer.len() + 8) as u64
    );
    assert_eq!(result.footer_length_bytes(), footer.len() as u32);
    assert_eq!(result.footer_bytes(), footer);
}

#[test]
fn read_parquet_footer_for_file_bootstraps_raw_footer_bytes_from_loopback_http() {
    let footer = b"serialized-parquet-footer";
    let prefix = b"row-group-bytes";
    let object = parquet_like_object(prefix, footer);
    let object_for_server = object.clone();
    let (url, _, server) = spawn_multi_request_server(2, move |request, _| {
        full_or_ranged_response(request, &object_for_server)
    });
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let file = MaterializedBrowserFile::new(
        "part-000.parquet",
        object.len() as u64,
        BTreeMap::new(),
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests"),
    );

    let result = runtime()
        .block_on(session.read_parquet_footer_for_file(&file))
        .expect("file-driven footer bootstrap should succeed");

    server.join().expect("test server should shut down cleanly");
    assert_eq!(result.object_size_bytes(), file.size_bytes());
    assert_eq!(result.footer_length_bytes(), footer.len() as u32);
    assert_eq!(result.footer_bytes(), footer);
}

#[test]
fn read_parquet_footer_for_file_rejects_descriptor_size_mismatches() {
    let footer = b"serialized-parquet-footer";
    let prefix = b"row-group-bytes";
    let object = parquet_like_object(prefix, footer);
    let object_for_server = object.clone();
    let (url, _, server) = spawn_multi_request_server(2, move |request, _| {
        full_or_ranged_response(request, &object_for_server)
    });
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let file = MaterializedBrowserFile::new(
        "part-000.parquet",
        object.len() as u64 + 1,
        BTreeMap::new(),
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests"),
    );

    let error = runtime()
        .block_on(session.read_parquet_footer_for_file(&file))
        .expect_err("descriptor size mismatches should fail");

    server.join().expect("test server should shut down cleanly");
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert!(error.message.contains(file.path()));
}

#[test]
fn read_parquet_metadata_for_file_rejects_malformed_footer_metadata() {
    let footer = b"not-a-valid-parquet-footer";
    let object = parquet_like_object(b"row-group-bytes", footer);
    let object_for_server = object.clone();
    let (url, _, server) = spawn_multi_request_server(2, move |request, _| {
        full_or_ranged_response(request, &object_for_server)
    });
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let file = MaterializedBrowserFile::new(
        "part-000.parquet",
        object.len() as u64,
        BTreeMap::new(),
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests"),
    );

    let error = runtime()
        .block_on(session.read_parquet_metadata_for_file(&file))
        .expect_err("malformed footer metadata should fail");

    server.join().expect("test server should shut down cleanly");
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert!(error.message.contains(file.path()));
}

#[test]
fn validate_uniform_schema_rejects_mixed_file_field_layouts() {
    let snapshot = BootstrappedBrowserSnapshot::new(
        "gs://axon-fixtures/mixed",
        5,
        vec![
            bootstrapped_file(
                "part-000.parquet",
                100,
                BTreeMap::new(),
                BrowserParquetFileMetadata {
                    object_size_bytes: 100,
                    footer_length_bytes: 32,
                    row_group_count: 1,
                    row_count: 3,
                    fields: vec![required_field("id", BrowserParquetPhysicalType::Int32)],
                    field_stats: BTreeMap::new(),
                },
            ),
            bootstrapped_file(
                "part-001.parquet",
                120,
                BTreeMap::new(),
                BrowserParquetFileMetadata {
                    object_size_bytes: 120,
                    footer_length_bytes: 40,
                    row_group_count: 1,
                    row_count: 4,
                    fields: vec![required_field("id", BrowserParquetPhysicalType::Int64)],
                    field_stats: BTreeMap::new(),
                },
            ),
        ],
    )
    .expect("duplicate-free snapshots should construct");

    let error = snapshot
        .validate_uniform_schema()
        .expect_err("mixed schemas should fail validation");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("part-001.parquet"));
}

#[test]
fn validate_uniform_schema_rejects_mixed_logical_types_with_the_same_physical_type() {
    let snapshot = BootstrappedBrowserSnapshot::new(
        "gs://axon-fixtures/mixed-logical",
        6,
        vec![
            bootstrapped_file(
                "part-000.parquet",
                100,
                BTreeMap::from([("category".to_string(), Some("A".to_string()))]),
                BrowserParquetFileMetadata {
                    object_size_bytes: 100,
                    footer_length_bytes: 32,
                    row_group_count: 1,
                    row_count: 3,
                    fields: vec![BrowserParquetField {
                        name: "name".to_string(),
                        physical_type: BrowserParquetPhysicalType::ByteArray,
                        logical_type: Some(BrowserParquetLogicalType::String),
                        converted_type: Some(BrowserParquetConvertedType::Utf8),
                        repetition: BrowserParquetRepetition::Optional,
                        nullable: true,
                        max_definition_level: 1,
                        max_repetition_level: 0,
                        type_length: None,
                        precision: None,
                        scale: None,
                    }],
                    field_stats: BTreeMap::new(),
                },
            ),
            bootstrapped_file(
                "part-001.parquet",
                120,
                BTreeMap::from([("category".to_string(), Some("B".to_string()))]),
                BrowserParquetFileMetadata {
                    object_size_bytes: 120,
                    footer_length_bytes: 40,
                    row_group_count: 1,
                    row_count: 4,
                    fields: vec![BrowserParquetField {
                        name: "name".to_string(),
                        physical_type: BrowserParquetPhysicalType::ByteArray,
                        logical_type: None,
                        converted_type: None,
                        repetition: BrowserParquetRepetition::Optional,
                        nullable: true,
                        max_definition_level: 1,
                        max_repetition_level: 0,
                        type_length: None,
                        precision: None,
                        scale: None,
                    }],
                    field_stats: BTreeMap::new(),
                },
            ),
        ],
    )
    .expect("duplicate-free snapshots should construct");

    let error = snapshot
        .validate_uniform_schema()
        .expect_err("logical-type mismatches should fail validation");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("part-001.parquet"));
}

#[test]
fn summarize_reports_sorted_partition_columns() {
    let summary = BootstrappedBrowserSnapshot::new(
        "gs://axon-fixtures/partitioned",
        7,
        vec![bootstrapped_file(
            "part-000.parquet",
            100,
            BTreeMap::from([
                ("region".to_string(), Some("us-east-1".to_string())),
                ("category".to_string(), Some("A".to_string())),
            ]),
            BrowserParquetFileMetadata {
                object_size_bytes: 100,
                footer_length_bytes: 32,
                row_group_count: 1,
                row_count: 3,
                fields: vec![required_field("id", BrowserParquetPhysicalType::Int32)],
                field_stats: BTreeMap::new(),
            },
        )],
    )
    .expect("duplicate-free snapshots should construct")
    .summarize()
    .expect("uniform snapshots should summarize");

    assert_eq!(
        summary.schema.partition_columns,
        vec!["category".to_string(), "region".to_string()]
    );
}

#[test]
fn bootstrap_snapshot_metadata_enforces_the_snapshot_preflight_deadline() {
    let footer = b"serialized-parquet-footer";
    let prefix = b"row-group-bytes";
    let object = parquet_like_object(prefix, footer);
    let object_for_server = object.clone();
    let (url, _, server) = spawn_multi_request_server(2, move |request, _| {
        thread::sleep(Duration::from_millis(70));
        full_or_ranged_response(request, &object_for_server)
    });
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig {
        request_timeout_ms: 200,
        snapshot_preflight_timeout_ms: 100,
        snapshot_preflight_max_concurrency: 1,
        ..BrowserRuntimeConfig::default()
    })
    .expect("preflight timeout configuration should be supported");
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let snapshot = MaterializedBrowserSnapshot::new(
        "gs://axon-fixtures/sample_table",
        9,
        vec![MaterializedBrowserFile::new(
            "part-000.parquet",
            object.len() as u64,
            BTreeMap::new(),
            source,
        )],
    )
    .expect("duplicate-free snapshots should construct");

    let error = runtime()
        .block_on(session.bootstrap_snapshot_metadata(&snapshot))
        .expect_err("snapshot preflight should fail once the batch deadline is exceeded");

    server.join().expect("test server should shut down cleanly");
    assert_eq!(error.code, QueryErrorCode::ExecutionFailed);
    assert!(error.message.contains("snapshot preflight"));
}

#[test]
fn bootstrapped_file_constructor_rejects_metadata_size_mismatches() {
    let error = BootstrappedBrowserFile::new(
        "part-000.parquet",
        99,
        BTreeMap::new(),
        BrowserParquetFileMetadata {
            object_size_bytes: 100,
            footer_length_bytes: 32,
            row_group_count: 1,
            row_count: 3,
            fields: vec![required_field("id", BrowserParquetPhysicalType::Int32)],
            field_stats: BTreeMap::new(),
        },
    )
    .expect_err("bootstrapped file sizes should align with metadata sizes");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("part-000.parquet"));
}

#[test]
fn bootstrapped_snapshot_constructor_rejects_duplicate_paths() {
    let error = BootstrappedBrowserSnapshot::new(
        "gs://axon-fixtures/duplicate-paths",
        11,
        vec![
            bootstrapped_file(
                "part-000.parquet",
                100,
                BTreeMap::new(),
                BrowserParquetFileMetadata {
                    object_size_bytes: 100,
                    footer_length_bytes: 32,
                    row_group_count: 1,
                    row_count: 3,
                    fields: vec![required_field("id", BrowserParquetPhysicalType::Int32)],
                    field_stats: BTreeMap::new(),
                },
            ),
            bootstrapped_file(
                "part-000.parquet",
                120,
                BTreeMap::new(),
                BrowserParquetFileMetadata {
                    object_size_bytes: 120,
                    footer_length_bytes: 40,
                    row_group_count: 1,
                    row_count: 4,
                    fields: vec![required_field("id", BrowserParquetPhysicalType::Int32)],
                    field_stats: BTreeMap::new(),
                },
            ),
        ],
    )
    .expect_err("duplicate paths should be rejected");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("part-000.parquet"));
}

#[test]
fn read_parquet_footer_rejects_objects_smaller_than_the_parquet_trailer() {
    let (url, requests, server) =
        spawn_test_server(|request| full_or_ranged_response(request, b"small"));
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let error = runtime()
        .block_on(session.read_parquet_footer(&source))
        .expect_err("objects shorter than 8 bytes should fail");

    let request = finish_request(server, requests);
    assert_eq!(request.headers.get("range"), Some(&"bytes=-8".to_string()));
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn read_parquet_footer_rejects_missing_trailing_magic() {
    let footer = b"serialized-parquet-footer";
    let mut object = b"row-group-bytes".to_vec();
    object.extend_from_slice(footer);
    object.extend_from_slice(&(footer.len() as u32).to_le_bytes());
    object.extend_from_slice(b"NOPE");
    let (url, requests, server) =
        spawn_test_server(move |request| full_or_ranged_response(request, &object));
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let error = runtime()
        .block_on(session.read_parquet_footer(&source))
        .expect_err("footer bootstrap should reject missing trailing magic");

    let request = finish_request(server, requests);
    assert_eq!(request.headers.get("range"), Some(&"bytes=-8".to_string()));
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn read_parquet_footer_rejects_zero_length_footers() {
    let mut object = b"row-group-bytes".to_vec();
    object.extend_from_slice(&0_u32.to_le_bytes());
    object.extend_from_slice(PARQUET_MAGIC);
    let (url, requests, server) =
        spawn_test_server(move |request| full_or_ranged_response(request, &object));
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let error = runtime()
        .block_on(session.read_parquet_footer(&source))
        .expect_err("zero-length footers should fail");

    let request = finish_request(server, requests);
    assert_eq!(request.headers.get("range"), Some(&"bytes=-8".to_string()));
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn read_parquet_footer_rejects_footer_lengths_that_point_outside_the_object() {
    let mut object = b"row-group-bytes".to_vec();
    object.extend_from_slice(b"tiny");
    object.extend_from_slice(&64_u32.to_le_bytes());
    object.extend_from_slice(PARQUET_MAGIC);
    let (url, requests, server) =
        spawn_test_server(move |request| full_or_ranged_response(request, &object));
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let error = runtime()
        .block_on(session.read_parquet_footer(&source))
        .expect_err("footer ranges that precede byte zero should fail");

    let request = finish_request(server, requests);
    assert_eq!(request.headers.get("range"), Some(&"bytes=-8".to_string()));
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn read_parquet_footer_rejects_oversized_footers_before_fetching_them() {
    let declared_footer_length = 20_u32 * 1024 * 1024;
    let object_size = u64::from(declared_footer_length) + 8;
    let trailer = {
        let mut trailer = declared_footer_length.to_le_bytes().to_vec();
        trailer.extend_from_slice(PARQUET_MAGIC);
        trailer
    };
    let (url, requests, server) = spawn_test_server(move |request| {
        assert_eq!(request.headers.get("range"), Some(&"bytes=-8".to_string()));
        TestResponse {
            status_line: "206 Partial Content",
            headers: vec![
                ("Content-Length".to_string(), trailer.len().to_string()),
                (
                    "Content-Range".to_string(),
                    format!(
                        "bytes {}-{}/{}",
                        object_size - 8,
                        object_size - 1,
                        object_size
                    ),
                ),
            ],
            body: trailer.clone(),
        }
    });
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let error = runtime()
        .block_on(session.read_parquet_footer(&source))
        .expect_err("oversized footers should fail before the second fetch");

    let request = finish_request(server, requests);
    assert_eq!(request.headers.get("range"), Some(&"bytes=-8".to_string()));
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn read_parquet_footer_rejects_footer_reads_when_object_size_changes_between_requests() {
    let footer = b"serialized-parquet-footer";
    let prefix = b"row-group-bytes";
    let object = parquet_like_object(prefix, footer);
    let footer_offset = prefix.len() as u64;
    let expected_range = format!(
        "bytes={footer_offset}-{}",
        footer_offset + footer.len() as u64 - 1
    );
    let trailer = object[object.len() - 8..].to_vec();
    let object_len = object.len() as u64;
    let changed_object_len = object_len + 1;
    let expected_range_in_server = expected_range.clone();
    let (url, requests, server) =
        spawn_multi_request_server(2, move |request, index| match index {
            0 => {
                assert_eq!(request.headers.get("range"), Some(&"bytes=-8".to_string()));
                TestResponse {
                    status_line: "206 Partial Content",
                    headers: vec![
                        ("Content-Length".to_string(), trailer.len().to_string()),
                        (
                            "Content-Range".to_string(),
                            format!("bytes {}-{}/{}", object_len - 8, object_len - 1, object_len),
                        ),
                    ],
                    body: trailer.clone(),
                }
            }
            1 => {
                assert_eq!(
                    request.headers.get("range"),
                    Some(&expected_range_in_server)
                );
                TestResponse {
                    status_line: "206 Partial Content",
                    headers: vec![
                        ("Content-Length".to_string(), footer.len().to_string()),
                        (
                            "Content-Range".to_string(),
                            format!(
                                "bytes {footer_offset}-{}",
                                footer_offset + footer.len() as u64 - 1
                            ) + &format!("/{changed_object_len}"),
                        ),
                    ],
                    body: footer.to_vec(),
                }
            }
            _ => unreachable!("only two requests are expected"),
        });
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let error = runtime()
        .block_on(session.read_parquet_footer(&source))
        .expect_err("object-size changes between footer reads should fail");

    let requests = finish_requests(server, requests, 2);
    assert_eq!(
        requests[0].headers.get("range"),
        Some(&"bytes=-8".to_string())
    );
    assert_eq!(requests[1].headers.get("range"), Some(&expected_range));
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn read_parquet_footer_rejects_footer_reads_when_entity_tag_changes_between_requests() {
    let footer = b"serialized-parquet-footer";
    let prefix = b"row-group-bytes";
    let object = parquet_like_object(prefix, footer);
    let footer_offset = prefix.len() as u64;
    let expected_range = format!(
        "bytes={footer_offset}-{}",
        footer_offset + footer.len() as u64 - 1
    );
    let trailer = object[object.len() - 8..].to_vec();
    let object_len = object.len();
    let expected_range_in_server = expected_range.clone();
    let (url, requests, server) =
        spawn_multi_request_server(2, move |request, index| match index {
            0 => {
                assert_eq!(request.headers.get("range"), Some(&"bytes=-8".to_string()));
                TestResponse {
                    status_line: "206 Partial Content",
                    headers: vec![
                        ("Content-Length".to_string(), trailer.len().to_string()),
                        (
                            "Content-Range".to_string(),
                            format!("bytes {}-{}/{}", object_len - 8, object_len - 1, object_len),
                        ),
                        ("ETag".to_string(), "\"v1\"".to_string()),
                    ],
                    body: trailer.clone(),
                }
            }
            1 => {
                assert_eq!(
                    request.headers.get("range"),
                    Some(&expected_range_in_server)
                );
                TestResponse {
                    status_line: "206 Partial Content",
                    headers: vec![
                        ("Content-Length".to_string(), footer.len().to_string()),
                        (
                            "Content-Range".to_string(),
                            format!(
                                "bytes {footer_offset}-{}",
                                footer_offset + footer.len() as u64 - 1
                            ) + &format!("/{}", object_len),
                        ),
                        ("ETag".to_string(), "\"v2\"".to_string()),
                    ],
                    body: footer.to_vec(),
                }
            }
            _ => unreachable!("only two requests are expected"),
        });
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let error = runtime()
        .block_on(session.read_parquet_footer(&source))
        .expect_err("entity-tag changes between footer reads should fail");

    let requests = finish_requests(server, requests, 2);
    assert_eq!(
        requests[0].headers.get("range"),
        Some(&"bytes=-8".to_string())
    );
    assert_eq!(requests[1].headers.get("range"), Some(&expected_range));
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn read_parquet_footer_preserves_http_range_reader_errors_from_the_trailer_read() {
    let (url, requests, server) = spawn_test_server(|request| {
        assert_eq!(request.headers.get("range"), Some(&"bytes=-8".to_string()));
        TestResponse {
            status_line: "401 Unauthorized",
            headers: vec![("Content-Length".to_string(), "0".to_string())],
            body: Vec::new(),
        }
    });
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let error = runtime()
        .block_on(session.read_parquet_footer(&source))
        .expect_err("trailer-read failures should surface");

    let request = finish_request(server, requests);
    assert_eq!(request.headers.get("range"), Some(&"bytes=-8".to_string()));
    assert_eq!(error.code, QueryErrorCode::AccessDenied);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn read_parquet_footer_preserves_http_range_reader_errors_from_the_footer_read() {
    let footer = b"serialized-parquet-footer";
    let prefix = b"row-group-bytes";
    let object = parquet_like_object(prefix, footer);
    let footer_offset = prefix.len() as u64;
    let expected_range = format!(
        "bytes={footer_offset}-{}",
        footer_offset + footer.len() as u64 - 1
    );
    let trailer = object[object.len() - 8..].to_vec();
    let object_len = object.len();
    let expected_range_in_server = expected_range.clone();
    let (url, requests, server) =
        spawn_multi_request_server(2, move |request, index| match index {
            0 => {
                assert_eq!(request.headers.get("range"), Some(&"bytes=-8".to_string()));
                TestResponse {
                    status_line: "206 Partial Content",
                    headers: vec![
                        ("Content-Length".to_string(), trailer.len().to_string()),
                        (
                            "Content-Range".to_string(),
                            format!("bytes {}-{}/{}", object_len - 8, object_len - 1, object_len),
                        ),
                    ],
                    body: trailer.clone(),
                }
            }
            1 => {
                assert_eq!(
                    request.headers.get("range"),
                    Some(&expected_range_in_server)
                );
                TestResponse {
                    status_line: "200 OK",
                    headers: vec![("Content-Length".to_string(), footer.len().to_string())],
                    body: footer.to_vec(),
                }
            }
            _ => unreachable!("only two requests are expected"),
        });
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let error = runtime()
        .block_on(session.read_parquet_footer(&source))
        .expect_err("footer-read failures should surface");

    let requests = finish_requests(server, requests, 2);
    assert_eq!(
        requests[0].headers.get("range"),
        Some(&"bytes=-8".to_string())
    );
    assert_eq!(requests[1].headers.get("range"), Some(&expected_range));
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("tokio runtime should be created for tests")
}

#[derive(Debug)]
struct CapturedRequest {
    headers: BTreeMap<String, String>,
}

struct TestResponse {
    status_line: &'static str,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

fn spawn_test_server<F>(handler: F) -> (String, Receiver<CapturedRequest>, JoinHandle<()>)
where
    F: FnOnce(&CapturedRequest) -> TestResponse + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral port should bind");
    let address = listener.local_addr().expect("listener addr should resolve");
    let url = format!("http://{address}/object");
    let (request_tx, request_rx) = mpsc::channel();

    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("test client should connect");
        let request = read_request(&mut stream);
        let response = handler(&request);
        write_response(&mut stream, response);
        let _ = request_tx.send(request);
    });

    (url, request_rx, server)
}

fn spawn_stalling_server(delay: Duration) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral port should bind");
    let address = listener.local_addr().expect("listener addr should resolve");
    let url = format!("http://{address}/object");

    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("test client should connect");
        let mut buffer = [0_u8; 4096];
        let _ = stream.read(&mut buffer);
        thread::sleep(delay);
    });

    (url, server)
}

fn spawn_stalling_footer_read_server(
    delay: Duration,
    trailer: Vec<u8>,
    object_len: usize,
) -> (String, Receiver<CapturedRequest>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral port should bind");
    let address = listener.local_addr().expect("listener addr should resolve");
    let url = format!("http://{address}/object");
    let (request_tx, request_rx) = mpsc::channel();

    let server = thread::spawn(move || {
        {
            let (mut trailer_stream, _) = listener.accept().expect("test client should connect");
            let trailer_request = read_request(&mut trailer_stream);
            write_response(
                &mut trailer_stream,
                TestResponse {
                    status_line: "206 Partial Content",
                    headers: vec![
                        ("Content-Length".to_string(), trailer.len().to_string()),
                        (
                            "Content-Range".to_string(),
                            format!("bytes {}-{}/{}", object_len - 8, object_len - 1, object_len),
                        ),
                    ],
                    body: trailer,
                },
            );
            let _ = request_tx.send(trailer_request);
        }

        let (mut footer_stream, _) = listener.accept().expect("test client should connect");
        let footer_request = read_request(&mut footer_stream);
        let _ = request_tx.send(footer_request);
        thread::sleep(delay);
    });

    (url, request_rx, server)
}

fn finish_request(server: JoinHandle<()>, requests: Receiver<CapturedRequest>) -> CapturedRequest {
    server.join().expect("test server should shut down cleanly");
    requests
        .recv()
        .expect("test should receive the captured request")
}

fn finish_requests(
    server: JoinHandle<()>,
    requests: Receiver<CapturedRequest>,
    expected_count: usize,
) -> Vec<CapturedRequest> {
    server.join().expect("test server should shut down cleanly");
    (0..expected_count)
        .map(|_| {
            requests
                .recv()
                .expect("test should receive the captured request")
        })
        .collect()
}

fn read_request(stream: &mut std::net::TcpStream) -> CapturedRequest {
    let mut buffer = [0_u8; 4096];
    let mut request = Vec::new();

    loop {
        let bytes_read = stream
            .read(&mut buffer)
            .expect("request bytes should be readable");
        if bytes_read == 0 {
            break;
        }
        request.extend_from_slice(&buffer[..bytes_read]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }

    let text = String::from_utf8(request).expect("request should be valid ASCII");
    let mut lines = text.split("\r\n");
    let _request_line = lines.next().expect("request line should be present");
    let mut headers = BTreeMap::new();

    for line in lines.take_while(|line| !line.is_empty()) {
        let (name, value) = line
            .split_once(':')
            .expect("header line should contain a colon");
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }

    CapturedRequest { headers }
}

fn write_response(stream: &mut std::net::TcpStream, response: TestResponse) {
    if let Err(error) = try_write_response(stream, response) {
        if !is_expected_client_disconnect(&error) {
            panic!("response should be writable: {error}");
        }
    }
}

fn try_write_response(
    stream: &mut std::net::TcpStream,
    response: TestResponse,
) -> std::io::Result<()> {
    write!(stream, "HTTP/1.1 {}\r\n", response.status_line)?;
    for (header, value) in response.headers {
        write!(stream, "{header}: {value}\r\n")?;
    }
    write!(stream, "\r\n")?;
    stream.write_all(&response.body)?;
    stream.flush()
}

fn is_expected_client_disconnect(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::NotConnected
    )
}

fn spawn_multi_request_server<F>(
    request_count: usize,
    handler: F,
) -> (String, Receiver<CapturedRequest>, JoinHandle<()>)
where
    F: Fn(&CapturedRequest, usize) -> TestResponse + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral port should bind");
    let address = listener.local_addr().expect("listener addr should resolve");
    let url = format!("http://{address}/object");
    let (request_tx, request_rx) = mpsc::channel();

    let server = thread::spawn(move || {
        for index in 0..request_count {
            let (mut stream, _) = listener.accept().expect("test client should connect");
            let request = read_request(&mut stream);
            let response = handler(&request, index);
            write_response(&mut stream, response);
            let _ = request_tx.send(request);
        }
    });

    (url, request_rx, server)
}

fn full_or_ranged_response(request: &CapturedRequest, body: &[u8]) -> TestResponse {
    let Some(range_header) = request.headers.get("range") else {
        return TestResponse {
            status_line: "200 OK",
            headers: vec![("Content-Length".to_string(), body.len().to_string())],
            body: body.to_vec(),
        };
    };

    let (start, end) = resolve_range(range_header, body.len());
    if start > end || end >= body.len() {
        return TestResponse {
            status_line: "416 Range Not Satisfiable",
            headers: vec![
                ("Content-Length".to_string(), "0".to_string()),
                (
                    "Content-Range".to_string(),
                    format!("bytes */{}", body.len()),
                ),
            ],
            body: Vec::new(),
        };
    }

    let ranged = body[start..=end].to_vec();
    TestResponse {
        status_line: "206 Partial Content",
        headers: vec![
            ("Content-Length".to_string(), ranged.len().to_string()),
            (
                "Content-Range".to_string(),
                format!("bytes {start}-{end}/{}", body.len()),
            ),
        ],
        body: ranged,
    }
}

fn resolve_range(range_header: &str, object_len: usize) -> (usize, usize) {
    let range = range_header
        .strip_prefix("bytes=")
        .expect("test server expects byte ranges");

    if let Some(suffix) = range.strip_prefix('-') {
        let suffix_len = suffix.parse::<usize>().expect("suffix length should parse");
        let start = object_len.saturating_sub(suffix_len);
        return (start, object_len.saturating_sub(1));
    }

    let (start, end) = range
        .split_once('-')
        .expect("range should contain a start/end separator");
    let start = start.parse::<usize>().expect("range start should parse");

    if end.is_empty() {
        return (start, object_len.saturating_sub(1));
    }

    let end = end.parse::<usize>().expect("range end should parse");
    (start, end)
}

fn parquet_like_object(prefix: &[u8], footer: &[u8]) -> Vec<u8> {
    let mut object = prefix.to_vec();
    object.extend_from_slice(footer);
    object.extend_from_slice(&(footer.len() as u32).to_le_bytes());
    object.extend_from_slice(PARQUET_MAGIC);
    object
}

fn execution_plan_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/conformance/browser-execution-plan-corpus.json")
}

fn load_execution_plan_corpus() -> Vec<ExecutionPlanCorpusCase> {
    serde_json::from_str(
        &std::fs::read_to_string(execution_plan_corpus_path())
            .expect("execution-plan corpus should be readable"),
    )
    .expect("execution-plan corpus should deserialize")
}

fn expected_aggregate_function(name: &str) -> BrowserAggregateFunction {
    match name {
        "avg" => BrowserAggregateFunction::Avg,
        "array_agg" => BrowserAggregateFunction::ArrayAgg,
        "bool_and" => BrowserAggregateFunction::BoolAnd,
        "bool_or" => BrowserAggregateFunction::BoolOr,
        "count_star" => BrowserAggregateFunction::CountStar,
        "count" => BrowserAggregateFunction::Count,
        "sum" => BrowserAggregateFunction::Sum,
        "min" => BrowserAggregateFunction::Min,
        "max" => BrowserAggregateFunction::Max,
        other => panic!("unsupported expected aggregate function '{other}'"),
    }
}

fn assert_filter_expr_matches(
    actual: &BrowserFilterExpr,
    expected: &ExpectedFilterExpr,
    case_name: &str,
) {
    match (actual, expected) {
        (BrowserFilterExpr::And { children }, ExpectedFilterExpr::And { children: expected }) => {
            assert_eq!(
                children.len(),
                expected.len(),
                "case '{}': filter child count should match",
                case_name
            );
            for (actual, expected) in children.iter().zip(expected.iter()) {
                assert_filter_expr_matches(actual, expected, case_name);
            }
        }
        (
            BrowserFilterExpr::Compare {
                source_column,
                comparison,
                literal,
            },
            ExpectedFilterExpr::Compare {
                source_column: expected_source_column,
                comparison: expected_comparison,
                literal: expected_literal,
            },
        ) => {
            assert_eq!(
                source_column, expected_source_column,
                "case '{}': filter source column should match",
                case_name
            );
            assert_eq!(
                comparison,
                &expected_comparison_op(expected_comparison),
                "case '{}': filter comparison should match",
                case_name
            );
            assert_scalar_value_matches(literal, expected_literal, case_name);
        }
        (
            BrowserFilterExpr::InList {
                source_column,
                literals,
            },
            ExpectedFilterExpr::InList {
                source_column: expected_source_column,
                literals: expected_literals,
            },
        ) => {
            assert_eq!(
                source_column, expected_source_column,
                "case '{}': IN-list source column should match",
                case_name
            );
            assert_eq!(
                literals.len(),
                expected_literals.len(),
                "case '{}': IN-list literal count should match",
                case_name
            );
            for (actual, expected) in literals.iter().zip(expected_literals.iter()) {
                assert_scalar_value_matches(actual, expected, case_name);
            }
        }
        (
            BrowserFilterExpr::IsNull { source_column },
            ExpectedFilterExpr::IsNull {
                source_column: expected_source_column,
            },
        )
        | (
            BrowserFilterExpr::IsNotNull { source_column },
            ExpectedFilterExpr::IsNotNull {
                source_column: expected_source_column,
            },
        ) => {
            assert_eq!(
                source_column, expected_source_column,
                "case '{}': null-check source column should match",
                case_name
            );
        }
        _ => panic!(
            "case '{}': execution-plan filter shape did not match expected {:?}",
            case_name, expected
        ),
    }
}

fn expected_comparison_op(name: &str) -> BrowserComparisonOp {
    match name {
        "eq" => BrowserComparisonOp::Eq,
        "gt" => BrowserComparisonOp::Gt,
        "gte" => BrowserComparisonOp::Gte,
        "lt" => BrowserComparisonOp::Lt,
        "lte" => BrowserComparisonOp::Lte,
        other => panic!("unsupported expected comparison '{other}'"),
    }
}

fn assert_scalar_value_matches(
    actual: &BrowserScalarValue,
    expected: &ExpectedScalarValue,
    case_name: &str,
) {
    match (actual, expected) {
        (BrowserScalarValue::Int64(actual), ExpectedScalarValue::Int64 { value }) => {
            assert_eq!(
                actual, value,
                "case '{}': int64 scalar value should match",
                case_name
            );
        }
        (BrowserScalarValue::String(actual), ExpectedScalarValue::String { value }) => {
            assert_eq!(
                actual, value,
                "case '{}': string scalar value should match",
                case_name
            );
        }
        (BrowserScalarValue::Boolean(actual), ExpectedScalarValue::Boolean { value }) => {
            assert_eq!(
                actual, value,
                "case '{}': boolean scalar value should match",
                case_name
            );
        }
        (BrowserScalarValue::Null, ExpectedScalarValue::Null) => {}
        _ => panic!(
            "case '{}': scalar value {:?} did not match expected {:?}",
            case_name, actual, expected
        ),
    }
}

fn bootstrapped_file(
    path: &str,
    size_bytes: u64,
    partition_values: BTreeMap<String, Option<String>>,
    metadata: BrowserParquetFileMetadata,
) -> BootstrappedBrowserFile {
    BootstrappedBrowserFile::new_with_object_etag(
        path,
        size_bytes,
        partition_values,
        metadata,
        Some(format!("\"{path}\"")),
    )
    .expect("valid bootstrapped files should construct")
}

fn bootstrapped_file_without_etag(
    path: &str,
    size_bytes: u64,
    partition_values: BTreeMap<String, Option<String>>,
    metadata: BrowserParquetFileMetadata,
) -> BootstrappedBrowserFile {
    BootstrappedBrowserFile::new(path, size_bytes, partition_values, metadata)
        .expect("valid bootstrapped files should construct")
}

fn bootstrapped_file_with_etag(
    path: &str,
    size_bytes: u64,
    partition_values: BTreeMap<String, Option<String>>,
    metadata: BrowserParquetFileMetadata,
    object_etag: Option<&str>,
) -> BootstrappedBrowserFile {
    BootstrappedBrowserFile::new_with_object_etag(
        path,
        size_bytes,
        partition_values,
        metadata,
        object_etag.map(str::to_string),
    )
    .expect("valid bootstrapped files should construct")
}

fn required_field(name: &str, physical_type: BrowserParquetPhysicalType) -> BrowserParquetField {
    BrowserParquetField {
        name: name.to_string(),
        physical_type,
        logical_type: None,
        converted_type: None,
        repetition: BrowserParquetRepetition::Required,
        nullable: false,
        max_definition_level: 0,
        max_repetition_level: 0,
        type_length: None,
        precision: None,
        scale: None,
    }
}

fn sample_bootstrapped_snapshot() -> BootstrappedBrowserSnapshot {
    BootstrappedBrowserSnapshot::new_with_partition_column_types(
        "gs://axon-fixtures/sample_table",
        7,
        vec![
            bootstrapped_file(
                "category=A/part-000.parquet",
                100,
                BTreeMap::from([("category".to_string(), Some("A".to_string()))]),
                BrowserParquetFileMetadata {
                    object_size_bytes: 100,
                    footer_length_bytes: 32,
                    row_group_count: 1,
                    row_count: 2,
                    fields: vec![
                        required_field("id", BrowserParquetPhysicalType::Int32),
                        required_field("value", BrowserParquetPhysicalType::Int32),
                    ],
                    field_stats: BTreeMap::new(),
                },
            ),
            bootstrapped_file(
                "category=B/part-001.parquet",
                120,
                BTreeMap::from([("category".to_string(), Some("B".to_string()))]),
                BrowserParquetFileMetadata {
                    object_size_bytes: 120,
                    footer_length_bytes: 40,
                    row_group_count: 1,
                    row_count: 3,
                    fields: vec![
                        required_field("id", BrowserParquetPhysicalType::Int32),
                        required_field("value", BrowserParquetPhysicalType::Int32),
                    ],
                    field_stats: BTreeMap::new(),
                },
            ),
            bootstrapped_file(
                "category=C/part-002.parquet",
                140,
                BTreeMap::from([("category".to_string(), Some("C".to_string()))]),
                BrowserParquetFileMetadata {
                    object_size_bytes: 140,
                    footer_length_bytes: 48,
                    row_group_count: 1,
                    row_count: 4,
                    fields: vec![
                        required_field("id", BrowserParquetPhysicalType::Int32),
                        required_field("value", BrowserParquetPhysicalType::Int32),
                    ],
                    field_stats: BTreeMap::new(),
                },
            ),
        ],
        BTreeMap::from([("category".to_string(), PartitionColumnType::String)]),
    )
    .expect("sample bootstrapped snapshots should construct")
}

fn null_partition_bootstrapped_snapshot() -> BootstrappedBrowserSnapshot {
    BootstrappedBrowserSnapshot::new_with_partition_column_types(
        "gs://axon-fixtures/null_partition_table",
        9,
        vec![
            bootstrapped_file(
                "region=NULL/part-000.parquet",
                80,
                BTreeMap::from([("region".to_string(), None)]),
                BrowserParquetFileMetadata {
                    object_size_bytes: 80,
                    footer_length_bytes: 24,
                    row_group_count: 1,
                    row_count: 2,
                    fields: vec![required_field("id", BrowserParquetPhysicalType::Int32)],
                    field_stats: BTreeMap::new(),
                },
            ),
            bootstrapped_file(
                "region=us-east/part-001.parquet",
                90,
                BTreeMap::from([("region".to_string(), Some("us-east".to_string()))]),
                BrowserParquetFileMetadata {
                    object_size_bytes: 90,
                    footer_length_bytes: 28,
                    row_group_count: 1,
                    row_count: 3,
                    fields: vec![required_field("id", BrowserParquetPhysicalType::Int32)],
                    field_stats: BTreeMap::new(),
                },
            ),
        ],
        BTreeMap::from([("region".to_string(), PartitionColumnType::String)]),
    )
    .expect("null-partition bootstrapped snapshots should construct")
}

fn stats_bootstrapped_snapshot() -> BootstrappedBrowserSnapshot {
    BootstrappedBrowserSnapshot::new_with_partition_column_types(
        "gs://axon-fixtures/stats_table",
        10,
        vec![
            bootstrapped_file(
                "category=A/part-000.parquet",
                100,
                BTreeMap::from([("category".to_string(), Some("A".to_string()))]),
                BrowserParquetFileMetadata {
                    object_size_bytes: 100,
                    footer_length_bytes: 32,
                    row_group_count: 1,
                    row_count: 2,
                    fields: vec![
                        required_field("id", BrowserParquetPhysicalType::Int32),
                        required_field("value", BrowserParquetPhysicalType::Int32),
                    ],
                    field_stats: BTreeMap::from([
                        (
                            "id".to_string(),
                            BrowserParquetFieldStats {
                                min_i64: Some(1),
                                max_i64: Some(3),
                                null_count: Some(0),
                            },
                        ),
                        (
                            "value".to_string(),
                            BrowserParquetFieldStats {
                                min_i64: Some(10),
                                max_i64: Some(30),
                                null_count: Some(0),
                            },
                        ),
                    ]),
                },
            ),
            bootstrapped_file(
                "category=B/part-001.parquet",
                120,
                BTreeMap::from([("category".to_string(), Some("B".to_string()))]),
                BrowserParquetFileMetadata {
                    object_size_bytes: 120,
                    footer_length_bytes: 40,
                    row_group_count: 1,
                    row_count: 3,
                    fields: vec![
                        required_field("id", BrowserParquetPhysicalType::Int32),
                        required_field("value", BrowserParquetPhysicalType::Int32),
                    ],
                    field_stats: BTreeMap::from([
                        (
                            "id".to_string(),
                            BrowserParquetFieldStats {
                                min_i64: Some(2),
                                max_i64: Some(5),
                                null_count: Some(0),
                            },
                        ),
                        (
                            "value".to_string(),
                            BrowserParquetFieldStats {
                                min_i64: Some(20),
                                max_i64: Some(50),
                                null_count: Some(0),
                            },
                        ),
                    ]),
                },
            ),
            bootstrapped_file(
                "category=C/part-002.parquet",
                140,
                BTreeMap::from([("category".to_string(), Some("C".to_string()))]),
                BrowserParquetFileMetadata {
                    object_size_bytes: 140,
                    footer_length_bytes: 48,
                    row_group_count: 1,
                    row_count: 4,
                    fields: vec![
                        required_field("id", BrowserParquetPhysicalType::Int32),
                        required_field("value", BrowserParquetPhysicalType::Int32),
                    ],
                    field_stats: BTreeMap::from([
                        (
                            "id".to_string(),
                            BrowserParquetFieldStats {
                                min_i64: Some(6),
                                max_i64: Some(6),
                                null_count: Some(0),
                            },
                        ),
                        (
                            "value".to_string(),
                            BrowserParquetFieldStats {
                                min_i64: Some(60),
                                max_i64: Some(60),
                                null_count: Some(0),
                            },
                        ),
                    ]),
                },
            ),
        ],
        BTreeMap::from([("category".to_string(), PartitionColumnType::String)]),
    )
    .expect("stats bootstrapped snapshots should construct")
}
