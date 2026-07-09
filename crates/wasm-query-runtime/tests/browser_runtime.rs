#![cfg(not(target_arch = "wasm32"))]

#[path = "../../delta-control-plane/tests/support/mod.rs"]
mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use arrow_array::{Array, BooleanArray, Int64Array, StringArray};
use arrow_ipc::reader::StreamReader;
use parquet::data_type::Int64Type;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use query_contract::{
    BrowserAccessMode, BrowserHttpFileDescriptor, BrowserHttpSnapshotDescriptor, CapabilityKey,
    CapabilityReport, CapabilityState, ExecutionTarget, FallbackReason, PartitionColumnType,
    QueryErrorCode, QueryRequest, ResolvedFileDescriptor, ResolvedSnapshotDescriptor,
    SnapshotResolutionRequest,
};
use serde::Deserialize;
use support::TestTableFixture;
use wasm_delta_snapshot::{
    DefaultJsonHandler, DefaultParquetHandler, LocalFileStorageHandler, SnapshotResolver,
};
use wasm_http_object_store::{HttpByteRange, HttpRangeReader};
use wasm_query_runtime::{
    runtime_target, BootstrappedBrowserFile, BootstrappedBrowserSnapshot, BrowserAggregateFunction,
    BrowserComparisonOp, BrowserExecutionBudget, BrowserExecutionOutputKind, BrowserFilterExpr,
    BrowserObjectAccessMode, BrowserObjectSource, BrowserParquetConvertedType, BrowserParquetField,
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
    let fixture = TestTableFixture::create_partitioned();
    let resolver = SnapshotResolver::new(
        LocalFileStorageHandler::default(),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );
    let resolved = runtime()
        .block_on(session.resolve_delta_snapshot(
            &resolver,
            SnapshotResolutionRequest {
                table_uri: fixture.raw_table_path(),
                snapshot_version: None,
            },
        ))
        .expect("delta snapshot should resolve");
    let planning_server = RequestCapturingObjectServer::from_fixture_paths(
        &fixture,
        resolved.active_files.iter().map(|file| file.path.clone()),
    );
    let planning_materialized = session
        .materialize_resolved_snapshot(&resolved, |file| {
            BrowserObjectSource::from_url(planning_server.url_for_path(&file.path))
        })
        .expect("resolved snapshot should materialize into browser object sources");
    let request = QueryRequest::new(
        planning_materialized.table_uri(),
        "SELECT id FROM axon_table WHERE category = 'A'",
        ExecutionTarget::BrowserWasm,
    );
    let prepared = runtime()
        .block_on(session.prepare_execution(&planning_materialized, &request))
        .expect("browser execution should prepare from bootstrapped metadata");
    let candidate_file = prepared
        .bootstrapped_snapshot()
        .active_files()
        .first()
        .expect("category-pruned execution should retain one candidate");
    let expected_etag = format!("\"fixture-{:016x}\"", candidate_file.size_bytes());
    let object_bytes = std::fs::read(fixture.table_root().join(candidate_file.path()))
        .expect("candidate parquet object should be readable");
    drop(planning_server);

    let expected_if_range = expected_etag.clone();
    let (url, requests, server) = spawn_test_server(move |request| {
        assert!(request.headers.contains_key("range"));
        assert_eq!(
            request.headers.get("if-range"),
            Some(&expected_if_range),
            "execution reads should enforce the bootstrapped entity tag",
        );
        full_or_ranged_response_with_custom_etag(request, &object_bytes, "\"v2\"")
    });
    let materialized = MaterializedBrowserSnapshot::new_with_partition_column_types(
        prepared.bootstrapped_snapshot().table_uri(),
        prepared.bootstrapped_snapshot().snapshot_version(),
        vec![MaterializedBrowserFile::new(
            candidate_file.path(),
            candidate_file.size_bytes(),
            candidate_file.partition_values().clone(),
            BrowserObjectSource::from_url(&url)
                .expect("loopback HTTP should be allowed in host tests"),
        )],
        planning_materialized.partition_column_types().clone(),
    )
    .expect("duplicate-free snapshots should construct");

    let error = runtime()
        .block_on(session.execute_plan(&materialized, prepared.execution_plan()))
        .expect_err("bootstrap-to-execution entity-tag drift should fail");

    let request = finish_request(server, requests);
    assert!(request.headers.contains_key("range"));
    assert_eq!(request.headers.get("if-range"), Some(&expected_etag));
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert!(error.message.contains("ETag"));
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
fn plan_query_fails_open_when_partition_values_match_zero_files() {
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
    assert!(!planned.pruning.used_partition_pruning);
    assert_eq!(planned.pruning.files_retained, 3);
    assert_eq!(planned.pruning.files_pruned, 0);
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
fn plan_query_normalizes_typed_partition_filters_by_declared_type() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let snapshot = typed_partition_bootstrapped_snapshot();

    let day = session
        .plan_query(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id FROM axon_table WHERE day = 7 ORDER BY id",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect("integer partition predicates should plan");
    assert_eq!(day.candidate_paths, vec!["day=7/enabled=true/region=US"]);
    assert_eq!(day.candidate_file_count, 1);
    assert_eq!(day.candidate_bytes, 100);
    assert_eq!(day.candidate_rows, 2);
    assert!(day.pruning.used_partition_pruning);
    assert_eq!(day.pruning.files_retained, 1);
    assert_eq!(day.pruning.files_pruned, 2);

    let enabled = session
        .plan_query(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id FROM axon_table WHERE enabled = true ORDER BY id",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect("boolean partition predicates should plan");
    assert_eq!(
        enabled.candidate_paths,
        vec![
            "day=7/enabled=true/region=US",
            "day=-3/enabled=true/region=007"
        ]
    );
    assert_eq!(enabled.candidate_file_count, 2);
    assert_eq!(enabled.candidate_bytes, 240);
    assert_eq!(enabled.candidate_rows, 6);
    assert!(enabled.pruning.used_partition_pruning);
    assert_eq!(enabled.pruning.files_retained, 2);
    assert_eq!(enabled.pruning.files_pruned, 1);

    let region = session
        .plan_query(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id FROM axon_table WHERE region = '007' ORDER BY id",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect("string partition predicates should plan");
    assert_eq!(
        region.candidate_paths,
        vec!["day=-3/enabled=true/region=007"]
    );
    assert_eq!(region.candidate_file_count, 1);
    assert_eq!(region.candidate_bytes, 140);
    assert_eq!(region.candidate_rows, 4);
    assert!(region.pruning.used_partition_pruning);
    assert_eq!(region.pruning.files_retained, 1);
    assert_eq!(region.pruning.files_pruned, 2);
}

#[test]
fn plan_query_fails_open_for_unsafe_typed_partition_values() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    for (name, snapshot, expected_paths) in [
        (
            "malformed",
            unsafe_day_partition_snapshot(
                "gs://axon-fixtures/malformed_day_partition",
                [
                    (
                        "day=007/part-000.parquet",
                        100,
                        2,
                        BTreeMap::from([("day".to_string(), Some("007".to_string()))]),
                    ),
                    (
                        "day=8/part-001.parquet",
                        120,
                        3,
                        BTreeMap::from([("day".to_string(), Some("8".to_string()))]),
                    ),
                ],
                BTreeMap::from([("day".to_string(), PartitionColumnType::Int64)]),
            ),
            ["day=007/part-000.parquet", "day=8/part-001.parquet"],
        ),
        (
            "ambiguous",
            unsafe_day_partition_snapshot(
                "gs://axon-fixtures/ambiguous_day_partition",
                [
                    (
                        "day=7/part-000.parquet",
                        100,
                        2,
                        BTreeMap::from([
                            ("DAY".to_string(), Some("7".to_string())),
                            ("day".to_string(), Some("7".to_string())),
                        ]),
                    ),
                    (
                        "day=8/part-001.parquet",
                        120,
                        3,
                        BTreeMap::from([
                            ("DAY".to_string(), Some("8".to_string())),
                            ("day".to_string(), Some("8".to_string())),
                        ]),
                    ),
                ],
                BTreeMap::from([
                    ("DAY".to_string(), PartitionColumnType::Int64),
                    ("day".to_string(), PartitionColumnType::Int64),
                ]),
            ),
            ["day=7/part-000.parquet", "day=8/part-001.parquet"],
        ),
    ] {
        let planned = session
            .plan_query(
                &snapshot,
                &QueryRequest::new(
                    snapshot.table_uri(),
                    "SELECT id FROM axon_table WHERE day = 7 ORDER BY id",
                    ExecutionTarget::BrowserWasm,
                ),
            )
            .unwrap_or_else(|error| {
                panic!("{name}: typed partition predicate should plan: {error:?}")
            });

        assert_eq!(
            planned.candidate_paths,
            expected_paths.map(str::to_string).to_vec(),
            "{name}: unsafe typed partition values should fail open"
        );
        assert_eq!(planned.candidate_file_count, 2, "{name}");
        assert_eq!(planned.candidate_bytes, 220, "{name}");
        assert_eq!(planned.candidate_rows, 5, "{name}");
        assert!(
            !planned.pruning.used_partition_pruning,
            "{name}: unsafe typed partition values should not prune"
        );
        assert_eq!(planned.pruning.files_retained, 2, "{name}");
        assert_eq!(planned.pruning.files_pruned, 0, "{name}");
    }
}

#[test]
fn plan_query_keeps_type_incompatible_partition_literals_conservative() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let snapshot = typed_partition_bootstrapped_snapshot();

    let planned = session
        .plan_query(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id FROM axon_table WHERE day = '7' ORDER BY id",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect("type-incompatible partition literals should still plan");

    assert_eq!(
        planned.candidate_paths,
        vec![
            "day=7/enabled=true/region=US".to_string(),
            "day=8/enabled=false/region=us".to_string(),
            "day=-3/enabled=true/region=007".to_string(),
        ]
    );
    assert_eq!(planned.candidate_file_count, 3);
    assert_eq!(planned.candidate_bytes, 360);
    assert_eq!(planned.candidate_rows, 9);
    assert!(!planned.pruning.used_partition_pruning);
    assert_eq!(planned.pruning.files_retained, 3);
    assert_eq!(planned.pruning.files_pruned, 0);
}

#[test]
fn plan_query_fails_open_for_unsupported_null_partition_columns() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let snapshot = unsafe_day_partition_snapshot(
        "gs://axon-fixtures/unsupported_null_partition",
        [
            (
                "mystery=NULL/part-000.parquet",
                100,
                2,
                BTreeMap::from([("mystery".to_string(), None)]),
            ),
            (
                "mystery=NULL/part-001.parquet",
                120,
                3,
                BTreeMap::from([("mystery".to_string(), None)]),
            ),
        ],
        BTreeMap::from([("mystery".to_string(), PartitionColumnType::Unsupported)]),
    );

    let planned = session
        .plan_query(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id FROM axon_table WHERE mystery IS NULL ORDER BY id",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect("unsupported null partition predicates should still plan");

    assert_eq!(
        planned.candidate_paths,
        vec![
            "mystery=NULL/part-000.parquet".to_string(),
            "mystery=NULL/part-001.parquet".to_string(),
        ]
    );
    assert_eq!(planned.candidate_file_count, 2);
    assert_eq!(planned.candidate_bytes, 220);
    assert_eq!(planned.candidate_rows, 5);
    assert!(!planned.pruning.used_partition_pruning);
    assert_eq!(planned.pruning.files_retained, 2);
    assert_eq!(planned.pruning.files_pruned, 0);
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
        browser_compatibility: CapabilityReport::default(),
        required_capabilities: CapabilityReport::default(),
        active_files: vec![
            BrowserHttpFileDescriptor {
                path: "z-last.parquet".to_string(),
                url: "https://example.com/z-last.parquet".to_string(),
                size_bytes: 333,
                partition_values: BTreeMap::from([("category".to_string(), Some("z".to_string()))]),
                stats: None,
                object_etag: None,
            },
            BrowserHttpFileDescriptor {
                path: "a-first.parquet".to_string(),
                url: "https://example.com/a-first.parquet".to_string(),
                size_bytes: 111,
                partition_values: BTreeMap::from([
                    ("category".to_string(), Some("a".to_string())),
                    ("region".to_string(), None),
                ]),
                stats: None,
                object_etag: None,
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
fn materialize_snapshot_allows_browser_local_blob_descriptors() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let descriptor = BrowserHttpSnapshotDescriptor {
        table_uri: "browser-local://delta-table/events".to_string(),
        snapshot_version: 3,
        partition_column_types: BTreeMap::new(),
        browser_compatibility: CapabilityReport::default(),
        required_capabilities: CapabilityReport::default(),
        active_files: vec![BrowserHttpFileDescriptor {
            path: "part-000.parquet".to_string(),
            url: "blob:https://127.0.0.1:5173/2cf91b7c-8f64-4d42-b85d-4f517db2ef21".to_string(),
            size_bytes: 128,
            partition_values: BTreeMap::new(),
            stats: None,
            object_etag: None,
        }],
    };

    let materialized = session
        .materialize_snapshot(&descriptor)
        .expect("browser-local blob descriptors should materialize");

    assert_eq!(
        materialized.active_files()[0].object_source().url(),
        descriptor.active_files[0].url
    );
}

#[test]
fn materialize_snapshot_allows_empty_active_file_lists() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let descriptor = BrowserHttpSnapshotDescriptor {
        table_uri: "gs://axon-fixtures/empty_table".to_string(),
        snapshot_version: 3,
        partition_column_types: BTreeMap::new(),
        browser_compatibility: CapabilityReport::default(),
        required_capabilities: CapabilityReport::default(),
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
fn runtime_prunes_files_from_delta_snapshot_before_opening_parquet() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let fixture = TestTableFixture::create_partitioned();
    let resolver = SnapshotResolver::new(
        LocalFileStorageHandler::default(),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );
    let resolved = runtime()
        .block_on(session.resolve_delta_snapshot(
            &resolver,
            SnapshotResolutionRequest {
                table_uri: fixture.raw_table_path(),
                snapshot_version: None,
            },
        ))
        .expect("delta snapshot should resolve");
    let server = RequestCapturingObjectServer::from_fixture_paths(
        &fixture,
        resolved.active_files.iter().map(|file| file.path.clone()),
    );
    let materialized = session
        .materialize_resolved_snapshot(&resolved, |file| {
            BrowserObjectSource::from_url(server.url_for_path(&file.path))
        })
        .expect("resolved snapshot should materialize into browser object sources");
    assert_eq!(
        materialized.partition_column_types(),
        &BTreeMap::from([("category".to_string(), PartitionColumnType::String)])
    );
    let request = QueryRequest::new(
        materialized.table_uri(),
        "SELECT id FROM axon_table WHERE category = 'A' ORDER BY id",
        ExecutionTarget::BrowserWasm,
    );
    let prepared = runtime()
        .block_on(session.prepare_execution(&materialized, &request))
        .expect("runtime should prune from Delta metadata before bootstrap");
    let bootstrapped_paths = server.recorded_paths();
    let result = runtime()
        .block_on(session.execute_plan(&materialized, prepared.execution_plan()))
        .expect("pruned browser execution should succeed");

    assert_eq!(prepared.planned_query().candidate_file_count, 1);
    assert_eq!(prepared.planned_query().pruning.files_pruned, 2);
    assert_eq!(prepared.bootstrapped_snapshot().active_files().len(), 1);
    assert!(!bootstrapped_paths.is_empty());
    assert!(bootstrapped_paths.iter().all(|path| {
        path == &format!(
            "/{}",
            prepared.bootstrapped_snapshot().active_files()[0].path()
        )
    }));
    assert_eq!(result.metrics().files_touched, 1);
    assert_eq!(result.metrics().files_skipped, 2);
    assert_eq!(result.metrics().footer_reads, Some(1));
    assert_eq!(
        result.metrics().scan_footer_range_reads,
        Some(0),
        "legacy runtime scan should reuse bootstrapped footer metadata"
    );
    assert_eq!(result.metrics().duplicate_range_reads, Some(0));
    assert_eq!(result.metrics().footer_cache_hits, Some(1));
    assert_eq!(result.metrics().footer_range_reads_avoided, Some(2));
    assert_eq!(
        result.metrics().access_mode,
        Some(BrowserAccessMode::BrowserSafeHttp)
    );
    assert!(
        result
            .metrics()
            .snapshot_bootstrap_duration_ms
            .is_some_and(|duration| duration > 0),
        "bootstrap duration should be populated for prepared browser execution"
    );
    assert_eq!(result.rows().len(), 2);
}

#[test]
fn runtime_prunes_files_from_delta_stats_before_opening_parquet() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let fixture = TestTableFixture::create_partitioned();
    let resolver = SnapshotResolver::new(
        LocalFileStorageHandler::default(),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );
    let mut resolved = runtime()
        .block_on(session.resolve_delta_snapshot(
            &resolver,
            SnapshotResolutionRequest {
                table_uri: fixture.raw_table_path(),
                snapshot_version: None,
            },
        ))
        .expect("delta snapshot should resolve");
    for file in &mut resolved.active_files {
        let category = file
            .partition_values
            .get("category")
            .and_then(Option::as_deref)
            .expect("partitioned fixture files should have category values");
        file.stats = Some(match category {
            "A" => delta_i64_stats(2, "value", 10, 30),
            "B" => delta_i64_stats(3, "value", 20, 50),
            "C" => delta_i64_stats(4, "value", 60, 60),
            other => panic!("unexpected fixture category {other}"),
        });
    }
    let server = RequestCapturingObjectServer::from_fixture_paths(
        &fixture,
        resolved.active_files.iter().map(|file| file.path.clone()),
    );
    let materialized = session
        .materialize_resolved_snapshot(&resolved, |file| {
            BrowserObjectSource::from_url(server.url_for_path(&file.path))
        })
        .expect("resolved snapshot should materialize into browser object sources");
    let request = QueryRequest::new(
        materialized.table_uri(),
        "SELECT id FROM axon_table WHERE value >= 40 ORDER BY id",
        ExecutionTarget::BrowserWasm,
    );

    let prepared = runtime()
        .block_on(session.prepare_execution(&materialized, &request))
        .expect("runtime should prune from Delta stats before parquet bootstrap");
    let result = runtime()
        .block_on(session.execute_plan(&materialized, prepared.execution_plan()))
        .expect("delta-stats-pruned browser execution should succeed");
    let fetched_paths = server.recorded_paths();

    assert_eq!(prepared.planned_query().candidate_file_count, 2);
    assert!(prepared.planned_query().pruning.used_file_stats_pruning);
    assert_eq!(prepared.planned_query().pruning.files_pruned, 1);
    assert_eq!(result.metrics().files_touched, 2);
    assert_eq!(result.metrics().files_skipped, 1);
    assert_eq!(result.metrics().footer_reads, Some(2));
    assert_eq!(result.metrics().row_groups_touched, 2);
    assert_eq!(result.metrics().row_groups_skipped, 0);
    assert!(
        fetched_paths
            .iter()
            .all(|path| !path.contains("category=A/")),
        "Delta stats should skip the A file before any parquet fetch"
    );
}

#[test]
fn runtime_fails_open_when_prebootstrap_partition_pruning_finds_zero_candidates() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let fixture = TestTableFixture::create_partitioned();
    let resolver = SnapshotResolver::new(
        LocalFileStorageHandler::default(),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );
    let resolved = runtime()
        .block_on(session.resolve_delta_snapshot(
            &resolver,
            SnapshotResolutionRequest {
                table_uri: fixture.raw_table_path(),
                snapshot_version: None,
            },
        ))
        .expect("delta snapshot should resolve");
    let server = RequestCapturingObjectServer::from_fixture_paths(
        &fixture,
        resolved.active_files.iter().map(|file| file.path.clone()),
    );
    let materialized = session
        .materialize_resolved_snapshot(&resolved, |file| {
            BrowserObjectSource::from_url(server.url_for_path(&file.path))
        })
        .expect("resolved snapshot should materialize into browser object sources");
    let request = QueryRequest::new(
        materialized.table_uri(),
        "SELECT id FROM axon_table WHERE category = 'Z' ORDER BY id",
        ExecutionTarget::BrowserWasm,
    );

    let prepared = runtime()
        .block_on(session.prepare_execution(&materialized, &request))
        .expect("runtime should fail open for zero-candidate partition-pruned execution");
    let result = runtime()
        .block_on(session.execute_plan(&materialized, prepared.execution_plan()))
        .expect("fail-open zero-candidate execution should succeed");
    let fetched_paths = server.recorded_paths().into_iter().collect::<BTreeSet<_>>();
    let active_paths = resolved
        .active_files
        .iter()
        .map(|file| format!("/{}", file.path))
        .collect::<BTreeSet<_>>();

    assert_eq!(prepared.bootstrapped_snapshot().active_files().len(), 3);
    assert_eq!(prepared.planned_query().candidate_file_count, 3);
    assert_eq!(prepared.planned_query().pruning.files_pruned, 0);
    assert_eq!(result.metrics().files_touched, 3);
    assert_eq!(result.metrics().files_skipped, 0);
    assert_eq!(result.metrics().footer_reads, Some(3));
    assert_eq!(result.metrics().prebootstrap_fail_open_count, Some(1));
    assert_eq!(result.metrics().prebootstrap_files_pruned, Some(0));
    assert_eq!(result.metrics().footer_reads_avoided, Some(0));
    assert_eq!(result.metrics().prebootstrap_candidate_files, Some(3));
    for active_path in active_paths {
        assert!(
            fetched_paths.contains(&active_path),
            "fail-open bootstrap should fetch footer metadata for {active_path}"
        );
    }
    assert!(result.rows().is_empty());
}

#[test]
fn runtime_fails_open_when_prebootstrap_delta_stats_pruning_finds_zero_candidates() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let fixture = TestTableFixture::create_partitioned();
    let resolver = SnapshotResolver::new(
        LocalFileStorageHandler::default(),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );
    let mut resolved = runtime()
        .block_on(session.resolve_delta_snapshot(
            &resolver,
            SnapshotResolutionRequest {
                table_uri: fixture.raw_table_path(),
                snapshot_version: None,
            },
        ))
        .expect("delta snapshot should resolve");
    for file in &mut resolved.active_files {
        let category = file
            .partition_values
            .get("category")
            .and_then(Option::as_deref)
            .expect("partitioned fixture files should have category values");
        file.stats = Some(match category {
            "A" => delta_i64_stats(2, "value", 10, 30),
            "B" => delta_i64_stats(3, "value", 20, 50),
            "C" => delta_i64_stats(4, "value", 60, 60),
            other => panic!("unexpected fixture category {other}"),
        });
    }
    let server = RequestCapturingObjectServer::from_fixture_paths(
        &fixture,
        resolved.active_files.iter().map(|file| file.path.clone()),
    );
    let materialized = session
        .materialize_resolved_snapshot(&resolved, |file| {
            BrowserObjectSource::from_url(server.url_for_path(&file.path))
        })
        .expect("resolved snapshot should materialize into browser object sources");
    let request = QueryRequest::new(
        materialized.table_uri(),
        "SELECT id FROM axon_table WHERE value > 100 ORDER BY id",
        ExecutionTarget::BrowserWasm,
    );

    let prepared = runtime()
        .block_on(session.prepare_execution(&materialized, &request))
        .expect("runtime should fail open for zero-candidate Delta-stats-pruned execution");
    let result = runtime()
        .block_on(session.execute_plan(&materialized, prepared.execution_plan()))
        .expect("fail-open zero-candidate Delta stats execution should succeed");
    let fetched_paths = server.recorded_paths().into_iter().collect::<BTreeSet<_>>();
    let active_paths = resolved
        .active_files
        .iter()
        .map(|file| format!("/{}", file.path))
        .collect::<BTreeSet<_>>();

    assert_eq!(prepared.bootstrapped_snapshot().active_files().len(), 3);
    assert_eq!(prepared.planned_query().candidate_file_count, 3);
    assert_eq!(prepared.planned_query().pruning.files_pruned, 0);
    assert_eq!(result.metrics().files_touched, 3);
    assert_eq!(result.metrics().files_skipped, 0);
    assert_eq!(result.metrics().footer_reads, Some(3));
    assert_eq!(result.metrics().prebootstrap_fail_open_count, Some(1));
    assert_eq!(result.metrics().prebootstrap_files_pruned, Some(0));
    assert_eq!(result.metrics().footer_reads_avoided, Some(0));
    assert_eq!(result.metrics().prebootstrap_candidate_files, Some(3));
    for active_path in active_paths {
        assert!(
            fetched_paths.contains(&active_path),
            "fail-open bootstrap should fetch footer metadata for {active_path}"
        );
    }
    assert!(result.rows().is_empty());
}

#[test]
fn runtime_pushes_integer_filters_into_parquet_row_group_pruning() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let object = parquet_bytes_with_i64_row_groups(&[&[1_i64, 2, 3], &[10_i64, 11, 12]]);
    let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
    let server = RequestCapturingObjectServer::from_objects(BTreeMap::from([(
        "/part-000.parquet".to_string(),
        object,
    )]));
    let resolved = ResolvedSnapshotDescriptor {
        table_uri: "gs://axon-fixtures/row-group-pruning".to_string(),
        snapshot_version: 0,
        partition_column_types: BTreeMap::new(),
        browser_compatibility: CapabilityReport::default(),
        required_capabilities: CapabilityReport::default(),
        active_files: vec![ResolvedFileDescriptor {
            path: "part-000.parquet".to_string(),
            size_bytes: object_size,
            partition_values: BTreeMap::new(),
            stats: None,
        }],
    };
    let materialized = session
        .materialize_resolved_snapshot(&resolved, |file| {
            BrowserObjectSource::from_url(server.url_for_path(&file.path))
        })
        .expect("resolved snapshot should materialize into browser object sources");
    let request = QueryRequest::new(
        materialized.table_uri(),
        "SELECT id FROM axon_table WHERE id >= 10 ORDER BY id",
        ExecutionTarget::BrowserWasm,
    );

    let prepared = runtime()
        .block_on(session.prepare_execution(&materialized, &request))
        .expect("single parquet file should prepare");
    let result = runtime()
        .block_on(session.execute_plan(&materialized, prepared.execution_plan()))
        .expect("row-group-pruned browser execution should succeed");

    assert_eq!(
        result
            .rows()
            .iter()
            .map(|row| row.values()[0].clone())
            .collect::<Vec<_>>(),
        vec![
            BrowserScalarValue::Int64(10),
            BrowserScalarValue::Int64(11),
            BrowserScalarValue::Int64(12),
        ]
    );
    assert_eq!(result.metrics().files_touched, 1);
    assert_eq!(result.metrics().files_skipped, 0);
    assert_eq!(result.metrics().row_groups_touched, 1);
    assert_eq!(result.metrics().row_groups_skipped, 1);
    assert_eq!(result.metrics().rows_emitted, 3);
}

#[test]
fn runtime_reuses_shared_parquet_range_cache_across_repeated_scans() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let object = parquet_bytes_with_i64_row_groups(&[&[1_i64, 2, 3], &[10_i64, 11, 12]]);
    let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
    let server = RequestCapturingObjectServer::from_objects(BTreeMap::from([(
        "/part-000.parquet".to_string(),
        object,
    )]));
    let resolved = ResolvedSnapshotDescriptor {
        table_uri: "gs://axon-fixtures/range-cache-reuse".to_string(),
        snapshot_version: 0,
        partition_column_types: BTreeMap::new(),
        browser_compatibility: CapabilityReport::default(),
        required_capabilities: CapabilityReport::default(),
        active_files: vec![ResolvedFileDescriptor {
            path: "part-000.parquet".to_string(),
            size_bytes: object_size,
            partition_values: BTreeMap::new(),
            stats: None,
        }],
    };
    let materialized = session
        .materialize_resolved_snapshot(&resolved, |file| {
            BrowserObjectSource::from_url(server.url_for_path(&file.path))
        })
        .expect("resolved snapshot should materialize into browser object sources");
    let request = QueryRequest::new(
        materialized.table_uri(),
        "SELECT id FROM axon_table WHERE id >= 10 ORDER BY id",
        ExecutionTarget::BrowserWasm,
    );
    let prepared = runtime()
        .block_on(session.prepare_execution(&materialized, &request))
        .expect("single parquet file should prepare");

    let first = runtime()
        .block_on(session.execute_plan(&materialized, prepared.execution_plan()))
        .expect("first browser execution should succeed");
    let requests_after_first = server.recorded_paths().len();
    let cache_after_first = session.range_cache().snapshot();
    assert!(
        cache_after_first.range_cache_stores > 0,
        "first scan should store fetched data ranges in the session range cache"
    );

    let second = runtime()
        .block_on(session.execute_plan(&materialized, prepared.execution_plan()))
        .expect("second browser execution should succeed");
    let cache_after_second = session.range_cache().snapshot();

    assert_eq!(first.output_names(), second.output_names());
    assert_eq!(first.rows(), second.rows());
    assert_eq!(
        server.recorded_paths().len(),
        requests_after_first + 1,
        "second scan should only perform the existing metadata-cache identity validation probe"
    );
    assert_eq!(second.metrics().bytes_fetched, 0);
    assert!(
        cache_after_second.range_cache_hits > cache_after_first.range_cache_hits,
        "second scan should be served by the session-owned range cache"
    );
}

#[test]
fn runtime_executes_partition_group_by_from_local_delta_snapshot() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let fixture = TestTableFixture::create_partitioned();
    let resolver = SnapshotResolver::new(
        LocalFileStorageHandler::default(),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );
    let resolved = runtime()
        .block_on(session.resolve_delta_snapshot(
            &resolver,
            SnapshotResolutionRequest {
                table_uri: fixture.raw_table_path(),
                snapshot_version: None,
            },
        ))
        .expect("delta snapshot should resolve");
    let server = RequestCapturingObjectServer::from_fixture_paths(
        &fixture,
        resolved.active_files.iter().map(|file| file.path.clone()),
    );
    let materialized = session
        .materialize_resolved_snapshot(&resolved, |file| {
            BrowserObjectSource::from_url(server.url_for_path(&file.path))
        })
        .expect("resolved snapshot should materialize into browser object sources");
    let request = QueryRequest::new(
        materialized.table_uri(),
        "SELECT category, SUM(value) AS total_value FROM axon_table GROUP BY category ORDER BY category",
        ExecutionTarget::BrowserWasm,
    );

    let prepared = runtime()
        .block_on(session.prepare_execution(&materialized, &request))
        .expect("partition-typed local snapshots should prepare grouped execution");
    let result = runtime()
        .block_on(session.execute_plan(&materialized, prepared.execution_plan()))
        .expect("partition-typed local snapshots should execute grouped queries");

    assert_eq!(
        materialized.partition_column_types()["category"],
        PartitionColumnType::String
    );
    assert_eq!(
        result.output_names(),
        &["category".to_string(), "total_value".to_string()]
    );
    assert_eq!(
        result
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![
                BrowserScalarValue::String("A".to_string()),
                BrowserScalarValue::Int64(40),
            ],
            vec![
                BrowserScalarValue::String("B".to_string()),
                BrowserScalarValue::Int64(110),
            ],
            vec![
                BrowserScalarValue::String("C".to_string()),
                BrowserScalarValue::Int64(60),
            ],
        ]
    );
}

#[test]
fn execute_plan_to_arrow_ipc_streams_supported_query_results() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let fixture = TestTableFixture::create_partitioned();
    let resolver = SnapshotResolver::new(
        LocalFileStorageHandler::default(),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );
    let resolved = runtime()
        .block_on(session.resolve_delta_snapshot(
            &resolver,
            SnapshotResolutionRequest {
                table_uri: fixture.raw_table_path(),
                snapshot_version: None,
            },
        ))
        .expect("delta snapshot should resolve");
    let server = RequestCapturingObjectServer::from_fixture_paths(
        &fixture,
        resolved.active_files.iter().map(|file| file.path.clone()),
    );
    let materialized = session
        .materialize_resolved_snapshot(&resolved, |file| {
            BrowserObjectSource::from_url(server.url_for_path(&file.path))
        })
        .expect("resolved snapshot should materialize into browser object sources");
    let request = QueryRequest::new(
        materialized.table_uri(),
        "SELECT category, SUM(value) AS total_value FROM axon_table GROUP BY category ORDER BY category",
        ExecutionTarget::BrowserWasm,
    );

    let prepared = runtime()
        .block_on(session.prepare_execution(&materialized, &request))
        .expect("partition-typed local snapshots should prepare grouped execution");
    let execution = runtime()
        .block_on(session.execute_plan_to_arrow_ipc(&materialized, prepared.execution_plan()))
        .expect("supported browser execution should encode Arrow IPC");
    let (output_names, rows) = decode_arrow_ipc_rows(&execution.ipc_bytes);

    assert_eq!(
        output_names,
        ["category".to_string(), "total_value".to_string()]
    );
    assert_eq!(
        rows,
        vec![
            vec![
                BrowserScalarValue::String("A".to_string()),
                BrowserScalarValue::Int64(40),
            ],
            vec![
                BrowserScalarValue::String("B".to_string()),
                BrowserScalarValue::Int64(110),
            ],
            vec![
                BrowserScalarValue::String("C".to_string()),
                BrowserScalarValue::Int64(60),
            ],
        ]
    );
    assert_eq!(execution.row_count, 3);
    assert_eq!(execution.encoded_bytes, execution.ipc_bytes.len() as u64);
    assert_eq!(
        execution
            .scan_metrics
            .iter()
            .map(|metrics| metrics.files_touched)
            .sum::<u64>(),
        prepared.planned_query().candidate_file_count
    );
    assert_eq!(
        execution
            .scan_metrics
            .iter()
            .map(|metrics| metrics.rows_emitted)
            .sum::<u64>(),
        prepared.planned_query().candidate_rows
    );
}

#[test]
fn runtime_stops_scanning_when_execution_future_is_cancelled() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let fixture = TestTableFixture::create_partitioned();
    let resolver = SnapshotResolver::new(
        LocalFileStorageHandler::default(),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );
    let resolved = runtime()
        .block_on(session.resolve_delta_snapshot(
            &resolver,
            SnapshotResolutionRequest {
                table_uri: fixture.raw_table_path(),
                snapshot_version: None,
            },
        ))
        .expect("delta snapshot should resolve");
    let planning_server = RequestCapturingObjectServer::from_fixture_paths(
        &fixture,
        resolved.active_files.iter().map(|file| file.path.clone()),
    );
    let materialized_for_prepare = session
        .materialize_resolved_snapshot(&resolved, |file| {
            BrowserObjectSource::from_url(planning_server.url_for_path(&file.path))
        })
        .expect("resolved snapshot should materialize into browser object sources");
    let request = QueryRequest::new(
        materialized_for_prepare.table_uri(),
        "SELECT id FROM axon_table ORDER BY id",
        ExecutionTarget::BrowserWasm,
    );
    let prepared = runtime()
        .block_on(session.prepare_execution(&materialized_for_prepare, &request))
        .expect("browser execution should prepare");
    drop(planning_server);

    let execution_server = StallingRequestCapturingObjectServer::from_fixture_paths(
        &fixture,
        resolved.active_files.iter().map(|file| file.path.clone()),
        Duration::from_millis(200),
    );
    let materialized_for_execute = session
        .materialize_resolved_snapshot(&resolved, |file| {
            BrowserObjectSource::from_url(execution_server.url_for_path(&file.path))
        })
        .expect("resolved snapshot should materialize into browser object sources");

    let runtime = runtime();
    let handle = runtime.spawn({
        let session = session.clone();
        let materialized_for_execute = materialized_for_execute.clone();
        let execution_plan = prepared.execution_plan().clone();
        async move {
            session
                .execute_plan_to_arrow_ipc(&materialized_for_execute, &execution_plan)
                .await
        }
    });

    thread::sleep(Duration::from_millis(40));
    handle.abort();
    let join_error = runtime
        .block_on(handle)
        .expect_err("aborted browser execution should report cancellation");

    assert!(join_error.is_cancelled());
    thread::sleep(Duration::from_millis(260));
    assert_eq!(
        execution_server.recorded_paths().len() <= 1,
        true,
        "cancelling execution should prevent later scan targets from opening"
    );
}

#[test]
fn runtime_rejects_over_budget_queries_with_structured_fallback_before_scanning() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig {
        execution_budget: Some(BrowserExecutionBudget {
            max_rows: 1,
            max_bytes: 1,
        }),
        ..BrowserRuntimeConfig::default()
    })
    .expect("budgeted config should be supported");
    let fixture = TestTableFixture::create_partitioned();
    let resolver = SnapshotResolver::new(
        LocalFileStorageHandler::default(),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );
    let resolved = runtime()
        .block_on(session.resolve_delta_snapshot(
            &resolver,
            SnapshotResolutionRequest {
                table_uri: fixture.raw_table_path(),
                snapshot_version: None,
            },
        ))
        .expect("delta snapshot should resolve");
    let server = RequestCapturingObjectServer::from_fixture_paths(
        &fixture,
        resolved.active_files.iter().map(|file| file.path.clone()),
    );
    let materialized = session
        .materialize_resolved_snapshot(&resolved, |file| {
            BrowserObjectSource::from_url(server.url_for_path(&file.path))
        })
        .expect("resolved snapshot should materialize into browser object sources");
    let request = QueryRequest::new(
        materialized.table_uri(),
        "SELECT id FROM axon_table ORDER BY id",
        ExecutionTarget::BrowserWasm,
    );
    let prepared = runtime()
        .block_on(session.prepare_execution(&materialized, &request))
        .expect("browser execution should prepare");
    let bootstrapped_paths = server.recorded_paths();

    let error = runtime()
        .block_on(session.execute_plan_to_arrow_ipc(&materialized, prepared.execution_plan()))
        .expect_err("over-budget browser execution should fail closed before scan collection");

    assert_eq!(error.code, QueryErrorCode::FallbackRequired);
    assert_eq!(
        error.fallback_reason,
        Some(FallbackReason::BrowserRuntimeConstraint)
    );
    assert_eq!(
        server.recorded_paths(),
        bootstrapped_paths,
        "budget fallback should not trigger additional scan reads after bootstrap"
    );
}

#[test]
fn runtime_rejects_local_delta_snapshots_requiring_native_only_capabilities_before_bootstrap() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let server = RequestCapturingObjectServer::from_objects(BTreeMap::from([(
        "/part-000.parquet".to_string(),
        parquet_like_object(b"rows", b"footer"),
    )]));
    let materialized = session
        .materialize_resolved_snapshot(
            &ResolvedSnapshotDescriptor {
                table_uri: "gs://axon-fixtures/deletion-vectors".to_string(),
                snapshot_version: 3,
                partition_column_types: BTreeMap::new(),
                browser_compatibility: CapabilityReport::from_pairs([(
                    CapabilityKey::DeletionVectors,
                    CapabilityState::NativeOnly,
                )]),
                required_capabilities: CapabilityReport::from_pairs([(
                    CapabilityKey::DeletionVectors,
                    CapabilityState::NativeOnly,
                )]),
                active_files: vec![ResolvedFileDescriptor {
                    path: "part-000.parquet".to_string(),
                    size_bytes: 128,
                    partition_values: BTreeMap::new(),
                    stats: None,
                }],
            },
            |file| BrowserObjectSource::from_url(server.url_for_path(&file.path)),
        )
        .expect("resolved snapshot should materialize");
    let request = QueryRequest::new(
        materialized.table_uri(),
        "SELECT id FROM axon_table",
        ExecutionTarget::BrowserWasm,
    );

    let error = runtime()
        .block_on(session.prepare_execution(&materialized, &request))
        .expect_err("native-only capabilities should stop before browser bootstrap");

    assert_eq!(error.code, QueryErrorCode::FallbackRequired);
    assert_eq!(
        error.fallback_reason,
        Some(FallbackReason::CapabilityGate {
            capability: CapabilityKey::DeletionVectors,
            required_state: CapabilityState::NativeOnly,
        })
    );
    assert!(server.recorded_paths().is_empty());
}

#[test]
fn bootstrap_snapshot_metadata_rejects_native_only_capabilities_before_network_io() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let object = parquet_like_object(b"rows", b"footer");
    let server = RequestCapturingObjectServer::from_objects(BTreeMap::from([(
        "/part-000.parquet".to_string(),
        object.clone(),
    )]));
    let materialized = session
        .materialize_resolved_snapshot(
            &ResolvedSnapshotDescriptor {
                table_uri: "gs://axon-fixtures/deletion-vectors".to_string(),
                snapshot_version: 3,
                partition_column_types: BTreeMap::new(),
                browser_compatibility: CapabilityReport::from_pairs([(
                    CapabilityKey::DeletionVectors,
                    CapabilityState::NativeOnly,
                )]),
                required_capabilities: CapabilityReport::from_pairs([(
                    CapabilityKey::DeletionVectors,
                    CapabilityState::NativeOnly,
                )]),
                active_files: vec![ResolvedFileDescriptor {
                    path: "part-000.parquet".to_string(),
                    size_bytes: object.len() as u64,
                    partition_values: BTreeMap::new(),
                    stats: None,
                }],
            },
            |file| BrowserObjectSource::from_url(server.url_for_path(&file.path)),
        )
        .expect("resolved snapshot should materialize");

    let error = runtime()
        .block_on(session.bootstrap_snapshot_metadata(&materialized))
        .expect_err("native-only capabilities should stop before snapshot bootstrap");

    assert_eq!(error.code, QueryErrorCode::FallbackRequired);
    assert_eq!(
        error.fallback_reason,
        Some(FallbackReason::CapabilityGate {
            capability: CapabilityKey::DeletionVectors,
            required_state: CapabilityState::NativeOnly,
        })
    );
    assert!(server.recorded_paths().is_empty());
}

#[test]
fn bootstrap_snapshot_metadata_hard_fails_unsupported_capabilities_before_network_io() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let object = parquet_like_object(b"rows", b"footer");
    let server = RequestCapturingObjectServer::from_objects(BTreeMap::from([(
        "/part-000.parquet".to_string(),
        object.clone(),
    )]));
    let materialized = session
        .materialize_resolved_snapshot(
            &ResolvedSnapshotDescriptor {
                table_uri: "gs://axon-fixtures/unknown-protocol-feature".to_string(),
                snapshot_version: 3,
                partition_column_types: BTreeMap::new(),
                browser_compatibility: CapabilityReport::from_pairs([(
                    CapabilityKey::UnknownProtocolFeatures,
                    CapabilityState::Unsupported,
                )]),
                required_capabilities: CapabilityReport::from_pairs([(
                    CapabilityKey::UnknownProtocolFeatures,
                    CapabilityState::Unsupported,
                )]),
                active_files: vec![ResolvedFileDescriptor {
                    path: "part-000.parquet".to_string(),
                    size_bytes: object.len() as u64,
                    partition_values: BTreeMap::new(),
                    stats: None,
                }],
            },
            |file| BrowserObjectSource::from_url(server.url_for_path(&file.path)),
        )
        .expect("resolved snapshot should materialize");

    let error = runtime()
        .block_on(session.bootstrap_snapshot_metadata(&materialized))
        .expect_err("unsupported capabilities should hard fail before snapshot bootstrap");

    assert_eq!(error.code, QueryErrorCode::UnsupportedFeature);
    assert_eq!(error.fallback_reason, None);
    assert!(error.message.contains("UnknownProtocolFeatures"));
    assert!(server.recorded_paths().is_empty());
}

#[test]
fn runtime_falls_back_when_snapshot_features_exceed_browser_capability() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let snapshot = BootstrappedBrowserSnapshot::new_with_partition_column_types(
        "gs://axon-fixtures/unsupported-partition",
        11,
        vec![bootstrapped_file(
            "region=us-east-1/part-000.parquet",
            100,
            BTreeMap::from([("region".to_string(), Some("us-east-1".to_string()))]),
            BrowserParquetFileMetadata {
                object_size_bytes: 100,
                footer_length_bytes: 32,
                row_group_count: 1,
                row_count: 2,
                fields: vec![required_field("id", BrowserParquetPhysicalType::Int32)],
                field_stats: BTreeMap::new(),
            },
        )],
        BTreeMap::from([("region".to_string(), PartitionColumnType::Unsupported)]),
    )
    .expect("unsupported partition snapshots should still construct");

    let error = session
        .build_execution_plan(
            &snapshot,
            &QueryRequest::new(
                snapshot.table_uri(),
                "SELECT id FROM axon_table WHERE region = 'us-east-1'",
                ExecutionTarget::BrowserWasm,
            ),
        )
        .expect_err("unsupported snapshot features should require browser fallback");

    assert_eq!(error.code, QueryErrorCode::FallbackRequired);
    assert_eq!(error.fallback_reason, Some(FallbackReason::NativeRequired));
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
        browser_compatibility: CapabilityReport::default(),
        required_capabilities: CapabilityReport::default(),
        active_files: vec![
            BrowserHttpFileDescriptor {
                path: duplicate_path.clone(),
                url: "https://example.com/one.parquet".to_string(),
                size_bytes: 128,
                partition_values: BTreeMap::new(),
                stats: None,
                object_etag: None,
            },
            BrowserHttpFileDescriptor {
                path: duplicate_path.clone(),
                url: "https://example.com/two.parquet".to_string(),
                size_bytes: 256,
                partition_values: BTreeMap::new(),
                stats: None,
                object_etag: None,
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
        browser_compatibility: CapabilityReport::default(),
        required_capabilities: CapabilityReport::default(),
        active_files: vec![BrowserHttpFileDescriptor {
            path: "part-000.parquet".to_string(),
            url: "https://signed example.test/object?sig=super-secret#fragment".to_string(),
            size_bytes: 128,
            partition_values: BTreeMap::new(),
            stats: None,
            object_etag: None,
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
        browser_compatibility: CapabilityReport::default(),
        required_capabilities: CapabilityReport::default(),
        active_files: vec![BrowserHttpFileDescriptor {
            path: "part-000.parquet".to_string(),
            url: "ftp://signed.example.test/object?sig=super-secret#fragment".to_string(),
            size_bytes: 128,
            partition_values: BTreeMap::new(),
            stats: None,
            object_etag: None,
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
        browser_compatibility: CapabilityReport::default(),
        required_capabilities: CapabilityReport::default(),
        active_files: vec![BrowserHttpFileDescriptor {
            path: "part-000.parquet".to_string(),
            url: "http://example.com/object?sig=super-secret#fragment".to_string(),
            size_bytes: 128,
            partition_values: BTreeMap::new(),
            stats: None,
            object_etag: None,
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
        browser_compatibility: CapabilityReport::default(),
        required_capabilities: CapabilityReport::default(),
        active_files: vec![BrowserHttpFileDescriptor {
            path: "part-000.parquet".to_string(),
            url: "http://127.0.0.1:8787/object?sig=super-secret#fragment".to_string(),
            size_bytes: 10,
            partition_values: BTreeMap::from([("category".to_string(), Some("A".to_string()))]),
            stats: None,
            object_etag: None,
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
    let (url, requests, server) = spawn_multi_request_server(1, move |request, _| {
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

    let requests = finish_requests(server, requests, 1);
    assert_eq!(requests.len(), 1);
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
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
    let (url, server) = spawn_stalling_server(Duration::from_millis(150));
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
            100,
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

fn decode_arrow_ipc_rows(ipc_bytes: &[u8]) -> (Vec<String>, Vec<Vec<BrowserScalarValue>>) {
    let mut reader = StreamReader::try_new(Cursor::new(ipc_bytes.to_vec()), None)
        .expect("runtime Arrow IPC output should decode");
    let output_names = reader
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect::<Vec<_>>();
    let mut rows = Vec::new();

    for batch in &mut reader {
        let batch = batch.expect("Arrow IPC batch should decode");
        for row_index in 0..batch.num_rows() {
            let row = batch
                .columns()
                .iter()
                .map(|column| arrow_value_to_browser_scalar(column.as_ref(), row_index))
                .collect::<Vec<_>>();
            rows.push(row);
        }
    }

    (output_names, rows)
}

fn arrow_value_to_browser_scalar(column: &dyn Array, row_index: usize) -> BrowserScalarValue {
    if column.is_null(row_index) {
        return BrowserScalarValue::Null;
    }

    if let Some(column) = column.as_any().downcast_ref::<Int64Array>() {
        return BrowserScalarValue::Int64(column.value(row_index));
    }
    if let Some(column) = column.as_any().downcast_ref::<StringArray>() {
        return BrowserScalarValue::String(column.value(row_index).to_string());
    }
    if let Some(column) = column.as_any().downcast_ref::<BooleanArray>() {
        return BrowserScalarValue::Boolean(column.value(row_index));
    }

    panic!(
        "unexpected Arrow IPC data type in runtime test: {:?}",
        column.data_type()
    );
}

#[derive(Debug)]
struct CapturedRequest {
    path: String,
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

        trailer_stream
            .set_nonblocking(true)
            .expect("accepted streams should allow bounded footer reads");
        listener
            .set_nonblocking(true)
            .expect("listener should allow bounded second accepts");

        let deadline = std::time::Instant::now() + delay + Duration::from_secs(1);
        loop {
            if let Some(footer_request) = try_read_request(&mut trailer_stream) {
                let _ = request_tx.send(footer_request);
                thread::sleep(delay);
                break;
            }

            match listener.accept() {
                Ok((mut footer_stream, _)) => {
                    footer_stream
                        .set_nonblocking(false)
                        .expect("accepted streams should allow blocking reads");
                    footer_stream
                        .set_read_timeout(Some(Duration::from_millis(50)))
                        .expect("accepted streams should allow bounded footer reads");
                    if let Some(footer_request) = try_read_request(&mut footer_stream) {
                        let _ = request_tx.send(footer_request);
                        thread::sleep(delay);
                    }
                    break;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("test client should connect for footer read: {error}"),
            }

            if std::time::Instant::now() >= deadline {
                break;
            }

            thread::sleep(Duration::from_millis(5));
        }
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
                .recv_timeout(Duration::from_millis(250))
                .expect("test should receive the captured request")
        })
        .collect()
}

fn read_request(stream: &mut std::net::TcpStream) -> CapturedRequest {
    try_read_request(stream).unwrap_or_else(|| CapturedRequest {
        path: String::new(),
        headers: BTreeMap::new(),
    })
}

fn try_read_request(stream: &mut std::net::TcpStream) -> Option<CapturedRequest> {
    let mut buffer = [0_u8; 4096];
    let mut request = Vec::new();

    loop {
        let bytes_read = match stream.read(&mut buffer) {
            Ok(bytes_read) => bytes_read,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                return None;
            }
            Err(error) if is_expected_client_disconnect(&error) => return None,
            Err(error) => panic!("request bytes should be readable: {error}"),
        };
        if bytes_read == 0 {
            break;
        }
        request.extend_from_slice(&buffer[..bytes_read]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }

    if request.is_empty() {
        return None;
    }

    let text = String::from_utf8(request).ok()?;
    let mut lines = text.split("\r\n");
    let request_line = lines.next()?;
    let path = request_line.split_whitespace().nth(1)?.to_string();
    let mut headers = BTreeMap::new();

    for line in lines.take_while(|line| !line.is_empty()) {
        let (name, value) = line.split_once(':')?;
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }

    Some(CapturedRequest { path, headers })
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

fn parquet_bytes_with_i64_row_groups(row_groups: &[&[i64]]) -> Vec<u8> {
    let schema = Arc::new(
        parse_message_type("message schema { REQUIRED INT64 id; }")
            .expect("parquet schema should parse"),
    );
    let mut bytes = Vec::new();
    let mut writer = SerializedFileWriter::new(
        &mut bytes,
        schema,
        Arc::new(WriterProperties::builder().build()),
    )
    .expect("parquet writer should construct");

    for values in row_groups {
        let mut row_group = writer
            .next_row_group()
            .expect("row-group writer should construct");
        if let Some(mut column) = row_group
            .next_column()
            .expect("column writer should be returned")
        {
            column
                .typed::<Int64Type>()
                .write_batch(values, None, None)
                .expect("test parquet rows should write");
            column.close().expect("column writer should close");
        }
        row_group.close().expect("row-group writer should close");
    }

    writer.close().expect("parquet writer should close");
    bytes
}

fn delta_i64_stats(num_records: u64, column: &str, min: i64, max: i64) -> String {
    format!(
        r#"{{"numRecords":{num_records},"minValues":{{"{column}":{min}}},"maxValues":{{"{column}":{max}}},"nullCount":{{"{column}":0}}}}"#
    )
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

fn typed_partition_bootstrapped_snapshot() -> BootstrappedBrowserSnapshot {
    BootstrappedBrowserSnapshot::new_with_partition_column_types(
        "gs://axon-fixtures/typed_partition_table",
        12,
        vec![
            bootstrapped_file(
                "day=7/enabled=true/region=US",
                100,
                BTreeMap::from([
                    ("day".to_string(), Some("7".to_string())),
                    ("enabled".to_string(), Some("true".to_string())),
                    ("region".to_string(), Some("US".to_string())),
                ]),
                simple_partition_metadata(100, 2),
            ),
            bootstrapped_file(
                "day=8/enabled=false/region=us",
                120,
                BTreeMap::from([
                    ("day".to_string(), Some("8".to_string())),
                    ("enabled".to_string(), Some("false".to_string())),
                    ("region".to_string(), Some("us".to_string())),
                ]),
                simple_partition_metadata(120, 3),
            ),
            bootstrapped_file(
                "day=-3/enabled=true/region=007",
                140,
                BTreeMap::from([
                    ("day".to_string(), Some("-3".to_string())),
                    ("enabled".to_string(), Some("true".to_string())),
                    ("region".to_string(), Some("007".to_string())),
                ]),
                simple_partition_metadata(140, 4),
            ),
        ],
        BTreeMap::from([
            ("day".to_string(), PartitionColumnType::Int64),
            ("enabled".to_string(), PartitionColumnType::Boolean),
            ("region".to_string(), PartitionColumnType::String),
        ]),
    )
    .expect("typed-partition bootstrapped snapshots should construct")
}

fn unsafe_day_partition_snapshot<const N: usize>(
    table_uri: &str,
    files: [(&str, u64, u64, BTreeMap<String, Option<String>>); N],
    partition_column_types: BTreeMap<String, PartitionColumnType>,
) -> BootstrappedBrowserSnapshot {
    BootstrappedBrowserSnapshot::new_with_partition_column_types(
        table_uri,
        13,
        files
            .into_iter()
            .map(|(path, size_bytes, row_count, partition_values)| {
                bootstrapped_file(
                    path,
                    size_bytes,
                    partition_values,
                    simple_partition_metadata(size_bytes, row_count),
                )
            })
            .collect(),
        partition_column_types,
    )
    .expect("unsafe partition snapshots should construct")
}

fn simple_partition_metadata(size_bytes: u64, row_count: u64) -> BrowserParquetFileMetadata {
    BrowserParquetFileMetadata {
        object_size_bytes: size_bytes,
        footer_length_bytes: 32,
        row_group_count: 1,
        row_count,
        fields: vec![required_field("id", BrowserParquetPhysicalType::Int32)],
        field_stats: BTreeMap::new(),
    }
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

struct RequestCapturingObjectServer {
    address: std::net::SocketAddr,
    stop: Arc<AtomicBool>,
    requests: Arc<Mutex<Vec<String>>>,
    thread: Option<JoinHandle<()>>,
}

struct StallingRequestCapturingObjectServer {
    address: std::net::SocketAddr,
    stop: Arc<AtomicBool>,
    requests: Arc<Mutex<Vec<String>>>,
    thread: Option<JoinHandle<()>>,
}

impl StallingRequestCapturingObjectServer {
    fn from_fixture_paths(
        fixture: &TestTableFixture,
        relative_paths: impl IntoIterator<Item = String>,
        delay: Duration,
    ) -> Self {
        let objects_by_path = relative_paths
            .into_iter()
            .map(|relative_path| {
                let absolute_path = fixture.table_root().join(&relative_path);
                let bytes = std::fs::read(&absolute_path).unwrap_or_else(|error| {
                    panic!(
                        "fixture object '{}' should be readable: {error}",
                        absolute_path.display()
                    )
                });
                (format!("/{}", relative_path), bytes)
            })
            .collect::<BTreeMap<_, _>>();

        Self::from_objects(objects_by_path, delay)
    }

    fn from_objects(objects_by_path: BTreeMap<String, Vec<u8>>, delay: Duration) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("loopback listener should bind");
        listener
            .set_nonblocking(true)
            .expect("listener should allow nonblocking accept");
        let address = listener
            .local_addr()
            .expect("listener should expose a local address");
        let stop = Arc::new(AtomicBool::new(false));
        let stop_for_thread = Arc::clone(&stop);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let requests_for_thread = Arc::clone(&requests);

        let thread = thread::spawn(move || {
            while !stop_for_thread.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        if stop_for_thread.load(Ordering::SeqCst) {
                            break;
                        }
                        stream
                            .set_nonblocking(false)
                            .expect("accepted streams should allow blocking reads");
                        handle_stalling_request_capturing_connection(
                            &mut stream,
                            &objects_by_path,
                            &requests_for_thread,
                            delay,
                        );
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => {
                        panic!("loopback object server accept should succeed: {error}")
                    }
                }
            }
        });

        Self {
            address,
            stop,
            requests,
            thread: Some(thread),
        }
    }

    fn url_for_path(&self, relative_path: &str) -> String {
        format!("http://{}/{}", self.address, relative_path)
    }

    fn recorded_paths(&self) -> Vec<String> {
        self.requests
            .lock()
            .expect("recorded requests should be readable")
            .clone()
    }
}

impl Drop for StallingRequestCapturingObjectServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.address);
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .expect("stalling request-capturing object server should shut down cleanly");
        }
    }
}

impl RequestCapturingObjectServer {
    fn from_fixture_paths(
        fixture: &TestTableFixture,
        relative_paths: impl IntoIterator<Item = String>,
    ) -> Self {
        let objects_by_path = relative_paths
            .into_iter()
            .map(|relative_path| {
                let absolute_path = fixture.table_root().join(&relative_path);
                let bytes = std::fs::read(&absolute_path).unwrap_or_else(|error| {
                    panic!(
                        "fixture object '{}' should be readable: {error}",
                        absolute_path.display()
                    )
                });
                (format!("/{}", relative_path), bytes)
            })
            .collect::<BTreeMap<_, _>>();

        Self::from_objects(objects_by_path)
    }

    fn from_objects(objects_by_path: BTreeMap<String, Vec<u8>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("loopback listener should bind");
        listener
            .set_nonblocking(true)
            .expect("listener should allow nonblocking accept");
        let address = listener
            .local_addr()
            .expect("listener should expose a local address");
        let stop = Arc::new(AtomicBool::new(false));
        let stop_for_thread = Arc::clone(&stop);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let requests_for_thread = Arc::clone(&requests);

        let thread = thread::spawn(move || {
            while !stop_for_thread.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        if stop_for_thread.load(Ordering::SeqCst) {
                            break;
                        }
                        stream
                            .set_nonblocking(false)
                            .expect("accepted streams should allow blocking reads");
                        handle_request_capturing_connection(
                            &mut stream,
                            &objects_by_path,
                            &requests_for_thread,
                        );
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => {
                        panic!("loopback object server accept should succeed: {error}")
                    }
                }
            }
        });

        Self {
            address,
            stop,
            requests,
            thread: Some(thread),
        }
    }

    fn url_for_path(&self, relative_path: &str) -> String {
        format!("http://{}/{}", self.address, relative_path)
    }

    fn recorded_paths(&self) -> Vec<String> {
        self.requests
            .lock()
            .expect("recorded requests should be readable")
            .clone()
    }
}

impl Drop for RequestCapturingObjectServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.address);
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .expect("request-capturing object server should shut down cleanly");
        }
    }
}

fn handle_request_capturing_connection(
    stream: &mut std::net::TcpStream,
    objects_by_path: &BTreeMap<String, Vec<u8>>,
    requests: &Arc<Mutex<Vec<String>>>,
) {
    let Some(request) = try_read_request(stream) else {
        return;
    };
    requests
        .lock()
        .expect("request log should be writable")
        .push(request.path.clone());
    let response = objects_by_path
        .get(&request.path)
        .map(|object| full_or_ranged_response_with_etag(&request, object))
        .unwrap_or_else(|| TestResponse {
            status_line: "404 Not Found",
            headers: vec![("Content-Length".to_string(), "0".to_string())],
            body: Vec::new(),
        });
    write_response(stream, response);
}

fn handle_stalling_request_capturing_connection(
    stream: &mut std::net::TcpStream,
    objects_by_path: &BTreeMap<String, Vec<u8>>,
    requests: &Arc<Mutex<Vec<String>>>,
    delay: Duration,
) {
    let request = read_request(stream);
    requests
        .lock()
        .expect("request log should be writable")
        .push(request.path.clone());
    thread::sleep(delay);
    let response = objects_by_path
        .get(&request.path)
        .map(|object| full_or_ranged_response_with_etag(&request, object))
        .unwrap_or_else(|| TestResponse {
            status_line: "404 Not Found",
            headers: vec![("Content-Length".to_string(), "0".to_string())],
            body: Vec::new(),
        });
    let _ = try_write_response(stream, response);
}

fn full_or_ranged_response_with_etag(request: &CapturedRequest, body: &[u8]) -> TestResponse {
    let mut response = full_or_ranged_response(request, body);
    response.headers.push((
        "ETag".to_string(),
        format!("\"fixture-{:016x}\"", body.len()),
    ));
    response
}

fn full_or_ranged_response_with_custom_etag(
    request: &CapturedRequest,
    body: &[u8],
    etag: &str,
) -> TestResponse {
    let mut response = full_or_ranged_response(request, body);
    response
        .headers
        .push(("ETag".to_string(), etag.to_string()));
    response
}
