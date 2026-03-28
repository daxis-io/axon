mod support;

use delta_control_plane::resolve_snapshot;
use deltalake::arrow::array::{Int64Array, UInt64Array};
use native_query_runtime::{execute_query, NativeQueryResult, DEFAULT_TABLE_NAME};
use query_contract::{ExecutionTarget, QueryErrorCode, QueryRequest, SnapshotResolutionRequest};
use support::{LoopbackObjectServer, TestTableFixture};
use wasm_query_runtime::{
    BootstrappedBrowserSnapshot, BrowserAggregateFunction, BrowserExecutionOutputKind,
    BrowserExecutionPlan, BrowserObjectSource, BrowserParquetConvertedType,
    BrowserParquetLogicalType, BrowserParquetPhysicalType, BrowserParquetRepetition,
    BrowserPlannedQuery, BrowserRuntimeConfig, BrowserRuntimeSession, MaterializedBrowserFile,
    MaterializedBrowserSnapshot,
};

struct SupportedBrowserSqlParityCase {
    name: &'static str,
    sql: String,
    assert_scan_metrics: bool,
    expected_outputs: Vec<ExpectedExecutionOutput>,
    expected_group_by_columns: Vec<&'static str>,
    expected_measures: Vec<ExpectedAggregateMeasure>,
    expected_order_by: Vec<ExpectedSortKey>,
    expected_limit: Option<u64>,
}

struct IntentionalBrowserNativeDivergenceCase {
    name: &'static str,
    sql: String,
}

#[derive(Clone, Copy)]
enum ExpectedOutputKind {
    Passthrough,
    Aggregate,
}

struct ExpectedExecutionOutput {
    output_name: &'static str,
    kind: ExpectedOutputKind,
    source_column: Option<&'static str>,
}

struct ExpectedAggregateMeasure {
    output_name: &'static str,
    function: BrowserAggregateFunction,
    source_column: Option<&'static str>,
}

struct ExpectedSortKey {
    output_name: &'static str,
    descending: bool,
}

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
            },
            wasm_query_runtime::BrowserParquetField {
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
            },
        ]
    );
    assert_eq!(
        summary.schema.partition_columns,
        vec!["category".to_string()]
    );

    let first_file = &bootstrapped.active_files()[0];
    assert_eq!(
        first_file.partition_values().get("category"),
        Some(&Some("A".to_string()))
    );
    assert_eq!(first_file.metadata().row_group_count, 1);
    assert_eq!(first_file.metadata().row_count, 2);
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
            },
            wasm_query_runtime::BrowserParquetField {
                name: "category".to_string(),
                physical_type: BrowserParquetPhysicalType::ByteArray,
                logical_type: Some(BrowserParquetLogicalType::String),
                converted_type: Some(BrowserParquetConvertedType::Utf8),
                repetition: BrowserParquetRepetition::Required,
                nullable: false,
                max_definition_level: 0,
                max_repetition_level: 0,
                type_length: None,
                precision: None,
                scale: None,
            },
            wasm_query_runtime::BrowserParquetField {
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
            },
        ]
    );
    assert!(summary.schema.partition_columns.is_empty());
    assert!(bootstrapped.active_files()[0].partition_values().is_empty());
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

#[test]
fn browser_preflight_stats_and_planning_match_native_partitioned_metrics() {
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
    let first_file_stats = &bootstrapped.active_files()[0].metadata().field_stats;

    assert_eq!(first_file_stats["id"].min_i64, Some(1));
    assert_eq!(first_file_stats["id"].max_i64, Some(3));
    assert_eq!(first_file_stats["value"].min_i64, Some(10));
    assert_eq!(first_file_stats["value"].max_i64, Some(30));
    assert_eq!(first_file_stats["value"].null_count, Some(0));

    for (sql, expected_files_touched, expected_files_skipped) in [
        (
            format!("SELECT id FROM {DEFAULT_TABLE_NAME} ORDER BY id"),
            3_u64,
            0_u64,
        ),
        (
            format!("SELECT id, value FROM {DEFAULT_TABLE_NAME} WHERE category = 'C' ORDER BY id"),
            1_u64,
            2_u64,
        ),
        (
            format!("SELECT id FROM {DEFAULT_TABLE_NAME} WHERE category = 'Z' ORDER BY id"),
            0_u64,
            3_u64,
        ),
        (
            format!("SELECT id FROM {DEFAULT_TABLE_NAME} WHERE value >= 40 ORDER BY id"),
            2_u64,
            1_u64,
        ),
    ] {
        let browser_plan = session
            .plan_query(
                &bootstrapped,
                &QueryRequest {
                    snapshot_version: Some(resolved_snapshot.snapshot_version),
                    ..QueryRequest::new(
                        &resolved_snapshot.table_uri,
                        sql.clone(),
                        ExecutionTarget::BrowserWasm,
                    )
                },
            )
            .expect("browser planner should accept supported partitioned queries");
        let native_result = execute_query(QueryRequest {
            snapshot_version: Some(resolved_snapshot.snapshot_version),
            ..QueryRequest::new(&resolved_snapshot.table_uri, sql, ExecutionTarget::Native)
        })
        .expect("native query should execute");

        assert_eq!(browser_plan.candidate_file_count, expected_files_touched);
        assert_eq!(browser_plan.pruning.files_pruned, expected_files_skipped);
        assert_eq!(native_result.metrics.files_touched, expected_files_touched);
        assert_eq!(native_result.metrics.files_skipped, expected_files_skipped);
    }
}

#[test]
fn supported_browser_sql_queries_have_native_parity_on_partitioned_fixture() {
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

    for case in supported_browser_sql_parity_cases() {
        let browser_plan = browser_plan_for_case(
            &session,
            &bootstrapped,
            &resolved_snapshot.table_uri,
            resolved_snapshot.snapshot_version,
            &case.sql,
        );
        let execution_plan = execution_plan_for_case(
            &session,
            &bootstrapped,
            &resolved_snapshot.table_uri,
            resolved_snapshot.snapshot_version,
            &case.sql,
        );
        let native_result = native_result_for_case(
            &resolved_snapshot.table_uri,
            Some(resolved_snapshot.snapshot_version),
            &case.sql,
        );

        assert_execution_plan_shape(&execution_plan, &browser_plan, &case);

        if case.assert_scan_metrics {
            assert_eq!(
                browser_plan.candidate_file_count, native_result.metrics.files_touched,
                "case '{}': browser/native touched-file parity should hold",
                case.name
            );
            assert_eq!(
                browser_plan.pruning.files_pruned, native_result.metrics.files_skipped,
                "case '{}': browser/native skipped-file parity should hold",
                case.name
            );
        }
    }
}

#[test]
fn intentional_browser_native_sql_envelope_divergences_are_explicit() {
    let fixture = TestTableFixture::create_partitioned();

    for case in intentional_browser_native_divergence_cases() {
        let native_result = native_result_for_case(&fixture.table_uri, None, &case.sql);
        assert!(
            !native_result.batches.is_empty(),
            "case '{}': native should execute known browser-only divergence cases",
            case.name
        );

        let browser_error = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
            .expect("default browser runtime config should be supported")
            .analyze_query_shape(&case.sql)
            .expect_err("browser should reject intentional native-only divergence cases");
        assert_eq!(
            browser_error.code,
            QueryErrorCode::InvalidRequest,
            "case '{}': browser rejection should remain an invalid-request envelope failure",
            case.name
        );
    }
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

fn supported_browser_sql_parity_cases() -> Vec<SupportedBrowserSqlParityCase> {
    vec![
        SupportedBrowserSqlParityCase {
            name: "limit_order_by",
            sql: format!("SELECT id FROM {DEFAULT_TABLE_NAME} ORDER BY id LIMIT 1"),
            assert_scan_metrics: false,
            expected_outputs: vec![ExpectedExecutionOutput {
                output_name: "id",
                kind: ExpectedOutputKind::Passthrough,
                source_column: Some("id"),
            }],
            expected_group_by_columns: vec![],
            expected_measures: vec![],
            expected_order_by: vec![ExpectedSortKey {
                output_name: "id",
                descending: false,
            }],
            expected_limit: Some(1),
        },
        SupportedBrowserSqlParityCase {
            name: "count_star",
            sql: format!("SELECT COUNT(*) AS row_count FROM {DEFAULT_TABLE_NAME}"),
            assert_scan_metrics: false,
            expected_outputs: vec![ExpectedExecutionOutput {
                output_name: "row_count",
                kind: ExpectedOutputKind::Aggregate,
                source_column: None,
            }],
            expected_group_by_columns: vec![],
            expected_measures: vec![ExpectedAggregateMeasure {
                output_name: "row_count",
                function: BrowserAggregateFunction::CountStar,
                source_column: None,
            }],
            expected_order_by: vec![],
            expected_limit: None,
        },
        SupportedBrowserSqlParityCase {
            name: "avg_value",
            sql: format!("SELECT AVG(value) AS avg_value FROM {DEFAULT_TABLE_NAME}"),
            assert_scan_metrics: false,
            expected_outputs: vec![ExpectedExecutionOutput {
                output_name: "avg_value",
                kind: ExpectedOutputKind::Aggregate,
                source_column: Some("value"),
            }],
            expected_group_by_columns: vec![],
            expected_measures: vec![ExpectedAggregateMeasure {
                output_name: "avg_value",
                function: BrowserAggregateFunction::Avg,
                source_column: Some("value"),
            }],
            expected_order_by: vec![],
            expected_limit: None,
        },
        SupportedBrowserSqlParityCase {
            name: "group_by_aggregate",
            sql: format!(
                "SELECT category, SUM(value) AS total_value \
                 FROM {DEFAULT_TABLE_NAME} \
                 GROUP BY category \
                 ORDER BY category \
                 LIMIT 2"
            ),
            assert_scan_metrics: false,
            expected_outputs: vec![
                ExpectedExecutionOutput {
                    output_name: "category",
                    kind: ExpectedOutputKind::Passthrough,
                    source_column: Some("category"),
                },
                ExpectedExecutionOutput {
                    output_name: "total_value",
                    kind: ExpectedOutputKind::Aggregate,
                    source_column: Some("value"),
                },
            ],
            expected_group_by_columns: vec!["category"],
            expected_measures: vec![ExpectedAggregateMeasure {
                output_name: "total_value",
                function: BrowserAggregateFunction::Sum,
                source_column: Some("value"),
            }],
            expected_order_by: vec![ExpectedSortKey {
                output_name: "category",
                descending: false,
            }],
            expected_limit: Some(2),
        },
        SupportedBrowserSqlParityCase {
            name: "cte_alias_projection",
            sql: format!(
                "WITH filtered AS (SELECT value AS total FROM {DEFAULT_TABLE_NAME}) \
                 SELECT total FROM filtered ORDER BY total"
            ),
            assert_scan_metrics: false,
            expected_outputs: vec![ExpectedExecutionOutput {
                output_name: "total",
                kind: ExpectedOutputKind::Passthrough,
                source_column: Some("value"),
            }],
            expected_group_by_columns: vec![],
            expected_measures: vec![],
            expected_order_by: vec![ExpectedSortKey {
                output_name: "total",
                descending: false,
            }],
            expected_limit: None,
        },
        SupportedBrowserSqlParityCase {
            name: "partition_pruned_equality",
            sql: format!("SELECT id FROM {DEFAULT_TABLE_NAME} WHERE category = 'C' ORDER BY id"),
            assert_scan_metrics: true,
            expected_outputs: vec![ExpectedExecutionOutput {
                output_name: "id",
                kind: ExpectedOutputKind::Passthrough,
                source_column: Some("id"),
            }],
            expected_group_by_columns: vec![],
            expected_measures: vec![],
            expected_order_by: vec![ExpectedSortKey {
                output_name: "id",
                descending: false,
            }],
            expected_limit: None,
        },
        SupportedBrowserSqlParityCase {
            name: "partition_pruned_no_match",
            sql: format!("SELECT id FROM {DEFAULT_TABLE_NAME} WHERE category = 'Z' ORDER BY id"),
            assert_scan_metrics: true,
            expected_outputs: vec![ExpectedExecutionOutput {
                output_name: "id",
                kind: ExpectedOutputKind::Passthrough,
                source_column: Some("id"),
            }],
            expected_group_by_columns: vec![],
            expected_measures: vec![],
            expected_order_by: vec![ExpectedSortKey {
                output_name: "id",
                descending: false,
            }],
            expected_limit: None,
        },
        SupportedBrowserSqlParityCase {
            name: "stats_pruned_range",
            sql: format!("SELECT id FROM {DEFAULT_TABLE_NAME} WHERE value >= 40 ORDER BY id"),
            assert_scan_metrics: true,
            expected_outputs: vec![ExpectedExecutionOutput {
                output_name: "id",
                kind: ExpectedOutputKind::Passthrough,
                source_column: Some("id"),
            }],
            expected_group_by_columns: vec![],
            expected_measures: vec![],
            expected_order_by: vec![ExpectedSortKey {
                output_name: "id",
                descending: false,
            }],
            expected_limit: None,
        },
    ]
}

fn intentional_browser_native_divergence_cases() -> Vec<IntentionalBrowserNativeDivergenceCase> {
    vec![
        IntentionalBrowserNativeDivergenceCase {
            name: "distinct_projection",
            sql: format!("SELECT DISTINCT id FROM {DEFAULT_TABLE_NAME}"),
        },
        IntentionalBrowserNativeDivergenceCase {
            name: "wildcard_projection",
            sql: format!("SELECT * FROM {DEFAULT_TABLE_NAME} LIMIT 1"),
        },
        IntentionalBrowserNativeDivergenceCase {
            name: "set_operation_union_all",
            sql: format!(
                "SELECT id FROM {DEFAULT_TABLE_NAME} \
                 UNION ALL \
                 SELECT id FROM {DEFAULT_TABLE_NAME}"
            ),
        },
    ]
}

fn browser_plan_for_case(
    session: &BrowserRuntimeSession,
    bootstrapped: &BootstrappedBrowserSnapshot,
    table_uri: &str,
    snapshot_version: i64,
    sql: &str,
) -> BrowserPlannedQuery {
    session
        .analyze_query_shape(sql)
        .expect("supported browser corpus cases should analyze");

    session
        .plan_query(
            bootstrapped,
            &QueryRequest {
                snapshot_version: Some(snapshot_version),
                ..QueryRequest::new(table_uri, sql, ExecutionTarget::BrowserWasm)
            },
        )
        .expect("supported browser corpus cases should plan")
}

fn execution_plan_for_case(
    session: &BrowserRuntimeSession,
    bootstrapped: &BootstrappedBrowserSnapshot,
    table_uri: &str,
    snapshot_version: i64,
    sql: &str,
) -> BrowserExecutionPlan {
    session
        .build_execution_plan(
            bootstrapped,
            &QueryRequest {
                snapshot_version: Some(snapshot_version),
                ..QueryRequest::new(table_uri, sql, ExecutionTarget::BrowserWasm)
            },
        )
        .expect("supported browser corpus cases should build execution plans")
}

fn assert_execution_plan_shape(
    execution_plan: &BrowserExecutionPlan,
    browser_plan: &BrowserPlannedQuery,
    case: &SupportedBrowserSqlParityCase,
) {
    assert_eq!(
        execution_plan.scan().candidate_paths(),
        browser_plan.candidate_paths.as_slice(),
        "case '{}': execution-plan candidate paths should match browser planning",
        case.name
    );
    assert_eq!(
        execution_plan.scan().candidate_file_count(),
        browser_plan.candidate_file_count,
        "case '{}': execution-plan candidate-file count should match browser planning",
        case.name
    );
    assert_eq!(
        execution_plan.scan().candidate_bytes(),
        browser_plan.candidate_bytes,
        "case '{}': execution-plan candidate bytes should match browser planning",
        case.name
    );
    assert_eq!(
        execution_plan.scan().candidate_rows(),
        browser_plan.candidate_rows,
        "case '{}': execution-plan candidate rows should match browser planning",
        case.name
    );
    assert_eq!(
        execution_plan.outputs().len(),
        case.expected_outputs.len(),
        "case '{}': execution-plan output count should match",
        case.name
    );
    for (output, expected) in execution_plan
        .outputs()
        .iter()
        .zip(case.expected_outputs.iter())
    {
        assert_eq!(
            output.output_name(),
            expected.output_name,
            "case '{}': execution-plan output name should match",
            case.name
        );
        match (output.kind(), expected.kind) {
            (
                BrowserExecutionOutputKind::Passthrough { source_column },
                ExpectedOutputKind::Passthrough,
            ) => {
                assert_eq!(
                    Some(source_column.as_str()),
                    expected.source_column,
                    "case '{}': passthrough source column should match",
                    case.name
                );
            }
            (
                BrowserExecutionOutputKind::Aggregate {
                    function: _,
                    source_column,
                },
                ExpectedOutputKind::Aggregate,
            ) => {
                assert_eq!(
                    source_column.as_deref(),
                    expected.source_column,
                    "case '{}': aggregate source column should match",
                    case.name
                );
            }
            _ => panic!(
                "case '{}': execution-plan output kind should match",
                case.name
            ),
        }
    }

    let aggregation = execution_plan.aggregation();
    assert_eq!(
        aggregation
            .map(|aggregation| {
                aggregation
                    .group_by_columns()
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
        case.expected_group_by_columns,
        "case '{}': execution-plan group-by columns should match",
        case.name
    );
    assert_eq!(
        aggregation
            .map(|aggregation| aggregation.measures().len())
            .unwrap_or_default(),
        case.expected_measures.len(),
        "case '{}': execution-plan measure count should match",
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
                "case '{}': execution-plan aggregate output name should match",
                case.name
            );
            assert_eq!(
                measure.function(),
                &expected.function,
                "case '{}': execution-plan aggregate function should match",
                case.name
            );
            assert_eq!(
                measure.source_column(),
                expected.source_column,
                "case '{}': execution-plan aggregate source column should match",
                case.name
            );
        }
    }

    assert_eq!(
        execution_plan.order_by().len(),
        case.expected_order_by.len(),
        "case '{}': execution-plan sort-key count should match",
        case.name
    );
    for (sort_key, expected) in execution_plan
        .order_by()
        .iter()
        .zip(case.expected_order_by.iter())
    {
        assert_eq!(
            sort_key.output_name(),
            expected.output_name,
            "case '{}': execution-plan sort-key output should match",
            case.name
        );
        assert_eq!(
            sort_key.descending(),
            expected.descending,
            "case '{}': execution-plan sort-key direction should match",
            case.name
        );
    }

    assert_eq!(
        execution_plan.limit(),
        case.expected_limit,
        "case '{}': execution-plan limit should match",
        case.name
    );
}

fn native_result_for_case(
    table_uri: &str,
    snapshot_version: Option<i64>,
    sql: &str,
) -> NativeQueryResult {
    execute_query(QueryRequest {
        snapshot_version,
        ..QueryRequest::new(table_uri, sql, ExecutionTarget::Native)
    })
    .expect("native oracle cases should execute")
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
