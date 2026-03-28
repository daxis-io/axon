mod support;

use delta_control_plane::resolve_snapshot;
use deltalake::arrow::array::{
    Array, Float64Array, Int32Array, Int64Array, StringArray, UInt64Array,
};
use deltalake::arrow::datatypes::DataType as ArrowDataType;
use deltalake::arrow::util::display::array_value_to_string;
use native_query_runtime::{execute_query, NativeQueryResult, DEFAULT_TABLE_NAME};
use query_contract::{
    ExecutionTarget, PartitionColumnType, QueryErrorCode, QueryRequest, SnapshotResolutionRequest,
};
use support::{LoopbackObjectServer, LoopbackObjectServerOptions, TestTableFixture};
use wasm_query_runtime::{
    BootstrappedBrowserSnapshot, BrowserAggregateFunction, BrowserComparisonOp,
    BrowserExecutionOutputKind, BrowserExecutionPlan, BrowserExecutionResult, BrowserFilterExpr,
    BrowserObjectSource, BrowserParquetConvertedType, BrowserParquetLogicalType,
    BrowserParquetPhysicalType, BrowserParquetRepetition, BrowserPlannedQuery,
    BrowserRuntimeConfig, BrowserRuntimeSession, BrowserScalarValue, MaterializedBrowserFile,
    MaterializedBrowserSnapshot,
};

struct SupportedBrowserSqlParityCase {
    name: &'static str,
    sql: String,
    assert_scan_metrics: bool,
    expected_required_columns: Vec<&'static str>,
    expected_filter: Option<ExpectedFilterExpr>,
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

enum ExpectedFilterExpr {
    Compare {
        source_column: &'static str,
        comparison: BrowserComparisonOp,
        literal: BrowserScalarValue,
    },
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
    let materialized = MaterializedBrowserSnapshot::new_with_partition_column_types(
        resolved_snapshot.table_uri,
        resolved_snapshot.snapshot_version,
        files,
        resolved_snapshot.partition_column_types,
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
fn supported_browser_sql_queries_lower_typed_filters_on_partitioned_fixture() {
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
        let execution_plan = execution_plan_for_case(
            &session,
            &bootstrapped,
            &resolved_snapshot.table_uri,
            resolved_snapshot.snapshot_version,
            &case.sql,
        );

        assert_eq!(
            execution_plan.scan().required_columns(),
            case.expected_required_columns.as_slice(),
            "case '{}': required scan columns should match",
            case.name
        );

        match (execution_plan.filter(), &case.expected_filter) {
            (Some(actual), Some(expected)) => {
                assert_filter_expr_matches(actual, expected, case.name);
            }
            (None, None) => {}
            (Some(actual), None) => {
                panic!(
                    "case '{}': execution plan unexpectedly lowered a filter: {:?}",
                    case.name, actual
                );
            }
            (None, Some(_)) => {
                panic!(
                    "case '{}': execution plan did not lower the expected filter",
                    case.name
                );
            }
        }
    }
}

#[test]
fn supported_browser_non_aggregate_queries_have_native_result_parity_on_partitioned_fixture() {
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

    for case in supported_browser_sql_parity_cases()
        .into_iter()
        .filter(|case| case.expected_measures.is_empty())
    {
        let execution_plan = execution_plan_for_case(
            &session,
            &bootstrapped,
            &resolved_snapshot.table_uri,
            resolved_snapshot.snapshot_version,
            &case.sql,
        );
        let browser_result = runtime()
            .block_on(session.execute_plan(&materialized, &execution_plan))
            .expect("browser execution should succeed for supported non-aggregate cases");
        let native_result = native_result_for_case(
            &resolved_snapshot.table_uri,
            Some(resolved_snapshot.snapshot_version),
            &case.sql,
        );

        assert_eq!(
            normalize_browser_result(&browser_result),
            normalize_native_result(
                &native_result,
                &case
                    .expected_outputs
                    .iter()
                    .map(|output| output.output_name.to_string())
                    .collect::<Vec<_>>(),
            ),
            "case '{}': browser/native rows should match",
            case.name
        );
    }
}

#[test]
fn supported_browser_aggregate_queries_have_native_result_parity_on_partitioned_fixture() {
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

    for case in supported_browser_sql_parity_cases()
        .into_iter()
        .filter(|case| !case.expected_measures.is_empty())
    {
        let execution_plan = execution_plan_for_case(
            &session,
            &bootstrapped,
            &resolved_snapshot.table_uri,
            resolved_snapshot.snapshot_version,
            &case.sql,
        );
        let browser_result = runtime()
            .block_on(session.execute_plan(&materialized, &execution_plan))
            .expect("browser execution should succeed for supported aggregate cases");
        let native_result = native_result_for_case(
            &resolved_snapshot.table_uri,
            Some(resolved_snapshot.snapshot_version),
            &case.sql,
        );

        assert_eq!(
            normalize_browser_result(&browser_result),
            normalize_native_result(
                &native_result,
                &case
                    .expected_outputs
                    .iter()
                    .map(|output| output.output_name.to_string())
                    .collect::<Vec<_>>(),
            ),
            "case '{}': browser/native rows should match",
            case.name
        );
    }
}

#[test]
fn browser_execution_reports_metrics_for_pruned_and_empty_scans() {
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

    for sql in [
        format!("SELECT id FROM {DEFAULT_TABLE_NAME} WHERE category = 'C' ORDER BY id"),
        format!("SELECT id FROM {DEFAULT_TABLE_NAME} WHERE category = 'Z' ORDER BY id"),
    ] {
        let execution_plan = execution_plan_for_case(
            &session,
            &bootstrapped,
            &resolved_snapshot.table_uri,
            resolved_snapshot.snapshot_version,
            &sql,
        );
        let browser_result = runtime()
            .block_on(session.execute_plan(&materialized, &execution_plan))
            .expect("browser execution should succeed for metric cases");
        let native_result = native_result_for_case(
            &resolved_snapshot.table_uri,
            Some(resolved_snapshot.snapshot_version),
            &sql,
        );

        assert_eq!(
            browser_result.metrics().bytes_fetched,
            execution_plan.scan().candidate_bytes(),
            "sql '{}': fetched bytes should match the candidate scan bytes",
            sql
        );
        assert_eq!(
            browser_result.metrics().files_touched,
            native_result.metrics.files_touched,
            "sql '{}': touched files should match native metrics",
            sql
        );
        assert_eq!(
            browser_result.metrics().files_skipped,
            native_result.metrics.files_skipped,
            "sql '{}': skipped files should match native metrics",
            sql
        );
    }
}

#[test]
fn browser_execution_rejects_bootstrap_to_execution_etag_drift_on_real_fixture_files() {
    let fixture = TestTableFixture::create_partitioned();
    let resolved_snapshot = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri.clone(),
        snapshot_version: None,
    })
    .expect("latest snapshot should resolve");
    let server = LoopbackObjectServer::from_fixture_paths_with_options(
        &fixture,
        resolved_snapshot
            .active_files
            .iter()
            .map(|file| file.path.clone()),
        LoopbackObjectServerOptions {
            range_etag: Some("\"bootstrap-v1\"".to_string()),
            full_etag: Some("\"execution-v2\"".to_string()),
            ..LoopbackObjectServerOptions::default()
        },
    );
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default browser runtime config should be supported");
    let materialized = materialize_loopback_snapshot(&resolved_snapshot, &server);
    let bootstrapped = runtime()
        .block_on(session.bootstrap_snapshot_metadata(&materialized))
        .expect("browser snapshot metadata bootstrap should succeed");
    assert!(
        bootstrapped
            .active_files()
            .iter()
            .all(|file| file.object_etag() == Some("\"bootstrap-v1\"")),
        "bootstrap should preserve the observed object etag for every file",
    );

    let sql = format!("SELECT id FROM {DEFAULT_TABLE_NAME} WHERE category = 'A' ORDER BY id");
    let execution_plan = execution_plan_for_case(
        &session,
        &bootstrapped,
        &resolved_snapshot.table_uri,
        resolved_snapshot.snapshot_version,
        &sql,
    );
    let error = runtime()
        .block_on(session.execute_plan(&materialized, &execution_plan))
        .expect_err("etag drift between bootstrap and execution should fail");

    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert!(error.message.contains("entity tag"));
}

#[test]
fn browser_execution_reports_nonzero_duration_metrics_for_successful_reads() {
    let fixture = TestTableFixture::create_partitioned();
    let resolved_snapshot = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri.clone(),
        snapshot_version: None,
    })
    .expect("latest snapshot should resolve");
    let server = LoopbackObjectServer::from_fixture_paths_with_options(
        &fixture,
        resolved_snapshot
            .active_files
            .iter()
            .map(|file| file.path.clone()),
        LoopbackObjectServerOptions {
            full_read_delay: Some(std::time::Duration::from_millis(25)),
            ..LoopbackObjectServerOptions::default()
        },
    );
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig {
        request_timeout_ms: 250,
        execution_timeout_ms: 1_000,
        ..BrowserRuntimeConfig::default()
    })
    .expect("delayed full-read browser runtime config should be supported");
    let materialized = materialize_loopback_snapshot(&resolved_snapshot, &server);
    let bootstrapped = runtime()
        .block_on(session.bootstrap_snapshot_metadata(&materialized))
        .expect("browser snapshot metadata bootstrap should succeed");

    let sql = format!("SELECT id FROM {DEFAULT_TABLE_NAME} WHERE category = 'A' ORDER BY id");
    let execution_plan = execution_plan_for_case(
        &session,
        &bootstrapped,
        &resolved_snapshot.table_uri,
        resolved_snapshot.snapshot_version,
        &sql,
    );
    let browser_result = runtime()
        .block_on(session.execute_plan(&materialized, &execution_plan))
        .expect("browser execution should succeed for delayed duration metrics");
    let native_result = native_result_for_case(
        &resolved_snapshot.table_uri,
        Some(resolved_snapshot.snapshot_version),
        &sql,
    );

    assert_eq!(
        normalize_browser_result(&browser_result),
        normalize_native_result(&native_result, &["id".to_string()]),
        "browser/native rows should match for delayed metric cases",
    );
    assert!(
        browser_result.metrics().duration_ms >= 20,
        "duration should reflect the delayed full-object read, got {} ms",
        browser_result.metrics().duration_ms
    );
    assert!(
        browser_result.metrics().duration_ms < 1_000,
        "duration should remain bounded by the configured execution timeout, got {} ms",
        browser_result.metrics().duration_ms
    );
}

#[test]
fn supported_browser_integer_partition_queries_have_native_parity_and_typed_filters() {
    let fixture = TestTableFixture::create_integer_partitioned_mixed_case();
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

    let filtered_sql =
        format!("SELECT id FROM {DEFAULT_TABLE_NAME} WHERE \"Year\" = 2024 ORDER BY id");
    let filtered_plan = execution_plan_for_case(
        &session,
        &bootstrapped,
        &resolved_snapshot.table_uri,
        resolved_snapshot.snapshot_version,
        &filtered_sql,
    );
    assert_eq!(filtered_plan.scan().required_columns(), ["id", "year"]);
    assert_eq!(
        filtered_plan.filter(),
        Some(&BrowserFilterExpr::Compare {
            source_column: "year".to_string(),
            comparison: BrowserComparisonOp::Eq,
            literal: BrowserScalarValue::Int64(2024),
        })
    );

    let filtered_browser = runtime()
        .block_on(session.execute_plan(&materialized, &filtered_plan))
        .expect("browser integer-partition execution should succeed");
    let filtered_native = native_result_for_case(
        &resolved_snapshot.table_uri,
        Some(resolved_snapshot.snapshot_version),
        &filtered_sql,
    );
    assert_eq!(
        normalize_browser_result(&filtered_browser),
        normalize_native_result(&filtered_native, &["id".to_string()]),
        "browser/native rows should match for typed integer partition filters",
    );

    let grouped_sql = format!(
        "SELECT \"Year\" AS year, SUM(value) AS total_value \
         FROM {DEFAULT_TABLE_NAME} \
         GROUP BY \"Year\" \
         ORDER BY year"
    );
    let grouped_plan = execution_plan_for_case(
        &session,
        &bootstrapped,
        &resolved_snapshot.table_uri,
        resolved_snapshot.snapshot_version,
        &grouped_sql,
    );
    let grouped_browser = runtime()
        .block_on(session.execute_plan(&materialized, &grouped_plan))
        .expect("browser integer-partition aggregates should succeed");
    let grouped_native = native_result_for_case(
        &resolved_snapshot.table_uri,
        Some(resolved_snapshot.snapshot_version),
        &grouped_sql,
    );
    assert_eq!(
        normalize_browser_result(&grouped_browser),
        normalize_native_result(
            &grouped_native,
            &["year".to_string(), "total_value".to_string()],
        ),
        "browser/native rows should match for grouped integer partition outputs",
    );
}

#[test]
fn supported_browser_numeric_string_partition_queries_have_native_parity_and_string_filters() {
    let fixture = TestTableFixture::create_numeric_string_partitioned();
    let resolved_snapshot = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri.clone(),
        snapshot_version: None,
    })
    .expect("latest snapshot should resolve");
    assert_eq!(
        resolved_snapshot.partition_column_types,
        std::collections::BTreeMap::from([("year_code".to_string(), PartitionColumnType::String,)])
    );
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

    let filtered_sql =
        format!("SELECT id FROM {DEFAULT_TABLE_NAME} WHERE year_code = '2024' ORDER BY id");
    let filtered_plan = execution_plan_for_case(
        &session,
        &bootstrapped,
        &resolved_snapshot.table_uri,
        resolved_snapshot.snapshot_version,
        &filtered_sql,
    );
    assert_eq!(filtered_plan.scan().required_columns(), ["id", "year_code"]);
    assert_eq!(
        filtered_plan.filter(),
        Some(&BrowserFilterExpr::Compare {
            source_column: "year_code".to_string(),
            comparison: BrowserComparisonOp::Eq,
            literal: BrowserScalarValue::String("2024".to_string()),
        })
    );

    let filtered_browser = runtime()
        .block_on(session.execute_plan(&materialized, &filtered_plan))
        .expect("browser numeric-string-partition execution should succeed");
    let filtered_native = native_result_for_case(
        &resolved_snapshot.table_uri,
        Some(resolved_snapshot.snapshot_version),
        &filtered_sql,
    );
    assert_eq!(
        normalize_browser_result(&filtered_browser),
        normalize_native_result(&filtered_native, &["id".to_string()]),
        "browser/native rows should match for typed string partition filters",
    );

    let grouped_sql = format!(
        "SELECT year_code, SUM(value) AS total_value \
         FROM {DEFAULT_TABLE_NAME} \
         GROUP BY year_code \
         ORDER BY year_code"
    );
    let grouped_plan = execution_plan_for_case(
        &session,
        &bootstrapped,
        &resolved_snapshot.table_uri,
        resolved_snapshot.snapshot_version,
        &grouped_sql,
    );
    let grouped_browser = runtime()
        .block_on(session.execute_plan(&materialized, &grouped_plan))
        .expect("browser numeric-string-partition aggregates should succeed");
    let grouped_native = native_result_for_case(
        &resolved_snapshot.table_uri,
        Some(resolved_snapshot.snapshot_version),
        &grouped_sql,
    );
    assert_eq!(
        normalize_browser_result(&grouped_browser),
        normalize_native_result(
            &grouped_native,
            &["year_code".to_string(), "total_value".to_string()],
        ),
        "browser/native rows should match for grouped string partition outputs",
    );
}

#[test]
fn supported_browser_nullable_order_by_queries_have_native_parity() {
    let fixture = TestTableFixture::create_nullable_values();
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

    for sql in [
        format!("SELECT id, value FROM {DEFAULT_TABLE_NAME} ORDER BY value, id"),
        format!("SELECT id, value FROM {DEFAULT_TABLE_NAME} ORDER BY value DESC, id"),
    ] {
        let execution_plan = execution_plan_for_case(
            &session,
            &bootstrapped,
            &resolved_snapshot.table_uri,
            resolved_snapshot.snapshot_version,
            &sql,
        );
        let browser_result = runtime()
            .block_on(session.execute_plan(&materialized, &execution_plan))
            .expect("browser nullable-sort execution should succeed");
        let native_result = native_result_for_case(
            &resolved_snapshot.table_uri,
            Some(resolved_snapshot.snapshot_version),
            &sql,
        );

        assert_eq!(
            normalize_browser_result(&browser_result),
            normalize_native_result(&native_result, &["id".to_string(), "value".to_string()],),
            "sql '{}': browser/native rows should match",
            sql
        );
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

    MaterializedBrowserSnapshot::new_with_partition_column_types(
        resolved_snapshot.table_uri.clone(),
        resolved_snapshot.snapshot_version,
        files,
        resolved_snapshot.partition_column_types.clone(),
    )
    .expect("duplicate-free snapshots should construct")
}

fn supported_browser_sql_parity_cases() -> Vec<SupportedBrowserSqlParityCase> {
    vec![
        SupportedBrowserSqlParityCase {
            name: "limit_order_by",
            sql: format!("SELECT id FROM {DEFAULT_TABLE_NAME} ORDER BY id LIMIT 1"),
            assert_scan_metrics: false,
            expected_required_columns: vec!["id"],
            expected_filter: None,
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
            expected_required_columns: vec![],
            expected_filter: None,
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
            expected_required_columns: vec!["value"],
            expected_filter: None,
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
            expected_required_columns: vec!["category", "value"],
            expected_filter: None,
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
            expected_required_columns: vec!["value"],
            expected_filter: None,
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
            expected_required_columns: vec!["category", "id"],
            expected_filter: Some(ExpectedFilterExpr::Compare {
                source_column: "category",
                comparison: BrowserComparisonOp::Eq,
                literal: BrowserScalarValue::String("C".to_string()),
            }),
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
            expected_required_columns: vec!["category", "id"],
            expected_filter: Some(ExpectedFilterExpr::Compare {
                source_column: "category",
                comparison: BrowserComparisonOp::Eq,
                literal: BrowserScalarValue::String("Z".to_string()),
            }),
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
            expected_required_columns: vec!["id", "value"],
            expected_filter: Some(ExpectedFilterExpr::Compare {
                source_column: "value",
                comparison: BrowserComparisonOp::Gte,
                literal: BrowserScalarValue::Int64(40),
            }),
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

#[derive(Debug, Eq, PartialEq)]
struct NormalizedQueryResult {
    output_names: Vec<String>,
    rows: Vec<Vec<BrowserScalarValue>>,
}

fn normalize_browser_result(result: &BrowserExecutionResult) -> NormalizedQueryResult {
    NormalizedQueryResult {
        output_names: result.output_names().to_vec(),
        rows: result
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect(),
    }
}

fn normalize_native_result(
    result: &NativeQueryResult,
    empty_output_names: &[String],
) -> NormalizedQueryResult {
    let output_names = result
        .batches
        .first()
        .map(|batch| {
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().to_string())
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| empty_output_names.to_vec());
    let mut rows = Vec::new();

    for batch in &result.batches {
        for row_index in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| arrow_value_to_scalar(column.as_ref(), row_index))
                    .collect(),
            );
        }
    }

    NormalizedQueryResult { output_names, rows }
}

fn arrow_value_to_scalar(column: &dyn Array, row_index: usize) -> BrowserScalarValue {
    if column.is_null(row_index) {
        return BrowserScalarValue::Null;
    }

    if let Some(values) = column.as_any().downcast_ref::<Int32Array>() {
        return BrowserScalarValue::Int64(i64::from(values.value(row_index)));
    }

    if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
        return BrowserScalarValue::Int64(values.value(row_index));
    }

    if let Some(values) = column.as_any().downcast_ref::<UInt64Array>() {
        return BrowserScalarValue::Int64(
            i64::try_from(values.value(row_index)).expect("fixture values should fit in i64"),
        );
    }

    if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
        return BrowserScalarValue::String(values.value(row_index).to_string());
    }

    if let Some(values) = column.as_any().downcast_ref::<Float64Array>() {
        let value = values.value(row_index);
        let rounded = value.round();
        assert!(
            (value - rounded).abs() < f64::EPSILON,
            "non-integral float parity normalization is outside the current fixture envelope",
        );
        return BrowserScalarValue::Int64(rounded as i64);
    }

    if matches!(
        column.data_type(),
        ArrowDataType::Dictionary(_, value_type) if **value_type == ArrowDataType::Utf8
    ) {
        return BrowserScalarValue::String(
            array_value_to_string(column, row_index)
                .expect("dictionary-encoded strings should format"),
        );
    }

    panic!(
        "unsupported arrow value in parity normalization: {:?}",
        column.data_type()
    );
}

fn assert_filter_expr_matches(
    actual: &BrowserFilterExpr,
    expected: &ExpectedFilterExpr,
    case_name: &str,
) {
    match (actual, expected) {
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
                comparison, expected_comparison,
                "case '{}': filter comparison should match",
                case_name
            );
            assert_eq!(
                literal, expected_literal,
                "case '{}': filter literal should match",
                case_name
            );
        }
        _ => panic!(
            "case '{}': execution-plan filter shape did not match expectation",
            case_name
        ),
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("tokio runtime should be created")
}
