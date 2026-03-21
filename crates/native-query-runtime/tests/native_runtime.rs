use std::path::PathBuf;
use std::sync::Arc;

use deltalake::arrow::array::{Int32Array, StringArray};
use deltalake::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::arrow::util::pretty::pretty_format_batches;
use deltalake::kernel::{DataType, PrimitiveType, StructField};
use deltalake::DeltaTable;
use native_query_runtime::{bootstrap_table, execute_query, DEFAULT_TABLE_NAME};
use query_contract::{
    CapabilityKey, CapabilityState, ExecutionTarget, QueryErrorCode, QueryExecutionOptions,
    QueryRequest,
};
use serde::Deserialize;
use tempfile::TempDir;

struct TestTableFixture {
    _tempdir: TempDir,
    table_uri: String,
}

#[derive(Debug, Deserialize)]
struct QueryCorpusCase {
    name: String,
    sql: String,
    expected_pretty: String,
}

impl TestTableFixture {
    fn create() -> Self {
        let tempdir = TempDir::new().expect("tempdir should be created");
        let table_uri = deltalake::ensure_table_uri(tempdir.path().to_string_lossy())
            .expect("table uri should be normalized")
            .to_string();

        tokio::runtime::Runtime::new()
            .expect("runtime should be created")
            .block_on(async {
                let table = DeltaTable::try_from_url(
                    deltalake::ensure_table_uri(&table_uri).expect("table uri should parse"),
                )
                .await
                .expect("table handle should be created");

                let table = table
                    .create()
                    .with_columns(vec![
                        StructField::new(
                            "id".to_string(),
                            DataType::Primitive(PrimitiveType::Integer),
                            false,
                        ),
                        StructField::new(
                            "category".to_string(),
                            DataType::Primitive(PrimitiveType::String),
                            false,
                        ),
                        StructField::new(
                            "value".to_string(),
                            DataType::Primitive(PrimitiveType::Integer),
                            false,
                        ),
                    ])
                    .with_table_name("axon_fixture")
                    .await
                    .expect("table should be created");

                let table = table
                    .write(vec![fixture_batch(
                        &[1, 2, 3],
                        &["A", "B", "A"],
                        &[10, 20, 30],
                    )])
                    .await
                    .expect("first batch should be written");

                table
                    .write(vec![fixture_batch(
                        &[4, 5, 6],
                        &["B", "B", "C"],
                        &[40, 50, 60],
                    )])
                    .await
                    .expect("second batch should be written");
            });

        Self {
            _tempdir: tempdir,
            table_uri,
        }
    }
}

fn fixture_batch(ids: &[i32], categories: &[&str], values: &[i32]) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowDataType::Int32, false),
        Field::new("category", ArrowDataType::Utf8, false),
        Field::new("value", ArrowDataType::Int32, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(categories.to_vec())),
            Arc::new(Int32Array::from(values.to_vec())),
        ],
    )
    .expect("fixture batch should be created")
}

fn query_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/conformance/native-runtime-sql-corpus.json")
}

fn load_query_corpus() -> Vec<QueryCorpusCase> {
    serde_json::from_str(
        &std::fs::read_to_string(query_corpus_path()).expect("query corpus should be readable"),
    )
    .expect("query corpus should deserialize")
}

#[test]
fn bootstrap_table_reports_local_snapshot_metadata() {
    let fixture = TestTableFixture::create();

    let bootstrap = bootstrap_table(&fixture.table_uri).expect("bootstrap should succeed");

    assert_eq!(bootstrap.table_uri, fixture.table_uri);
    assert_eq!(bootstrap.version, 2);
    assert_eq!(bootstrap.active_files, 2);
}

#[test]
fn bootstrap_table_rejects_non_delta_locations() {
    let tempdir = TempDir::new().expect("tempdir should be created");
    let table_uri = deltalake::ensure_table_uri(tempdir.path().to_string_lossy())
        .expect("table uri should be normalized")
        .to_string();

    let error = bootstrap_table(&table_uri).expect_err("empty directory is not a delta table");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert_eq!(error.target, ExecutionTarget::Native);
}

#[test]
fn bootstrap_table_does_not_create_missing_local_directories() {
    let tempdir = TempDir::new().expect("tempdir should be created");
    let missing_path = tempdir.path().join("missing-table");

    let error = bootstrap_table(&missing_path.to_string_lossy())
        .expect_err("missing local path should fail before bootstrap");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(
        !missing_path.exists(),
        "bootstrap should not create local directories for read-only access"
    );
}

#[test]
fn execute_query_runs_the_native_sql_corpus_with_golden_results() {
    let fixture = TestTableFixture::create();
    let mut saw_metrics = false;
    let corpus = load_query_corpus();

    assert!(
        corpus.len() >= 4,
        "corpus should contain the Sprint 1 query cases"
    );

    for case in corpus {
        let request = QueryRequest::new(&fixture.table_uri, case.sql, ExecutionTarget::Native);
        let result = execute_query(request).unwrap_or_else(|error| {
            panic!(
                "query case '{}' should execute successfully: {error:?}",
                case.name
            )
        });

        assert_eq!(
            pretty_format_batches(&result.batches)
                .expect("batches should format")
                .to_string(),
            case.expected_pretty,
            "query case '{}' should match the golden result",
            case.name
        );
        assert_eq!(
            result.capabilities.state(CapabilityKey::RangeReads),
            Some(CapabilityState::Supported),
            "query case '{}' should report range read support",
            case.name
        );
        assert_eq!(
            result.metrics.files_touched, 2,
            "query case '{}' should report the active file count",
            case.name
        );
        assert_eq!(
            result.metrics.files_skipped, 0,
            "query case '{}' should report zero skipped files for the fixture",
            case.name
        );
        saw_metrics |= result.metrics.bytes_fetched > 0 && result.metrics.duration_ms > 0;
    }

    assert!(
        saw_metrics,
        "at least one corpus case should report populated metrics"
    );
}

#[test]
fn execute_query_optionally_returns_explain_output() {
    let fixture = TestTableFixture::create();
    let request = QueryRequest::new(
        &fixture.table_uri,
        format!("SELECT id FROM {DEFAULT_TABLE_NAME} WHERE value >= 40"),
        ExecutionTarget::Native,
    )
    .with_options(QueryExecutionOptions {
        include_explain: true,
        collect_metrics: true,
    });

    let result = execute_query(request).expect("query should execute");
    let explain = result
        .explain_lines
        .expect("explain output should be collected")
        .join("\n");

    assert!(explain.contains("TableScan") || explain.contains("DataSourceExec"));
}

#[test]
fn execute_query_rejects_queries_without_axon_table() {
    let fixture = TestTableFixture::create();
    let request = QueryRequest::new(&fixture.table_uri, "SELECT 1", ExecutionTarget::Native);

    let error = execute_query(request).expect_err("constant queries should be rejected");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(
        error.message.contains(DEFAULT_TABLE_NAME),
        "error should explain the required table binding: {}",
        error.message
    );
}

#[test]
fn execute_query_rejects_non_read_only_sql() {
    let fixture = TestTableFixture::create();
    let request = QueryRequest::new(
        &fixture.table_uri,
        format!("CREATE VIEW scratch_view AS SELECT * FROM {DEFAULT_TABLE_NAME}"),
        ExecutionTarget::Native,
    );

    let error = execute_query(request).expect_err("DDL should be rejected");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
}

#[test]
fn execute_query_rejects_non_table_sources() {
    let fixture = TestTableFixture::create();
    let request = QueryRequest::new(
        &fixture.table_uri,
        "SELECT * FROM generate_series(1, 2)",
        ExecutionTarget::Native,
    );

    let error = execute_query(request).expect_err("table functions should be rejected");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(
        error.message.contains(DEFAULT_TABLE_NAME),
        "error should explain the required table binding: {}",
        error.message
    );
}

#[test]
fn execute_query_allows_ctes_built_from_axon_table() {
    let fixture = TestTableFixture::create();
    let request = QueryRequest::new(
        &fixture.table_uri,
        format!(
            "WITH filtered AS (SELECT id FROM {DEFAULT_TABLE_NAME} WHERE value >= 50) \
             SELECT id FROM filtered ORDER BY id"
        ),
        ExecutionTarget::Native,
    );

    let result = execute_query(request).expect("CTEs over axon_table should execute");

    assert_eq!(
        pretty_format_batches(&result.batches)
            .expect("batches should format")
            .to_string(),
        "+----+\n| id |\n+----+\n| 5  |\n| 6  |\n+----+"
    );
}

#[test]
fn execute_query_is_safe_to_call_from_an_existing_tokio_runtime() {
    let fixture = TestTableFixture::create();
    let request = QueryRequest::new(
        &fixture.table_uri,
        format!("SELECT id FROM {DEFAULT_TABLE_NAME} ORDER BY id LIMIT 1"),
        ExecutionTarget::Native,
    );

    let result = tokio::runtime::Runtime::new()
        .expect("runtime should be created")
        .block_on(async move { execute_query(request) });

    assert!(
        result.is_ok(),
        "sync query API should remain callable inside an existing runtime"
    );
}

#[test]
fn bootstrap_table_supports_env_gated_gcs_smoke() {
    let Ok(table_uri) = std::env::var("AXON_GCS_TEST_TABLE_URI") else {
        return;
    };

    let bootstrap = bootstrap_table(&table_uri).expect("gcs bootstrap should succeed");

    assert_eq!(bootstrap.table_uri, table_uri);
    assert!(bootstrap.version >= 0);
    assert!(bootstrap.active_files > 0);
}

#[test]
fn execute_query_supports_env_gated_gcs_smoke() {
    let Ok(table_uri) = std::env::var("AXON_GCS_TEST_TABLE_URI") else {
        return;
    };

    let request = QueryRequest::new(
        &table_uri,
        format!("SELECT * FROM {DEFAULT_TABLE_NAME} LIMIT 1"),
        ExecutionTarget::Native,
    );

    let result = execute_query(request).expect("gcs query should succeed");
    let row_count: usize = result.batches.iter().map(RecordBatch::num_rows).sum();

    assert_eq!(row_count, 1, "query smoke should return a single row");
    assert!(
        result.metrics.bytes_fetched > 0,
        "query smoke should report snapshot-derived byte metrics"
    );
    assert!(
        result.metrics.files_touched > 0,
        "query smoke should report touched files"
    );
    assert!(
        result.metrics.duration_ms > 0,
        "query smoke should record elapsed time"
    );
    assert_eq!(
        result.capabilities.state(CapabilityKey::RangeReads),
        Some(CapabilityState::Supported),
        "query smoke should report range-read support"
    );
}
