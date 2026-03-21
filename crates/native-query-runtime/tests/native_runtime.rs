use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use deltalake::arrow::array::{Int32Array, StringArray};
use deltalake::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::arrow::util::pretty::pretty_format_batches;
use deltalake::kernel::{DataType, MetadataValue, PrimitiveType, StructField};
use deltalake::{DeltaTable, TableProperty};
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

#[derive(Debug, Deserialize)]
struct PartitionedQueryCorpusCase {
    name: String,
    sql: String,
    expected_pretty: String,
    expected_files_touched: u64,
    expected_files_skipped: u64,
}

impl TestTableFixture {
    fn create() -> Self {
        let fixture = Self::create_with_columns_and_configuration(default_table_columns(), vec![]);

        tokio::runtime::Runtime::new()
            .expect("runtime should be created")
            .block_on(async {
                let table = DeltaTable::try_from_url(
                    deltalake::ensure_table_uri(&fixture.table_uri)
                        .expect("table uri should parse"),
                )
                .await
                .expect("table handle should be created");

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

        fixture
    }

    fn create_partitioned() -> Self {
        let fixture = Self::create_with_columns_configuration_and_partitions(
            default_table_columns(),
            vec![],
            vec!["category"],
        );

        tokio::runtime::Runtime::new()
            .expect("runtime should be created")
            .block_on(async {
                let table = DeltaTable::try_from_url(
                    deltalake::ensure_table_uri(&fixture.table_uri)
                        .expect("table uri should parse"),
                )
                .await
                .expect("table handle should be created");

                table
                    .write(vec![fixture_batch(
                        &[1, 2, 3, 4, 5, 6],
                        &["A", "B", "A", "B", "B", "C"],
                        &[10, 20, 30, 40, 50, 60],
                    )])
                    .await
                    .expect("partitioned batch should be written");
            });

        fixture
    }

    fn create_with_columns_and_configuration(
        columns: Vec<StructField>,
        configuration: Vec<(String, Option<String>)>,
    ) -> Self {
        Self::create_with_columns_configuration_and_partitions(columns, configuration, vec![])
    }

    fn create_with_columns_configuration_and_partitions(
        columns: Vec<StructField>,
        configuration: Vec<(String, Option<String>)>,
        partition_columns: Vec<&str>,
    ) -> Self {
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
                    .with_columns(columns)
                    .with_partition_columns(partition_columns)
                    .with_table_name("axon_fixture")
                    .with_configuration(configuration)
                    .await
                    .expect("table should be created");

                drop(table);
            });

        Self {
            _tempdir: tempdir,
            table_uri,
        }
    }

    fn data_file_paths(&self) -> Vec<PathBuf> {
        let mut paths = Vec::new();
        collect_matching_paths(self._tempdir.path(), &mut paths, |path| {
            path.extension()
                .is_some_and(|extension| extension == "parquet")
        });
        paths.sort();
        paths
    }
}

fn default_table_columns() -> Vec<StructField> {
    vec![
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
    ]
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

fn partitioned_query_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/conformance/native-runtime-partitioned-sql-corpus.json")
}

fn load_partitioned_query_corpus() -> Vec<PartitionedQueryCorpusCase> {
    serde_json::from_str(
        &std::fs::read_to_string(partitioned_query_corpus_path())
            .expect("partitioned query corpus should be readable"),
    )
    .expect("partitioned query corpus should deserialize")
}

fn collect_matching_paths<F>(root: &Path, matches: &mut Vec<PathBuf>, predicate: F)
where
    F: Fn(&Path) -> bool + Copy,
{
    for entry in fs::read_dir(root).expect("directory should be readable") {
        let entry = entry.expect("directory entry should load");
        let path = entry.path();
        if path.is_dir() {
            collect_matching_paths(&path, matches, predicate);
        } else if predicate(&path) {
            matches.push(path);
        }
    }
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
fn bootstrap_table_rejects_change_data_feed_tables() {
    let fixture = TestTableFixture::create_with_columns_and_configuration(
        default_table_columns(),
        vec![(
            TableProperty::EnableChangeDataFeed.as_ref().to_string(),
            Some("true".to_string()),
        )],
    );

    let error = bootstrap_table(&fixture.table_uri)
        .expect_err("change data feed tables should be rejected in Sprint 1");

    assert_eq!(error.code, QueryErrorCode::UnsupportedFeature);
    assert!(
        error.message.contains("ChangeDataFeed"),
        "error should identify the rejected capability: {}",
        error.message
    );
}

#[test]
fn bootstrap_table_rejects_timestamp_ntz_tables() {
    let fixture = TestTableFixture::create_with_columns_and_configuration(
        vec![
            StructField::new(
                "id".to_string(),
                DataType::Primitive(PrimitiveType::Integer),
                false,
            ),
            StructField::new(
                "event_time".to_string(),
                DataType::Primitive(PrimitiveType::TimestampNtz),
                true,
            ),
        ],
        vec![],
    );

    let error = bootstrap_table(&fixture.table_uri)
        .expect_err("timestamp_ntz tables should be rejected in Sprint 1");

    assert_eq!(error.code, QueryErrorCode::UnsupportedFeature);
    assert!(
        error.message.contains("TimestampNtz"),
        "error should identify the rejected capability: {}",
        error.message
    );
}

#[test]
fn bootstrap_table_rejects_column_mapping_tables() {
    let fixture = TestTableFixture::create_with_columns_and_configuration(
        vec![
            (StructField::new(
                "id".to_string(),
                DataType::Primitive(PrimitiveType::Integer),
                false,
            )
            .with_metadata([
                ("delta.columnMapping.id", MetadataValue::Number(1.into())),
                (
                    "delta.columnMapping.physicalName",
                    MetadataValue::String("col-physical-id".to_string()),
                ),
            ])),
            (StructField::new(
                "category".to_string(),
                DataType::Primitive(PrimitiveType::String),
                false,
            )
            .with_metadata([
                ("delta.columnMapping.id", MetadataValue::Number(2.into())),
                (
                    "delta.columnMapping.physicalName",
                    MetadataValue::String("col-physical-category".to_string()),
                ),
            ])),
        ],
        vec![
            (
                TableProperty::ColumnMappingMode.as_ref().to_string(),
                Some("name".to_string()),
            ),
            (
                TableProperty::MinReaderVersion.as_ref().to_string(),
                Some("2".to_string()),
            ),
            (
                TableProperty::MinWriterVersion.as_ref().to_string(),
                Some("5".to_string()),
            ),
        ],
    );

    let error = bootstrap_table(&fixture.table_uri)
        .expect_err("column mapping tables should be rejected in Sprint 2");

    assert_eq!(error.code, QueryErrorCode::UnsupportedFeature);
    assert!(
        error.message.contains("ColumnMapping"),
        "error should identify the rejected capability: {}",
        error.message
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
fn execute_query_reports_file_pruning_metrics_for_unpartitioned_filters() {
    let fixture = TestTableFixture::create();
    let request = QueryRequest::new(
        &fixture.table_uri,
        format!("SELECT id FROM {DEFAULT_TABLE_NAME} WHERE value >= 50 ORDER BY id"),
        ExecutionTarget::Native,
    );

    let result = execute_query(request).expect("query should execute");

    assert_eq!(
        pretty_format_batches(&result.batches)
            .expect("batches should format")
            .to_string(),
        "+----+\n| id |\n+----+\n| 5  |\n| 6  |\n+----+"
    );
    assert_eq!(
        result.metrics.files_touched, 1,
        "execution metrics should report only scanned files"
    );
    assert_eq!(
        result.metrics.files_skipped, 1,
        "execution metrics should report pruned files"
    );
    assert!(
        result.metrics.bytes_fetched > 0,
        "execution metrics should report scanned bytes"
    );
}

#[test]
fn execute_query_runs_the_partitioned_sql_corpus_with_pruning_metrics() {
    let fixture = TestTableFixture::create_partitioned();
    let corpus = load_partitioned_query_corpus();

    assert!(
        corpus.len() >= 5,
        "partitioned corpus should contain the Sprint 2 query cases"
    );

    for case in corpus {
        let request = QueryRequest::new(&fixture.table_uri, case.sql, ExecutionTarget::Native);
        let result = execute_query(request).unwrap_or_else(|error| {
            panic!(
                "partitioned query case '{}' should execute successfully: {error:?}",
                case.name
            )
        });

        assert_eq!(
            pretty_format_batches(&result.batches)
                .expect("batches should format")
                .to_string(),
            case.expected_pretty,
            "partitioned query case '{}' should match the golden result",
            case.name
        );
        assert_eq!(
            result.metrics.files_touched, case.expected_files_touched,
            "partitioned query case '{}' should report scanned files",
            case.name
        );
        assert_eq!(
            result.metrics.files_skipped, case.expected_files_skipped,
            "partitioned query case '{}' should report skipped files",
            case.name
        );
    }
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
fn execute_query_maps_missing_data_files_to_object_store_protocol_errors() {
    let fixture = TestTableFixture::create();
    let mut data_files = fixture.data_file_paths();
    let missing_file = data_files
        .pop()
        .expect("fixture should have at least one parquet data file");
    fs::remove_file(&missing_file).expect("data file should be removable");

    let request = QueryRequest::new(
        &fixture.table_uri,
        format!("SELECT id FROM {DEFAULT_TABLE_NAME} ORDER BY id"),
        ExecutionTarget::Native,
    );

    let error = execute_query(request).expect_err("query should fail when data files are missing");

    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
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
        "query smoke should report execution-derived byte metrics"
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
