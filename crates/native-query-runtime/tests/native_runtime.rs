use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use deltalake::arrow::array::{Int32Array, StringArray};
use deltalake::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::arrow::util::pretty::pretty_format_batches;
use deltalake::kernel::{Action, DataType, MetadataValue, PrimitiveType, Protocol, StructField};
use deltalake::{DeltaTable, TableProperty};
use native_query_runtime::{bootstrap_table, execute_query, DEFAULT_TABLE_NAME};
use query_contract::{
    CapabilityKey, CapabilityState, ExecutionTarget, QueryErrorCode, QueryExecutionOptions,
    QueryRequest,
};
use serde::Deserialize;
use serde_json::json;
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
    #[serde(default)]
    expected_files_touched: Option<u64>,
    #[serde(default)]
    expected_files_skipped: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct PartitionedQueryCorpusCase {
    name: String,
    sql: String,
    expected_pretty: String,
    #[serde(default)]
    expected_files_touched: Option<u64>,
    #[serde(default)]
    expected_files_skipped: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct SnapshotVersionQueryCorpusCase {
    name: String,
    snapshot_version: i64,
    sql: String,
    expected_pretty: String,
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

    fn create_with_columns_configuration_partitions_and_actions(
        columns: Vec<StructField>,
        configuration: Vec<(String, Option<String>)>,
        partition_columns: Vec<&str>,
        actions: Vec<Action>,
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

                table
                    .create()
                    .with_columns(columns)
                    .with_partition_columns(partition_columns)
                    .with_table_name("axon_fixture")
                    .with_configuration(configuration)
                    .with_actions(actions)
                    .await
                    .expect("table should be created");
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

fn snapshot_version_query_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/conformance/native-runtime-snapshot-version-sql-corpus.json")
}

fn load_snapshot_version_query_corpus() -> Vec<SnapshotVersionQueryCorpusCase> {
    serde_json::from_str(
        &std::fs::read_to_string(snapshot_version_query_corpus_path())
            .expect("snapshot-version query corpus should be readable"),
    )
    .expect("snapshot-version query corpus should deserialize")
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
fn bootstrap_table_rejects_deletion_vector_enabled_tables_without_dv_add_actions() {
    let fixture = TestTableFixture::create_with_columns_configuration_partitions_and_actions(
        default_table_columns(),
        vec![(
            TableProperty::EnableDeletionVectors.as_ref().to_string(),
            Some("true".to_string()),
        )],
        vec![],
        vec![deletion_vector_protocol_action()],
    );

    let error = bootstrap_table(&fixture.table_uri)
        .expect_err("deletion-vector-enabled tables should be rejected without scanning adds");

    assert_eq!(error.code, QueryErrorCode::UnsupportedFeature);
    assert!(
        error.message.contains("DeletionVectors"),
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
        corpus.len() >= 12,
        "corpus should contain the Sprint 4 latest-snapshot query cases"
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
        if let Some(expected_files_touched) = case.expected_files_touched {
            assert_eq!(
                result.metrics.files_touched, expected_files_touched,
                "query case '{}' should report the expected scanned file count",
                case.name
            );
        }
        if let Some(expected_files_skipped) = case.expected_files_skipped {
            assert_eq!(
                result.metrics.files_skipped, expected_files_skipped,
                "query case '{}' should report the expected skipped file count",
                case.name
            );
        }
        saw_metrics |= result.metrics.bytes_fetched > 0 && result.metrics.duration_ms > 0;
    }

    assert!(
        saw_metrics,
        "at least one corpus case should report populated metrics"
    );
}

#[test]
fn execute_query_reports_the_full_capability_matrix_for_supported_tables() {
    let fixture = TestTableFixture::create();
    let request = QueryRequest::new(
        &fixture.table_uri,
        format!("SELECT id FROM {DEFAULT_TABLE_NAME} ORDER BY id LIMIT 1"),
        ExecutionTarget::Native,
    );

    let result = execute_query(request).expect("query should execute");

    assert_eq!(
        result.capabilities.state(CapabilityKey::ChangeDataFeed),
        Some(CapabilityState::Unsupported)
    );
    assert_eq!(
        result.capabilities.state(CapabilityKey::ColumnMapping),
        Some(CapabilityState::Unsupported)
    );
    assert_eq!(
        result.capabilities.state(CapabilityKey::DeletionVectors),
        Some(CapabilityState::Unsupported)
    );
    assert_eq!(
        result
            .capabilities
            .state(CapabilityKey::MultiPartitionExecution),
        Some(CapabilityState::Supported)
    );
    assert_eq!(
        result.capabilities.state(CapabilityKey::ProxyAccess),
        Some(CapabilityState::Unsupported)
    );
    assert_eq!(
        result.capabilities.state(CapabilityKey::RangeReads),
        Some(CapabilityState::Supported)
    );
    assert_eq!(
        result.capabilities.state(CapabilityKey::SignedUrlAccess),
        Some(CapabilityState::Unsupported)
    );
    assert_eq!(
        result.capabilities.state(CapabilityKey::TimeTravel),
        Some(CapabilityState::Supported)
    );
    assert_eq!(
        result.capabilities.state(CapabilityKey::TimestampNtz),
        Some(CapabilityState::Unsupported)
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
        corpus.len() >= 10,
        "partitioned corpus should contain the Sprint 4 partitioned query cases"
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
        if let Some(expected_files_touched) = case.expected_files_touched {
            assert_eq!(
                result.metrics.files_touched, expected_files_touched,
                "partitioned query case '{}' should report scanned files",
                case.name
            );
        }
        if let Some(expected_files_skipped) = case.expected_files_skipped {
            assert_eq!(
                result.metrics.files_skipped, expected_files_skipped,
                "partitioned query case '{}' should report skipped files",
                case.name
            );
        }
    }
}

#[test]
fn execute_query_runs_the_snapshot_version_sql_corpus_with_golden_results() {
    let fixture = TestTableFixture::create();
    let corpus = load_snapshot_version_query_corpus();

    assert!(
        corpus.len() >= 4,
        "snapshot-version corpus should contain the Sprint 4 historical query cases"
    );

    for case in corpus {
        let request = QueryRequest {
            snapshot_version: Some(case.snapshot_version),
            ..QueryRequest::new(&fixture.table_uri, case.sql, ExecutionTarget::Native)
        };
        let result = execute_query(request).unwrap_or_else(|error| {
            panic!(
                "snapshot-version query case '{}' should execute successfully: {error:?}",
                case.name
            )
        });

        assert_eq!(
            pretty_format_batches(&result.batches)
                .expect("batches should format")
                .to_string(),
            case.expected_pretty,
            "snapshot-version query case '{}' should match the golden result",
            case.name
        );
        assert_eq!(
            result.capabilities.state(CapabilityKey::TimeTravel),
            Some(CapabilityState::Supported),
            "snapshot-version query case '{}' should report native time-travel support",
            case.name
        );
    }
}

#[test]
fn execute_query_supports_explicit_snapshot_versions() {
    let fixture = TestTableFixture::create();

    let latest_request = QueryRequest {
        snapshot_version: None,
        ..QueryRequest::new(
            &fixture.table_uri,
            format!("SELECT COUNT(*) AS row_count FROM {DEFAULT_TABLE_NAME}"),
            ExecutionTarget::Native,
        )
    };
    let historical_request = QueryRequest {
        snapshot_version: Some(1),
        ..QueryRequest::new(
            &fixture.table_uri,
            format!("SELECT COUNT(*) AS row_count FROM {DEFAULT_TABLE_NAME}"),
            ExecutionTarget::Native,
        )
    };

    let latest_result = execute_query(latest_request).expect("latest query should execute");
    let historical_result =
        execute_query(historical_request).expect("historical query should execute");

    assert_eq!(
        pretty_format_batches(&latest_result.batches)
            .expect("batches should format")
            .to_string(),
        "+-----------+\n| row_count |\n+-----------+\n| 6         |\n+-----------+"
    );
    assert_eq!(
        pretty_format_batches(&historical_result.batches)
            .expect("batches should format")
            .to_string(),
        "+-----------+\n| row_count |\n+-----------+\n| 3         |\n+-----------+"
    );
    assert_eq!(
        historical_result
            .capabilities
            .state(CapabilityKey::TimeTravel),
        Some(CapabilityState::Supported),
        "historical query should report native time-travel support"
    );
}

#[test]
fn execute_query_rejects_negative_snapshot_versions() {
    let fixture = TestTableFixture::create();
    let request = QueryRequest {
        snapshot_version: Some(-1),
        ..QueryRequest::new(
            &fixture.table_uri,
            format!("SELECT COUNT(*) AS row_count FROM {DEFAULT_TABLE_NAME}"),
            ExecutionTarget::Native,
        )
    };

    let error = execute_query(request).expect_err("negative versions should be rejected");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(
        error.message.contains("snapshot_version"),
        "error should identify the rejected field: {}",
        error.message
    );
}

#[test]
fn execute_query_rejects_unknown_snapshot_versions() {
    let fixture = TestTableFixture::create();
    let request = QueryRequest {
        snapshot_version: Some(99),
        ..QueryRequest::new(
            &fixture.table_uri,
            format!("SELECT COUNT(*) AS row_count FROM {DEFAULT_TABLE_NAME}"),
            ExecutionTarget::Native,
        )
    };

    let error = execute_query(request).expect_err("unknown versions should fail deterministically");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(
        error.message.contains("snapshot"),
        "error should describe the unavailable snapshot: {}",
        error.message
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

#[cfg(unix)]
#[test]
fn execute_query_maps_permission_denied_files_to_access_denied() {
    use std::os::unix::fs::PermissionsExt;

    let fixture = TestTableFixture::create();
    let data_files = fixture.data_file_paths();
    let denied_file = data_files
        .first()
        .expect("fixture should have at least one parquet data file");
    let original_permissions = fs::metadata(denied_file)
        .expect("metadata should load")
        .permissions();

    let mut denied_permissions = original_permissions.clone();
    denied_permissions.set_mode(0o000);
    fs::set_permissions(denied_file, denied_permissions).expect("permissions should update");

    let request = QueryRequest::new(
        &fixture.table_uri,
        format!("SELECT id FROM {DEFAULT_TABLE_NAME} ORDER BY id"),
        ExecutionTarget::Native,
    );

    let error = execute_query(request).expect_err("permission denied should surface");

    fs::set_permissions(denied_file, original_permissions).expect("permissions should be restored");

    assert_eq!(error.code, QueryErrorCode::AccessDenied);
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

#[test]
fn bootstrap_table_rejects_env_gated_forbidden_gcs_smoke() {
    let Ok(table_uri) = std::env::var("AXON_GCS_TEST_FORBIDDEN_TABLE_URI") else {
        return;
    };

    let error = bootstrap_table(&table_uri).expect_err("forbidden gcs bootstrap should fail");

    assert_eq!(error.code, QueryErrorCode::AccessDenied);
    assert_eq!(error.target, ExecutionTarget::Native);
}

#[test]
fn bootstrap_table_rejects_env_gated_not_found_gcs_smoke() {
    let Ok(table_uri) = std::env::var("AXON_GCS_TEST_NOT_FOUND_TABLE_URI") else {
        return;
    };

    let error = bootstrap_table(&table_uri).expect_err("missing gcs table should fail");

    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert_eq!(error.target, ExecutionTarget::Native);
}

#[test]
fn execute_query_rejects_env_gated_stale_history_gcs_smoke() {
    let Ok(table_uri) = std::env::var("AXON_GCS_TEST_STALE_HISTORY_TABLE_URI") else {
        return;
    };
    let Ok(snapshot_version) = std::env::var("AXON_GCS_TEST_STALE_HISTORY_SNAPSHOT_VERSION") else {
        return;
    };
    let snapshot_version: i64 = snapshot_version
        .parse()
        .expect("stale-history gcs snapshot version should parse");

    let request = QueryRequest {
        snapshot_version: Some(snapshot_version),
        ..QueryRequest::new(
            &table_uri,
            format!("SELECT COUNT(*) AS row_count FROM {DEFAULT_TABLE_NAME}"),
            ExecutionTarget::Native,
        )
    };

    let error = execute_query(request).expect_err("stale-history gcs query should fail");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert_eq!(error.target, ExecutionTarget::Native);
    assert!(
        error.message.contains("snapshot"),
        "stale-history error should describe the unavailable snapshot: {}",
        error.message
    );
}

#[test]
fn execute_query_rejects_env_gated_missing_object_gcs_smoke() {
    let Ok(table_uri) = std::env::var("AXON_GCS_TEST_MISSING_OBJECT_TABLE_URI") else {
        return;
    };

    let request = QueryRequest::new(
        &table_uri,
        format!("SELECT * FROM {DEFAULT_TABLE_NAME} LIMIT 1"),
        ExecutionTarget::Native,
    );

    let error = execute_query(request).expect_err("missing-object gcs query should fail");

    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert_eq!(error.target, ExecutionTarget::Native);
}

#[test]
fn execute_query_supports_env_gated_partitioned_gcs_pruning_smoke() {
    let Ok(table_uri) = std::env::var("AXON_GCS_TEST_PARTITIONED_TABLE_URI") else {
        return;
    };

    let request = QueryRequest::new(
        &table_uri,
        format!("SELECT COUNT(*) AS row_count FROM {DEFAULT_TABLE_NAME} WHERE category = 'C'"),
        ExecutionTarget::Native,
    );

    let result = execute_query(request).expect("partitioned gcs pruning query should succeed");
    let pretty = pretty_format_batches(&result.batches)
        .expect("batches should format")
        .to_string();

    assert!(
        pretty.contains("row_count"),
        "partitioned gcs pruning smoke should return an aggregate row"
    );
    assert!(
        result.metrics.files_touched > 0,
        "partitioned gcs pruning smoke should scan at least one file"
    );
    assert!(
        result.metrics.files_skipped > 0,
        "partitioned gcs pruning smoke should report skipped files"
    );
}

#[test]
fn execute_query_supports_env_gated_partitioned_gcs_snapshot_version_smoke() {
    let Ok(table_uri) = std::env::var("AXON_GCS_TEST_PARTITIONED_TABLE_URI") else {
        return;
    };
    let Ok(snapshot_version) = std::env::var("AXON_GCS_TEST_PARTITIONED_TABLE_SNAPSHOT_VERSION")
    else {
        return;
    };
    let snapshot_version: i64 = snapshot_version
        .parse()
        .expect("partitioned gcs snapshot version should parse");

    let latest_request = QueryRequest::new(
        &table_uri,
        format!("SELECT COUNT(*) AS row_count FROM {DEFAULT_TABLE_NAME}"),
        ExecutionTarget::Native,
    );
    let historical_request = QueryRequest {
        snapshot_version: Some(snapshot_version),
        ..QueryRequest::new(
            &table_uri,
            format!("SELECT COUNT(*) AS row_count FROM {DEFAULT_TABLE_NAME}"),
            ExecutionTarget::Native,
        )
    };

    let latest_result =
        execute_query(latest_request).expect("latest partitioned query should work");
    let historical_result =
        execute_query(historical_request).expect("historical partitioned query should work");

    let latest_pretty = pretty_format_batches(&latest_result.batches)
        .expect("batches should format")
        .to_string();
    let historical_pretty = pretty_format_batches(&historical_result.batches)
        .expect("batches should format")
        .to_string();

    assert_ne!(
        latest_pretty, historical_pretty,
        "historical partitioned gcs smoke should return a different aggregate than latest"
    );
    assert_eq!(
        historical_result
            .capabilities
            .state(CapabilityKey::TimeTravel),
        Some(CapabilityState::Supported),
        "historical partitioned gcs smoke should report native time-travel support"
    );
}
