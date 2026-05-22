#![cfg(not(target_arch = "wasm32"))]

use std::{collections::BTreeMap, sync::Arc};

use arrow_array::{
    cast::AsArray, Array, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use wasm_datafusion_poc::{DeltaActiveFile, DeltaTableDescriptor, WasmDataFusionEngine};

#[tokio::test]
async fn browser_datafusion_runtime_matches_sql_corpus() {
    let corpus = load_corpus();
    let execution_paths = corpus_execution_paths();
    assert!(
        execution_paths
            .iter()
            .any(|path| path.uses_axon_scan_exec()),
        "runtime SQL corpus should include a descriptor-backed AxonParquetScanExec path"
    );

    for case in corpus {
        for path in execution_paths {
            execute_corpus_case(path, &case).await;
        }
    }
}

#[derive(Clone, Copy)]
enum CorpusExecutionPath {
    MemTable,
    DescriptorBackedAxonScan,
}

impl CorpusExecutionPath {
    fn name(self) -> &'static str {
        match self {
            Self::MemTable => "MemTable",
            Self::DescriptorBackedAxonScan => "descriptor-backed AxonParquetScanExec",
        }
    }

    fn uses_axon_scan_exec(self) -> bool {
        matches!(self, Self::DescriptorBackedAxonScan)
    }
}

fn corpus_execution_paths() -> [CorpusExecutionPath; 2] {
    [
        CorpusExecutionPath::MemTable,
        CorpusExecutionPath::DescriptorBackedAxonScan,
    ]
}

async fn execute_corpus_case(path: CorpusExecutionPath, case: &CorpusCase) {
    let mut engine = WasmDataFusionEngine::new();
    register_corpus_table(path, &mut engine, corpus_table()).await;

    if path.uses_axon_scan_exec() {
        let physical_plan = explain_physical_plan_text(&engine, &case.sql).await;
        assert!(
            physical_plan.contains("AxonParquetScanExec"),
            "{} via {}: expected descriptor-backed AxonParquetScanExec path:\n{physical_plan}",
            case.name,
            path.name()
        );
    }

    let (schema, batches) = engine
        .sql_to_record_batches(&case.sql)
        .await
        .unwrap_or_else(|error| {
            panic!(
                "{} via {}: SQL query failed: {error:?}",
                case.name,
                path.name()
            )
        });

    let actual_columns = schema
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        actual_columns,
        case.expected_columns,
        "{} via {}: output columns differed",
        case.name,
        path.name()
    );

    let actual_rows = normalize_batches(&batches);
    assert_eq!(
        actual_rows,
        case.expected_rows,
        "{} via {}: output rows differed",
        case.name,
        path.name()
    );
}

async fn register_corpus_table(
    path: CorpusExecutionPath,
    engine: &mut WasmDataFusionEngine,
    batch: RecordBatch,
) {
    match path {
        CorpusExecutionPath::MemTable => engine
            .register_record_batches("t", batch.schema(), vec![batch])
            .await
            .unwrap_or_else(|error| panic!("{} registration failed: {error:?}", path.name())),
        CorpusExecutionPath::DescriptorBackedAxonScan => engine
            .open_delta_table_with_record_batch_partitions(
                corpus_delta_descriptor(batch.schema()),
                vec![vec![batch]],
            )
            .await
            .unwrap_or_else(|error| panic!("{} registration failed: {error:?}", path.name())),
    }
}

async fn explain_physical_plan_text(engine: &WasmDataFusionEngine, sql: &str) -> String {
    let (_schema, batches) = engine
        .sql_to_record_batches(&format!("EXPLAIN {sql}"))
        .await
        .expect("EXPLAIN should run through DataFusion");
    let mut lines = Vec::new();

    for batch in batches {
        for row_index in 0..batch.num_rows() {
            for column_index in 0..batch.num_columns() {
                match batch.schema().field(column_index).data_type() {
                    DataType::Utf8 => lines.push(
                        batch
                            .column(column_index)
                            .as_string::<i32>()
                            .value(row_index)
                            .to_string(),
                    ),
                    DataType::LargeUtf8 => lines.push(
                        batch
                            .column(column_index)
                            .as_string::<i64>()
                            .value(row_index)
                            .to_string(),
                    ),
                    _ => {}
                }
            }
        }
    }

    lines
        .join("\n")
        .split_once("physical_plan\n")
        .map(|(_header, physical_plan)| physical_plan.to_string())
        .expect("EXPLAIN output should include a physical plan")
}

#[derive(Debug)]
struct CorpusCase {
    name: String,
    sql: String,
    expected_columns: Vec<String>,
    expected_rows: Vec<Vec<Scalar>>,
}

#[derive(Debug, PartialEq)]
enum Scalar {
    Int(i64),
    Utf8(String),
    Bool(bool),
    Null,
}

fn load_corpus() -> Vec<CorpusCase> {
    let corpus: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/conformance/browser-datafusion-runtime-corpus.json"
    ))
    .expect("browser DataFusion runtime corpus should parse as JSON");

    corpus
        .as_array()
        .expect("browser DataFusion runtime corpus should be a JSON array")
        .iter()
        .map(parse_case)
        .collect()
}

fn parse_case(value: &serde_json::Value) -> CorpusCase {
    CorpusCase {
        name: string_field(value, "name"),
        sql: string_field(value, "sql"),
        expected_columns: string_array_field(value, "expected_columns"),
        expected_rows: rows_field(value, "expected_rows"),
    }
}

fn string_field(value: &serde_json::Value, field: &str) -> String {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| panic!("corpus case field {field} should be a string"))
        .to_string()
}

fn string_array_field(value: &serde_json::Value, field: &str) -> Vec<String> {
    value
        .get(field)
        .and_then(serde_json::Value::as_array)
        .unwrap_or_else(|| panic!("corpus case field {field} should be an array"))
        .iter()
        .map(|item| {
            item.as_str()
                .unwrap_or_else(|| panic!("corpus case field {field} should contain strings"))
                .to_string()
        })
        .collect()
}

fn rows_field(value: &serde_json::Value, field: &str) -> Vec<Vec<Scalar>> {
    value
        .get(field)
        .and_then(serde_json::Value::as_array)
        .unwrap_or_else(|| panic!("corpus case field {field} should be an array"))
        .iter()
        .map(|row| {
            row.as_array()
                .unwrap_or_else(|| panic!("corpus case field {field} should contain row arrays"))
                .iter()
                .map(parse_scalar)
                .collect()
        })
        .collect()
}

fn parse_scalar(value: &serde_json::Value) -> Scalar {
    if value.is_null() {
        Scalar::Null
    } else if let Some(number) = value.as_i64() {
        Scalar::Int(number)
    } else if let Some(text) = value.as_str() {
        Scalar::Utf8(text.to_string())
    } else if let Some(boolean) = value.as_bool() {
        Scalar::Bool(boolean)
    } else {
        panic!("unsupported expected scalar in corpus: {value}")
    }
}

fn corpus_table() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("flag", DataType::Boolean, true),
        Field::new("optional_count", DataType::Int64, true),
        Field::new("note", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int32Array::from(vec![5, 12, 25, 20])),
            Arc::new(StringArray::from(vec!["A", "B", "B", "C"])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
            ])),
            Arc::new(Int64Array::from(vec![
                Some(100_i64),
                None,
                Some(300_i64),
                Some(400_i64),
            ])),
            Arc::new(StringArray::from(vec![
                Some("alpha"),
                None,
                Some("bravo"),
                Some("charlie"),
            ])),
        ],
    )
    .expect("corpus table should construct")
}

fn corpus_delta_descriptor(schema: SchemaRef) -> DeltaTableDescriptor {
    DeltaTableDescriptor {
        table_name: "t".to_string(),
        table_version: 42,
        schema,
        partition_columns: Vec::new(),
        partition_column_types: BTreeMap::new(),
        active_files: vec![DeltaActiveFile {
            path: "part-000.parquet".to_string(),
            url: "https://example.test/corpus/part-000.parquet".to_string(),
            size_bytes: 4096,
            partition_values: BTreeMap::new(),
            object_etag: Some("\"corpus-etag\"".to_string()),
            stats_json: Some(r#"{"numRecords":4}"#.to_string()),
            deletion_vector: None,
        }],
    }
}

fn normalize_batches(batches: &[RecordBatch]) -> Vec<Vec<Scalar>> {
    batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row_index| {
                batch
                    .columns()
                    .iter()
                    .map(|column| normalize_scalar(column.as_ref(), row_index))
                    .collect()
            })
        })
        .collect()
}

fn normalize_scalar(column: &dyn Array, row_index: usize) -> Scalar {
    if column.is_null(row_index) {
        return Scalar::Null;
    }

    match column.data_type() {
        DataType::Int32 => Scalar::Int(i64::from(
            column
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32 column should downcast")
                .value(row_index),
        )),
        DataType::Int64 => Scalar::Int(
            column
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 column should downcast")
                .value(row_index),
        ),
        DataType::Utf8 => Scalar::Utf8(
            column
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Utf8 column should downcast")
                .value(row_index)
                .to_string(),
        ),
        DataType::Boolean => Scalar::Bool(
            column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("Boolean column should downcast")
                .value(row_index),
        ),
        DataType::Null => Scalar::Null,
        other => panic!("unsupported Arrow type in browser DataFusion SQL corpus: {other:?}"),
    }
}
