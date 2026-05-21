#![cfg(not(target_arch = "wasm32"))]

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

#[tokio::test]
async fn browser_datafusion_runtime_matches_sql_corpus() {
    let corpus = load_corpus();

    for case in corpus {
        let mut engine = wasm_datafusion_poc::WasmDataFusionEngine::new();
        let batch = corpus_table();
        engine
            .register_record_batches("t", batch.schema(), vec![batch])
            .await
            .unwrap_or_else(|error| panic!("{}: table registration failed: {error:?}", case.name));

        let (schema, batches) = engine
            .sql_to_record_batches(&case.sql)
            .await
            .unwrap_or_else(|error| panic!("{}: SQL query failed: {error:?}", case.name));

        let actual_columns = schema
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            actual_columns, case.expected_columns,
            "{}: output columns differed",
            case.name
        );

        let actual_rows = normalize_batches(&batches);
        assert_eq!(
            actual_rows, case.expected_rows,
            "{}: output rows differed",
            case.name
        );
    }
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
