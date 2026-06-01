#![cfg(not(target_arch = "wasm32"))]

use std::{collections::BTreeMap, sync::Arc};

use arrow_array::{
    cast::AsArray, Array, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use query_contract::PartitionColumnType;
use wasm_datafusion_poc::{DeltaActiveFile, DeltaTableDescriptor, WasmDataFusionEngine};

#[tokio::test]
async fn browser_datafusion_matches_daxis_query_shape_corpus() {
    let corpus = load_corpus();
    let execution_paths = corpus_execution_paths();
    assert!(
        execution_paths
            .iter()
            .any(|path| path.uses_axon_scan_exec()),
        "Daxis query corpus should include the descriptor-backed AxonParquetScanExec path"
    );

    for case in corpus {
        for path in execution_paths {
            execute_daxis_case(path, &case).await;
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

async fn execute_daxis_case(path: CorpusExecutionPath, case: &CorpusCase) {
    let mut engine = WasmDataFusionEngine::new();
    register_orders_table(path, &mut engine, orders_table()).await;

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

async fn register_orders_table(
    path: CorpusExecutionPath,
    engine: &mut WasmDataFusionEngine,
    batch: RecordBatch,
) {
    match path {
        CorpusExecutionPath::MemTable => engine
            .register_record_batches("orders", batch.schema(), vec![batch])
            .await
            .unwrap_or_else(|error| panic!("{} registration failed: {error:?}", path.name())),
        CorpusExecutionPath::DescriptorBackedAxonScan => engine
            .open_delta_table_with_record_batch_partitions(
                orders_delta_descriptor(batch.schema()),
                orders_partitions(batch.schema()),
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
        "../../../tests/conformance/daxis-browser-datafusion-query-corpus.json"
    ))
    .expect("Daxis browser DataFusion query corpus should parse as JSON");

    corpus
        .as_array()
        .expect("Daxis browser DataFusion query corpus should be a JSON array")
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
        panic!("unsupported expected scalar in Daxis corpus: {value}")
    }
}

fn orders_table() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("order_date", DataType::Utf8, false),
        Field::new("customer_tier", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("amount_cents", DataType::Int64, false),
        Field::new("discount_cents", DataType::Int32, true),
        Field::new("is_priority", DataType::Boolean, true),
    ]));
    orders_record_batch(
        schema,
        &[1001, 1002, 1003, 1004, 1005],
        &[
            "2026-05-30",
            "2026-05-30",
            "2026-05-29",
            "2026-05-30",
            "2026-05-28",
        ],
        &["gold", "silver", "gold", "bronze", "gold"],
        &["shipped", "pending", "shipped", "cancelled", "pending"],
        &[12_500, 8_750, 22_000, 4_000, 15_000],
        &[Some(500), None, Some(1_000), Some(0), Some(750)],
        &[Some(true), Some(false), Some(true), None, Some(false)],
    )
}

fn orders_partitions(schema: SchemaRef) -> Vec<Vec<RecordBatch>> {
    vec![
        vec![orders_record_batch(
            Arc::clone(&schema),
            &[1001, 1002, 1004],
            &["2026-05-30", "2026-05-30", "2026-05-30"],
            &["gold", "silver", "bronze"],
            &["shipped", "pending", "cancelled"],
            &[12_500, 8_750, 4_000],
            &[Some(500), None, Some(0)],
            &[Some(true), Some(false), None],
        )],
        vec![orders_record_batch(
            Arc::clone(&schema),
            &[1003],
            &["2026-05-29"],
            &["gold"],
            &["shipped"],
            &[22_000],
            &[Some(1_000)],
            &[Some(true)],
        )],
        vec![orders_record_batch(
            schema,
            &[1005],
            &["2026-05-28"],
            &["gold"],
            &["pending"],
            &[15_000],
            &[Some(750)],
            &[Some(false)],
        )],
    ]
}

#[allow(clippy::too_many_arguments)]
fn orders_record_batch(
    schema: SchemaRef,
    order_ids: &[i64],
    order_dates: &[&str],
    customer_tiers: &[&str],
    statuses: &[&str],
    amount_cents: &[i64],
    discount_cents: &[Option<i32>],
    is_priority: &[Option<bool>],
) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(order_ids.to_vec())),
            Arc::new(StringArray::from(order_dates.to_vec())),
            Arc::new(StringArray::from(customer_tiers.to_vec())),
            Arc::new(StringArray::from(statuses.to_vec())),
            Arc::new(Int64Array::from(amount_cents.to_vec())),
            Arc::new(Int32Array::from(discount_cents.to_vec())),
            Arc::new(BooleanArray::from(is_priority.to_vec())),
        ],
    )
    .expect("orders corpus table should construct")
}

fn orders_delta_descriptor(schema: SchemaRef) -> DeltaTableDescriptor {
    DeltaTableDescriptor {
        table_name: "orders".to_string(),
        table_version: 42,
        schema,
        partition_columns: vec!["order_date".to_string()],
        partition_column_types: BTreeMap::from([(
            "order_date".to_string(),
            PartitionColumnType::String,
        )]),
        active_files: vec![
            DeltaActiveFile {
                path: "order_date=2026-05-30/part-00000.parquet".to_string(),
                url: "https://example.test/daxis/orders/order_date=2026-05-30/part-00000.parquet"
                    .to_string(),
                size_bytes: 4096,
                partition_values: BTreeMap::from([(
                    "order_date".to_string(),
                    Some("2026-05-30".to_string()),
                )]),
                object_etag: Some("\"daxis-orders-2026-05-30\"".to_string()),
                stats_json: Some(r#"{"numRecords":3}"#.to_string()),
                deletion_vector: None,
            },
            DeltaActiveFile {
                path: "order_date=2026-05-29/part-00000.parquet".to_string(),
                url: "https://example.test/daxis/orders/order_date=2026-05-29/part-00000.parquet"
                    .to_string(),
                size_bytes: 2048,
                partition_values: BTreeMap::from([(
                    "order_date".to_string(),
                    Some("2026-05-29".to_string()),
                )]),
                object_etag: Some("\"daxis-orders-2026-05-29\"".to_string()),
                stats_json: Some(r#"{"numRecords":1}"#.to_string()),
                deletion_vector: None,
            },
            DeltaActiveFile {
                path: "order_date=2026-05-28/part-00000.parquet".to_string(),
                url: "https://example.test/daxis/orders/order_date=2026-05-28/part-00000.parquet"
                    .to_string(),
                size_bytes: 2048,
                partition_values: BTreeMap::from([(
                    "order_date".to_string(),
                    Some("2026-05-28".to_string()),
                )]),
                object_etag: Some("\"daxis-orders-2026-05-28\"".to_string()),
                stats_json: Some(r#"{"numRecords":1}"#.to_string()),
                deletion_vector: None,
            },
        ],
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
        other => panic!("unsupported Arrow type in Daxis browser DataFusion corpus: {other:?}"),
    }
}
