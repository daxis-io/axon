//! Shared Axon engine UAT corpus machinery.
//!
//! Both the host runner (`axon_engine_uat_corpus.rs`) and the wasm32 runner
//! (`axon_engine_wasm_uat_corpus.rs`) include this module, so there is exactly
//! one definition of the corpus, the fixtures, and the comparison rules. The
//! two targets previously carried independent copies, which let the wasm slice
//! drift to four hand-written cases against a narrower schema while still being
//! reported as the same acceptance gate.
//!
//! Anything target-specific stays in the runner: the host uses `#[tokio::test]`,
//! wasm uses `#[wasm_bindgen_test]`. Everything below is identical for both.

#![allow(dead_code)]

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow_array::{
    cast::AsArray, Array, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use wasm_datafusion_poc::{DeltaActiveFile, DeltaTableDescriptor, WasmDataFusionEngine};

/// Every SQL class the corpus is required to exercise. Kept here so both
/// targets enforce the same floor.
pub const REQUIRED_SQL_CLASSES: [&str; 26] = [
    "projection",
    "filtering",
    "boolean_logic",
    "null_semantics",
    "arithmetic",
    "cast",
    "case_expression",
    "string_function",
    "aggregate_count",
    "aggregate_sum",
    "aggregate_min_max",
    "aggregate_avg",
    "grouped_aggregate",
    "having",
    "distinct",
    "ordering",
    "limit",
    "offset",
    "cte",
    "derived_table",
    "subquery",
    "inner_join",
    "left_join",
    "union_all",
    "window_function",
    "descriptor_backed_scan",
];

pub const MINIMUM_CORPUS_CASES: usize = 18;

#[derive(Clone, Copy)]
pub enum CorpusExecutionPath {
    MemTable,
    DescriptorBackedOrdersScan,
}

impl CorpusExecutionPath {
    pub fn name(self) -> &'static str {
        match self {
            Self::MemTable => "MemTable",
            Self::DescriptorBackedOrdersScan => "descriptor-backed orders AxonParquetScanExec",
        }
    }

    pub fn uses_axon_scan_exec(self) -> bool {
        matches!(self, Self::DescriptorBackedOrdersScan)
    }
}

pub fn corpus_execution_paths() -> [CorpusExecutionPath; 2] {
    [
        CorpusExecutionPath::MemTable,
        CorpusExecutionPath::DescriptorBackedOrdersScan,
    ]
}

#[derive(Debug)]
pub struct CorpusCase {
    pub name: String,
    pub sql: String,
    pub covered_sql_classes: BTreeSet<String>,
    pub expected_columns: Vec<String>,
    pub expected_rows: Vec<Vec<Scalar>>,
}

impl CorpusCase {
    pub fn covers(&self, sql_class: &str) -> bool {
        self.covered_sql_classes.contains(sql_class)
    }
}

#[derive(Debug, PartialEq)]
pub enum Scalar {
    Int(i64),
    Float(String),
    Utf8(String),
    Bool(bool),
    Null,
}

/// Runs one corpus case through one execution path and asserts the full result.
///
/// `target` names the runner ("host" / "actual wasm") so a failure message says
/// which target produced it.
pub async fn execute_uat_case(target: &str, path: CorpusExecutionPath, case: &CorpusCase) {
    let mut engine = WasmDataFusionEngine::new();
    register_uat_tables(target, path, &mut engine).await;

    if path.uses_axon_scan_exec() && case.covers("descriptor_backed_scan") {
        let physical_plan = explain_physical_plan_text(&engine, &case.sql).await;
        assert!(
            physical_plan.contains("AxonParquetScanExec"),
            "{} via {} ({target}): expected descriptor-backed AxonParquetScanExec path:\n{physical_plan}",
            case.name,
            path.name()
        );
    }

    let (schema, batches) = engine
        .sql_to_record_batches(&case.sql)
        .await
        .unwrap_or_else(|error| {
            panic!(
                "{} via {} ({target}): SQL query failed: {error:?}",
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
        "{} via {} ({target}): output columns differed",
        case.name,
        path.name()
    );

    let actual_rows = normalize_batches(&batches);
    assert_eq!(
        actual_rows,
        case.expected_rows,
        "{} via {} ({target}): output rows differed",
        case.name,
        path.name()
    );
}

pub async fn register_uat_tables(
    target: &str,
    path: CorpusExecutionPath,
    engine: &mut WasmDataFusionEngine,
) {
    let orders = orders_table();
    match path {
        CorpusExecutionPath::MemTable => engine
            .register_record_batches("orders", orders.schema(), vec![orders])
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "{} ({target}) orders registration failed: {error:?}",
                    path.name()
                )
            }),
        CorpusExecutionPath::DescriptorBackedOrdersScan => engine
            .open_delta_table_with_record_batch_partitions(
                orders_delta_descriptor(orders.schema()),
                orders_partitions(orders.schema()),
            )
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "{} ({target}) orders registration failed: {error:?}",
                    path.name()
                )
            }),
    }

    engine
        .register_record_batches(
            "customers",
            customers_table().schema(),
            vec![customers_table()],
        )
        .await
        .unwrap_or_else(|error| panic!("({target}) customers registration failed: {error:?}"));
    engine
        .register_record_batches(
            "shipments",
            shipments_table().schema(),
            vec![shipments_table()],
        )
        .await
        .unwrap_or_else(|error| panic!("({target}) shipments registration failed: {error:?}"));
}

pub async fn explain_physical_plan_text(engine: &WasmDataFusionEngine, sql: &str) -> String {
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

pub fn load_corpus() -> Vec<CorpusCase> {
    let corpus: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tests/conformance/axon-engine-query-uat-corpus.json"
    ))
    .expect("Axon engine UAT query corpus should parse as JSON");

    corpus
        .as_array()
        .expect("Axon engine UAT query corpus should be a JSON array")
        .iter()
        .map(parse_case)
        .collect()
}

fn parse_case(value: &serde_json::Value) -> CorpusCase {
    CorpusCase {
        name: string_field(value, "name"),
        sql: string_field(value, "sql"),
        covered_sql_classes: string_array_field(value, "covered_sql_classes")
            .into_iter()
            .collect(),
        expected_columns: string_array_field(value, "expected_columns"),
        expected_rows: rows_field(value, "expected_rows"),
    }
}

pub fn assert_suite_coverage(corpus: &[CorpusCase]) {
    assert!(
        corpus.len() >= MINIMUM_CORPUS_CASES,
        "Axon engine UAT corpus should cover at least {MINIMUM_CORPUS_CASES} query cases"
    );

    let covered = corpus
        .iter()
        .flat_map(|case| case.covered_sql_classes.iter().cloned())
        .collect::<BTreeSet<_>>();

    for required in REQUIRED_SQL_CLASSES {
        assert!(
            covered.contains(required),
            "Axon engine UAT corpus should cover SQL class '{required}'"
        );
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
    } else if let Some(number) = value.as_f64() {
        Scalar::Float(format_float(number))
    } else {
        panic!("unsupported expected scalar in UAT corpus: {value}")
    }
}

pub fn orders_table() -> RecordBatch {
    let schema = orders_schema();
    orders_record_batch(
        schema,
        &[1001, 1002, 1003, 1004, 1005, 1006],
        &[1, 2, 1, 3, 4, 5],
        &[
            "2026-05-30",
            "2026-05-30",
            "2026-05-29",
            "2026-05-30",
            "2026-05-28",
            "2026-05-31",
        ],
        &["gold", "silver", "gold", "bronze", "gold", "silver"],
        &[
            "shipped",
            "pending",
            "shipped",
            "cancelled",
            "pending",
            "shipped",
        ],
        &[12_500, 8_750, 22_000, 4_000, 15_000, 6_100],
        &[Some(500), None, Some(1_000), Some(0), Some(750), None],
        &[
            Some(true),
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
        ],
        &[
            Some("east"),
            Some("west"),
            Some("east"),
            Some("south"),
            Some("west"),
            None,
        ],
    )
}

/// How many scan partitions the descriptor-backed `orders` table is split into.
///
/// The host uses three, so the scan has to merge across inputs rather than
/// reading one batch that already holds every row.
///
/// wasm32 uses one, and this is a real engine limitation rather than a test
/// convenience: DataFusion merges multiple partitions via `JoinSet::spawn`
/// (datafusion-common-runtime-53.1.0/src/join_set.rs:66), which is
/// `tokio::spawn` and aborts on `wasm32-unknown-unknown` with
/// `RuntimeError: unreachable`. Multi-partition descriptor-backed scans are
/// therefore not currently executable in the browser engine. Every one of the
/// 18 corpus cases passes on wasm at a single partition, so this constant is
/// the *only* axis on which the two targets differ. Raise it to 2 to reproduce
/// the abort.
#[cfg(not(target_arch = "wasm32"))]
pub const ORDERS_SCAN_PARTITIONS: usize = 3;
#[cfg(target_arch = "wasm32")]
pub const ORDERS_SCAN_PARTITIONS: usize = 1;

pub fn orders_partitions(schema: SchemaRef) -> Vec<Vec<RecordBatch>> {
    if ORDERS_SCAN_PARTITIONS == 1 {
        return vec![vec![orders_table()]];
    }

    assert_eq!(
        ORDERS_SCAN_PARTITIONS, 3,
        "orders fixture defines a 1-partition and a 3-partition layout only"
    );

    vec![
        vec![orders_record_batch(
            Arc::clone(&schema),
            &[1001, 1003],
            &[1, 1],
            &["2026-05-30", "2026-05-29"],
            &["gold", "gold"],
            &["shipped", "shipped"],
            &[12_500, 22_000],
            &[Some(500), Some(1_000)],
            &[Some(true), Some(true)],
            &[Some("east"), Some("east")],
        )],
        vec![orders_record_batch(
            Arc::clone(&schema),
            &[1002, 1005],
            &[2, 4],
            &["2026-05-30", "2026-05-28"],
            &["silver", "gold"],
            &["pending", "pending"],
            &[8_750, 15_000],
            &[None, Some(750)],
            &[Some(false), Some(false)],
            &[Some("west"), Some("west")],
        )],
        vec![orders_record_batch(
            schema,
            &[1004, 1006],
            &[3, 5],
            &["2026-05-30", "2026-05-31"],
            &["bronze", "silver"],
            &["cancelled", "shipped"],
            &[4_000, 6_100],
            &[Some(0), None],
            &[None, Some(true)],
            &[Some("south"), None],
        )],
    ]
}

#[allow(clippy::too_many_arguments)]
fn orders_record_batch(
    schema: SchemaRef,
    order_ids: &[i64],
    customer_ids: &[i64],
    order_dates: &[&str],
    customer_tiers: &[&str],
    statuses: &[&str],
    amount_cents: &[i64],
    discount_cents: &[Option<i64>],
    is_priority: &[Option<bool>],
    regions: &[Option<&str>],
) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(order_ids.to_vec())),
            Arc::new(Int64Array::from(customer_ids.to_vec())),
            Arc::new(StringArray::from(order_dates.to_vec())),
            Arc::new(StringArray::from(customer_tiers.to_vec())),
            Arc::new(StringArray::from(statuses.to_vec())),
            Arc::new(Int64Array::from(amount_cents.to_vec())),
            Arc::new(Int64Array::from(discount_cents.to_vec())),
            Arc::new(BooleanArray::from(is_priority.to_vec())),
            Arc::new(StringArray::from(regions.to_vec())),
        ],
    )
    .expect("orders batch should construct")
}

fn orders_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("order_date", DataType::Utf8, false),
        Field::new("customer_tier", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("amount_cents", DataType::Int64, false),
        Field::new("discount_cents", DataType::Int64, true),
        Field::new("is_priority", DataType::Boolean, true),
        Field::new("region", DataType::Utf8, true),
    ]))
}

pub fn customers_table() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("customer_name", DataType::Utf8, false),
        Field::new("segment", DataType::Utf8, false),
        Field::new("target_cents", DataType::Int64, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec![
                "Northstar",
                "Meridian",
                "Redline",
                "Summit",
            ])),
            Arc::new(StringArray::from(vec![
                "enterprise",
                "self_serve",
                "self_serve",
                "enterprise",
            ])),
            Arc::new(Int64Array::from(vec![30_000, 10_000, 5_000, 20_000])),
        ],
    )
    .expect("customers batch should construct")
}

pub fn shipments_table() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("shipped_at", DataType::Utf8, true),
        Field::new("carrier", DataType::Utf8, true),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1001, 1003, 1006])),
            Arc::new(StringArray::from(vec![
                Some("2026-05-31"),
                Some("2026-05-30"),
                None,
            ])),
            Arc::new(StringArray::from(vec![Some("UPS"), Some("FedEx"), None])),
        ],
    )
    .expect("shipments batch should construct")
}

pub fn orders_delta_descriptor(schema: SchemaRef) -> DeltaTableDescriptor {
    DeltaTableDescriptor {
        table_name: "orders".to_string(),
        table_version: 42,
        schema,
        partition_columns: Vec::new(),
        partition_column_types: BTreeMap::new(),
        active_files: vec![
            active_file("orders-east.parquet", "\"uat-east\""),
            active_file("orders-west.parquet", "\"uat-west\""),
            active_file("orders-rest.parquet", "\"uat-rest\""),
        ],
    }
}

fn active_file(path: &str, etag: &str) -> DeltaActiveFile {
    DeltaActiveFile {
        path: path.to_string(),
        url: format!("https://example.test/uat/{path}"),
        size_bytes: 4096,
        partition_values: BTreeMap::new(),
        object_etag: Some(etag.to_string()),
        stats_json: Some(r#"{"numRecords":2}"#.to_string()),
        deletion_vector: None,
    }
}

pub fn normalize_batches(batches: &[RecordBatch]) -> Vec<Vec<Scalar>> {
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
        DataType::Int64 => Scalar::Int(
            column
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 column should downcast")
                .value(row_index),
        ),
        DataType::Int32 => Scalar::Int(i64::from(
            column
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32 column should downcast")
                .value(row_index),
        )),
        DataType::UInt64 => Scalar::Int(
            column
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("UInt64 column should downcast")
                .value(row_index)
                .try_into()
                .expect("UInt64 test value should fit in i64"),
        ),
        DataType::Float64 => Scalar::Float(format_float(
            column
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("Float64 column should downcast")
                .value(row_index),
        )),
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
        other => panic!("unsupported Arrow type in Axon engine UAT corpus: {other:?}"),
    }
}

fn format_float(value: f64) -> String {
    format!("{value:.6}")
}
