//! Experimental browser DataFusion proof-of-concept.
//!
//! This crate is intentionally isolated from Axon's default browser runtime and worker artifact.

use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use query_contract::{ExecutionTarget, QueryError, QueryErrorCode};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

pub const OWNER: &str = "Runtime / engine team";
pub const RESPONSIBILITY: &str =
    "Experimental browser DataFusion proof over browser-safe object bytes.";
pub const DEFAULT_TABLE_NAME: &str = "t";
pub const SMOKE_SQL: &str =
    "SELECT id, value FROM t WHERE category = 'B' AND value > 10 ORDER BY id";

pub fn runtime_target() -> ExecutionTarget {
    ExecutionTarget::BrowserWasm
}

pub fn is_experimental() -> bool {
    true
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataFusionCompileMarker {
    pub table_name: &'static str,
    pub datafusion_version: &'static str,
}

pub fn datafusion_compile_marker() -> DataFusionCompileMarker {
    let _context = SessionContext::new();
    DataFusionCompileMarker {
        table_name: DEFAULT_TABLE_NAME,
        datafusion_version: "52.4.0",
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExperimentalQueryResult {
    pub row_count: usize,
    pub column_names: Vec<String>,
}

pub async fn query_record_batch(
    sql: &str,
    batch: RecordBatch,
) -> Result<ExperimentalQueryResult, QueryError> {
    let batches = query_record_batches(sql, batch).await?;
    let row_count = batches.iter().map(RecordBatch::num_rows).sum();
    let column_names = batches
        .first()
        .map(|batch| {
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().to_string())
                .collect()
        })
        .unwrap_or_default();

    Ok(ExperimentalQueryResult {
        row_count,
        column_names,
    })
}

pub async fn query_record_batches(
    sql: &str,
    batch: RecordBatch,
) -> Result<Vec<RecordBatch>, QueryError> {
    let schema = batch.schema();
    let table = MemTable::try_new(schema, vec![vec![batch]]).map_err(map_datafusion_error)?;
    let context = SessionContext::new();
    context
        .register_table(DEFAULT_TABLE_NAME, Arc::new(table))
        .map_err(map_datafusion_error)?;
    let frame = context.sql(sql).await.map_err(map_datafusion_error)?;
    frame.collect().await.map_err(map_datafusion_error)
}

pub async fn query_record_batch_to_arrow_ipc(
    sql: &str,
    batch: RecordBatch,
) -> Result<Vec<u8>, QueryError> {
    let batches = query_record_batches(sql, batch).await?;
    encode_record_batches_to_arrow_ipc(&batches)
}

pub fn synthetic_record_batch() -> Result<RecordBatch, QueryError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int32Array::from(vec![5, 12, 25, 20])),
            Arc::new(StringArray::from(vec!["A", "B", "B", "C"])),
        ],
    )
    .map_err(|error| {
        QueryError::new(
            QueryErrorCode::ExecutionFailed,
            format!("experimental DataFusion batch construction failed: {error}"),
            runtime_target(),
        )
    })
}

fn encode_record_batches_to_arrow_ipc(batches: &[RecordBatch]) -> Result<Vec<u8>, QueryError> {
    let schema = batches.first().map(RecordBatch::schema).ok_or_else(|| {
        QueryError::new(
            QueryErrorCode::ExecutionFailed,
            "experimental browser DataFusion query returned no Arrow batches",
            runtime_target(),
        )
    })?;
    let mut encoded = Vec::new();
    let mut writer = StreamWriter::try_new(&mut encoded, schema.as_ref()).map_err(|error| {
        QueryError::new(
            QueryErrorCode::ExecutionFailed,
            format!("experimental browser DataFusion could not open Arrow IPC writer: {error}"),
            runtime_target(),
        )
    })?;
    for batch in batches {
        writer.write(batch).map_err(|error| {
            QueryError::new(
                QueryErrorCode::ExecutionFailed,
                format!(
                    "experimental browser DataFusion could not encode Arrow IPC batch: {error}"
                ),
                runtime_target(),
            )
        })?;
    }
    writer.finish().map_err(|error| {
        QueryError::new(
            QueryErrorCode::ExecutionFailed,
            format!("experimental browser DataFusion could not finish Arrow IPC stream: {error}"),
            runtime_target(),
        )
    })?;

    Ok(encoded)
}

fn map_datafusion_error(error: datafusion::error::DataFusionError) -> QueryError {
    QueryError::new(
        QueryErrorCode::ExecutionFailed,
        format!("experimental browser DataFusion query failed: {error}"),
        runtime_target(),
    )
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub async fn run_datafusion_smoke_query() -> Result<Vec<u8>, JsValue> {
    let batch = synthetic_record_batch().map_err(query_error_to_js_value)?;
    query_record_batch_to_arrow_ipc(SMOKE_SQL, batch)
        .await
        .map_err(query_error_to_js_value)
}

#[cfg(target_arch = "wasm32")]
fn query_error_to_js_value(error: QueryError) -> JsValue {
    JsValue::from_str(&format!("{:?}: {}", error.code, error.message))
}
