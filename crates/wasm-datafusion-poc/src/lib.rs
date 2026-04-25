//! Experimental browser DataFusion proof-of-concept.
//!
//! This crate is intentionally isolated from Axon's default browser runtime and worker artifact.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use query_contract::{ExecutionTarget, QueryError, QueryErrorCode};

pub const OWNER: &str = "Runtime / engine team";
pub const RESPONSIBILITY: &str =
    "Experimental browser DataFusion proof over browser-safe object bytes.";
pub const DEFAULT_TABLE_NAME: &str = "axon_table";

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
    let schema = batch.schema();
    let table = MemTable::try_new(schema, vec![vec![batch]]).map_err(map_datafusion_error)?;
    let context = SessionContext::new();
    context
        .register_table(DEFAULT_TABLE_NAME, Arc::new(table))
        .map_err(map_datafusion_error)?;
    let frame = context.sql(sql).await.map_err(map_datafusion_error)?;
    let batches = frame.collect().await.map_err(map_datafusion_error)?;
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

pub fn synthetic_record_batch() -> Result<RecordBatch, QueryError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["A", "B", "B"])),
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

fn map_datafusion_error(error: datafusion::error::DataFusionError) -> QueryError {
    QueryError::new(
        QueryErrorCode::ExecutionFailed,
        format!("experimental browser DataFusion query failed: {error}"),
        runtime_target(),
    )
}
