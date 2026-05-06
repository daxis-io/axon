//! Experimental browser DataFusion proof-of-concept.
//!
//! This crate is intentionally isolated from Axon's default browser runtime and worker artifact.

use std::{any::Any, collections::BTreeMap, future::Future, pin::Pin, sync::Arc};

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::{empty::EmptyExec, ExecutionPlan};
use datafusion::prelude::SessionContext;
use query_contract::{ExecutionTarget, PartitionColumnType, QueryError, QueryErrorCode};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

const DEFAULT_CATALOG_NAME: &str = "datafusion";
const DEFAULT_SCHEMA_NAME: &str = "public";

type TableScanFuture<'a> =
    Pin<Box<dyn Future<Output = DataFusionResult<Arc<dyn ExecutionPlan>>> + Send + 'a>>;

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
        datafusion_version: datafusion::DATAFUSION_VERSION,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExperimentalQueryResult {
    pub row_count: usize,
    pub column_names: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DeltaTableDescriptor {
    pub table_name: String,
    pub table_version: i64,
    pub schema: SchemaRef,
    pub partition_columns: Vec<String>,
    pub partition_column_types: BTreeMap<String, PartitionColumnType>,
    pub active_files: Vec<DeltaActiveFile>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeltaActiveFile {
    pub path: String,
    pub url: String,
    pub size_bytes: u64,
    pub partition_values: BTreeMap<String, Option<String>>,
    pub object_etag: Option<String>,
    pub stats_json: Option<String>,
    pub deletion_vector: Option<DeletionVectorDescriptor>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeletionVectorDescriptor {
    pub storage_type: String,
    pub path_or_inline_dv: String,
    pub offset: Option<i64>,
    pub size_in_bytes: Option<i64>,
    pub cardinality: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct AxonDeltaTableProvider {
    descriptor: DeltaTableDescriptor,
}

impl AxonDeltaTableProvider {
    pub fn new(descriptor: DeltaTableDescriptor) -> Self {
        Self { descriptor }
    }

    fn projected_schema(&self, projection: Option<&Vec<usize>>) -> DataFusionResult<SchemaRef> {
        match projection {
            Some(projection) => self
                .descriptor
                .schema
                .project(projection)
                .map(Arc::new)
                .map_err(|error| DataFusionError::ArrowError(Box::new(error), None)),
            None => Ok(Arc::clone(&self.descriptor.schema)),
        }
    }
}

impl TableProvider for AxonDeltaTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.descriptor.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn scan<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        _state: &'life1 dyn Session,
        projection: Option<&'life2 Vec<usize>>,
        _filters: &'life3 [Expr],
        _limit: Option<usize>,
    ) -> TableScanFuture<'async_trait>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let schema = self.projected_schema(projection)?;
            Ok(Arc::new(EmptyExec::new(schema)) as Arc<dyn ExecutionPlan>)
        })
    }
}

pub struct WasmDataFusionEngine {
    ctx: SessionContext,
}

impl WasmDataFusionEngine {
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
        }
    }

    pub async fn open_delta_table(
        &mut self,
        descriptor: DeltaTableDescriptor,
    ) -> Result<(), QueryError> {
        let table_name = descriptor.table_name.clone();
        self.ctx
            .register_table(
                table_name,
                Arc::new(AxonDeltaTableProvider::new(descriptor)),
            )
            .map_err(map_datafusion_error)?;

        Ok(())
    }

    pub fn table_names(&self) -> Vec<String> {
        let mut names = self
            .ctx
            .catalog(DEFAULT_CATALOG_NAME)
            .and_then(|catalog| catalog.schema(DEFAULT_SCHEMA_NAME))
            .map(|schema| schema.table_names())
            .unwrap_or_default();
        names.sort();
        names
    }

    pub async fn register_record_batches(
        &mut self,
        table_name: &str,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<(), QueryError> {
        let table = MemTable::try_new(schema, vec![batches]).map_err(map_datafusion_error)?;
        self.ctx
            .register_table(table_name, Arc::new(table))
            .map_err(map_datafusion_error)?;

        Ok(())
    }

    pub async fn sql_to_record_batches(
        &self,
        sql: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), QueryError> {
        let frame = self.ctx.sql(sql).await.map_err(map_datafusion_error)?;
        let output_schema = frame.schema().inner().clone();
        let batches = frame.collect().await.map_err(map_datafusion_error)?;
        Ok((output_schema, batches))
    }

    pub async fn sql_to_arrow_ipc(&self, sql: &str) -> Result<Vec<u8>, QueryError> {
        let (schema, batches) = self.sql_to_record_batches(sql).await?;
        encode_record_batches_to_arrow_ipc(schema, &batches)
    }
}

impl Default for WasmDataFusionEngine {
    fn default() -> Self {
        Self::new()
    }
}

pub async fn query_record_batch(
    sql: &str,
    batch: RecordBatch,
) -> Result<ExperimentalQueryResult, QueryError> {
    let (schema, batches) = query_record_batches_with_schema(sql, batch).await?;
    let row_count = batches.iter().map(RecordBatch::num_rows).sum();
    let column_names = schema
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect();

    Ok(ExperimentalQueryResult {
        row_count,
        column_names,
    })
}

pub async fn query_record_batches(
    sql: &str,
    batch: RecordBatch,
) -> Result<Vec<RecordBatch>, QueryError> {
    let (_schema, batches) = query_record_batches_with_schema(sql, batch).await?;
    Ok(batches)
}

async fn query_record_batches_with_schema(
    sql: &str,
    batch: RecordBatch,
) -> Result<(SchemaRef, Vec<RecordBatch>), QueryError> {
    let schema = batch.schema();
    let mut engine = WasmDataFusionEngine::new();
    engine
        .register_record_batches(DEFAULT_TABLE_NAME, schema, vec![batch])
        .await?;
    engine.sql_to_record_batches(sql).await
}

pub async fn query_record_batch_to_arrow_ipc(
    sql: &str,
    batch: RecordBatch,
) -> Result<Vec<u8>, QueryError> {
    let (schema, batches) = query_record_batches_with_schema(sql, batch).await?;
    encode_record_batches_to_arrow_ipc(schema, &batches)
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

fn encode_record_batches_to_arrow_ipc(
    schema: SchemaRef,
    batches: &[RecordBatch],
) -> Result<Vec<u8>, QueryError> {
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
