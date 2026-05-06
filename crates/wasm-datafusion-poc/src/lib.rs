//! Experimental browser DataFusion proof-of-concept.
//!
//! This crate is intentionally isolated from Axon's default browser runtime and worker artifact.

use std::{any::Any, collections::BTreeMap, fmt, future::Future, pin::Pin, sync::Arc};

use arrow_array::{Int32Array, RecordBatch, RecordBatchOptions, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    execution_plan::{Boundedness, EmissionType},
    stream::RecordBatchStreamAdapter,
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::prelude::SessionContext;
use futures_util::{stream, TryStreamExt};
use query_contract::{ExecutionTarget, PartitionColumnType, QueryError, QueryErrorCode};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;
use wasm_http_object_store::HttpRangeReader;
use wasm_parquet_engine::{stream_scan_targets, ObjectSource, ScanTarget};

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
    in_memory_partitions: Vec<Vec<RecordBatch>>,
}

impl AxonDeltaTableProvider {
    pub fn new(descriptor: DeltaTableDescriptor) -> Self {
        Self {
            descriptor,
            in_memory_partitions: Vec::new(),
        }
    }

    pub fn with_record_batch_partitions(
        descriptor: DeltaTableDescriptor,
        partitions: Vec<Vec<RecordBatch>>,
    ) -> Self {
        Self {
            descriptor,
            in_memory_partitions: partitions,
        }
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
            Ok(Arc::new(AxonParquetScanExec::new(
                self.descriptor.clone(),
                schema,
                projection.cloned(),
                self.in_memory_partitions.clone(),
            )) as Arc<dyn ExecutionPlan>)
        })
    }
}

#[derive(Debug)]
pub struct AxonParquetScanExec {
    descriptor: DeltaTableDescriptor,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    partitions: Vec<Vec<RecordBatch>>,
    properties: PlanProperties,
}

impl AxonParquetScanExec {
    fn new(
        descriptor: DeltaTableDescriptor,
        projected_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        partitions: Vec<Vec<RecordBatch>>,
    ) -> Self {
        let partition_count = partitions.len().max(1);
        let properties = Self::compute_properties(Arc::clone(&projected_schema), partition_count);

        Self {
            descriptor,
            projected_schema,
            projection,
            partitions,
            properties,
        }
    }

    fn compute_properties(schema: SchemaRef, partition_count: usize) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }

    fn partition_count(&self) -> usize {
        if self.partitions.is_empty() {
            1
        } else {
            self.partitions.len()
        }
    }

    fn scan_targets(&self) -> DataFusionResult<Vec<ScanTarget>> {
        self.descriptor
            .active_files
            .iter()
            .map(delta_active_file_to_scan_target)
            .collect()
    }

    fn required_columns(&self) -> Vec<String> {
        self.projected_schema
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect()
    }

    fn execute_in_memory(&self, partition: usize) -> DataFusionResult<SendableRecordBatchStream> {
        let batches = self.partitions.get(partition).cloned().unwrap_or_default();
        let projection = self.projection.clone();
        let projected_batches = batches.into_iter().map(move |batch| {
            if let Some(projection) = &projection {
                batch
                    .project(projection)
                    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
            } else {
                Ok(batch)
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.projected_schema),
            stream::iter(projected_batches),
        )))
    }

    fn execute_parquet_scan(&self) -> DataFusionResult<SendableRecordBatchStream> {
        let reader = HttpRangeReader::new();
        let targets = self.scan_targets()?;
        let required_columns = self.required_columns();
        let partition_column_types = self.descriptor.partition_column_types.clone();
        let stream_schema = Arc::clone(&self.projected_schema);
        let projected_schema = Arc::clone(&self.projected_schema);
        let parquet_batches = stream::once(async move {
            stream_scan_targets(
                &reader,
                &targets,
                &required_columns,
                &partition_column_types,
                None,
            )
            .await
            .map(|scan| {
                scan.batches
                    .map_err(map_parquet_query_error)
                    .and_then(move |batch| {
                        let projected_schema = Arc::clone(&projected_schema);
                        async move { align_record_batch_to_schema(batch, projected_schema) }
                    })
            })
            .map_err(map_parquet_query_error)
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            parquet_batches,
        )))
    }
}

impl DisplayAs for AxonParquetScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "AxonParquetScanExec: table={}, active_files={}, partitions={}",
                self.descriptor.table_name,
                self.descriptor.active_files.len(),
                self.partition_count()
            ),
            DisplayFormatType::TreeRender => write!(f, ""),
        }
    }
}

impl ExecutionPlan for AxonParquetScanExec {
    fn name(&self) -> &str {
        "AxonParquetScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "AxonParquetScanExec does not accept child execution plans".to_string(),
            ));
        }

        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition >= self.partition_count() {
            return Err(DataFusionError::Internal(format!(
                "AxonParquetScanExec invalid partition {partition} (expected less than {})",
                self.partition_count()
            )));
        }

        if self.partitions.is_empty() {
            self.execute_parquet_scan()
        } else {
            self.execute_in_memory(partition)
        }
    }
}

fn delta_active_file_to_scan_target(active_file: &DeltaActiveFile) -> DataFusionResult<ScanTarget> {
    if active_file.deletion_vector.is_some() {
        return Err(DataFusionError::NotImplemented(
            "AxonParquetScanExec does not yet support Delta deletion vectors".to_string(),
        ));
    }

    Ok(ScanTarget {
        object_source: ObjectSource::new(active_file.url.clone()),
        object_etag: active_file.object_etag.clone(),
        path: active_file.path.clone(),
        size_bytes: active_file.size_bytes,
        partition_values: active_file.partition_values.clone(),
    })
}

fn align_record_batch_to_schema(
    batch: RecordBatch,
    schema: SchemaRef,
) -> DataFusionResult<RecordBatch> {
    let batch_schema = batch.schema();
    let projection = schema
        .fields()
        .iter()
        .map(|field| decoded_batch_index_for_projected_field(&batch_schema, field.name()))
        .collect::<DataFusionResult<Vec<_>>>()?;
    let projected = batch
        .project(&projection)
        .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;

    let options = RecordBatchOptions::new().with_row_count(Some(projected.num_rows()));
    RecordBatch::try_new_with_options(schema, projected.columns().to_vec(), &options)
        .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

fn decoded_batch_index_for_projected_field(
    batch_schema: &SchemaRef,
    projected_field_name: &str,
) -> DataFusionResult<usize> {
    if let Ok(index) = batch_schema.index_of(projected_field_name) {
        return Ok(index);
    }

    let normalized_projected_name = normalize_column_name(projected_field_name);
    let mut matches = batch_schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_index, field)| normalize_column_name(field.name()) == normalized_projected_name)
        .map(|(index, _field)| index);

    let Some(index) = matches.next() else {
        return Err(DataFusionError::Execution(format!(
            "AxonParquetScanExec could not align decoded batch to projected schema column '{projected_field_name}'"
        )));
    };
    if matches.next().is_some() {
        return Err(DataFusionError::Execution(format!(
            "AxonParquetScanExec decoded batch has multiple columns matching projected schema column '{projected_field_name}' after name normalization"
        )));
    }

    Ok(index)
}

fn normalize_column_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

fn map_parquet_query_error(error: QueryError) -> DataFusionError {
    DataFusionError::Execution(format!(
        "experimental browser DataFusion parquet scan failed: {:?}: {}",
        error.code, error.message
    ))
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

    /// Registers a descriptor-backed table with controlled in-memory scan partitions.
    ///
    /// This proof-of-concept hook keeps DataFusion execution tests deterministic
    /// while normal descriptor-backed scans stream browser Parquet batches.
    pub async fn open_delta_table_with_record_batch_partitions(
        &mut self,
        descriptor: DeltaTableDescriptor,
        partitions: Vec<Vec<RecordBatch>>,
    ) -> Result<(), QueryError> {
        let table_name = descriptor.table_name.clone();
        self.ctx
            .register_table(
                table_name,
                Arc::new(AxonDeltaTableProvider::with_record_batch_partitions(
                    descriptor, partitions,
                )),
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
