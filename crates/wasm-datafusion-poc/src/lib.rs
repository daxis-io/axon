//! Experimental browser DataFusion proof-of-concept.
//!
//! This crate is intentionally isolated from Axon's default browser runtime and worker artifact.

mod ipc_cursor;

pub use ipc_cursor::{
    ArrowIpcPhase, CursorFault, DataFusionIpcCursor, IpcCursorItem, IpcCursorMetrics, IpcPreview,
    IpcStreamLimits, IpcTransportChunk, QueryCancelHandle, QueryTerminal, QueryTerminalStatus,
};

#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicU8};
use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    error::Error as StdError,
    fmt,
    future::Future,
    io::{self, Write},
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use arrow_array::{Int32Array, RecordBatch, RecordBatchOptions, StringArray};
use arrow_ipc::writer::StreamWriter;
pub use arrow_schema::DataType as DeltaTableFieldDataType;
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::ScalarValue;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    execution_plan::{execute_stream, Boundedness, EmissionType},
    stream::RecordBatchStreamAdapter,
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::prelude::SessionContext;
use futures_util::{
    stream::{self, BoxStream},
    StreamExt, TryStreamExt,
};
use query_contract::{
    BrowserHttpSnapshotDescriptor, ExecutionTarget, FallbackReason, PartitionColumnType,
    PartitionLiteralValue, QueryError, QueryErrorCode,
};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;
use wasm_http_object_store::HttpRangeReader;
use wasm_parquet_engine::{
    merge_parquet_range_read_metrics,
    stream_scan_target_batches_with_row_group_pruning_caches_and_query, ObjectSource,
    ParquetIntegerComparison, ParquetMetadataCache, ParquetRangeCache, ParquetRangeQueryContext,
    ParquetRangeReadMetrics, ParquetRowGroupPruningPredicate, ScanTarget, ScanTargetMetricsHandle,
    ScanTargetMetricsSnapshot,
};

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataFusionArrowIpcResult {
    pub ipc_bytes: Vec<u8>,
    pub row_count: u64,
    pub encoded_bytes: u64,
    pub scan_metrics: DataFusionScanMetricsSummary,
    pub physical_plan: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BrowserQueryBudget {
    pub max_scan_bytes: Option<u64>,
    pub max_output_ipc_bytes: Option<u64>,
    pub max_batches_in_flight: Option<usize>,
    pub max_rows_returned: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct BrowserQueryCancellation {
    generation: Arc<AtomicU64>,
    observed_generation: u64,
    #[cfg(test)]
    cancel_at_checkpoint: Arc<AtomicU8>,
}

impl BrowserQueryCancellation {
    pub fn new() -> Self {
        Self {
            generation: Arc::new(AtomicU64::new(0)),
            observed_generation: 0,
            #[cfg(test)]
            cancel_at_checkpoint: Arc::new(AtomicU8::new(CANCEL_AT_NO_CHECKPOINT)),
        }
    }

    pub fn cancel(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.generation.load(Ordering::SeqCst) != self.observed_generation
    }

    fn snapshot(&self) -> Self {
        Self {
            generation: Arc::clone(&self.generation),
            observed_generation: self.generation.load(Ordering::SeqCst),
            #[cfg(test)]
            cancel_at_checkpoint: Arc::clone(&self.cancel_at_checkpoint),
        }
    }

    fn check_cancelled(&self) -> Result<(), QueryError> {
        self.check_cancelled_at("query execution")
    }

    fn check_cancelled_at(&self, point: &str) -> Result<(), QueryError> {
        #[cfg(test)]
        if self.should_cancel_at_checkpoint(point) {
            self.cancel();
        }

        if self.is_cancelled() {
            Err(query_cancelled_error(point))
        } else {
            Ok(())
        }
    }

    #[cfg(test)]
    fn cancel_at_next_arrow_ipc_batch_encoding_checkpoint_for_test(&self) {
        self.cancel_at_checkpoint
            .store(CANCEL_AT_ARROW_IPC_OUTPUT, Ordering::SeqCst);
    }

    #[cfg(test)]
    fn should_cancel_at_checkpoint(&self, point: &str) -> bool {
        point == "Arrow IPC batch encoding"
            && self
                .cancel_at_checkpoint
                .compare_exchange(
                    CANCEL_AT_ARROW_IPC_OUTPUT,
                    CANCEL_AT_NO_CHECKPOINT,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
    }
}

impl Default for BrowserQueryCancellation {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
const CANCEL_AT_NO_CHECKPOINT: u8 = 0;
#[cfg(test)]
const CANCEL_AT_ARROW_IPC_OUTPUT: u8 = 1;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DataFusionScanMetricsSummary {
    pub scan_count: u64,
    pub files_touched: u64,
    pub files_skipped: u64,
    pub row_groups_touched: u64,
    pub row_groups_skipped: u64,
    pub rows_emitted: u64,
    pub bytes_fetched: u64,
    pub range_read_metrics: ParquetRangeReadMetrics,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeltaTableSchema {
    pub fields: Vec<DeltaTableSchemaField>,
}

impl DeltaTableSchema {
    pub fn new(fields: Vec<DeltaTableSchemaField>) -> Self {
        Self { fields }
    }

    fn arrow_schema(&self) -> SchemaRef {
        Arc::new(Schema::new(
            self.fields
                .iter()
                .map(DeltaTableSchemaField::arrow_field)
                .collect::<Vec<_>>(),
        ))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeltaTableSchemaField {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl DeltaTableSchemaField {
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
        }
    }

    fn arrow_field(&self) -> Field {
        Field::new(self.name.clone(), self.data_type.clone(), self.nullable)
    }
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

impl DeltaTableDescriptor {
    pub fn from_browser_http_snapshot(
        table_name: impl Into<String>,
        snapshot: &BrowserHttpSnapshotDescriptor,
        schema: DeltaTableSchema,
    ) -> Result<Self, QueryError> {
        Self::from_browser_http_snapshot_with_object_etags(
            table_name,
            snapshot,
            schema,
            &BTreeMap::new(),
        )
    }

    pub fn from_browser_http_snapshot_with_object_etags(
        table_name: impl Into<String>,
        snapshot: &BrowserHttpSnapshotDescriptor,
        schema: DeltaTableSchema,
        object_etags_by_path: &BTreeMap<String, String>,
    ) -> Result<Self, QueryError> {
        let table_name = table_name.into();
        if table_name.trim().is_empty() {
            return Err(QueryError::new(
                QueryErrorCode::InvalidRequest,
                "browser DataFusion table names cannot be empty",
                runtime_target(),
            ));
        }

        Ok(Self {
            table_name,
            table_version: snapshot.snapshot_version,
            schema: schema.arrow_schema(),
            partition_columns: browser_snapshot_partition_columns(snapshot),
            partition_column_types: snapshot.partition_column_types.clone(),
            active_files: snapshot
                .active_files
                .iter()
                .map(|file| DeltaActiveFile {
                    path: file.path.clone(),
                    url: file.url.clone(),
                    size_bytes: file.size_bytes,
                    partition_values: file.partition_values.clone(),
                    object_etag: object_etags_by_path.get(&file.path).cloned(),
                    stats_json: file.stats.clone(),
                    deletion_vector: None,
                })
                .collect(),
        })
    }
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

fn browser_snapshot_partition_columns(snapshot: &BrowserHttpSnapshotDescriptor) -> Vec<String> {
    let mut partition_columns = snapshot
        .partition_column_types
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    for file in &snapshot.active_files {
        partition_columns.extend(file.partition_values.keys().cloned());
    }
    partition_columns.into_iter().collect()
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AxonParquetScanTrace {
    pub projected_columns: Vec<String>,
    pub limit: Option<usize>,
    pub filters: Vec<String>,
    pub exact_filters: Vec<String>,
    pub inexact_filters: Vec<String>,
    pub files_total: usize,
    pub files_planned: usize,
    pub planned_scan_bytes: u64,
    pub planned_file_paths: Vec<String>,
    pub files_skipped: u64,
    pub row_groups_touched: u64,
    pub row_groups_skipped: u64,
    pub bytes_fetched: u64,
    pub rows_emitted: u64,
    pub range_read_metrics: ParquetRangeReadMetrics,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct AxonParquetScanDebug {
    projected_columns: Vec<String>,
    limit: Option<usize>,
    filters: Vec<String>,
    exact_filters: Vec<String>,
    inexact_filters: Vec<String>,
    files_total: usize,
    files_planned: usize,
    planned_scan_bytes: u64,
    planned_file_paths: Vec<String>,
    files_skipped: u64,
}

impl AxonParquetScanDebug {
    fn to_trace(&self, metrics: AxonParquetScanMetrics) -> AxonParquetScanTrace {
        AxonParquetScanTrace {
            projected_columns: self.projected_columns.clone(),
            limit: self.limit,
            filters: self.filters.clone(),
            exact_filters: self.exact_filters.clone(),
            inexact_filters: self.inexact_filters.clone(),
            files_total: self.files_total,
            files_planned: self.files_planned,
            planned_scan_bytes: self.planned_scan_bytes,
            planned_file_paths: self.planned_file_paths.clone(),
            files_skipped: self.files_skipped,
            row_groups_touched: metrics.row_groups_touched,
            row_groups_skipped: metrics.row_groups_skipped,
            bytes_fetched: metrics.bytes_fetched,
            rows_emitted: metrics.rows_emitted,
            range_read_metrics: metrics.range_read_metrics,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct AxonParquetScanMetrics {
    row_groups_touched: u64,
    row_groups_skipped: u64,
    bytes_fetched: u64,
    rows_emitted: u64,
    range_read_metrics: ParquetRangeReadMetrics,
}

#[derive(Clone, Debug)]
pub struct AxonDeltaTableProvider {
    descriptor: Arc<DeltaTableDescriptor>,
    in_memory_partitions: Vec<Vec<RecordBatch>>,
    query_budget: BrowserQueryBudget,
    cancellation: BrowserQueryCancellation,
    metadata_cache: ParquetMetadataCache,
    range_cache: ParquetRangeCache,
}

impl AxonDeltaTableProvider {
    pub fn new(descriptor: DeltaTableDescriptor) -> Self {
        Self::with_query_controls(
            descriptor,
            Vec::new(),
            BrowserQueryBudget::default(),
            BrowserQueryCancellation::default(),
            ParquetMetadataCache::default(),
        )
    }

    pub fn with_query_controls(
        descriptor: DeltaTableDescriptor,
        in_memory_partitions: Vec<Vec<RecordBatch>>,
        query_budget: BrowserQueryBudget,
        cancellation: BrowserQueryCancellation,
        metadata_cache: ParquetMetadataCache,
    ) -> Self {
        Self::with_query_controls_and_caches(
            descriptor,
            in_memory_partitions,
            query_budget,
            cancellation,
            metadata_cache,
            ParquetRangeCache::default(),
        )
    }

    pub fn with_query_controls_and_caches(
        descriptor: DeltaTableDescriptor,
        in_memory_partitions: Vec<Vec<RecordBatch>>,
        query_budget: BrowserQueryBudget,
        cancellation: BrowserQueryCancellation,
        metadata_cache: ParquetMetadataCache,
        range_cache: ParquetRangeCache,
    ) -> Self {
        Self {
            descriptor: Arc::new(descriptor),
            in_memory_partitions,
            query_budget,
            cancellation,
            metadata_cache,
            range_cache,
        }
    }

    pub fn with_record_batch_partitions(
        descriptor: DeltaTableDescriptor,
        partitions: Vec<Vec<RecordBatch>>,
    ) -> Self {
        Self::with_query_controls(
            descriptor,
            partitions,
            BrowserQueryBudget::default(),
            BrowserQueryCancellation::default(),
            ParquetMetadataCache::default(),
        )
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

    fn can_apply_exact_partition_filters(&self) -> bool {
        self.in_memory_partitions.is_empty()
            || self.in_memory_partitions.len() == self.descriptor.active_files.len()
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
        state: &'life1 dyn Session,
        projection: Option<&'life2 Vec<usize>>,
        filters: &'life3 [Expr],
        limit: Option<usize>,
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
            let query_context = state
                .config()
                .get_extension::<ParquetRangeQueryContext>()
                .map(|context| context.as_ref().clone());
            let cancellation = state
                .config()
                .get_extension::<BrowserQueryCancellation>()
                .map(|cancellation| cancellation.as_ref().clone())
                .unwrap_or_else(|| self.cancellation.snapshot());
            Ok(Arc::new(AxonParquetScanExec::new(
                Arc::clone(&self.descriptor),
                schema,
                projection.cloned(),
                filters.to_vec(),
                limit,
                self.in_memory_partitions.clone(),
                self.query_budget,
                cancellation,
                self.metadata_cache.clone(),
                self.range_cache.clone(),
                query_context,
            )) as Arc<dyn ExecutionPlan>)
        })
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        let exact_partition_filters_enabled = self.can_apply_exact_partition_filters();
        let parquet_row_group_pruning_enabled = self.in_memory_partitions.is_empty();
        Ok(filters
            .iter()
            .map(|filter| {
                classify_filter_pushdown(
                    filter,
                    self.descriptor.as_ref(),
                    exact_partition_filters_enabled,
                    parquet_row_group_pruning_enabled,
                )
            })
            .collect())
    }
}

#[derive(Debug)]
pub struct AxonParquetScanExec {
    descriptor: Arc<DeltaTableDescriptor>,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    planned_file_indices: Vec<usize>,
    planned_partition_indices: Vec<usize>,
    row_group_predicate: Option<ParquetRowGroupPruningPredicate>,
    partitions: Vec<Vec<RecordBatch>>,
    properties: Arc<PlanProperties>,
    debug: AxonParquetScanDebug,
    metrics: Arc<Mutex<AxonParquetScanMetrics>>,
    limit_remaining: Arc<Mutex<Option<usize>>>,
    query_budget: BrowserQueryBudget,
    cancellation: BrowserQueryCancellation,
    metadata_cache: ParquetMetadataCache,
    range_cache: ParquetRangeCache,
    query_context: Option<ParquetRangeQueryContext>,
}

impl AxonParquetScanExec {
    fn new(
        descriptor: Arc<DeltaTableDescriptor>,
        projected_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        limit: Option<usize>,
        partitions: Vec<Vec<RecordBatch>>,
        query_budget: BrowserQueryBudget,
        cancellation: BrowserQueryCancellation,
        metadata_cache: ParquetMetadataCache,
        range_cache: ParquetRangeCache,
        query_context: Option<ParquetRangeQueryContext>,
    ) -> Self {
        let plan = AxonScanPlan::new(
            descriptor.as_ref(),
            &projected_schema,
            &filters,
            limit,
            &partitions,
        );
        let partition_count = if partitions.is_empty() {
            1
        } else {
            plan.planned_partition_indices.len().max(1)
        };
        let properties = Arc::new(Self::compute_properties(
            Arc::clone(&projected_schema),
            partition_count,
        ));

        Self {
            descriptor,
            projected_schema,
            projection,
            limit: plan.limit,
            planned_file_indices: plan.planned_file_indices,
            planned_partition_indices: plan.planned_partition_indices,
            row_group_predicate: plan.row_group_predicate,
            partitions,
            properties,
            debug: plan.debug,
            metrics: Arc::new(Mutex::new(AxonParquetScanMetrics::default())),
            limit_remaining: Arc::new(Mutex::new(plan.limit)),
            query_budget,
            cancellation,
            metadata_cache,
            range_cache,
            query_context,
        }
    }

    pub fn pushdown_trace(&self) -> AxonParquetScanTrace {
        let metrics = self
            .metrics
            .lock()
            .expect("AxonParquetScanExec metrics should not be poisoned")
            .clone();
        self.debug.to_trace(metrics)
    }

    fn planned_scan_bytes(&self) -> u64 {
        self.debug.planned_scan_bytes
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
            self.planned_partition_indices.len().max(1)
        }
    }

    fn required_columns(&self) -> Vec<String> {
        self.projected_schema
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect()
    }

    fn execute_in_memory(&self, partition: usize) -> DataFusionResult<SendableRecordBatchStream> {
        let batches = self
            .planned_partition_indices
            .get(partition)
            .and_then(|source_partition| self.partitions.get(*source_partition))
            .cloned()
            .unwrap_or_default();
        let projection = self.projection.clone();
        let metrics = Arc::clone(&self.metrics);
        let limit_remaining = Arc::clone(&self.limit_remaining);
        let cancellation = self.cancellation.clone();
        let projected_batches = batches.into_iter().filter_map(move |batch| {
            if let Err(error) = cancellation.check_cancelled() {
                return Some(Err(datafusion_error_from_query_error(error)));
            }
            let projected = if let Some(projection) = &projection {
                batch
                    .project(projection)
                    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
            } else {
                Ok(batch)
            };
            let projected = match projected {
                Ok(projected) => projected,
                Err(error) => return Some(Err(error)),
            };
            let rows_to_emit = take_scan_rows(&limit_remaining, projected.num_rows());
            if rows_to_emit == 0 {
                return None;
            }
            let projected = slice_record_batch(projected, rows_to_emit);
            record_scan_rows_emitted(&metrics, projected.num_rows());
            Some(Ok(projected))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.projected_schema),
            stream::iter(projected_batches),
        )))
    }

    fn execute_parquet_scan(&self) -> DataFusionResult<SendableRecordBatchStream> {
        let reader = HttpRangeReader::new();
        let descriptor = Arc::clone(&self.descriptor);
        let planned_file_indices = self.planned_file_indices.clone();
        let required_columns = self.required_columns();
        let partition_column_types = self.descriptor.partition_column_types.clone();
        let row_group_predicate = self.row_group_predicate.clone();
        let metrics = Arc::clone(&self.metrics);
        let limit_remaining = Arc::clone(&self.limit_remaining);
        let query_budget = self.query_budget;
        let cancellation = self.cancellation.clone();
        let metadata_cache = self.metadata_cache.clone();
        let range_cache = self.range_cache.clone();
        let query_context = self.query_context.clone();
        let stream_schema = Arc::clone(&self.projected_schema);
        let projected_schema = Arc::clone(&self.projected_schema);
        let parquet_batches = stream::once(async move {
            stream_planned_scan_targets(
                reader,
                descriptor,
                planned_file_indices,
                required_columns,
                partition_column_types,
                row_group_predicate,
                metrics,
                limit_remaining,
                query_budget,
                cancellation,
                metadata_cache,
                range_cache,
                query_context,
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

#[derive(Clone, Debug)]
struct AxonScanPlan {
    limit: Option<usize>,
    planned_file_indices: Vec<usize>,
    planned_partition_indices: Vec<usize>,
    row_group_predicate: Option<ParquetRowGroupPruningPredicate>,
    debug: AxonParquetScanDebug,
}

impl AxonScanPlan {
    fn new(
        descriptor: &DeltaTableDescriptor,
        projected_schema: &SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
        partitions: &[Vec<RecordBatch>],
    ) -> Self {
        let exact_partition_filters_enabled =
            partitions.is_empty() || partitions.len() == descriptor.active_files.len();
        let partition_constraints = if exact_partition_filters_enabled {
            filters
                .iter()
                .flat_map(|filter| extract_partition_constraints(filter, descriptor))
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let conjuncts = filter_conjuncts(filters);
        let file_stats_constraints = conjuncts
            .iter()
            .filter_map(|filter| file_stats_pruning_constraint(filter, descriptor))
            .collect::<Vec<_>>();
        let planned_file_indices =
            planned_file_indices(descriptor, &partition_constraints, &file_stats_constraints);
        let planned_partition_indices =
            planned_partition_indices(descriptor, partitions, &planned_file_indices);
        let files_total = descriptor.active_files.len();
        let files_planned = planned_file_indices.len();
        let planned_file_paths = planned_file_indices
            .iter()
            .filter_map(|index| descriptor.active_files.get(*index))
            .map(|file| file.path.clone())
            .collect::<Vec<_>>();
        let planned_scan_bytes = planned_file_indices
            .iter()
            .filter_map(|index| descriptor.active_files.get(*index))
            .fold(0_u64, |total, file| total.saturating_add(file.size_bytes));
        let files_skipped = files_total.saturating_sub(files_planned) as u64;
        let exact_filters = conjuncts
            .iter()
            .filter(|filter| {
                exact_partition_filters_enabled
                    && exact_partition_constraints(filter, descriptor).is_some()
            })
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let inexact_filters = conjuncts
            .iter()
            .filter(|filter| {
                !(exact_partition_filters_enabled
                    && exact_partition_constraints(filter, descriptor).is_some())
            })
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let limit = inexact_filters.is_empty().then_some(limit).flatten();
        let debug = AxonParquetScanDebug {
            projected_columns: projected_column_names(projected_schema),
            limit,
            filters: filters.iter().map(ToString::to_string).collect(),
            exact_filters,
            inexact_filters,
            files_total,
            files_planned,
            planned_scan_bytes,
            planned_file_paths,
            files_skipped,
        };

        Self {
            limit,
            planned_file_indices,
            planned_partition_indices,
            row_group_predicate: filters
                .iter()
                .find_map(|filter| row_group_pruning_predicate(filter, descriptor)),
            debug,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PartitionConstraint {
    column: String,
    kind: PartitionConstraintKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PartitionConstraintKind {
    AllowedValues(Vec<Option<String>>),
    NotNull,
}

fn projected_column_names(schema: &SchemaRef) -> Vec<String> {
    schema
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect()
}

fn planned_file_indices(
    descriptor: &DeltaTableDescriptor,
    constraints: &[PartitionConstraint],
    stats_constraints: &[FileStatsConstraint],
) -> Vec<usize> {
    descriptor
        .active_files
        .iter()
        .enumerate()
        .filter(|(_index, file)| {
            partition_values_match(descriptor, file, constraints)
                && file_stats_match(file, stats_constraints)
        })
        .map(|(index, _file)| index)
        .collect()
}

fn planned_partition_indices(
    descriptor: &DeltaTableDescriptor,
    partitions: &[Vec<RecordBatch>],
    planned_file_indices: &[usize],
) -> Vec<usize> {
    if partitions.is_empty() {
        Vec::new()
    } else if descriptor.active_files.len() == partitions.len() {
        planned_file_indices
            .iter()
            .copied()
            .filter(|index| *index < partitions.len())
            .collect()
    } else {
        (0..partitions.len()).collect()
    }
}

fn partition_values_match(
    descriptor: &DeltaTableDescriptor,
    active_file: &DeltaActiveFile,
    constraints: &[PartitionConstraint],
) -> bool {
    constraints.iter().all(|constraint| {
        let Some(partition_type) = partition_column_type(&constraint.column, descriptor) else {
            return false;
        };
        let Some(value) =
            normalized_active_file_partition_value(active_file, &constraint.column, partition_type)
        else {
            return false;
        };

        match &constraint.kind {
            PartitionConstraintKind::AllowedValues(values) => {
                values.iter().any(|expected| expected == &value)
            }
            PartitionConstraintKind::NotNull => value.is_some(),
        }
    })
}

fn extract_partition_constraints(
    expr: &Expr,
    descriptor: &DeltaTableDescriptor,
) -> Vec<PartitionConstraint> {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            let mut constraints = extract_partition_constraints(&binary.left, descriptor);
            constraints.extend(extract_partition_constraints(&binary.right, descriptor));
            constraints
        }
        other => exact_partition_constraints(other, descriptor).unwrap_or_default(),
    }
}

fn filter_conjuncts(filters: &[Expr]) -> Vec<&Expr> {
    let mut conjuncts = Vec::new();
    for filter in filters {
        collect_filter_conjuncts(filter, &mut conjuncts);
    }
    conjuncts
}

fn collect_filter_conjuncts<'a>(expr: &'a Expr, conjuncts: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            collect_filter_conjuncts(&binary.left, conjuncts);
            collect_filter_conjuncts(&binary.right, conjuncts);
        }
        other => conjuncts.push(other),
    }
}

fn classify_filter_pushdown(
    expr: &Expr,
    descriptor: &DeltaTableDescriptor,
    exact_partition_filters_enabled: bool,
    parquet_row_group_pruning_enabled: bool,
) -> TableProviderFilterPushDown {
    let mut conjuncts = Vec::new();
    collect_filter_conjuncts(expr, &mut conjuncts);

    let mut any_usable = false;
    let mut all_exact = true;
    for conjunct in conjuncts {
        match classify_filter_conjunct(
            conjunct,
            descriptor,
            exact_partition_filters_enabled,
            parquet_row_group_pruning_enabled,
        ) {
            TableProviderFilterPushDown::Exact => {
                any_usable = true;
            }
            TableProviderFilterPushDown::Inexact => {
                any_usable = true;
                all_exact = false;
            }
            TableProviderFilterPushDown::Unsupported => {
                all_exact = false;
            }
        }
    }

    if any_usable && all_exact {
        TableProviderFilterPushDown::Exact
    } else if any_usable {
        TableProviderFilterPushDown::Inexact
    } else {
        TableProviderFilterPushDown::Unsupported
    }
}

fn classify_filter_conjunct(
    expr: &Expr,
    descriptor: &DeltaTableDescriptor,
    exact_partition_filters_enabled: bool,
    parquet_row_group_pruning_enabled: bool,
) -> TableProviderFilterPushDown {
    if exact_partition_filters_enabled && exact_partition_constraints(expr, descriptor).is_some() {
        return TableProviderFilterPushDown::Exact;
    }

    if filter_has_usable_pruning(expr, descriptor, parquet_row_group_pruning_enabled) {
        TableProviderFilterPushDown::Inexact
    } else {
        TableProviderFilterPushDown::Unsupported
    }
}

fn filter_has_usable_pruning(
    expr: &Expr,
    descriptor: &DeltaTableDescriptor,
    parquet_row_group_pruning_enabled: bool,
) -> bool {
    let Some(predicate) = row_group_pruning_predicate(expr, descriptor) else {
        return false;
    };
    parquet_row_group_pruning_enabled
        || descriptor
            .active_files
            .iter()
            .any(|file| delta_stats_i64_min_max(file, predicate.column.as_str()).is_some())
}

fn exact_partition_constraints(
    expr: &Expr,
    descriptor: &DeltaTableDescriptor,
) -> Option<Vec<PartitionConstraint>> {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            let mut constraints = exact_partition_constraints(&binary.left, descriptor)?;
            constraints.extend(exact_partition_constraints(&binary.right, descriptor)?);
            Some(constraints)
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
            let left = exact_partition_constraints(&binary.left, descriptor)?;
            let right = exact_partition_constraints(&binary.right, descriptor)?;
            merge_partition_constraint_disjunction(left, right)
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            partition_equality_constraint(&binary.left, &binary.right, descriptor)
                .or_else(|| partition_equality_constraint(&binary.right, &binary.left, descriptor))
                .map(|constraint| vec![constraint])
        }
        Expr::Column(column) => boolean_partition_constraint(&column.name, true, descriptor)
            .map(|constraint| vec![constraint]),
        Expr::Not(expr) => expr.try_as_col().and_then(|column| {
            boolean_partition_constraint(&column.name, false, descriptor)
                .map(|constraint| vec![constraint])
        }),
        Expr::IsTrue(expr) => expr.try_as_col().and_then(|column| {
            boolean_partition_constraint(&column.name, true, descriptor)
                .map(|constraint| vec![constraint])
        }),
        Expr::IsFalse(expr) => expr.try_as_col().and_then(|column| {
            boolean_partition_constraint(&column.name, false, descriptor)
                .map(|constraint| vec![constraint])
        }),
        Expr::IsNull(expr) => expr
            .try_as_col()
            .and_then(|column| partition_column_name(&column.name, descriptor))
            .and_then(|column| {
                let constraint = PartitionConstraint {
                    column,
                    kind: PartitionConstraintKind::AllowedValues(vec![None]),
                };
                partition_constraint_safe_for_descriptor(&constraint, descriptor)
                    .then_some(vec![constraint])
            }),
        Expr::IsNotNull(expr) => expr
            .try_as_col()
            .and_then(|column| partition_column_name(&column.name, descriptor))
            .and_then(|column| {
                let constraint = PartitionConstraint {
                    column,
                    kind: PartitionConstraintKind::NotNull,
                };
                partition_constraint_safe_for_descriptor(&constraint, descriptor)
                    .then_some(vec![constraint])
            }),
        Expr::InList(in_list) if !in_list.negated => {
            let column = in_list
                .expr
                .try_as_col()
                .and_then(|column| partition_column_name(&column.name, descriptor))?;
            let partition_type = partition_column_type(&column, descriptor)?;
            let values = in_list
                .list
                .iter()
                .map(|expr| scalar_partition_value_from_expr(expr, partition_type))
                .collect::<Option<Vec<_>>>()?;
            if values.iter().any(Option::is_none) {
                return None;
            }
            let constraint = PartitionConstraint {
                column,
                kind: PartitionConstraintKind::AllowedValues(values),
            };
            partition_constraint_safe_for_descriptor(&constraint, descriptor)
                .then_some(vec![constraint])
        }
        _ => None,
    }
}

fn merge_partition_constraint_disjunction(
    left: Vec<PartitionConstraint>,
    right: Vec<PartitionConstraint>,
) -> Option<Vec<PartitionConstraint>> {
    let [left] = left.as_slice() else {
        return None;
    };
    let [right] = right.as_slice() else {
        return None;
    };
    if left.column != right.column {
        return None;
    }

    let (
        PartitionConstraintKind::AllowedValues(left_values),
        PartitionConstraintKind::AllowedValues(right_values),
    ) = (&left.kind, &right.kind)
    else {
        return None;
    };

    let mut values = left_values.clone();
    for value in right_values {
        if !values.contains(value) {
            values.push(value.clone());
        }
    }
    Some(vec![PartitionConstraint {
        column: left.column.clone(),
        kind: PartitionConstraintKind::AllowedValues(values),
    }])
}

fn partition_equality_constraint(
    left: &Expr,
    right: &Expr,
    descriptor: &DeltaTableDescriptor,
) -> Option<PartitionConstraint> {
    let column = left
        .try_as_col()
        .and_then(|column| partition_column_name(&column.name, descriptor))?;
    let partition_type = partition_column_type(&column, descriptor)?;
    let value = scalar_partition_value_from_expr(right, partition_type)?;
    value.as_ref()?;
    let constraint = PartitionConstraint {
        column,
        kind: PartitionConstraintKind::AllowedValues(vec![value]),
    };
    partition_constraint_safe_for_descriptor(&constraint, descriptor).then_some(constraint)
}

fn boolean_partition_constraint(
    column_name: &str,
    value: bool,
    descriptor: &DeltaTableDescriptor,
) -> Option<PartitionConstraint> {
    let column = partition_column_name(column_name, descriptor)?;
    let partition_type = partition_column_type(&column, descriptor)?;
    if partition_type != PartitionColumnType::Boolean {
        return None;
    }
    let value =
        partition_type.normalize_partition_literal(&PartitionLiteralValue::Boolean(value))?;
    let constraint = PartitionConstraint {
        column,
        kind: PartitionConstraintKind::AllowedValues(vec![value]),
    };
    partition_constraint_safe_for_descriptor(&constraint, descriptor).then_some(constraint)
}

fn partition_column_name(column_name: &str, descriptor: &DeltaTableDescriptor) -> Option<String> {
    descriptor
        .partition_columns
        .iter()
        .find(|partition_column| partition_column.as_str() == column_name)
        .cloned()
}

fn partition_column_type(
    column_name: &str,
    descriptor: &DeltaTableDescriptor,
) -> Option<PartitionColumnType> {
    let normalized_column = normalize_partition_column_key(column_name);
    let mut matches = descriptor
        .partition_column_types
        .iter()
        .filter(|(name, _partition_type)| normalize_partition_column_key(name) == normalized_column)
        .map(|(_name, partition_type)| *partition_type);
    let partition_type = matches.next()?;
    matches.next().is_none().then_some(partition_type)
}

fn partition_constraint_safe_for_descriptor(
    constraint: &PartitionConstraint,
    descriptor: &DeltaTableDescriptor,
) -> bool {
    let Some(partition_type) = partition_column_type(&constraint.column, descriptor) else {
        return false;
    };

    descriptor.active_files.iter().all(|file| {
        normalized_active_file_partition_value(file, &constraint.column, partition_type).is_some()
    })
}

fn normalized_active_file_partition_value(
    active_file: &DeltaActiveFile,
    column: &str,
    partition_type: PartitionColumnType,
) -> Option<Option<String>> {
    let normalized_column = normalize_partition_column_key(column);
    let mut values = active_file
        .partition_values
        .iter()
        .filter(|(key, _value)| normalize_partition_column_key(key) == normalized_column)
        .map(|(_key, value)| value.as_deref());
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    partition_type.normalize_partition_value(value)
}

fn normalize_partition_column_key(name: &str) -> String {
    name.to_ascii_lowercase()
}

fn scalar_partition_value_from_expr(
    expr: &Expr,
    partition_type: PartitionColumnType,
) -> Option<Option<String>> {
    match expr {
        Expr::Literal(value, _) => scalar_partition_value(value, partition_type),
        _ => None,
    }
}

fn scalar_partition_value(
    value: &ScalarValue,
    partition_type: PartitionColumnType,
) -> Option<Option<String>> {
    let literal = scalar_partition_literal(value)?;
    partition_type.normalize_partition_literal(&literal)
}

fn scalar_partition_literal(value: &ScalarValue) -> Option<PartitionLiteralValue> {
    match value {
        ScalarValue::Null => Some(PartitionLiteralValue::Null),
        ScalarValue::Utf8(None) | ScalarValue::Utf8View(None) | ScalarValue::LargeUtf8(None) => {
            Some(PartitionLiteralValue::Null)
        }
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Some(PartitionLiteralValue::String(value.clone())),
        ScalarValue::Boolean(value) => {
            Some(value.map_or(PartitionLiteralValue::Null, PartitionLiteralValue::Boolean))
        }
        ScalarValue::Int8(value) => Some(value.map_or(PartitionLiteralValue::Null, |value| {
            PartitionLiteralValue::Int64(i64::from(value))
        })),
        ScalarValue::Int16(value) => Some(value.map_or(PartitionLiteralValue::Null, |value| {
            PartitionLiteralValue::Int64(i64::from(value))
        })),
        ScalarValue::Int32(value) => Some(value.map_or(PartitionLiteralValue::Null, |value| {
            PartitionLiteralValue::Int64(i64::from(value))
        })),
        ScalarValue::Int64(value) => {
            Some(value.map_or(PartitionLiteralValue::Null, PartitionLiteralValue::Int64))
        }
        ScalarValue::UInt8(value) => Some(value.map_or(PartitionLiteralValue::Null, |value| {
            PartitionLiteralValue::Int64(i64::from(value))
        })),
        ScalarValue::UInt16(value) => Some(value.map_or(PartitionLiteralValue::Null, |value| {
            PartitionLiteralValue::Int64(i64::from(value))
        })),
        ScalarValue::UInt32(value) => Some(value.map_or(PartitionLiteralValue::Null, |value| {
            PartitionLiteralValue::Int64(i64::from(value))
        })),
        ScalarValue::UInt64(value) => Some(match value {
            Some(value) => PartitionLiteralValue::Int64(i64::try_from(*value).ok()?),
            None => PartitionLiteralValue::Null,
        }),
        _ => None,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FileStatsConstraint {
    column: String,
    comparison: ParquetIntegerComparison,
}

fn file_stats_pruning_constraint(
    expr: &Expr,
    descriptor: &DeltaTableDescriptor,
) -> Option<FileStatsConstraint> {
    match row_group_pruning_predicate(expr, descriptor) {
        Some(predicate) => Some(FileStatsConstraint {
            column: predicate.column,
            comparison: predicate.comparison,
        }),
        None => None,
    }
}

fn file_stats_match(active_file: &DeltaActiveFile, constraints: &[FileStatsConstraint]) -> bool {
    constraints.iter().all(|constraint| {
        let Some((min_value, max_value)) =
            delta_stats_i64_min_max(active_file, constraint.column.as_str())
        else {
            return true;
        };
        integer_range_can_match(min_value, max_value, &constraint.comparison)
    })
}

fn delta_stats_i64_min_max(active_file: &DeltaActiveFile, column: &str) -> Option<(i64, i64)> {
    let stats_json = active_file.stats_json.as_ref()?;
    let stats: serde_json::Value = serde_json::from_str(stats_json).ok()?;
    let min_value = stats.get("minValues")?.get(column)?.as_i64()?;
    let max_value = stats.get("maxValues")?.get(column)?.as_i64()?;
    Some((min_value, max_value))
}

fn integer_range_can_match(
    min_value: i64,
    max_value: i64,
    comparison: &ParquetIntegerComparison,
) -> bool {
    match comparison {
        ParquetIntegerComparison::Eq(value) => min_value <= *value && *value <= max_value,
        ParquetIntegerComparison::Gt(value) => max_value > *value,
        ParquetIntegerComparison::Gte(value) => max_value >= *value,
        ParquetIntegerComparison::Lt(value) => min_value < *value,
        ParquetIntegerComparison::Lte(value) => min_value <= *value,
    }
}

fn row_group_pruning_predicate(
    expr: &Expr,
    descriptor: &DeltaTableDescriptor,
) -> Option<ParquetRowGroupPruningPredicate> {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            row_group_pruning_predicate(&binary.left, descriptor)
                .or_else(|| row_group_pruning_predicate(&binary.right, descriptor))
        }
        Expr::BinaryExpr(binary) => {
            integer_comparison_predicate(&binary.left, binary.op, &binary.right, descriptor)
                .or_else(|| {
                    reverse_operator(binary.op).and_then(|op| {
                        integer_comparison_predicate(&binary.right, op, &binary.left, descriptor)
                    })
                })
        }
        _ => None,
    }
}

fn integer_comparison_predicate(
    left: &Expr,
    op: Operator,
    right: &Expr,
    descriptor: &DeltaTableDescriptor,
) -> Option<ParquetRowGroupPruningPredicate> {
    let column = left.try_as_col()?;
    if descriptor
        .partition_columns
        .iter()
        .any(|partition_column| partition_column == &column.name)
    {
        return None;
    }
    let value = scalar_i64_from_expr(right)?;
    let comparison = match op {
        Operator::Eq => ParquetIntegerComparison::Eq(value),
        Operator::Gt => ParquetIntegerComparison::Gt(value),
        Operator::GtEq => ParquetIntegerComparison::Gte(value),
        Operator::Lt => ParquetIntegerComparison::Lt(value),
        Operator::LtEq => ParquetIntegerComparison::Lte(value),
        _ => return None,
    };
    Some(ParquetRowGroupPruningPredicate {
        column: column.name.clone(),
        comparison,
    })
}

fn reverse_operator(op: Operator) -> Option<Operator> {
    match op {
        Operator::Eq => Some(Operator::Eq),
        Operator::Gt => Some(Operator::Lt),
        Operator::GtEq => Some(Operator::LtEq),
        Operator::Lt => Some(Operator::Gt),
        Operator::LtEq => Some(Operator::GtEq),
        _ => None,
    }
}

fn scalar_i64_from_expr(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(ScalarValue::Int8(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::Int16(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::Int32(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::Int64(Some(value)), _) => Some(*value),
        Expr::Literal(ScalarValue::UInt8(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::UInt16(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::UInt32(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::UInt64(Some(value)), _) => i64::try_from(*value).ok(),
        _ => None,
    }
}

fn record_scan_rows_emitted(metrics: &Arc<Mutex<AxonParquetScanMetrics>>, rows: usize) {
    let rows = u64::try_from(rows).unwrap_or(u64::MAX);
    let mut metrics = metrics
        .lock()
        .expect("AxonParquetScanExec metrics should not be poisoned");
    metrics.rows_emitted = metrics.rows_emitted.saturating_add(rows);
}

fn record_scan_parquet_metrics(
    metrics: &Arc<Mutex<AxonParquetScanMetrics>>,
    snapshot: ScanTargetMetricsSnapshot,
) {
    let mut scan_metrics = metrics
        .lock()
        .expect("AxonParquetScanExec metrics should not be poisoned");
    scan_metrics.row_groups_touched = snapshot.row_groups_touched;
    scan_metrics.row_groups_skipped = snapshot.row_groups_skipped;
    scan_metrics.bytes_fetched = snapshot.bytes_fetched;
    scan_metrics.range_read_metrics = snapshot.range_read_metrics;
}

fn add_scan_metrics(
    left: &ScanTargetMetricsSnapshot,
    right: &ScanTargetMetricsSnapshot,
) -> ScanTargetMetricsSnapshot {
    ScanTargetMetricsSnapshot {
        files_touched: left.files_touched.saturating_add(right.files_touched),
        files_skipped: left.files_skipped.saturating_add(right.files_skipped),
        row_groups_touched: left
            .row_groups_touched
            .saturating_add(right.row_groups_touched),
        row_groups_skipped: left
            .row_groups_skipped
            .saturating_add(right.row_groups_skipped),
        rows_emitted: left.rows_emitted.saturating_add(right.rows_emitted),
        bytes_fetched: left.bytes_fetched.saturating_add(right.bytes_fetched),
        footer_reads: left.footer_reads.saturating_add(right.footer_reads),
        metadata_probe_round_trips: left
            .metadata_probe_round_trips
            .saturating_add(right.metadata_probe_round_trips),
        range_read_metrics: merge_parquet_range_read_metrics(
            &left.range_read_metrics,
            &right.range_read_metrics,
        ),
    }
}

fn validate_scan_budget_for_metrics(
    metrics: &ScanTargetMetricsSnapshot,
    query_budget: BrowserQueryBudget,
) -> Result<(), QueryError> {
    validate_query_budget_value_option_at(
        "max_scan_bytes",
        metrics.bytes_fetched,
        query_budget.max_scan_bytes,
        "scan stream",
    )
}

fn take_scan_rows(limit_remaining: &Arc<Mutex<Option<usize>>>, available_rows: usize) -> usize {
    let mut limit_remaining = limit_remaining
        .lock()
        .expect("AxonParquetScanExec limit should not be poisoned");
    let Some(remaining) = limit_remaining.as_mut() else {
        return available_rows;
    };
    let rows_to_emit = available_rows.min(*remaining);
    *remaining -= rows_to_emit;
    rows_to_emit
}

fn scan_limit_exhausted(limit_remaining: &Arc<Mutex<Option<usize>>>) -> bool {
    let limit_remaining = limit_remaining
        .lock()
        .expect("AxonParquetScanExec limit should not be poisoned");
    matches!(*limit_remaining, Some(0))
}

fn slice_record_batch(batch: RecordBatch, rows: usize) -> RecordBatch {
    if rows == batch.num_rows() {
        batch
    } else {
        batch.slice(0, rows)
    }
}

struct PlannedScanTargetBatchStream {
    batches: BoxStream<'static, Result<RecordBatch, QueryError>>,
}

struct PlannedScanTargetsState {
    reader: HttpRangeReader,
    descriptor: Arc<DeltaTableDescriptor>,
    planned_file_indices: Vec<usize>,
    next_target_index: usize,
    current_batches: Option<BoxStream<'static, Result<RecordBatch, QueryError>>>,
    current_metrics: Option<Arc<dyn ScanTargetMetricsHandle + Send + Sync>>,
    completed_metrics: ScanTargetMetricsSnapshot,
    required_columns: Vec<String>,
    partition_column_types: BTreeMap<String, PartitionColumnType>,
    row_group_predicate: Option<ParquetRowGroupPruningPredicate>,
    metrics: Arc<Mutex<AxonParquetScanMetrics>>,
    limit_remaining: Arc<Mutex<Option<usize>>>,
    query_budget: BrowserQueryBudget,
    cancellation: BrowserQueryCancellation,
    metadata_cache: ParquetMetadataCache,
    range_cache: ParquetRangeCache,
    query_context: Option<ParquetRangeQueryContext>,
}

async fn stream_planned_scan_targets(
    reader: HttpRangeReader,
    descriptor: Arc<DeltaTableDescriptor>,
    planned_file_indices: Vec<usize>,
    required_columns: Vec<String>,
    partition_column_types: BTreeMap<String, PartitionColumnType>,
    row_group_predicate: Option<ParquetRowGroupPruningPredicate>,
    metrics: Arc<Mutex<AxonParquetScanMetrics>>,
    limit_remaining: Arc<Mutex<Option<usize>>>,
    query_budget: BrowserQueryBudget,
    cancellation: BrowserQueryCancellation,
    metadata_cache: ParquetMetadataCache,
    range_cache: ParquetRangeCache,
    query_context: Option<ParquetRangeQueryContext>,
) -> Result<PlannedScanTargetBatchStream, QueryError> {
    let state = PlannedScanTargetsState {
        reader,
        descriptor,
        planned_file_indices,
        next_target_index: 0,
        current_batches: None,
        current_metrics: None,
        completed_metrics: ScanTargetMetricsSnapshot::default(),
        required_columns,
        partition_column_types,
        row_group_predicate,
        metrics,
        limit_remaining,
        query_budget,
        cancellation,
        metadata_cache,
        range_cache,
        query_context,
    };
    let batches = stream::try_unfold(state, |mut state| async move {
        loop {
            state.cancellation.check_cancelled_at("scan stream")?;
            if scan_limit_exhausted(&state.limit_remaining) {
                return Ok(None);
            }

            if let Some(current_batches) = &mut state.current_batches {
                match current_batches.next().await {
                    Some(batch) => {
                        let batch = batch?;
                        if let Some(metrics) = &state.current_metrics {
                            let metrics_snapshot =
                                add_scan_metrics(&state.completed_metrics, &metrics.snapshot());
                            record_scan_parquet_metrics(&state.metrics, metrics_snapshot.clone());
                            validate_scan_budget_for_metrics(
                                &metrics_snapshot,
                                state.query_budget,
                            )?;
                        }
                        let rows_to_emit = take_scan_rows(&state.limit_remaining, batch.num_rows());
                        if rows_to_emit == 0 {
                            continue;
                        }
                        let batch = slice_record_batch(batch, rows_to_emit);
                        record_scan_rows_emitted(&state.metrics, batch.num_rows());
                        return Ok(Some((batch, state)));
                    }
                    None => {
                        if let Some(metrics) = &state.current_metrics {
                            state.completed_metrics =
                                add_scan_metrics(&state.completed_metrics, &metrics.snapshot());
                            record_scan_parquet_metrics(
                                &state.metrics,
                                state.completed_metrics.clone(),
                            );
                            validate_scan_budget_for_metrics(
                                &state.completed_metrics,
                                state.query_budget,
                            )?;
                        }
                        state.current_batches = None;
                        state.current_metrics = None;
                    }
                }
            }

            let Some(file_index) = state
                .planned_file_indices
                .get(state.next_target_index)
                .copied()
            else {
                return Ok(None);
            };
            state.next_target_index += 1;
            state.cancellation.check_cancelled_at("scan stream")?;
            let target = scan_target_for_planned_index(state.descriptor.as_ref(), file_index)?;

            let scan = stream_scan_target_batches_with_row_group_pruning_caches_and_query(
                &state.reader,
                &target,
                &state.required_columns,
                &state.partition_column_types,
                None,
                state.row_group_predicate.as_ref(),
                Some(&state.metadata_cache),
                Some(&state.range_cache),
                state.query_context.as_ref(),
            )
            .await?;
            state.cancellation.check_cancelled_at("scan stream")?;
            let metrics_snapshot =
                add_scan_metrics(&state.completed_metrics, &scan.metrics.snapshot());
            record_scan_parquet_metrics(&state.metrics, metrics_snapshot.clone());
            validate_scan_budget_for_metrics(&metrics_snapshot, state.query_budget)?;
            state.current_metrics = Some(scan.metrics);
            state.current_batches = Some(scan.batches);
        }
    })
    .boxed();

    Ok(PlannedScanTargetBatchStream { batches })
}

fn scan_target_for_planned_index(
    descriptor: &DeltaTableDescriptor,
    file_index: usize,
) -> Result<ScanTarget, QueryError> {
    let active_file = descriptor.active_files.get(file_index).ok_or_else(|| {
        QueryError::new(
            QueryErrorCode::ExecutionFailed,
            format!("browser DataFusion planned file index {file_index} is out of range"),
            runtime_target(),
        )
    })?;

    delta_active_file_to_scan_target(active_file).map_err(map_datafusion_error)
}

impl DisplayAs for AxonParquetScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let trace = self.pushdown_trace();
                write!(
                    f,
                    "AxonParquetScanExec: table={}, active_files={}, partitions={}, projected_columns={:?}, filters={:?}, limit={:?}, planned_scan_bytes={}, files_skipped={}, row_groups_skipped={}, bytes_fetched={}, rows_emitted={}",
                    self.descriptor.table_name,
                    self.descriptor.active_files.len(),
                    self.partition_count(),
                    trace.projected_columns,
                    trace.filters,
                    self.limit,
                    trace.planned_scan_bytes,
                    trace.files_skipped,
                    trace.row_groups_skipped,
                    trace.bytes_fetched,
                    trace.rows_emitted
                )
            }
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

    fn properties(&self) -> &Arc<PlanProperties> {
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
        self.cancellation
            .check_cancelled_at("scan execution")
            .map_err(datafusion_error_from_query_error)?;
        if let Some(max_scan_bytes) = self.query_budget.max_scan_bytes {
            validate_query_budget_value_at(
                "max_scan_bytes",
                self.planned_scan_bytes(),
                max_scan_bytes,
                "planned scan",
            )
            .map_err(datafusion_error_from_query_error)?;
        }
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
            "browser DataFusion scan cannot execute Delta deletion vectors".to_string(),
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
    datafusion_error_from_query_error(error)
}

#[derive(Debug)]
struct WrappedQueryError(QueryError);

impl fmt::Display for WrappedQueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.0.code, self.0.message)
    }
}

impl StdError for WrappedQueryError {}

fn datafusion_error_from_query_error(error: QueryError) -> DataFusionError {
    DataFusionError::External(Box::new(WrappedQueryError(error)))
}

fn query_error_from_datafusion_error(error: &DataFusionError) -> Option<QueryError> {
    match error {
        DataFusionError::External(external) => external
            .downcast_ref::<WrappedQueryError>()
            .map(|wrapped| wrapped.0.clone())
            .or_else(|| query_error_from_std_error(external.as_ref())),
        DataFusionError::Context(_, source) | DataFusionError::Diagnostic(_, source) => {
            query_error_from_datafusion_error(source)
        }
        _ => None,
    }
}

fn query_error_from_arrow_error(error: &ArrowError) -> Option<QueryError> {
    match error {
        ArrowError::ExternalError(external) => external
            .downcast_ref::<WrappedQueryError>()
            .map(|wrapped| wrapped.0.clone())
            .or_else(|| query_error_from_std_error(external.as_ref())),
        ArrowError::IoError(_, source) => source
            .get_ref()
            .and_then(|source| {
                source
                    .downcast_ref::<WrappedQueryError>()
                    .map(|wrapped| wrapped.0.clone())
                    .or_else(|| query_error_from_std_error(source))
            })
            .or_else(|| query_error_from_std_error(source)),
        _ => None,
    }
}

fn query_error_from_std_error(error: &(dyn StdError + 'static)) -> Option<QueryError> {
    if let Some(wrapped) = error.downcast_ref::<WrappedQueryError>() {
        return Some(wrapped.0.clone());
    }
    error.source().and_then(query_error_from_std_error)
}

fn query_cancelled_error(point: &str) -> QueryError {
    QueryError::new(
        QueryErrorCode::ExecutionFailed,
        format!("experimental browser DataFusion query cancelled during {point}"),
        runtime_target(),
    )
}

fn is_query_cancelled_error(error: &QueryError) -> bool {
    error.code == QueryErrorCode::ExecutionFailed
        && error
            .message
            .starts_with("experimental browser DataFusion query cancelled")
}

fn query_budget_exceeded_error_at(
    budget_name: &str,
    observed: u64,
    max_allowed: u64,
    point: &str,
) -> QueryError {
    QueryError::new(
        QueryErrorCode::FallbackRequired,
        format!(
            "experimental browser DataFusion query exceeded {budget_name} budget during {point} ({observed} > {max_allowed})"
        ),
        runtime_target(),
    )
    .with_fallback_reason(FallbackReason::BrowserRuntimeConstraint)
}

fn validate_query_budget_value_at(
    budget_name: &str,
    observed: u64,
    max_allowed: u64,
    point: &str,
) -> Result<(), QueryError> {
    if observed > max_allowed {
        Err(query_budget_exceeded_error_at(
            budget_name,
            observed,
            max_allowed,
            point,
        ))
    } else {
        Ok(())
    }
}

fn validate_query_budget_value_option_at(
    budget_name: &str,
    observed: u64,
    max_allowed: Option<u64>,
    point: &str,
) -> Result<(), QueryError> {
    if let Some(max_allowed) = max_allowed {
        validate_query_budget_value_at(budget_name, observed, max_allowed, point)
    } else {
        Ok(())
    }
}

fn validate_query_budget_for_plan(
    plan: &Arc<dyn ExecutionPlan>,
    query_budget: BrowserQueryBudget,
) -> Result<(), QueryError> {
    if query_budget.max_scan_bytes.is_none() {
        return Ok(());
    }

    let planned_scan_bytes = datafusion_planned_scan_bytes_from_plan(plan)?;
    validate_query_budget_value_option_at(
        "max_scan_bytes",
        planned_scan_bytes,
        query_budget.max_scan_bytes,
        "planned scan",
    )
}

fn datafusion_planned_scan_bytes_from_plan(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<u64, QueryError> {
    let mut planned_scan_bytes = 0_u64;
    collect_datafusion_planned_scan_bytes(plan, &mut planned_scan_bytes)?;
    Ok(planned_scan_bytes)
}

fn collect_datafusion_planned_scan_bytes(
    plan: &Arc<dyn ExecutionPlan>,
    planned_scan_bytes: &mut u64,
) -> Result<(), QueryError> {
    if let Some(scan) = plan.as_any().downcast_ref::<AxonParquetScanExec>() {
        *planned_scan_bytes = checked_datafusion_metric_add(
            *planned_scan_bytes,
            scan.planned_scan_bytes(),
            "planned scan bytes",
        )?;
    }

    for child in plan.children() {
        collect_datafusion_planned_scan_bytes(child, planned_scan_bytes)?;
    }

    Ok(())
}

async fn collect_record_batches_with_controls(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
    query_budget: BrowserQueryBudget,
    cancellation: &BrowserQueryCancellation,
) -> Result<Vec<RecordBatch>, QueryError> {
    if matches!(query_budget.max_batches_in_flight, Some(0)) {
        return Err(query_budget_exceeded_error_at(
            "max_batches_in_flight",
            1,
            0,
            "record batch output",
        ));
    }

    cancellation.check_cancelled_at("record batch output")?;
    let mut stream = execute_stream(plan, task_ctx).map_err(map_datafusion_error)?;
    let mut batches = Vec::new();
    let mut rows_returned = 0_u64;

    while let Some(batch) = stream.next().await {
        cancellation.check_cancelled_at("record batch output")?;
        let batch = batch.map_err(map_datafusion_error)?;
        let next_batch_count = batches.len().checked_add(1).ok_or_else(|| {
            QueryError::new(
                QueryErrorCode::ExecutionFailed,
                "experimental browser DataFusion output batch counts overflowed usize",
                runtime_target(),
            )
        })?;
        validate_query_budget_value_option_at(
            "max_batches_in_flight",
            datafusion_metric_from_usize(next_batch_count, "output batch count")?,
            query_budget
                .max_batches_in_flight
                .map(|max| u64::try_from(max).unwrap_or(u64::MAX)),
            "record batch output",
        )?;

        let batch_rows = u64::try_from(batch.num_rows()).map_err(|_| {
            QueryError::new(
                QueryErrorCode::ExecutionFailed,
                "experimental browser DataFusion row counts overflowed u64",
                runtime_target(),
            )
        })?;
        let next_rows_returned = rows_returned.checked_add(batch_rows).ok_or_else(|| {
            QueryError::new(
                QueryErrorCode::ExecutionFailed,
                "experimental browser DataFusion row totals overflowed u64",
                runtime_target(),
            )
        })?;
        validate_query_budget_value_option_at(
            "max_rows_returned",
            next_rows_returned,
            query_budget.max_rows_returned,
            "record batch output",
        )?;

        rows_returned = next_rows_returned;
        batches.push(batch);
        cancellation.check_cancelled_at("record batch output")?;
    }

    Ok(batches)
}

#[derive(Clone)]
pub struct WasmDataFusionEngine {
    ctx: SessionContext,
    query_budget: BrowserQueryBudget,
    cancellation: BrowserQueryCancellation,
    metadata_cache: ParquetMetadataCache,
    range_cache: ParquetRangeCache,
}

impl fmt::Debug for WasmDataFusionEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WasmDataFusionEngine")
            .finish_non_exhaustive()
    }
}

impl WasmDataFusionEngine {
    pub fn new() -> Self {
        Self::with_budget(BrowserQueryBudget::default())
    }

    pub fn with_budget(query_budget: BrowserQueryBudget) -> Self {
        Self::with_budget_and_cancellation(query_budget, BrowserQueryCancellation::default())
    }

    pub fn with_budget_and_cancellation(
        query_budget: BrowserQueryBudget,
        cancellation: BrowserQueryCancellation,
    ) -> Self {
        Self::with_budget_cancellation_and_metadata_cache(
            query_budget,
            cancellation,
            ParquetMetadataCache::default(),
        )
    }

    pub fn with_budget_cancellation_and_metadata_cache(
        query_budget: BrowserQueryBudget,
        cancellation: BrowserQueryCancellation,
        metadata_cache: ParquetMetadataCache,
    ) -> Self {
        Self::with_budget_cancellation_and_caches(
            query_budget,
            cancellation,
            metadata_cache,
            ParquetRangeCache::default(),
        )
    }

    pub fn with_budget_cancellation_and_caches(
        query_budget: BrowserQueryBudget,
        cancellation: BrowserQueryCancellation,
        metadata_cache: ParquetMetadataCache,
        range_cache: ParquetRangeCache,
    ) -> Self {
        Self {
            ctx: SessionContext::new(),
            query_budget,
            cancellation,
            metadata_cache,
            range_cache,
        }
    }

    pub fn query_budget(&self) -> BrowserQueryBudget {
        self.query_budget
    }

    pub fn set_query_budget(&mut self, query_budget: BrowserQueryBudget) -> BrowserQueryBudget {
        let previous = self.query_budget;
        self.query_budget = query_budget;
        previous
    }

    pub fn cancellation_token(&self) -> BrowserQueryCancellation {
        self.cancellation.clone()
    }

    pub fn cancel_running_queries(&self) {
        self.cancellation.cancel();
    }

    pub async fn open_delta_table(
        &mut self,
        descriptor: DeltaTableDescriptor,
    ) -> Result<(), QueryError> {
        let table_name = descriptor.table_name.clone();
        self.ctx
            .register_table(
                table_name,
                Arc::new(AxonDeltaTableProvider::with_query_controls_and_caches(
                    descriptor,
                    Vec::new(),
                    self.query_budget,
                    self.cancellation.clone(),
                    self.metadata_cache.clone(),
                    self.range_cache.clone(),
                )),
            )
            .map_err(map_datafusion_error)?;

        Ok(())
    }

    pub fn deregister_table(&mut self, table_name: &str) -> Result<bool, QueryError> {
        self.ctx
            .deregister_table(table_name)
            .map(|removed| removed.is_some())
            .map_err(map_datafusion_error)
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
                Arc::new(AxonDeltaTableProvider::with_query_controls_and_caches(
                    descriptor,
                    partitions,
                    self.query_budget,
                    self.cancellation.clone(),
                    self.metadata_cache.clone(),
                    self.range_cache.clone(),
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
        let (schema, batches, _scan_metrics) = self.sql_to_record_batches_with_metrics(sql).await?;
        Ok((schema, batches))
    }

    pub async fn sql_to_record_batches_with_metrics(
        &self,
        sql: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>, DataFusionScanMetricsSummary), QueryError> {
        self.sql_to_record_batches_with_metrics_inner(sql).await
    }

    async fn sql_to_record_batches_with_metrics_inner(
        &self,
        sql: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>, DataFusionScanMetricsSummary), QueryError> {
        let cancellation = self.cancellation.snapshot();
        cancellation.check_cancelled_at("SQL planning")?;
        let (query_ctx, query_context) = self.begin_query_session(&cancellation);
        let frame = query_ctx.sql(sql).await.map_err(map_datafusion_error)?;
        cancellation.check_cancelled_at("SQL planning")?;
        let output_schema = frame.schema().inner().clone();
        let task_ctx = query_ctx.task_ctx();
        let plan = frame
            .create_physical_plan()
            .await
            .map_err(map_datafusion_error)?;
        validate_query_budget_for_plan(&plan, self.query_budget)?;
        cancellation.check_cancelled_at("record batch output")?;
        let batches = collect_record_batches_with_controls(
            Arc::clone(&plan),
            task_ctx,
            self.query_budget,
            &cancellation,
        )
        .await?;
        cancellation.check_cancelled_at("record batch output")?;
        let mut scan_metrics = datafusion_scan_metrics_from_plan(&plan)?;
        scan_metrics.range_read_metrics = merge_parquet_range_read_metrics(
            &scan_metrics.range_read_metrics,
            &query_context.finalize()?,
        );
        validate_query_budget_value_option_at(
            "max_scan_bytes",
            scan_metrics.bytes_fetched,
            self.query_budget.max_scan_bytes,
            "scan stream",
        )?;

        Ok((output_schema, batches, scan_metrics))
    }

    pub async fn sql_to_arrow_ipc(&self, sql: &str) -> Result<Vec<u8>, QueryError> {
        Ok(self.sql_to_arrow_ipc_result(sql).await?.ipc_bytes)
    }

    pub async fn sql_to_arrow_ipc_result(
        &self,
        sql: &str,
    ) -> Result<DataFusionArrowIpcResult, QueryError> {
        self.sql_to_arrow_ipc_result_inner(sql, false).await
    }

    pub async fn sql_to_arrow_ipc_result_with_physical_plan(
        &self,
        sql: &str,
    ) -> Result<DataFusionArrowIpcResult, QueryError> {
        self.sql_to_arrow_ipc_result_inner(sql, true).await
    }

    async fn sql_to_arrow_ipc_result_inner(
        &self,
        sql: &str,
        include_physical_plan: bool,
    ) -> Result<DataFusionArrowIpcResult, QueryError> {
        let max_pending_encoded_batch_bytes = self
            .query_budget
            .max_output_ipc_bytes
            .and_then(|max| usize::try_from(max).ok())
            .map(|max| max.min(ipc_cursor::DEFAULT_MAX_PENDING_ENCODED_BATCH_BYTES))
            .unwrap_or(ipc_cursor::DEFAULT_MAX_PENDING_ENCODED_BATCH_BYTES);
        let mut cursor = self
            .start_arrow_ipc_cursor(
                sql,
                IpcStreamLimits {
                    max_transport_chunk_bytes: ipc_cursor::DEFAULT_IPC_TRANSPORT_CHUNK_BYTES,
                    max_pending_encoded_batch_bytes,
                    max_total_encoded_bytes: self.query_budget.max_output_ipc_bytes,
                    preview_rows: 0,
                    max_preview_string_bytes: None,
                },
                include_physical_plan,
            )
            .await?;
        let mut ipc_bytes = Vec::new();

        while let Some(item) = cursor.next().await.map_err(|error| {
            QueryError::new(
                QueryErrorCode::ExecutionFailed,
                format!("experimental browser DataFusion Arrow IPC cursor fault: {error}"),
                runtime_target(),
            )
        })? {
            match item {
                IpcCursorItem::Chunk(chunk) => {
                    ipc_bytes
                        .len()
                        .checked_add(chunk.bytes.len())
                        .ok_or_else(|| {
                            QueryError::new(
                                QueryErrorCode::ExecutionFailed,
                                "experimental browser DataFusion Arrow IPC byte lengths overflowed usize",
                                runtime_target(),
                            )
                        })?;
                    ipc_bytes.try_reserve(chunk.bytes.len()).map_err(|_| {
                        QueryError::new(
                            QueryErrorCode::ExecutionFailed,
                            "experimental browser DataFusion could not reserve Arrow IPC result bytes",
                            runtime_target(),
                        )
                    })?;
                    ipc_bytes.extend_from_slice(&chunk.bytes);
                }
                IpcCursorItem::Terminal(terminal) => {
                    if terminal.status != QueryTerminalStatus::Succeeded {
                        return Err(terminal.error.unwrap_or_else(|| {
                            QueryError::new(
                                QueryErrorCode::ExecutionFailed,
                                "experimental browser DataFusion Arrow IPC cursor stopped without an error",
                                runtime_target(),
                            )
                        }));
                    }
                    let scan_metrics = terminal.scan_metrics.ok_or_else(|| {
                        QueryError::new(
                            QueryErrorCode::ExecutionFailed,
                            "experimental browser DataFusion Arrow IPC cursor succeeded without scan metrics",
                            runtime_target(),
                        )
                    })?;
                    return Ok(DataFusionArrowIpcResult {
                        ipc_bytes,
                        row_count: terminal.row_count,
                        encoded_bytes: terminal.encoded_bytes,
                        scan_metrics,
                        physical_plan: terminal.physical_plan,
                    });
                }
            }
        }

        Err(QueryError::new(
            QueryErrorCode::ExecutionFailed,
            "experimental browser DataFusion Arrow IPC cursor closed before a terminal outcome",
            runtime_target(),
        ))
    }

    fn begin_query_session(
        &self,
        cancellation: &BrowserQueryCancellation,
    ) -> (SessionContext, ParquetRangeQueryContext) {
        let query_context = self.range_cache.begin_query();
        let mut state = self.ctx.state();
        state
            .config_mut()
            .set_extension(Arc::new(query_context.clone()));
        state
            .config_mut()
            .set_extension(Arc::new(cancellation.clone()));
        (SessionContext::new_with_state(state), query_context)
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
    let cancellation = BrowserQueryCancellation::default();
    encode_record_batches_to_arrow_ipc_with_controls(
        schema,
        batches,
        BrowserQueryBudget::default(),
        &cancellation,
    )
}

fn encode_record_batches_to_arrow_ipc_with_controls(
    schema: SchemaRef,
    batches: &[RecordBatch],
    query_budget: BrowserQueryBudget,
    cancellation: &BrowserQueryCancellation,
) -> Result<Vec<u8>, QueryError> {
    cancellation.check_cancelled_at("Arrow IPC output")?;
    let buffer = BudgetedArrowIpcBuffer::new(query_budget);
    let mut writer = StreamWriter::try_new(buffer, schema.as_ref()).map_err(|error| {
        map_arrow_ipc_error(error, |error| {
            format!("experimental browser DataFusion could not open Arrow IPC writer: {error}")
        })
    })?;

    for batch in batches {
        cancellation.check_cancelled_at("Arrow IPC output")?;
        writer.write(batch).map_err(|error| {
            map_arrow_ipc_error(error, |error| {
                format!("experimental browser DataFusion could not encode Arrow IPC batch: {error}")
            })
        })?;
        cancellation.check_cancelled_at("Arrow IPC batch encoding")?;
    }
    cancellation.check_cancelled_at("Arrow IPC output")?;
    writer.finish().map_err(|error| {
        map_arrow_ipc_error(error, |error| {
            format!("experimental browser DataFusion could not finish Arrow IPC stream: {error}")
        })
    })?;
    cancellation.check_cancelled_at("Arrow IPC output")?;

    let buffer = writer.into_inner().map_err(|error| {
        map_arrow_ipc_error(error, |error| {
            format!("experimental browser DataFusion could not close Arrow IPC stream: {error}")
        })
    })?;
    Ok(buffer.into_inner())
}

#[derive(Debug)]
struct BudgetedArrowIpcBuffer {
    bytes: Vec<u8>,
    query_budget: BrowserQueryBudget,
}

impl BudgetedArrowIpcBuffer {
    fn new(query_budget: BrowserQueryBudget) -> Self {
        Self {
            bytes: Vec::new(),
            query_budget,
        }
    }

    fn into_inner(self) -> Vec<u8> {
        self.bytes
    }

    fn validate_next_len(&self, additional: usize) -> io::Result<()> {
        let next_len = self.bytes.len().checked_add(additional).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Other,
                WrappedQueryError(QueryError::new(
                    QueryErrorCode::ExecutionFailed,
                    "experimental browser DataFusion Arrow IPC byte lengths overflowed usize",
                    runtime_target(),
                )),
            )
        })?;
        let observed = u64::try_from(next_len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                WrappedQueryError(QueryError::new(
                    QueryErrorCode::ExecutionFailed,
                    "experimental browser DataFusion Arrow IPC byte lengths overflowed u64",
                    runtime_target(),
                )),
            )
        })?;
        validate_query_budget_value_option_at(
            "max_output_ipc_bytes",
            observed,
            self.query_budget.max_output_ipc_bytes,
            "Arrow IPC output",
        )
        .map_err(|error| io::Error::new(io::ErrorKind::Other, WrappedQueryError(error)))
    }
}

impl Write for BudgetedArrowIpcBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.validate_next_len(buf.len())?;
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn map_arrow_ipc_error(
    error: ArrowError,
    context: impl FnOnce(&ArrowError) -> String,
) -> QueryError {
    if let Some(query_error) = query_error_from_arrow_error(&error) {
        return query_error;
    }

    QueryError::new(
        QueryErrorCode::ExecutionFailed,
        context(&error),
        runtime_target(),
    )
}

fn datafusion_scan_metrics_from_plan(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<DataFusionScanMetricsSummary, QueryError> {
    let mut metrics = DataFusionScanMetricsSummary::default();
    collect_datafusion_scan_metrics(plan, &mut metrics)?;
    Ok(metrics)
}

fn collect_datafusion_scan_metrics(
    plan: &Arc<dyn ExecutionPlan>,
    metrics: &mut DataFusionScanMetricsSummary,
) -> Result<(), QueryError> {
    if let Some(scan) = plan.as_any().downcast_ref::<AxonParquetScanExec>() {
        add_scan_trace_to_datafusion_metrics(metrics, &scan.pushdown_trace())?;
    }

    for child in plan.children() {
        collect_datafusion_scan_metrics(child, metrics)?;
    }

    Ok(())
}

fn add_scan_trace_to_datafusion_metrics(
    metrics: &mut DataFusionScanMetricsSummary,
    trace: &AxonParquetScanTrace,
) -> Result<(), QueryError> {
    metrics.scan_count = checked_datafusion_metric_add(metrics.scan_count, 1, "scan count")?;
    metrics.files_touched = checked_datafusion_metric_add(
        metrics.files_touched,
        datafusion_metric_from_usize(trace.files_planned, "files touched")?,
        "files touched",
    )?;
    metrics.files_skipped =
        checked_datafusion_metric_add(metrics.files_skipped, trace.files_skipped, "files skipped")?;
    metrics.row_groups_touched = checked_datafusion_metric_add(
        metrics.row_groups_touched,
        trace.row_groups_touched,
        "row groups touched",
    )?;
    metrics.row_groups_skipped = checked_datafusion_metric_add(
        metrics.row_groups_skipped,
        trace.row_groups_skipped,
        "row groups skipped",
    )?;
    metrics.rows_emitted =
        checked_datafusion_metric_add(metrics.rows_emitted, trace.rows_emitted, "rows emitted")?;
    metrics.bytes_fetched =
        checked_datafusion_metric_add(metrics.bytes_fetched, trace.bytes_fetched, "bytes fetched")?;
    metrics.range_read_metrics =
        merge_parquet_range_read_metrics(&metrics.range_read_metrics, &trace.range_read_metrics);

    Ok(())
}

fn datafusion_metric_from_usize(value: usize, metric_name: &str) -> Result<u64, QueryError> {
    u64::try_from(value).map_err(|_| datafusion_metric_overflow_error(metric_name))
}

fn checked_datafusion_metric_add(
    left: u64,
    right: u64,
    metric_name: &str,
) -> Result<u64, QueryError> {
    left.checked_add(right)
        .ok_or_else(|| datafusion_metric_overflow_error(metric_name))
}

fn datafusion_metric_overflow_error(metric_name: &str) -> QueryError {
    QueryError::new(
        QueryErrorCode::ExecutionFailed,
        format!("experimental browser DataFusion {metric_name} metrics overflowed u64"),
        runtime_target(),
    )
}

fn map_datafusion_error(error: datafusion::error::DataFusionError) -> QueryError {
    if let Some(query_error) = query_error_from_datafusion_error(&error) {
        return query_error;
    }

    match error {
        DataFusionError::NotImplemented(message) => QueryError::new(
            QueryErrorCode::UnsupportedFeature,
            message,
            runtime_target(),
        ),
        other => QueryError::new(
            QueryErrorCode::ExecutionFailed,
            format!("experimental browser DataFusion query failed: {other}"),
            runtime_target(),
        ),
    }
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread::{self, JoinHandle};
    use std::time::Duration;

    use parquet::data_type::Int64Type;
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;

    #[test]
    fn delta_table_schema_field_preserves_arrow_data_type() {
        let schema = DeltaTableSchema::new(vec![DeltaTableSchemaField::new(
            "payload",
            DeltaTableFieldDataType::Binary,
            true,
        )]);

        let arrow_schema = schema.arrow_schema();

        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Binary);
    }

    #[test]
    fn arrow_ipc_result_reports_axon_scan_metrics() {
        test_runtime().block_on(async {
            let batch = synthetic_record_batch().expect("synthetic batch should build");
            let descriptor = DeltaTableDescriptor {
                table_name: DEFAULT_TABLE_NAME.to_string(),
                table_version: 1,
                schema: batch.schema(),
                partition_columns: Vec::new(),
                partition_column_types: BTreeMap::new(),
                active_files: vec![DeltaActiveFile {
                    path: "part-000.parquet".to_string(),
                    url: "https://example.test/part-000.parquet".to_string(),
                    size_bytes: 1024,
                    partition_values: BTreeMap::new(),
                    object_etag: None,
                    stats_json: None,
                    deletion_vector: None,
                }],
            };
            let mut engine = WasmDataFusionEngine::new();
            engine
                .open_delta_table_with_record_batch_partitions(descriptor, vec![vec![batch]])
                .await
                .expect("descriptor table should register");

            let result = engine
                .sql_to_arrow_ipc_result("SELECT id FROM t LIMIT 1")
                .await
                .expect("DataFusion SQL should execute");

            assert_eq!(result.row_count, 1);
            assert!(!result.ipc_bytes.is_empty());
            assert_eq!(
                result.scan_metrics,
                DataFusionScanMetricsSummary {
                    scan_count: 1,
                    files_touched: 1,
                    files_skipped: 0,
                    row_groups_touched: 0,
                    row_groups_skipped: 0,
                    rows_emitted: 1,
                    bytes_fetched: 0,
                    range_read_metrics: ParquetRangeReadMetrics::default(),
                }
            );
        });
    }

    #[test]
    fn arrow_ipc_result_can_report_the_executed_physical_plan() {
        test_runtime().block_on(async {
            let batch = synthetic_record_batch().expect("synthetic batch should build");
            let descriptor = DeltaTableDescriptor {
                table_name: DEFAULT_TABLE_NAME.to_string(),
                table_version: 1,
                schema: batch.schema(),
                partition_columns: Vec::new(),
                partition_column_types: BTreeMap::new(),
                active_files: vec![DeltaActiveFile {
                    path: "part-000.parquet".to_string(),
                    url: "https://example.test/part-000.parquet".to_string(),
                    size_bytes: 1024,
                    partition_values: BTreeMap::new(),
                    object_etag: None,
                    stats_json: None,
                    deletion_vector: None,
                }],
            };
            let mut engine = WasmDataFusionEngine::new();
            engine
                .open_delta_table_with_record_batch_partitions(descriptor, vec![vec![batch]])
                .await
                .expect("descriptor table should register");

            let result = engine
                .sql_to_arrow_ipc_result_with_physical_plan("SELECT id FROM t LIMIT 1")
                .await
                .expect("DataFusion SQL should execute");

            let physical_plan = result
                .physical_plan
                .as_deref()
                .expect("executed physical plan should be attached");
            assert!(
                physical_plan.contains("DataFusion physical plan"),
                "physical plan text: {physical_plan}"
            );
            assert!(
                physical_plan.contains("AxonParquetScanExec"),
                "physical plan text: {physical_plan}"
            );
        });
    }

    #[test]
    fn unsupported_scan_features_use_stable_runtime_category_message() {
        let active_file = DeltaActiveFile {
            path: "part-000.parquet".to_string(),
            url: "https://example.test/part-000.parquet".to_string(),
            size_bytes: 1024,
            partition_values: BTreeMap::new(),
            object_etag: None,
            stats_json: None,
            deletion_vector: Some(DeletionVectorDescriptor {
                storage_type: "u".to_string(),
                path_or_inline_dv: "dv/part-000.bin".to_string(),
                offset: Some(0),
                size_in_bytes: Some(128),
                cardinality: Some(1),
            }),
        };

        let error = delta_active_file_to_scan_target(&active_file)
            .expect_err("deletion vectors should remain unsupported by the scan path");

        match error {
            DataFusionError::NotImplemented(message) => assert!(
                message.starts_with("browser DataFusion scan cannot execute"),
                "scan unsupported error should use stable category wording: {message}"
            ),
            other => panic!("expected DataFusion unsupported-feature error, got {other:?}"),
        }
    }

    #[test]
    fn arrow_ipc_batch_encoding_cancellation_reports_structured_shape() {
        test_runtime().block_on(async {
            let mut engine = WasmDataFusionEngine::with_budget(BrowserQueryBudget::default());
            let cancellation = engine.cancellation_token();
            let (schema, batches) = cancellable_output_batches();

            engine
                .register_record_batches("wide_events", schema, batches)
                .await
                .expect("wide record batches should register");

            cancellation.cancel_at_next_arrow_ipc_batch_encoding_checkpoint_for_test();

            let error = engine
                .sql_to_arrow_ipc("SELECT id, payload FROM wide_events")
                .await
                .expect_err("cancelled Arrow IPC batch encoding should fail");

            assert_eq!(error.code, QueryErrorCode::ExecutionFailed);
            assert_eq!(error.fallback_reason, None);
            assert_eq!(error.target, runtime_target());
            assert!(error.message.contains("cancelled"));
            assert!(error.message.contains("Arrow IPC batch encoding"));

            let result = engine
                .sql_to_arrow_ipc("SELECT id FROM wide_events LIMIT 1")
                .await
                .expect("a cancelled Arrow IPC output should not poison later queries");
            assert!(!result.is_empty());
        });
    }

    #[test]
    fn scan_stream_budget_rejects_actual_bytes_after_fetching_parquet() {
        test_runtime().block_on(async {
            let parquet_object = parquet_bytes_with_i64_columns(&[(1, 10), (2, 20), (3, 30)]);
            let object_size = u64::try_from(parquet_object.len()).expect("object size should fit");
            let server = RequestCapturingServer::new(parquet_object);
            let descriptor = Arc::new(DeltaTableDescriptor {
                table_name: "events".to_string(),
                table_version: 1,
                schema: Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("value", DataType::Int64, false),
                ])),
                partition_columns: Vec::new(),
                partition_column_types: BTreeMap::new(),
                active_files: vec![DeltaActiveFile {
                    path: "part-000.parquet".to_string(),
                    url: server.url(),
                    size_bytes: object_size,
                    partition_values: BTreeMap::new(),
                    object_etag: None,
                    stats_json: None,
                    deletion_vector: None,
                }],
            });
            let stream = stream_planned_scan_targets(
                HttpRangeReader::new(),
                descriptor,
                vec![0],
                vec!["id".to_string(), "value".to_string()],
                BTreeMap::new(),
                None,
                Arc::new(Mutex::new(AxonParquetScanMetrics::default())),
                Arc::new(Mutex::new(None)),
                BrowserQueryBudget {
                    max_scan_bytes: Some(1),
                    ..BrowserQueryBudget::default()
                },
                BrowserQueryCancellation::default(),
                ParquetMetadataCache::default(),
                ParquetRangeCache::default(),
                None,
            )
            .await;
            let error = match stream {
                Err(error) => error,
                Ok(mut stream) => match stream.batches.next().await {
                    Some(Err(error)) => error,
                    Some(Ok(_batch)) => panic!("stream-time scan budget should fail"),
                    None => panic!("stream should not finish before budget enforcement"),
                },
            };

            assert!(
                !server.recorded_requests().is_empty(),
                "stream-time scan budget should fail after object I/O"
            );
            assert_eq!(error.code, QueryErrorCode::FallbackRequired);
            assert_eq!(
                error.fallback_reason,
                Some(FallbackReason::BrowserRuntimeConstraint)
            );
            assert_eq!(error.target, runtime_target());
            assert!(error.message.contains("max_scan_bytes"));
            assert!(error.message.contains("scan stream"));
        });
    }

    #[test]
    fn scan_stream_reports_unsupported_later_planned_file_lazily() {
        test_runtime().block_on(async {
            let parquet_object = parquet_bytes_with_i64_columns(&[(1, 10), (2, 20), (3, 30)]);
            let object_size = u64::try_from(parquet_object.len()).expect("object size should fit");
            let server = RequestCapturingServer::new(parquet_object);
            let descriptor = Arc::new(DeltaTableDescriptor {
                table_name: "events".to_string(),
                table_version: 1,
                schema: Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("value", DataType::Int64, false),
                ])),
                partition_columns: Vec::new(),
                partition_column_types: BTreeMap::new(),
                active_files: vec![
                    DeltaActiveFile {
                        path: "part-000.parquet".to_string(),
                        url: server.url(),
                        size_bytes: object_size,
                        partition_values: BTreeMap::new(),
                        object_etag: None,
                        stats_json: None,
                        deletion_vector: None,
                    },
                    DeltaActiveFile {
                        path: "part-001.parquet".to_string(),
                        url: "https://example.test/part-001.parquet".to_string(),
                        size_bytes: 1024,
                        partition_values: BTreeMap::new(),
                        object_etag: None,
                        stats_json: None,
                        deletion_vector: Some(DeletionVectorDescriptor {
                            storage_type: "u".to_string(),
                            path_or_inline_dv: "dv/part-001.bin".to_string(),
                            offset: Some(0),
                            size_in_bytes: Some(128),
                            cardinality: Some(1),
                        }),
                    },
                ],
            });
            let mut stream = stream_planned_scan_targets(
                HttpRangeReader::new(),
                descriptor,
                vec![0, 1],
                vec!["id".to_string(), "value".to_string()],
                BTreeMap::new(),
                None,
                Arc::new(Mutex::new(AxonParquetScanMetrics::default())),
                Arc::new(Mutex::new(None)),
                BrowserQueryBudget::default(),
                BrowserQueryCancellation::default(),
                ParquetMetadataCache::default(),
                ParquetRangeCache::default(),
                None,
            )
            .await
            .expect("stream construction should not validate later planned files eagerly");

            let first_batch = stream
                .batches
                .next()
                .await
                .expect("first planned file should emit a batch")
                .expect("first planned file should scan successfully");
            assert_eq!(first_batch.num_rows(), 3);

            let error = stream
                .batches
                .next()
                .await
                .expect("later unsupported file should fail while advancing the stream")
                .expect_err("deletion-vector file should remain unsupported");

            assert_eq!(error.code, QueryErrorCode::UnsupportedFeature);
            assert_eq!(error.target, runtime_target());
            assert!(error.message.contains("deletion vectors"));
        });
    }

    fn cancellable_output_batches() -> (SchemaRef, Vec<RecordBatch>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let payload = "cancel-output-value".repeat(256);
        let batches = (0_i32..128)
            .map(|batch_index| {
                let ids = (0_i32..128)
                    .map(|row| batch_index * 128 + row)
                    .collect::<Vec<_>>();
                let payloads = std::iter::repeat(payload.as_str())
                    .take(ids.len())
                    .collect::<Vec<_>>();
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int32Array::from(ids)),
                        Arc::new(StringArray::from(payloads)),
                    ],
                )
                .expect("cancellable output batch should construct")
            })
            .collect::<Vec<_>>();

        (schema, batches)
    }

    fn parquet_bytes_with_i64_columns(rows: &[(i64, i64)]) -> Vec<u8> {
        let schema = Arc::new(
            parse_message_type("message schema { REQUIRED INT64 id; REQUIRED INT64 value; }")
                .expect("parquet schema should parse"),
        );
        let mut bytes = Vec::new();
        let mut writer = SerializedFileWriter::new(
            &mut bytes,
            schema,
            Arc::new(WriterProperties::builder().build()),
        )
        .expect("parquet writer should construct");
        let ids = rows.iter().map(|(id, _)| *id).collect::<Vec<_>>();
        let values = rows.iter().map(|(_, value)| *value).collect::<Vec<_>>();

        let mut row_group = writer
            .next_row_group()
            .expect("row-group writer should construct");
        if let Some(mut column) = row_group
            .next_column()
            .expect("id column writer should be returned")
        {
            column
                .typed::<Int64Type>()
                .write_batch(&ids, None, None)
                .expect("id values should write");
            column.close().expect("id column writer should close");
        }
        if let Some(mut column) = row_group
            .next_column()
            .expect("value column writer should be returned")
        {
            column
                .typed::<Int64Type>()
                .write_batch(&values, None, None)
                .expect("value values should write");
            column.close().expect("value column writer should close");
        }
        row_group.close().expect("row-group writer should close");
        writer.close().expect("file writer should close");
        bytes
    }

    #[derive(Clone, Debug)]
    struct CapturedRequest {
        headers: BTreeMap<String, String>,
    }

    fn read_request(stream: &mut std::net::TcpStream) -> CapturedRequest {
        let mut buffer = Vec::new();
        let mut chunk = [0_u8; 512];
        loop {
            let read = stream
                .read(&mut chunk)
                .expect("test server should read request bytes");
            if read == 0 {
                break;
            }
            buffer.extend_from_slice(&chunk[..read]);
            if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        let request = String::from_utf8_lossy(&buffer);
        let headers = request
            .lines()
            .skip(1)
            .take_while(|line| !line.is_empty())
            .filter_map(|line| {
                let (name, value) = line.split_once(':')?;
                Some((name.trim().to_ascii_lowercase(), value.trim().to_string()))
            })
            .collect();

        CapturedRequest { headers }
    }

    struct TestResponse {
        status_line: &'static str,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    }

    fn write_response(stream: &mut std::net::TcpStream, response: TestResponse) {
        write!(stream, "HTTP/1.1 {}\r\n", response.status_line).expect("status line should write");
        write!(stream, "Connection: close\r\n").expect("connection header should write");
        for (header, value) in response.headers {
            write!(stream, "{header}: {value}\r\n").expect("header should write");
        }
        write!(stream, "\r\n").expect("header terminator should write");
        stream
            .write_all(&response.body)
            .expect("response body should write");
        stream.flush().expect("response should flush");
    }

    fn full_or_ranged_response(request: &CapturedRequest, body: &[u8]) -> TestResponse {
        let Some(range_header) = request.headers.get("range") else {
            return TestResponse {
                status_line: "200 OK",
                headers: vec![("Content-Length".to_string(), body.len().to_string())],
                body: body.to_vec(),
            };
        };

        let (start, end) = resolve_range(range_header, body.len());
        if start > end || end >= body.len() {
            return TestResponse {
                status_line: "416 Range Not Satisfiable",
                headers: vec![
                    ("Content-Length".to_string(), "0".to_string()),
                    (
                        "Content-Range".to_string(),
                        format!("bytes */{}", body.len()),
                    ),
                ],
                body: Vec::new(),
            };
        }

        let ranged = body[start..=end].to_vec();
        TestResponse {
            status_line: "206 Partial Content",
            headers: vec![
                ("Content-Length".to_string(), ranged.len().to_string()),
                (
                    "Content-Range".to_string(),
                    format!("bytes {start}-{end}/{}", body.len()),
                ),
            ],
            body: ranged,
        }
    }

    fn resolve_range(range_header: &str, object_len: usize) -> (usize, usize) {
        let range = range_header
            .strip_prefix("bytes=")
            .expect("test server expects byte ranges");
        if let Some(suffix) = range.strip_prefix('-') {
            let suffix_len = suffix.parse::<usize>().expect("suffix length should parse");
            let start = object_len.saturating_sub(suffix_len);
            return (start, object_len.saturating_sub(1));
        }
        let (start, end) = range
            .split_once('-')
            .expect("range should include '-' separator");
        let start = start.parse::<usize>().expect("range start should parse");
        if end.is_empty() {
            return (start, object_len.saturating_sub(1));
        }
        let end = end.parse::<usize>().expect("range end should parse");
        (start, end.min(object_len.saturating_sub(1)))
    }

    struct RequestCapturingServer {
        address: std::net::SocketAddr,
        stop: Arc<AtomicBool>,
        requests: Arc<Mutex<Vec<CapturedRequest>>>,
        thread: Option<JoinHandle<()>>,
    }

    impl RequestCapturingServer {
        fn new(body: Vec<u8>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral port should bind");
            listener
                .set_nonblocking(true)
                .expect("listener should allow nonblocking accept");
            let address = listener.local_addr().expect("listener addr should resolve");
            let stop = Arc::new(AtomicBool::new(false));
            let stop_for_thread = Arc::clone(&stop);
            let requests = Arc::new(Mutex::new(Vec::new()));
            let requests_for_thread = Arc::clone(&requests);
            let body = Arc::new(body);

            let thread = thread::spawn(move || {
                while !stop_for_thread.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            if stop_for_thread.load(Ordering::SeqCst) {
                                break;
                            }
                            stream
                                .set_nonblocking(false)
                                .expect("accepted streams should allow blocking reads");
                            let body = Arc::clone(&body);
                            let requests = Arc::clone(&requests_for_thread);
                            thread::spawn(move || {
                                let request = read_request(&mut stream);
                                requests
                                    .lock()
                                    .expect("recorded requests should be writable")
                                    .push(request.clone());
                                write_response(
                                    &mut stream,
                                    full_or_ranged_response(&request, &body),
                                );
                            });
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(error) => panic!("test server accept should succeed: {error}"),
                    }
                }
            });

            Self {
                address,
                stop,
                requests,
                thread: Some(thread),
            }
        }

        fn url(&self) -> String {
            format!("http://{}/object", self.address)
        }

        fn recorded_requests(&self) -> Vec<CapturedRequest> {
            self.requests
                .lock()
                .expect("recorded requests should be readable")
                .clone()
        }
    }

    impl Drop for RequestCapturingServer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            let _ = std::net::TcpStream::connect(self.address);
            if let Some(thread) = self.thread.take() {
                thread.join().expect("test server should shut down cleanly");
            }
        }
    }

    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Runtime::new().expect("test runtime should construct")
    }
}
