//! Experimental browser DataFusion proof-of-concept.
//!
//! This crate is intentionally isolated from Axon's default browser runtime and worker artifact.

use std::{
    any::Any,
    collections::BTreeMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

use arrow_array::{Int32Array, RecordBatch, RecordBatchOptions, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::ScalarValue;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    execution_plan::{Boundedness, EmissionType},
    stream::RecordBatchStreamAdapter,
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::prelude::SessionContext;
use futures_util::{
    stream::{self, BoxStream},
    StreamExt, TryStreamExt,
};
use query_contract::{ExecutionTarget, PartitionColumnType, QueryError, QueryErrorCode};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;
use wasm_http_object_store::HttpRangeReader;
use wasm_parquet_engine::{
    stream_scan_target_batches_with_row_group_pruning, ObjectSource, ParquetIntegerComparison,
    ParquetRowGroupPruningPredicate, ScanTarget, ScanTargetMetricsHandle,
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AxonParquetScanTrace {
    pub projected_columns: Vec<String>,
    pub limit: Option<usize>,
    pub filters: Vec<String>,
    pub exact_filters: Vec<String>,
    pub inexact_filters: Vec<String>,
    pub files_total: usize,
    pub files_planned: usize,
    pub planned_file_paths: Vec<String>,
    pub files_skipped: u64,
    pub row_groups_skipped: u64,
    pub bytes_fetched: u64,
    pub rows_emitted: u64,
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
        _state: &'life1 dyn Session,
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
            Ok(Arc::new(AxonParquetScanExec::new(
                self.descriptor.clone(),
                schema,
                projection.cloned(),
                filters.to_vec(),
                limit,
                self.in_memory_partitions.clone(),
            )) as Arc<dyn ExecutionPlan>)
        })
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        let exact_partition_filters_enabled = self.can_apply_exact_partition_filters();
        Ok(filters
            .iter()
            .map(|filter| {
                if exact_partition_filters_enabled
                    && exact_partition_constraints(filter, &self.descriptor).is_some()
                {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Inexact
                }
            })
            .collect())
    }
}

#[derive(Debug)]
pub struct AxonParquetScanExec {
    descriptor: DeltaTableDescriptor,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    planned_file_indices: Vec<usize>,
    planned_partition_indices: Vec<usize>,
    row_group_predicate: Option<ParquetRowGroupPruningPredicate>,
    partitions: Vec<Vec<RecordBatch>>,
    properties: PlanProperties,
    trace: Arc<Mutex<AxonParquetScanTrace>>,
    limit_remaining: Arc<Mutex<Option<usize>>>,
}

impl AxonParquetScanExec {
    fn new(
        descriptor: DeltaTableDescriptor,
        projected_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        limit: Option<usize>,
        partitions: Vec<Vec<RecordBatch>>,
    ) -> Self {
        let plan = AxonScanPlan::new(&descriptor, &projected_schema, &filters, limit, &partitions);
        let partition_count = if partitions.is_empty() {
            1
        } else {
            plan.planned_partition_indices.len().max(1)
        };
        let properties = Self::compute_properties(Arc::clone(&projected_schema), partition_count);

        Self {
            descriptor,
            projected_schema,
            projection,
            limit,
            planned_file_indices: plan.planned_file_indices,
            planned_partition_indices: plan.planned_partition_indices,
            row_group_predicate: plan.row_group_predicate,
            partitions,
            properties,
            trace: Arc::new(Mutex::new(plan.trace)),
            limit_remaining: Arc::new(Mutex::new(limit)),
        }
    }

    pub fn pushdown_trace(&self) -> AxonParquetScanTrace {
        self.trace
            .lock()
            .expect("AxonParquetScanExec trace should not be poisoned")
            .clone()
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

    fn scan_targets(&self) -> DataFusionResult<Vec<ScanTarget>> {
        self.planned_file_indices
            .iter()
            .filter_map(|file_index| self.descriptor.active_files.get(*file_index))
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
        let batches = self
            .planned_partition_indices
            .get(partition)
            .and_then(|source_partition| self.partitions.get(*source_partition))
            .cloned()
            .unwrap_or_default();
        let projection = self.projection.clone();
        let trace = Arc::clone(&self.trace);
        let limit_remaining = Arc::clone(&self.limit_remaining);
        let projected_batches = batches.into_iter().filter_map(move |batch| {
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
            record_trace_rows_emitted(&trace, projected.num_rows());
            Some(Ok(projected))
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
        let row_group_predicate = self.row_group_predicate.clone();
        let trace = Arc::clone(&self.trace);
        let limit_remaining = Arc::clone(&self.limit_remaining);
        let stream_schema = Arc::clone(&self.projected_schema);
        let projected_schema = Arc::clone(&self.projected_schema);
        let parquet_batches = stream::once(async move {
            stream_planned_scan_targets(
                reader,
                targets,
                required_columns,
                partition_column_types,
                row_group_predicate,
                trace,
                limit_remaining,
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
    planned_file_indices: Vec<usize>,
    planned_partition_indices: Vec<usize>,
    row_group_predicate: Option<ParquetRowGroupPruningPredicate>,
    trace: AxonParquetScanTrace,
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
        let planned_file_indices = planned_file_indices(descriptor, &partition_constraints);
        let planned_partition_indices =
            planned_partition_indices(descriptor, partitions, &planned_file_indices);
        let files_total = descriptor.active_files.len();
        let files_planned = planned_file_indices.len();
        let planned_file_paths = planned_file_indices
            .iter()
            .filter_map(|index| descriptor.active_files.get(*index))
            .map(|file| file.path.clone())
            .collect::<Vec<_>>();
        let files_skipped = files_total.saturating_sub(files_planned) as u64;
        let exact_filters = filters
            .iter()
            .filter(|filter| {
                exact_partition_filters_enabled
                    && exact_partition_constraints(filter, descriptor).is_some()
            })
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let inexact_filters = filters
            .iter()
            .filter(|filter| {
                !(exact_partition_filters_enabled
                    && exact_partition_constraints(filter, descriptor).is_some())
            })
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let trace = AxonParquetScanTrace {
            projected_columns: projected_column_names(projected_schema),
            limit,
            filters: filters.iter().map(ToString::to_string).collect(),
            exact_filters,
            inexact_filters,
            files_total,
            files_planned,
            planned_file_paths,
            files_skipped,
            row_groups_skipped: 0,
            bytes_fetched: 0,
            rows_emitted: 0,
        };

        Self {
            planned_file_indices,
            planned_partition_indices,
            row_group_predicate: filters
                .iter()
                .find_map(|filter| row_group_pruning_predicate(filter, descriptor)),
            trace,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PartitionConstraint {
    column: String,
    values: Vec<Option<String>>,
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
) -> Vec<usize> {
    descriptor
        .active_files
        .iter()
        .enumerate()
        .filter(|(_index, file)| partition_values_match(file, constraints))
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
    active_file: &DeltaActiveFile,
    constraints: &[PartitionConstraint],
) -> bool {
    constraints.iter().all(|constraint| {
        let value = active_file
            .partition_values
            .get(&constraint.column)
            .cloned()
            .unwrap_or(None);
        constraint.values.iter().any(|expected| expected == &value)
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
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            partition_equality_constraint(&binary.left, &binary.right, descriptor)
                .or_else(|| partition_equality_constraint(&binary.right, &binary.left, descriptor))
                .map(|constraint| vec![constraint])
        }
        Expr::IsNull(expr) => expr
            .try_as_col()
            .and_then(|column| partition_column_name(&column.name, descriptor))
            .map(|column| {
                vec![PartitionConstraint {
                    column,
                    values: vec![None],
                }]
            }),
        Expr::InList(in_list) if !in_list.negated => {
            let column = in_list
                .expr
                .try_as_col()
                .and_then(|column| partition_column_name(&column.name, descriptor))?;
            let values = in_list
                .list
                .iter()
                .map(scalar_partition_value_from_expr)
                .collect::<Option<Vec<_>>>()?;
            if values.iter().any(Option::is_none) {
                return None;
            }
            Some(vec![PartitionConstraint { column, values }])
        }
        _ => None,
    }
}

fn partition_equality_constraint(
    left: &Expr,
    right: &Expr,
    descriptor: &DeltaTableDescriptor,
) -> Option<PartitionConstraint> {
    let column = left
        .try_as_col()
        .and_then(|column| partition_column_name(&column.name, descriptor))?;
    let value = scalar_partition_value_from_expr(right)?;
    value.as_ref()?;
    Some(PartitionConstraint {
        column,
        values: vec![value],
    })
}

fn partition_column_name(column_name: &str, descriptor: &DeltaTableDescriptor) -> Option<String> {
    descriptor
        .partition_columns
        .iter()
        .find(|partition_column| partition_column.as_str() == column_name)
        .cloned()
}

fn scalar_partition_value_from_expr(expr: &Expr) -> Option<Option<String>> {
    match expr {
        Expr::Literal(value, _) => scalar_partition_value(value),
        _ => None,
    }
}

fn scalar_partition_value(value: &ScalarValue) -> Option<Option<String>> {
    match value {
        ScalarValue::Null => Some(None),
        ScalarValue::Utf8(value) | ScalarValue::Utf8View(value) | ScalarValue::LargeUtf8(value) => {
            Some(value.clone())
        }
        ScalarValue::Boolean(value) => Some(value.map(|value| value.to_string())),
        ScalarValue::Int8(value) => Some(value.map(|value| value.to_string())),
        ScalarValue::Int16(value) => Some(value.map(|value| value.to_string())),
        ScalarValue::Int32(value) => Some(value.map(|value| value.to_string())),
        ScalarValue::Int64(value) => Some(value.map(|value| value.to_string())),
        ScalarValue::UInt8(value) => Some(value.map(|value| value.to_string())),
        ScalarValue::UInt16(value) => Some(value.map(|value| value.to_string())),
        ScalarValue::UInt32(value) => Some(value.map(|value| value.to_string())),
        ScalarValue::UInt64(value) => Some(value.map(|value| value.to_string())),
        _ => None,
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

fn record_trace_rows_emitted(trace: &Arc<Mutex<AxonParquetScanTrace>>, rows: usize) {
    let rows = u64::try_from(rows).unwrap_or(u64::MAX);
    let mut trace = trace
        .lock()
        .expect("AxonParquetScanExec trace should not be poisoned");
    trace.rows_emitted = trace.rows_emitted.saturating_add(rows);
}

fn record_trace_parquet_metrics(
    trace: &Arc<Mutex<AxonParquetScanTrace>>,
    metrics: ScanTargetMetricsSnapshot,
) {
    let mut trace = trace
        .lock()
        .expect("AxonParquetScanExec trace should not be poisoned");
    trace.row_groups_skipped = metrics.row_groups_skipped;
    trace.bytes_fetched = metrics.bytes_fetched;
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
    }
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
    targets: Vec<ScanTarget>,
    next_target_index: usize,
    current_batches: Option<BoxStream<'static, Result<RecordBatch, QueryError>>>,
    current_metrics: Option<Arc<dyn ScanTargetMetricsHandle + Send + Sync>>,
    completed_metrics: ScanTargetMetricsSnapshot,
    required_columns: Vec<String>,
    partition_column_types: BTreeMap<String, PartitionColumnType>,
    row_group_predicate: Option<ParquetRowGroupPruningPredicate>,
    trace: Arc<Mutex<AxonParquetScanTrace>>,
    limit_remaining: Arc<Mutex<Option<usize>>>,
}

async fn stream_planned_scan_targets(
    reader: HttpRangeReader,
    targets: Vec<ScanTarget>,
    required_columns: Vec<String>,
    partition_column_types: BTreeMap<String, PartitionColumnType>,
    row_group_predicate: Option<ParquetRowGroupPruningPredicate>,
    trace: Arc<Mutex<AxonParquetScanTrace>>,
    limit_remaining: Arc<Mutex<Option<usize>>>,
) -> Result<PlannedScanTargetBatchStream, QueryError> {
    let state = PlannedScanTargetsState {
        reader,
        targets,
        next_target_index: 0,
        current_batches: None,
        current_metrics: None,
        completed_metrics: ScanTargetMetricsSnapshot::default(),
        required_columns,
        partition_column_types,
        row_group_predicate,
        trace,
        limit_remaining,
    };
    let batches = stream::try_unfold(state, |mut state| async move {
        loop {
            if scan_limit_exhausted(&state.limit_remaining) {
                return Ok(None);
            }

            if let Some(current_batches) = &mut state.current_batches {
                match current_batches.next().await {
                    Some(batch) => {
                        let batch = batch?;
                        if let Some(metrics) = &state.current_metrics {
                            record_trace_parquet_metrics(
                                &state.trace,
                                add_scan_metrics(&state.completed_metrics, &metrics.snapshot()),
                            );
                        }
                        let rows_to_emit = take_scan_rows(&state.limit_remaining, batch.num_rows());
                        if rows_to_emit == 0 {
                            continue;
                        }
                        let batch = slice_record_batch(batch, rows_to_emit);
                        record_trace_rows_emitted(&state.trace, batch.num_rows());
                        return Ok(Some((batch, state)));
                    }
                    None => {
                        if let Some(metrics) = &state.current_metrics {
                            state.completed_metrics =
                                add_scan_metrics(&state.completed_metrics, &metrics.snapshot());
                            record_trace_parquet_metrics(
                                &state.trace,
                                state.completed_metrics.clone(),
                            );
                        }
                        state.current_batches = None;
                        state.current_metrics = None;
                    }
                }
            }

            let Some(target) = state.targets.get(state.next_target_index).cloned() else {
                return Ok(None);
            };
            state.next_target_index += 1;

            let scan = stream_scan_target_batches_with_row_group_pruning(
                &state.reader,
                &target,
                &state.required_columns,
                &state.partition_column_types,
                None,
                state.row_group_predicate.as_ref(),
            )
            .await?;
            record_trace_parquet_metrics(
                &state.trace,
                add_scan_metrics(&state.completed_metrics, &scan.metrics.snapshot()),
            );
            state.current_metrics = Some(scan.metrics);
            state.current_batches = Some(scan.batches);
        }
    })
    .boxed();

    Ok(PlannedScanTargetBatchStream { batches })
}

impl DisplayAs for AxonParquetScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let trace = self.pushdown_trace();
                write!(
                    f,
                    "AxonParquetScanExec: table={}, active_files={}, partitions={}, projected_columns={:?}, filters={:?}, limit={:?}, files_skipped={}, row_groups_skipped={}, bytes_fetched={}, rows_emitted={}",
                    self.descriptor.table_name,
                    self.descriptor.active_files.len(),
                    self.partition_count(),
                    trace.projected_columns,
                    trace.filters,
                    self.limit,
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
