//! DataFusion-backed browser query session for UI/runtime builds.
//!
//! This crate is intentionally separate from `wasm-query-session`, which keeps the legacy narrow
//! custom runtime isolated for compatibility and eventual removal.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::mem::size_of;
use std::ops::ControlFlow;

use arrow_schema::TimeUnit;
use query_contract::{
    BrowserHttpSnapshotDescriptor, ExecutionTarget, FallbackReason, ParquetInspectionSummary,
    PartitionColumnType, QueryError, QueryErrorCode, QueryMetricsSummary, QueryRequest,
    QueryResponse, QueryResultPage, QueryRuntimeLimits, MAX_QUERY_RESULT_PAGE_LIMIT,
};
use sqlparser::ast::{
    ObjectName, Query as SqlQuery, Select, SelectFlavor, SetExpr as SqlSetExpr,
    Statement as SqlStatement, TableFactor as SqlTableFactor, TableWithJoins as SqlTableWithJoins,
    Visit, Visitor,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use wasm_datafusion_poc::{
    BrowserQueryCancellation, DataFusionArrowIpcResult, DataFusionScanMetricsSummary,
    DeltaTableDescriptor, DeltaTableFieldDataType, DeltaTableSchema, DeltaTableSchemaField,
    WasmDataFusionEngine,
};
use wasm_query_runtime::{
    runtime_target, BootstrappedBrowserSnapshot, BrowserExecutionBudget,
    BrowserParquetConvertedType, BrowserParquetField, BrowserParquetLogicalType,
    BrowserParquetPhysicalType, BrowserParquetRepetition, BrowserParquetTimeUnit,
    BrowserRuntimeConfig, BrowserRuntimeInstant, BrowserRuntimeSession,
    MaterializedBrowserSnapshot, RuntimeArrowIpcResult,
};

pub const OWNER: &str = "Runtime / engine team";
pub const RESPONSIBILITY: &str =
    "DataFusion-backed browser session for UI/runtime builds over browser-safe descriptors.";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BrowserDataFusionQueryBudget {
    pub max_scan_bytes: Option<u64>,
    pub max_output_ipc_bytes: Option<u64>,
    pub max_batches_in_flight: Option<usize>,
    pub max_rows_returned: Option<u64>,
}

impl From<BrowserDataFusionQueryBudget> for wasm_datafusion_poc::BrowserQueryBudget {
    fn from(budget: BrowserDataFusionQueryBudget) -> Self {
        Self {
            max_scan_bytes: budget.max_scan_bytes,
            max_output_ipc_bytes: budget.max_output_ipc_bytes,
            max_batches_in_flight: budget.max_batches_in_flight,
            max_rows_returned: budget.max_rows_returned,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserDataFusionSessionQueryResult {
    pub response: QueryResponse,
    pub runtime_result: RuntimeArrowIpcResult,
}

#[derive(Clone, Debug)]
pub struct BrowserDataFusionSession {
    runtime: BrowserRuntimeSession,
    query_budget: BrowserDataFusionQueryBudget,
    datafusion: WasmDataFusionEngine,
    tables: BTreeMap<String, CachedDataFusionTable>,
    max_cached_bytes: u64,
    next_access_millis: u64,
}

#[derive(Clone, Debug)]
pub struct BrowserDataFusionCancellation {
    cancellation: BrowserQueryCancellation,
}

impl BrowserDataFusionCancellation {
    pub fn cancel_running_queries(&self) {
        self.cancellation.cancel();
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CachedDataFusionTable {
    descriptor: BrowserHttpSnapshotDescriptor,
    materialized: MaterializedBrowserSnapshot,
    bootstrapped: Option<BootstrappedBrowserSnapshot>,
    cached_bytes: u64,
    last_used_millis: u64,
}

impl BrowserDataFusionSession {
    pub fn new(config: BrowserRuntimeConfig, max_cached_bytes: u64) -> Result<Self, QueryError> {
        let query_budget = datafusion_query_budget_from_runtime_config(&config);
        Self::new_with_query_budget(config, max_cached_bytes, query_budget)
    }

    pub fn new_with_query_budget(
        config: BrowserRuntimeConfig,
        max_cached_bytes: u64,
        query_budget: BrowserDataFusionQueryBudget,
    ) -> Result<Self, QueryError> {
        Ok(Self {
            runtime: BrowserRuntimeSession::new(config)?,
            query_budget,
            datafusion: WasmDataFusionEngine::with_budget(query_budget.into()),
            tables: BTreeMap::new(),
            max_cached_bytes,
            next_access_millis: 0,
        })
    }

    pub fn runtime(&self) -> &BrowserRuntimeSession {
        &self.runtime
    }

    pub fn datafusion_query_budget(&self) -> BrowserDataFusionQueryBudget {
        self.query_budget
    }

    pub fn cancellation_handle(&self) -> BrowserDataFusionCancellation {
        BrowserDataFusionCancellation {
            cancellation: self.datafusion.cancellation_token(),
        }
    }

    pub fn max_cached_bytes(&self) -> u64 {
        self.max_cached_bytes
    }

    pub fn cached_bytes(&self) -> u64 {
        self.tables.values().fold(0_u64, |total, table| {
            total.saturating_add(table.cached_bytes())
        })
    }

    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    pub fn contains_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    pub fn table(&self, name: &str) -> Option<&CachedDataFusionTable> {
        self.tables.get(name)
    }

    pub async fn open_delta_table(
        &mut self,
        name: impl Into<String>,
        descriptor: BrowserHttpSnapshotDescriptor,
    ) -> Result<(), QueryError> {
        let name = name.into();
        let materialized = self.runtime.materialize_snapshot(&descriptor)?;
        let bootstrapped = if materialized.active_files().is_empty() {
            None
        } else {
            Some(
                self.runtime
                    .bootstrap_snapshot_metadata(&materialized)
                    .await?,
            )
        };
        let schema = datafusion_delta_schema(&descriptor, bootstrapped.as_ref())?;
        let delta_descriptor =
            DeltaTableDescriptor::from_browser_http_snapshot(name.clone(), &descriptor, schema)?;
        let cached_bytes = cached_table_bytes(&materialized, bootstrapped.as_ref())?;
        let last_used_millis = self.bump_access_clock();

        if self.tables.contains_key(&name) {
            self.datafusion.deregister_table(&name)?;
            self.tables.remove(&name);
        }
        self.datafusion.open_delta_table(delta_descriptor).await?;
        self.insert_table(
            name,
            descriptor,
            materialized,
            bootstrapped,
            cached_bytes,
            last_used_millis,
        )
    }

    pub async fn inspect_parquet(
        &mut self,
        name: &str,
        path: &str,
    ) -> Result<ParquetInspectionSummary, QueryError> {
        let file = {
            let table = self.touch_table(name)?;
            table
                .materialized
                .active_files()
                .iter()
                .find(|file| file.path() == path)
                .cloned()
                .ok_or_else(|| missing_table_file_error(name, path))?
        };

        self.runtime.inspect_parquet_file(&file).await
    }

    pub async fn sql(
        &mut self,
        name: &str,
        request: &QueryRequest,
    ) -> Result<BrowserDataFusionSessionQueryResult, QueryError> {
        let execution_budget = self.runtime.config().execution_budget;
        let (capabilities, file_count, footer_reads, snapshot_bootstrap_duration_ms, access_mode) = {
            let table = self.touch_table(name)?;
            validate_datafusion_request_match(table, request)?;
            validate_datafusion_sql_scope(name, &request.sql)?;
            validate_datafusion_execution_budget(table, execution_budget)?;
            (
                table.materialized.required_capabilities().clone(),
                u64::try_from(table.materialized.active_files().len()).map_err(|_| {
                    QueryError::new(
                        QueryErrorCode::ExecutionFailed,
                        "browser DataFusion session file counts overflowed u64",
                        runtime_target(),
                    )
                })?,
                table
                    .bootstrapped
                    .as_ref()
                    .and_then(BootstrappedBrowserSnapshot::footer_reads),
                table
                    .bootstrapped
                    .as_ref()
                    .and_then(BootstrappedBrowserSnapshot::snapshot_bootstrap_duration_ms),
                table
                    .bootstrapped
                    .as_ref()
                    .and_then(BootstrappedBrowserSnapshot::access_mode),
            )
        };
        let started_at = BrowserRuntimeInstant::now();
        let sql = datafusion_sql_with_result_page(&request.sql, request.options.result_page)?;
        let query_budget =
            datafusion_query_budget_for_request(self.query_budget, request.options.runtime_limits);
        let has_request_budget = query_budget != self.query_budget;
        let datafusion_result = if has_request_budget {
            self.reregister_datafusion_table_with_budget(name, query_budget)
                .await?;
            let result = self
                .execute_datafusion_sql(sql.as_ref(), request.options.include_explain)
                .await;
            let restore_result = self
                .reregister_datafusion_table_with_budget(name, self.query_budget)
                .await;
            match (result, restore_result) {
                (Ok(datafusion_result), Ok(())) => datafusion_result,
                (Err(error), Ok(())) => return Err(error),
                (Ok(_), Err(error)) => return Err(error),
                (Err(error), Err(_)) => return Err(error),
            }
        } else {
            self.execute_datafusion_sql(sql.as_ref(), request.options.include_explain)
                .await?
        };
        let explain = datafusion_result.physical_plan.clone();
        let scan_metrics = datafusion_result.scan_metrics.clone();
        let runtime_result = runtime_result_from_datafusion(datafusion_result);

        Ok(BrowserDataFusionSessionQueryResult {
            response: QueryResponse {
                executed_on: ExecutionTarget::BrowserWasm,
                capabilities,
                fallback_reason: None,
                metrics: datafusion_query_metrics(
                    scan_metrics,
                    file_count,
                    footer_reads,
                    snapshot_bootstrap_duration_ms,
                    access_mode,
                    started_at,
                ),
                explain,
            },
            runtime_result,
        })
    }

    async fn execute_datafusion_sql(
        &self,
        sql: &str,
        include_explain: bool,
    ) -> Result<DataFusionArrowIpcResult, QueryError> {
        if include_explain {
            self.datafusion
                .sql_to_arrow_ipc_result_with_physical_plan(sql)
                .await
        } else {
            self.datafusion.sql_to_arrow_ipc_result(sql).await
        }
    }

    async fn reregister_datafusion_table_with_budget(
        &mut self,
        name: &str,
        query_budget: BrowserDataFusionQueryBudget,
    ) -> Result<(), QueryError> {
        let (descriptor, bootstrapped) = {
            let table = self
                .tables
                .get(name)
                .ok_or_else(|| missing_table_error(name))?;
            (table.descriptor.clone(), table.bootstrapped.clone())
        };
        let schema = datafusion_delta_schema(&descriptor, bootstrapped.as_ref())?;
        let delta_descriptor = DeltaTableDescriptor::from_browser_http_snapshot(
            name.to_string(),
            &descriptor,
            schema,
        )?;
        self.datafusion.set_query_budget(query_budget.into());
        self.datafusion.deregister_table(name)?;
        self.datafusion.open_delta_table(delta_descriptor).await
    }

    pub fn remove_table(&mut self, name: &str) -> Option<CachedDataFusionTable> {
        let removed = self.tables.remove(name);
        if removed.is_some() {
            let _ = self.datafusion.deregister_table(name);
        }
        removed
    }

    pub fn dispose_table(&mut self, name: &str) -> bool {
        self.remove_table(name).is_some()
    }

    fn insert_table(
        &mut self,
        name: String,
        descriptor: BrowserHttpSnapshotDescriptor,
        materialized: MaterializedBrowserSnapshot,
        bootstrapped: Option<BootstrappedBrowserSnapshot>,
        cached_bytes: u64,
        last_used_millis: u64,
    ) -> Result<(), QueryError> {
        self.tables.insert(
            name.clone(),
            CachedDataFusionTable {
                descriptor,
                materialized,
                bootstrapped,
                cached_bytes,
                last_used_millis,
            },
        );
        self.evict_to_budget(&name)?;

        Ok(())
    }

    fn touch_table(&mut self, name: &str) -> Result<&mut CachedDataFusionTable, QueryError> {
        let last_used_millis = self.bump_access_clock();
        let table = self
            .tables
            .get_mut(name)
            .ok_or_else(|| missing_table_error(name))?;
        table.last_used_millis = last_used_millis;
        Ok(table)
    }

    fn bump_access_clock(&mut self) -> u64 {
        self.next_access_millis = self.next_access_millis.saturating_add(1);
        self.next_access_millis
    }

    fn evict_to_budget(&mut self, pinned_name: &str) -> Result<(), QueryError> {
        while self.cached_bytes() > self.max_cached_bytes && self.tables.len() > 1 {
            let eviction_candidate = self
                .tables
                .iter()
                .filter(|(name, _)| name.as_str() != pinned_name)
                .min_by_key(|(_, table)| table.last_used_millis)
                .map(|(name, _)| name.clone());

            let Some(eviction_candidate) = eviction_candidate else {
                break;
            };

            if self.tables.remove(&eviction_candidate).is_some() {
                self.datafusion.deregister_table(&eviction_candidate)?;
            }
        }

        Ok(())
    }
}

impl CachedDataFusionTable {
    pub fn descriptor(&self) -> &BrowserHttpSnapshotDescriptor {
        &self.descriptor
    }

    pub fn materialized_snapshot(&self) -> &MaterializedBrowserSnapshot {
        &self.materialized
    }

    pub fn bootstrapped_snapshot(&self) -> Option<&BootstrappedBrowserSnapshot> {
        self.bootstrapped.as_ref()
    }

    pub fn cached_bytes(&self) -> u64 {
        self.cached_bytes
    }

    pub fn last_used_millis(&self) -> u64 {
        self.last_used_millis
    }
}

fn datafusion_delta_schema(
    descriptor: &BrowserHttpSnapshotDescriptor,
    bootstrapped: Option<&BootstrappedBrowserSnapshot>,
) -> Result<DeltaTableSchema, QueryError> {
    let mut fields = Vec::new();
    let mut field_names = BTreeSet::new();

    if let Some(bootstrapped) = bootstrapped {
        for field in bootstrapped.validate_uniform_schema()?.fields {
            if field_names.insert(field.name.clone()) {
                fields.push(datafusion_schema_field_from_parquet_field(&field)?);
            }
        }
    }

    for partition_column in descriptor_partition_columns(descriptor) {
        if field_names.insert(partition_column.clone()) {
            fields.push(DeltaTableSchemaField::new(
                partition_column.clone(),
                datafusion_partition_column_type(descriptor, &partition_column)?,
                true,
            ));
        }
    }

    Ok(DeltaTableSchema::new(fields))
}

fn descriptor_partition_columns(descriptor: &BrowserHttpSnapshotDescriptor) -> Vec<String> {
    let mut partition_columns = descriptor
        .partition_column_types
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    for file in &descriptor.active_files {
        partition_columns.extend(file.partition_values.keys().cloned());
    }
    partition_columns.into_iter().collect()
}

fn datafusion_partition_column_type(
    descriptor: &BrowserHttpSnapshotDescriptor,
    column: &str,
) -> Result<DeltaTableFieldDataType, QueryError> {
    match descriptor.partition_column_types.get(column) {
        Some(PartitionColumnType::String) | None => Ok(DeltaTableFieldDataType::Utf8),
        Some(PartitionColumnType::Int64) => Ok(DeltaTableFieldDataType::Int64),
        Some(PartitionColumnType::Boolean) => Ok(DeltaTableFieldDataType::Boolean),
        Some(PartitionColumnType::Unsupported) => Err(QueryError::new(
            QueryErrorCode::UnsupportedFeature,
            format!(
                "browser DataFusion schema does not yet support partition column '{column}' type"
            ),
            runtime_target(),
        )),
    }
}

fn datafusion_schema_field_from_parquet_field(
    field: &BrowserParquetField,
) -> Result<DeltaTableSchemaField, QueryError> {
    Ok(DeltaTableSchemaField::new(
        field.name.clone(),
        datafusion_data_type_from_parquet_field(field)?,
        field.nullable,
    ))
}

fn datafusion_data_type_from_parquet_field(
    field: &BrowserParquetField,
) -> Result<DeltaTableFieldDataType, QueryError> {
    if matches!(field.repetition, BrowserParquetRepetition::Repeated)
        || field.max_definition_level > 1
        || field.max_repetition_level > 0
    {
        return Err(unsupported_datafusion_parquet_field(field));
    }

    match &field.physical_type {
        BrowserParquetPhysicalType::Boolean if field_has_no_annotations(field) => {
            Ok(DeltaTableFieldDataType::Boolean)
        }
        BrowserParquetPhysicalType::Int32 => datafusion_int32_type_from_parquet_field(field),
        BrowserParquetPhysicalType::Int64 => datafusion_int64_type_from_parquet_field(field),
        BrowserParquetPhysicalType::Float if field_has_no_annotations(field) => {
            Ok(DeltaTableFieldDataType::Float32)
        }
        BrowserParquetPhysicalType::Double if field_has_no_annotations(field) => {
            Ok(DeltaTableFieldDataType::Float64)
        }
        BrowserParquetPhysicalType::ByteArray => {
            datafusion_byte_array_type_from_parquet_field(field)
        }
        BrowserParquetPhysicalType::Int96 | BrowserParquetPhysicalType::FixedLenByteArray => {
            Err(unsupported_datafusion_parquet_field(field))
        }
        BrowserParquetPhysicalType::Boolean
        | BrowserParquetPhysicalType::Float
        | BrowserParquetPhysicalType::Double => Err(unsupported_datafusion_parquet_field(field)),
    }
}

fn datafusion_int32_type_from_parquet_field(
    field: &BrowserParquetField,
) -> Result<DeltaTableFieldDataType, QueryError> {
    match field.logical_type.as_ref() {
        Some(BrowserParquetLogicalType::Integer {
            bit_width: 32,
            is_signed: true,
        }) => {
            return if matches!(
                field.converted_type.as_ref(),
                None | Some(BrowserParquetConvertedType::Int32)
            ) {
                Ok(DeltaTableFieldDataType::Int32)
            } else {
                Err(unsupported_datafusion_parquet_field(field))
            };
        }
        Some(BrowserParquetLogicalType::Date) => {
            return if matches!(
                field.converted_type.as_ref(),
                None | Some(BrowserParquetConvertedType::Date)
            ) {
                Ok(DeltaTableFieldDataType::Date32)
            } else {
                Err(unsupported_datafusion_parquet_field(field))
            };
        }
        Some(_) => return Err(unsupported_datafusion_parquet_field(field)),
        None => {}
    }

    match field.converted_type.as_ref() {
        None | Some(BrowserParquetConvertedType::Int32) => Ok(DeltaTableFieldDataType::Int32),
        Some(BrowserParquetConvertedType::Date) => Ok(DeltaTableFieldDataType::Date32),
        Some(_) => Err(unsupported_datafusion_parquet_field(field)),
    }
}

fn datafusion_int64_type_from_parquet_field(
    field: &BrowserParquetField,
) -> Result<DeltaTableFieldDataType, QueryError> {
    match field.logical_type.as_ref() {
        Some(BrowserParquetLogicalType::Integer {
            bit_width: 64,
            is_signed: true,
        }) => {
            return if matches!(
                field.converted_type.as_ref(),
                None | Some(BrowserParquetConvertedType::Int64)
            ) {
                Ok(DeltaTableFieldDataType::Int64)
            } else {
                Err(unsupported_datafusion_parquet_field(field))
            };
        }
        Some(BrowserParquetLogicalType::Timestamp {
            is_adjusted_to_utc: true,
            unit,
        }) => {
            let Some(time_unit) = datafusion_timestamp_time_unit(unit) else {
                return Err(unsupported_datafusion_parquet_field(field));
            };
            return if converted_timestamp_type_matches(field.converted_type.as_ref(), time_unit) {
                Ok(DeltaTableFieldDataType::Timestamp(
                    time_unit,
                    Some("UTC".into()),
                ))
            } else {
                Err(unsupported_datafusion_parquet_field(field))
            };
        }
        Some(_) => return Err(unsupported_datafusion_parquet_field(field)),
        None => {}
    }

    match field.converted_type.as_ref() {
        None | Some(BrowserParquetConvertedType::Int64) => Ok(DeltaTableFieldDataType::Int64),
        Some(BrowserParquetConvertedType::TimestampMillis) => Ok(
            DeltaTableFieldDataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        ),
        Some(BrowserParquetConvertedType::TimestampMicros) => Ok(
            DeltaTableFieldDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ),
        Some(_) => Err(unsupported_datafusion_parquet_field(field)),
    }
}

fn datafusion_timestamp_time_unit(unit: &BrowserParquetTimeUnit) -> Option<TimeUnit> {
    match unit {
        BrowserParquetTimeUnit::Millis => Some(TimeUnit::Millisecond),
        BrowserParquetTimeUnit::Micros => Some(TimeUnit::Microsecond),
        BrowserParquetTimeUnit::Nanos => None,
    }
}

fn converted_timestamp_type_matches(
    converted_type: Option<&BrowserParquetConvertedType>,
    time_unit: TimeUnit,
) -> bool {
    matches!(
        (converted_type, time_unit),
        (None, _)
            | (
                Some(BrowserParquetConvertedType::TimestampMillis),
                TimeUnit::Millisecond
            )
            | (
                Some(BrowserParquetConvertedType::TimestampMicros),
                TimeUnit::Microsecond
            )
    )
}

fn datafusion_byte_array_type_from_parquet_field(
    field: &BrowserParquetField,
) -> Result<DeltaTableFieldDataType, QueryError> {
    match field.logical_type.as_ref() {
        Some(BrowserParquetLogicalType::String) | Some(BrowserParquetLogicalType::Json) => {
            return Ok(DeltaTableFieldDataType::Utf8);
        }
        Some(BrowserParquetLogicalType::Bson)
        | Some(BrowserParquetLogicalType::Enum)
        | Some(BrowserParquetLogicalType::Geometry { .. })
        | Some(BrowserParquetLogicalType::Geography { .. })
        | Some(BrowserParquetLogicalType::Unrecognized { .. }) => {
            return Ok(DeltaTableFieldDataType::Binary);
        }
        Some(_) => return Err(unsupported_datafusion_parquet_field(field)),
        None => {}
    }

    match field.converted_type.as_ref() {
        None => Ok(DeltaTableFieldDataType::Binary),
        Some(BrowserParquetConvertedType::Utf8) | Some(BrowserParquetConvertedType::Json) => {
            Ok(DeltaTableFieldDataType::Utf8)
        }
        Some(BrowserParquetConvertedType::Bson) | Some(BrowserParquetConvertedType::Enum) => {
            Ok(DeltaTableFieldDataType::Binary)
        }
        Some(_) => Err(unsupported_datafusion_parquet_field(field)),
    }
}

fn field_has_no_annotations(field: &BrowserParquetField) -> bool {
    field.logical_type.is_none() && field.converted_type.is_none()
}

fn unsupported_datafusion_parquet_field(field: &BrowserParquetField) -> QueryError {
    let logical_type = field
        .logical_type
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| "<none>".to_string());
    let converted_type = field
        .converted_type
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| "<none>".to_string());

    QueryError::new(
        QueryErrorCode::UnsupportedFeature,
        format!(
            "browser DataFusion schema does not yet support Parquet field '{}' with physical type {}, logical type {}, converted type {}, definition level {}, repetition level {}, repetition {}",
            field.name,
            field.physical_type,
            logical_type,
            converted_type,
            field.max_definition_level,
            field.max_repetition_level,
            field.repetition
        ),
        runtime_target(),
    )
}

fn runtime_result_from_datafusion(
    datafusion_result: DataFusionArrowIpcResult,
) -> RuntimeArrowIpcResult {
    RuntimeArrowIpcResult {
        ipc_bytes: datafusion_result.ipc_bytes.into(),
        row_count: datafusion_result.row_count,
        encoded_bytes: datafusion_result.encoded_bytes,
        scan_metrics: Vec::new(),
    }
}

fn datafusion_query_metrics(
    scan_metrics: DataFusionScanMetricsSummary,
    fallback_file_count: u64,
    footer_reads: Option<u64>,
    snapshot_bootstrap_duration_ms: Option<u64>,
    access_mode: Option<query_contract::BrowserAccessMode>,
    started_at: BrowserRuntimeInstant,
) -> QueryMetricsSummary {
    QueryMetricsSummary {
        bytes_fetched: scan_metrics.bytes_fetched,
        duration_ms: started_at.elapsed_ms(),
        files_touched: if scan_metrics.scan_count > 0 {
            scan_metrics.files_touched
        } else {
            fallback_file_count
        },
        files_skipped: scan_metrics.files_skipped,
        row_groups_touched: scan_metrics.row_groups_touched,
        row_groups_skipped: scan_metrics.row_groups_skipped,
        footer_reads,
        rows_emitted: scan_metrics.rows_emitted,
        snapshot_bootstrap_duration_ms,
        access_mode,
    }
}

fn validate_datafusion_request_match(
    table: &CachedDataFusionTable,
    request: &QueryRequest,
) -> Result<(), QueryError> {
    let table_uri = table.materialized.table_uri();
    if request.table_uri != table_uri {
        return Err(QueryError::new(
            QueryErrorCode::InvalidRequest,
            format!(
                "query table URI '{}' does not match open table URI '{}'",
                request.table_uri, table_uri
            ),
            runtime_target(),
        ));
    }

    if let Some(snapshot_version) = request.snapshot_version {
        let table_snapshot_version = table.materialized.snapshot_version();
        if snapshot_version != table_snapshot_version {
            return Err(QueryError::new(
                QueryErrorCode::InvalidRequest,
                format!(
                    "query snapshot version {snapshot_version} does not match open table snapshot version {table_snapshot_version}"
                ),
                runtime_target(),
            ));
        }
    }

    Ok(())
}

fn validate_datafusion_sql_scope(table_name: &str, sql: &str) -> Result<(), QueryError> {
    let statements = Parser::parse_sql(&GenericDialect {}, sql)
        .map_err(|error| invalid_datafusion_sql(format!("invalid SQL: {error}")))?;
    if statements.len() != 1 {
        return Err(invalid_datafusion_sql(
            "sql must contain exactly one read-only SELECT statement",
        ));
    }

    let statement = statements
        .into_iter()
        .next()
        .expect("statement length already checked");
    let SqlStatement::Query(query) = statement else {
        return Err(invalid_datafusion_sql(
            "only read-only SELECT statements are supported",
        ));
    };

    validate_datafusion_query_scope(table_name, query.as_ref())
}

fn validate_datafusion_query_scope(table_name: &str, query: &SqlQuery) -> Result<(), QueryError> {
    if query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
        || query.with.is_some()
    {
        return Err(invalid_datafusion_sql(
            "only read-only SELECT statements are supported",
        ));
    }

    match query.body.as_ref() {
        SqlSetExpr::Select(select) => {
            validate_datafusion_select_scope(table_name, select.as_ref())?;
            validate_datafusion_query_relation_scope(table_name, query)
        }
        SqlSetExpr::Query(query) => validate_datafusion_query_scope(table_name, query.as_ref()),
        _ => Err(invalid_datafusion_sql(
            "only read-only SELECT statements are supported",
        )),
    }
}

fn validate_datafusion_select_scope(table_name: &str, select: &Select) -> Result<(), QueryError> {
    if select.into.is_some()
        || select.top.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || select.connect_by.is_some()
        || select.flavor != SelectFlavor::Standard
    {
        return Err(invalid_datafusion_sql(
            "only read-only SELECT statements are supported",
        ));
    }

    if select.from.len() != 1 {
        return Err(wrong_datafusion_table_error(table_name));
    }

    validate_datafusion_table_with_joins(
        table_name,
        select.from.first().expect("select.from length checked"),
    )
}

fn validate_datafusion_table_with_joins(
    table_name: &str,
    table_with_joins: &SqlTableWithJoins,
) -> Result<(), QueryError> {
    if !table_with_joins.joins.is_empty() {
        return Err(wrong_datafusion_table_error(table_name));
    }

    match &table_with_joins.relation {
        SqlTableFactor::Table {
            name,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
            ..
        } => {
            if args.is_some()
                || !with_hints.is_empty()
                || version.is_some()
                || *with_ordinality
                || !partitions.is_empty()
                || json_path.is_some()
                || sample.is_some()
                || !index_hints.is_empty()
            {
                return Err(wrong_datafusion_table_error(table_name));
            }

            let relation_name = object_name_to_relation_name(name)
                .ok_or_else(|| wrong_datafusion_table_error(table_name))?;
            if relation_name == table_name.to_ascii_lowercase() {
                Ok(())
            } else {
                Err(wrong_datafusion_table_error(table_name))
            }
        }
        SqlTableFactor::Derived { subquery, .. } => {
            validate_datafusion_query_scope(table_name, subquery.as_ref())
        }
        _ => Err(wrong_datafusion_table_error(table_name)),
    }
}

fn validate_datafusion_query_relation_scope(
    table_name: &str,
    query: &SqlQuery,
) -> Result<(), QueryError> {
    let mut visitor = DataFusionRelationScopeVisitor {
        table_name: table_name.to_ascii_lowercase(),
    };
    match query.visit(&mut visitor) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(()) => Err(wrong_datafusion_table_error(table_name)),
    }
}

struct DataFusionRelationScopeVisitor {
    table_name: String,
}

impl Visitor for DataFusionRelationScopeVisitor {
    type Break = ();

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
        match object_name_to_relation_name(relation) {
            Some(relation_name) if relation_name == self.table_name => ControlFlow::Continue(()),
            _ => ControlFlow::Break(()),
        }
    }
}

fn object_name_to_relation_name(name: &ObjectName) -> Option<String> {
    let [part] = name.0.as_slice() else {
        return None;
    };

    Some(part.as_ident()?.value.to_ascii_lowercase())
}

fn wrong_datafusion_table_error(table_name: &str) -> QueryError {
    invalid_datafusion_sql(format!("query must read only from table '{table_name}'"))
}

fn invalid_datafusion_sql(message: impl Into<String>) -> QueryError {
    QueryError::new(
        QueryErrorCode::InvalidRequest,
        format!(
            "browser DataFusion SQL scope does not allow: {}",
            message.into()
        ),
        runtime_target(),
    )
}

fn datafusion_sql_with_result_page(
    sql: &str,
    result_page: Option<QueryResultPage>,
) -> Result<Cow<'_, str>, QueryError> {
    let Some(result_page) = result_page else {
        return Ok(Cow::Borrowed(sql));
    };

    if result_page.limit == 0 {
        return Err(QueryError::new(
            QueryErrorCode::InvalidRequest,
            "browser DataFusion result page limit must be at least 1",
            runtime_target(),
        ));
    }
    if result_page.limit > MAX_QUERY_RESULT_PAGE_LIMIT {
        return Err(QueryError::new(
            QueryErrorCode::InvalidRequest,
            format!(
                "browser DataFusion result page limit {} exceeds maximum {}",
                result_page.limit, MAX_QUERY_RESULT_PAGE_LIMIT
            ),
            runtime_target(),
        ));
    }

    let mut inner_sql = sql.trim();
    while let Some(stripped) = inner_sql.strip_suffix(';') {
        inner_sql = stripped.trim_end();
    }

    if inner_sql.is_empty() {
        return Err(QueryError::new(
            QueryErrorCode::InvalidRequest,
            "browser DataFusion result page requires non-empty SQL",
            runtime_target(),
        ));
    }

    if datafusion_sql_has_top_level_limit_clause(inner_sql)? {
        return Ok(Cow::Owned(format!(
            "SELECT * FROM ({inner_sql}) AS axon_result_page LIMIT {} OFFSET {}",
            result_page.limit, result_page.offset
        )));
    }

    Ok(Cow::Owned(format!(
        "{inner_sql} LIMIT {} OFFSET {}",
        result_page.limit, result_page.offset
    )))
}

fn datafusion_sql_has_top_level_limit_clause(sql: &str) -> Result<bool, QueryError> {
    let mut statements = Parser::parse_sql(&GenericDialect {}, sql)
        .map_err(|error| invalid_datafusion_sql(format!("invalid SQL: {error}")))?;
    let Some(statement) = statements.pop() else {
        return Ok(false);
    };
    let SqlStatement::Query(query) = statement else {
        return Ok(false);
    };

    Ok(query.limit_clause.is_some() || query.fetch.is_some())
}

fn datafusion_query_budget_from_runtime_config(
    config: &BrowserRuntimeConfig,
) -> BrowserDataFusionQueryBudget {
    config
        .execution_budget
        .map(|execution_budget| BrowserDataFusionQueryBudget {
            max_scan_bytes: Some(execution_budget.max_bytes),
            max_output_ipc_bytes: Some(execution_budget.max_bytes),
            max_batches_in_flight: None,
            max_rows_returned: Some(execution_budget.max_rows),
        })
        .unwrap_or_default()
}

fn datafusion_query_budget_for_request(
    session_budget: BrowserDataFusionQueryBudget,
    request_limits: Option<QueryRuntimeLimits>,
) -> BrowserDataFusionQueryBudget {
    let Some(request_limits) = request_limits else {
        return session_budget;
    };

    BrowserDataFusionQueryBudget {
        max_scan_bytes: min_optional_budget(
            session_budget.max_scan_bytes,
            request_limits.max_scan_bytes,
        ),
        max_output_ipc_bytes: min_optional_budget(
            session_budget.max_output_ipc_bytes,
            request_limits.max_arrow_ipc_bytes,
        ),
        max_batches_in_flight: session_budget.max_batches_in_flight,
        max_rows_returned: min_optional_budget(
            session_budget.max_rows_returned,
            request_limits.max_result_rows,
        ),
    }
}

fn min_optional_budget(session_limit: Option<u64>, request_limit: Option<u64>) -> Option<u64> {
    match (session_limit, request_limit) {
        (Some(session_limit), Some(request_limit)) => Some(session_limit.min(request_limit)),
        (Some(session_limit), None) => Some(session_limit),
        (None, Some(request_limit)) => Some(request_limit),
        (None, None) => None,
    }
}

fn validate_datafusion_execution_budget(
    table: &CachedDataFusionTable,
    execution_budget: Option<BrowserExecutionBudget>,
) -> Result<(), QueryError> {
    let Some(execution_budget) = execution_budget else {
        return Ok(());
    };

    let estimated_rows = datafusion_table_estimated_rows(table)?;
    if estimated_rows > execution_budget.max_rows {
        return Err(datafusion_budget_exceeded_error(
            "row estimate",
            estimated_rows,
            execution_budget.max_rows,
        ));
    }

    let estimated_bytes = datafusion_table_estimated_bytes(table)?;
    if estimated_bytes > execution_budget.max_bytes {
        return Err(datafusion_budget_exceeded_error(
            "byte estimate",
            estimated_bytes,
            execution_budget.max_bytes,
        ));
    }

    Ok(())
}

fn datafusion_table_estimated_rows(table: &CachedDataFusionTable) -> Result<u64, QueryError> {
    table
        .bootstrapped
        .as_ref()
        .map(|snapshot| {
            snapshot
                .active_files()
                .iter()
                .try_fold(0_u64, |total, file| {
                    total
                        .checked_add(file.metadata().row_count)
                        .ok_or_else(|| datafusion_budget_total_overflow_error("row"))
                })
        })
        .unwrap_or(Ok(0))
}

fn datafusion_table_estimated_bytes(table: &CachedDataFusionTable) -> Result<u64, QueryError> {
    table
        .materialized
        .active_files()
        .iter()
        .try_fold(0_u64, |total, file| {
            total
                .checked_add(file.size_bytes())
                .ok_or_else(|| datafusion_budget_total_overflow_error("byte"))
        })
}

fn datafusion_budget_total_overflow_error(total_kind: &str) -> QueryError {
    QueryError::new(
        QueryErrorCode::ExecutionFailed,
        format!("browser DataFusion {total_kind} totals overflowed u64"),
        runtime_target(),
    )
}

fn datafusion_budget_exceeded_error(
    observed_kind: &str,
    observed: u64,
    max_allowed: u64,
) -> QueryError {
    QueryError::new(
        QueryErrorCode::FallbackRequired,
        format!(
            "browser DataFusion execution exceeded the configured {observed_kind} budget ({observed} > {max_allowed})"
        ),
        runtime_target(),
    )
    .with_fallback_reason(FallbackReason::BrowserRuntimeConstraint)
}

fn cached_table_bytes(
    materialized: &MaterializedBrowserSnapshot,
    bootstrapped: Option<&BootstrappedBrowserSnapshot>,
) -> Result<u64, QueryError> {
    let materialized_bytes = materialized_snapshot_bytes(materialized)?;
    let bootstrapped_bytes = bootstrapped
        .map(bootstrapped_snapshot_bytes)
        .transpose()?
        .unwrap_or(0);

    materialized_bytes
        .checked_add(bootstrapped_bytes)
        .ok_or_else(|| {
            QueryError::new(
                QueryErrorCode::ExecutionFailed,
                "browser DataFusion cached table size overflowed u64",
                runtime_target(),
            )
        })
}

fn materialized_snapshot_bytes(snapshot: &MaterializedBrowserSnapshot) -> Result<u64, QueryError> {
    snapshot
        .active_files()
        .iter()
        .try_fold(0_u64, |total, file| {
            total.checked_add(file.size_bytes()).ok_or_else(|| {
                QueryError::new(
                    QueryErrorCode::ExecutionFailed,
                    "browser DataFusion materialized snapshot size overflowed u64",
                    runtime_target(),
                )
            })
        })
}

fn bootstrapped_snapshot_bytes(snapshot: &BootstrappedBrowserSnapshot) -> Result<u64, QueryError> {
    snapshot
        .active_files()
        .iter()
        .try_fold(0_u64, |total, file| {
            let metadata = file.metadata();
            total
                .checked_add(metadata.object_size_bytes)
                .and_then(|value| value.checked_add(u64::from(metadata.footer_length_bytes)))
                .ok_or_else(|| {
                    QueryError::new(
                        QueryErrorCode::ExecutionFailed,
                        "browser DataFusion bootstrapped snapshot size overflowed u64",
                        runtime_target(),
                    )
                })
        })
}

fn missing_table_error(name: &str) -> QueryError {
    QueryError::new(
        QueryErrorCode::InvalidRequest,
        format!("browser DataFusion session does not have an open table named '{name}'"),
        runtime_target(),
    )
}

fn missing_table_file_error(name: &str, path: &str) -> QueryError {
    QueryError::new(
        QueryErrorCode::InvalidRequest,
        format!(
            "browser DataFusion session table '{name}' does not have an active parquet file at path '{path}'"
        ),
        runtime_target(),
    )
}

pub fn memory_baseline_bytes() -> u64 {
    size_of::<BrowserDataFusionSession>() as u64
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    use query_contract::{BrowserHttpFileDescriptor, CapabilityReport};
    use wasm_query_runtime::{
        BrowserObjectSource, MaterializedBrowserFile, MaterializedBrowserSnapshot,
    };

    #[test]
    fn open_delta_table_registers_descriptor_and_sql_returns_arrow_ipc() {
        let mut session = BrowserDataFusionSession::new(BrowserRuntimeConfig::default(), u64::MAX)
            .expect("default browser runtime config should be supported");
        let descriptor = empty_delta_descriptor();
        let request = QueryRequest::new(
            descriptor.table_uri.clone(),
            "SELECT COUNT(*) AS rows FROM events",
            ExecutionTarget::BrowserWasm,
        );

        test_runtime().block_on(async {
            session
                .open_delta_table("events", descriptor)
                .await
                .expect("open_delta_table should register the DataFusion table");

            let result = session
                .sql("events", &request)
                .await
                .expect("SQL should execute through the DataFusion session");

            assert!(session.contains_table("events"));
            assert!(!result.runtime_result.ipc_bytes.is_empty());
            assert_eq!(result.runtime_result.row_count, 1);
            assert_eq!(result.response.executed_on, ExecutionTarget::BrowserWasm);
            assert_eq!(result.response.metrics.rows_emitted, 0);
            assert!(result.response.explain.is_none());
        });
    }

    #[test]
    fn datafusion_session_sql_returns_explain_when_requested() {
        let mut session = BrowserDataFusionSession::new(BrowserRuntimeConfig::default(), u64::MAX)
            .expect("default browser runtime config should be supported");
        let descriptor = empty_delta_descriptor();
        let mut request = QueryRequest::new(
            descriptor.table_uri.clone(),
            "SELECT COUNT(*) AS rows FROM events",
            ExecutionTarget::BrowserWasm,
        );
        request.options.include_explain = true;

        let result = test_runtime()
            .block_on(async {
                session
                    .open_delta_table("events", descriptor)
                    .await
                    .expect("open_delta_table should register the DataFusion table");
                session.sql("events", &request).await
            })
            .expect("SQL should execute through the DataFusion session");

        let explain = result
            .response
            .explain
            .as_deref()
            .expect("include_explain should populate response.explain");
        assert!(
            explain.contains("DataFusion"),
            "explain text should identify the DataFusion plan: {explain}"
        );
        assert!(
            explain.contains("events"),
            "explain text should reference the registered table: {explain}"
        );
    }

    #[test]
    fn datafusion_session_applies_request_runtime_limits() {
        let mut session = BrowserDataFusionSession::new(BrowserRuntimeConfig::default(), u64::MAX)
            .expect("default browser runtime config should be supported");
        let descriptor = empty_delta_descriptor();
        let request = QueryRequest::new(
            descriptor.table_uri.clone(),
            "SELECT COUNT(*) AS rows FROM events",
            ExecutionTarget::BrowserWasm,
        )
        .with_options(query_contract::QueryExecutionOptions {
            runtime_limits: Some(query_contract::QueryRuntimeLimits {
                max_result_rows: None,
                max_arrow_ipc_bytes: Some(1),
                max_scan_bytes: None,
            }),
            ..query_contract::QueryExecutionOptions::default()
        });

        let error = test_runtime()
            .block_on(async {
                session
                    .open_delta_table("events", descriptor)
                    .await
                    .expect("open_delta_table should register the DataFusion table");
                session.sql("events", &request).await
            })
            .expect_err("per-request Arrow IPC budget should be enforced");

        assert_eq!(error.code, QueryErrorCode::FallbackRequired);
        assert!(
            error.message.contains("max_output_ipc_bytes"),
            "error should describe the request runtime budget: {}",
            error.message
        );
    }

    #[test]
    fn result_page_wraps_datafusion_sql_with_limit_and_offset() {
        let page = QueryResultPage {
            limit: 501,
            offset: 500,
        };

        let paged_sql = datafusion_sql_with_result_page("SELECT * FROM events;", Some(page))
            .expect("valid SELECT should be pageable");

        assert_eq!(
            paged_sql.as_ref(),
            "SELECT * FROM events LIMIT 501 OFFSET 500"
        );
    }

    #[test]
    fn result_page_rejects_limits_over_runtime_limit() {
        let error = datafusion_sql_with_result_page(
            "SELECT * FROM events",
            Some(QueryResultPage {
                limit: MAX_QUERY_RESULT_PAGE_LIMIT + 1,
                offset: 0,
            }),
        )
        .expect_err("oversized result pages should be rejected");

        assert_eq!(error.code, QueryErrorCode::InvalidRequest);
        assert!(
            error.message.contains("result page limit"),
            "error should describe the oversized result page limit: {}",
            error.message
        );
    }

    #[test]
    fn result_page_wraps_datafusion_sql_with_top_level_offset() {
        let page = QueryResultPage {
            limit: 501,
            offset: 500,
        };

        let paged_sql = datafusion_sql_with_result_page(
            "SELECT * FROM events ORDER BY id OFFSET 10;",
            Some(page),
        )
        .expect("valid SELECT with OFFSET should be pageable");

        assert_eq!(
            paged_sql.as_ref(),
            "SELECT * FROM (SELECT * FROM events ORDER BY id OFFSET 10) AS axon_result_page LIMIT 501 OFFSET 500"
        );
    }

    #[test]
    fn sql_requires_table_to_be_opened_before_query_execution() {
        let mut session = BrowserDataFusionSession::new(BrowserRuntimeConfig::default(), u64::MAX)
            .expect("default browser runtime config should be supported");
        let request = QueryRequest::new(
            "gs://axon-fixtures/empty",
            "SELECT COUNT(*) AS rows FROM events",
            ExecutionTarget::BrowserWasm,
        );

        let error = test_runtime()
            .block_on(session.sql("events", &request))
            .expect_err("SQL must fail before the table is opened");

        assert_eq!(error.code, QueryErrorCode::InvalidRequest);
        assert!(error.message.contains("open table named 'events'"));
    }

    #[test]
    fn datafusion_sql_rejects_request_for_a_different_table_uri() {
        let mut session = BrowserDataFusionSession::new(BrowserRuntimeConfig::default(), u64::MAX)
            .expect("default browser runtime config should be supported");
        let descriptor = empty_delta_descriptor();
        let request = QueryRequest::new(
            "gs://axon-fixtures/different",
            "SELECT COUNT(*) AS rows FROM events",
            ExecutionTarget::BrowserWasm,
        );

        let error = test_runtime()
            .block_on(async {
                session
                    .open_delta_table("events", descriptor)
                    .await
                    .expect("open_delta_table should register the DataFusion table");
                session.sql("events", &request).await
            })
            .expect_err("DataFusion SQL should keep table/query descriptor matching");

        assert_eq!(error.code, QueryErrorCode::InvalidRequest);
        assert!(error.message.contains("does not match open table"));
    }

    #[test]
    fn disposed_datafusion_table_can_be_reopened_with_the_same_name() {
        let mut session = BrowserDataFusionSession::new(BrowserRuntimeConfig::default(), u64::MAX)
            .expect("default browser runtime config should be supported");

        test_runtime().block_on(async {
            session
                .open_delta_table("events", empty_delta_descriptor())
                .await
                .expect("first open should register the DataFusion table");
            assert!(session.dispose_table("events"));

            session
                .open_delta_table("events", empty_delta_descriptor())
                .await
                .expect("dispose should deregister the DataFusion table name");
        });
    }

    #[test]
    fn opening_existing_datafusion_table_replaces_registered_descriptor() {
        let mut session = BrowserDataFusionSession::new(BrowserRuntimeConfig::default(), u64::MAX)
            .expect("default browser runtime config should be supported");
        let first_descriptor = delta_descriptor("gs://axon-fixtures/empty-v1", 1);
        let second_descriptor = delta_descriptor("gs://axon-fixtures/empty-v2", 2);
        let mut second_request = QueryRequest::new(
            second_descriptor.table_uri.clone(),
            "SELECT COUNT(*) AS rows FROM events",
            ExecutionTarget::BrowserWasm,
        );
        second_request.snapshot_version = Some(second_descriptor.snapshot_version);
        let first_request = QueryRequest::new(
            first_descriptor.table_uri.clone(),
            "SELECT COUNT(*) AS rows FROM events",
            ExecutionTarget::BrowserWasm,
        );

        test_runtime().block_on(async {
            session
                .open_delta_table("events", first_descriptor)
                .await
                .expect("first open should register the DataFusion table");
            session
                .open_delta_table("events", second_descriptor)
                .await
                .expect("same-name open should replace the registered DataFusion table");

            session
                .sql("events", &second_request)
                .await
                .expect("query should run against the replacement descriptor");
            let error = session
                .sql("events", &first_request)
                .await
                .expect_err("old descriptor should no longer match the open table");

            assert_eq!(error.code, QueryErrorCode::InvalidRequest);
            assert!(error.message.contains("does not match open table"));
        });
    }

    #[test]
    fn datafusion_sql_rejects_non_select_statements_before_execution() {
        let mut session = BrowserDataFusionSession::new(BrowserRuntimeConfig::default(), u64::MAX)
            .expect("default browser runtime config should be supported");
        let request = QueryRequest::new(
            "gs://axon-fixtures/empty",
            "CREATE TABLE copied AS SELECT * FROM events",
            ExecutionTarget::BrowserWasm,
        );

        let error = test_runtime()
            .block_on(async {
                session
                    .open_delta_table("events", empty_delta_descriptor())
                    .await
                    .expect("open_delta_table should register the DataFusion table");
                session.sql("events", &request).await
            })
            .expect_err("DataFusion SQL should reject non-SELECT statements");

        assert_eq!(error.code, QueryErrorCode::InvalidRequest);
        assert!(error.message.contains("read-only SELECT"));
    }

    #[test]
    fn datafusion_sql_rejects_scalar_subquery_against_other_open_table() {
        let mut session = BrowserDataFusionSession::new(BrowserRuntimeConfig::default(), u64::MAX)
            .expect("default browser runtime config should be supported");
        let events = delta_descriptor("gs://axon-fixtures/events", 1);
        let metrics = delta_descriptor("gs://axon-fixtures/metrics", 1);
        let request = QueryRequest::new(
            events.table_uri.clone(),
            "SELECT (SELECT COUNT(*) FROM metrics) AS leaked FROM events",
            ExecutionTarget::BrowserWasm,
        );

        let error = test_runtime()
            .block_on(async {
                session
                    .open_delta_table("events", events)
                    .await
                    .expect("events table should open");
                session
                    .open_delta_table("metrics", metrics)
                    .await
                    .expect("metrics table should open");
                session.sql("events", &request).await
            })
            .expect_err("DataFusion SQL should reject nested reads from another table");

        assert_eq!(error.code, QueryErrorCode::InvalidRequest);
        assert!(error.message.contains("read only from table 'events'"));
    }

    #[test]
    fn datafusion_sql_rejects_exists_subquery_against_other_open_table() {
        let mut session = BrowserDataFusionSession::new(BrowserRuntimeConfig::default(), u64::MAX)
            .expect("default browser runtime config should be supported");
        let events = delta_descriptor("gs://axon-fixtures/events", 1);
        let metrics = delta_descriptor("gs://axon-fixtures/metrics", 1);
        let request = QueryRequest::new(
            events.table_uri.clone(),
            "SELECT COUNT(*) AS rows FROM events WHERE EXISTS (SELECT 1 FROM metrics)",
            ExecutionTarget::BrowserWasm,
        );

        let error = test_runtime()
            .block_on(async {
                session
                    .open_delta_table("events", events)
                    .await
                    .expect("events table should open");
                session
                    .open_delta_table("metrics", metrics)
                    .await
                    .expect("metrics table should open");
                session.sql("events", &request).await
            })
            .expect_err("DataFusion SQL should reject predicate subqueries from another table");

        assert_eq!(error.code, QueryErrorCode::InvalidRequest);
        assert!(error.message.contains("read only from table 'events'"));
    }

    #[test]
    fn datafusion_sql_rejects_tables_over_configured_byte_budget() {
        let table = CachedDataFusionTable {
            descriptor: empty_delta_descriptor(),
            materialized: large_materialized_snapshot(),
            bootstrapped: None,
            cached_bytes: 128,
            last_used_millis: 1,
        };
        let budget = BrowserExecutionBudget {
            max_rows: u64::MAX,
            max_bytes: 1,
        };

        let error = validate_datafusion_execution_budget(&table, Some(budget))
            .expect_err("DataFusion SQL should reject tables over the byte budget");

        assert_eq!(error.code, QueryErrorCode::FallbackRequired);
    }

    #[test]
    fn schema_adapter_unannotated_byte_array_parquet_field_maps_to_datafusion_binary() {
        let field = byte_array_field("payload", None, None);

        let data_type = datafusion_data_type_from_parquet_field(&field)
            .expect("browser DataFusion should use Arrow binary for unannotated BYTE_ARRAY");

        assert_eq!(data_type, DeltaTableFieldDataType::Binary);
    }

    #[test]
    fn schema_adapter_string_byte_array_parquet_fields_map_to_datafusion_utf8() {
        let cases = [
            byte_array_field(
                "string_logical",
                Some(BrowserParquetLogicalType::String),
                None,
            ),
            byte_array_field(
                "utf8_converted",
                None,
                Some(BrowserParquetConvertedType::Utf8),
            ),
        ];

        for field in cases {
            let data_type = datafusion_data_type_from_parquet_field(&field)
                .expect("browser DataFusion should mirror Arrow string BYTE_ARRAY mapping");

            assert_eq!(
                data_type,
                DeltaTableFieldDataType::Utf8,
                "field {}",
                field.name
            );
        }
    }

    #[test]
    fn schema_adapter_json_byte_array_parquet_fields_map_to_datafusion_utf8() {
        let cases = [
            byte_array_field("json_logical", Some(BrowserParquetLogicalType::Json), None),
            byte_array_field(
                "json_converted",
                None,
                Some(BrowserParquetConvertedType::Json),
            ),
        ];

        for field in cases {
            let data_type = datafusion_data_type_from_parquet_field(&field)
                .expect("browser DataFusion should mirror Arrow JSON BYTE_ARRAY mapping");

            assert_eq!(
                data_type,
                DeltaTableFieldDataType::Utf8,
                "field {}",
                field.name
            );
        }
    }

    #[test]
    fn schema_adapter_arrow_binary_byte_array_annotations_map_to_datafusion_binary() {
        let cases = [
            byte_array_field("bson_logical", Some(BrowserParquetLogicalType::Bson), None),
            byte_array_field(
                "bson_converted",
                None,
                Some(BrowserParquetConvertedType::Bson),
            ),
            byte_array_field("enum_logical", Some(BrowserParquetLogicalType::Enum), None),
            byte_array_field(
                "enum_converted",
                None,
                Some(BrowserParquetConvertedType::Enum),
            ),
            byte_array_field(
                "geometry_logical",
                Some(BrowserParquetLogicalType::Geometry { crs: None }),
                None,
            ),
            byte_array_field(
                "geography_logical",
                Some(BrowserParquetLogicalType::Geography {
                    crs: None,
                    algorithm: None,
                }),
                None,
            ),
            byte_array_field(
                "unrecognized_logical",
                Some(BrowserParquetLogicalType::Unrecognized { field_id: 123 }),
                None,
            ),
        ];

        for field in cases {
            let data_type = datafusion_data_type_from_parquet_field(&field)
                .expect("browser DataFusion should mirror Arrow binary BYTE_ARRAY mapping");

            assert_eq!(
                data_type,
                DeltaTableFieldDataType::Binary,
                "field {}",
                field.name
            );
        }
    }

    #[test]
    fn schema_adapter_date_parquet_fields_map_to_datafusion_date32() {
        let cases = [
            parquet_field(
                "date_logical",
                BrowserParquetPhysicalType::Int32,
                Some(BrowserParquetLogicalType::Date),
                None,
            ),
            parquet_field(
                "date_converted",
                BrowserParquetPhysicalType::Int32,
                None,
                Some(BrowserParquetConvertedType::Date),
            ),
            parquet_field(
                "date_logical_and_converted",
                BrowserParquetPhysicalType::Int32,
                Some(BrowserParquetLogicalType::Date),
                Some(BrowserParquetConvertedType::Date),
            ),
        ];

        for field in cases {
            let data_type = datafusion_data_type_from_parquet_field(&field)
                .expect("browser DataFusion should mirror Arrow DATE mapping");

            assert_eq!(
                data_type,
                DeltaTableFieldDataType::Date32,
                "field {}",
                field.name
            );
        }
    }

    #[test]
    fn schema_adapter_utc_timestamp_parquet_fields_map_to_datafusion_timestamps() {
        let cases = [
            (
                parquet_field(
                    "timestamp_millis_logical",
                    BrowserParquetPhysicalType::Int64,
                    Some(BrowserParquetLogicalType::Timestamp {
                        is_adjusted_to_utc: true,
                        unit: BrowserParquetTimeUnit::Millis,
                    }),
                    None,
                ),
                DeltaTableFieldDataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            ),
            (
                parquet_field(
                    "timestamp_micros_logical",
                    BrowserParquetPhysicalType::Int64,
                    Some(BrowserParquetLogicalType::Timestamp {
                        is_adjusted_to_utc: true,
                        unit: BrowserParquetTimeUnit::Micros,
                    }),
                    None,
                ),
                DeltaTableFieldDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
            (
                parquet_field(
                    "timestamp_millis_converted",
                    BrowserParquetPhysicalType::Int64,
                    None,
                    Some(BrowserParquetConvertedType::TimestampMillis),
                ),
                DeltaTableFieldDataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            ),
            (
                parquet_field(
                    "timestamp_micros_converted",
                    BrowserParquetPhysicalType::Int64,
                    None,
                    Some(BrowserParquetConvertedType::TimestampMicros),
                ),
                DeltaTableFieldDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
        ];

        for (field, expected) in cases {
            let data_type = datafusion_data_type_from_parquet_field(&field)
                .expect("browser DataFusion should mirror Arrow UTC timestamp mapping");

            assert_eq!(data_type, expected, "field {}", field.name);
        }
    }

    #[test]
    fn schema_adapter_unsupported_timestamp_shapes_remain_explicitly_unsupported() {
        let cases = [
            parquet_field(
                "timestamp_local_micros",
                BrowserParquetPhysicalType::Int64,
                Some(BrowserParquetLogicalType::Timestamp {
                    is_adjusted_to_utc: false,
                    unit: BrowserParquetTimeUnit::Micros,
                }),
                None,
            ),
            parquet_field(
                "timestamp_nanos",
                BrowserParquetPhysicalType::Int64,
                Some(BrowserParquetLogicalType::Timestamp {
                    is_adjusted_to_utc: true,
                    unit: BrowserParquetTimeUnit::Nanos,
                }),
                None,
            ),
            parquet_field(
                "time_micros",
                BrowserParquetPhysicalType::Int64,
                Some(BrowserParquetLogicalType::Time {
                    is_adjusted_to_utc: true,
                    unit: BrowserParquetTimeUnit::Micros,
                }),
                None,
            ),
            parquet_field(
                "timestamp_conflicting_converted",
                BrowserParquetPhysicalType::Int64,
                Some(BrowserParquetLogicalType::Timestamp {
                    is_adjusted_to_utc: true,
                    unit: BrowserParquetTimeUnit::Millis,
                }),
                Some(BrowserParquetConvertedType::TimestampMicros),
            ),
        ];

        for field in cases {
            let error = datafusion_data_type_from_parquet_field(&field)
                .expect_err("unsupported timestamp field shapes should fail explicitly");

            assert_eq!(error.code, QueryErrorCode::UnsupportedFeature);
            assert!(
                error.message.contains(&field.name),
                "error should include the unsupported field name: {}",
                error.message
            );
        }
    }

    #[test]
    fn schema_adapter_unsupported_parquet_shapes_remain_explicitly_unsupported() {
        let cases = [
            parquet_field(
                "list_payload",
                BrowserParquetPhysicalType::ByteArray,
                Some(BrowserParquetLogicalType::List),
                None,
            ),
            parquet_field(
                "map_payload",
                BrowserParquetPhysicalType::ByteArray,
                Some(BrowserParquetLogicalType::Map),
                None,
            ),
            parquet_field(
                "variant_payload",
                BrowserParquetPhysicalType::ByteArray,
                Some(BrowserParquetLogicalType::Variant {
                    specification_version: Some(1),
                }),
                None,
            ),
            parquet_field(
                "unknown_payload",
                BrowserParquetPhysicalType::ByteArray,
                Some(BrowserParquetLogicalType::Unknown),
                None,
            ),
            parquet_field(
                "decimal_payload",
                BrowserParquetPhysicalType::ByteArray,
                Some(BrowserParquetLogicalType::Decimal {
                    scale: 2,
                    precision: 10,
                }),
                None,
            ),
            parquet_field(
                "fixed_payload",
                BrowserParquetPhysicalType::FixedLenByteArray,
                None,
                None,
            ),
            parquet_field(
                "legacy_timestamp",
                BrowserParquetPhysicalType::Int96,
                None,
                None,
            ),
        ];

        for field in cases {
            let error = datafusion_data_type_from_parquet_field(&field)
                .expect_err("unsupported browser field shapes should fail explicitly");

            assert_eq!(error.code, QueryErrorCode::UnsupportedFeature);
            assert!(
                error.message.contains("logical type"),
                "error should include annotation details: {}",
                error.message
            );
            assert!(
                error.message.contains("converted type"),
                "error should include annotation details: {}",
                error.message
            );
        }
    }

    #[test]
    fn unsupported_classification_messages_use_stable_runtime_categories() {
        let schema_error = datafusion_data_type_from_parquet_field(&parquet_field(
            "timestamp_local_micros",
            BrowserParquetPhysicalType::Int64,
            Some(BrowserParquetLogicalType::Timestamp {
                is_adjusted_to_utc: false,
                unit: BrowserParquetTimeUnit::Micros,
            }),
            None,
        ))
        .expect_err("local timestamp schema should remain unsupported");

        assert_eq!(schema_error.code, QueryErrorCode::UnsupportedFeature);
        assert!(
            schema_error
                .message
                .starts_with("browser DataFusion schema does not yet support"),
            "schema error should use stable category wording: {}",
            schema_error.message
        );

        let sql_error =
            validate_datafusion_sql_scope("events", "CREATE TABLE copied AS SELECT * FROM events")
                .expect_err("non-SELECT SQL should remain outside browser DataFusion scope");

        assert_eq!(sql_error.code, QueryErrorCode::InvalidRequest);
        assert!(
            sql_error
                .message
                .starts_with("browser DataFusion SQL scope does not allow"),
            "SQL scope error should use stable category wording: {}",
            sql_error.message
        );
    }

    #[test]
    fn schema_adapter_repeated_nested_parquet_shapes_remain_explicitly_unsupported() {
        let mut field = parquet_field(
            "repeated_list_payload",
            BrowserParquetPhysicalType::ByteArray,
            Some(BrowserParquetLogicalType::List),
            None,
        );
        field.repetition = BrowserParquetRepetition::Repeated;
        field.max_repetition_level = 1;

        let error = datafusion_data_type_from_parquet_field(&field)
            .expect_err("actual repeated browser field shapes should fail explicitly");

        assert_eq!(error.code, QueryErrorCode::UnsupportedFeature);
        assert!(
            error.message.contains("repeated_list_payload"),
            "error should include the unsupported field name: {}",
            error.message
        );
        assert!(
            error.message.contains("physical type BYTE_ARRAY"),
            "error should include physical type details: {}",
            error.message
        );
        assert!(
            error.message.contains("logical type LIST"),
            "error should include logical type details: {}",
            error.message
        );
        assert!(
            error.message.contains("converted type <none>"),
            "error should include converted type details: {}",
            error.message
        );
    }

    #[test]
    fn schema_adapter_non_repeated_nested_leaf_remains_explicitly_unsupported() {
        let mut field = parquet_field(
            "nested_payload",
            BrowserParquetPhysicalType::ByteArray,
            None,
            None,
        );
        field.repetition = BrowserParquetRepetition::Optional;
        field.max_definition_level = 2;
        field.max_repetition_level = 0;

        let error = datafusion_data_type_from_parquet_field(&field)
            .expect_err("non-repeated nested leaf field shapes should fail explicitly");

        assert_eq!(error.code, QueryErrorCode::UnsupportedFeature);
        assert!(
            error.message.contains("nested_payload"),
            "error should include the unsupported field name: {}",
            error.message
        );
        assert!(
            error.message.contains("physical type BYTE_ARRAY"),
            "error should include physical type details: {}",
            error.message
        );
        assert!(
            error.message.contains("definition level 2"),
            "error should include definition level details: {}",
            error.message
        );
        assert!(
            error.message.contains("repetition level 0"),
            "error should include repetition level details: {}",
            error.message
        );
    }

    fn byte_array_field(
        name: &str,
        logical_type: Option<BrowserParquetLogicalType>,
        converted_type: Option<BrowserParquetConvertedType>,
    ) -> BrowserParquetField {
        parquet_field(
            name,
            BrowserParquetPhysicalType::ByteArray,
            logical_type,
            converted_type,
        )
    }

    fn parquet_field(
        name: &str,
        physical_type: BrowserParquetPhysicalType,
        logical_type: Option<BrowserParquetLogicalType>,
        converted_type: Option<BrowserParquetConvertedType>,
    ) -> BrowserParquetField {
        BrowserParquetField {
            name: name.to_string(),
            physical_type,
            logical_type,
            converted_type,
            repetition: BrowserParquetRepetition::Optional,
            nullable: true,
            max_definition_level: 1,
            max_repetition_level: 0,
            type_length: None,
            precision: None,
            scale: None,
        }
    }

    fn empty_delta_descriptor() -> BrowserHttpSnapshotDescriptor {
        delta_descriptor("gs://axon-fixtures/empty", 1)
    }

    fn delta_descriptor(table_uri: &str, snapshot_version: i64) -> BrowserHttpSnapshotDescriptor {
        BrowserHttpSnapshotDescriptor {
            table_uri: table_uri.to_string(),
            snapshot_version,
            partition_column_types: BTreeMap::new(),
            browser_compatibility: CapabilityReport::default(),
            required_capabilities: CapabilityReport::default(),
            active_files: Vec::<BrowserHttpFileDescriptor>::new(),
        }
    }

    fn large_materialized_snapshot() -> MaterializedBrowserSnapshot {
        let file = MaterializedBrowserFile::new(
            "part-000.parquet",
            128,
            BTreeMap::new(),
            BrowserObjectSource::from_url("https://example.invalid/part-000.parquet")
                .expect("fixture URL should be browser-safe"),
        );
        MaterializedBrowserSnapshot::new("gs://axon-fixtures/large", 1, vec![file])
            .expect("large snapshot should construct")
    }

    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Runtime::new().expect("tokio runtime should be created")
    }
}
