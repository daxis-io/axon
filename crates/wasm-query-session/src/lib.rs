//! In-memory browser session shell over runtime-owned snapshot state.

use std::collections::BTreeMap;
use std::mem::size_of;
use std::time::Instant;

#[cfg(feature = "datafusion")]
use std::collections::BTreeSet;

use query_contract::{
    BrowserHttpSnapshotDescriptor, ExecutionTarget, QueryError, QueryErrorCode,
    QueryMetricsSummary, QueryRequest, QueryResponse,
};
#[cfg(feature = "datafusion")]
use query_contract::{FallbackReason, PartitionColumnType};
#[cfg(feature = "datafusion")]
use sqlparser::ast::{
    ObjectName, Query as SqlQuery, Select, SelectFlavor, SetExpr as SqlSetExpr,
    Statement as SqlStatement, TableFactor as SqlTableFactor, TableWithJoins as SqlTableWithJoins,
};
#[cfg(feature = "datafusion")]
use sqlparser::dialect::GenericDialect;
#[cfg(feature = "datafusion")]
use sqlparser::parser::Parser;
#[cfg(feature = "datafusion")]
use wasm_datafusion_poc::{
    DataFusionArrowIpcResult, DataFusionScanMetricsSummary, DeltaTableDescriptor,
    DeltaTableFieldDataType, DeltaTableSchema, DeltaTableSchemaField, WasmDataFusionEngine,
};
use wasm_query_runtime::{
    runtime_target, BootstrappedBrowserFile, BootstrappedBrowserSnapshot, BrowserExecutionPlan,
    BrowserPlannedQuery, BrowserRuntimeConfig, BrowserRuntimeSession, MaterializedBrowserSnapshot,
    RuntimeArrowIpcResult,
};
#[cfg(feature = "datafusion")]
use wasm_query_runtime::{
    BrowserExecutionBudget, BrowserParquetConvertedType, BrowserParquetField,
    BrowserParquetLogicalType, BrowserParquetPhysicalType,
};

pub const OWNER: &str = "Runtime / engine team";
pub const RESPONSIBILITY: &str =
    "In-memory browser session caching for runtime-owned descriptors and snapshots.";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BrowserQueryBudget {
    pub max_scan_bytes: Option<u64>,
    pub max_output_ipc_bytes: Option<u64>,
    pub max_batches_in_flight: Option<usize>,
    pub max_rows_returned: Option<u64>,
}

#[cfg(feature = "datafusion")]
impl From<BrowserQueryBudget> for wasm_datafusion_poc::BrowserQueryBudget {
    fn from(budget: BrowserQueryBudget) -> Self {
        Self {
            max_scan_bytes: budget.max_scan_bytes,
            max_output_ipc_bytes: budget.max_output_ipc_bytes,
            max_batches_in_flight: budget.max_batches_in_flight,
            max_rows_returned: budget.max_rows_returned,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserSessionQueryResult {
    pub response: QueryResponse,
    pub runtime_result: RuntimeArrowIpcResult,
}

#[derive(Clone, Debug)]
pub struct BrowserQuerySession {
    runtime: BrowserRuntimeSession,
    query_budget: BrowserQueryBudget,
    #[cfg(feature = "datafusion")]
    datafusion: WasmDataFusionEngine,
    tables: BTreeMap<String, CachedTable>,
    max_cached_bytes: u64,
    next_access_millis: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CachedTable {
    descriptor: Option<BrowserHttpSnapshotDescriptor>,
    materialized: MaterializedBrowserSnapshot,
    bootstrapped: Option<BootstrappedBrowserSnapshot>,
    bootstrapped_request: Option<CachedBootstrapRequest>,
    datafusion_registered: bool,
    cached_bytes: u64,
    last_used_millis: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CachedBootstrapRequest {
    table_uri: String,
    snapshot_version: Option<i64>,
    sql: String,
}

impl BrowserQuerySession {
    pub fn new(config: BrowserRuntimeConfig, max_cached_bytes: u64) -> Result<Self, QueryError> {
        let query_budget = datafusion_query_budget_from_runtime_config(&config);
        Self::new_with_query_budget(config, max_cached_bytes, query_budget)
    }

    pub fn new_with_query_budget(
        config: BrowserRuntimeConfig,
        max_cached_bytes: u64,
        query_budget: BrowserQueryBudget,
    ) -> Result<Self, QueryError> {
        Ok(Self {
            runtime: BrowserRuntimeSession::new(config)?,
            query_budget,
            #[cfg(feature = "datafusion")]
            datafusion: WasmDataFusionEngine::with_budget(query_budget.into()),
            tables: BTreeMap::new(),
            max_cached_bytes,
            next_access_millis: 0,
        })
    }

    pub fn runtime(&self) -> &BrowserRuntimeSession {
        &self.runtime
    }

    pub fn datafusion_query_budget(&self) -> BrowserQueryBudget {
        self.query_budget
    }

    pub fn max_cached_bytes(&self) -> u64 {
        self.max_cached_bytes
    }

    pub fn cached_bytes(&self) -> u64 {
        self.tables.values().fold(0_u64, |total, table| {
            total.saturating_add(table.cached_bytes())
        })
    }

    pub fn table(&self, name: &str) -> Option<&CachedTable> {
        self.tables.get(name)
    }

    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    pub fn contains_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    pub fn cache_table(
        &mut self,
        name: impl Into<String>,
        materialized: MaterializedBrowserSnapshot,
        bootstrapped: Option<BootstrappedBrowserSnapshot>,
    ) -> Result<(), QueryError> {
        self.insert_table(name.into(), None, materialized, bootstrapped, None, false)
    }

    pub fn open_table(
        &mut self,
        name: impl Into<String>,
        descriptor: BrowserHttpSnapshotDescriptor,
    ) -> Result<(), QueryError> {
        let materialized = self.runtime.materialize_snapshot(&descriptor)?;
        self.insert_table(
            name.into(),
            Some(descriptor),
            materialized,
            None,
            None,
            false,
        )
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
        #[cfg(feature = "datafusion")]
        {
            let schema = datafusion_delta_schema(&descriptor, bootstrapped.as_ref())?;
            let delta_descriptor = DeltaTableDescriptor::from_browser_http_snapshot(
                name.clone(),
                &descriptor,
                schema,
            )?;

            self.datafusion.open_delta_table(delta_descriptor).await?;
            self.insert_table(
                name,
                Some(descriptor),
                materialized,
                bootstrapped,
                None,
                true,
            )
        }

        #[cfg(not(feature = "datafusion"))]
        {
            self.insert_table(
                name,
                Some(descriptor),
                materialized,
                bootstrapped,
                None,
                false,
            )
        }
    }

    pub fn plan_query(
        &mut self,
        name: &str,
        request: &QueryRequest,
    ) -> Result<BrowserPlannedQuery, QueryError> {
        let snapshot = self.touch_bootstrapped_snapshot(name, request)?.clone();
        self.runtime.plan_query(&snapshot, request)
    }

    pub fn build_execution_plan(
        &mut self,
        name: &str,
        request: &QueryRequest,
    ) -> Result<BrowserExecutionPlan, QueryError> {
        let snapshot = self.touch_bootstrapped_snapshot(name, request)?.clone();
        self.runtime.build_execution_plan(&snapshot, request)
    }

    pub async fn sql(
        &mut self,
        name: &str,
        request: &QueryRequest,
    ) -> Result<BrowserSessionQueryResult, QueryError> {
        #[cfg(feature = "datafusion")]
        {
            if self.touch_table(name)?.datafusion_registered {
                return self.sql_datafusion(name, request).await;
            }
        }

        #[cfg(not(feature = "datafusion"))]
        {
            self.touch_table(name)?;
        }

        let materialized = self
            .table(name)
            .ok_or_else(|| missing_table_error(name))?
            .materialized
            .clone();
        let plan = if self.has_cached_bootstrap(name, request)? {
            self.build_execution_plan(name, request)?
        } else {
            let prepared = self
                .runtime
                .prepare_execution(&materialized, request)
                .await?;
            let execution_plan = prepared.execution_plan().clone();
            self.store_bootstrapped_snapshot(
                name,
                prepared.bootstrapped_snapshot().clone(),
                CachedBootstrapRequest::from_request(request),
            )?;
            execution_plan
        };
        let started_at = Instant::now();
        let runtime_result = self
            .runtime
            .execute_plan_to_arrow_ipc(&materialized, &plan)
            .await?;

        Ok(BrowserSessionQueryResult {
            response: QueryResponse {
                executed_on: ExecutionTarget::BrowserWasm,
                capabilities: materialized.required_capabilities().clone(),
                fallback_reason: None,
                metrics: execution_metrics(&plan, &runtime_result, started_at)?,
            },
            runtime_result,
        })
    }

    #[cfg(feature = "datafusion")]
    async fn sql_datafusion(
        &mut self,
        name: &str,
        request: &QueryRequest,
    ) -> Result<BrowserSessionQueryResult, QueryError> {
        let (capabilities, file_count, footer_reads, snapshot_bootstrap_duration_ms, access_mode) = {
            let table = self.table(name).ok_or_else(|| missing_table_error(name))?;
            validate_datafusion_request_match(table, request)?;
            validate_datafusion_sql_scope(name, &request.sql)?;
            validate_datafusion_execution_budget(table, self.runtime.config().execution_budget)?;
            (
                table.materialized.required_capabilities().clone(),
                u64::try_from(table.materialized.active_files().len()).map_err(|_| {
                    QueryError::new(
                        QueryErrorCode::ExecutionFailed,
                        "browser session DataFusion file counts overflowed u64",
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
        let started_at = Instant::now();
        let datafusion_result = self
            .datafusion
            .sql_to_arrow_ipc_result(&request.sql)
            .await?;
        let scan_metrics = datafusion_result.scan_metrics.clone();
        let runtime_result = runtime_result_from_datafusion(datafusion_result);

        Ok(BrowserSessionQueryResult {
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
            },
            runtime_result,
        })
    }

    pub fn remove_table(&mut self, name: &str) -> Option<CachedTable> {
        let removed = self.tables.remove(name);
        if let Some(table) = &removed {
            let _ = self.deregister_removed_table(name, table);
        }
        removed
    }

    pub fn dispose_table(&mut self, name: &str) -> bool {
        self.remove_table(name).is_some()
    }

    fn has_cached_bootstrap(
        &mut self,
        name: &str,
        request: &QueryRequest,
    ) -> Result<bool, QueryError> {
        let key = CachedBootstrapRequest::from_request(request);
        let table = self.touch_table(name)?;
        Ok(table.bootstrapped_request.as_ref() == Some(&key))
    }

    fn store_bootstrapped_snapshot(
        &mut self,
        name: &str,
        snapshot: BootstrappedBrowserSnapshot,
        request: CachedBootstrapRequest,
    ) -> Result<(), QueryError> {
        let pinned_name = name.to_string();
        let table = self.touch_table(name)?;
        table.bootstrapped = Some(snapshot);
        table.bootstrapped_request = Some(request);
        table.cached_bytes = cached_table_bytes(&table.materialized, table.bootstrapped.as_ref())?;
        self.evict_to_budget(&pinned_name)?;
        Ok(())
    }

    fn insert_table(
        &mut self,
        name: String,
        descriptor: Option<BrowserHttpSnapshotDescriptor>,
        materialized: MaterializedBrowserSnapshot,
        bootstrapped: Option<BootstrappedBrowserSnapshot>,
        bootstrapped_request: Option<CachedBootstrapRequest>,
        datafusion_registered: bool,
    ) -> Result<(), QueryError> {
        let cached_bytes = cached_table_bytes(&materialized, bootstrapped.as_ref())?;
        let last_used_millis = self.bump_access_clock();

        let replaced = self.tables.get(&name).cloned();
        if let Some(replaced) = &replaced {
            self.deregister_removed_table(&name, replaced)?;
        }

        self.tables.insert(
            name.clone(),
            CachedTable {
                descriptor,
                materialized,
                bootstrapped,
                bootstrapped_request,
                datafusion_registered,
                cached_bytes,
                last_used_millis,
            },
        );
        self.evict_to_budget(&name)?;

        Ok(())
    }

    fn touch_bootstrapped_snapshot(
        &mut self,
        name: &str,
        request: &QueryRequest,
    ) -> Result<&BootstrappedBrowserSnapshot, QueryError> {
        let table = self.touch_table(name)?;
        let expected_request = CachedBootstrapRequest::from_request(request);
        if table.bootstrapped_request.as_ref() != Some(&expected_request) {
            return Err(missing_bootstrap_error(name));
        }
        table
            .bootstrapped
            .as_ref()
            .ok_or_else(|| missing_bootstrap_error(name))
    }

    fn touch_table(&mut self, name: &str) -> Result<&mut CachedTable, QueryError> {
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

            if let Some(evicted) = self.tables.remove(&eviction_candidate) {
                self.deregister_removed_table(&eviction_candidate, &evicted)?;
            }
        }

        Ok(())
    }

    fn deregister_removed_table(
        &mut self,
        name: &str,
        table: &CachedTable,
    ) -> Result<(), QueryError> {
        #[cfg(feature = "datafusion")]
        if table.datafusion_registered {
            self.datafusion.deregister_table(name)?;
        }
        #[cfg(not(feature = "datafusion"))]
        {
            let _ = (name, table);
        }
        Ok(())
    }
}

impl CachedTable {
    pub fn descriptor(&self) -> Option<&BrowserHttpSnapshotDescriptor> {
        self.descriptor.as_ref()
    }

    pub fn materialized_snapshot(&self) -> &MaterializedBrowserSnapshot {
        &self.materialized
    }

    pub fn bootstrapped_snapshot(&self) -> Option<&BootstrappedBrowserSnapshot> {
        self.bootstrapped.as_ref()
    }

    pub fn datafusion_registered(&self) -> bool {
        self.datafusion_registered
    }

    pub fn cached_bytes(&self) -> u64 {
        self.cached_bytes
    }

    pub fn last_used_millis(&self) -> u64 {
        self.last_used_millis
    }
}

impl CachedBootstrapRequest {
    fn from_request(request: &QueryRequest) -> Self {
        Self {
            table_uri: request.table_uri.clone(),
            snapshot_version: request.snapshot_version,
            sql: request.sql.clone(),
        }
    }
}

#[cfg(feature = "datafusion")]
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

#[cfg(feature = "datafusion")]
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

#[cfg(feature = "datafusion")]
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
            format!("browser DataFusion does not support partition column '{column}' type"),
            runtime_target(),
        )),
    }
}

#[cfg(feature = "datafusion")]
fn datafusion_schema_field_from_parquet_field(
    field: &BrowserParquetField,
) -> Result<DeltaTableSchemaField, QueryError> {
    Ok(DeltaTableSchemaField::new(
        field.name.clone(),
        datafusion_data_type_from_parquet_field(field)?,
        field.nullable,
    ))
}

#[cfg(feature = "datafusion")]
fn datafusion_data_type_from_parquet_field(
    field: &BrowserParquetField,
) -> Result<DeltaTableFieldDataType, QueryError> {
    if matches!(
        field.logical_type.as_ref(),
        Some(BrowserParquetLogicalType::String)
    ) || matches!(
        field.converted_type.as_ref(),
        Some(BrowserParquetConvertedType::Utf8)
    ) {
        return Ok(DeltaTableFieldDataType::Utf8);
    }

    match &field.physical_type {
        BrowserParquetPhysicalType::Boolean => Ok(DeltaTableFieldDataType::Boolean),
        BrowserParquetPhysicalType::Int32 => Ok(DeltaTableFieldDataType::Int32),
        BrowserParquetPhysicalType::Int64 => Ok(DeltaTableFieldDataType::Int64),
        BrowserParquetPhysicalType::Float => Ok(DeltaTableFieldDataType::Float32),
        BrowserParquetPhysicalType::Double => Ok(DeltaTableFieldDataType::Float64),
        BrowserParquetPhysicalType::Int96
        | BrowserParquetPhysicalType::ByteArray
        | BrowserParquetPhysicalType::FixedLenByteArray => {
            Err(unsupported_datafusion_parquet_field(field))
        }
    }
}

#[cfg(feature = "datafusion")]
fn unsupported_datafusion_parquet_field(field: &BrowserParquetField) -> QueryError {
    QueryError::new(
        QueryErrorCode::UnsupportedFeature,
        format!(
            "browser DataFusion does not yet support Parquet field '{}' with physical type {}",
            field.name, field.physical_type
        ),
        runtime_target(),
    )
}

#[cfg(feature = "datafusion")]
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

#[cfg(feature = "datafusion")]
fn datafusion_query_metrics(
    scan_metrics: DataFusionScanMetricsSummary,
    fallback_file_count: u64,
    footer_reads: Option<u64>,
    snapshot_bootstrap_duration_ms: Option<u64>,
    access_mode: Option<query_contract::BrowserAccessMode>,
    started_at: Instant,
) -> QueryMetricsSummary {
    QueryMetricsSummary {
        bytes_fetched: scan_metrics.bytes_fetched,
        duration_ms: u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
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

#[cfg(feature = "datafusion")]
fn validate_datafusion_request_match(
    table: &CachedTable,
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

#[cfg(feature = "datafusion")]
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

#[cfg(feature = "datafusion")]
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
        SqlSetExpr::Select(select) => validate_datafusion_select_scope(table_name, select.as_ref()),
        SqlSetExpr::Query(query) => validate_datafusion_query_scope(table_name, query.as_ref()),
        _ => Err(invalid_datafusion_sql(
            "only read-only SELECT statements are supported",
        )),
    }
}

#[cfg(feature = "datafusion")]
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

#[cfg(feature = "datafusion")]
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

#[cfg(feature = "datafusion")]
fn object_name_to_relation_name(name: &ObjectName) -> Option<String> {
    let [part] = name.0.as_slice() else {
        return None;
    };

    Some(part.as_ident()?.value.to_ascii_lowercase())
}

#[cfg(feature = "datafusion")]
fn wrong_datafusion_table_error(table_name: &str) -> QueryError {
    invalid_datafusion_sql(format!("query must read only from table '{table_name}'"))
}

#[cfg(feature = "datafusion")]
fn invalid_datafusion_sql(message: impl Into<String>) -> QueryError {
    QueryError::new(QueryErrorCode::InvalidRequest, message, runtime_target())
}

fn datafusion_query_budget_from_runtime_config(
    config: &BrowserRuntimeConfig,
) -> BrowserQueryBudget {
    config
        .execution_budget
        .map(|execution_budget| BrowserQueryBudget {
            max_scan_bytes: Some(execution_budget.max_bytes),
            max_output_ipc_bytes: Some(execution_budget.max_bytes),
            max_batches_in_flight: None,
            max_rows_returned: Some(execution_budget.max_rows),
        })
        .unwrap_or_default()
}

#[cfg(feature = "datafusion")]
fn validate_datafusion_execution_budget(
    table: &CachedTable,
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

#[cfg(feature = "datafusion")]
fn datafusion_table_estimated_rows(table: &CachedTable) -> Result<u64, QueryError> {
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

#[cfg(feature = "datafusion")]
fn datafusion_table_estimated_bytes(table: &CachedTable) -> Result<u64, QueryError> {
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

#[cfg(feature = "datafusion")]
fn datafusion_budget_total_overflow_error(total_kind: &str) -> QueryError {
    QueryError::new(
        QueryErrorCode::ExecutionFailed,
        format!("browser DataFusion {total_kind} totals overflowed u64"),
        runtime_target(),
    )
}

#[cfg(feature = "datafusion")]
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
        .ok_or_else(cached_bytes_overflow_error)
}

fn materialized_snapshot_bytes(snapshot: &MaterializedBrowserSnapshot) -> Result<u64, QueryError> {
    snapshot
        .active_files()
        .iter()
        .try_fold(0_u64, |total, file| {
            total
                .checked_add(file.size_bytes())
                .ok_or_else(cached_bytes_overflow_error)
        })
}

fn bootstrapped_snapshot_bytes(snapshot: &BootstrappedBrowserSnapshot) -> Result<u64, QueryError> {
    let mut total = size_of::<BootstrappedBrowserSnapshot>() as u64;
    total = total
        .checked_add(string_bytes(snapshot.table_uri())?)
        .ok_or_else(cached_bytes_overflow_error)?;
    total = total
        .checked_add(capability_report_bytes(snapshot.required_capabilities())?)
        .ok_or_else(cached_bytes_overflow_error)?;

    for column in snapshot.partition_column_types().keys() {
        total = total
            .checked_add(string_bytes(column)?)
            .and_then(|subtotal| {
                subtotal.checked_add(size_of::<query_contract::PartitionColumnType>() as u64)
            })
            .ok_or_else(cached_bytes_overflow_error)?;
    }

    snapshot
        .active_files()
        .iter()
        .try_fold(total, |subtotal, file| {
            subtotal
                .checked_add(bootstrapped_file_bytes(file)?)
                .ok_or_else(cached_bytes_overflow_error)
        })
}

fn bootstrapped_file_bytes(file: &BootstrappedBrowserFile) -> Result<u64, QueryError> {
    let metadata = file.metadata();
    let mut total = size_of::<BootstrappedBrowserFile>() as u64;
    total = total
        .checked_add(string_bytes(file.path())?)
        .ok_or_else(cached_bytes_overflow_error)?;
    total = total
        .checked_add(option_string_bytes(file.object_etag())?)
        .ok_or_else(cached_bytes_overflow_error)?;
    total = total
        .checked_add(partition_values_bytes(file.partition_values())?)
        .ok_or_else(cached_bytes_overflow_error)?;
    total = total
        .checked_add(size_of::<wasm_query_runtime::BrowserParquetFileMetadata>() as u64)
        .ok_or_else(cached_bytes_overflow_error)?;

    for field in &metadata.fields {
        total = total
            .checked_add(size_of::<wasm_query_runtime::BrowserParquetField>() as u64)
            .and_then(|subtotal| subtotal.checked_add(string_bytes(&field.name).ok()?))
            .ok_or_else(cached_bytes_overflow_error)?;
    }

    for name in metadata.field_stats.keys() {
        total = total
            .checked_add(size_of::<wasm_query_runtime::BrowserParquetFieldStats>() as u64)
            .and_then(|subtotal| subtotal.checked_add(string_bytes(name).ok()?))
            .ok_or_else(cached_bytes_overflow_error)?;
    }

    Ok(total)
}

fn partition_values_bytes(
    partition_values: &BTreeMap<String, Option<String>>,
) -> Result<u64, QueryError> {
    partition_values
        .iter()
        .try_fold(0_u64, |total, (key, value)| {
            total
                .checked_add(string_bytes(key)?)
                .and_then(|subtotal| {
                    subtotal.checked_add(option_string_bytes(value.as_deref()).ok()?)
                })
                .ok_or_else(cached_bytes_overflow_error)
        })
}

fn capability_report_bytes(report: &query_contract::CapabilityReport) -> Result<u64, QueryError> {
    let mut total = size_of::<query_contract::CapabilityReport>() as u64;
    for _ in &report.capabilities {
        total = total
            .checked_add(
                (size_of::<query_contract::CapabilityKey>()
                    + size_of::<query_contract::CapabilityState>()) as u64,
            )
            .ok_or_else(cached_bytes_overflow_error)?;
    }
    Ok(total)
}

fn string_bytes(value: &str) -> Result<u64, QueryError> {
    u64::try_from(value.len()).map_err(|_| cached_bytes_overflow_error())
}

fn option_string_bytes(value: Option<&str>) -> Result<u64, QueryError> {
    value
        .map(string_bytes)
        .transpose()
        .map(|bytes| bytes.unwrap_or(0))
}

fn cached_bytes_overflow_error() -> QueryError {
    QueryError::new(
        QueryErrorCode::ExecutionFailed,
        "browser session cached-byte totals overflowed u64",
        runtime_target(),
    )
}

fn execution_metrics(
    plan: &BrowserExecutionPlan,
    runtime_result: &RuntimeArrowIpcResult,
    started_at: Instant,
) -> Result<QueryMetricsSummary, QueryError> {
    let bytes_fetched = runtime_result
        .scan_metrics
        .iter()
        .try_fold(0_u64, |total, metrics| {
            total.checked_add(metrics.bytes_fetched).ok_or_else(|| {
                QueryError::new(
                    QueryErrorCode::ExecutionFailed,
                    "browser session byte totals overflowed u64",
                    runtime_target(),
                )
            })
        })?;
    let row_groups_touched =
        runtime_result
            .scan_metrics
            .iter()
            .try_fold(0_u64, |total, metrics| {
                total
                    .checked_add(metrics.row_groups_touched)
                    .ok_or_else(|| {
                        QueryError::new(
                            QueryErrorCode::ExecutionFailed,
                            "browser session row-group touched totals overflowed u64",
                            runtime_target(),
                        )
                    })
            })?;
    let row_groups_skipped =
        runtime_result
            .scan_metrics
            .iter()
            .try_fold(0_u64, |total, metrics| {
                total
                    .checked_add(metrics.row_groups_skipped)
                    .ok_or_else(|| {
                        QueryError::new(
                            QueryErrorCode::ExecutionFailed,
                            "browser session row-group skipped totals overflowed u64",
                            runtime_target(),
                        )
                    })
            })?;
    let rows_emitted = runtime_result
        .scan_metrics
        .iter()
        .try_fold(0_u64, |total, metrics| {
            total.checked_add(metrics.rows_emitted).ok_or_else(|| {
                QueryError::new(
                    QueryErrorCode::ExecutionFailed,
                    "browser session emitted-row totals overflowed u64",
                    runtime_target(),
                )
            })
        })?;

    Ok(QueryMetricsSummary {
        bytes_fetched,
        duration_ms: u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
        files_touched: plan.scan().candidate_file_count(),
        files_skipped: plan.pruning().files_pruned,
        row_groups_touched,
        row_groups_skipped,
        footer_reads: plan.footer_reads(),
        rows_emitted,
        snapshot_bootstrap_duration_ms: plan.snapshot_bootstrap_duration_ms(),
        access_mode: plan.access_mode(),
    })
}

fn missing_table_error(name: &str) -> QueryError {
    QueryError::new(
        QueryErrorCode::InvalidRequest,
        format!("browser session does not have an open table named '{name}'"),
        runtime_target(),
    )
}

fn missing_bootstrap_error(name: &str) -> QueryError {
    QueryError::new(
        QueryErrorCode::ExecutionFailed,
        format!("browser session table '{name}' did not retain bootstrapped snapshot state"),
        runtime_target(),
    )
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    use query_contract::{BrowserHttpFileDescriptor, CapabilityReport};
    #[cfg(feature = "datafusion")]
    use wasm_query_runtime::{
        BrowserExecutionBudget, BrowserObjectSource, MaterializedBrowserFile,
    };

    #[cfg(feature = "datafusion")]
    #[test]
    fn open_delta_table_registers_descriptor_and_sql_returns_arrow_ipc() {
        let mut session = BrowserQuerySession::new(BrowserRuntimeConfig::default(), u64::MAX)
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
                .expect("SQL should execute through the opened DataFusion table");

            assert!(session.contains_table("events"));
            assert!(!result.runtime_result.ipc_bytes.is_empty());
            assert_eq!(result.runtime_result.row_count, 1);
            assert_eq!(result.response.executed_on, ExecutionTarget::BrowserWasm);
            assert_eq!(result.response.metrics.rows_emitted, 0);
        });
    }

    #[cfg(not(feature = "datafusion"))]
    #[test]
    fn default_open_delta_table_uses_narrow_session_path() {
        let mut session = BrowserQuerySession::new(BrowserRuntimeConfig::default(), u64::MAX)
            .expect("session should construct");

        test_runtime().block_on(async {
            session
                .open_delta_table("events", empty_delta_descriptor())
                .await
                .expect("default open_delta_table should cache the table");
        });

        assert!(!session
            .table("events")
            .expect("table should be cached")
            .datafusion_registered());
    }

    #[test]
    fn sql_requires_table_to_be_opened_before_query_execution() {
        let mut session = BrowserQuerySession::new(BrowserRuntimeConfig::default(), u64::MAX)
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

    #[cfg(feature = "datafusion")]
    #[test]
    fn datafusion_sql_rejects_request_for_a_different_table_uri() {
        let mut session = BrowserQuerySession::new(BrowserRuntimeConfig::default(), u64::MAX)
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

    #[cfg(feature = "datafusion")]
    #[test]
    fn disposed_datafusion_table_can_be_reopened_with_the_same_name() {
        let mut session = BrowserQuerySession::new(BrowserRuntimeConfig::default(), u64::MAX)
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

    #[cfg(feature = "datafusion")]
    #[test]
    fn evicted_datafusion_table_can_be_reopened_with_the_same_name() {
        let mut session = BrowserQuerySession::new(BrowserRuntimeConfig::default(), 1)
            .expect("default browser runtime config should be supported");

        test_runtime().block_on(async {
            session
                .open_delta_table("events", empty_delta_descriptor())
                .await
                .expect("first open should register the DataFusion table");
            session
                .cache_table("large_legacy", large_materialized_snapshot(), None)
                .expect("large legacy table should force eviction");
            assert!(!session.contains_table("events"));

            session
                .open_delta_table("events", empty_delta_descriptor())
                .await
                .expect("eviction should deregister the DataFusion table name");
        });
    }

    #[cfg(feature = "datafusion")]
    #[test]
    fn replaced_datafusion_table_can_be_reopened_with_the_same_name() {
        let mut session = BrowserQuerySession::new(BrowserRuntimeConfig::default(), u64::MAX)
            .expect("default browser runtime config should be supported");

        test_runtime().block_on(async {
            session
                .open_delta_table("events", empty_delta_descriptor())
                .await
                .expect("first open should register the DataFusion table");
            session
                .cache_table("events", large_materialized_snapshot(), None)
                .expect("replacement should deregister the prior DataFusion table");
            assert!(!session
                .table("events")
                .expect("replacement table should be cached")
                .datafusion_registered());

            session
                .open_delta_table("events", empty_delta_descriptor())
                .await
                .expect("replacement should free the DataFusion table name");
        });
    }

    #[cfg(feature = "datafusion")]
    #[test]
    fn datafusion_sql_rejects_non_select_statements_before_execution() {
        let mut session = BrowserQuerySession::new(BrowserRuntimeConfig::default(), u64::MAX)
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
            .expect_err(
                "DataFusion SQL should reject non-SELECT statements at the session boundary",
            );

        assert_eq!(error.code, QueryErrorCode::InvalidRequest);
        assert!(error.message.contains("read-only SELECT"));
    }

    #[cfg(feature = "datafusion")]
    #[test]
    fn datafusion_sql_rejects_select_into_before_execution() {
        let mut session = BrowserQuerySession::new(BrowserRuntimeConfig::default(), u64::MAX)
            .expect("default browser runtime config should be supported");
        let request = QueryRequest::new(
            "gs://axon-fixtures/empty",
            "SELECT * INTO copied FROM events",
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
            .expect_err("DataFusion SQL should reject SELECT INTO at the session boundary");

        assert_eq!(error.code, QueryErrorCode::InvalidRequest);
        assert!(error.message.contains("read-only SELECT"));
    }

    #[cfg(feature = "datafusion")]
    #[test]
    fn datafusion_sql_rejects_queries_against_other_open_tables() {
        let mut session = BrowserQuerySession::new(BrowserRuntimeConfig::default(), u64::MAX)
            .expect("default browser runtime config should be supported");
        let request = QueryRequest::new(
            "gs://axon-fixtures/empty",
            "SELECT COUNT(*) AS rows FROM metrics",
            ExecutionTarget::BrowserWasm,
        );

        let error = test_runtime()
            .block_on(async {
                session
                    .open_delta_table("events", empty_delta_descriptor())
                    .await
                    .expect("events should open");
                session
                    .open_delta_table("metrics", metrics_delta_descriptor())
                    .await
                    .expect("metrics should open");
                session.sql("events", &request).await
            })
            .expect_err("DataFusion SQL should stay scoped to the command table");

        assert_eq!(error.code, QueryErrorCode::InvalidRequest);
        assert!(error
            .message
            .contains("query must read only from table 'events'"));
    }

    #[cfg(feature = "datafusion")]
    #[test]
    fn datafusion_sql_rejects_tables_over_configured_byte_budget() {
        let table = CachedTable {
            descriptor: None,
            materialized: large_materialized_snapshot(),
            bootstrapped: None,
            bootstrapped_request: None,
            datafusion_registered: true,
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

    fn empty_delta_descriptor() -> BrowserHttpSnapshotDescriptor {
        BrowserHttpSnapshotDescriptor {
            table_uri: "gs://axon-fixtures/empty".to_string(),
            snapshot_version: 1,
            partition_column_types: BTreeMap::new(),
            browser_compatibility: CapabilityReport::default(),
            required_capabilities: CapabilityReport::default(),
            active_files: Vec::<BrowserHttpFileDescriptor>::new(),
        }
    }

    #[cfg(feature = "datafusion")]
    fn metrics_delta_descriptor() -> BrowserHttpSnapshotDescriptor {
        BrowserHttpSnapshotDescriptor {
            table_uri: "gs://axon-fixtures/metrics".to_string(),
            snapshot_version: 1,
            partition_column_types: BTreeMap::new(),
            browser_compatibility: CapabilityReport::default(),
            required_capabilities: CapabilityReport::default(),
            active_files: Vec::<BrowserHttpFileDescriptor>::new(),
        }
    }

    #[cfg(feature = "datafusion")]
    fn large_materialized_snapshot() -> MaterializedBrowserSnapshot {
        MaterializedBrowserSnapshot::new(
            "gs://axon-fixtures/large",
            1,
            vec![MaterializedBrowserFile::new(
                "part-000.parquet",
                128,
                BTreeMap::new(),
                BrowserObjectSource::from_url("http://127.0.0.1:9/part-000.parquet")
                    .expect("loopback object source should be allowed in host tests"),
            )],
        )
        .expect("large materialized snapshot should construct")
    }

    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Runtime::new().expect("test runtime should construct")
    }
}
