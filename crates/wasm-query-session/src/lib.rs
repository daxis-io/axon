//! In-memory browser session shell over runtime-owned snapshot state.

use std::collections::BTreeMap;
use std::mem::size_of;
use std::time::Instant;

use query_contract::{
    BrowserHttpSnapshotDescriptor, ExecutionTarget, QueryError, QueryErrorCode,
    QueryMetricsSummary, QueryRequest, QueryResponse,
};
use wasm_query_runtime::{
    runtime_target, BootstrappedBrowserFile, BootstrappedBrowserSnapshot, BrowserExecutionPlan,
    BrowserPlannedQuery, BrowserRuntimeConfig, BrowserRuntimeSession, MaterializedBrowserSnapshot,
    RuntimeArrowIpcResult,
};

pub const OWNER: &str = "Runtime / engine team";
pub const RESPONSIBILITY: &str =
    "In-memory browser session caching for runtime-owned descriptors and snapshots.";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserSessionQueryResult {
    pub response: QueryResponse,
    pub runtime_result: RuntimeArrowIpcResult,
}

#[derive(Clone, Debug)]
pub struct BrowserQuerySession {
    runtime: BrowserRuntimeSession,
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
        Ok(Self {
            runtime: BrowserRuntimeSession::new(config)?,
            tables: BTreeMap::new(),
            max_cached_bytes,
            next_access_millis: 0,
        })
    }

    pub fn runtime(&self) -> &BrowserRuntimeSession {
        &self.runtime
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
        self.insert_table(name.into(), None, materialized, bootstrapped, None)
    }

    pub fn open_table(
        &mut self,
        name: impl Into<String>,
        descriptor: BrowserHttpSnapshotDescriptor,
    ) -> Result<(), QueryError> {
        let materialized = self.runtime.materialize_snapshot(&descriptor)?;
        self.insert_table(name.into(), Some(descriptor), materialized, None, None)
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

    pub fn remove_table(&mut self, name: &str) -> Option<CachedTable> {
        self.tables.remove(name)
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
        self.evict_to_budget(&pinned_name);
        Ok(())
    }

    fn insert_table(
        &mut self,
        name: String,
        descriptor: Option<BrowserHttpSnapshotDescriptor>,
        materialized: MaterializedBrowserSnapshot,
        bootstrapped: Option<BootstrappedBrowserSnapshot>,
        bootstrapped_request: Option<CachedBootstrapRequest>,
    ) -> Result<(), QueryError> {
        let cached_bytes = cached_table_bytes(&materialized, bootstrapped.as_ref())?;
        let last_used_millis = self.bump_access_clock();

        self.tables.insert(
            name.clone(),
            CachedTable {
                descriptor,
                materialized,
                bootstrapped,
                bootstrapped_request,
                cached_bytes,
                last_used_millis,
            },
        );
        self.evict_to_budget(&name);

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

    fn evict_to_budget(&mut self, pinned_name: &str) {
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

            self.tables.remove(&eviction_candidate);
        }
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
                .ok_or_else(|| cached_bytes_overflow_error())
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

    for (column, _) in snapshot.partition_column_types() {
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

    for (name, _) in &metadata.field_stats {
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

    Ok(QueryMetricsSummary {
        bytes_fetched,
        duration_ms: u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
        files_touched: plan.scan().candidate_file_count(),
        files_skipped: plan.pruning().files_pruned,
        footer_reads: plan.footer_reads(),
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
