//! Legacy in-memory browser session shell over runtime-owned snapshot state.
//!
//! This crate intentionally owns only the narrow custom runtime path. Production browser
//! DataFusion sessions live in `wasm-datafusion-session` so the custom engine can be removed
//! without unwinding the UI DataFusion runtime.

use std::collections::BTreeMap;
use std::mem::size_of;

use query_contract::{
    BrowserHttpParquetDatasetDescriptor, BrowserHttpSnapshotDescriptor, ExecutionTarget,
    FallbackReason, ParquetInspectionSummary, QueryError, QueryErrorCode, QueryMetricsSummary,
    QueryRequest, QueryResponse, QueryRuntimeLimits,
};
use wasm_query_runtime::{
    execution_range_read_metrics, runtime_target, BootstrappedBrowserFile,
    BootstrappedBrowserSnapshot, BrowserExecutionPlan, BrowserPlannedQuery, BrowserRuntimeConfig,
    BrowserRuntimeInstant, BrowserRuntimeSession, MaterializedBrowserSnapshot,
    RuntimeArrowIpcResult,
};

pub const OWNER: &str = "Runtime / engine team";
pub const RESPONSIBILITY: &str =
    "Legacy in-memory browser session caching for narrow runtime-owned descriptors and snapshots.";
const PARQUET_DATASET_RUNTIME_VERSION: i64 = 0;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BrowserQueryBudget {
    pub max_scan_bytes: Option<u64>,
    pub max_scan_overfetch_bytes: Option<u64>,
    pub max_output_ipc_bytes: Option<u64>,
    pub max_batches_in_flight: Option<usize>,
    pub max_rows_returned: Option<u64>,
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
        let query_budget = query_budget_from_runtime_config(&config);
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
            tables: BTreeMap::new(),
            max_cached_bytes,
            next_access_millis: 0,
        })
    }

    pub fn runtime(&self) -> &BrowserRuntimeSession {
        &self.runtime
    }

    pub fn query_budget(&self) -> BrowserQueryBudget {
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
        self.insert_table(name, Some(descriptor), materialized, bootstrapped, None)
    }

    pub async fn open_parquet_dataset(
        &mut self,
        name: impl Into<String>,
        dataset: BrowserHttpParquetDatasetDescriptor,
    ) -> Result<(), QueryError> {
        let name = name.into();
        let materialized = self.runtime.materialize_parquet_dataset(&dataset)?;
        let descriptor = snapshot_descriptor_from_parquet_dataset(dataset);
        let bootstrapped = if materialized.active_files().is_empty() {
            None
        } else {
            Some(
                self.runtime
                    .bootstrap_snapshot_metadata(&materialized)
                    .await?,
            )
        };
        self.insert_table(name, Some(descriptor), materialized, bootstrapped, None)
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
        self.touch_table(name)?;

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
        let query_budget =
            query_budget_for_request(self.query_budget, request.options.runtime_limits);
        validate_plan_budget(&plan, query_budget)?;
        let started_at = BrowserRuntimeInstant::now();
        let runtime_result = self
            .runtime
            .execute_plan_to_arrow_ipc(&materialized, &plan)
            .await?;
        validate_arrow_result_budget(&runtime_result, query_budget)?;
        validate_scan_overfetch_budget(&plan, &runtime_result, query_budget)?;
        let explain = if request.options.include_explain {
            Some(format!("{plan:#?}"))
        } else {
            None
        };

        Ok(BrowserSessionQueryResult {
            response: QueryResponse {
                executed_on: ExecutionTarget::BrowserWasm,
                capabilities: materialized.required_capabilities().clone(),
                fallback_reason: None,
                metrics: execution_metrics(&plan, &runtime_result, started_at)?,
                explain,
            },
            runtime_result,
        })
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

            self.tables.remove(&eviction_candidate);
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

    pub fn cached_bytes(&self) -> u64 {
        self.cached_bytes
    }

    pub fn last_used_millis(&self) -> u64 {
        self.last_used_millis
    }
}

fn snapshot_descriptor_from_parquet_dataset(
    dataset: BrowserHttpParquetDatasetDescriptor,
) -> BrowserHttpSnapshotDescriptor {
    BrowserHttpSnapshotDescriptor {
        table_uri: dataset.table_uri,
        snapshot_version: PARQUET_DATASET_RUNTIME_VERSION,
        partition_column_types: dataset.partition_column_types,
        browser_compatibility: dataset.browser_compatibility,
        required_capabilities: dataset.required_capabilities,
        active_files: dataset.files,
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

fn query_budget_from_runtime_config(config: &BrowserRuntimeConfig) -> BrowserQueryBudget {
    config
        .execution_budget
        .map(|execution_budget| BrowserQueryBudget {
            max_scan_bytes: Some(execution_budget.max_bytes),
            max_scan_overfetch_bytes: None,
            max_output_ipc_bytes: Some(execution_budget.max_bytes),
            max_batches_in_flight: None,
            max_rows_returned: Some(execution_budget.max_rows),
        })
        .unwrap_or_default()
}

fn query_budget_for_request(
    session_budget: BrowserQueryBudget,
    request_limits: Option<QueryRuntimeLimits>,
) -> BrowserQueryBudget {
    let request_limits = request_limits.unwrap_or_default();
    BrowserQueryBudget {
        max_scan_bytes: min_optional_budget(
            session_budget.max_scan_bytes,
            request_limits.max_scan_bytes,
        ),
        max_scan_overfetch_bytes: min_optional_budget(
            session_budget.max_scan_overfetch_bytes,
            request_limits.max_scan_overfetch_bytes,
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
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(limit), None) | (None, Some(limit)) => Some(limit),
        (None, None) => None,
    }
}

fn validate_plan_budget(
    plan: &BrowserExecutionPlan,
    query_budget: BrowserQueryBudget,
) -> Result<(), QueryError> {
    if let Some(max_scan_bytes) = query_budget.max_scan_bytes {
        let candidate_bytes = plan.scan().candidate_bytes();
        if candidate_bytes > max_scan_bytes {
            return Err(browser_budget_exceeded_error(
                "max_scan_bytes",
                candidate_bytes,
                max_scan_bytes,
            ));
        }
    }

    Ok(())
}

fn validate_arrow_result_budget(
    runtime_result: &RuntimeArrowIpcResult,
    query_budget: BrowserQueryBudget,
) -> Result<(), QueryError> {
    if let Some(max_rows_returned) = query_budget.max_rows_returned {
        if runtime_result.row_count > max_rows_returned {
            return Err(browser_budget_exceeded_error(
                "max_rows_returned",
                runtime_result.row_count,
                max_rows_returned,
            ));
        }
    }

    if let Some(max_output_ipc_bytes) = query_budget.max_output_ipc_bytes {
        if runtime_result.encoded_bytes > max_output_ipc_bytes {
            return Err(browser_budget_exceeded_error(
                "max_output_ipc_bytes",
                runtime_result.encoded_bytes,
                max_output_ipc_bytes,
            ));
        }
    }

    Ok(())
}

fn validate_scan_overfetch_budget(
    plan: &BrowserExecutionPlan,
    runtime_result: &RuntimeArrowIpcResult,
    query_budget: BrowserQueryBudget,
) -> Result<(), QueryError> {
    let Some(max_scan_overfetch_bytes) = query_budget.max_scan_overfetch_bytes else {
        return Ok(());
    };
    let scan_overfetch_bytes =
        execution_range_read_metrics(plan, &runtime_result.scan_metrics).scan_overfetch_bytes();
    if scan_overfetch_bytes > max_scan_overfetch_bytes {
        return Err(browser_budget_exceeded_error(
            "max_scan_overfetch_bytes",
            scan_overfetch_bytes,
            max_scan_overfetch_bytes,
        ));
    }
    Ok(())
}

fn browser_budget_exceeded_error(
    observed_kind: &str,
    observed: u64,
    max_allowed: u64,
) -> QueryError {
    QueryError::new(
        QueryErrorCode::FallbackRequired,
        format!(
            "browser execution exceeded the configured {observed_kind} budget ({observed} > {max_allowed})"
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
    started_at: BrowserRuntimeInstant,
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
    let range_read_metrics = execution_range_read_metrics(plan, &runtime_result.scan_metrics);

    Ok(QueryMetricsSummary {
        bytes_fetched,
        duration_ms: started_at.elapsed_ms(),
        files_touched: plan.scan().candidate_file_count(),
        files_skipped: plan.pruning().files_pruned,
        prebootstrap_fail_open_count: plan.pruning().prebootstrap_fail_open_count,
        prebootstrap_files_pruned: plan.pruning().prebootstrap_files_pruned,
        footer_reads_avoided: plan.pruning().footer_reads_avoided,
        prebootstrap_candidate_files: plan.pruning().prebootstrap_candidate_files,
        row_groups_touched,
        row_groups_skipped,
        footer_reads: plan.footer_reads(),
        bootstrap_footer_range_reads: Some(range_read_metrics.bootstrap_footer_range_reads),
        scan_footer_range_reads: Some(range_read_metrics.scan_footer_range_reads),
        scan_data_range_reads: Some(range_read_metrics.scan_data_range_reads),
        duplicate_range_reads: Some(range_read_metrics.duplicate_range_reads),
        coalesced_range_reads: Some(range_read_metrics.coalesced_range_reads),
        coalesced_gap_bytes_fetched: Some(range_read_metrics.coalesced_gap_bytes_fetched),
        scan_overfetch_bytes: Some(range_read_metrics.scan_overfetch_bytes()),
        footer_cache_hits: Some(range_read_metrics.footer_cache_hits),
        footer_cache_misses: Some(range_read_metrics.footer_cache_misses),
        footer_range_reads_avoided: Some(range_read_metrics.footer_range_reads_avoided),
        footer_cache_degraded_identity_reads: Some(
            range_read_metrics.footer_cache_degraded_identity_reads,
        ),
        identity_present_range_reads: Some(range_read_metrics.identity_present_range_reads),
        identity_missing_range_reads: Some(range_read_metrics.identity_missing_range_reads),
        range_cache_hits: Some(range_read_metrics.range_cache_hits),
        range_cache_misses: Some(range_read_metrics.range_cache_misses),
        range_cache_bytes_reused: Some(range_read_metrics.range_cache_bytes_reused),
        range_cache_bytes_stored: Some(range_read_metrics.range_cache_bytes_stored),
        range_cache_validation_misses: Some(range_read_metrics.range_cache_validation_misses),
        range_cache_degraded_identity_reads: Some(
            range_read_metrics.range_cache_degraded_identity_reads,
        ),
        range_readahead_requests: Some(range_read_metrics.range_readahead_requests),
        range_readahead_bytes_fetched: Some(range_read_metrics.range_readahead_bytes_fetched),
        range_readahead_bytes_used: Some(range_read_metrics.range_readahead_bytes_used),
        range_readahead_wasted_bytes: Some(range_read_metrics.range_readahead_wasted_bytes),
        descriptor_resolution_count: None,
        delta_log_manifest_list_count: None,
        delta_log_manifest_list_duration_ms: None,
        snapshot_resolve_count: None,
        snapshot_resolve_duration_ms: None,
        descriptor_cache_hit: None,
        session_reuse_count: None,
        opened_table_reuse_count: None,
        identity_refresh_count: None,
        access_envelope_refresh_count: None,
        rows_emitted,
        snapshot_bootstrap_duration_ms: plan.snapshot_bootstrap_duration_ms(),
        access_mode: plan.access_mode(),
        arrow_ipc_bytes: Some(runtime_result.encoded_bytes),
        arrow_ipc_chunk_count: None,
        preview_rows: None,
        preview_string_bytes: None,
        planning_duration_ms: None,
        arrow_ipc_encode_duration_ms: Some(runtime_result.encode_duration_ms),
        preview_duration_ms: None,
        coordinator_peak_staged_bytes: None,
        coordinator_staging_limit_bytes: None,
        cursor_peak_pending_encoded_bytes: None,
        cursor_peak_transport_chunk_bytes: None,
    })
}

fn missing_table_error(name: &str) -> QueryError {
    QueryError::new(
        QueryErrorCode::InvalidRequest,
        format!("browser session does not have an open table named '{name}'"),
        runtime_target(),
    )
}

fn missing_table_file_error(name: &str, path: &str) -> QueryError {
    QueryError::new(
        QueryErrorCode::InvalidRequest,
        format!(
            "browser session table '{name}' does not have an active parquet file at path '{path}'"
        ),
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

        assert!(session.contains_table("events"));
        assert!(session
            .table("events")
            .expect("table should be cached")
            .bootstrapped_snapshot()
            .is_none());
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

    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Runtime::new().expect("test runtime should construct")
    }
}
