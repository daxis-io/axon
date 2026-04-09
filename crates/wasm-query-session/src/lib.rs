//! In-memory browser session shell over runtime-owned snapshot state.

use std::collections::BTreeMap;
use std::time::Instant;

use query_contract::{
    BrowserHttpSnapshotDescriptor, ExecutionTarget, QueryError, QueryErrorCode,
    QueryMetricsSummary, QueryRequest, QueryResponse,
};
use wasm_query_runtime::{
    runtime_target, BootstrappedBrowserSnapshot, BrowserExecutionPlan, BrowserPlannedQuery,
    BrowserRuntimeConfig, BrowserRuntimeSession, MaterializedBrowserSnapshot,
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
    cached_bytes: u64,
    last_used_millis: u64,
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
        self.insert_table(name.into(), None, materialized, bootstrapped)
    }

    pub fn open_table(
        &mut self,
        name: impl Into<String>,
        descriptor: BrowserHttpSnapshotDescriptor,
    ) -> Result<(), QueryError> {
        let materialized = self.runtime.materialize_snapshot(&descriptor)?;
        self.insert_table(name.into(), Some(descriptor), materialized, None)
    }

    pub fn plan_query(
        &mut self,
        name: &str,
        request: &QueryRequest,
    ) -> Result<BrowserPlannedQuery, QueryError> {
        let snapshot = self.touch_bootstrapped_snapshot(name)?.clone();
        self.runtime.plan_query(&snapshot, request)
    }

    pub fn build_execution_plan(
        &mut self,
        name: &str,
        request: &QueryRequest,
    ) -> Result<BrowserExecutionPlan, QueryError> {
        let snapshot = self.touch_bootstrapped_snapshot(name)?.clone();
        self.runtime.build_execution_plan(&snapshot, request)
    }

    pub async fn sql(
        &mut self,
        name: &str,
        request: &QueryRequest,
    ) -> Result<BrowserSessionQueryResult, QueryError> {
        self.ensure_bootstrapped(name).await?;
        let materialized = self
            .table(name)
            .ok_or_else(|| missing_table_error(name))?
            .materialized
            .clone();
        let plan = self.build_execution_plan(name, request)?;
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

    async fn ensure_bootstrapped(&mut self, name: &str) -> Result<(), QueryError> {
        if self
            .table(name)
            .and_then(CachedTable::bootstrapped_snapshot)
            .is_some()
        {
            self.touch_table(name)?;
            return Ok(());
        }

        let materialized = self
            .table(name)
            .ok_or_else(|| missing_table_error(name))?
            .materialized
            .clone();
        let bootstrapped = self
            .runtime
            .bootstrap_snapshot_metadata(&materialized)
            .await?;
        let table = self.touch_table(name)?;
        table.bootstrapped = Some(bootstrapped);

        Ok(())
    }

    fn insert_table(
        &mut self,
        name: String,
        descriptor: Option<BrowserHttpSnapshotDescriptor>,
        materialized: MaterializedBrowserSnapshot,
        bootstrapped: Option<BootstrappedBrowserSnapshot>,
    ) -> Result<(), QueryError> {
        let cached_bytes = cached_bytes(&materialized)?;
        let last_used_millis = self.bump_access_clock();

        self.tables.insert(
            name.clone(),
            CachedTable {
                descriptor,
                materialized,
                bootstrapped,
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
    ) -> Result<&BootstrappedBrowserSnapshot, QueryError> {
        let table = self.touch_table(name)?;
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

fn cached_bytes(snapshot: &MaterializedBrowserSnapshot) -> Result<u64, QueryError> {
    snapshot
        .active_files()
        .iter()
        .try_fold(0_u64, |total, file| {
            total.checked_add(file.size_bytes()).ok_or_else(|| {
                QueryError::new(
                    QueryErrorCode::ExecutionFailed,
                    "browser session cached-byte totals overflowed u64",
                    runtime_target(),
                )
            })
        })
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
