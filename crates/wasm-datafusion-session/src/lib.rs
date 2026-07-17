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
    BrowserHttpParquetDatasetDescriptor, BrowserHttpSnapshotDescriptor, CapabilityReport,
    ExecutionTarget, FallbackReason, ParquetInspectionSummary, PartitionColumnType, QueryError,
    QueryErrorCode, QueryMetricsSummary, QueryRequest, QueryResponse, QueryResultPage,
    QueryRuntimeLimits, MAX_QUERY_RESULT_PAGE_LIMIT,
};
use sqlparser::ast::{
    ObjectName, Query as SqlQuery, Select, SelectFlavor, SetExpr as SqlSetExpr,
    Statement as SqlStatement, TableFactor as SqlTableFactor, TableWithJoins as SqlTableWithJoins,
    Visit, Visitor,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
pub use wasm_datafusion_poc::{
    ArrowIpcPhase, IpcCursorMetrics, IpcPreview, IpcStreamLimits, IpcTransportChunk,
    QueryTerminalStatus,
};
use wasm_datafusion_poc::{
    BrowserQueryCancellation, CursorFault, DataFusionIpcCursor, DataFusionScanMetricsSummary,
    DeltaTableDescriptor, DeltaTableFieldDataType, DeltaTableSchema, DeltaTableSchemaField,
    IpcCursorItem, QueryCancelHandle, QueryTerminal, WasmDataFusionEngine,
};
use wasm_query_runtime::{
    runtime_target, BootstrappedBrowserSnapshot, BrowserExecutionBudget,
    BrowserParquetConvertedType, BrowserParquetField, BrowserParquetLogicalType,
    BrowserParquetPhysicalType, BrowserParquetRepetition, BrowserParquetTimeUnit,
    BrowserPrebootstrapPruningSummary, BrowserRuntimeConfig, BrowserRuntimeInstant,
    BrowserRuntimeSession, MaterializedBrowserSnapshot, PrebootstrappedBrowserSnapshot,
    RuntimeArrowIpcResult,
};

pub const OWNER: &str = "Runtime / engine team";
pub const RESPONSIBILITY: &str =
    "DataFusion-backed browser session for UI/runtime builds over browser-safe descriptors.";
const PARQUET_DATASET_RUNTIME_VERSION: i64 = 0;

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

#[derive(Clone, Debug, PartialEq)]
pub enum BrowserDataFusionIpcCursorItem {
    Chunk(IpcTransportChunk),
    Terminal(BrowserDataFusionQueryTerminal),
}

#[derive(Clone, Debug, PartialEq)]
pub struct BrowserDataFusionQueryTerminal {
    pub status: QueryTerminalStatus,
    pub error: Option<QueryError>,
    pub response: Option<QueryResponse>,
    pub row_count: u64,
    pub encoded_bytes: u64,
    pub preview: IpcPreview,
    pub cursor_metrics: IpcCursorMetrics,
}

pub struct BrowserDataFusionIpcCursor {
    cursor: DataFusionIpcCursor,
    completion: Option<BrowserDataFusionCursorCompletion>,
}

#[derive(Clone, Debug)]
pub struct BrowserDataFusionCursorCancellation {
    cancellation: QueryCancelHandle,
}

impl BrowserDataFusionCursorCancellation {
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    pub fn cancel_for_deadline(&self) {
        self.cancellation.cancel_for_deadline();
    }
}

impl std::fmt::Debug for BrowserDataFusionIpcCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrowserDataFusionIpcCursor")
            .field("cursor", &self.cursor)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
struct BrowserDataFusionCursorCompletion {
    capabilities: CapabilityReport,
    fallback_file_count: u64,
    footer_reads: Option<u64>,
    bootstrap_range_read_metrics: wasm_parquet_engine::ParquetRangeReadMetrics,
    snapshot_bootstrap_duration_ms: Option<u64>,
    access_mode: Option<query_contract::BrowserAccessMode>,
    prebootstrap_pruning: BrowserPrebootstrapPruningSummary,
    reuse_metrics: DataFusionSessionReuseMetrics,
    started_at: BrowserRuntimeInstant,
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

impl BrowserDataFusionIpcCursor {
    pub async fn next(&mut self) -> Result<Option<BrowserDataFusionIpcCursorItem>, CursorFault> {
        let Some(item) = self.cursor.next().await? else {
            return Ok(None);
        };
        Ok(Some(match item {
            IpcCursorItem::Chunk(chunk) => BrowserDataFusionIpcCursorItem::Chunk(chunk),
            IpcCursorItem::Terminal(terminal) => {
                BrowserDataFusionIpcCursorItem::Terminal(self.complete_terminal(terminal))
            }
        }))
    }

    pub fn cancel_handle(&self) -> BrowserDataFusionCursorCancellation {
        BrowserDataFusionCursorCancellation {
            cancellation: self.cursor.cancel_handle(),
        }
    }

    pub fn close(&mut self) {
        self.cursor.close();
        self.completion = None;
    }

    fn complete_terminal(&mut self, mut terminal: QueryTerminal) -> BrowserDataFusionQueryTerminal {
        let response = if terminal.status == QueryTerminalStatus::Succeeded {
            self.completion.take().and_then(|completion| {
                let scan_metrics = terminal.scan_metrics.take()?;
                let range_read_metrics = completion
                    .bootstrap_range_read_metrics
                    .merge(&scan_metrics.range_read_metrics);
                Some(QueryResponse {
                    executed_on: ExecutionTarget::BrowserWasm,
                    capabilities: completion.capabilities,
                    fallback_reason: None,
                    metrics: datafusion_query_metrics(
                        scan_metrics,
                        completion.fallback_file_count,
                        completion.footer_reads,
                        range_read_metrics,
                        completion.snapshot_bootstrap_duration_ms,
                        completion.access_mode,
                        completion.prebootstrap_pruning,
                        completion.reuse_metrics,
                        completion.started_at,
                        terminal.encoded_bytes,
                    ),
                    explain: terminal.physical_plan.take(),
                })
            })
        } else {
            self.completion = None;
            None
        };

        BrowserDataFusionQueryTerminal {
            status: terminal.status,
            error: terminal.error,
            response,
            row_count: terminal.row_count,
            encoded_bytes: terminal.encoded_bytes,
            preview: terminal.preview,
            cursor_metrics: terminal.cursor_metrics,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CachedDataFusionTable {
    descriptor: BrowserHttpSnapshotDescriptor,
    materialized: MaterializedBrowserSnapshot,
    last_prepared: Option<CachedPreparedDataFusionTable>,
    registered_table: Option<RegisteredDataFusionTable>,
    pending_reuse_metrics: DataFusionSessionReuseMetrics,
    cached_bytes: u64,
    last_used_millis: u64,
}

#[derive(Clone, Debug)]
struct PreparedDataFusionTable {
    descriptor: BrowserHttpSnapshotDescriptor,
    bootstrapped: BootstrappedBrowserSnapshot,
    pruning: BrowserPrebootstrapPruningSummary,
    cache_key: Option<PreparedDataFusionTableCacheKey>,
    reuse_metrics: DataFusionSessionReuseMetrics,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CachedPreparedDataFusionTable {
    key: PreparedDataFusionTableCacheKey,
    bootstrapped: BootstrappedBrowserSnapshot,
    pruning: BrowserPrebootstrapPruningSummary,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RegisteredDataFusionTable {
    key: PreparedDataFusionTableCacheKey,
    query_budget: BrowserDataFusionQueryBudget,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PreparedDataFusionTableCacheKey {
    sql: String,
    table_uri: String,
    snapshot_version: i64,
    partition_column_types: BTreeMap<String, PartitionColumnType>,
    browser_compatibility: CapabilityReport,
    required_capabilities: CapabilityReport,
    descriptor_files: Vec<PreparedDescriptorFileIdentity>,
    candidate_object_identities: Vec<PreparedCandidateObjectIdentity>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PreparedDescriptorFileIdentity {
    path: String,
    size_bytes: u64,
    partition_values: BTreeMap<String, Option<String>>,
    stats: Option<String>,
    object_etag: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PreparedCandidateObjectIdentity {
    path: String,
    size_bytes: u64,
    object_etag: String,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct DataFusionSessionReuseMetrics {
    session_reuse_count: u64,
    opened_table_reuse_count: u64,
    identity_refresh_count: u64,
    access_envelope_refresh_count: u64,
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
        let runtime = BrowserRuntimeSession::new(config)?;
        let metadata_cache = runtime.metadata_cache().clone();
        let range_cache = runtime.range_cache().clone();
        Ok(Self {
            runtime,
            query_budget,
            datafusion: WasmDataFusionEngine::with_budget_cancellation_and_caches(
                query_budget.into(),
                BrowserQueryCancellation::default(),
                metadata_cache,
                range_cache,
            ),
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
        let materialized = self.runtime.materialize_snapshot(&descriptor)?;
        self.open_materialized_table(name.into(), descriptor, materialized)
            .await
    }

    pub async fn open_parquet_dataset(
        &mut self,
        name: impl Into<String>,
        dataset: BrowserHttpParquetDatasetDescriptor,
    ) -> Result<(), QueryError> {
        let materialized = self.runtime.materialize_parquet_dataset(&dataset)?;
        let descriptor = snapshot_descriptor_from_parquet_dataset(dataset);
        self.open_materialized_table(name.into(), descriptor, materialized)
            .await
    }

    async fn open_materialized_table(
        &mut self,
        name: String,
        descriptor: BrowserHttpSnapshotDescriptor,
        materialized: MaterializedBrowserSnapshot,
    ) -> Result<(), QueryError> {
        let carried_prepared = self
            .tables
            .get(&name)
            .and_then(|table| table.last_prepared.as_ref())
            .and_then(|prepared| {
                let key = prepared_cache_key_for_descriptor_reopen(
                    &descriptor,
                    &prepared.bootstrapped,
                    &prepared.key.sql,
                )?;
                prepared_cache_key_matches_reopen(&prepared.key, &key).then(|| {
                    let mut prepared = prepared.clone();
                    prepared.key = key;
                    prepared
                })
            });
        let mut pending_reuse_metrics = DataFusionSessionReuseMetrics::default();
        if self.tables.contains_key(&name) {
            if carried_prepared.is_some() {
                pending_reuse_metrics.access_envelope_refresh_count = 1;
            } else if self
                .tables
                .get(&name)
                .and_then(|table| table.last_prepared.as_ref())
                .is_some()
            {
                pending_reuse_metrics.identity_refresh_count = 1;
            }
        }
        let cached_bytes = cached_table_bytes(
            &materialized,
            carried_prepared
                .as_ref()
                .map(|prepared| &prepared.bootstrapped),
        )?;
        let last_used_millis = self.bump_access_clock();

        if self.tables.contains_key(&name) {
            self.datafusion.deregister_table(&name)?;
            self.tables.remove(&name);
        }
        if materialized.active_files().is_empty() {
            self.register_datafusion_table_descriptor(&name, &descriptor, None, self.query_budget)
                .await?;
        }
        self.insert_table(
            name,
            descriptor,
            materialized,
            carried_prepared,
            pending_reuse_metrics,
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
        let query_budget =
            datafusion_query_budget_for_request(self.query_budget, request.options.runtime_limits);
        let max_pending_encoded_batch_bytes = query_budget
            .max_output_ipc_bytes
            .and_then(|max| usize::try_from(max).ok())
            .map(|max| max.min(8 * 1024 * 1024))
            .unwrap_or(8 * 1024 * 1024);
        let mut cursor = self
            .start_arrow_ipc_cursor(
                name,
                request,
                IpcStreamLimits {
                    max_transport_chunk_bytes: 1024 * 1024,
                    max_pending_encoded_batch_bytes,
                    max_total_encoded_bytes: query_budget.max_output_ipc_bytes,
                    preview_rows: 0,
                    max_preview_string_bytes: None,
                },
            )
            .await?;
        let mut ipc_bytes = Vec::new();

        while let Some(item) = cursor.next().await.map_err(cursor_fault_error)? {
            match item {
                BrowserDataFusionIpcCursorItem::Chunk(chunk) => {
                    ipc_bytes
                        .len()
                        .checked_add(chunk.bytes.len())
                        .ok_or_else(|| {
                            session_execution_error(
                                "browser DataFusion Arrow IPC byte lengths overflowed usize",
                            )
                        })?;
                    ipc_bytes.try_reserve(chunk.bytes.len()).map_err(|_| {
                        session_execution_error(
                            "browser DataFusion could not reserve Arrow IPC result bytes",
                        )
                    })?;
                    ipc_bytes.extend_from_slice(&chunk.bytes);
                }
                BrowserDataFusionIpcCursorItem::Terminal(terminal) => {
                    if terminal.status != QueryTerminalStatus::Succeeded {
                        return Err(terminal.error.unwrap_or_else(|| {
                            session_execution_error(
                                "browser DataFusion Arrow IPC cursor stopped without an error",
                            )
                        }));
                    }
                    let response = terminal.response.ok_or_else(|| {
                        session_execution_error(
                            "browser DataFusion Arrow IPC cursor succeeded without metadata",
                        )
                    })?;
                    return Ok(BrowserDataFusionSessionQueryResult {
                        response,
                        runtime_result: RuntimeArrowIpcResult {
                            ipc_bytes: ipc_bytes.into(),
                            row_count: terminal.row_count,
                            encoded_bytes: terminal.encoded_bytes,
                            encode_duration_ms: 0,
                            scan_metrics: Vec::new(),
                        },
                    });
                }
            }
        }

        Err(session_execution_error(
            "browser DataFusion Arrow IPC cursor closed before a terminal outcome",
        ))
    }

    pub async fn start_arrow_ipc_cursor(
        &mut self,
        name: &str,
        request: &QueryRequest,
        limits: IpcStreamLimits,
    ) -> Result<BrowserDataFusionIpcCursor, QueryError> {
        let execution_budget = self.runtime.config().execution_budget;
        let capabilities = {
            let table = self.touch_table(name)?;
            validate_datafusion_request_match(table, request)?;
            validate_datafusion_sql_scope(name, &request.sql)?;
            table.materialized.required_capabilities().clone()
        };
        let started_at = BrowserRuntimeInstant::now();
        let sql = datafusion_sql_with_result_page(&request.sql, request.options.result_page)?;
        let query_budget =
            datafusion_query_budget_for_request(self.query_budget, request.options.runtime_limits);
        let has_request_budget = query_budget != self.query_budget;
        let prepared = self
            .bootstrap_datafusion_table_for_sql(name, &request.sql)
            .await?;
        validate_datafusion_bootstrapped_execution_budget(
            &prepared.bootstrapped,
            execution_budget,
        )?;
        let mut reuse_metrics = prepared.reuse_metrics;
        if self
            .register_prepared_datafusion_table(name, &prepared, query_budget)
            .await?
        {
            reuse_metrics.opened_table_reuse_count =
                reuse_metrics.opened_table_reuse_count.saturating_add(1);
        }

        let cursor_result = self
            .datafusion
            .start_arrow_ipc_cursor(sql.as_ref(), limits, request.options.include_explain)
            .await;
        let cursor = if has_request_budget {
            let restore_result = self
                .register_prepared_datafusion_table(name, &prepared, self.query_budget)
                .await;
            match (cursor_result, restore_result) {
                (Ok(cursor), Ok(_)) => cursor,
                (Err(error), Ok(_)) => return Err(error),
                (Ok(_), Err(error)) => return Err(error),
                (Err(error), Err(_)) => return Err(error),
            }
        } else {
            cursor_result?
        };

        Ok(BrowserDataFusionIpcCursor {
            cursor,
            completion: Some(BrowserDataFusionCursorCompletion {
                capabilities,
                fallback_file_count: prepared.pruning.candidate_files,
                footer_reads: prepared.bootstrapped.footer_reads(),
                bootstrap_range_read_metrics: prepared.bootstrapped.range_read_metrics().clone(),
                snapshot_bootstrap_duration_ms: prepared
                    .bootstrapped
                    .snapshot_bootstrap_duration_ms(),
                access_mode: prepared.bootstrapped.access_mode(),
                prebootstrap_pruning: prepared.pruning,
                reuse_metrics,
                started_at,
            }),
        })
    }

    async fn bootstrap_datafusion_table_for_sql(
        &mut self,
        name: &str,
        sql: &str,
    ) -> Result<PreparedDataFusionTable, QueryError> {
        let (descriptor, materialized, cached_prepared, pending_reuse_metrics) = {
            let table = self
                .tables
                .get(name)
                .ok_or_else(|| missing_table_error(name))?;
            (
                table.descriptor.clone(),
                table.materialized.clone(),
                table.last_prepared.clone(),
                table.pending_reuse_metrics,
            )
        };

        if let Some(cached_prepared) = cached_prepared {
            if let Some(cache_key) =
                prepared_cache_key_for_bootstrapped(&descriptor, &cached_prepared.bootstrapped, sql)
            {
                if cache_key == cached_prepared.key {
                    let bootstrapped =
                        bootstrapped_snapshot_for_prepared_reuse(&cached_prepared.bootstrapped)?;
                    let filtered_descriptor =
                        datafusion_descriptor_for_bootstrapped_snapshot(&descriptor, &bootstrapped);
                    if let Some(table) = self.tables.get_mut(name) {
                        table.pending_reuse_metrics = DataFusionSessionReuseMetrics::default();
                    }
                    let mut reuse_metrics = pending_reuse_metrics;
                    reuse_metrics.session_reuse_count =
                        reuse_metrics.session_reuse_count.saturating_add(1);
                    return Ok(PreparedDataFusionTable {
                        descriptor: filtered_descriptor,
                        bootstrapped,
                        pruning: cached_prepared.pruning,
                        cache_key: Some(cache_key),
                        reuse_metrics,
                    });
                }
            }
        }

        let PrebootstrappedBrowserSnapshot {
            bootstrapped_snapshot,
            pruning,
        } = self
            .runtime
            .bootstrap_datafusion_sql_candidates(&materialized, name, sql)
            .await?;
        let filtered_descriptor =
            datafusion_descriptor_for_bootstrapped_snapshot(&descriptor, &bootstrapped_snapshot);
        let cached_bytes = cached_table_bytes(&materialized, Some(&bootstrapped_snapshot))?;
        let cache_key =
            prepared_cache_key_for_bootstrapped(&descriptor, &bootstrapped_snapshot, sql);

        if let Some(table) = self.tables.get_mut(name) {
            table.last_prepared = cache_key.clone().map(|key| CachedPreparedDataFusionTable {
                key,
                bootstrapped: bootstrapped_snapshot.clone(),
                pruning: pruning.clone(),
            });
            table.cached_bytes = cached_bytes;
            table.pending_reuse_metrics = DataFusionSessionReuseMetrics::default();
        }
        self.evict_to_budget(name)?;

        Ok(PreparedDataFusionTable {
            descriptor: filtered_descriptor,
            bootstrapped: bootstrapped_snapshot,
            pruning,
            cache_key,
            reuse_metrics: pending_reuse_metrics,
        })
    }

    async fn register_prepared_datafusion_table(
        &mut self,
        name: &str,
        prepared: &PreparedDataFusionTable,
        query_budget: BrowserDataFusionQueryBudget,
    ) -> Result<bool, QueryError> {
        if let Some(cache_key) = &prepared.cache_key {
            if self
                .tables
                .get(name)
                .and_then(|table| table.registered_table.as_ref())
                .is_some_and(|registered| {
                    registered.key == *cache_key && registered.query_budget == query_budget
                })
            {
                return Ok(true);
            }
        }

        self.register_datafusion_table_descriptor(
            name,
            &prepared.descriptor,
            Some(&prepared.bootstrapped),
            query_budget,
        )
        .await?;
        if let Some(table) = self.tables.get_mut(name) {
            table.registered_table = prepared
                .cache_key
                .clone()
                .map(|key| RegisteredDataFusionTable { key, query_budget });
        }
        Ok(false)
    }

    async fn register_datafusion_table_descriptor(
        &mut self,
        name: &str,
        descriptor: &BrowserHttpSnapshotDescriptor,
        bootstrapped: Option<&BootstrappedBrowserSnapshot>,
        query_budget: BrowserDataFusionQueryBudget,
    ) -> Result<(), QueryError> {
        let schema = datafusion_delta_schema(descriptor, bootstrapped)?;
        let object_etags = bootstrapped_object_etags_by_path(bootstrapped);
        let delta_descriptor = DeltaTableDescriptor::from_browser_http_snapshot_with_object_etags(
            name.to_string(),
            descriptor,
            schema,
            &object_etags,
        )?;
        self.datafusion.set_query_budget(query_budget.into());
        let _ = self.datafusion.deregister_table(name)?;
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
        last_prepared: Option<CachedPreparedDataFusionTable>,
        pending_reuse_metrics: DataFusionSessionReuseMetrics,
        cached_bytes: u64,
        last_used_millis: u64,
    ) -> Result<(), QueryError> {
        self.tables.insert(
            name.clone(),
            CachedDataFusionTable {
                descriptor,
                materialized,
                last_prepared,
                registered_table: None,
                pending_reuse_metrics,
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

    pub fn last_prepared_snapshot(&self) -> Option<&BootstrappedBrowserSnapshot> {
        self.last_prepared
            .as_ref()
            .map(|prepared| &prepared.bootstrapped)
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

fn bootstrapped_object_etags_by_path(
    bootstrapped: Option<&BootstrappedBrowserSnapshot>,
) -> BTreeMap<String, String> {
    bootstrapped
        .into_iter()
        .flat_map(BootstrappedBrowserSnapshot::active_files)
        .filter_map(|file| {
            file.object_etag()
                .map(|object_etag| (file.path().to_string(), object_etag.to_string()))
        })
        .collect()
}

fn datafusion_descriptor_for_bootstrapped_snapshot(
    descriptor: &BrowserHttpSnapshotDescriptor,
    bootstrapped: &BootstrappedBrowserSnapshot,
) -> BrowserHttpSnapshotDescriptor {
    let candidate_paths = bootstrapped
        .active_files()
        .iter()
        .map(|file| file.path().to_string())
        .collect::<BTreeSet<_>>();
    let mut descriptor = descriptor.clone();
    descriptor
        .active_files
        .retain(|file| candidate_paths.contains(&file.path));
    descriptor
}

fn prepared_cache_key_for_bootstrapped(
    descriptor: &BrowserHttpSnapshotDescriptor,
    bootstrapped: &BootstrappedBrowserSnapshot,
    sql: &str,
) -> Option<PreparedDataFusionTableCacheKey> {
    if descriptor.table_uri != bootstrapped.table_uri()
        || descriptor.snapshot_version != bootstrapped.snapshot_version()
    {
        return None;
    }

    let mut descriptor_files = descriptor
        .active_files
        .iter()
        .map(|file| {
            let object_etag = match file.object_etag.as_deref() {
                Some(object_etag) => Some(strong_quoted_object_etag(object_etag)?),
                None => None,
            };
            Some(PreparedDescriptorFileIdentity {
                path: file.path.clone(),
                size_bytes: file.size_bytes,
                partition_values: file.partition_values.clone(),
                stats: file.stats.clone(),
                object_etag,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    descriptor_files.sort_by(|left, right| left.path.cmp(&right.path));

    let descriptor_files_by_path = descriptor_files
        .iter()
        .map(|file| (file.path.as_str(), file))
        .collect::<BTreeMap<_, _>>();
    let mut candidate_object_identities = Vec::new();
    for file in bootstrapped.active_files() {
        let descriptor_file = descriptor_files_by_path.get(file.path())?;
        if descriptor_file.size_bytes != file.size_bytes() {
            return None;
        }
        let object_etag = strong_quoted_object_etag(file.object_etag()?)?;
        if descriptor_file
            .object_etag
            .as_ref()
            .is_some_and(|descriptor_object_etag| descriptor_object_etag != &object_etag)
        {
            return None;
        }
        candidate_object_identities.push(PreparedCandidateObjectIdentity {
            path: file.path().to_string(),
            size_bytes: file.size_bytes(),
            object_etag,
        });
    }
    candidate_object_identities.sort_by(|left, right| left.path.cmp(&right.path));

    Some(PreparedDataFusionTableCacheKey {
        sql: sql.to_string(),
        table_uri: descriptor.table_uri.clone(),
        snapshot_version: descriptor.snapshot_version,
        partition_column_types: descriptor.partition_column_types.clone(),
        browser_compatibility: descriptor.browser_compatibility.clone(),
        required_capabilities: descriptor.required_capabilities.clone(),
        descriptor_files,
        candidate_object_identities,
    })
}

fn prepared_cache_key_for_descriptor_reopen(
    descriptor: &BrowserHttpSnapshotDescriptor,
    bootstrapped: &BootstrappedBrowserSnapshot,
    sql: &str,
) -> Option<PreparedDataFusionTableCacheKey> {
    let key = prepared_cache_key_for_bootstrapped(descriptor, bootstrapped, sql)?;
    let descriptor_files_by_path = key
        .descriptor_files
        .iter()
        .map(|file| (file.path.as_str(), file))
        .collect::<BTreeMap<_, _>>();
    for candidate in &key.candidate_object_identities {
        let descriptor_file = descriptor_files_by_path.get(candidate.path.as_str())?;
        if descriptor_file.object_etag.as_deref()? != candidate.object_etag {
            return None;
        }
    }
    Some(key)
}

fn prepared_cache_key_matches_reopen(
    existing: &PreparedDataFusionTableCacheKey,
    candidate: &PreparedDataFusionTableCacheKey,
) -> bool {
    if existing == candidate {
        return true;
    }
    existing.sql == candidate.sql
        && existing.table_uri == candidate.table_uri
        && existing.snapshot_version == candidate.snapshot_version
        && existing.partition_column_types == candidate.partition_column_types
        && existing.browser_compatibility == candidate.browser_compatibility
        && existing.required_capabilities == candidate.required_capabilities
        && existing.candidate_object_identities == candidate.candidate_object_identities
        && descriptor_file_identities_match_reopen(
            &existing.descriptor_files,
            &candidate.descriptor_files,
        )
}

fn descriptor_file_identities_match_reopen(
    existing: &[PreparedDescriptorFileIdentity],
    candidate: &[PreparedDescriptorFileIdentity],
) -> bool {
    if existing.len() != candidate.len() {
        return false;
    }
    existing.iter().zip(candidate).all(|(existing, candidate)| {
        existing.path == candidate.path
            && existing.size_bytes == candidate.size_bytes
            && existing.partition_values == candidate.partition_values
            && existing.stats == candidate.stats
            && match (&existing.object_etag, &candidate.object_etag) {
                (Some(existing), Some(candidate)) => existing == candidate,
                (None, Some(_)) => true,
                _ => existing.object_etag == candidate.object_etag,
            }
    })
}

fn bootstrapped_snapshot_for_prepared_reuse(
    snapshot: &BootstrappedBrowserSnapshot,
) -> Result<BootstrappedBrowserSnapshot, QueryError> {
    BootstrappedBrowserSnapshot::new_with_partition_metadata_and_telemetry(
        snapshot.table_uri(),
        snapshot.snapshot_version(),
        snapshot.active_files().to_vec(),
        snapshot.partition_column_types().clone(),
        snapshot.required_capabilities().clone(),
        Some(0),
        Some(0),
        snapshot.access_mode(),
    )
}

fn strong_quoted_object_etag(etag: &str) -> Option<String> {
    let etag = etag.trim();
    if etag.starts_with("W/") || etag.starts_with("w/") {
        return None;
    }
    if !(etag.starts_with('"') && etag.ends_with('"')) {
        return None;
    }
    Some(etag.to_string())
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

fn datafusion_query_metrics(
    scan_metrics: DataFusionScanMetricsSummary,
    fallback_file_count: u64,
    footer_reads: Option<u64>,
    range_read_metrics: wasm_parquet_engine::ParquetRangeReadMetrics,
    snapshot_bootstrap_duration_ms: Option<u64>,
    access_mode: Option<query_contract::BrowserAccessMode>,
    prebootstrap_pruning: BrowserPrebootstrapPruningSummary,
    reuse_metrics: DataFusionSessionReuseMetrics,
    started_at: BrowserRuntimeInstant,
    encoded_bytes: u64,
) -> QueryMetricsSummary {
    QueryMetricsSummary {
        bytes_fetched: scan_metrics.bytes_fetched,
        duration_ms: started_at.elapsed_ms(),
        files_touched: if scan_metrics.scan_count > 0 {
            scan_metrics.files_touched
        } else {
            fallback_file_count
        },
        files_skipped: scan_metrics
            .files_skipped
            .saturating_add(prebootstrap_pruning.files_pruned),
        prebootstrap_fail_open_count: Some(prebootstrap_pruning.fail_open_count),
        prebootstrap_files_pruned: Some(prebootstrap_pruning.files_pruned),
        footer_reads_avoided: Some(prebootstrap_pruning.footer_reads_avoided),
        prebootstrap_candidate_files: Some(prebootstrap_pruning.candidate_files),
        row_groups_touched: scan_metrics.row_groups_touched,
        row_groups_skipped: scan_metrics.row_groups_skipped,
        footer_reads,
        bootstrap_footer_range_reads: Some(range_read_metrics.bootstrap_footer_range_reads),
        scan_footer_range_reads: Some(range_read_metrics.scan_footer_range_reads),
        scan_data_range_reads: Some(range_read_metrics.scan_data_range_reads),
        duplicate_range_reads: Some(range_read_metrics.duplicate_range_reads),
        coalesced_range_reads: Some(range_read_metrics.coalesced_range_reads),
        coalesced_gap_bytes_fetched: Some(range_read_metrics.coalesced_gap_bytes_fetched),
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
        session_reuse_count: Some(reuse_metrics.session_reuse_count),
        opened_table_reuse_count: Some(reuse_metrics.opened_table_reuse_count),
        identity_refresh_count: Some(reuse_metrics.identity_refresh_count),
        access_envelope_refresh_count: Some(reuse_metrics.access_envelope_refresh_count),
        rows_emitted: scan_metrics.rows_emitted,
        snapshot_bootstrap_duration_ms,
        access_mode,
        arrow_ipc_bytes: Some(encoded_bytes),
        arrow_ipc_chunk_count: None,
        preview_rows: None,
        preview_string_bytes: None,
        planning_duration_ms: None,
        arrow_ipc_encode_duration_ms: None,
        preview_duration_ms: None,
    }
}

fn cursor_fault_error(error: CursorFault) -> QueryError {
    session_execution_error(format!(
        "browser DataFusion Arrow IPC cursor fault: {error}"
    ))
}

fn session_execution_error(message: impl Into<String>) -> QueryError {
    QueryError::new(QueryErrorCode::ExecutionFailed, message, runtime_target())
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
        || !select.connect_by.is_empty()
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

fn validate_datafusion_bootstrapped_execution_budget(
    bootstrapped: &BootstrappedBrowserSnapshot,
    execution_budget: Option<BrowserExecutionBudget>,
) -> Result<(), QueryError> {
    let Some(execution_budget) = execution_budget else {
        return Ok(());
    };

    let estimated_rows = bootstrapped
        .active_files()
        .iter()
        .try_fold(0_u64, |total, file| {
            total
                .checked_add(file.metadata().row_count)
                .ok_or_else(|| datafusion_budget_total_overflow_error("row"))
        })?;
    if estimated_rows > execution_budget.max_rows {
        return Err(datafusion_budget_exceeded_error(
            "row estimate",
            estimated_rows,
            execution_budget.max_rows,
        ));
    }

    let estimated_bytes = bootstrapped
        .active_files()
        .iter()
        .try_fold(0_u64, |total, file| {
            total
                .checked_add(file.size_bytes())
                .ok_or_else(|| datafusion_budget_total_overflow_error("byte"))
        })?;
    if estimated_bytes > execution_budget.max_bytes {
        return Err(datafusion_budget_exceeded_error(
            "byte estimate",
            estimated_bytes,
            execution_budget.max_bytes,
        ));
    }

    Ok(())
}

fn min_optional_budget(session_limit: Option<u64>, request_limit: Option<u64>) -> Option<u64> {
    match (session_limit, request_limit) {
        (Some(session_limit), Some(request_limit)) => Some(session_limit.min(request_limit)),
        (Some(session_limit), None) => Some(session_limit),
        (None, Some(request_limit)) => Some(request_limit),
        (None, None) => None,
    }
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
    fn cursor_restores_default_registration_before_the_first_pull() {
        let default_budget = BrowserDataFusionQueryBudget {
            max_output_ipc_bytes: Some(4096),
            ..BrowserDataFusionQueryBudget::default()
        };
        let mut session = BrowserDataFusionSession::new_with_query_budget(
            BrowserRuntimeConfig::default(),
            u64::MAX,
            default_budget,
        )
        .expect("default browser runtime config should be supported");
        let descriptor = empty_delta_descriptor();
        let mut request = QueryRequest::new(
            descriptor.table_uri.clone(),
            "SELECT COUNT(*) AS rows FROM events",
            ExecutionTarget::BrowserWasm,
        );
        request.options.runtime_limits = Some(QueryRuntimeLimits {
            max_arrow_ipc_bytes: Some(2048),
            ..QueryRuntimeLimits::default()
        });

        test_runtime().block_on(async {
            session
                .open_delta_table("events", descriptor)
                .await
                .unwrap();
            let mut cursor = session
                .start_arrow_ipc_cursor(
                    "events",
                    &request,
                    IpcStreamLimits {
                        max_transport_chunk_bytes: 64,
                        max_pending_encoded_batch_bytes: 2048,
                        max_total_encoded_bytes: Some(2048),
                        preview_rows: 1,
                        max_preview_string_bytes: None,
                    },
                )
                .await
                .unwrap();

            assert_eq!(session.datafusion.query_budget(), default_budget.into());
            let first = cursor.next().await.unwrap().unwrap();
            assert!(matches!(first, BrowserDataFusionIpcCursorItem::Chunk(_)));
            cursor.close();
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
                max_preview_string_bytes: None,
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
    fn datafusion_sql_rejects_bootstrapped_tables_over_configured_byte_budget() {
        let snapshot = large_bootstrapped_snapshot();
        let budget = BrowserExecutionBudget {
            max_rows: u64::MAX,
            max_bytes: 1,
        };

        let error = validate_datafusion_bootstrapped_execution_budget(&snapshot, Some(budget))
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

    fn large_bootstrapped_snapshot() -> BootstrappedBrowserSnapshot {
        let metadata = wasm_query_runtime::BrowserParquetFileMetadata {
            object_size_bytes: 128,
            footer_length_bytes: 8,
            row_group_count: 1,
            row_count: 1,
            fields: vec![byte_array_field("payload", None, None)],
            field_stats: BTreeMap::new(),
        };
        let file = wasm_query_runtime::BootstrappedBrowserFile::new(
            "part-000.parquet",
            128,
            BTreeMap::new(),
            metadata,
        )
        .expect("bootstrapped file should construct");

        BootstrappedBrowserSnapshot::new("gs://axon-fixtures/large", 1, vec![file])
            .expect("large bootstrapped snapshot should construct")
    }

    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Runtime::new().expect("tokio runtime should be created")
    }
}
