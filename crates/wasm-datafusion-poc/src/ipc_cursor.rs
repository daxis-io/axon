use std::fmt;
use std::future::Future;
use std::io::{self, Write};
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use arrow_array::cast::{
    as_boolean_array, as_largestring_array, as_primitive_array, as_string_array,
};
use arrow_array::types::{
    Date32Type, Float32Type, Float64Type, Int32Type, Int64Type, TimestampMicrosecondType,
    TimestampMillisecondType,
};
use arrow_array::{Array, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, SchemaRef, TimeUnit};
use bytes::{Bytes, BytesMut};
use datafusion::physical_plan::{execute_stream, ExecutionPlan, SendableRecordBatchStream};
use futures_util::future::{select, Either};
use futures_util::task::AtomicWaker;
use futures_util::{pin_mut, StreamExt};
use query_contract::{FallbackReason, QueryError, QueryErrorCode};
use serde_json::{json, Value};
use wasm_parquet_engine::merge_parquet_range_read_metrics;

use super::{
    datafusion_metric_from_usize, datafusion_scan_metrics_from_plan, is_query_cancelled_error,
    map_arrow_ipc_error, map_datafusion_error, query_budget_exceeded_error_at,
    validate_query_budget_for_plan, validate_query_budget_value_option_at,
    validate_scan_overfetch_budget, BrowserQueryBudget, BrowserQueryCancellation,
    DataFusionScanMetricsSummary, ParquetRangeQueryContext, WasmDataFusionEngine,
};

pub const DEFAULT_IPC_TRANSPORT_CHUNK_BYTES: usize = 1024 * 1024;
pub const DEFAULT_MAX_PENDING_ENCODED_BATCH_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IpcStreamLimits {
    pub max_transport_chunk_bytes: usize,
    pub max_pending_encoded_batch_bytes: usize,
    pub max_total_encoded_bytes: Option<u64>,
    pub preview_rows: usize,
    pub max_preview_string_bytes: Option<u64>,
}

impl Default for IpcStreamLimits {
    fn default() -> Self {
        Self {
            max_transport_chunk_bytes: DEFAULT_IPC_TRANSPORT_CHUNK_BYTES,
            max_pending_encoded_batch_bytes: DEFAULT_MAX_PENDING_ENCODED_BATCH_BYTES,
            max_total_encoded_bytes: None,
            preview_rows: 0,
            max_preview_string_bytes: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArrowIpcPhase {
    Schema,
    Data,
    EndOfStream,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IpcTransportChunk {
    pub sequence: u64,
    pub phase: ArrowIpcPhase,
    pub logical_batch_sequence: Option<u64>,
    pub fragment_index: u32,
    pub end_of_logical_batch: bool,
    pub rows_completed: u64,
    pub bytes: Bytes,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryTerminalStatus {
    Succeeded,
    Cancelled,
    DeadlineExceeded,
    Failed,
}

#[derive(Clone, Debug, PartialEq)]
pub struct IpcPreview {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub row_count: u64,
    pub preview_row_limit: usize,
    pub truncated: bool,
    pub string_bytes: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IpcCursorMetrics {
    pub peak_input_batch_bytes: u64,
    pub peak_pending_encoded_bytes: u64,
    pub peak_pending_encoded_capacity: u64,
    pub peak_transport_chunk_bytes: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryTerminal {
    pub status: QueryTerminalStatus,
    pub error: Option<QueryError>,
    pub row_count: u64,
    pub encoded_bytes: u64,
    pub scan_metrics: Option<DataFusionScanMetricsSummary>,
    pub physical_plan: Option<String>,
    pub preview: IpcPreview,
    pub cursor_metrics: IpcCursorMetrics,
}

#[derive(Clone, Debug, PartialEq)]
pub enum IpcCursorItem {
    Chunk(IpcTransportChunk),
    Terminal(QueryTerminal),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CursorFault {
    pub message: String,
}

impl CursorFault {
    fn invariant(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for CursorFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for CursorFault {}

const CANCEL_ACTIVE: u8 = 0;
const CANCEL_REQUESTED: u8 = 1;
const CANCEL_DEADLINE: u8 = 2;

#[derive(Debug)]
struct QueryCancelState {
    reason: AtomicU8,
    waker: AtomicWaker,
}

#[derive(Clone, Debug)]
pub struct QueryCancelHandle {
    state: Arc<QueryCancelState>,
}

impl QueryCancelHandle {
    fn new() -> Self {
        Self {
            state: Arc::new(QueryCancelState {
                reason: AtomicU8::new(CANCEL_ACTIVE),
                waker: AtomicWaker::new(),
            }),
        }
    }

    pub fn cancel(&self) {
        self.request(CANCEL_REQUESTED);
    }

    pub fn cancel_for_deadline(&self) {
        self.request(CANCEL_DEADLINE);
    }

    pub fn is_cancelled(&self) -> bool {
        self.reason().is_some()
    }

    fn request(&self, reason: u8) {
        if self
            .state
            .reason
            .compare_exchange(CANCEL_ACTIVE, reason, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.state.waker.wake();
        }
    }

    fn reason(&self) -> Option<QueryTerminalStatus> {
        match self.state.reason.load(Ordering::SeqCst) {
            CANCEL_ACTIVE => None,
            CANCEL_REQUESTED => Some(QueryTerminalStatus::Cancelled),
            CANCEL_DEADLINE => Some(QueryTerminalStatus::DeadlineExceeded),
            _ => Some(QueryTerminalStatus::Failed),
        }
    }

    fn cancelled(&self) -> CancelledFuture {
        CancelledFuture {
            state: Arc::clone(&self.state),
        }
    }
}

struct CancelledFuture {
    state: Arc<QueryCancelState>,
}

impl Future for CancelledFuture {
    type Output = QueryTerminalStatus;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let reason = self.state.reason.load(Ordering::SeqCst);
        if reason != CANCEL_ACTIVE {
            return Poll::Ready(cancel_status(reason));
        }
        self.state.waker.register(cx.waker());
        let reason = self.state.reason.load(Ordering::SeqCst);
        if reason == CANCEL_ACTIVE {
            Poll::Pending
        } else {
            Poll::Ready(cancel_status(reason))
        }
    }
}

fn cancel_status(reason: u8) -> QueryTerminalStatus {
    match reason {
        CANCEL_REQUESTED => QueryTerminalStatus::Cancelled,
        CANCEL_DEADLINE => QueryTerminalStatus::DeadlineExceeded,
        _ => QueryTerminalStatus::Failed,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CursorState {
    Schema,
    Executing,
    EndOfStream,
    AwaitingTerminal,
    Terminal,
    Closed,
}

#[derive(Clone, Copy, Debug)]
struct PendingLogicalChunk {
    phase: ArrowIpcPhase,
    logical_batch_sequence: Option<u64>,
    fragment_index: u32,
    batch_rows: u64,
}

#[derive(Debug)]
struct SinkFaultState {
    error: Mutex<Option<QueryError>>,
}

impl SinkFaultState {
    fn take(&self) -> Option<QueryError> {
        self.error.lock().ok().and_then(|mut error| error.take())
    }

    fn set(&self, error: QueryError) {
        if let Ok(mut slot) = self.error.lock() {
            *slot = Some(error);
        }
    }
}

#[derive(Debug)]
struct DrainableIpcSink {
    pending: BytesMut,
    max_pending: usize,
    max_total: Option<u64>,
    total_encoded: u64,
    faults: Arc<SinkFaultState>,
}

impl DrainableIpcSink {
    fn new(max_pending: usize, max_total: Option<u64>, faults: Arc<SinkFaultState>) -> Self {
        Self {
            pending: BytesMut::new(),
            max_pending,
            max_total,
            total_encoded: 0,
            faults,
        }
    }

    fn pending_len(&self) -> usize {
        self.pending.len()
    }

    fn pending_capacity(&self) -> usize {
        self.pending.capacity()
    }

    fn total_encoded(&self) -> u64 {
        self.total_encoded
    }

    fn take_chunk(&mut self, max: usize) -> Bytes {
        let length = max.min(self.pending.len());
        self.pending.split_to(length).freeze()
    }

    fn reset_if_empty(&mut self) {
        if self.pending.is_empty() && self.pending.capacity() > 0 {
            self.pending = BytesMut::new();
        }
    }

    fn grow_for(&mut self, next_pending: usize) {
        if next_pending <= self.pending.capacity() {
            return;
        }
        let target = self
            .pending
            .capacity()
            .max(64)
            .saturating_mul(2)
            .max(next_pending)
            .min(self.max_pending);
        let mut pending = BytesMut::with_capacity(target);
        pending.extend_from_slice(&self.pending);
        self.pending = pending;
    }

    fn reject(&self, error: QueryError) -> io::Error {
        self.faults.set(error);
        io::Error::other("Arrow IPC cursor sink limit exceeded")
    }
}

impl Write for DrainableIpcSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let next_pending = self.pending.len().checked_add(buf.len()).ok_or_else(|| {
            self.reject(cursor_execution_error(
                "Arrow IPC pending byte length overflowed usize",
            ))
        })?;
        let buf_len = u64::try_from(buf.len()).map_err(|_| {
            self.reject(cursor_execution_error(
                "Arrow IPC write length overflowed u64",
            ))
        })?;
        let next_total = self.total_encoded.checked_add(buf_len).ok_or_else(|| {
            self.reject(cursor_execution_error(
                "Arrow IPC total byte length overflowed u64",
            ))
        })?;
        if let Some(max_total) = self.max_total {
            if next_total > max_total {
                return Err(self.reject(query_budget_exceeded_error_at(
                    "max_output_ipc_bytes",
                    next_total,
                    max_total,
                    "Arrow IPC output",
                )));
            }
        }
        if next_pending > self.max_pending {
            return Err(self.reject(query_budget_exceeded_error_at(
                "max_pending_encoded_batch_bytes",
                u64::try_from(next_pending).unwrap_or(u64::MAX),
                u64::try_from(self.max_pending).unwrap_or(u64::MAX),
                "Arrow IPC batch encoding",
            )));
        }

        self.grow_for(next_pending);
        self.pending.extend_from_slice(buf);
        self.total_encoded = next_total;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub struct DataFusionIpcCursor {
    state: CursorState,
    stream: Option<SendableRecordBatchStream>,
    plan: Option<Arc<dyn ExecutionPlan>>,
    query_context: Option<ParquetRangeQueryContext>,
    writer: Option<StreamWriter<DrainableIpcSink>>,
    sink_faults: Arc<SinkFaultState>,
    limits: IpcStreamLimits,
    query_budget: BrowserQueryBudget,
    cancel_handle: QueryCancelHandle,
    query_cancellation: BrowserQueryCancellation,
    pending: PendingLogicalChunk,
    next_sequence: u64,
    next_logical_batch_sequence: u64,
    rows_completed: u64,
    physical_plan: Option<String>,
    preview: PreviewBuilder,
    cursor_metrics: IpcCursorMetrics,
}

impl fmt::Debug for DataFusionIpcCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataFusionIpcCursor")
            .field("state", &self.state)
            .field("next_sequence", &self.next_sequence)
            .field(
                "next_logical_batch_sequence",
                &self.next_logical_batch_sequence,
            )
            .field("rows_completed", &self.rows_completed)
            .finish_non_exhaustive()
    }
}

impl DataFusionIpcCursor {
    pub async fn next(&mut self) -> Result<Option<IpcCursorItem>, CursorFault> {
        loop {
            if matches!(self.state, CursorState::Terminal | CursorState::Closed) {
                return Ok(None);
            }
            if let Some(status) = self.cancel_handle.reason() {
                return Ok(Some(IpcCursorItem::Terminal(self.finish_cancelled(status))));
            }
            if let Err(error) = self
                .query_cancellation
                .check_cancelled_at("Arrow IPC output")
            {
                return Ok(Some(IpcCursorItem::Terminal(self.finish_failed(error))));
            }
            if self.pending_len()? > 0 {
                return self.drain_pending().map(IpcCursorItem::Chunk).map(Some);
            }

            match self.state {
                CursorState::Schema => self.state = CursorState::Executing,
                CursorState::Executing => match self.poll_next_batch().await? {
                    NextBatch::Batch(batch) => {
                        if let Err(error) = self.prepare_batch(batch) {
                            return Ok(Some(IpcCursorItem::Terminal(self.finish_failed(error))));
                        }
                    }
                    NextBatch::Finished => {
                        if let Err(error) = self.finish_writer() {
                            return Ok(Some(IpcCursorItem::Terminal(self.finish_failed(error))));
                        }
                    }
                    NextBatch::Cancelled(status) => {
                        return Ok(Some(IpcCursorItem::Terminal(self.finish_cancelled(status))));
                    }
                    NextBatch::Failed(error) => {
                        return Ok(Some(IpcCursorItem::Terminal(self.finish_failed(error))));
                    }
                },
                CursorState::EndOfStream => self.state = CursorState::AwaitingTerminal,
                CursorState::AwaitingTerminal => {
                    return Ok(Some(IpcCursorItem::Terminal(self.finish_succeeded())));
                }
                CursorState::Terminal | CursorState::Closed => return Ok(None),
            }
        }
    }

    pub fn cancel_handle(&self) -> QueryCancelHandle {
        self.cancel_handle.clone()
    }

    pub fn close(&mut self) {
        self.state = CursorState::Closed;
        self.drop_execution_state();
    }

    fn pending_len(&self) -> Result<usize, CursorFault> {
        self.writer
            .as_ref()
            .map(|writer| writer.get_ref().pending_len())
            .ok_or_else(|| CursorFault::invariant("Arrow IPC cursor writer is missing"))
    }

    fn drain_pending(&mut self) -> Result<IpcTransportChunk, CursorFault> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| CursorFault::invariant("Arrow IPC cursor writer is missing"))?;
        let bytes = writer
            .get_mut()
            .take_chunk(self.limits.max_transport_chunk_bytes);
        let end_of_logical_batch = writer.get_ref().pending_len() == 0;
        let rows_completed = if end_of_logical_batch {
            self.rows_completed
                .checked_add(self.pending.batch_rows)
                .ok_or_else(|| {
                    CursorFault::invariant("Arrow IPC cursor row count overflowed u64")
                })?
        } else {
            self.rows_completed
        };
        let chunk = IpcTransportChunk {
            sequence: self.next_sequence,
            phase: self.pending.phase,
            logical_batch_sequence: self.pending.logical_batch_sequence,
            fragment_index: self.pending.fragment_index,
            end_of_logical_batch,
            rows_completed,
            bytes,
        };
        self.cursor_metrics.peak_transport_chunk_bytes = self
            .cursor_metrics
            .peak_transport_chunk_bytes
            .max(u64::try_from(chunk.bytes.len()).unwrap_or(u64::MAX));
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| CursorFault::invariant("Arrow IPC chunk sequence overflowed u64"))?;

        if end_of_logical_batch {
            self.rows_completed = rows_completed;
            match self.pending.phase {
                ArrowIpcPhase::Schema => self.state = CursorState::Executing,
                ArrowIpcPhase::Data => {
                    self.next_logical_batch_sequence = self
                        .next_logical_batch_sequence
                        .checked_add(1)
                        .ok_or_else(|| {
                            CursorFault::invariant(
                                "Arrow IPC logical batch sequence overflowed u64",
                            )
                        })?;
                    self.state = CursorState::Executing;
                }
                ArrowIpcPhase::EndOfStream => self.state = CursorState::AwaitingTerminal,
            }
        } else {
            self.pending.fragment_index =
                self.pending.fragment_index.checked_add(1).ok_or_else(|| {
                    CursorFault::invariant("Arrow IPC fragment index overflowed u32")
                })?;
        }

        Ok(chunk)
    }

    async fn poll_next_batch(&mut self) -> Result<NextBatch, CursorFault> {
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| CursorFault::invariant("Arrow IPC cursor stream is missing"))?;
        let cancel_handle = self.cancel_handle.clone();
        let next = stream.next();
        let cancelled = cancel_handle.cancelled();
        pin_mut!(next);
        pin_mut!(cancelled);
        match select(next, cancelled).await {
            Either::Left((item, _)) => {
                if let Some(status) = self.cancel_handle.reason() {
                    return Ok(NextBatch::Cancelled(status));
                }
                match item {
                    Some(Ok(batch)) => Ok(NextBatch::Batch(batch)),
                    Some(Err(error)) => {
                        let error = map_datafusion_error(error);
                        if is_query_cancelled_error(&error) {
                            Ok(NextBatch::Cancelled(QueryTerminalStatus::Cancelled))
                        } else {
                            Ok(NextBatch::Failed(error))
                        }
                    }
                    None => Ok(NextBatch::Finished),
                }
            }
            Either::Right((status, _)) => Ok(NextBatch::Cancelled(status)),
        }
    }

    fn prepare_batch(&mut self, batch: RecordBatch) -> Result<(), QueryError> {
        let retained_bytes = batch.get_array_memory_size();
        let retained_u64 =
            datafusion_metric_from_usize(retained_bytes, "retained input batch bytes")?;
        self.cursor_metrics.peak_input_batch_bytes =
            self.cursor_metrics.peak_input_batch_bytes.max(retained_u64);
        let pending_limit =
            u64::try_from(self.limits.max_pending_encoded_batch_bytes).unwrap_or(u64::MAX);
        if self
            .limits
            .max_total_encoded_bytes
            .is_some_and(|max_total| max_total <= pending_limit && retained_u64 > max_total)
        {
            validate_query_budget_value_option_at(
                "max_output_ipc_bytes",
                retained_u64,
                self.limits.max_total_encoded_bytes,
                "Arrow IPC output retained input batch",
            )?;
        } else {
            validate_query_budget_value_option_at(
                "max_pending_encoded_batch_bytes",
                retained_u64,
                Some(pending_limit),
                "retained input batch",
            )?;
        }

        let batch_rows = u64::try_from(batch.num_rows())
            .map_err(|_| cursor_execution_error("Arrow IPC batch row count overflowed u64"))?;
        let next_rows = self
            .rows_completed
            .checked_add(batch_rows)
            .ok_or_else(|| cursor_execution_error("Arrow IPC cursor row total overflowed u64"))?;
        validate_query_budget_value_option_at(
            "max_rows_returned",
            next_rows,
            self.query_budget.max_rows_returned,
            "Arrow IPC output",
        )?;
        validate_query_budget_value_option_at(
            "max_batches_in_flight",
            1,
            self.query_budget
                .max_batches_in_flight
                .map(|max| u64::try_from(max).unwrap_or(u64::MAX)),
            "Arrow IPC output",
        )?;
        self.preview.append(&batch)?;

        let writer = self.writer.as_mut().ok_or_else(|| {
            cursor_execution_error("Arrow IPC cursor writer is missing during batch encoding")
        })?;
        writer.get_mut().reset_if_empty();
        if let Err(error) = writer.write(&batch) {
            return Err(self.take_sink_or_arrow_error(error, "encode Arrow IPC batch"));
        }
        self.query_cancellation
            .check_cancelled_at("Arrow IPC batch encoding")?;
        self.record_pending_metrics();
        self.pending = PendingLogicalChunk {
            phase: ArrowIpcPhase::Data,
            logical_batch_sequence: Some(self.next_logical_batch_sequence),
            fragment_index: 0,
            batch_rows,
        };
        Ok(())
    }

    fn finish_writer(&mut self) -> Result<(), QueryError> {
        let writer = self.writer.as_mut().ok_or_else(|| {
            cursor_execution_error("Arrow IPC cursor writer is missing during EOS encoding")
        })?;
        writer.get_mut().reset_if_empty();
        if let Err(error) = writer.finish() {
            return Err(self.take_sink_or_arrow_error(error, "finish Arrow IPC stream"));
        }
        self.record_pending_metrics();
        self.pending = PendingLogicalChunk {
            phase: ArrowIpcPhase::EndOfStream,
            logical_batch_sequence: None,
            fragment_index: 0,
            batch_rows: 0,
        };
        self.state = CursorState::EndOfStream;
        Ok(())
    }

    fn take_sink_or_arrow_error(
        &self,
        error: arrow_schema::ArrowError,
        action: &str,
    ) -> QueryError {
        self.sink_faults.take().unwrap_or_else(|| {
            map_arrow_ipc_error(error, |error| {
                format!("experimental browser DataFusion could not {action}: {error}")
            })
        })
    }

    fn record_pending_metrics(&mut self) {
        if let Some(writer) = self.writer.as_ref() {
            self.cursor_metrics.peak_pending_encoded_bytes = self
                .cursor_metrics
                .peak_pending_encoded_bytes
                .max(u64::try_from(writer.get_ref().pending_len()).unwrap_or(u64::MAX));
            self.cursor_metrics.peak_pending_encoded_capacity = self
                .cursor_metrics
                .peak_pending_encoded_capacity
                .max(u64::try_from(writer.get_ref().pending_capacity()).unwrap_or(u64::MAX));
        }
    }

    fn finish_succeeded(&mut self) -> QueryTerminal {
        let result = self.finalize_scan_metrics();
        match result {
            Ok(scan_metrics) => {
                let terminal = QueryTerminal {
                    status: QueryTerminalStatus::Succeeded,
                    error: None,
                    row_count: self.rows_completed,
                    encoded_bytes: self.encoded_bytes(),
                    scan_metrics: Some(scan_metrics),
                    physical_plan: self.physical_plan.take(),
                    preview: self.preview.finish(),
                    cursor_metrics: self.cursor_metrics.clone(),
                };
                self.state = CursorState::Terminal;
                self.drop_execution_state();
                terminal
            }
            Err(error) => self.finish_failed(error),
        }
    }

    fn finish_failed(&mut self, error: QueryError) -> QueryTerminal {
        let status = if is_query_cancelled_error(&error) {
            QueryTerminalStatus::Cancelled
        } else {
            QueryTerminalStatus::Failed
        };
        self.finish_terminal(status, Some(error))
    }

    fn finish_cancelled(&mut self, status: QueryTerminalStatus) -> QueryTerminal {
        let message = match status {
            QueryTerminalStatus::DeadlineExceeded => {
                "experimental browser DataFusion query deadline exceeded during Arrow IPC cursor pull"
            }
            _ => "experimental browser DataFusion query cancelled during Arrow IPC cursor pull",
        };
        let error = QueryError::new(
            QueryErrorCode::ExecutionFailed,
            message,
            super::runtime_target(),
        );
        self.finish_terminal(status, Some(error))
    }

    fn finish_terminal(
        &mut self,
        status: QueryTerminalStatus,
        error: Option<QueryError>,
    ) -> QueryTerminal {
        let terminal = QueryTerminal {
            status,
            error,
            row_count: self.rows_completed,
            encoded_bytes: self.encoded_bytes(),
            scan_metrics: None,
            physical_plan: self.physical_plan.take(),
            preview: self.preview.finish(),
            cursor_metrics: self.cursor_metrics.clone(),
        };
        self.state = CursorState::Terminal;
        self.drop_execution_state();
        terminal
    }

    fn finalize_scan_metrics(&mut self) -> Result<DataFusionScanMetricsSummary, QueryError> {
        let plan = self.plan.as_ref().ok_or_else(|| {
            cursor_execution_error("Arrow IPC cursor plan is missing during finalization")
        })?;
        let mut scan_metrics = datafusion_scan_metrics_from_plan(plan)?;
        let query_context = self.query_context.take().ok_or_else(|| {
            cursor_execution_error("Arrow IPC query context is missing during finalization")
        })?;
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
        validate_scan_overfetch_budget(
            &scan_metrics.range_read_metrics,
            self.query_budget,
            "scan stream",
        )?;
        Ok(scan_metrics)
    }

    fn encoded_bytes(&self) -> u64 {
        self.writer
            .as_ref()
            .map(|writer| writer.get_ref().total_encoded())
            .unwrap_or(0)
    }

    fn drop_execution_state(&mut self) {
        self.stream = None;
        self.plan = None;
        self.query_context = None;
        self.writer = None;
    }
}

impl Drop for DataFusionIpcCursor {
    fn drop(&mut self) {
        self.drop_execution_state();
    }
}

enum NextBatch {
    Batch(RecordBatch),
    Finished,
    Cancelled(QueryTerminalStatus),
    Failed(QueryError),
}

impl WasmDataFusionEngine {
    pub async fn start_arrow_ipc_cursor(
        &self,
        sql: &str,
        mut limits: IpcStreamLimits,
        include_physical_plan: bool,
    ) -> Result<DataFusionIpcCursor, QueryError> {
        validate_stream_limits(limits)?;
        limits.max_total_encoded_bytes = min_optional_u64(
            limits.max_total_encoded_bytes,
            self.query_budget().max_output_ipc_bytes,
        );
        if let Some(max_total) = limits.max_total_encoded_bytes {
            limits.max_pending_encoded_batch_bytes = limits
                .max_pending_encoded_batch_bytes
                .min(usize::try_from(max_total).unwrap_or(usize::MAX));
            if limits.max_pending_encoded_batch_bytes == 0 {
                return Err(query_budget_exceeded_error_at(
                    "max_output_ipc_bytes",
                    1,
                    max_total,
                    "Arrow IPC schema",
                ));
            }
        }
        if matches!(self.query_budget().max_batches_in_flight, Some(0)) {
            return Err(query_budget_exceeded_error_at(
                "max_batches_in_flight",
                1,
                0,
                "Arrow IPC output",
            ));
        }

        let query_cancellation = self.cancellation_token().snapshot();
        query_cancellation.check_cancelled_at("SQL planning")?;
        let (query_ctx, query_context) = self.begin_query_session(&query_cancellation);
        let frame = query_ctx.sql(sql).await.map_err(map_datafusion_error)?;
        query_cancellation.check_cancelled_at("SQL planning")?;
        let output_schema = frame.schema().inner().clone();
        reject_dictionary_schema(&output_schema)?;
        let task_ctx = query_ctx.task_ctx();
        let plan = frame
            .create_physical_plan()
            .await
            .map_err(map_datafusion_error)?;
        validate_query_budget_for_plan(&plan, self.query_budget())?;
        let physical_plan = include_physical_plan.then(|| {
            format!(
                "DataFusion physical plan\n{}",
                datafusion::physical_plan::displayable(plan.as_ref()).indent(false)
            )
        });
        let stream = execute_stream(Arc::clone(&plan), task_ctx).map_err(map_datafusion_error)?;
        let faults = Arc::new(SinkFaultState {
            error: Mutex::new(None),
        });
        let sink = DrainableIpcSink::new(
            limits.max_pending_encoded_batch_bytes,
            limits.max_total_encoded_bytes,
            Arc::clone(&faults),
        );
        let writer = StreamWriter::try_new(sink, output_schema.as_ref()).map_err(|error| {
            faults.take().unwrap_or_else(|| {
                map_arrow_ipc_error(error, |error| {
                    format!(
                        "experimental browser DataFusion could not open Arrow IPC cursor writer: {error}"
                    )
                })
            })
        })?;
        let mut cursor = DataFusionIpcCursor {
            state: CursorState::Schema,
            stream: Some(stream),
            plan: Some(plan),
            query_context: Some(query_context),
            writer: Some(writer),
            sink_faults: faults,
            limits,
            query_budget: self.query_budget(),
            cancel_handle: QueryCancelHandle::new(),
            query_cancellation,
            pending: PendingLogicalChunk {
                phase: ArrowIpcPhase::Schema,
                logical_batch_sequence: None,
                fragment_index: 0,
                batch_rows: 0,
            },
            next_sequence: 0,
            next_logical_batch_sequence: 0,
            rows_completed: 0,
            physical_plan,
            preview: PreviewBuilder::new(output_schema, limits),
            cursor_metrics: IpcCursorMetrics::default(),
        };
        cursor.record_pending_metrics();
        Ok(cursor)
    }
}

fn validate_stream_limits(limits: IpcStreamLimits) -> Result<(), QueryError> {
    if limits.max_transport_chunk_bytes == 0 {
        return Err(cursor_invalid_request(
            "max_transport_chunk_bytes must be positive",
        ));
    }
    if limits.max_pending_encoded_batch_bytes == 0 {
        return Err(cursor_invalid_request(
            "max_pending_encoded_batch_bytes must be positive",
        ));
    }
    Ok(())
}

fn min_optional_u64(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn reject_dictionary_schema(schema: &SchemaRef) -> Result<(), QueryError> {
    if schema
        .fields()
        .iter()
        .any(|field| contains_dictionary(field.data_type()))
    {
        return Err(QueryError::new(
            QueryErrorCode::UnsupportedFeature,
            "experimental browser DataFusion Arrow IPC dictionary output is unsupported in cursor v0",
            super::runtime_target(),
        ));
    }
    Ok(())
}

fn contains_dictionary(data_type: &DataType) -> bool {
    match data_type {
        DataType::Dictionary(_, _) => true,
        DataType::List(field)
        | DataType::ListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::LargeList(field)
        | DataType::LargeListView(field)
        | DataType::Map(field, _) => contains_dictionary(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| contains_dictionary(field.data_type())),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| contains_dictionary(field.data_type())),
        DataType::RunEndEncoded(run_ends, values) => {
            contains_dictionary(run_ends.data_type()) || contains_dictionary(values.data_type())
        }
        _ => false,
    }
}

struct PreviewBuilder {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
    row_count: u64,
    row_limit: usize,
    string_bytes: u64,
    max_string_bytes: Option<u64>,
}

impl PreviewBuilder {
    fn new(schema: SchemaRef, limits: IpcStreamLimits) -> Self {
        Self {
            columns: schema
                .fields()
                .iter()
                .map(|field| field.name().to_string())
                .collect(),
            rows: Vec::new(),
            row_count: 0,
            row_limit: limits.preview_rows,
            string_bytes: 0,
            max_string_bytes: limits.max_preview_string_bytes,
        }
    }

    fn append(&mut self, batch: &RecordBatch) -> Result<(), QueryError> {
        let batch_rows = u64::try_from(batch.num_rows())
            .map_err(|_| cursor_execution_error("preview batch row count overflowed u64"))?;
        let next_row_count = self
            .row_count
            .checked_add(batch_rows)
            .ok_or_else(|| cursor_execution_error("preview row count overflowed u64"))?;
        let preview_prefix_len =
            preview_prefix_len(batch.num_rows(), self.rows.len(), self.row_limit);
        for row_index in 0..preview_prefix_len {
            let row = batch
                .columns()
                .iter()
                .map(|array| preview_value(array.as_ref(), row_index))
                .collect::<Vec<_>>();
            let row_string_bytes = row.iter().try_fold(0_u64, |total, value| {
                total
                    .checked_add(
                        value
                            .as_str()
                            .map(str::len)
                            .map(|value| u64::try_from(value).unwrap_or(u64::MAX))
                            .unwrap_or(0),
                    )
                    .ok_or_else(|| cursor_execution_error("preview string bytes overflowed u64"))
            })?;
            let next_string_bytes = self
                .string_bytes
                .checked_add(row_string_bytes)
                .ok_or_else(|| cursor_execution_error("preview string bytes overflowed u64"))?;
            if let Some(max_allowed) = self.max_string_bytes {
                if next_string_bytes > max_allowed {
                    return Err(QueryError::new(
                        QueryErrorCode::FallbackRequired,
                        format!(
                            "browser execution exceeded the configured max_preview_string_bytes budget ({next_string_bytes} > {max_allowed})"
                        ),
                        super::runtime_target(),
                    )
                    .with_fallback_reason(FallbackReason::BrowserRuntimeConstraint));
                }
            }
            self.string_bytes = next_string_bytes;
            self.rows.push(row);
        }
        self.row_count = next_row_count;
        Ok(())
    }

    fn finish(&mut self) -> IpcPreview {
        IpcPreview {
            columns: std::mem::take(&mut self.columns),
            rows: std::mem::take(&mut self.rows),
            row_count: self.row_count,
            preview_row_limit: self.row_limit,
            truncated: self.row_count > u64::try_from(self.row_limit).unwrap_or(u64::MAX),
            string_bytes: self.string_bytes,
        }
    }
}

fn preview_prefix_len(batch_rows: usize, materialized_rows: usize, row_limit: usize) -> usize {
    row_limit.saturating_sub(materialized_rows).min(batch_rows)
}

fn preview_value(array: &dyn Array, row_index: usize) -> Value {
    if array.is_null(row_index) {
        return Value::Null;
    }
    match array.data_type() {
        DataType::Boolean => json!(as_boolean_array(array).value(row_index)),
        DataType::Int32 => json!(as_primitive_array::<Int32Type>(array).value(row_index)),
        DataType::Int64 => json!(as_primitive_array::<Int64Type>(array)
            .value(row_index)
            .to_string()),
        DataType::Float32 => json!(as_primitive_array::<Float32Type>(array).value(row_index)),
        DataType::Float64 => {
            serde_json::Number::from_f64(as_primitive_array::<Float64Type>(array).value(row_index))
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        DataType::Date32 => as_primitive_array::<Date32Type>(array)
            .value_as_date(row_index)
            .map(|value| json!(value.to_string()))
            .unwrap_or(Value::Null),
        DataType::Timestamp(TimeUnit::Millisecond, Some(timezone))
            if is_utc_timezone(timezone.as_ref()) =>
        {
            as_primitive_array::<TimestampMillisecondType>(array)
                .value_as_datetime(row_index)
                .map(|value| json!(format!("{}Z", value.format("%Y-%m-%dT%H:%M:%S%.3f"))))
                .unwrap_or(Value::Null)
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(timezone))
            if is_utc_timezone(timezone.as_ref()) =>
        {
            as_primitive_array::<TimestampMicrosecondType>(array)
                .value_as_datetime(row_index)
                .map(|value| json!(format!("{}Z", value.format("%Y-%m-%dT%H:%M:%S%.6f"))))
                .unwrap_or(Value::Null)
        }
        DataType::Utf8 => json!(as_string_array(array).value(row_index)),
        DataType::LargeUtf8 => json!(as_largestring_array(array).value(row_index)),
        other => json!(format!("<unsupported {other}>")),
    }
}

fn is_utc_timezone(timezone: &str) -> bool {
    matches!(timezone, "UTC" | "Z" | "+00:00" | "-00:00")
}

fn cursor_invalid_request(message: impl Into<String>) -> QueryError {
    QueryError::new(
        QueryErrorCode::InvalidRequest,
        message,
        super::runtime_target(),
    )
}

fn cursor_execution_error(message: impl Into<String>) -> QueryError {
    QueryError::new(
        QueryErrorCode::ExecutionFailed,
        message,
        super::runtime_target(),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Fields};

    use super::{contains_dictionary, preview_prefix_len};

    #[test]
    fn preview_prefix_covers_only_rows_remaining_under_the_limit() {
        assert_eq!(preview_prefix_len(10, 0, 2), 2);
        assert_eq!(preview_prefix_len(10, 1, 2), 1);
        assert_eq!(preview_prefix_len(10, 2, 2), 0);
        assert_eq!(preview_prefix_len(1, 0, usize::MAX), 1);
    }

    #[test]
    fn dictionary_detection_descends_through_nested_output_types() {
        let dictionary = DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
        let nested = DataType::Struct(Fields::from(vec![Field::new(
            "items",
            DataType::List(Arc::new(Field::new("item", dictionary, true))),
            true,
        )]));

        assert!(contains_dictionary(&nested));
    }
}
