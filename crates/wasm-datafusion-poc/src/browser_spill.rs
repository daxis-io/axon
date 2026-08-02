use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::io::{self, Read, Write};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::execution::spill_storage::{
    SpillFileRef, SpillReadMode, SpillReader, SpillScopeId, SpillStorage, SpillStorageAccounting,
    SpillWriter,
};
use wasm_bindgen::{prelude::*, JsCast};

const OPERATION_PENDING: u32 = 0;
const OPERATION_SUCCEEDED: u32 = 1;
const OPERATION_FAILED: u32 = 2;
const ERROR_UNAVAILABLE: u32 = 1;
const ERROR_QUOTA_EXCEEDED: u32 = 2;
const OPFS_BRIDGE_BUFFER_BYTES: usize = 256 * 1024;

thread_local! {
    static OPERATION_WAKERS: RefCell<HashMap<(u32, u32), Waker>> =
        RefCell::new(HashMap::new());
}

#[wasm_bindgen]
pub fn axon_spill_wake(execution_id: u32, operation_id: u32) {
    OPERATION_WAKERS.with(|wakers| {
        if let Some(waker) = wakers.borrow_mut().remove(&(execution_id, operation_id)) {
            waker.wake();
        }
    });
}

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = globalThis, js_name = axonSpillStartCreateScope)]
    fn start_create_scope(execution_id: u32) -> u32;
    #[wasm_bindgen(js_namespace = globalThis, js_name = axonSpillStartCreateWriter)]
    fn start_create_writer(execution_id: u32, scope_id: u32) -> u32;
    #[wasm_bindgen(js_namespace = globalThis, js_name = axonSpillStartOpenReader)]
    fn start_open_reader(execution_id: u32, file_id: u32) -> u32;
    #[wasm_bindgen(js_namespace = globalThis, js_name = axonSpillStartDeleteFile)]
    fn start_delete_file(execution_id: u32, file_id: u32) -> u32;
    #[wasm_bindgen(js_namespace = globalThis, js_name = axonSpillStartDeleteScope)]
    fn start_delete_scope(execution_id: u32, scope_id: u32) -> u32;
    #[wasm_bindgen(js_namespace = globalThis, js_name = axonSpillOperationStatus)]
    fn operation_status(execution_id: u32, operation_id: u32) -> u32;
    #[wasm_bindgen(js_namespace = globalThis, js_name = axonSpillOperationResultFirst)]
    fn operation_result_first(execution_id: u32, operation_id: u32) -> u32;
    #[wasm_bindgen(js_namespace = globalThis, js_name = axonSpillOperationResultSecond)]
    fn operation_result_second(execution_id: u32, operation_id: u32) -> u32;
    #[wasm_bindgen(js_namespace = globalThis, js_name = axonSpillOperationErrorKind)]
    fn operation_error_kind(execution_id: u32, operation_id: u32) -> u32;
    #[wasm_bindgen(js_namespace = globalThis, js_name = axonSpillReleaseOperation)]
    fn release_operation(execution_id: u32, operation_id: u32);
    #[wasm_bindgen(catch, js_namespace = globalThis, js_name = axonSpillWrite)]
    fn host_write(
        execution_id: u32,
        handle_id: u32,
        bytes: &[u8],
        at: usize,
    ) -> std::result::Result<usize, JsValue>;
    #[wasm_bindgen(catch, js_namespace = globalThis, js_name = axonSpillRead)]
    fn host_read(
        execution_id: u32,
        handle_id: u32,
        bytes: &mut [u8],
        at: usize,
    ) -> std::result::Result<usize, JsValue>;
    #[wasm_bindgen(catch, js_namespace = globalThis, js_name = axonSpillFlush)]
    fn host_flush(execution_id: u32, handle_id: u32) -> std::result::Result<(), JsValue>;
    #[wasm_bindgen(catch, js_namespace = globalThis, js_name = axonSpillClose)]
    fn host_close(execution_id: u32, handle_id: u32) -> std::result::Result<(), JsValue>;
    #[wasm_bindgen(js_namespace = globalThis, js_name = axonSpillBytesWritten)]
    fn bytes_written(execution_id: u32) -> f64;
    #[wasm_bindgen(js_namespace = globalThis, js_name = axonSpillBytesRead)]
    fn bytes_read(execution_id: u32) -> f64;
    #[wasm_bindgen(js_namespace = globalThis, js_name = axonSpillFilesCreated)]
    fn files_created(execution_id: u32) -> f64;
    #[wasm_bindgen(js_namespace = globalThis, js_name = axonSpillActiveBytes)]
    fn active_bytes(execution_id: u32) -> f64;
    #[wasm_bindgen(js_namespace = globalThis, js_name = axonSpillPeakActiveBytes)]
    fn peak_active_bytes(execution_id: u32) -> f64;
    #[wasm_bindgen(js_namespace = globalThis, js_name = axonSpillActiveFiles)]
    fn active_files(execution_id: u32) -> f64;
    #[wasm_bindgen(js_namespace = globalThis, js_name = axonSpillMergePasses)]
    fn merge_passes(execution_id: u32) -> f64;
    #[wasm_bindgen(js_namespace = globalThis, js_name = axonSpillRecordMergePass)]
    fn record_merge_pass(execution_id: u32);
}

#[derive(Clone)]
pub(crate) struct BrowserOpfsSpillStorage {
    execution_id: u32,
    memory_pool: Arc<dyn MemoryPool>,
}

impl BrowserOpfsSpillStorage {
    pub(crate) fn new(execution_id: u32, memory_pool: Arc<dyn MemoryPool>) -> Arc<Self> {
        Arc::new(Self {
            execution_id,
            memory_pool,
        })
    }

    fn unavailable(&self) -> Result<()> {
        if self.execution_id == 0 {
            Err(storage_error(ERROR_UNAVAILABLE))
        } else {
            Ok(())
        }
    }
}

impl fmt::Debug for BrowserOpfsSpillStorage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BrowserOpfsSpillStorage")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SpillStorage for BrowserOpfsSpillStorage {
    async fn create_scope(&self) -> Result<SpillScopeId> {
        self.unavailable()?;
        let (scope_id, _) =
            HostOperation::new(self.execution_id, start_create_scope(self.execution_id)).await?;
        Ok(SpillScopeId::new(scope_id.to_string()))
    }

    async fn create_writer(&self, scope: &SpillScopeId) -> Result<Box<dyn SpillWriter>> {
        self.unavailable()?;
        let scope_id = parse_opaque_id(scope.opaque_id(), "scope")?;
        let (file_id, handle_id) = HostOperation::new(
            self.execution_id,
            start_create_writer(self.execution_id, scope_id),
        )
        .await?;
        Ok(Box::new(OpfsWriter {
            execution_id: self.execution_id,
            scope: scope.clone(),
            file_id,
            handle_id,
            offset: 0,
            closed: false,
            buffer: Vec::new(),
            bridge_reservation: MemoryConsumer::new("BrowserOpfsSpillWriter")
                .register(&self.memory_pool),
        }))
    }

    async fn open_reader(&self, file: &SpillFileRef) -> Result<Box<dyn SpillReader>> {
        self.unavailable()?;
        let file_id = parse_opaque_id(file.opaque_id(), "file")?;
        let (handle_id, _) = HostOperation::new(
            self.execution_id,
            start_open_reader(self.execution_id, file_id),
        )
        .await?;
        Ok(Box::new(OpfsReader {
            execution_id: self.execution_id,
            handle_id,
            offset: 0,
            closed: false,
            buffer: Vec::new(),
            buffer_offset: 0,
            bridge_reservation: MemoryConsumer::new("BrowserOpfsSpillReader")
                .register(&self.memory_pool),
        }))
    }

    async fn delete_file(&self, file: &SpillFileRef) -> Result<()> {
        self.unavailable()?;
        let file_id = parse_opaque_id(file.opaque_id(), "file")?;
        HostOperation::new(
            self.execution_id,
            start_delete_file(self.execution_id, file_id),
        )
        .await?;
        Ok(())
    }

    async fn delete_scope(&self, scope: &SpillScopeId) -> Result<()> {
        self.unavailable()?;
        let scope_id = parse_opaque_id(scope.opaque_id(), "scope")?;
        HostOperation::new(
            self.execution_id,
            start_delete_scope(self.execution_id, scope_id),
        )
        .await?;
        Ok(())
    }

    fn accounting(&self) -> SpillStorageAccounting {
        if self.execution_id == 0 {
            return SpillStorageAccounting::default();
        }
        SpillStorageAccounting {
            bytes_written: numeric_metric(bytes_written(self.execution_id)),
            bytes_read: numeric_metric(bytes_read(self.execution_id)),
            files_created: numeric_metric(files_created(self.execution_id)),
            active_files: numeric_metric(active_files(self.execution_id)),
            active_bytes: numeric_metric(active_bytes(self.execution_id)),
            peak_active_bytes: numeric_metric(peak_active_bytes(self.execution_id)),
            merge_passes: numeric_metric(merge_passes(self.execution_id)),
        }
    }

    fn max_merge_fan_in(&self) -> Option<usize> {
        Some(8)
    }

    fn record_merge_pass(&self) {
        if self.execution_id != 0 {
            record_merge_pass(self.execution_id);
        }
    }
}

struct HostOperation {
    execution_id: u32,
    operation_id: u32,
}

impl HostOperation {
    fn new(execution_id: u32, operation_id: u32) -> Self {
        Self {
            execution_id,
            operation_id,
        }
    }

    fn current_result(&self) -> Option<Result<(u32, u32)>> {
        match operation_status(self.execution_id, self.operation_id) {
            OPERATION_PENDING => None,
            OPERATION_SUCCEEDED => {
                let result = (
                    operation_result_first(self.execution_id, self.operation_id),
                    operation_result_second(self.execution_id, self.operation_id),
                );
                release_operation(self.execution_id, self.operation_id);
                Some(Ok(result))
            }
            OPERATION_FAILED => {
                let kind = operation_error_kind(self.execution_id, self.operation_id);
                release_operation(self.execution_id, self.operation_id);
                Some(Err(storage_error(kind)))
            }
            _ => Some(Err(DataFusionError::Execution(
                "spill host returned an invalid operation status".to_owned(),
            ))),
        }
    }
}

impl Future for HostOperation {
    type Output = Result<(u32, u32)>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(result) = self.current_result() {
            return Poll::Ready(result);
        }
        OPERATION_WAKERS.with(|wakers| {
            wakers
                .borrow_mut()
                .insert((self.execution_id, self.operation_id), cx.waker().clone());
        });
        if let Some(result) = self.current_result() {
            OPERATION_WAKERS.with(|wakers| {
                wakers
                    .borrow_mut()
                    .remove(&(self.execution_id, self.operation_id));
            });
            Poll::Ready(result)
        } else {
            Poll::Pending
        }
    }
}

impl Drop for HostOperation {
    fn drop(&mut self) {
        OPERATION_WAKERS.with(|wakers| {
            wakers
                .borrow_mut()
                .remove(&(self.execution_id, self.operation_id));
        });
        release_operation(self.execution_id, self.operation_id);
    }
}

struct OpfsWriter {
    execution_id: u32,
    scope: SpillScopeId,
    file_id: u32,
    handle_id: u32,
    offset: usize,
    closed: bool,
    buffer: Vec<u8>,
    bridge_reservation: MemoryReservation,
}

impl Write for OpfsWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        if bytes.len() >= OPFS_BRIDGE_BUFFER_BYTES {
            self.flush_buffer()?;
            return self.write_direct(bytes);
        }
        self.ensure_buffer()?;
        if self.buffer.len() + bytes.len() > OPFS_BRIDGE_BUFFER_BYTES {
            self.flush_buffer()?;
        }
        self.buffer.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_buffer()?;
        host_flush(self.execution_id, self.handle_id).map_err(js_io_error)
    }
}

impl OpfsWriter {
    fn ensure_buffer(&mut self) -> io::Result<()> {
        if self.buffer.capacity() != 0 {
            return Ok(());
        }
        self.bridge_reservation
            .try_resize(OPFS_BRIDGE_BUFFER_BYTES)
            .map_err(|_| {
                io::Error::new(io::ErrorKind::OutOfMemory, "operator_memory/quota_exceeded")
            })?;
        if let Err(error) = self.buffer.try_reserve_exact(OPFS_BRIDGE_BUFFER_BYTES) {
            self.bridge_reservation.free();
            return Err(io::Error::other(error));
        }
        if let Err(error) = self.bridge_reservation.try_resize(self.buffer.capacity()) {
            self.buffer = Vec::new();
            self.bridge_reservation.free();
            return Err(io::Error::new(io::ErrorKind::OutOfMemory, error));
        }
        Ok(())
    }

    fn write_direct(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let retained = self.bridge_reservation.size();
        self.bridge_reservation
            .try_resize(retained.max(bytes.len()))
            .map_err(|_| {
                io::Error::new(io::ErrorKind::OutOfMemory, "operator_memory/quota_exceeded")
            })?;
        let result =
            host_write(self.execution_id, self.handle_id, bytes, self.offset).map_err(js_io_error);
        self.bridge_reservation.resize(retained);
        let written = result?;
        self.offset = self
            .offset
            .checked_add(written)
            .ok_or_else(|| io::Error::other("spill write offset overflow"))?;
        Ok(written)
    }

    fn flush_buffer(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let written = host_write(self.execution_id, self.handle_id, &self.buffer, self.offset)
            .map_err(js_io_error)?;
        self.offset = self
            .offset
            .checked_add(written)
            .ok_or_else(|| io::Error::other("spill write offset overflow"))?;
        self.buffer.clear();
        Ok(())
    }
}

impl SpillWriter for OpfsWriter {
    fn finish(mut self: Box<Self>) -> Result<SpillFileRef> {
        self.flush().map_err(DataFusionError::IoError)?;
        self.close().map_err(DataFusionError::IoError)?;
        Ok(SpillFileRef::new(
            self.scope.clone(),
            self.file_id.to_string(),
        ))
    }
}

impl OpfsWriter {
    fn close(&mut self) -> io::Result<()> {
        if self.closed {
            return Ok(());
        }
        host_close(self.execution_id, self.handle_id).map_err(js_io_error)?;
        self.closed = true;
        Ok(())
    }
}

impl Drop for OpfsWriter {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

struct OpfsReader {
    execution_id: u32,
    handle_id: u32,
    offset: usize,
    closed: bool,
    buffer: Vec<u8>,
    buffer_offset: usize,
    bridge_reservation: MemoryReservation,
}

impl Read for OpfsReader {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        if self.buffer_offset == self.buffer.len() {
            self.refill()?;
        }
        let available = &self.buffer[self.buffer_offset..];
        let read = available.len().min(bytes.len());
        bytes[..read].copy_from_slice(&available[..read]);
        self.buffer_offset += read;
        Ok(read)
    }
}

impl OpfsReader {
    fn refill(&mut self) -> io::Result<()> {
        if self.buffer.capacity() == 0 {
            self.bridge_reservation
                .try_resize(OPFS_BRIDGE_BUFFER_BYTES)
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::OutOfMemory, "operator_memory/quota_exceeded")
                })?;
            if let Err(error) = self.buffer.try_reserve_exact(OPFS_BRIDGE_BUFFER_BYTES) {
                self.bridge_reservation.free();
                return Err(io::Error::other(error));
            }
            if let Err(error) = self.bridge_reservation.try_resize(self.buffer.capacity()) {
                self.buffer = Vec::new();
                self.bridge_reservation.free();
                return Err(io::Error::new(io::ErrorKind::OutOfMemory, error));
            }
        }
        self.buffer.resize(OPFS_BRIDGE_BUFFER_BYTES, 0);
        let read = host_read(
            self.execution_id,
            self.handle_id,
            &mut self.buffer,
            self.offset,
        )
        .map_err(js_io_error)?;
        self.offset = self
            .offset
            .checked_add(read)
            .ok_or_else(|| io::Error::other("spill read offset overflow"))?;
        self.buffer.truncate(read);
        self.buffer_offset = 0;
        Ok(())
    }
}

impl SpillReader for OpfsReader {
    fn read_mode(&self) -> SpillReadMode {
        SpillReadMode::Inline
    }
}

impl OpfsReader {
    fn close(&mut self) -> io::Result<()> {
        if self.closed {
            return Ok(());
        }
        host_close(self.execution_id, self.handle_id).map_err(js_io_error)?;
        self.closed = true;
        Ok(())
    }
}

impl Drop for OpfsReader {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn storage_error(kind: u32) -> DataFusionError {
    match kind {
        ERROR_UNAVAILABLE => {
            DataFusionError::ResourcesExhausted("spill_storage/unavailable".to_owned())
        }
        ERROR_QUOTA_EXCEEDED => {
            DataFusionError::ResourcesExhausted("spill_storage/quota_exceeded".to_owned())
        }
        31 => DataFusionError::Execution("spill_storage/io_failure/create_scope".to_owned()),
        32 => DataFusionError::Execution("spill_storage/io_failure/create_writer".to_owned()),
        33 => DataFusionError::Execution("spill_storage/io_failure/open_reader".to_owned()),
        34 => DataFusionError::Execution("spill_storage/io_failure/delete_file".to_owned()),
        35 => DataFusionError::Execution("spill_storage/io_failure/delete_scope".to_owned()),
        _ => DataFusionError::Execution("spill_storage/io_failure".to_owned()),
    }
}

fn js_io_error(error: JsValue) -> io::Error {
    let message = error.as_string().or_else(|| {
        error
            .dyn_ref::<js_sys::Error>()
            .map(|error| String::from(error.message()))
    });
    let message = message.unwrap_or_default();
    if message.contains("quota exceeded") {
        io::Error::new(io::ErrorKind::StorageFull, "spill_storage/quota_exceeded")
    } else if message.contains("spill_storage/io_failure") {
        io::Error::other(message)
    } else {
        io::Error::other("spill_storage/io_failure")
    }
}

fn parse_opaque_id(value: &str, kind: &str) -> Result<u32> {
    value
        .parse::<u32>()
        .map_err(|_| DataFusionError::Internal(format!("invalid opaque spill {kind} identifier")))
}

fn numeric_metric(value: f64) -> u64 {
    if value.is_finite() && value >= 0.0 {
        value.min(u64::MAX as f64) as u64
    } else {
        0
    }
}
