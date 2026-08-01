// Licensed under the Apache License, Version 2.0.

//! Worker-local OPFS adapter for DataFusion's path-free spill contract.
//!
//! Browser objects remain in the TypeScript host registry. Rust carries only
//! opaque numeric IDs and owned byte buffers across DataFusion's `Send` seams.

#[cfg(target_feature = "atomics")]
compile_error!("the OPFS spill bridge requires Axon's single-threaded wasm32 worker build");

use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion_execution::spill_storage::{
    SpillAppendWriter, SpillFileRef, SpillScope, SpillScopeId, SpillSequentialReader, SpillStorage,
    SpillStorageError, SpillStorageErrorReason, SpillStorageMetrics, SpillStorageResult,
};
use js_sys::{Promise, Reflect, Uint8Array};
use wasm_bindgen::prelude::wasm_bindgen;
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::JsFuture;

#[wasm_bindgen(inline_js = r#"
const KEY = "__axon_opfs_spill_v1__";
function bridge() {
  const value = globalThis[KEY];
  if (value !== undefined) return value;
  const error = new Error("browser spill storage is unavailable");
  error.reason = "unavailable";
  throw error;
}
export function axon_spill_create_scope() {
  return Promise.resolve().then(() => bridge().createScope());
}
export function axon_spill_create_file(scopeId) {
  return Promise.resolve().then(() => bridge().createFile(scopeId));
}
export function axon_spill_append(writerId, bytes) {
  bridge().append(writerId, bytes);
  return Promise.resolve();
}
export function axon_spill_finalize_writer(writerId) {
  bridge().finalizeWriter(writerId);
  return Promise.resolve();
}
export function axon_spill_open_reader(scopeId, fileId) {
  return Promise.resolve().then(() => bridge().openReader(scopeId, fileId));
}
export function axon_spill_read_next(readerId, target) {
  return bridge().readNext(readerId, target);
}
export function axon_spill_close_reader(readerId) {
  return bridge().closeReader(readerId);
}
export function axon_spill_delete_file(scopeId, fileId) {
  return Promise.resolve().then(() => bridge().deleteFile(scopeId, fileId));
}
export function axon_spill_delete_scope(scopeId) {
  return Promise.resolve().then(() => bridge().deleteScope(scopeId));
}
export function axon_spill_release_scope(scopeId) {
  Promise.resolve().then(() => bridge().releaseScope(scopeId)).catch(() => {});
}
export function axon_spill_record_merge_pass() {
  return bridge().recordMergePass();
}
export function axon_spill_metrics() {
  return bridge().metrics();
}
"#)]
extern "C" {
    #[wasm_bindgen(catch)]
    fn axon_spill_create_scope() -> Result<Promise, JsValue>;
    #[wasm_bindgen(catch)]
    fn axon_spill_create_file(scope_id: f64) -> Result<Promise, JsValue>;
    #[wasm_bindgen(catch)]
    fn axon_spill_append(writer_id: f64, bytes: &Uint8Array) -> Result<Promise, JsValue>;
    #[wasm_bindgen(catch)]
    fn axon_spill_finalize_writer(writer_id: f64) -> Result<Promise, JsValue>;
    #[wasm_bindgen(catch)]
    fn axon_spill_open_reader(scope_id: f64, file_id: f64) -> Result<Promise, JsValue>;
    #[wasm_bindgen(catch)]
    fn axon_spill_read_next(reader_id: f64, target: &Uint8Array) -> Result<JsValue, JsValue>;
    #[wasm_bindgen(catch)]
    fn axon_spill_close_reader(reader_id: f64) -> Result<(), JsValue>;
    #[wasm_bindgen(catch)]
    fn axon_spill_delete_file(scope_id: f64, file_id: f64) -> Result<Promise, JsValue>;
    #[wasm_bindgen(catch)]
    fn axon_spill_delete_scope(scope_id: f64) -> Result<Promise, JsValue>;
    #[wasm_bindgen(catch)]
    fn axon_spill_release_scope(scope_id: f64) -> Result<(), JsValue>;
    #[wasm_bindgen(catch)]
    fn axon_spill_record_merge_pass() -> Result<(), JsValue>;
    #[wasm_bindgen(catch)]
    fn axon_spill_metrics() -> Result<JsValue, JsValue>;
}

/// OPFS backend installed into the browser query runtime.
#[derive(Debug, Default)]
pub(crate) struct OpfsSpillStorage;

#[async_trait::async_trait]
impl SpillStorage for OpfsSpillStorage {
    async fn create_scope(&self) -> SpillStorageResult<Arc<dyn SpillScope>> {
        let promise = axon_spill_create_scope().map_err(js_error)?;
        let scope_id = send_js_future(promise, map_opaque_id).await?;
        Ok(Arc::new(OpfsSpillScope { scope_id }))
    }
}

#[derive(Debug)]
struct OpfsSpillScope {
    scope_id: u64,
}

impl Drop for OpfsSpillScope {
    fn drop(&mut self) {
        let _ = axon_spill_release_scope(self.scope_id as f64);
    }
}

#[async_trait::async_trait]
impl SpillScope for OpfsSpillScope {
    fn id(&self) -> SpillScopeId {
        SpillScopeId::new(self.scope_id)
    }

    async fn create_file(&self) -> SpillStorageResult<(SpillFileRef, Box<dyn SpillAppendWriter>)> {
        let promise = axon_spill_create_file(self.scope_id as f64).map_err(js_error)?;
        let (file_id, writer_id) = send_js_future(promise, map_created_file).await?;
        Ok((
            SpillFileRef::new(self.id(), file_id),
            Box::new(OpfsSpillWriter { writer_id }),
        ))
    }

    async fn open_reader(
        &self,
        file: SpillFileRef,
    ) -> SpillStorageResult<Box<dyn SpillSequentialReader>> {
        require_scope(self.id(), file)?;
        let promise = axon_spill_open_reader(self.scope_id as f64, file.file_id() as f64)
            .map_err(js_error)?;
        let reader_id = send_js_future(promise, map_opaque_id).await?;
        Ok(Box::new(OpfsSpillReader {
            reader_id,
            closed: false,
        }))
    }

    async fn delete_file(&self, file: SpillFileRef) -> SpillStorageResult<()> {
        require_scope(self.id(), file)?;
        let promise = axon_spill_delete_file(self.scope_id as f64, file.file_id() as f64)
            .map_err(js_error)?;
        send_js_future(promise, map_unit).await
    }

    async fn delete_scope(&self) -> SpillStorageResult<()> {
        let promise = axon_spill_delete_scope(self.scope_id as f64).map_err(js_error)?;
        send_js_future(promise, map_unit).await
    }

    fn record_merge_pass(&self) {
        let _ = axon_spill_record_merge_pass();
    }

    fn metrics(&self) -> SpillStorageMetrics {
        axon_spill_metrics()
            .ok()
            .map(|value| SpillStorageMetrics {
                current_bytes: numeric_property(&value, "activeBytes"),
                peak_bytes: numeric_property(&value, "peakActiveBytes"),
                files_created: numeric_property(&value, "filesCreated"),
                active_files: numeric_property(&value, "activeFiles"),
                merge_passes: numeric_property(&value, "mergePasses"),
            })
            .unwrap_or_default()
    }
}

#[derive(Debug)]
struct OpfsSpillWriter {
    writer_id: u64,
}

#[async_trait::async_trait]
impl SpillAppendWriter for OpfsSpillWriter {
    async fn append(&mut self, bytes: &[u8]) -> SpillStorageResult<()> {
        // The imported bridge consumes this view synchronously and never retains it.
        // No Rust allocation or await occurs until the view has been dropped.
        let bytes = unsafe { Uint8Array::view(bytes) };
        let promise = axon_spill_append(self.writer_id as f64, &bytes).map_err(js_error)?;
        drop(bytes);
        send_js_future(promise, map_unit).await
    }

    async fn finalize(self: Box<Self>) -> SpillStorageResult<()> {
        let promise = axon_spill_finalize_writer(self.writer_id as f64).map_err(js_error)?;
        send_js_future(promise, map_unit).await
    }
}

#[derive(Debug)]
struct OpfsSpillReader {
    reader_id: u64,
    closed: bool,
}

#[async_trait::async_trait]
impl SpillSequentialReader for OpfsSpillReader {
    async fn read_next(&mut self, max_bytes: usize) -> SpillStorageResult<Option<Vec<u8>>> {
        let mut bytes = vec![0; max_bytes];
        // `readNext` fills this view synchronously and does not retain it.
        let target = unsafe { Uint8Array::view(&bytes) };
        let value = axon_spill_read_next(self.reader_id as f64, &target).map_err(js_error)?;
        drop(target);
        if value.is_undefined() || value.is_null() {
            axon_spill_close_reader(self.reader_id as f64).map_err(js_error)?;
            self.closed = true;
            return Ok(None);
        }
        let read = value.as_f64().ok_or_else(io_failure)?;
        if !read.is_safe_integer() || read <= 0.0 || read as usize > max_bytes {
            return Err(io_failure());
        }
        bytes.truncate(read as usize);
        Ok(Some(bytes))
    }
}

impl Drop for OpfsSpillReader {
    fn drop(&mut self) {
        if !self.closed {
            let _ = axon_spill_close_reader(self.reader_id as f64);
        }
    }
}

fn require_scope(expected: SpillScopeId, file: SpillFileRef) -> SpillStorageResult<()> {
    if file.scope_id() != expected {
        return Err(SpillStorageError::new(
            SpillStorageErrorReason::Unavailable,
            "browser spill file is unavailable",
        ));
    }
    Ok(())
}

fn map_opaque_id(value: JsValue) -> SpillStorageResult<u64> {
    let value = value.as_f64().ok_or_else(io_failure)?;
    if !value.is_safe_integer() || value <= 0.0 {
        return Err(io_failure());
    }
    Ok(value as u64)
}

fn map_created_file(value: JsValue) -> SpillStorageResult<(u64, u64)> {
    let file_id =
        map_opaque_id(Reflect::get(&value, &JsValue::from_str("fileId")).map_err(js_error)?)?;
    let writer_id =
        map_opaque_id(Reflect::get(&value, &JsValue::from_str("writerId")).map_err(js_error)?)?;
    Ok((file_id, writer_id))
}

fn map_unit(_value: JsValue) -> SpillStorageResult<()> {
    Ok(())
}

fn numeric_property(value: &JsValue, property: &str) -> u64 {
    Reflect::get(value, &JsValue::from_str(property))
        .ok()
        .and_then(|value| value.as_f64())
        .filter(|value| value.is_finite() && *value >= 0.0)
        .map(|value| value as u64)
        .unwrap_or(0)
}

fn js_error(value: JsValue) -> SpillStorageError {
    let reason = Reflect::get(&value, &JsValue::from_str("reason"))
        .ok()
        .and_then(|value| value.as_string());
    match reason.as_deref() {
        Some("quota_exceeded") => SpillStorageError::new(
            SpillStorageErrorReason::QuotaExceeded,
            "browser spill storage quota exceeded",
        ),
        Some("unavailable") => SpillStorageError::new(
            SpillStorageErrorReason::Unavailable,
            "browser spill storage is unavailable",
        ),
        _ => io_failure(),
    }
}

fn io_failure() -> SpillStorageError {
    SpillStorageError::new(
        SpillStorageErrorReason::IoFailure,
        "browser spill storage I/O failed",
    )
}

fn send_js_future<T: Send + 'static>(
    promise: Promise,
    map: fn(JsValue) -> SpillStorageResult<T>,
) -> SendJsFuture<T> {
    SendJsFuture {
        inner: JsFuture::from(promise),
        map,
        marker: PhantomData,
    }
}

struct SendJsFuture<T> {
    inner: JsFuture,
    map: fn(JsValue) -> SpillStorageResult<T>,
    marker: PhantomData<T>,
}

// `wasm32-unknown-unknown` has no shared-memory threads in Axon's build. The
// future is created, polled, and dropped by the same dedicated query worker.
unsafe impl<T: Send> Send for SendJsFuture<T> {}
impl<T> Unpin for SendJsFuture<T> {}

impl<T: Send> Future for SendJsFuture<T> {
    type Output = SpillStorageResult<T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(value)) => Poll::Ready((this.map)(value)),
            Poll::Ready(Err(error)) => Poll::Ready(Err(js_error(error))),
        }
    }
}

trait SafeInteger {
    fn is_safe_integer(self) -> bool;
}

impl SafeInteger for f64 {
    fn is_safe_integer(self) -> bool {
        self.is_finite() && self.fract() == 0.0 && self.abs() <= 9_007_199_254_740_991.0
    }
}
