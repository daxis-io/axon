use std::fmt;
use std::future::Future;
#[cfg(target_arch = "wasm32")]
use std::pin::Pin;
#[cfg(target_arch = "wasm32")]
use std::task::{Context, Poll};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::{self, BoxStream};
use futures::StreamExt;
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use query_contract::{ObjectGrantObject, QueryError, QueryErrorCode};

use crate::{BrokeredObjectStore, ObjectGrantBrokerClient, RangeCacheIdentity};

const STORE_NAME: &str = "axon-brokered-object-store";

/// A read-only `object_store` facade over an already-authorized Axon brokered store.
///
/// This adapter is intentionally limited to metadata and known-file bounded reads. It does not
/// discover Delta files, resolve access policy, list objects, perform full-object reads, or expose
/// a native fallback route. The epoch-valued `last_modified` field is a trait-required sentinel;
/// it is non-authoritative and must not be used for discovery or `DynamicScan` metadata.
pub struct BrokeredObjectStoreAdapter<C> {
    inner: BrokeredObjectStore<C>,
}

impl<C> BrokeredObjectStoreAdapter<C> {
    /// Wraps an object store whose grant and policy have already been authorized by Axon.
    pub fn from_authorized_store(inner: BrokeredObjectStore<C>) -> Self {
        Self { inner }
    }
}

impl<C> fmt::Debug for BrokeredObjectStoreAdapter<C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BrokeredObjectStoreAdapter")
            .finish_non_exhaustive()
    }
}

impl<C> fmt::Display for BrokeredObjectStoreAdapter<C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(STORE_NAME)
    }
}

#[async_trait]
impl<C> ObjectStore for BrokeredObjectStoreAdapter<C>
where
    C: ObjectGrantBrokerClient + Send + Sync + 'static,
{
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(unsupported_operation("put_opts"))
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(unsupported_operation("put_multipart_opts"))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if options.version.is_some() {
            return Err(unsupported_operation("versioned get_opts"));
        }
        if options.if_modified_since.is_some() || options.if_unmodified_since.is_some() {
            return Err(unsupported_operation(
                "last-modified get_opts preconditions",
            ));
        }
        if options.head && options.range.is_some() {
            return Err(unsupported_operation("ranged head get_opts"));
        }
        if !options.head && options.range.is_none() {
            return Err(unsupported_operation("full-object get_opts"));
        }

        let path = location.to_string();
        let granted = adapter_send_future(self.inner.head(path.clone()))
            .await
            .map_err(|error| map_broker_error(&path, error))?;
        let metadata = object_metadata(location, granted)?;
        options.check_preconditions(&metadata)?;

        if options.head {
            return Ok(GetResult {
                payload: GetResultPayload::Stream(stream::empty().boxed()),
                meta: metadata,
                range: 0..0,
                attributes: Attributes::default(),
            });
        }

        let requested_range = options
            .range
            .as_ref()
            .expect("non-head get_opts range was validated above");
        let range = requested_range.as_range(metadata.size).map_err(|source| {
            object_store::Error::Generic {
                store: STORE_NAME,
                source: Box::new(source),
            }
        })?;
        if range.is_empty() {
            return Err(terminal_adapter_error(
                "bounded object-store reads must request at least one byte",
            ));
        }
        let etag = metadata
            .e_tag
            .as_deref()
            .expect("object metadata construction requires a strong ETag");
        let bytes = adapter_send_future(self.inner.get_range_with_identity(
            &path,
            range.start,
            range.end,
            metadata.size,
            etag,
        ))
        .await
        .map_err(|error| map_broker_error(&path, error))?;

        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed()),
            meta: metadata,
            range,
            attributes: Attributes::default(),
        })
    }

    fn delete_stream(
        &self,
        _locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        stream::iter([Err(unsupported_operation("delete_stream"))]).boxed()
    }

    fn list(&self, _prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        stream::iter([Err(unsupported_operation("list"))]).boxed()
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        Err(unsupported_operation("list_with_delimiter"))
    }

    async fn copy_opts(
        &self,
        _from: &Path,
        _to: &Path,
        _options: CopyOptions,
    ) -> object_store::Result<()> {
        Err(unsupported_operation("copy_opts"))
    }
}

fn object_metadata(
    requested_location: &Path,
    granted: ObjectGrantObject,
) -> object_store::Result<ObjectMeta> {
    if granted.path != requested_location.as_ref() {
        return Err(terminal_adapter_error(format!(
            "broker returned metadata for '{}' while '{}' was requested",
            granted.path, requested_location
        )));
    }
    let etag = granted.etag.ok_or_else(|| {
        terminal_adapter_error("broker metadata did not include the required strong quoted ETag")
    })?;
    if RangeCacheIdentity::strong(&granted.path, &etag, granted.size_bytes).is_none() {
        return Err(terminal_adapter_error(
            "broker metadata did not include a strong quoted ETag",
        ));
    }

    Ok(ObjectMeta {
        location: requested_location.clone(),
        // The broker contract has no authoritative modification time. This epoch is only the
        // sentinel required by object_store and is unsuitable for discovery or DynamicScan.
        last_modified: DateTime::<Utc>::UNIX_EPOCH,
        size: granted.size_bytes,
        e_tag: Some(etag),
        version: None,
    })
}

#[derive(Debug)]
struct BrokeredAdapterError(QueryError);

impl fmt::Display for BrokeredAdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0.message)
    }
}

impl std::error::Error for BrokeredAdapterError {}

fn map_broker_error(path: &str, error: QueryError) -> object_store::Error {
    let code = error.code;
    let source: Box<dyn std::error::Error + Send + Sync> = Box::new(BrokeredAdapterError(error));
    match code {
        QueryErrorCode::ObjectNotFound => object_store::Error::NotFound {
            path: path.to_string(),
            source,
        },
        QueryErrorCode::AccessDenied | QueryErrorCode::SecurityPolicyViolation => {
            object_store::Error::PermissionDenied {
                path: path.to_string(),
                source,
            }
        }
        _ => object_store::Error::Generic {
            store: STORE_NAME,
            source,
        },
    }
}

#[derive(Debug)]
struct AdapterOperationError(String);

impl fmt::Display for AdapterOperationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for AdapterOperationError {}

fn unsupported_operation(operation: &'static str) -> object_store::Error {
    object_store::Error::NotSupported {
        source: Box::new(AdapterOperationError(format!(
            "{STORE_NAME} rejects {operation} before broker or network I/O"
        ))),
    }
}

fn terminal_adapter_error(message: impl Into<String>) -> object_store::Error {
    object_store::Error::Generic {
        store: STORE_NAME,
        source: Box::new(AdapterOperationError(message.into())),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn adapter_send_future<F>(future: F) -> F
where
    F: Future + Send,
{
    future
}

#[cfg(target_arch = "wasm32")]
fn adapter_send_future<F>(future: F) -> SameWorkerFuture<F>
where
    F: Future,
{
    SameWorkerFuture {
        inner: Box::pin(future),
    }
}

#[cfg(target_arch = "wasm32")]
struct SameWorkerFuture<F> {
    inner: Pin<Box<F>>,
}

// SAFETY: Feature-enabled wasm32 builds reject atomics above. Axon's runtime creates, polls, and
// drops these broker and Fetch futures on the same dedicated query worker, so the future never
// crosses an agent or thread boundary. This marker only bridges that invariant to object_store's
// cross-target Send future contract.
#[cfg(target_arch = "wasm32")]
unsafe impl<F> Send for SameWorkerFuture<F> {}

#[cfg(target_arch = "wasm32")]
impl<F> Unpin for SameWorkerFuture<F> {}

#[cfg(target_arch = "wasm32")]
impl<F> Future for SameWorkerFuture<F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().inner.as_mut().poll(context)
    }
}
