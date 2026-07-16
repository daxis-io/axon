//! HTTP range-read adapter for browser-safe object access over exact HTTP byte ranges.

use std::fmt::Write;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use query_contract::{
    validate_browser_object_url, BrokeredObjectAccess, BrowserObjectUrlPolicy, ExecutionTarget,
    FallbackReason, ObjectGrantBatchSignRequest, ObjectGrantBatchSignResponse,
    ObjectGrantHeadRequest, ObjectGrantListRequest, ObjectGrantListResponse, ObjectGrantObject,
    ObjectGrantRangeRequest, ObjectGrantSignedUrl, QueryError, QueryErrorCode,
};
use reqwest::header::{CONTENT_LENGTH, CONTENT_RANGE, ETAG, IF_RANGE, RANGE};
use reqwest::{StatusCode, Url};
use sha2::{Digest, Sha256};

#[cfg(target_arch = "wasm32")]
use js_sys::{Function, Object, Promise, Reflect, Uint8Array};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::{JsCast, JsValue};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::JsFuture;

pub const OWNER: &str = "Runtime / engine team";
pub const RESPONSIBILITY: &str = "Browser-safe object reads over HTTP range requests.";
const DEFAULT_MEMORY_PERSISTENT_CACHE_ENTRIES: usize = 128;
#[cfg(target_arch = "wasm32")]
const EXTENT_CACHE_ENTRY_MAGIC: &[u8] = b"AXON_EXTENT_CACHE_V1\0";
#[cfg(target_arch = "wasm32")]
const EXTENT_CACHE_INDEX_MAGIC: &[u8] = b"AXON_EXTENT_INDEX_V1\0";
#[cfg(target_arch = "wasm32")]
const JS_MAX_SAFE_INTEGER_U64: u64 = 9_007_199_254_740_991;

pub fn supported_target() -> ExecutionTarget {
    ExecutionTarget::BrowserWasm
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpByteRange {
    Full,
    Bounded { offset: u64, length: u64 },
    FromOffset { offset: u64 },
    Suffix { length: u64 },
}

impl HttpByteRange {
    fn header_value(self) -> Result<Option<String>, QueryError> {
        match self {
            Self::Full => Ok(None),
            Self::Bounded { offset, length } => {
                if length == 0 {
                    return Err(invalid_request(
                        "bounded byte ranges must request at least one byte",
                    ));
                }

                let end = offset
                    .checked_add(length - 1)
                    .ok_or_else(|| invalid_request("bounded byte range overflowed u64"))?;
                Ok(Some(format!("bytes={offset}-{end}")))
            }
            Self::FromOffset { offset } => Ok(Some(format!("bytes={offset}-"))),
            Self::Suffix { length } => {
                if length == 0 {
                    return Err(invalid_request(
                        "suffix byte ranges must request at least one byte",
                    ));
                }

                Ok(Some(format!("bytes=-{length}")))
            }
        }
    }

    fn expects_partial_response(self) -> bool {
        !matches!(self, Self::Full)
    }

    fn validate_content_range(
        self,
        content_range: ParsedContentRange,
        display_url: &str,
    ) -> Result<(), QueryError> {
        match self {
            Self::Full => Ok(()),
            Self::Bounded { offset, length } => {
                let expected_end = offset
                    .checked_add(length - 1)
                    .ok_or_else(|| protocol_error("bounded byte range overflowed u64"))?;
                if content_range.start != offset || content_range.end != expected_end {
                    return Err(protocol_error(format!(
                        "partial response from '{display_url}' returned bytes {}-{}, but the request asked for bytes {offset}-{expected_end}",
                        content_range.start, content_range.end
                    )));
                }
                Ok(())
            }
            Self::FromOffset { offset } => {
                let expected_end = content_range.total_size.checked_sub(1).ok_or_else(|| {
                    protocol_error("content-range total size must be at least one byte")
                })?;
                if content_range.start != offset || content_range.end != expected_end {
                    return Err(protocol_error(format!(
                        "partial response from '{display_url}' returned bytes {}-{}, but the request asked for bytes {offset}-",
                        content_range.start, content_range.end
                    )));
                }
                Ok(())
            }
            Self::Suffix { length } => {
                let expected_start = content_range.total_size.saturating_sub(length);
                let expected_end = content_range.total_size.checked_sub(1).ok_or_else(|| {
                    protocol_error("content-range total size must be at least one byte")
                })?;
                if content_range.start != expected_start || content_range.end != expected_end {
                    return Err(protocol_error(format!(
                        "partial response from '{display_url}' returned bytes {}-{}, but the request asked for the final {length} bytes",
                        content_range.start, content_range.end
                    )));
                }
                Ok(())
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpObjectMetadata {
    pub url: String,
    pub size_bytes: Option<u64>,
    pub etag: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpRangeReadResult {
    pub metadata: HttpObjectMetadata,
    pub bytes: Bytes,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HttpMetadataProbeRequirements {
    pub require_size: bool,
    pub require_etag: bool,
}

impl HttpMetadataProbeRequirements {
    pub fn is_satisfied_by(self, metadata: &HttpObjectMetadata) -> bool {
        (!self.require_size || metadata.size_bytes.is_some())
            && (!self.require_etag || metadata.etag.is_some())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HttpRangeValidation {
    IfRangeEtag(String),
}

impl HttpRangeValidation {
    pub fn if_range_etag(etag: String) -> Self {
        Self::IfRangeEtag(etag)
    }

    fn apply_request(
        self,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder, QueryError> {
        match self {
            Self::IfRangeEtag(etag) => {
                if etag.trim().is_empty() {
                    return Err(invalid_request("if-range validators must not be empty"));
                }
                Ok(request.header(IF_RANGE, etag))
            }
        }
    }

    fn validate_response_identity(
        self,
        response_etag: Option<&str>,
        display_url: &str,
    ) -> Result<(), QueryError> {
        match self {
            Self::IfRangeEtag(expected) => match response_etag {
                Some(actual) if actual == expected => Ok(()),
                Some(actual) => Err(protocol_error(format!(
                    "range validation for '{display_url}' expected ETag {expected}, but the response returned {actual}"
                ))),
                None => Err(protocol_error(format!(
                    "range validation for '{display_url}' required an exposed ETag header, but none was returned"
                ))),
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ByteExtent {
    pub offset: u64,
    pub length: u64,
}

impl ByteExtent {
    pub fn new(offset: u64, length: u64) -> Result<Self, QueryError> {
        if length == 0 {
            return Err(invalid_request(
                "byte extents must request at least one byte",
            ));
        }

        let _ = offset
            .checked_add(length)
            .ok_or_else(|| invalid_request("byte extent overflowed u64"))?;

        Ok(Self { offset, length })
    }

    fn end_exclusive(self) -> Result<u64, QueryError> {
        self.offset
            .checked_add(self.length)
            .ok_or_else(|| protocol_error("byte extent overflowed u64"))
    }

    fn merge(self, other: Self) -> Result<Self, QueryError> {
        let start = self.offset.min(other.offset);
        let end = self.end_exclusive()?.max(other.end_exclusive()?);
        Self::new(start, end - start)
    }

    pub fn contains(self, other: Self) -> bool {
        match (self.end_exclusive(), other.end_exclusive()) {
            (Ok(self_end), Ok(other_end)) => self.offset <= other.offset && self_end >= other_end,
            _ => false,
        }
    }

    fn touches_or_overlaps(self, other: Self) -> bool {
        match (self.end_exclusive(), other.end_exclusive()) {
            (Ok(self_end), Ok(other_end)) => self_end >= other.offset && other_end >= self.offset,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ExtentCacheKey {
    pub resource: String,
    pub identity: Option<String>,
}

impl ExtentCacheKey {
    pub fn new(resource: impl Into<String>, identity: Option<String>) -> Self {
        Self {
            resource: resource.into(),
            identity,
        }
    }

    pub fn from_identity(identity: &RangeCacheIdentity) -> Self {
        let size_bytes = identity.size_bytes.to_string();
        let token = format!(
            "axon:shared-range-cache:v1:{}:{}:{}:{}",
            identity.etag.len(),
            identity.etag,
            size_bytes.len(),
            size_bytes
        );
        Self::new(identity.resource.clone(), Some(token))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RangeCacheIdentity {
    resource: String,
    etag: String,
    size_bytes: u64,
}

impl RangeCacheIdentity {
    pub fn strong(
        resource: impl Into<String>,
        etag: impl Into<String>,
        size_bytes: u64,
    ) -> Option<Self> {
        let resource = resource.into();
        let etag = etag.into();
        if resource.trim().is_empty()
            || etag.len() <= 2
            || !etag.starts_with('"')
            || !etag.ends_with('"')
        {
            return None;
        }

        Some(Self {
            resource,
            etag,
            size_bytes,
        })
    }

    fn trusted_local(
        resource: impl Into<String>,
        identity: impl Into<String>,
        size_bytes: u64,
    ) -> Self {
        Self {
            resource: resource.into(),
            etag: identity.into(),
            size_bytes,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RangeCacheLookup {
    Hit { bytes: Bytes },
    Miss,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeCacheLookupObservation {
    pub lookup: RangeCacheLookup,
    pub bytes_reused: u64,
    pub validation_misses: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RangeCacheStoreOutcome {
    Stored,
    CacheError,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RangeCacheStoreObservation {
    pub outcome: RangeCacheStoreOutcome,
    pub bytes_stored: u64,
    pub validation_misses: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SharedRangeCacheMetrics {
    pub network_bytes_fetched: u64,
    pub cache_bytes_reused: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub validation_misses: u64,
    pub cache_errors: u64,
}

#[derive(Debug, Default)]
struct MemoryRangeCacheState {
    entries: Vec<ExtentCacheEntry>,
    retained_bytes: u64,
    reserved_bytes: u64,
    metrics: SharedRangeCacheMetrics,
}

#[derive(Debug, Default)]
pub struct MemoryRangeCache {
    max_entries: Option<usize>,
    max_retained_bytes: Option<u64>,
    state: Mutex<MemoryRangeCacheState>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MemoryRangeCacheCapacity {
    pub retained_bytes: u64,
    pub reserved_bytes: u64,
    pub max_retained_bytes: Option<u64>,
}

#[derive(Debug)]
pub struct MemoryRangeCacheReservation {
    cache: Arc<MemoryRangeCache>,
    bytes: u64,
    active: bool,
}

impl MemoryRangeCacheReservation {
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for MemoryRangeCacheReservation {
    fn drop(&mut self) {
        if self.active {
            self.cache.release_reservation(self.bytes);
            self.active = false;
        }
    }
}

impl MemoryRangeCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_entries(max_entries: usize) -> Self {
        Self {
            max_entries: Some(max_entries.max(1)),
            max_retained_bytes: None,
            state: Mutex::new(MemoryRangeCacheState::default()),
        }
    }

    pub fn with_limits(max_entries: usize, max_retained_bytes: u64) -> Self {
        Self {
            max_entries: Some(max_entries.max(1)),
            max_retained_bytes: Some(max_retained_bytes),
            state: Mutex::new(MemoryRangeCacheState::default()),
        }
    }

    pub fn try_reserve(self: &Arc<Self>, bytes: u64) -> Option<MemoryRangeCacheReservation> {
        if bytes == 0 || self.max_retained_bytes.is_none() {
            return None;
        }
        let (mut state, recovered_poison) = self.lock_state();
        if recovered_poison {
            state.metrics.cache_errors = state.metrics.cache_errors.saturating_add(1);
            return None;
        }
        let projected = state
            .retained_bytes
            .checked_add(state.reserved_bytes)?
            .checked_add(bytes)?;
        if projected > self.max_retained_bytes? {
            return None;
        }
        state.reserved_bytes = state.reserved_bytes.saturating_add(bytes);
        Some(MemoryRangeCacheReservation {
            cache: Arc::clone(self),
            bytes,
            active: true,
        })
    }

    pub fn capacity(&self) -> MemoryRangeCacheCapacity {
        let (mut state, recovered_poison) = self.lock_state();
        if recovered_poison {
            state.metrics.cache_errors = state.metrics.cache_errors.saturating_add(1);
        }
        MemoryRangeCacheCapacity {
            retained_bytes: state.retained_bytes,
            reserved_bytes: state.reserved_bytes,
            max_retained_bytes: self.max_retained_bytes,
        }
    }

    fn lock_state(&self) -> (std::sync::MutexGuard<'_, MemoryRangeCacheState>, bool) {
        match self.state.lock() {
            Ok(state) => (state, false),
            Err(poisoned) => {
                self.state.clear_poison();
                (poisoned.into_inner(), true)
            }
        }
    }

    pub fn load(
        &self,
        identity: &RangeCacheIdentity,
        requested_extent: ByteExtent,
    ) -> RangeCacheLookup {
        self.load_observed(identity, requested_extent).lookup
    }

    pub fn load_observed(
        &self,
        identity: &RangeCacheIdentity,
        requested_extent: ByteExtent,
    ) -> RangeCacheLookupObservation {
        self.load_internal(identity, requested_extent, true)
    }

    fn load_internal(
        &self,
        identity: &RangeCacheIdentity,
        requested_extent: ByteExtent,
        record_miss: bool,
    ) -> RangeCacheLookupObservation {
        let (mut state, recovered_poison) = self.lock_state();
        if recovered_poison {
            state.metrics.cache_errors = state.metrics.cache_errors.saturating_add(1);
            if record_miss {
                state.metrics.cache_misses = state.metrics.cache_misses.saturating_add(1);
            }
            return RangeCacheLookupObservation {
                lookup: RangeCacheLookup::Miss,
                bytes_reused: 0,
                validation_misses: 0,
            };
        }
        let key = ExtentCacheKey::from_identity(identity);
        let validation_misses = evict_stale_memory_entries(&mut state, &key);

        let Some(position) = state
            .entries
            .iter()
            .position(|entry| entry.can_satisfy(&key, requested_extent))
        else {
            if record_miss {
                state.metrics.cache_misses = state.metrics.cache_misses.saturating_add(1);
            }
            return RangeCacheLookupObservation {
                lookup: RangeCacheLookup::Miss,
                bytes_reused: 0,
                validation_misses,
            };
        };
        let entry = state.entries[position].clone();
        if self.max_entries.is_some() {
            let recently_used = state.entries.remove(position);
            state.entries.push(recently_used);
        }

        match entry.slice(requested_extent) {
            Ok(bytes) => {
                state.metrics.cache_hits = state.metrics.cache_hits.saturating_add(1);
                state.metrics.cache_bytes_reused = state
                    .metrics
                    .cache_bytes_reused
                    .saturating_add(requested_extent.length);
                RangeCacheLookupObservation {
                    lookup: RangeCacheLookup::Hit { bytes },
                    bytes_reused: requested_extent.length,
                    validation_misses,
                }
            }
            Err(_) => {
                state.metrics.cache_errors = state.metrics.cache_errors.saturating_add(1);
                if record_miss {
                    state.metrics.cache_misses = state.metrics.cache_misses.saturating_add(1);
                }
                RangeCacheLookupObservation {
                    lookup: RangeCacheLookup::Miss,
                    bytes_reused: 0,
                    validation_misses,
                }
            }
        }
    }

    pub fn store(
        &self,
        identity: &RangeCacheIdentity,
        extent: ByteExtent,
        bytes: Bytes,
    ) -> RangeCacheStoreOutcome {
        self.store_observed(identity, extent, bytes).outcome
    }

    pub fn store_observed(
        &self,
        identity: &RangeCacheIdentity,
        extent: ByteExtent,
        bytes: Bytes,
    ) -> RangeCacheStoreObservation {
        let response_bytes = bytes.len() as u64;
        match self.store_entry(identity, extent, bytes) {
            Ok((_, validation_misses)) => RangeCacheStoreObservation {
                outcome: RangeCacheStoreOutcome::Stored,
                bytes_stored: response_bytes,
                validation_misses,
            },
            Err(()) => RangeCacheStoreObservation {
                outcome: RangeCacheStoreOutcome::CacheError,
                bytes_stored: 0,
                validation_misses: 0,
            },
        }
    }

    pub fn store_reserved(
        &self,
        mut reservation: MemoryRangeCacheReservation,
        identity: &RangeCacheIdentity,
        extent: ByteExtent,
        bytes: Bytes,
    ) -> RangeCacheStoreObservation {
        let response_bytes = bytes.len() as u64;
        if !std::ptr::eq(self, Arc::as_ptr(&reservation.cache))
            || response_bytes > reservation.bytes
        {
            return RangeCacheStoreObservation {
                outcome: RangeCacheStoreOutcome::CacheError,
                bytes_stored: 0,
                validation_misses: 0,
            };
        }

        let (mut state, recovered_poison) = self.lock_state();
        if recovered_poison {
            state.metrics.cache_errors = state.metrics.cache_errors.saturating_add(1);
            return RangeCacheStoreObservation {
                outcome: RangeCacheStoreOutcome::CacheError,
                bytes_stored: 0,
                validation_misses: 0,
            };
        }
        state.reserved_bytes = state.reserved_bytes.saturating_sub(reservation.bytes);
        reservation.active = false;
        match self.store_entry_locked(&mut state, identity, extent, bytes) {
            Ok((_, validation_misses)) => RangeCacheStoreObservation {
                outcome: RangeCacheStoreOutcome::Stored,
                bytes_stored: response_bytes,
                validation_misses,
            },
            Err(()) => RangeCacheStoreObservation {
                outcome: RangeCacheStoreOutcome::CacheError,
                bytes_stored: 0,
                validation_misses: 0,
            },
        }
    }

    fn store_entry(
        &self,
        identity: &RangeCacheIdentity,
        extent: ByteExtent,
        bytes: Bytes,
    ) -> Result<(ExtentCacheEntry, u64), ()> {
        let (mut state, recovered_poison) = self.lock_state();
        if recovered_poison {
            state.metrics.cache_errors = state.metrics.cache_errors.saturating_add(1);
            return Err(());
        }
        self.store_entry_locked(&mut state, identity, extent, bytes)
    }

    fn store_entry_locked(
        &self,
        state: &mut MemoryRangeCacheState,
        identity: &RangeCacheIdentity,
        extent: ByteExtent,
        bytes: Bytes,
    ) -> Result<(ExtentCacheEntry, u64), ()> {
        let key = ExtentCacheKey::from_identity(identity);
        let validation_misses = evict_stale_memory_entries(state, &key);
        let Ok(mut new_entry) = ExtentCacheEntry::new(key, extent, bytes) else {
            state.metrics.cache_errors = state.metrics.cache_errors.saturating_add(1);
            return Err(());
        };
        let original_entries = state.entries.clone();
        let mut retained = Vec::with_capacity(state.entries.len() + 1);
        let entries = std::mem::take(&mut state.entries);

        for existing in entries {
            // Byte-capped caches keep physical request extents independently
            // evictable instead of repeatedly copying one growing merged buffer.
            if self.max_retained_bytes.is_none()
                && existing.key == new_entry.key
                && existing.extent.touches_or_overlaps(new_entry.extent)
            {
                match existing.merge(new_entry) {
                    Ok(merged) => new_entry = merged,
                    Err(_) => {
                        state.metrics.cache_errors = state.metrics.cache_errors.saturating_add(1);
                        state.entries = original_entries;
                        state.retained_bytes = retained_bytes(&state.entries);
                        return Err(());
                    }
                }
            } else {
                retained.push(existing);
            }
        }
        retained.push(new_entry.clone());
        if let Some(max_entries) = self.max_entries {
            while retained.len() > max_entries {
                retained.remove(0);
            }
        }
        if let Some(max_retained_bytes) = self.max_retained_bytes {
            let available_bytes = max_retained_bytes.saturating_sub(state.reserved_bytes);
            while retained_bytes(&retained) > available_bytes && retained.len() > 1 {
                retained.remove(0);
            }
            if retained_bytes(&retained) > available_bytes {
                state.metrics.cache_errors = state.metrics.cache_errors.saturating_add(1);
                state.entries = original_entries;
                state.retained_bytes = retained_bytes(&state.entries);
                return Err(());
            }
        }
        state.entries = retained;
        state.retained_bytes = retained_bytes(&state.entries);

        Ok((new_entry, validation_misses))
    }

    fn release_reservation(&self, bytes: u64) {
        let (mut state, recovered_poison) = self.lock_state();
        if recovered_poison {
            state.metrics.cache_errors = state.metrics.cache_errors.saturating_add(1);
        }
        state.reserved_bytes = state.reserved_bytes.saturating_sub(bytes);
    }

    pub fn metrics(&self) -> SharedRangeCacheMetrics {
        let (mut state, recovered_poison) = self.lock_state();
        if recovered_poison {
            state.metrics.cache_errors = state.metrics.cache_errors.saturating_add(1);
        }
        state.metrics
    }

    pub fn record_network_bytes_fetched(&self, bytes: u64) {
        let (mut state, recovered_poison) = self.lock_state();
        if recovered_poison {
            state.metrics.cache_errors = state.metrics.cache_errors.saturating_add(1);
        }
        state.metrics.network_bytes_fetched =
            state.metrics.network_bytes_fetched.saturating_add(bytes);
    }

    fn record_hit(&self, bytes: u64) {
        let (mut state, recovered_poison) = self.lock_state();
        if recovered_poison {
            state.metrics.cache_errors = state.metrics.cache_errors.saturating_add(1);
        }
        state.metrics.cache_hits = state.metrics.cache_hits.saturating_add(1);
        state.metrics.cache_bytes_reused = state.metrics.cache_bytes_reused.saturating_add(bytes);
    }

    fn record_miss(&self) {
        let (mut state, recovered_poison) = self.lock_state();
        if recovered_poison {
            state.metrics.cache_errors = state.metrics.cache_errors.saturating_add(1);
        }
        state.metrics.cache_misses = state.metrics.cache_misses.saturating_add(1);
    }

    fn record_cache_error(&self) {
        let (mut state, recovered_poison) = self.lock_state();
        if recovered_poison {
            state.metrics.cache_errors = state.metrics.cache_errors.saturating_add(1);
        }
        state.metrics.cache_errors = state.metrics.cache_errors.saturating_add(1);
    }

    fn entries_for_testing(&self) -> Vec<ExtentCacheEntry> {
        let (mut state, recovered_poison) = self.lock_state();
        if recovered_poison {
            state.metrics.cache_errors = state.metrics.cache_errors.saturating_add(1);
        }
        state.entries.clone()
    }
}

fn evict_stale_memory_entries(state: &mut MemoryRangeCacheState, key: &ExtentCacheKey) -> u64 {
    let stale_count = state
        .entries
        .iter()
        .filter(|entry| entry.key.resource == key.resource && entry.key != *key)
        .count() as u64;
    if stale_count == 0 {
        return 0;
    }

    state
        .entries
        .retain(|entry| entry.key.resource != key.resource || entry.key == *key);
    state.retained_bytes = retained_bytes(&state.entries);
    state.metrics.validation_misses = state.metrics.validation_misses.saturating_add(stale_count);
    stale_count
}

fn retained_bytes(entries: &[ExtentCacheEntry]) -> u64 {
    entries.iter().fold(0_u64, |total, entry| {
        total.saturating_add(entry.bytes.len() as u64)
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtentCacheEntry {
    pub key: ExtentCacheKey,
    pub extent: ByteExtent,
    pub bytes: Bytes,
}

impl ExtentCacheEntry {
    pub fn new(key: ExtentCacheKey, extent: ByteExtent, bytes: Bytes) -> Result<Self, QueryError> {
        if extent.length == 0 {
            return Err(invalid_request(
                "cached extents must request at least one byte",
            ));
        }
        let _ = extent
            .offset
            .checked_add(extent.length)
            .ok_or_else(|| invalid_request("cached extent overflowed u64"))?;

        let expected_len = usize::try_from(extent.length)
            .map_err(|_| invalid_request("byte extent length overflowed usize"))?;
        if bytes.len() != expected_len {
            return Err(invalid_request(format!(
                "cached extent length mismatch: extent declared {} bytes, but payload had {} bytes",
                extent.length,
                bytes.len()
            )));
        }

        Ok(Self { key, extent, bytes })
    }

    pub fn can_satisfy(&self, request_key: &ExtentCacheKey, request_extent: ByteExtent) -> bool {
        &self.key == request_key && self.extent.contains(request_extent)
    }

    pub fn slice(&self, request_extent: ByteExtent) -> Result<Bytes, QueryError> {
        if !self.extent.contains(request_extent) {
            return Err(invalid_request(format!(
                "cached extent {}..{} cannot satisfy requested extent {}..{}",
                self.extent.offset,
                self.extent.end_exclusive()?,
                request_extent.offset,
                request_extent.end_exclusive()?
            )));
        }

        let start = request_extent
            .offset
            .checked_sub(self.extent.offset)
            .ok_or_else(|| invalid_request("requested cached extent underflowed u64"))?;
        let end = start
            .checked_add(request_extent.length)
            .ok_or_else(|| invalid_request("requested cached extent overflowed u64"))?;
        let start = usize::try_from(start)
            .map_err(|_| invalid_request("requested cached extent start overflowed usize"))?;
        let end = usize::try_from(end)
            .map_err(|_| invalid_request("requested cached extent end overflowed usize"))?;

        Ok(self.bytes.slice(start..end))
    }

    fn merge(self, other: Self) -> Result<Self, QueryError> {
        if self.key != other.key {
            return Err(invalid_request(
                "cached extents with different cache keys cannot be merged",
            ));
        }
        if !self.extent.touches_or_overlaps(other.extent) {
            return Err(invalid_request(
                "cached extents must touch or overlap before they can be merged",
            ));
        }

        let merged_extent = self.extent.merge(other.extent)?;
        let merged_len = usize::try_from(merged_extent.length)
            .map_err(|_| invalid_request("merged cached extent length overflowed usize"))?;
        let mut merged_bytes = vec![0_u8; merged_len];
        copy_extent_bytes(&mut merged_bytes, merged_extent.offset, &self)?;
        copy_extent_bytes(&mut merged_bytes, merged_extent.offset, &other)?;

        Self::new(self.key, merged_extent, Bytes::from(merged_bytes))
    }
}

fn copy_extent_bytes(
    destination: &mut [u8],
    merged_offset: u64,
    entry: &ExtentCacheEntry,
) -> Result<(), QueryError> {
    let start = entry
        .extent
        .offset
        .checked_sub(merged_offset)
        .ok_or_else(|| invalid_request("merged cached extent underflowed u64"))?;
    let start = usize::try_from(start)
        .map_err(|_| invalid_request("merged cached extent start overflowed usize"))?;
    let end = start
        .checked_add(entry.bytes.len())
        .ok_or_else(|| invalid_request("merged cached extent end overflowed usize"))?;
    destination[start..end].copy_from_slice(entry.bytes.as_ref());
    Ok(())
}

pub fn coalesce_extents(mut extents: Vec<ByteExtent>) -> Result<Vec<ByteExtent>, QueryError> {
    if extents.is_empty() {
        return Ok(Vec::new());
    }

    extents.sort_by(|left, right| {
        left.offset
            .cmp(&right.offset)
            .then(left.length.cmp(&right.length))
    });

    let mut coalesced = Vec::with_capacity(extents.len());
    let mut current = extents[0];
    for extent in extents.into_iter().skip(1) {
        if current.touches_or_overlaps(extent) {
            current = current.merge(extent)?;
            continue;
        }
        coalesced.push(current);
        current = extent;
    }
    coalesced.push(current);

    Ok(coalesced)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrowserCacheMode {
    MemoryOnly,
    Persistent,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BrowserTransportMetrics {
    pub bytes_fetched: u64,
    pub bytes_reused: u64,
    pub validation_misses: u64,
    pub persistent_cache_errors: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserObjectMetadata {
    pub resource: String,
    pub size_bytes: u64,
    pub identity: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserObjectReadResult {
    pub metadata: BrowserObjectMetadata,
    pub extent: ByteExtent,
    pub bytes: Bytes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserLocalObject {
    resource: String,
    source: BrowserLocalObjectSource,
    identity: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum BrowserLocalObjectSource {
    Bytes(Bytes),
    #[cfg(target_arch = "wasm32")]
    Blob(web_sys::Blob),
}

impl BrowserLocalObject {
    pub fn from_bytes(resource: impl Into<String>, bytes: impl Into<Bytes>) -> Self {
        let resource = resource.into();
        let bytes = bytes.into();
        let identity = derive_local_identity(&bytes);

        Self {
            resource,
            source: BrowserLocalObjectSource::Bytes(bytes),
            identity,
        }
    }

    pub fn from_bytes_with_identity(
        resource: impl Into<String>,
        bytes: impl Into<Bytes>,
        identity: impl Into<String>,
    ) -> Self {
        Self {
            resource: resource.into(),
            source: BrowserLocalObjectSource::Bytes(bytes.into()),
            identity: identity.into(),
        }
    }

    #[cfg(target_arch = "wasm32")]
    pub fn from_blob_with_identity(
        resource: impl Into<String>,
        blob: web_sys::Blob,
        identity: impl Into<String>,
    ) -> Self {
        Self {
            resource: resource.into(),
            source: BrowserLocalObjectSource::Blob(blob),
            identity: identity.into(),
        }
    }

    fn metadata(&self) -> Result<BrowserObjectMetadata, QueryError> {
        Ok(BrowserObjectMetadata {
            resource: self.resource.clone(),
            size_bytes: match &self.source {
                BrowserLocalObjectSource::Bytes(bytes) => bytes.len() as u64,
                #[cfg(target_arch = "wasm32")]
                BrowserLocalObjectSource::Blob(blob) => blob_size_to_u64(blob.size())?,
            },
            identity: self.identity.clone(),
        })
    }

    async fn read_extent(&self, extent: ByteExtent) -> Result<Bytes, QueryError> {
        match &self.source {
            BrowserLocalObjectSource::Bytes(bytes) => self.read_bytes_extent(bytes, extent),
            #[cfg(target_arch = "wasm32")]
            BrowserLocalObjectSource::Blob(blob) => {
                read_blob_extent(blob, &self.resource, extent).await
            }
        }
    }

    fn read_bytes_extent(&self, bytes: &Bytes, extent: ByteExtent) -> Result<Bytes, QueryError> {
        let object_len = bytes.len() as u64;
        let end = extent.end_exclusive()?;
        if end > object_len {
            return Err(protocol_error(format!(
                "browser-local object '{}' only exposes {} bytes, but extent {}..{} was requested",
                self.resource, object_len, extent.offset, end
            )));
        }

        let start = usize::try_from(extent.offset)
            .map_err(|_| invalid_request("browser-local extent start overflowed usize"))?;
        let end = usize::try_from(end)
            .map_err(|_| invalid_request("browser-local extent end overflowed usize"))?;

        Ok(bytes.slice(start..end))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrowserObject {
    Http { url: String },
    Local(BrowserLocalObject),
}

impl BrowserObject {
    pub fn http(url: impl Into<String>) -> Self {
        Self::Http { url: url.into() }
    }

    pub fn local(object: BrowserLocalObject) -> Self {
        Self::Local(object)
    }
}

pub type BrokerFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, QueryError>> + 'a>>;

pub trait ObjectGrantBrokerClient {
    fn list<'a>(
        &'a self,
        grant_id: &'a str,
        request: ObjectGrantListRequest,
    ) -> BrokerFuture<'a, ObjectGrantListResponse>;

    fn head<'a>(
        &'a self,
        grant_id: &'a str,
        request: ObjectGrantHeadRequest,
    ) -> BrokerFuture<'a, ObjectGrantObject>;

    fn batch_sign<'a>(
        &'a self,
        grant_id: &'a str,
        request: ObjectGrantBatchSignRequest,
    ) -> BrokerFuture<'a, ObjectGrantBatchSignResponse>;

    fn proxy_range<'a>(
        &'a self,
        grant_id: &'a str,
        request: ObjectGrantRangeRequest,
    ) -> BrokerFuture<'a, Bytes>;
}

pub struct BrokeredObjectStore<C> {
    grant_id: String,
    access: BrokeredObjectAccess,
    client: C,
    http: HttpRangeReader,
    request_timeout: Option<Duration>,
}

impl<C> BrokeredObjectStore<C>
where
    C: ObjectGrantBrokerClient,
{
    pub fn new(grant_id: impl Into<String>, access: BrokeredObjectAccess, client: C) -> Self {
        Self::with_http_reader(grant_id, access, client, HttpRangeReader::new())
    }

    pub fn with_http_reader(
        grant_id: impl Into<String>,
        access: BrokeredObjectAccess,
        client: C,
        http: HttpRangeReader,
    ) -> Self {
        Self {
            grant_id: grant_id.into(),
            access,
            client,
            http,
            request_timeout: None,
        }
    }

    pub fn with_request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = Some(request_timeout);
        self
    }

    pub async fn list(
        &self,
        prefix: impl Into<String>,
    ) -> Result<Vec<ObjectGrantObject>, QueryError> {
        self.require_capability(
            self.access.list,
            "brokered object grant does not advertise list capability",
            FallbackReason::SecurityPolicy,
        )?;
        let response = self
            .client
            .list(
                &self.grant_id,
                ObjectGrantListRequest {
                    prefix: prefix.into(),
                },
            )
            .await?;
        Ok(response.objects)
    }

    pub async fn head(&self, path: impl Into<String>) -> Result<ObjectGrantObject, QueryError> {
        self.require_capability(
            self.access.head,
            "brokered object grant does not advertise head capability",
            FallbackReason::SecurityPolicy,
        )?;
        self.client
            .head(&self.grant_id, ObjectGrantHeadRequest { path: path.into() })
            .await
    }

    pub async fn get(&self, path: impl Into<String>) -> Result<Bytes, QueryError> {
        self.require_capability(
            self.access.get,
            "brokered object grant does not advertise full-object read capability",
            FallbackReason::RangeReadUnavailable,
        )?;
        let path = path.into();
        if self.access.batch_sign {
            return self.read_signed_url(&path, HttpByteRange::Full).await;
        }

        self.require_capability(
            self.access.proxy_range,
            "brokered object grant does not advertise proxy range capability",
            FallbackReason::RangeReadUnavailable,
        )?;
        let metadata = self.head(path.clone()).await?;
        if metadata.size_bytes == 0 {
            return Ok(Bytes::new());
        }
        let bytes = self.proxy_range(&path, 0, metadata.size_bytes).await?;
        validate_proxy_range_length(bytes, metadata.size_bytes)
    }

    pub async fn get_range(
        &self,
        path: impl Into<String>,
        start: u64,
        end: u64,
    ) -> Result<Bytes, QueryError> {
        self.require_capability(
            self.access.range_get,
            "brokered object grant does not advertise range read capability",
            FallbackReason::RangeReadUnavailable,
        )?;
        if end <= start {
            return Err(invalid_request(
                "brokered range end must be greater than range start",
            ));
        }

        let path = path.into();
        if self.access.batch_sign {
            let length = end
                .checked_sub(start)
                .ok_or_else(|| invalid_request("brokered range underflowed u64"))?;
            return self
                .read_signed_url(
                    &path,
                    HttpByteRange::Bounded {
                        offset: start,
                        length,
                    },
                )
                .await;
        }

        self.require_capability(
            self.access.proxy_range,
            "brokered object grant does not advertise proxy range capability",
            FallbackReason::RangeReadUnavailable,
        )?;
        let bytes = self.proxy_range(&path, start, end).await?;
        let expected_len = end
            .checked_sub(start)
            .ok_or_else(|| invalid_request("brokered range underflowed u64"))?;
        validate_proxy_range_length(bytes, expected_len)
    }

    async fn read_signed_url(&self, path: &str, range: HttpByteRange) -> Result<Bytes, QueryError> {
        let signed_url = self.sign_one(path).await?;
        validate_signed_url_not_expired(&signed_url)?;
        let parsed = validate_browser_object_url(
            &signed_url.url,
            supported_target(),
            BrowserObjectUrlPolicy::HttpsOrLoopbackHttpForHostTests,
            "brokered signed object URL",
        )?;
        let result = self
            .http
            .read_range_with_timeout(parsed.as_str(), range, self.request_timeout)
            .await?;
        Ok(result.bytes)
    }

    async fn sign_one(&self, path: &str) -> Result<ObjectGrantSignedUrl, QueryError> {
        let response = self
            .client
            .batch_sign(
                &self.grant_id,
                ObjectGrantBatchSignRequest {
                    paths: vec![path.to_string()],
                },
            )
            .await?;
        response
            .signed_urls
            .into_iter()
            .find(|signed| signed.path == path)
            .ok_or_else(|| {
                protocol_error(
                    "object grant broker did not return a signed URL for the requested path",
                )
            })
    }

    async fn proxy_range(&self, path: &str, start: u64, end: u64) -> Result<Bytes, QueryError> {
        self.client
            .proxy_range(
                &self.grant_id,
                ObjectGrantRangeRequest {
                    path: path.to_string(),
                    start,
                    end,
                },
            )
            .await
    }

    fn require_capability(
        &self,
        allowed: bool,
        message: &'static str,
        reason: FallbackReason,
    ) -> Result<(), QueryError> {
        if allowed {
            return Ok(());
        }

        Err(QueryError::new(
            QueryErrorCode::FallbackRequired,
            message,
            supported_target(),
        )
        .with_fallback_reason(reason))
    }
}

fn validate_signed_url_not_expired(signed_url: &ObjectGrantSignedUrl) -> Result<(), QueryError> {
    if signed_url.expires_at_epoch_ms <= now_epoch_ms()? {
        return Err(QueryError::new(
            QueryErrorCode::FallbackRequired,
            "brokered signed object URL expired before browser read",
            supported_target(),
        )
        .with_fallback_reason(FallbackReason::SignedUrlExpired));
    }

    Ok(())
}

fn now_epoch_ms() -> Result<u64, QueryError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| cache_error("system clock is before Unix epoch"))?
        .as_millis();
    u64::try_from(millis).map_err(|_| cache_error("system clock epoch milliseconds overflowed u64"))
}

fn validate_proxy_range_length(bytes: Bytes, expected_len: u64) -> Result<Bytes, QueryError> {
    if bytes.len() as u64 != expected_len {
        return Err(protocol_error(format!(
            "brokered proxy range returned {} bytes, but {expected_len} were requested",
            bytes.len()
        )));
    }

    Ok(bytes)
}

pub type PersistentCacheFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, QueryError>> + 'a>>;

pub trait PersistentExtentCache {
    fn load<'a>(
        &'a self,
        key: &'a ExtentCacheKey,
        requested_extent: ByteExtent,
    ) -> PersistentCacheFuture<'a, Option<ExtentCacheEntry>>;

    fn store<'a>(&'a self, entry: &'a ExtentCacheEntry) -> PersistentCacheFuture<'a, ()>;
}

#[derive(Debug)]
pub struct MemoryPersistentExtentCache {
    max_entries: usize,
    entries: Mutex<Vec<ExtentCacheEntry>>,
}

impl Default for MemoryPersistentExtentCache {
    fn default() -> Self {
        Self::with_max_entries(DEFAULT_MEMORY_PERSISTENT_CACHE_ENTRIES)
    }
}

impl MemoryPersistentExtentCache {
    pub fn with_max_entries(max_entries: usize) -> Self {
        Self {
            max_entries: max_entries.max(1),
            entries: Mutex::new(Vec::new()),
        }
    }

    pub fn load_extent(
        &self,
        key: &ExtentCacheKey,
        requested_extent: ByteExtent,
    ) -> Result<Option<ExtentCacheEntry>, QueryError> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| cache_error("persistent memory cache lock was poisoned"))?;
        let Some(position) = entries
            .iter()
            .position(|entry| entry.can_satisfy(key, requested_extent))
        else {
            return Ok(None);
        };
        let entry = entries.remove(position);
        let loaded = entry.clone();
        entries.push(entry);
        Ok(Some(loaded))
    }

    pub fn store_extent(&self, entry: &ExtentCacheEntry) -> Result<(), QueryError> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| cache_error("persistent memory cache lock was poisoned"))?;
        let mut new_entry = entry.clone();
        let mut retained = Vec::with_capacity(entries.len() + 1);

        for existing in entries.drain(..) {
            if existing.key == new_entry.key
                && existing.extent.touches_or_overlaps(new_entry.extent)
            {
                new_entry = existing.merge(new_entry)?;
            } else {
                retained.push(existing);
            }
        }

        retained.push(new_entry);
        while retained.len() > self.max_entries {
            retained.remove(0);
        }
        *entries = retained;

        Ok(())
    }
}

impl PersistentExtentCache for MemoryPersistentExtentCache {
    fn load<'a>(
        &'a self,
        key: &'a ExtentCacheKey,
        requested_extent: ByteExtent,
    ) -> PersistentCacheFuture<'a, Option<ExtentCacheEntry>> {
        Box::pin(async move { self.load_extent(key, requested_extent) })
    }

    fn store<'a>(&'a self, entry: &'a ExtentCacheEntry) -> PersistentCacheFuture<'a, ()> {
        Box::pin(async move { self.store_extent(entry) })
    }
}

pub struct SharedRangeCache {
    memory: Arc<MemoryRangeCache>,
    persistent: Option<Arc<dyn PersistentExtentCache>>,
}

impl Default for SharedRangeCache {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedRangeCache {
    pub fn new() -> Self {
        Self {
            memory: Arc::new(MemoryRangeCache::new()),
            persistent: None,
        }
    }

    pub fn with_persistent_cache(persistent: Arc<dyn PersistentExtentCache>) -> Self {
        Self {
            memory: Arc::new(MemoryRangeCache::new()),
            persistent: Some(persistent),
        }
    }

    pub async fn load(
        &self,
        identity: &RangeCacheIdentity,
        requested_extent: ByteExtent,
    ) -> RangeCacheLookup {
        if let hit @ RangeCacheLookup::Hit { .. } = self
            .memory
            .load_internal(identity, requested_extent, false)
            .lookup
        {
            return hit;
        }

        if let Some(persistent) = self.persistent.as_ref().map(Arc::clone) {
            let key = ExtentCacheKey::from_identity(identity);
            match persistent.load(&key, requested_extent).await {
                Ok(Some(entry)) => {
                    match ExtentCacheEntry::new(entry.key, entry.extent, entry.bytes) {
                        Ok(entry) if entry.key == key => match entry.slice(requested_extent) {
                            Ok(bytes) => {
                                if self
                                    .memory
                                    .store_entry(identity, entry.extent, entry.bytes)
                                    .is_ok()
                                {
                                    self.memory.record_hit(requested_extent.length);
                                    return RangeCacheLookup::Hit { bytes };
                                }
                            }
                            Err(_) => self.memory.record_cache_error(),
                        },
                        Ok(_) | Err(_) => self.memory.record_cache_error(),
                    }
                }
                Err(_) => self.memory.record_cache_error(),
                Ok(None) => {}
            }
        }

        self.memory.record_miss();
        RangeCacheLookup::Miss
    }

    pub async fn store(
        &self,
        identity: &RangeCacheIdentity,
        extent: ByteExtent,
        bytes: Bytes,
    ) -> RangeCacheStoreOutcome {
        let Ok((entry, _)) = self.memory.store_entry(identity, extent, bytes) else {
            return RangeCacheStoreOutcome::CacheError;
        };

        if let Some(persistent) = self.persistent.as_ref().map(Arc::clone) {
            if persistent.store(&entry).await.is_err() {
                self.memory.record_cache_error();
                return RangeCacheStoreOutcome::CacheError;
            }
        }

        RangeCacheStoreOutcome::Stored
    }

    pub fn metrics(&self) -> SharedRangeCacheMetrics {
        self.memory.metrics()
    }

    pub fn record_network_bytes_fetched(&self, bytes: u64) {
        self.memory.record_network_bytes_fetched(bytes);
    }

    fn has_persistent_cache(&self) -> bool {
        self.persistent.is_some()
    }

    fn entries_for_testing(&self) -> Vec<ExtentCacheEntry> {
        self.memory.entries_for_testing()
    }
}

pub struct BrowserObjectRangeReader {
    http: HttpRangeReader,
    range_cache: SharedRangeCache,
}

impl Default for BrowserObjectRangeReader {
    fn default() -> Self {
        Self::new()
    }
}

impl BrowserObjectRangeReader {
    pub fn new() -> Self {
        Self::with_http_reader(HttpRangeReader::new())
    }

    pub fn with_http_reader(http: HttpRangeReader) -> Self {
        Self {
            http,
            range_cache: SharedRangeCache::new(),
        }
    }

    pub fn with_persistent_cache(persistent_cache: Arc<dyn PersistentExtentCache>) -> Self {
        Self::with_http_reader_and_persistent_cache(HttpRangeReader::new(), persistent_cache)
    }

    pub fn with_http_reader_and_persistent_cache(
        http: HttpRangeReader,
        persistent_cache: Arc<dyn PersistentExtentCache>,
    ) -> Self {
        Self {
            http,
            range_cache: SharedRangeCache::with_persistent_cache(persistent_cache),
        }
    }

    pub fn cache_mode(&self) -> BrowserCacheMode {
        if self.range_cache.has_persistent_cache() {
            BrowserCacheMode::Persistent
        } else {
            BrowserCacheMode::MemoryOnly
        }
    }

    pub fn metrics(&self) -> BrowserTransportMetrics {
        let metrics = self.range_cache.metrics();
        BrowserTransportMetrics {
            bytes_fetched: metrics.network_bytes_fetched,
            bytes_reused: metrics.cache_bytes_reused,
            validation_misses: metrics.validation_misses,
            persistent_cache_errors: metrics.cache_errors,
        }
    }

    #[doc(hidden)]
    pub fn cached_extents_for_testing(&self) -> Vec<ExtentCacheEntry> {
        self.range_cache.entries_for_testing()
    }

    pub async fn read_extent(
        &mut self,
        object: &BrowserObject,
        extent: ByteExtent,
        known_metadata: Option<BrowserObjectMetadata>,
    ) -> Result<BrowserObjectReadResult, QueryError> {
        match object {
            BrowserObject::Http { url } => self.read_http_extent(url, extent, known_metadata).await,
            BrowserObject::Local(local) => self.read_local_extent(local, extent).await,
        }
    }

    async fn read_http_extent(
        &mut self,
        url: &str,
        extent: ByteExtent,
        known_metadata: Option<BrowserObjectMetadata>,
    ) -> Result<BrowserObjectReadResult, QueryError> {
        let (metadata, probe_bytes_fetched) = self
            .resolve_http_metadata_for_extent(url, known_metadata)
            .await?;
        let identity = RangeCacheIdentity::strong(
            metadata.resource.clone(),
            metadata.identity.clone(),
            metadata.size_bytes,
        );
        self.range_cache
            .record_network_bytes_fetched(probe_bytes_fetched);

        if let Some(identity) = identity.as_ref() {
            if let RangeCacheLookup::Hit { bytes } = self.range_cache.load(identity, extent).await {
                return Ok(BrowserObjectReadResult {
                    metadata,
                    extent,
                    bytes,
                });
            }
        }

        let fetched = self
            .http
            .read_range_with_validation(
                url,
                HttpByteRange::Bounded {
                    offset: extent.offset,
                    length: extent.length,
                },
                Some(HttpRangeValidation::if_range_etag(
                    metadata.identity.clone(),
                )),
                None,
            )
            .await?;
        let bytes = fetched.bytes;
        let fetched_metadata = metadata_from_http(fetched.metadata)?;
        self.range_cache.record_network_bytes_fetched(extent.length);
        if let Some(identity) = RangeCacheIdentity::strong(
            fetched_metadata.resource.clone(),
            fetched_metadata.identity.clone(),
            fetched_metadata.size_bytes,
        ) {
            let _ = self
                .range_cache
                .store(&identity, extent, bytes.clone())
                .await;
        }

        Ok(BrowserObjectReadResult {
            metadata: fetched_metadata,
            extent,
            bytes,
        })
    }

    async fn resolve_http_metadata_for_extent(
        &self,
        url: &str,
        known_metadata: Option<BrowserObjectMetadata>,
    ) -> Result<(BrowserObjectMetadata, u64), QueryError> {
        let requested_resource = canonical_http_resource(url)?;
        if let Some(mut known_metadata) = known_metadata {
            if canonical_http_resource(&known_metadata.resource)? == requested_resource {
                known_metadata.resource = requested_resource;
                return Ok((known_metadata, 0));
            }
        }

        let metadata = self
            .http
            .probe_metadata_with_timeout(
                url,
                HttpMetadataProbeRequirements {
                    require_size: true,
                    require_etag: true,
                },
                None,
            )
            .await?;
        let metadata = metadata_from_http(metadata)?;

        Ok((metadata.clone(), metadata_probe_body_len(&metadata)))
    }

    async fn read_local_extent(
        &mut self,
        object: &BrowserLocalObject,
        extent: ByteExtent,
    ) -> Result<BrowserObjectReadResult, QueryError> {
        let metadata = object.metadata()?;
        let identity = RangeCacheIdentity::trusted_local(
            metadata.resource.clone(),
            metadata.identity.clone(),
            metadata.size_bytes,
        );

        if let RangeCacheLookup::Hit { bytes } = self.range_cache.load(&identity, extent).await {
            return Ok(BrowserObjectReadResult {
                metadata,
                extent,
                bytes,
            });
        }

        let bytes = object.read_extent(extent).await?;
        self.range_cache.record_network_bytes_fetched(extent.length);
        let _ = self
            .range_cache
            .store(&identity, extent, bytes.clone())
            .await;

        Ok(BrowserObjectReadResult {
            metadata,
            extent,
            bytes,
        })
    }
}

fn metadata_from_http(metadata: HttpObjectMetadata) -> Result<BrowserObjectMetadata, QueryError> {
    Ok(BrowserObjectMetadata {
        resource: metadata.url,
        size_bytes: metadata.size_bytes.ok_or_else(|| {
            protocol_error("validated object reads require browser-visible object size metadata")
        })?,
        identity: metadata.etag.ok_or_else(|| {
            protocol_error("validated object reads require an exposed object identity header")
        })?,
    })
}

fn metadata_probe_body_len(metadata: &BrowserObjectMetadata) -> u64 {
    if metadata.size_bytes == 0 {
        0
    } else {
        1
    }
}

fn canonical_http_resource(url: &str) -> Result<String, QueryError> {
    parse_url(url).map(|url| redacted_url(&url))
}

fn derive_local_identity(bytes: &Bytes) -> String {
    let digest = Sha256::digest(bytes.as_ref());
    let mut identity = String::with_capacity("local:sha256:".len() + digest.len() * 2);
    identity.push_str("local:sha256:");
    for byte in digest {
        let _ = write!(&mut identity, "{byte:02x}");
    }

    identity
}

#[cfg(target_arch = "wasm32")]
fn extent_cache_file_name(key: &ExtentCacheKey, extent: ByteExtent) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"axon:extent-cache-file:v1");
    hash_string(&mut hasher, &key.resource);
    match &key.identity {
        Some(identity) => {
            hasher.update([1]);
            hash_string(&mut hasher, identity);
        }
        None => hasher.update([0]),
    }
    hasher.update(extent.offset.to_le_bytes());
    hasher.update(extent.length.to_le_bytes());

    let digest = hasher.finalize();
    let mut file_name = String::with_capacity(digest.len() * 2 + ".bin".len());
    for byte in digest {
        let _ = write!(&mut file_name, "{byte:02x}");
    }
    file_name.push_str(".bin");
    file_name
}

#[cfg(target_arch = "wasm32")]
fn extent_cache_index_file_name(key: &ExtentCacheKey) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"axon:extent-cache-index:v1");
    hash_string(&mut hasher, &key.resource);
    match &key.identity {
        Some(identity) => {
            hasher.update([1]);
            hash_string(&mut hasher, identity);
        }
        None => hasher.update([0]),
    }

    let digest = hasher.finalize();
    let mut file_name = String::with_capacity(digest.len() * 2 + ".idx".len());
    for byte in digest {
        let _ = write!(&mut file_name, "{byte:02x}");
    }
    file_name.push_str(".idx");
    file_name
}

#[cfg(target_arch = "wasm32")]
fn hash_string(hasher: &mut Sha256, value: &str) {
    hasher.update((value.len() as u64).to_le_bytes());
    hasher.update(value.as_bytes());
}

#[cfg(target_arch = "wasm32")]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ExtentCacheIndex {
    extents: Vec<ByteExtent>,
}

#[cfg(target_arch = "wasm32")]
impl ExtentCacheIndex {
    fn containing_extent(&self, requested_extent: ByteExtent) -> Option<ByteExtent> {
        self.extents
            .iter()
            .copied()
            .filter(|extent| extent.contains(requested_extent))
            .min_by_key(|extent| (extent.length, extent.offset))
    }
}

#[cfg(target_arch = "wasm32")]
fn serialize_extent_cache_index(index: &ExtentCacheIndex) -> Result<Vec<u8>, QueryError> {
    let count = u32::try_from(index.extents.len())
        .map_err(|_| invalid_request("cache index length overflowed u32"))?;
    let mut encoded =
        Vec::with_capacity(EXTENT_CACHE_INDEX_MAGIC.len() + 4 + index.extents.len() * 16);

    encoded.extend_from_slice(EXTENT_CACHE_INDEX_MAGIC);
    encoded.extend_from_slice(&count.to_le_bytes());
    for extent in &index.extents {
        encoded.extend_from_slice(&extent.offset.to_le_bytes());
        encoded.extend_from_slice(&extent.length.to_le_bytes());
    }

    Ok(encoded)
}

#[cfg(target_arch = "wasm32")]
fn deserialize_extent_cache_index(encoded: &[u8]) -> Result<ExtentCacheIndex, QueryError> {
    let mut cursor = 0_usize;
    read_magic(
        encoded,
        &mut cursor,
        EXTENT_CACHE_INDEX_MAGIC,
        "cache index",
    )?;
    let count = read_u32(encoded, &mut cursor)? as usize;
    let mut extents = Vec::with_capacity(count);
    for _ in 0..count {
        let offset = read_u64(encoded, &mut cursor)?;
        let length = read_u64(encoded, &mut cursor)?;
        extents.push(ByteExtent::new(offset, length)?);
    }
    if cursor != encoded.len() {
        return Err(cache_error("cache index contained trailing bytes"));
    }

    Ok(ExtentCacheIndex { extents })
}

#[cfg(target_arch = "wasm32")]
fn serialize_extent_cache_entry(entry: &ExtentCacheEntry) -> Result<Vec<u8>, QueryError> {
    let resource = entry.key.resource.as_bytes();
    let resource_len = u32::try_from(resource.len())
        .map_err(|_| invalid_request("cache resource length overflowed u32"))?;
    let identity = entry.key.identity.as_deref().map(str::as_bytes);
    let identity_len = match identity {
        Some(identity) => u32::try_from(identity.len())
            .map_err(|_| invalid_request("cache identity length overflowed u32"))?,
        None => u32::MAX,
    };
    let payload_len = u64::try_from(entry.bytes.len())
        .map_err(|_| invalid_request("cache payload length overflowed u64"))?;
    let mut encoded = Vec::with_capacity(
        EXTENT_CACHE_ENTRY_MAGIC.len()
            + 4
            + 4
            + 8
            + 8
            + 8
            + resource.len()
            + identity.map_or(0, <[u8]>::len)
            + entry.bytes.len(),
    );

    encoded.extend_from_slice(EXTENT_CACHE_ENTRY_MAGIC);
    encoded.extend_from_slice(&resource_len.to_le_bytes());
    encoded.extend_from_slice(&identity_len.to_le_bytes());
    encoded.extend_from_slice(&entry.extent.offset.to_le_bytes());
    encoded.extend_from_slice(&entry.extent.length.to_le_bytes());
    encoded.extend_from_slice(&payload_len.to_le_bytes());
    encoded.extend_from_slice(resource);
    if let Some(identity) = identity {
        encoded.extend_from_slice(identity);
    }
    encoded.extend_from_slice(entry.bytes.as_ref());

    Ok(encoded)
}

#[cfg(target_arch = "wasm32")]
fn deserialize_extent_cache_entry(encoded: &[u8]) -> Result<ExtentCacheEntry, QueryError> {
    let mut cursor = 0_usize;
    read_magic(
        encoded,
        &mut cursor,
        EXTENT_CACHE_ENTRY_MAGIC,
        "cache entry",
    )?;
    let resource_len = read_u32(encoded, &mut cursor)? as usize;
    let identity_len = read_u32(encoded, &mut cursor)?;
    let offset = read_u64(encoded, &mut cursor)?;
    let length = read_u64(encoded, &mut cursor)?;
    let payload_len = usize::try_from(read_u64(encoded, &mut cursor)?)
        .map_err(|_| cache_error("cache payload length overflowed usize"))?;
    let resource = read_string(encoded, &mut cursor, resource_len, "resource")?;
    let identity = if identity_len == u32::MAX {
        None
    } else {
        Some(read_string(
            encoded,
            &mut cursor,
            identity_len as usize,
            "identity",
        )?)
    };
    let payload = read_slice(encoded, &mut cursor, payload_len, "payload")?;
    if cursor != encoded.len() {
        return Err(cache_error("cache entry contained trailing bytes"));
    }

    ExtentCacheEntry::new(
        ExtentCacheKey::new(resource, identity),
        ByteExtent::new(offset, length)?,
        Bytes::copy_from_slice(payload),
    )
}

#[cfg(target_arch = "wasm32")]
fn read_magic(
    encoded: &[u8],
    cursor: &mut usize,
    expected: &[u8],
    field: &str,
) -> Result<(), QueryError> {
    let magic = read_slice(encoded, cursor, expected.len(), "magic")?;
    if magic != expected {
        return Err(cache_error(format!("{field} magic did not match")));
    }
    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn read_u32(encoded: &[u8], cursor: &mut usize) -> Result<u32, QueryError> {
    let bytes = read_slice(encoded, cursor, 4, "u32")?;
    Ok(u32::from_le_bytes(
        bytes
            .try_into()
            .expect("read_slice should return exactly four bytes"),
    ))
}

#[cfg(target_arch = "wasm32")]
fn read_u64(encoded: &[u8], cursor: &mut usize) -> Result<u64, QueryError> {
    let bytes = read_slice(encoded, cursor, 8, "u64")?;
    Ok(u64::from_le_bytes(
        bytes
            .try_into()
            .expect("read_slice should return exactly eight bytes"),
    ))
}

#[cfg(target_arch = "wasm32")]
fn read_string(
    encoded: &[u8],
    cursor: &mut usize,
    length: usize,
    field: &str,
) -> Result<String, QueryError> {
    let bytes = read_slice(encoded, cursor, length, field)?;
    String::from_utf8(bytes.to_vec())
        .map_err(|error| cache_error(format!("cache entry {field} was not valid UTF-8: {error}")))
}

#[cfg(target_arch = "wasm32")]
fn read_slice<'a>(
    encoded: &'a [u8],
    cursor: &mut usize,
    length: usize,
    field: &str,
) -> Result<&'a [u8], QueryError> {
    let end = cursor
        .checked_add(length)
        .ok_or_else(|| cache_error(format!("cache entry {field} length overflowed usize")))?;
    let slice = encoded
        .get(*cursor..end)
        .ok_or_else(|| cache_error(format!("cache entry ended while reading {field}")))?;
    *cursor = end;
    Ok(slice)
}

#[cfg(target_arch = "wasm32")]
#[derive(Clone)]
pub struct OpfsPersistentExtentCache {
    directory: JsValue,
    max_entries: usize,
}

#[cfg(target_arch = "wasm32")]
impl OpfsPersistentExtentCache {
    pub async fn open_default() -> Result<Self, QueryError> {
        Self::open("axon-extent-cache").await
    }

    pub async fn open(namespace: impl Into<String>) -> Result<Self, QueryError> {
        Self::open_with_max_entries(namespace, DEFAULT_MEMORY_PERSISTENT_CACHE_ENTRIES).await
    }

    pub async fn open_with_max_entries(
        namespace: impl Into<String>,
        max_entries: usize,
    ) -> Result<Self, QueryError> {
        let root = opfs_root_directory().await?;
        Self::open_in_directory_with_max_entries(root, namespace, max_entries).await
    }

    pub async fn open_in_directory(
        directory: JsValue,
        namespace: impl Into<String>,
    ) -> Result<Self, QueryError> {
        Self::open_in_directory_with_max_entries(
            directory,
            namespace,
            DEFAULT_MEMORY_PERSISTENT_CACHE_ENTRIES,
        )
        .await
    }

    pub async fn open_in_directory_with_max_entries(
        directory: JsValue,
        namespace: impl Into<String>,
        max_entries: usize,
    ) -> Result<Self, QueryError> {
        let namespace = validate_opfs_name(namespace.into(), "namespace")?;
        let directory = opfs_directory_handle(&directory, &namespace, true).await?;
        Ok(Self::from_directory_handle_with_max_entries(
            directory,
            max_entries,
        ))
    }

    pub fn from_directory_handle(directory: JsValue) -> Self {
        Self::from_directory_handle_with_max_entries(
            directory,
            DEFAULT_MEMORY_PERSISTENT_CACHE_ENTRIES,
        )
    }

    pub fn from_directory_handle_with_max_entries(directory: JsValue, max_entries: usize) -> Self {
        Self {
            directory,
            max_entries: max_entries.max(1),
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl PersistentExtentCache for OpfsPersistentExtentCache {
    fn load<'a>(
        &'a self,
        key: &'a ExtentCacheKey,
        requested_extent: ByteExtent,
    ) -> PersistentCacheFuture<'a, Option<ExtentCacheEntry>> {
        Box::pin(async move {
            let index = opfs_load_index(&self.directory, key).await?;
            let Some(stored_extent) = index.containing_extent(requested_extent) else {
                return Ok(None);
            };
            let Some(entry) = opfs_load_entry(&self.directory, key, stored_extent).await? else {
                return Ok(None);
            };

            if entry.can_satisfy(key, requested_extent) {
                Ok(Some(entry))
            } else {
                Ok(None)
            }
        })
    }

    fn store<'a>(&'a self, entry: &'a ExtentCacheEntry) -> PersistentCacheFuture<'a, ()> {
        Box::pin(async move {
            let mut index = opfs_load_index(&self.directory, &entry.key).await?;
            let mut new_entry = entry.clone();
            let mut obsolete_extents = Vec::new();
            let mut retained_extents = Vec::with_capacity(index.extents.len() + 1);
            index.extents.sort_by(|left, right| {
                left.offset
                    .cmp(&right.offset)
                    .then(left.length.cmp(&right.length))
            });

            for extent in index.extents {
                if extent.touches_or_overlaps(new_entry.extent) {
                    if let Some(existing_entry) =
                        opfs_load_entry(&self.directory, &entry.key, extent).await?
                    {
                        new_entry = existing_entry.merge(new_entry)?;
                    }
                    obsolete_extents.push(extent);
                } else {
                    retained_extents.push(extent);
                }
            }

            retained_extents.push(new_entry.extent);
            while retained_extents.len() > self.max_entries {
                obsolete_extents.push(retained_extents.remove(0));
            }

            opfs_store_entry(&self.directory, &new_entry).await?;
            opfs_store_index(
                &self.directory,
                &new_entry.key,
                &ExtentCacheIndex {
                    extents: retained_extents,
                },
            )
            .await?;

            for extent in obsolete_extents {
                if extent != new_entry.extent {
                    let _ = opfs_remove_entry(
                        &self.directory,
                        &extent_cache_file_name(&new_entry.key, extent),
                    )
                    .await;
                }
            }

            Ok(())
        })
    }
}

#[cfg(target_arch = "wasm32")]
async fn opfs_load_index(
    directory: &JsValue,
    key: &ExtentCacheKey,
) -> Result<ExtentCacheIndex, QueryError> {
    let file_name = extent_cache_index_file_name(key);
    let Some(encoded) = opfs_read_optional_file(directory, &file_name).await? else {
        return Ok(ExtentCacheIndex::default());
    };
    deserialize_extent_cache_index(&encoded)
}

#[cfg(target_arch = "wasm32")]
async fn opfs_store_index(
    directory: &JsValue,
    key: &ExtentCacheKey,
    index: &ExtentCacheIndex,
) -> Result<(), QueryError> {
    let file_name = extent_cache_index_file_name(key);
    let encoded = serialize_extent_cache_index(index)?;
    opfs_write_file(directory, &file_name, &encoded).await
}

#[cfg(target_arch = "wasm32")]
async fn opfs_load_entry(
    directory: &JsValue,
    key: &ExtentCacheKey,
    stored_extent: ByteExtent,
) -> Result<Option<ExtentCacheEntry>, QueryError> {
    let file_name = extent_cache_file_name(key, stored_extent);
    let Some(encoded) = opfs_read_optional_file(directory, &file_name).await? else {
        return Ok(None);
    };
    let entry = deserialize_extent_cache_entry(&encoded)?;

    if entry.can_satisfy(key, stored_extent) {
        Ok(Some(entry))
    } else {
        Ok(None)
    }
}

#[cfg(target_arch = "wasm32")]
async fn opfs_store_entry(directory: &JsValue, entry: &ExtentCacheEntry) -> Result<(), QueryError> {
    let file_name = extent_cache_file_name(&entry.key, entry.extent);
    let encoded = serialize_extent_cache_entry(entry)?;
    opfs_write_file(directory, &file_name, &encoded).await
}

#[cfg(target_arch = "wasm32")]
async fn opfs_read_optional_file(
    directory: &JsValue,
    file_name: &str,
) -> Result<Option<Bytes>, QueryError> {
    let file_handle = match opfs_file_handle(directory, file_name, false).await {
        Ok(file_handle) => file_handle,
        Err(error) if js_error_name_is(&error, "NotFoundError") => return Ok(None),
        Err(error) => return Err(js_cache_error("OPFS cache getFileHandle failed", error)),
    };
    let file = opfs_get_file(&file_handle).await?;
    blob_to_bytes(&file).await.map(Some)
}

#[cfg(target_arch = "wasm32")]
async fn opfs_write_file(
    directory: &JsValue,
    file_name: &str,
    bytes: &[u8],
) -> Result<(), QueryError> {
    let file_handle = opfs_file_handle(directory, file_name, true)
        .await
        .map_err(|error| js_cache_error("OPFS cache getFileHandle failed", error))?;
    let writable = opfs_create_writable(&file_handle).await?;
    opfs_write_all(&writable, bytes).await?;
    opfs_close_writable(&writable).await
}

#[cfg(target_arch = "wasm32")]
async fn opfs_root_directory() -> Result<JsValue, QueryError> {
    let global = js_sys::global();
    let navigator = Reflect::get(&global, &JsValue::from_str("navigator"))
        .map_err(|error| js_cache_error("browser navigator lookup failed", error))?;
    let storage = Reflect::get(&navigator, &JsValue::from_str("storage"))
        .map_err(|error| js_cache_error("browser storage manager lookup failed", error))?;
    let get_directory = js_function(&storage, "getDirectory")?;
    let promise = get_directory
        .call0(&storage)
        .map_err(|error| js_cache_error("OPFS getDirectory failed", error))?;
    await_js_promise(promise)
        .await
        .map_err(|error| js_cache_error("OPFS getDirectory failed", error))
}

#[cfg(target_arch = "wasm32")]
async fn opfs_directory_handle(
    directory: &JsValue,
    name: &str,
    create: bool,
) -> Result<JsValue, QueryError> {
    let get_directory_handle = js_function(directory, "getDirectoryHandle")?;
    let options = create_options(create)?;
    let promise = get_directory_handle
        .call2(directory, &JsValue::from_str(name), &options)
        .map_err(|error| js_cache_error("OPFS getDirectoryHandle failed", error))?;
    await_js_promise(promise)
        .await
        .map_err(|error| js_cache_error("OPFS getDirectoryHandle failed", error))
}

#[cfg(target_arch = "wasm32")]
async fn opfs_file_handle(
    directory: &JsValue,
    name: &str,
    create: bool,
) -> Result<JsValue, JsValue> {
    let get_file_handle = js_function_value(directory, "getFileHandle")?;
    let options = create_options_value(create)?;
    let promise = get_file_handle.call2(directory, &JsValue::from_str(name), &options)?;
    await_js_promise(promise).await
}

#[cfg(target_arch = "wasm32")]
async fn opfs_get_file(file_handle: &JsValue) -> Result<web_sys::Blob, QueryError> {
    let get_file = js_function(file_handle, "getFile")?;
    let promise = get_file
        .call0(file_handle)
        .map_err(|error| js_cache_error("OPFS getFile failed", error))?;
    let file = await_js_promise(promise)
        .await
        .map_err(|error| js_cache_error("OPFS getFile failed", error))?;
    file.dyn_into::<web_sys::Blob>()
        .map_err(|error| js_cache_error("OPFS getFile returned a non-Blob value", error))
}

#[cfg(target_arch = "wasm32")]
async fn opfs_create_writable(file_handle: &JsValue) -> Result<JsValue, QueryError> {
    let create_writable = js_function(file_handle, "createWritable")?;
    let promise = create_writable
        .call0(file_handle)
        .map_err(|error| js_cache_error("OPFS createWritable failed", error))?;
    await_js_promise(promise)
        .await
        .map_err(|error| js_cache_error("OPFS createWritable failed", error))
}

#[cfg(target_arch = "wasm32")]
async fn opfs_write_all(writable: &JsValue, bytes: &[u8]) -> Result<(), QueryError> {
    let write = js_function(writable, "write")?;
    let array = Uint8Array::from(bytes);
    let promise = write
        .call1(writable, array.as_ref())
        .map_err(|error| js_cache_error("OPFS write failed", error))?;
    await_js_promise(promise)
        .await
        .map_err(|error| js_cache_error("OPFS write failed", error))?;
    Ok(())
}

#[cfg(target_arch = "wasm32")]
async fn opfs_close_writable(writable: &JsValue) -> Result<(), QueryError> {
    let close = js_function(writable, "close")?;
    let promise = close
        .call0(writable)
        .map_err(|error| js_cache_error("OPFS close failed", error))?;
    await_js_promise(promise)
        .await
        .map_err(|error| js_cache_error("OPFS close failed", error))?;
    Ok(())
}

#[cfg(target_arch = "wasm32")]
async fn opfs_remove_entry(directory: &JsValue, name: &str) -> Result<(), QueryError> {
    let remove_entry = js_function(directory, "removeEntry")?;
    let promise = remove_entry
        .call1(directory, &JsValue::from_str(name))
        .map_err(|error| js_cache_error("OPFS removeEntry failed", error))?;
    await_js_promise(promise)
        .await
        .map_err(|error| js_cache_error("OPFS removeEntry failed", error))?;
    Ok(())
}

#[cfg(target_arch = "wasm32")]
async fn read_blob_extent(
    blob: &web_sys::Blob,
    resource: &str,
    extent: ByteExtent,
) -> Result<Bytes, QueryError> {
    let object_len = blob_size_to_u64(blob.size())?;
    let end = extent.end_exclusive()?;
    if end > object_len {
        return Err(protocol_error(format!(
            "browser-local object '{resource}' only exposes {object_len} bytes, but extent {}..{} was requested",
            extent.offset, end
        )));
    }
    let start = u64_to_safe_js_number(extent.offset, "browser Blob extent start")?;
    let end = u64_to_safe_js_number(end, "browser Blob extent end")?;
    let slice = blob
        .slice_with_f64_and_f64(start, end)
        .map_err(|error| js_cache_error("browser Blob slice failed", error))?;

    blob_to_bytes(&slice).await
}

#[cfg(target_arch = "wasm32")]
async fn read_blob_url_range(
    url: &Url,
    range: HttpByteRange,
    validation: Option<HttpRangeValidation>,
) -> Result<HttpRangeReadResult, QueryError> {
    let display_url = redacted_url(url);
    if validation.is_some() {
        return Err(protocol_error(format!(
            "range validation for '{display_url}' requires an ETag, but browser-local blob URLs do not expose one"
        )));
    }

    let range_header = range.header_value()?;
    let response = fetch_blob_url_response(url, range_header.as_deref(), &display_url).await?;
    let status = js_response_status(&response, &display_url)?;
    if let Some(error) = map_status_error(status, &display_url) {
        return Err(error);
    }

    let content_range = if range.expects_partial_response() {
        if status == StatusCode::OK {
            return read_blob_url_full_response_range(&response, &display_url, range).await;
        }
        if status != StatusCode::PARTIAL_CONTENT {
            return Err(protocol_error(format!(
                "range request to browser-local blob URL '{display_url}' expected HTTP 206 Partial Content, got {status}"
            )));
        }
        let content_range = parse_js_content_range(&response, &display_url)?;
        range.validate_content_range(content_range, &display_url)?;
        Some(content_range)
    } else {
        if status != StatusCode::OK {
            return Err(protocol_error(format!(
                "full-object request to browser-local blob URL '{display_url}' expected HTTP 200 OK, got {status}"
            )));
        }
        None
    };

    let object_size = match content_range {
        Some(content_range) => Some(content_range.total_size),
        None => {
            parse_optional_js_response_header_u64(&response, CONTENT_LENGTH.as_str(), &display_url)?
        }
    };
    let bytes = response_array_buffer_bytes(
        &response,
        format!("browser fetch response body for '{display_url}' failed"),
    )
    .await?;
    if let Some(content_range) = content_range {
        let expected_length = content_range
            .end
            .checked_sub(content_range.start)
            .and_then(|delta| delta.checked_add(1))
            .ok_or_else(|| protocol_error("content-range length overflowed u64"))?;

        if bytes.len() as u64 != expected_length {
            return Err(protocol_error(format!(
                "browser-local blob response body from '{display_url}' returned {} bytes, but Content-Range declared {expected_length}",
                bytes.len()
            )));
        }
    }

    Ok(HttpRangeReadResult {
        metadata: HttpObjectMetadata {
            url: display_url,
            size_bytes: object_size.or(Some(bytes.len() as u64)),
            etag: None,
        },
        bytes,
    })
}

#[cfg(target_arch = "wasm32")]
async fn probe_blob_url_metadata(
    url: &Url,
    requirements: HttpMetadataProbeRequirements,
) -> Result<HttpObjectMetadata, QueryError> {
    let display_url = redacted_url(url);
    let response = match fetch_blob_url_response(url, Some("bytes=0-0"), &display_url).await {
        Ok(response) => response,
        Err(_) => {
            return probe_blob_url_metadata_via_full_fetch(url, &display_url, requirements).await;
        }
    };
    let status = js_response_status(&response, &display_url)?;
    if status == StatusCode::RANGE_NOT_SATISFIABLE {
        let unsatisfied_size =
            parse_js_unsatisfied_content_range_total_size(&response, &display_url)?;
        if unsatisfied_size == Some(0) {
            let metadata = HttpObjectMetadata {
                url: display_url,
                size_bytes: Some(0),
                etag: None,
            };
            validate_metadata_probe_requirements(&metadata, requirements)?;
            return Ok(metadata);
        }
    }
    if let Some(error) = map_status_error(status, &display_url) {
        return Err(error);
    }
    if status == StatusCode::OK {
        return probe_blob_url_metadata_from_full_response(&response, &display_url, requirements)
            .await;
    }
    if status != StatusCode::PARTIAL_CONTENT {
        return Err(protocol_error(format!(
            "metadata probe to browser-local blob URL '{display_url}' expected HTTP 206 Partial Content, got {status}"
        )));
    }

    let content_range = parse_js_content_range(&response, &display_url)?;
    HttpByteRange::Bounded {
        offset: 0,
        length: 1,
    }
    .validate_content_range(content_range, &display_url)?;
    let bytes = response_array_buffer_bytes(
        &response,
        format!("browser metadata probe body for '{display_url}' failed"),
    )
    .await?;
    if bytes.len() != 1 {
        return Err(protocol_error(format!(
            "browser metadata probe body from '{display_url}' returned {} bytes, expected 1",
            bytes.len()
        )));
    }

    let metadata = HttpObjectMetadata {
        url: display_url,
        size_bytes: Some(content_range.total_size),
        etag: None,
    };
    validate_metadata_probe_requirements(&metadata, requirements)?;
    Ok(metadata)
}

#[cfg(target_arch = "wasm32")]
async fn probe_blob_url_metadata_via_full_fetch(
    url: &Url,
    display_url: &str,
    requirements: HttpMetadataProbeRequirements,
) -> Result<HttpObjectMetadata, QueryError> {
    let response = fetch_blob_url_response(url, None, display_url).await?;
    let status = js_response_status(&response, display_url)?;
    if let Some(error) = map_status_error(status, display_url) {
        return Err(error);
    }
    if status != StatusCode::OK {
        return Err(protocol_error(format!(
            "metadata fallback to browser-local blob URL '{display_url}' expected HTTP 200 OK, got {status}"
        )));
    }

    probe_blob_url_metadata_from_full_response(&response, display_url, requirements).await
}

#[cfg(target_arch = "wasm32")]
async fn probe_blob_url_metadata_from_full_response(
    response: &JsValue,
    display_url: &str,
    requirements: HttpMetadataProbeRequirements,
) -> Result<HttpObjectMetadata, QueryError> {
    let blob = response_blob(
        response,
        format!("browser metadata fallback Blob for '{display_url}' failed"),
    )
    .await?;
    let size_bytes = blob_size_to_u64(blob.size())?;
    if let Some(content_length) =
        parse_optional_js_response_header_u64(response, CONTENT_LENGTH.as_str(), display_url)?
    {
        if content_length != size_bytes {
            return Err(protocol_error(format!(
                "browser metadata fallback body from '{display_url}' exposed {size_bytes} bytes, but Content-Length declared {content_length}"
            )));
        }
    }
    let metadata = HttpObjectMetadata {
        url: display_url.to_string(),
        size_bytes: Some(size_bytes),
        etag: None,
    };
    validate_metadata_probe_requirements(&metadata, requirements)?;
    Ok(metadata)
}

#[cfg(target_arch = "wasm32")]
async fn read_blob_url_full_response_range(
    response: &JsValue,
    display_url: &str,
    range: HttpByteRange,
) -> Result<HttpRangeReadResult, QueryError> {
    let bytes = response_array_buffer_bytes(
        response,
        format!("browser range fallback body for '{display_url}' failed"),
    )
    .await?;
    let object_size = validate_blob_full_response_size(response, bytes.len(), display_url)?;
    let bytes = slice_blob_full_response_bytes(bytes, range, object_size, display_url)?;

    Ok(HttpRangeReadResult {
        metadata: HttpObjectMetadata {
            url: display_url.to_string(),
            size_bytes: Some(object_size),
            etag: None,
        },
        bytes,
    })
}

#[cfg(target_arch = "wasm32")]
fn validate_blob_full_response_size(
    response: &JsValue,
    byte_len: usize,
    display_url: &str,
) -> Result<u64, QueryError> {
    let actual_size = byte_len as u64;
    if let Some(content_length) =
        parse_optional_js_response_header_u64(response, CONTENT_LENGTH.as_str(), display_url)?
    {
        if content_length != actual_size {
            return Err(protocol_error(format!(
                "browser-local blob response body from '{display_url}' returned {actual_size} bytes, but Content-Length declared {content_length}"
            )));
        }
        return Ok(content_length);
    }

    Ok(actual_size)
}

#[cfg(target_arch = "wasm32")]
fn slice_blob_full_response_bytes(
    bytes: Bytes,
    range: HttpByteRange,
    object_size: u64,
    display_url: &str,
) -> Result<Bytes, QueryError> {
    let (start, end) = match range {
        HttpByteRange::Full => return Ok(bytes),
        HttpByteRange::Bounded { offset, length } => {
            let end = offset
                .checked_add(length)
                .ok_or_else(|| protocol_error("bounded byte range overflowed u64"))?;
            if offset >= object_size || end > object_size {
                return Err(protocol_error(format!(
                    "browser-local blob URL '{display_url}' only exposes {object_size} bytes, but bytes {offset}..{end} were requested"
                )));
            }
            (offset, end)
        }
        HttpByteRange::FromOffset { offset } => {
            if offset >= object_size {
                return Err(protocol_error(format!(
                    "browser-local blob URL '{display_url}' only exposes {object_size} bytes, but bytes from {offset} were requested"
                )));
            }
            (offset, object_size)
        }
        HttpByteRange::Suffix { length } => (object_size.saturating_sub(length), object_size),
    };
    let start = usize::try_from(start).map_err(|_| {
        protocol_error("browser-local blob range start exceeded addressable memory")
    })?;
    let end = usize::try_from(end)
        .map_err(|_| protocol_error("browser-local blob range end exceeded addressable memory"))?;

    if start == 0 && end == bytes.len() {
        return Ok(bytes);
    }

    Ok(Bytes::copy_from_slice(&bytes[start..end]))
}

#[cfg(target_arch = "wasm32")]
async fn fetch_blob_url_response(
    url: &Url,
    range_header: Option<&str>,
    display_url: &str,
) -> Result<JsValue, QueryError> {
    let global = js_sys::global();
    let fetch = js_function(&global, "fetch")?;
    let promise = if let Some(range_header) = range_header {
        let headers = Object::new();
        Reflect::set(
            &headers,
            &JsValue::from_str(RANGE.as_str()),
            &JsValue::from_str(range_header),
        )
        .map_err(|error| js_cache_error("browser blob fetch headers could not be set", error))?;
        let init = Object::new();
        Reflect::set(&init, &JsValue::from_str("headers"), &headers)
            .map_err(|error| js_cache_error("browser blob fetch init could not be set", error))?;
        fetch.call2(&global, &JsValue::from_str(url.as_str()), &init)
    } else {
        fetch.call1(&global, &JsValue::from_str(url.as_str()))
    }
    .map_err(|error| js_cache_error(format!("browser fetch for '{display_url}' failed"), error))?
    .dyn_into::<Promise>()
    .map_err(|error| js_cache_error("browser fetch did not return a Promise", error))?;

    JsFuture::from(promise)
        .await
        .map_err(|error| js_cache_error(format!("browser fetch for '{display_url}' failed"), error))
}

#[cfg(target_arch = "wasm32")]
async fn response_array_buffer_bytes(
    response: &JsValue,
    context: String,
) -> Result<Bytes, QueryError> {
    let buffer = js_function(response, "arrayBuffer")?
        .call0(&response)
        .map_err(|error| {
            js_cache_error(
                "browser fetch response could not produce an ArrayBuffer",
                error,
            )
        })?;
    let buffer = await_js_promise(buffer)
        .await
        .map_err(|error| js_cache_error(context, error))?;
    Ok(array_buffer_to_bytes(&buffer))
}

#[cfg(target_arch = "wasm32")]
async fn response_blob(response: &JsValue, context: String) -> Result<web_sys::Blob, QueryError> {
    let blob = js_function(response, "blob")?
        .call0(response)
        .map_err(|error| {
            js_cache_error("browser fetch response could not produce a Blob", error)
        })?;
    let blob = await_js_promise(blob)
        .await
        .map_err(|error| js_cache_error(context, error))?;
    blob.dyn_into::<web_sys::Blob>()
        .map_err(|error| js_cache_error("browser fetch response returned a non-Blob value", error))
}

#[cfg(target_arch = "wasm32")]
fn js_response_status(response: &JsValue, display_url: &str) -> Result<StatusCode, QueryError> {
    let status = Reflect::get(response, &JsValue::from_str("status"))
        .map_err(|error| js_cache_error("browser response status could not be read", error))?
        .as_f64()
        .ok_or_else(|| {
            cache_error(format!(
                "browser response from '{display_url}' did not expose a numeric status"
            ))
        })?;
    let status = u16::try_from(status as u64).map_err(|_| {
        cache_error(format!(
            "browser response from '{display_url}' returned out-of-range status {status}"
        ))
    })?;
    StatusCode::from_u16(status).map_err(|error| {
        cache_error(format!(
            "browser response from '{display_url}' returned invalid status {status}: {error}"
        ))
    })
}

#[cfg(target_arch = "wasm32")]
fn js_response_header(
    response: &JsValue,
    name: &str,
    display_url: &str,
) -> Result<Option<String>, QueryError> {
    let headers = Reflect::get(response, &JsValue::from_str("headers"))
        .map_err(|error| js_cache_error("browser response headers could not be read", error))?;
    let get = js_function(&headers, "get")?;
    let value = get
        .call1(&headers, &JsValue::from_str(name))
        .map_err(|error| {
            js_cache_error(
                format!("browser response header '{name}' for '{display_url}' could not be read"),
                error,
            )
        })?;
    if value.is_null() || value.is_undefined() {
        return Ok(None);
    }
    value.as_string().map(Some).ok_or_else(|| {
        cache_error(format!(
            "browser response header '{name}' for '{display_url}' was not a string"
        ))
    })
}

#[cfg(target_arch = "wasm32")]
fn parse_js_content_range(
    response: &JsValue,
    display_url: &str,
) -> Result<ParsedContentRange, QueryError> {
    let content_range = js_response_header(response, CONTENT_RANGE.as_str(), display_url)?
        .ok_or_else(|| {
            protocol_error(format!(
                "partial response from '{display_url}' did not include a Content-Range header"
            ))
        })?;
    parse_content_range_value(&content_range, display_url)
}

#[cfg(target_arch = "wasm32")]
fn parse_js_unsatisfied_content_range_total_size(
    response: &JsValue,
    display_url: &str,
) -> Result<Option<u64>, QueryError> {
    let Some(content_range) = js_response_header(response, CONTENT_RANGE.as_str(), display_url)?
    else {
        return Ok(None);
    };
    parse_unsatisfied_content_range_total_size_value(&content_range, display_url)
}

#[cfg(target_arch = "wasm32")]
fn parse_optional_js_response_header_u64(
    response: &JsValue,
    name: &str,
    display_url: &str,
) -> Result<Option<u64>, QueryError> {
    js_response_header(response, name, display_url)?
        .map(|value| {
            value.parse::<u64>().map_err(|error| {
                protocol_error(format!(
                    "browser response header '{name}' for '{display_url}' returned an invalid integer: {error}"
                ))
            })
        })
        .transpose()
}

#[cfg(target_arch = "wasm32")]
async fn blob_to_bytes(blob: &web_sys::Blob) -> Result<Bytes, QueryError> {
    let buffer = JsFuture::from(blob.array_buffer())
        .await
        .map_err(|error| js_cache_error("browser Blob arrayBuffer failed", error))?;
    Ok(array_buffer_to_bytes(&buffer))
}

#[cfg(target_arch = "wasm32")]
fn array_buffer_to_bytes(buffer: &JsValue) -> Bytes {
    let array = Uint8Array::new(buffer);
    let mut bytes = vec![0_u8; array.length() as usize];
    array.copy_to(bytes.as_mut_slice());
    Bytes::from(bytes)
}

#[cfg(target_arch = "wasm32")]
fn blob_size_to_u64(size: f64) -> Result<u64, QueryError> {
    if !size.is_finite() || size < 0.0 || size > JS_MAX_SAFE_INTEGER_U64 as f64 {
        return Err(protocol_error("browser Blob reported an invalid size"));
    }
    Ok(size as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_range_cache_recovers_poisoned_metrics_once() {
        let cache = Arc::new(MemoryRangeCache::new());
        let poisoned_cache = Arc::clone(&cache);
        let poison = std::thread::spawn(move || {
            let _guard = poisoned_cache
                .state
                .lock()
                .expect("fresh cache lock should succeed");
            panic!("poison the cache lock");
        });
        assert!(poison.join().is_err());

        cache.record_network_bytes_fetched(4);
        let identity = RangeCacheIdentity::strong("https://example.test/object", "\"v1\"", 4)
            .expect("quoted ETag should be cacheable");
        assert_eq!(
            cache.load(&identity, ByteExtent::new(0, 4).expect("valid extent")),
            RangeCacheLookup::Miss
        );

        let metrics = cache.metrics();
        assert_eq!(metrics.network_bytes_fetched, 4);
        assert_eq!(metrics.cache_misses, 1);
        assert_eq!(metrics.cache_errors, 1);
    }
}

#[cfg(target_arch = "wasm32")]
fn u64_to_safe_js_number(value: u64, field: &str) -> Result<f64, QueryError> {
    if value > JS_MAX_SAFE_INTEGER_U64 {
        return Err(invalid_request(format!(
            "{field} exceeds JavaScript's safe integer range"
        )));
    }
    Ok(value as f64)
}

#[cfg(target_arch = "wasm32")]
fn validate_opfs_name(name: String, field: &str) -> Result<String, QueryError> {
    if name.is_empty() {
        return Err(invalid_request(format!(
            "OPFS cache {field} cannot be empty"
        )));
    }
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(invalid_request(format!(
            "OPFS cache {field} must contain only ASCII letters, digits, '.', '-', or '_'"
        )));
    }
    Ok(name)
}

#[cfg(target_arch = "wasm32")]
fn js_function(target: &JsValue, name: &str) -> Result<Function, QueryError> {
    js_function_value(target, name)
        .map_err(|error| js_cache_error(format!("browser method '{name}' lookup failed"), error))
}

#[cfg(target_arch = "wasm32")]
fn js_function_value(target: &JsValue, name: &str) -> Result<Function, JsValue> {
    Reflect::get(target, &JsValue::from_str(name))?.dyn_into::<Function>()
}

#[cfg(target_arch = "wasm32")]
fn create_options(create: bool) -> Result<JsValue, QueryError> {
    create_options_value(create)
        .map_err(|error| js_cache_error("OPFS create options could not be built", error))
}

#[cfg(target_arch = "wasm32")]
fn create_options_value(create: bool) -> Result<JsValue, JsValue> {
    let options = Object::new();
    Reflect::set(
        &options,
        &JsValue::from_str("create"),
        &JsValue::from_bool(create),
    )?;
    Ok(options.into())
}

#[cfg(target_arch = "wasm32")]
async fn await_js_promise(value: JsValue) -> Result<JsValue, JsValue> {
    let promise = value.dyn_into::<Promise>()?;
    JsFuture::from(promise).await
}

#[cfg(target_arch = "wasm32")]
fn js_error_name_is(error: &JsValue, expected: &str) -> bool {
    Reflect::get(error, &JsValue::from_str("name"))
        .ok()
        .and_then(|value| value.as_string())
        .as_deref()
        == Some(expected)
}

#[cfg(target_arch = "wasm32")]
fn js_cache_error(context: impl Into<String>, error: JsValue) -> QueryError {
    let context = context.into();
    let message = error
        .as_string()
        .or_else(|| {
            Reflect::get(&error, &JsValue::from_str("message"))
                .ok()
                .and_then(|value| value.as_string())
        })
        .unwrap_or_else(|| format!("{error:?}"));
    cache_error(format!("{context}: {message}"))
}

#[derive(Clone, Debug)]
pub struct HttpRangeReader {
    client: reqwest::Client,
}

#[cfg(target_arch = "wasm32")]
fn bypass_browser_http_cache(request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    request.fetch_cache_no_store()
}

#[cfg(not(target_arch = "wasm32"))]
fn bypass_browser_http_cache(request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    request
}

impl Default for HttpRangeReader {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpRangeReader {
    pub fn new() -> Self {
        Self::with_client(reqwest::Client::new())
    }

    pub fn with_client(client: reqwest::Client) -> Self {
        Self { client }
    }

    pub async fn read_range(
        &self,
        url: &str,
        range: HttpByteRange,
    ) -> Result<HttpRangeReadResult, QueryError> {
        self.read_range_with_timeout(url, range, None).await
    }

    pub async fn read_range_with_timeout(
        &self,
        url: &str,
        range: HttpByteRange,
        timeout: Option<Duration>,
    ) -> Result<HttpRangeReadResult, QueryError> {
        self.read_range_with_validation(url, range, None, timeout)
            .await
    }

    pub async fn read_range_with_validation(
        &self,
        url: &str,
        range: HttpByteRange,
        validation: Option<HttpRangeValidation>,
        timeout: Option<Duration>,
    ) -> Result<HttpRangeReadResult, QueryError> {
        let url = parse_url(url)?;
        let display_url = redacted_url(&url);
        #[cfg(target_arch = "wasm32")]
        if url.scheme() == "blob" {
            return read_blob_url_range(&url, range, validation).await;
        }
        let range_header = range.header_value()?;

        let mut request = self.client.get(url.clone());
        if let Some(range_header) = &range_header {
            request = request.header(RANGE, range_header);
        }
        if range.expects_partial_response() {
            if let Some(validation) = validation.clone() {
                request = validation.apply_request(request)?;
            }
        }
        if let Some(timeout) = timeout {
            request = request.timeout(timeout);
        }

        let response = bypass_browser_http_cache(request)
            .send()
            .await
            .map_err(|error| {
                QueryError::new(
                    QueryErrorCode::ExecutionFailed,
                    format!("http request to '{display_url}' failed: {error}"),
                    supported_target(),
                )
            })?;

        if let Some(error) = map_status_error(response.status(), &display_url) {
            return Err(error);
        }

        let content_range = if range.expects_partial_response() {
            if response.status() != StatusCode::PARTIAL_CONTENT {
                if validation.is_some() {
                    return Err(protocol_error(format!(
                        "range request to '{display_url}' expected HTTP 206 Partial Content, got {}; If-Range validation likely failed due object identity drift",
                        response.status()
                    )));
                }
                return Err(protocol_error(format!(
                    "range request to '{display_url}' expected HTTP 206 Partial Content, got {}",
                    response.status()
                )));
            }

            let content_range = parse_content_range(response.headers(), &display_url)?;
            range.validate_content_range(content_range, &display_url)?;
            Some(content_range)
        } else {
            if response.status() != StatusCode::OK {
                return Err(protocol_error(format!(
                    "full-object request to '{display_url}' expected HTTP 200 OK, got {}",
                    response.status()
                )));
            }

            None
        };
        let object_size = match content_range {
            Some(content_range) => Some(content_range.total_size),
            None => parse_optional_content_length(response.headers(), &display_url)?,
        };
        let etag = parse_optional_header_string(response.headers(), ETAG.as_str(), &display_url)?;
        if let Some(validation) = validation {
            validation.validate_response_identity(etag.as_deref(), &display_url)?;
        }

        let bytes = response.bytes().await.map_err(|error| {
            QueryError::new(
                QueryErrorCode::ExecutionFailed,
                format!("http response body from '{display_url}' could not be read: {error}"),
                supported_target(),
            )
        })?;

        if let Some(content_range) = content_range {
            let expected_length = content_range
                .end
                .checked_sub(content_range.start)
                .and_then(|delta| delta.checked_add(1))
                .ok_or_else(|| protocol_error("content-range length overflowed u64"))?;

            if bytes.len() as u64 != expected_length {
                return Err(protocol_error(format!(
                    "http response body from '{display_url}' returned {} bytes, but Content-Range declared {expected_length}",
                    bytes.len()
                )));
            }
        }

        Ok(HttpRangeReadResult {
            metadata: HttpObjectMetadata {
                url: display_url,
                size_bytes: object_size.or(Some(bytes.len() as u64)),
                etag,
            },
            bytes,
        })
    }

    pub async fn probe_metadata(
        &self,
        url: &str,
        requirements: HttpMetadataProbeRequirements,
    ) -> Result<HttpObjectMetadata, QueryError> {
        self.probe_metadata_with_timeout(url, requirements, None)
            .await
    }

    pub async fn probe_metadata_with_timeout(
        &self,
        url: &str,
        requirements: HttpMetadataProbeRequirements,
        timeout: Option<Duration>,
    ) -> Result<HttpObjectMetadata, QueryError> {
        let url = parse_url(url)?;
        let display_url = redacted_url(&url);
        #[cfg(target_arch = "wasm32")]
        if url.scheme() == "blob" {
            return probe_blob_url_metadata(&url, requirements).await;
        }
        let mut request = self.client.get(url.clone()).header(RANGE, "bytes=0-0");
        if let Some(timeout) = timeout {
            request = request.timeout(timeout);
        }
        let response = bypass_browser_http_cache(request)
            .send()
            .await
            .map_err(|error| {
                QueryError::new(
                    QueryErrorCode::ExecutionFailed,
                    format!("http request to '{display_url}' failed: {error}"),
                    supported_target(),
                )
            })?;

        if response.status() == StatusCode::RANGE_NOT_SATISFIABLE {
            let unsatisfied_size =
                parse_unsatisfied_content_range_total_size(response.headers(), &display_url)?;
            if unsatisfied_size == Some(0) {
                let etag =
                    parse_optional_header_string(response.headers(), ETAG.as_str(), &display_url)?;
                let metadata = HttpObjectMetadata {
                    url: display_url,
                    size_bytes: Some(0),
                    etag,
                };
                validate_metadata_probe_requirements(&metadata, requirements)?;
                return Ok(metadata);
            }
        }

        if let Some(error) = map_status_error(response.status(), &display_url) {
            return Err(error);
        }
        if response.status() != StatusCode::PARTIAL_CONTENT {
            return Err(protocol_error(format!(
                "metadata probe to '{display_url}' expected HTTP 206 Partial Content, got {}",
                response.status()
            )));
        }

        let content_range = parse_content_range(response.headers(), &display_url)?;
        HttpByteRange::Bounded {
            offset: 0,
            length: 1,
        }
        .validate_content_range(content_range, &display_url)?;
        let etag = parse_optional_header_string(response.headers(), ETAG.as_str(), &display_url)?;
        let bytes = response.bytes().await.map_err(|error| {
            QueryError::new(
                QueryErrorCode::ExecutionFailed,
                format!("http response body from '{display_url}' could not be read: {error}"),
                supported_target(),
            )
        })?;
        let expected_length = content_range
            .end
            .checked_sub(content_range.start)
            .and_then(|delta| delta.checked_add(1))
            .ok_or_else(|| protocol_error("content-range length overflowed u64"))?;
        if bytes.len() as u64 != expected_length {
            return Err(protocol_error(format!(
                "http response body from '{display_url}' returned {} bytes, but Content-Range declared {expected_length}",
                bytes.len()
            )));
        }

        let metadata = HttpObjectMetadata {
            url: display_url,
            size_bytes: Some(content_range.total_size),
            etag,
        };
        validate_metadata_probe_requirements(&metadata, requirements)?;

        Ok(metadata)
    }

    pub async fn resolve_metadata(
        &self,
        url: &str,
        known_metadata: Option<HttpObjectMetadata>,
        requirements: HttpMetadataProbeRequirements,
    ) -> Result<HttpObjectMetadata, QueryError> {
        self.resolve_metadata_with_timeout(url, known_metadata, requirements, None)
            .await
    }

    pub async fn resolve_metadata_with_timeout(
        &self,
        url: &str,
        known_metadata: Option<HttpObjectMetadata>,
        requirements: HttpMetadataProbeRequirements,
        timeout: Option<Duration>,
    ) -> Result<HttpObjectMetadata, QueryError> {
        let requested_url = parse_url(url)?;
        let requested_display_url = redacted_url(&requested_url);
        if let Some(known_metadata) = known_metadata {
            if known_metadata.url == requested_display_url
                && requirements.is_satisfied_by(&known_metadata)
            {
                return Ok(known_metadata);
            }
        }

        self.probe_metadata_with_timeout(url, requirements, timeout)
            .await
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ParsedContentRange {
    start: u64,
    end: u64,
    total_size: u64,
}

fn parse_url(url: &str) -> Result<Url, QueryError> {
    let display_url = redacted_input_url(url);
    let parsed = Url::parse(url).map_err(|error| {
        QueryError::new(
            QueryErrorCode::InvalidRequest,
            format!("invalid HTTP object URL '{display_url}': {error}"),
            supported_target(),
        )
    })?;

    match parsed.scheme() {
        "http" | "https" => Ok(parsed),
        #[cfg(target_arch = "wasm32")]
        "blob" => Ok(parsed),
        scheme => Err(invalid_request(format!(
            "invalid HTTP object URL '{}': unsupported scheme '{scheme}'",
            redacted_url(&parsed)
        ))),
    }
}

fn map_status_error(status: StatusCode, display_url: &str) -> Option<QueryError> {
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Some(QueryError::new(
            QueryErrorCode::AccessDenied,
            format!("http request to '{display_url}' was denied with status {status}"),
            supported_target(),
        )),
        StatusCode::NOT_FOUND => Some(QueryError::new(
            QueryErrorCode::ObjectNotFound,
            format!("http request to '{display_url}' failed with status {status}"),
            supported_target(),
        )),
        StatusCode::RANGE_NOT_SATISFIABLE => Some(protocol_error(format!(
            "http request to '{display_url}' failed with status {status}"
        ))),
        status if status.is_client_error() => Some(protocol_error(format!(
            "http request to '{display_url}' failed with client error status {status}"
        ))),
        status if status.is_server_error() => Some(QueryError::new(
            QueryErrorCode::ExecutionFailed,
            format!("http request to '{display_url}' failed with server error status {status}"),
            supported_target(),
        )),
        _ => None,
    }
}

fn parse_optional_content_length(
    headers: &reqwest::header::HeaderMap,
    display_url: &str,
) -> Result<Option<u64>, QueryError> {
    headers
        .get(CONTENT_LENGTH)
        .map(|value| parse_header_u64(value, CONTENT_LENGTH.as_str(), display_url))
        .transpose()
}

fn parse_content_range(
    headers: &reqwest::header::HeaderMap,
    display_url: &str,
) -> Result<ParsedContentRange, QueryError> {
    let content_range = headers
        .get(CONTENT_RANGE)
        .ok_or_else(|| {
            protocol_error(format!(
                "partial response from '{display_url}' did not include a Content-Range header"
            ))
        })?
        .to_str()
        .map_err(|error| {
            protocol_error(format!(
                "partial response from '{display_url}' returned a non-UTF8 Content-Range header: {error}"
            ))
        })?;

    parse_content_range_value(content_range, display_url)
}

fn parse_content_range_value(
    content_range: &str,
    display_url: &str,
) -> Result<ParsedContentRange, QueryError> {
    let (unit, spec) = content_range.split_once(' ').ok_or_else(|| {
        protocol_error(format!(
            "partial response from '{display_url}' returned an invalid Content-Range header: {content_range}"
        ))
    })?;
    if unit != "bytes" {
        return Err(protocol_error(format!(
            "partial response from '{display_url}' returned unsupported Content-Range units: {content_range}"
        )));
    }

    let (range_spec, total_size) = spec.split_once('/').ok_or_else(|| {
        protocol_error(format!(
            "partial response from '{display_url}' returned an invalid Content-Range header: {content_range}"
        ))
    })?;

    if range_spec == "*" || total_size == "*" {
        return Err(protocol_error(format!(
            "partial response from '{display_url}' returned an unsatisfied Content-Range header: {content_range}"
        )));
    }

    let (start, end) = range_spec.split_once('-').ok_or_else(|| {
        protocol_error(format!(
            "partial response from '{display_url}' returned an invalid Content-Range header: {content_range}"
        ))
    })?;

    let start = start.parse::<u64>().map_err(|error| {
        protocol_error(format!(
            "partial response from '{display_url}' returned an invalid Content-Range start: {error}"
        ))
    })?;
    let end = end.parse::<u64>().map_err(|error| {
        protocol_error(format!(
            "partial response from '{display_url}' returned an invalid Content-Range end: {error}"
        ))
    })?;
    let total_size = total_size.parse::<u64>().map_err(|error| {
        protocol_error(format!(
            "partial response from '{display_url}' returned an invalid Content-Range size: {error}"
        ))
    })?;

    if end < start {
        return Err(protocol_error(format!(
            "partial response from '{display_url}' returned a descending Content-Range: {content_range}"
        )));
    }
    if end >= total_size {
        return Err(protocol_error(format!(
            "partial response from '{display_url}' returned an out-of-bounds Content-Range: {content_range}"
        )));
    }

    Ok(ParsedContentRange {
        start,
        end,
        total_size,
    })
}

fn parse_unsatisfied_content_range_total_size(
    headers: &reqwest::header::HeaderMap,
    display_url: &str,
) -> Result<Option<u64>, QueryError> {
    let Some(content_range) = headers.get(CONTENT_RANGE) else {
        return Ok(None);
    };
    let content_range = content_range.to_str().map_err(|error| {
        protocol_error(format!(
            "response from '{display_url}' returned a non-UTF8 Content-Range header: {error}"
        ))
    })?;

    parse_unsatisfied_content_range_total_size_value(content_range, display_url)
}

fn parse_unsatisfied_content_range_total_size_value(
    content_range: &str,
    display_url: &str,
) -> Result<Option<u64>, QueryError> {
    let (unit, spec) = content_range.split_once(' ').ok_or_else(|| {
        protocol_error(format!(
            "response from '{display_url}' returned an invalid Content-Range header: {content_range}"
        ))
    })?;
    if unit != "bytes" {
        return Err(protocol_error(format!(
            "response from '{display_url}' returned unsupported Content-Range units: {content_range}"
        )));
    }
    let (range_spec, total_size) = spec.split_once('/').ok_or_else(|| {
        protocol_error(format!(
            "response from '{display_url}' returned an invalid Content-Range header: {content_range}"
        ))
    })?;
    if range_spec != "*" {
        return Ok(None);
    }
    if total_size == "*" {
        return Err(protocol_error(format!(
            "response from '{display_url}' returned an invalid unsatisfied Content-Range header: {content_range}"
        )));
    }

    let total_size = total_size.parse::<u64>().map_err(|error| {
        protocol_error(format!(
            "response from '{display_url}' returned an invalid Content-Range size: {error}"
        ))
    })?;
    Ok(Some(total_size))
}

fn validate_metadata_probe_requirements(
    metadata: &HttpObjectMetadata,
    requirements: HttpMetadataProbeRequirements,
) -> Result<(), QueryError> {
    if requirements.require_size && metadata.size_bytes.is_none() {
        return Err(protocol_error(format!(
            "metadata probe for '{}' required object size metadata, but no browser-visible size headers were returned",
            metadata.url
        )));
    }
    if requirements.require_etag && metadata.etag.is_none() {
        return Err(protocol_error(format!(
            "metadata probe for '{}' required an exposed ETag header, but none was returned",
            metadata.url
        )));
    }

    Ok(())
}

fn parse_header_u64(
    value: &reqwest::header::HeaderValue,
    header_name: &str,
    display_url: &str,
) -> Result<u64, QueryError> {
    let text = value.to_str().map_err(|error| {
        protocol_error(format!(
            "response from '{display_url}' returned a non-UTF8 {header_name} header: {error}"
        ))
    })?;

    text.parse::<u64>().map_err(|error| {
        protocol_error(format!(
            "response from '{display_url}' returned an invalid {header_name} header: {error}"
        ))
    })
}

fn parse_optional_header_string(
    headers: &reqwest::header::HeaderMap,
    header_name: &str,
    display_url: &str,
) -> Result<Option<String>, QueryError> {
    headers
        .get(header_name)
        .map(|value| {
            value.to_str().map(|text| text.to_string()).map_err(|error| {
                protocol_error(format!(
                    "response from '{display_url}' returned a non-UTF8 {header_name} header: {error}"
                ))
            })
        })
        .transpose()
}

fn redacted_input_url(url: &str) -> String {
    let end = url.find(['?', '#']).unwrap_or(url.len());
    url[..end].to_string()
}

fn redacted_url(url: &Url) -> String {
    let mut redacted = url.clone();
    let _ = redacted.set_username("");
    let _ = redacted.set_password(None);
    redacted.set_query(None);
    redacted.set_fragment(None);
    redacted.to_string()
}

fn invalid_request(message: impl Into<String>) -> QueryError {
    QueryError::new(QueryErrorCode::InvalidRequest, message, supported_target())
}

fn cache_error(message: impl Into<String>) -> QueryError {
    QueryError::new(QueryErrorCode::ExecutionFailed, message, supported_target())
}

fn protocol_error(message: impl Into<String>) -> QueryError {
    QueryError::new(
        QueryErrorCode::ObjectStoreProtocol,
        message,
        supported_target(),
    )
}
