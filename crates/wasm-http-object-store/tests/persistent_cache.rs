use std::sync::{Arc, Mutex};

use bytes::Bytes;
use query_contract::QueryError;
use wasm_http_object_store::{
    BrowserCacheMode, BrowserLocalObject, BrowserObject, BrowserObjectRangeReader, ByteExtent,
    ExtentCacheEntry, ExtentCacheKey, MemoryPersistentExtentCache, PersistentCacheFuture,
    PersistentExtentCache, RangeCacheIdentity, RangeCacheLookup, RangeCacheStoreOutcome,
    SharedRangeCache,
};

fn strong_identity(resource: &str, etag: &str, size_bytes: u64) -> RangeCacheIdentity {
    RangeCacheIdentity::strong(resource, etag, size_bytes)
        .expect("quoted strong ETag should form a cache identity")
}

#[derive(Default)]
struct RecordingPersistentCache {
    entries: Mutex<Vec<ExtentCacheEntry>>,
}

impl PersistentExtentCache for RecordingPersistentCache {
    fn load<'a>(
        &'a self,
        key: &'a ExtentCacheKey,
        requested_extent: ByteExtent,
    ) -> PersistentCacheFuture<'a, Option<ExtentCacheEntry>> {
        Box::pin(async move {
            let entries = self
                .entries
                .lock()
                .expect("cache lock should not be poisoned");
            Ok(entries
                .iter()
                .find(|entry| entry.can_satisfy(key, requested_extent))
                .cloned())
        })
    }

    fn store<'a>(&'a self, entry: &'a ExtentCacheEntry) -> PersistentCacheFuture<'a, ()> {
        Box::pin(async move {
            self.entries
                .lock()
                .expect("cache lock should not be poisoned")
                .push(entry.clone());
            Ok(())
        })
    }
}

#[derive(Default)]
struct FailingPersistentCache;

impl PersistentExtentCache for FailingPersistentCache {
    fn load<'a>(
        &'a self,
        _key: &ExtentCacheKey,
        _requested_extent: ByteExtent,
    ) -> PersistentCacheFuture<'a, Option<ExtentCacheEntry>> {
        Box::pin(async {
            Err(QueryError::new(
                query_contract::QueryErrorCode::ExecutionFailed,
                "cache unavailable",
                wasm_http_object_store::supported_target(),
            ))
        })
    }

    fn store<'a>(&'a self, _entry: &'a ExtentCacheEntry) -> PersistentCacheFuture<'a, ()> {
        Box::pin(async {
            Err(QueryError::new(
                query_contract::QueryErrorCode::ExecutionFailed,
                "cache unavailable",
                wasm_http_object_store::supported_target(),
            ))
        })
    }
}

#[derive(Default)]
struct UnsatisfyingPersistentCache;

impl PersistentExtentCache for UnsatisfyingPersistentCache {
    fn load<'a>(
        &'a self,
        key: &'a ExtentCacheKey,
        _requested_extent: ByteExtent,
    ) -> PersistentCacheFuture<'a, Option<ExtentCacheEntry>> {
        Box::pin(async move {
            ExtentCacheEntry::new(
                key.clone(),
                ByteExtent::new(0, 2).expect("valid extent"),
                Bytes::from_static(b"ab"),
            )
            .map(Some)
        })
    }

    fn store<'a>(&'a self, _entry: &'a ExtentCacheEntry) -> PersistentCacheFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Default)]
struct MalformedPersistentCache;

impl PersistentExtentCache for MalformedPersistentCache {
    fn load<'a>(
        &'a self,
        key: &'a ExtentCacheKey,
        _requested_extent: ByteExtent,
    ) -> PersistentCacheFuture<'a, Option<ExtentCacheEntry>> {
        Box::pin(async move {
            Ok(Some(ExtentCacheEntry {
                key: key.clone(),
                extent: ByteExtent::new(0, 4).expect("valid extent"),
                bytes: Bytes::from_static(b"ab"),
            }))
        })
    }

    fn store<'a>(&'a self, _entry: &'a ExtentCacheEntry) -> PersistentCacheFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn memory_persistent_cache_reports_hit_and_miss() {
    let cache = MemoryPersistentExtentCache::with_max_entries(8);
    let key = ExtentCacheKey::new("https://example.test/object", Some("\"v1\"".to_string()));
    let entry = ExtentCacheEntry::new(
        key.clone(),
        ByteExtent::new(0, 8).expect("valid extent"),
        Bytes::from_static(b"abcdefgh"),
    )
    .expect("cache entry should be valid");

    cache
        .store(&entry)
        .await
        .expect("cache store should succeed");
    let hit = cache
        .load(&key, ByteExtent::new(2, 4).expect("valid extent"))
        .await
        .expect("cache load should succeed")
        .expect("stored extent should satisfy subrange");
    let miss = cache
        .load(
            &ExtentCacheKey::new("https://example.test/other", Some("\"v1\"".to_string())),
            ByteExtent::new(2, 4).expect("valid extent"),
        )
        .await
        .expect("cache load should succeed");

    assert_eq!(
        hit.slice(ByteExtent::new(2, 4).expect("valid extent"))
            .unwrap(),
        b"cdef"[..]
    );
    assert!(miss.is_none());
}

#[tokio::test]
async fn memory_persistent_cache_rejects_identity_mismatch() {
    let cache = MemoryPersistentExtentCache::with_max_entries(8);
    let key_v1 = ExtentCacheKey::new("file:///fixture.parquet", Some("v1".to_string()));
    let key_v2 = ExtentCacheKey::new("file:///fixture.parquet", Some("v2".to_string()));
    let entry = ExtentCacheEntry::new(
        key_v1,
        ByteExtent::new(0, 4).expect("valid extent"),
        Bytes::from_static(b"v1!!"),
    )
    .expect("cache entry should be valid");

    cache
        .store(&entry)
        .await
        .expect("cache store should succeed");
    let stale = cache
        .load(&key_v2, ByteExtent::new(0, 4).expect("valid extent"))
        .await
        .expect("cache load should succeed");

    assert!(stale.is_none());
}

#[tokio::test]
async fn memory_persistent_cache_overwrites_matching_extent_and_evicts_oldest_entry() {
    let cache = MemoryPersistentExtentCache::with_max_entries(1);
    let first_key = ExtentCacheKey::new("file:///first.parquet", Some("v1".to_string()));
    let second_key = ExtentCacheKey::new("file:///second.parquet", Some("v1".to_string()));
    let first = ExtentCacheEntry::new(
        first_key.clone(),
        ByteExtent::new(0, 4).expect("valid extent"),
        Bytes::from_static(b"aaaa"),
    )
    .expect("cache entry should be valid");
    let second = ExtentCacheEntry::new(
        second_key.clone(),
        ByteExtent::new(0, 4).expect("valid extent"),
        Bytes::from_static(b"bbbb"),
    )
    .expect("cache entry should be valid");
    let replacement = ExtentCacheEntry::new(
        second_key.clone(),
        ByteExtent::new(0, 4).expect("valid extent"),
        Bytes::from_static(b"cccc"),
    )
    .expect("cache entry should be valid");

    cache
        .store(&first)
        .await
        .expect("first store should succeed");
    cache
        .store(&second)
        .await
        .expect("second store should succeed");
    cache
        .store(&replacement)
        .await
        .expect("replacement store should succeed");

    let evicted = cache
        .load(&first_key, ByteExtent::new(0, 4).expect("valid extent"))
        .await
        .expect("cache load should succeed");
    let overwritten = cache
        .load(&second_key, ByteExtent::new(0, 4).expect("valid extent"))
        .await
        .expect("cache load should succeed")
        .expect("second entry should remain cached");

    assert!(evicted.is_none());
    assert_eq!(overwritten.bytes.as_ref(), b"cccc");
}

#[tokio::test]
async fn persistent_cache_failures_are_treated_as_cache_misses() {
    let cache = Arc::new(FailingPersistentCache);
    let object = BrowserObject::local(BrowserLocalObject::from_bytes_with_identity(
        "file:///fixture.parquet",
        Bytes::from_static(b"abcdefgh"),
        "v1",
    ));

    let mut reader = BrowserObjectRangeReader::with_persistent_cache(cache);
    let first = reader
        .read_extent(&object, ByteExtent::new(0, 4).expect("valid extent"), None)
        .await
        .expect("store failures should not fail the object read");
    let second = reader
        .read_extent(
            &object,
            ByteExtent::new(4, 4).expect("valid extent"),
            Some(first.metadata),
        )
        .await
        .expect("load and store failures should be cache misses");

    assert_eq!(first.bytes.as_ref(), b"abcd");
    assert_eq!(second.bytes.as_ref(), b"efgh");
    assert_eq!(reader.metrics().bytes_fetched, 8);
    assert_eq!(reader.metrics().bytes_reused, 0);
    assert_eq!(reader.metrics().persistent_cache_errors, 4);
}

#[tokio::test]
async fn stale_extents_are_rejected_when_object_identity_changes() {
    let mut reader = BrowserObjectRangeReader::new();
    let original = BrowserObject::local(BrowserLocalObject::from_bytes_with_identity(
        "file:///fixture.parquet",
        Bytes::from_static(b"abcdefgh"),
        "v1",
    ));
    let replaced = BrowserObject::local(BrowserLocalObject::from_bytes_with_identity(
        "file:///fixture.parquet",
        Bytes::from_static(b"wxyzefgh"),
        "v2",
    ));

    reader
        .read_extent(
            &original,
            ByteExtent::new(0, 4).expect("valid extent"),
            None,
        )
        .await
        .expect("initial extent should populate the cache");
    let updated = reader
        .read_extent(
            &replaced,
            ByteExtent::new(0, 4).expect("valid extent"),
            None,
        )
        .await
        .expect("identity drift should fetch new bytes instead of reusing stale cache");

    assert_eq!(updated.bytes.as_ref(), b"wxyz");
    assert_eq!(reader.metrics().bytes_fetched, 8);
    assert_eq!(reader.metrics().bytes_reused, 0);
    assert_eq!(reader.metrics().validation_misses, 1);
    assert_eq!(reader.cached_extents_for_testing().len(), 1);
    assert_eq!(
        reader.cached_extents_for_testing()[0].bytes.as_ref(),
        b"wxyz"
    );
}

#[tokio::test]
async fn persistent_cache_hooks_report_mode_and_reuse_cached_extents() {
    let cache = Arc::new(RecordingPersistentCache::default());
    let object = BrowserObject::local(BrowserLocalObject::from_bytes_with_identity(
        "file:///fixture.parquet",
        Bytes::from_static(b"abcdefgh"),
        "v1",
    ));

    let mut warm_reader = BrowserObjectRangeReader::with_persistent_cache(cache.clone());
    let warmed = warm_reader
        .read_extent(&object, ByteExtent::new(0, 8).expect("valid extent"), None)
        .await
        .expect("warming read should succeed");

    let mut cold_reader = BrowserObjectRangeReader::with_persistent_cache(cache);
    let reused = cold_reader
        .read_extent(
            &object,
            ByteExtent::new(2, 4).expect("valid extent"),
            Some(warmed.metadata),
        )
        .await
        .expect("subsequent reader should reuse the persistent cache hook");

    assert_eq!(warm_reader.cache_mode(), BrowserCacheMode::Persistent);
    assert_eq!(cold_reader.cache_mode(), BrowserCacheMode::Persistent);
    assert_eq!(reused.bytes.as_ref(), b"cdef");
    assert_eq!(cold_reader.metrics().bytes_fetched, 0);
    assert_eq!(cold_reader.metrics().bytes_reused, 4);
}

#[tokio::test]
async fn shared_range_cache_reuses_persistent_containing_subranges() {
    let persistent = Arc::new(MemoryPersistentExtentCache::with_max_entries(8));
    let identity = strong_identity("https://example.test/object", "\"v1\"", 8);
    let warm_cache = SharedRangeCache::with_persistent_cache(persistent.clone());
    let cold_cache = SharedRangeCache::with_persistent_cache(persistent);

    assert_eq!(
        warm_cache
            .store(
                &identity,
                ByteExtent::new(0, 8).expect("valid extent"),
                Bytes::from_static(b"abcdefgh"),
            )
            .await,
        RangeCacheStoreOutcome::Stored
    );
    assert_eq!(
        cold_cache
            .load(&identity, ByteExtent::new(2, 4).expect("valid extent"))
            .await,
        RangeCacheLookup::Hit {
            bytes: Bytes::from_static(b"cdef")
        }
    );

    let metrics = cold_cache.metrics();
    assert_eq!(metrics.cache_hits, 1);
    assert_eq!(metrics.cache_misses, 0);
    assert_eq!(metrics.cache_bytes_reused, 4);
}

#[tokio::test]
async fn shared_range_cache_isolated_from_legacy_raw_etag_keys() {
    let persistent = Arc::new(MemoryPersistentExtentCache::with_max_entries(8));
    let identity = strong_identity("https://example.test/object", "\"v1\"", 8);
    let legacy_key = ExtentCacheKey::new("https://example.test/object", Some("\"v1\"".to_string()));
    let shared_key = ExtentCacheKey::from_identity(&identity);
    let legacy_entry = ExtentCacheEntry::new(
        legacy_key.clone(),
        ByteExtent::new(0, 8).expect("valid extent"),
        Bytes::from_static(b"legacy!!"),
    )
    .expect("valid legacy entry");
    persistent
        .store(&legacy_entry)
        .await
        .expect("legacy store should succeed");
    let cache = SharedRangeCache::with_persistent_cache(persistent);

    assert_ne!(shared_key, legacy_key);
    assert_eq!(
        cache
            .load(&identity, ByteExtent::new(0, 4).expect("valid extent"))
            .await,
        RangeCacheLookup::Miss
    );
    assert_eq!(cache.metrics().cache_misses, 1);
}

#[tokio::test]
async fn shared_range_cache_failures_are_metric_bearing_and_fail_open() {
    let identity = strong_identity("https://example.test/object", "\"v1\"", 4);
    let cache = SharedRangeCache::with_persistent_cache(Arc::new(FailingPersistentCache));
    let extent = ByteExtent::new(0, 4).expect("valid extent");

    assert_eq!(cache.load(&identity, extent).await, RangeCacheLookup::Miss);
    assert_eq!(
        cache
            .store(&identity, extent, Bytes::from_static(b"abcd"))
            .await,
        RangeCacheStoreOutcome::CacheError
    );
    assert_eq!(
        cache.load(&identity, extent).await,
        RangeCacheLookup::Hit {
            bytes: Bytes::from_static(b"abcd")
        }
    );

    let metrics = cache.metrics();
    assert_eq!(metrics.cache_hits, 1);
    assert_eq!(metrics.cache_misses, 1);
    assert_eq!(metrics.cache_errors, 2);
    assert_eq!(metrics.cache_bytes_reused, 4);
}

#[tokio::test]
async fn shared_range_cache_slice_failures_become_metric_bearing_misses() {
    let identity = strong_identity("https://example.test/object", "\"v1\"", 4);
    let cache = SharedRangeCache::with_persistent_cache(Arc::new(UnsatisfyingPersistentCache));

    assert_eq!(
        cache
            .load(&identity, ByteExtent::new(0, 4).expect("valid extent"))
            .await,
        RangeCacheLookup::Miss
    );
    assert_eq!(cache.metrics().cache_misses, 1);
    assert_eq!(cache.metrics().cache_errors, 1);
}

#[tokio::test]
async fn shared_range_cache_malformed_persistent_entries_fail_open() {
    let identity = strong_identity("https://example.test/object", "\"v1\"", 4);
    let cache = SharedRangeCache::with_persistent_cache(Arc::new(MalformedPersistentCache));

    assert_eq!(
        cache
            .load(&identity, ByteExtent::new(0, 4).expect("valid extent"))
            .await,
        RangeCacheLookup::Miss
    );
    assert_eq!(cache.metrics().cache_misses, 1);
    assert_eq!(cache.metrics().cache_errors, 1);
}
