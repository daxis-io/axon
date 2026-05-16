use std::sync::{Arc, Mutex};

use bytes::Bytes;
use query_contract::QueryError;
use wasm_http_object_store::{
    BrowserCacheMode, BrowserLocalObject, BrowserObject, BrowserObjectRangeReader, ByteExtent,
    ExtentCacheEntry, ExtentCacheKey, MemoryPersistentExtentCache, PersistentCacheFuture,
    PersistentExtentCache,
};

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
        reader.cached_extents_for_testing()[0]
            .key
            .identity
            .as_deref(),
        Some("v2")
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
