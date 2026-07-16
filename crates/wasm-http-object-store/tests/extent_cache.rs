use bytes::Bytes;
use wasm_http_object_store::{
    coalesce_extents, BrowserLocalObject, BrowserObject, BrowserObjectRangeReader, ByteExtent,
    ExtentCacheEntry, ExtentCacheKey, MemoryRangeCache, RangeCacheIdentity, RangeCacheLookup,
    RangeCacheStoreOutcome,
};

fn strong_identity(resource: &str, etag: &str, size_bytes: u64) -> RangeCacheIdentity {
    RangeCacheIdentity::strong(resource, etag, size_bytes)
        .expect("quoted strong ETag should form a cache identity")
}

#[tokio::test]
async fn coalesces_adjacent_ranges_before_fetch() {
    let merged = coalesce_extents(vec![
        ByteExtent::new(64, 8).expect("valid extent"),
        ByteExtent::new(72, 8).expect("valid extent"),
        ByteExtent::new(0, 16).expect("valid extent"),
        ByteExtent::new(8, 8).expect("valid extent"),
    ])
    .expect("coalescing should succeed");

    assert_eq!(
        merged,
        vec![
            ByteExtent::new(0, 16).expect("valid extent"),
            ByteExtent::new(64, 16).expect("valid extent"),
        ]
    );
}

#[tokio::test]
async fn extent_cache_keys_are_resource_and_identity_scoped() {
    let key = ExtentCacheKey::new("https://example.com/object", Some("\"v1\"".to_string()));
    let entry = ExtentCacheEntry::new(
        key.clone(),
        ByteExtent::new(0, 8).expect("valid extent"),
        Bytes::from_static(b"abcdefgh"),
    )
    .expect("entry should be valid");

    assert!(entry.can_satisfy(&key, ByteExtent::new(2, 4).expect("valid extent"),));
    assert!(!entry.can_satisfy(
        &ExtentCacheKey::new("https://example.com/object", Some("\"v2\"".to_string())),
        ByteExtent::new(2, 4).expect("valid extent"),
    ));
}

#[tokio::test]
async fn extent_cache_coalesces_adjacent_ranges_and_reuses_subranges() {
    let local = BrowserLocalObject::from_bytes("blob:fixture", Bytes::from_static(b"abcdefgh"));
    let object = BrowserObject::local(local);
    let mut reader = BrowserObjectRangeReader::new();

    let first = reader
        .read_extent(&object, ByteExtent::new(0, 4).expect("valid extent"), None)
        .await
        .expect("first extent read should succeed");
    let second = reader
        .read_extent(
            &object,
            ByteExtent::new(4, 4).expect("valid extent"),
            Some(first.metadata.clone()),
        )
        .await
        .expect("adjacent extent read should succeed");
    let reused = reader
        .read_extent(
            &object,
            ByteExtent::new(2, 4).expect("valid extent"),
            Some(second.metadata.clone()),
        )
        .await
        .expect("subrange should be served from the merged extent cache");

    assert_eq!(reused.bytes.as_ref(), b"cdef");
    assert_eq!(reader.cached_extents_for_testing().len(), 1);
    assert_eq!(
        reader.cached_extents_for_testing()[0].extent,
        ByteExtent::new(0, 8).expect("valid extent")
    );
    assert_eq!(reader.metrics().bytes_fetched, 8);
    assert_eq!(reader.metrics().bytes_reused, 4);
}

#[test]
fn strong_range_cache_identity_rejects_invalid_inputs() {
    assert!(RangeCacheIdentity::strong("", "\"v1\"", 8).is_none());
    assert!(RangeCacheIdentity::strong("https://example.com/object", "", 8).is_none());
    assert!(RangeCacheIdentity::strong("https://example.com/object", "W/\"v1\"", 8).is_none());
    assert!(RangeCacheIdentity::strong("https://example.com/object", "v1", 8).is_none());
    assert!(RangeCacheIdentity::strong("https://example.com/object", "\"\"", 8).is_none());
}

#[test]
fn memory_range_cache_reuses_identity_scoped_containing_subranges() {
    let identity = strong_identity("https://example.com/object", "\"v1\"", 8);
    let other_resource = strong_identity("https://example.com/other", "\"v1\"", 8);
    let cache = MemoryRangeCache::new();

    assert_eq!(
        cache.store(
            &identity,
            ByteExtent::new(0, 8).expect("valid extent"),
            Bytes::from_static(b"abcdefgh"),
        ),
        RangeCacheStoreOutcome::Stored
    );
    assert_eq!(
        cache.load(&identity, ByteExtent::new(2, 4).expect("valid extent")),
        RangeCacheLookup::Hit {
            bytes: Bytes::from_static(b"cdef")
        }
    );
    assert_eq!(
        cache.load(
            &other_resource,
            ByteExtent::new(2, 4).expect("valid extent")
        ),
        RangeCacheLookup::Miss
    );
}

#[test]
fn memory_range_cache_with_max_entries_evicts_the_oldest_extent() {
    let first_identity = strong_identity("https://example.com/first", "\"v1\"", 8);
    let second_identity = strong_identity("https://example.com/second", "\"v1\"", 8);
    let cache = MemoryRangeCache::with_max_entries(1);
    let extent = ByteExtent::new(0, 4).expect("valid extent");

    assert_eq!(
        cache.store(&first_identity, extent, Bytes::from_static(b"old!")),
        RangeCacheStoreOutcome::Stored
    );
    assert_eq!(
        cache.store(&second_identity, extent, Bytes::from_static(b"new!")),
        RangeCacheStoreOutcome::Stored
    );

    assert_eq!(cache.load(&first_identity, extent), RangeCacheLookup::Miss);
    assert_eq!(
        cache.load(&second_identity, extent),
        RangeCacheLookup::Hit {
            bytes: Bytes::from_static(b"new!")
        }
    );
}

#[test]
fn memory_range_cache_with_max_entries_refreshes_recently_used_extents() {
    let first_identity = strong_identity("https://example.com/first", "\"v1\"", 8);
    let second_identity = strong_identity("https://example.com/second", "\"v1\"", 8);
    let third_identity = strong_identity("https://example.com/third", "\"v1\"", 8);
    let cache = MemoryRangeCache::with_max_entries(2);
    let extent = ByteExtent::new(0, 4).expect("valid extent");

    assert_eq!(
        cache.store(&first_identity, extent, Bytes::from_static(b"one!")),
        RangeCacheStoreOutcome::Stored
    );
    assert_eq!(
        cache.store(&second_identity, extent, Bytes::from_static(b"two!")),
        RangeCacheStoreOutcome::Stored
    );
    assert!(matches!(
        cache.load(&first_identity, extent),
        RangeCacheLookup::Hit { .. }
    ));
    assert_eq!(
        cache.store(&third_identity, extent, Bytes::from_static(b"tri!")),
        RangeCacheStoreOutcome::Stored
    );

    assert_eq!(cache.load(&second_identity, extent), RangeCacheLookup::Miss);
    assert!(matches!(
        cache.load(&first_identity, extent),
        RangeCacheLookup::Hit { .. }
    ));
    assert!(matches!(
        cache.load(&third_identity, extent),
        RangeCacheLookup::Hit { .. }
    ));
}

#[test]
fn memory_range_cache_evicts_etag_and_size_drift() {
    let identity_v1 = strong_identity("https://example.com/object", "\"v1\"", 8);
    let identity_v2 = strong_identity("https://example.com/object", "\"v2\"", 8);
    let identity_size_drift = strong_identity("https://example.com/object", "\"v2\"", 9);
    let cache = MemoryRangeCache::new();
    let extent = ByteExtent::new(0, 4).expect("valid extent");

    assert_eq!(
        cache.store(&identity_v1, extent, Bytes::from_static(b"old!")),
        RangeCacheStoreOutcome::Stored
    );
    assert_eq!(cache.load(&identity_v2, extent), RangeCacheLookup::Miss);
    assert_eq!(cache.load(&identity_v1, extent), RangeCacheLookup::Miss);

    assert_eq!(
        cache.store(&identity_v2, extent, Bytes::from_static(b"new!")),
        RangeCacheStoreOutcome::Stored
    );
    assert_eq!(
        cache.load(&identity_size_drift, extent),
        RangeCacheLookup::Miss
    );
    assert_eq!(cache.load(&identity_v2, extent), RangeCacheLookup::Miss);

    assert_eq!(cache.metrics().validation_misses, 2);
}

#[test]
fn memory_range_cache_hits_do_not_change_network_bytes() {
    let identity = strong_identity("https://example.com/object", "\"v1\"", 8);
    let cache = MemoryRangeCache::new();
    cache.record_network_bytes_fetched(8);
    assert_eq!(
        cache.store(
            &identity,
            ByteExtent::new(0, 8).expect("valid extent"),
            Bytes::from_static(b"abcdefgh"),
        ),
        RangeCacheStoreOutcome::Stored
    );

    assert!(matches!(
        cache.load(&identity, ByteExtent::new(2, 4).expect("valid extent")),
        RangeCacheLookup::Hit { .. }
    ));
    assert_eq!(cache.metrics().network_bytes_fetched, 8);
}

#[test]
fn memory_range_cache_reports_hit_miss_reuse_and_validation_metrics() {
    let identity_v1 = strong_identity("https://example.com/object", "\"v1\"", 8);
    let identity_v2 = strong_identity("https://example.com/object", "\"v2\"", 8);
    let cache = MemoryRangeCache::new();

    assert_eq!(
        cache.store(
            &identity_v1,
            ByteExtent::new(0, 8).expect("valid extent"),
            Bytes::from_static(b"abcdefgh"),
        ),
        RangeCacheStoreOutcome::Stored
    );
    assert!(matches!(
        cache.load(&identity_v1, ByteExtent::new(2, 4).expect("valid extent")),
        RangeCacheLookup::Hit { .. }
    ));
    assert_eq!(
        cache.load(&identity_v1, ByteExtent::new(8, 1).expect("valid extent")),
        RangeCacheLookup::Miss
    );
    assert_eq!(
        cache.load(&identity_v2, ByteExtent::new(0, 4).expect("valid extent")),
        RangeCacheLookup::Miss
    );

    let metrics = cache.metrics();
    assert_eq!(metrics.cache_hits, 1);
    assert_eq!(metrics.cache_misses, 2);
    assert_eq!(metrics.cache_bytes_reused, 4);
    assert_eq!(metrics.validation_misses, 1);
    assert_eq!(metrics.cache_errors, 0);
}

#[test]
fn memory_range_cache_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<MemoryRangeCache>();
}

#[test]
fn observed_lookup_reports_only_this_calls_hit_and_validation_rejections() {
    let cache = MemoryRangeCache::new();
    let old = strong_identity("https://example.test/data", "\"old\"", 4);
    let new = strong_identity("https://example.test/data", "\"new\"", 4);
    let extent = ByteExtent::new(0, 4).expect("valid extent");
    cache.store(&old, extent, Bytes::from_static(b"old!"));

    let miss = cache.load_observed(&new, extent);
    assert_eq!(miss.lookup, RangeCacheLookup::Miss);
    assert_eq!(miss.validation_misses, 1);
    assert_eq!(miss.bytes_reused, 0);

    cache.store(&new, extent, Bytes::from_static(b"new!"));
    let hit = cache.load_observed(&new, extent);
    assert_eq!(
        hit.lookup,
        RangeCacheLookup::Hit {
            bytes: Bytes::from_static(b"new!")
        }
    );
    assert_eq!(hit.validation_misses, 0);
    assert_eq!(hit.bytes_reused, 4);
}

#[test]
fn observed_store_reports_accepted_response_bytes_for_only_this_call() {
    let cache = MemoryRangeCache::new();
    let old_identity = strong_identity("https://example.test/data", "\"v1\"", 4);
    let new_identity = strong_identity("https://example.test/data", "\"v2\"", 4);
    let extent = ByteExtent::new(0, 4).expect("valid extent");

    cache.store(&old_identity, extent, Bytes::from_static(b"old!"));
    let stored = cache.store_observed(&new_identity, extent, Bytes::from_static(b"new!"));
    assert_eq!(stored.bytes_stored, 4);
    assert_eq!(stored.validation_misses, 1);
}
