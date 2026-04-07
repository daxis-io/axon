use std::sync::{Arc, Mutex};

use bytes::Bytes;
use wasm_http_object_store::{
    BrowserCacheMode, BrowserLocalObject, BrowserObject, BrowserObjectRangeReader, ByteExtent,
    ExtentCacheEntry, ExtentCacheKey, PersistentExtentCache,
};

#[derive(Default)]
struct RecordingPersistentCache {
    entries: Mutex<Vec<ExtentCacheEntry>>,
}

impl PersistentExtentCache for RecordingPersistentCache {
    fn load(
        &self,
        key: &ExtentCacheKey,
        requested_extent: ByteExtent,
    ) -> Result<Option<ExtentCacheEntry>, query_contract::QueryError> {
        let entries = self
            .entries
            .lock()
            .expect("cache lock should not be poisoned");
        Ok(entries
            .iter()
            .find(|entry| entry.can_satisfy(key, requested_extent))
            .cloned())
    }

    fn store(&self, entry: &ExtentCacheEntry) -> Result<(), query_contract::QueryError> {
        self.entries
            .lock()
            .expect("cache lock should not be poisoned")
            .push(entry.clone());
        Ok(())
    }
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
    assert_eq!(reader.cached_extents().len(), 1);
    assert_eq!(
        reader.cached_extents()[0].key.identity.as_deref(),
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
