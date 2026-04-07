#![cfg(target_arch = "wasm32")]

use std::sync::Arc;

use query_contract::QueryError;
use wasm_bindgen_test::wasm_bindgen_test;
use wasm_http_object_store::{
    BrowserCacheMode, BrowserObjectRangeReader, ByteExtent, ExtentCacheEntry, ExtentCacheKey,
    PersistentExtentCache,
};

#[derive(Default)]
struct NoopPersistentCache;

impl PersistentExtentCache for NoopPersistentCache {
    fn load(
        &self,
        _key: &ExtentCacheKey,
        _requested_extent: ByteExtent,
    ) -> Result<Option<ExtentCacheEntry>, QueryError> {
        Ok(None)
    }

    fn store(&self, _entry: &ExtentCacheEntry) -> Result<(), QueryError> {
        Ok(())
    }
}

#[wasm_bindgen_test]
fn browser_local_cache_smoke_reports_memory_only_mode_by_default() {
    let reader = BrowserObjectRangeReader::new();

    assert_eq!(reader.cache_mode(), BrowserCacheMode::MemoryOnly);
}

#[wasm_bindgen_test]
fn browser_local_cache_smoke_reports_persistent_cache_mode() {
    let reader = BrowserObjectRangeReader::with_persistent_cache(Arc::new(NoopPersistentCache));

    assert_eq!(reader.cache_mode(), BrowserCacheMode::Persistent);
}
