#![cfg(target_arch = "wasm32")]

use std::sync::Arc;

use bytes::Bytes;
use wasm_bindgen::JsValue;
use wasm_bindgen_test::wasm_bindgen_test;
use wasm_http_object_store::{
    BrowserCacheMode, BrowserObjectRangeReader, ByteExtent, ExtentCacheEntry, ExtentCacheKey,
    OpfsPersistentExtentCache, PersistentCacheFuture, PersistentExtentCache,
};

#[derive(Default)]
struct NoopPersistentCache;

impl PersistentExtentCache for NoopPersistentCache {
    fn load<'a>(
        &'a self,
        _key: &ExtentCacheKey,
        _requested_extent: ByteExtent,
    ) -> PersistentCacheFuture<'a, Option<ExtentCacheEntry>> {
        Box::pin(async { Ok(None) })
    }

    fn store<'a>(&'a self, _entry: &'a ExtentCacheEntry) -> PersistentCacheFuture<'a, ()> {
        Box::pin(async { Ok(()) })
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

#[wasm_bindgen_test(async)]
async fn opfs_cache_reuses_containing_extent_from_mock_directory() {
    let cache = OpfsPersistentExtentCache::from_directory_handle(mock_opfs_directory());
    let key = ExtentCacheKey::new("https://example.test/object", Some("\"v1\"".to_string()));
    let entry = ExtentCacheEntry::new(
        key.clone(),
        ByteExtent::new(0, 8).expect("valid extent"),
        Bytes::from_static(b"abcdefgh"),
    )
    .expect("valid cache entry");

    cache.store(&entry).await.expect("store should succeed");
    let hit = cache
        .load(&key, ByteExtent::new(2, 4).expect("valid extent"))
        .await
        .expect("load should succeed")
        .expect("containing extent should satisfy subrange");

    assert_eq!(
        hit.slice(ByteExtent::new(2, 4).expect("valid extent"))
            .expect("stored extent should slice"),
        b"cdef"[..]
    );
}

#[wasm_bindgen_test(async)]
async fn opfs_cache_overwrites_matching_extent_and_evicts_oldest_extent() {
    let cache =
        OpfsPersistentExtentCache::from_directory_handle_with_max_entries(mock_opfs_directory(), 1);
    let key = ExtentCacheKey::new("https://example.test/object", Some("\"v1\"".to_string()));
    let first = ExtentCacheEntry::new(
        key.clone(),
        ByteExtent::new(0, 4).expect("valid extent"),
        Bytes::from_static(b"aaaa"),
    )
    .expect("valid cache entry");
    let second = ExtentCacheEntry::new(
        key.clone(),
        ByteExtent::new(8, 4).expect("valid extent"),
        Bytes::from_static(b"bbbb"),
    )
    .expect("valid cache entry");
    let replacement = ExtentCacheEntry::new(
        key.clone(),
        ByteExtent::new(8, 4).expect("valid extent"),
        Bytes::from_static(b"cccc"),
    )
    .expect("valid cache entry");

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
        .load(&key, ByteExtent::new(0, 4).expect("valid extent"))
        .await
        .expect("load should succeed");
    let overwritten = cache
        .load(&key, ByteExtent::new(8, 4).expect("valid extent"))
        .await
        .expect("load should succeed")
        .expect("replacement extent should remain cached");

    assert!(evicted.is_none());
    assert_eq!(overwritten.bytes.as_ref(), b"cccc");
}

fn mock_opfs_directory() -> JsValue {
    js_sys::Function::new_no_args(
        r#"
        const files = new Map();
        return {
          async getFileHandle(name, options = {}) {
            if (!files.has(name) && !options.create) {
              throw new DOMException('not found', 'NotFoundError');
            }
            if (!files.has(name)) {
              files.set(name, new Uint8Array());
            }
            return {
              async getFile() {
                return new Blob([files.get(name)]);
              },
              async createWritable() {
                return {
                  async write(chunk) {
                    files.set(name, new Uint8Array(chunk));
                  },
                  async close() {}
                };
              }
            };
          },
          async removeEntry(name) {
            files.delete(name);
          }
        };
        "#,
    )
    .call0(&JsValue::UNDEFINED)
    .expect("mock OPFS directory should be constructed")
}
