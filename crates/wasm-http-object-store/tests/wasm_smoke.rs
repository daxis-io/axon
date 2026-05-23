#![cfg(target_arch = "wasm32")]

use std::sync::Arc;

use bytes::Bytes;
use wasm_bindgen::JsValue;
use wasm_bindgen_test::wasm_bindgen_test;
use wasm_http_object_store::{
    BrowserCacheMode, BrowserObjectRangeReader, ByteExtent, ExtentCacheEntry, ExtentCacheKey,
    HttpByteRange, HttpMetadataProbeRequirements, HttpRangeReader, OpfsPersistentExtentCache,
    PersistentCacheFuture, PersistentExtentCache,
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

#[wasm_bindgen_test(async)]
async fn blob_url_metadata_probe_handles_zero_byte_objects() {
    let url = blob_url_from_bytes(&[]);
    let metadata = HttpRangeReader::new()
        .probe_metadata(
            &url,
            HttpMetadataProbeRequirements {
                require_size: true,
                require_etag: false,
            },
        )
        .await
        .expect("zero-byte blob URL metadata should resolve");
    revoke_blob_url(&url);

    assert_eq!(metadata.size_bytes, Some(0));
    assert_eq!(metadata.etag, None);
}

#[wasm_bindgen_test(async)]
async fn blob_url_range_read_falls_back_when_browser_ignores_range_header() {
    let offset = 512 * 1024_u64;
    let mut payload = vec![b'x'; offset as usize + 512 * 1024];
    payload[offset as usize..offset as usize + 3].copy_from_slice(b"cde");
    let url = blob_url_from_bytes(&payload);
    install_fetch_that_ignores_blob_range(&url);

    let result = HttpRangeReader::new()
        .read_range(&url, HttpByteRange::Bounded { offset, length: 3 })
        .await
        .expect("blob URL range read should fall back when fetch ignores Range");

    restore_fetch_mock();
    revoke_blob_url(&url);

    assert_eq!(result.metadata.size_bytes, Some(payload.len() as u64));
    assert_eq!(result.metadata.etag, None);
    let bytes = result.bytes;
    assert_eq!(bytes.as_ref(), b"cde");
    let bytes = bytes
        .try_into_mut()
        .expect("fallback range bytes should be uniquely owned");
    assert_eq!(
        bytes.capacity(),
        bytes.len(),
        "blob URL range fallback should not retain unused full-response capacity"
    );
}

#[wasm_bindgen_test(async)]
async fn blob_url_metadata_probe_falls_back_when_browser_ignores_range_header() {
    let url = blob_url_from_bytes(b"abcdef");
    install_fetch_that_ignores_blob_range(&url);

    let metadata = HttpRangeReader::new()
        .probe_metadata(
            &url,
            HttpMetadataProbeRequirements {
                require_size: true,
                require_etag: false,
            },
        )
        .await
        .expect("blob URL metadata probe should fall back when fetch ignores Range");

    restore_fetch_mock();
    revoke_blob_url(&url);

    assert_eq!(metadata.size_bytes, Some(6));
    assert_eq!(metadata.etag, None);
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

fn blob_url_from_bytes(bytes: &[u8]) -> String {
    let bytes = js_sys::Uint8Array::from(bytes);
    js_sys::Function::new_with_args("bytes", "return URL.createObjectURL(new Blob([bytes]));")
        .call1(&JsValue::UNDEFINED, &bytes)
        .expect("blob URL should be created")
        .as_string()
        .expect("blob URL should be a string")
}

fn revoke_blob_url(url: &str) {
    js_sys::Function::new_with_args("url", "URL.revokeObjectURL(url);")
        .call1(&JsValue::UNDEFINED, &JsValue::from_str(url))
        .expect("blob URL should be revoked");
}

fn install_fetch_that_ignores_blob_range(url: &str) {
    js_sys::Function::new_with_args(
        "url",
        r#"
        if (!globalThis.__axonOriginalFetch) {
          globalThis.__axonOriginalFetch = globalThis.fetch;
        }
        globalThis.fetch = (input, init) => {
          const requestUrl = typeof input === 'string' ? input : input?.url;
          if (requestUrl === url && init?.headers && new Headers(init.headers).has('range')) {
            return globalThis.__axonOriginalFetch.call(globalThis, input);
          }
          return globalThis.__axonOriginalFetch.call(globalThis, input, init);
        };
        "#,
    )
    .call1(&JsValue::UNDEFINED, &JsValue::from_str(url))
    .expect("fetch mock should be installed");
}

fn restore_fetch_mock() {
    js_sys::Function::new_no_args(
        r#"
        if (globalThis.__axonOriginalFetch) {
          globalThis.fetch = globalThis.__axonOriginalFetch;
          delete globalThis.__axonOriginalFetch;
        }
        "#,
    )
    .call0(&JsValue::UNDEFINED)
    .expect("fetch mock should be restored");
}
