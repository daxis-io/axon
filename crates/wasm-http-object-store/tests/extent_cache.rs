use bytes::Bytes;
use wasm_http_object_store::{
    coalesce_extents, BrowserLocalObject, BrowserObject, BrowserObjectRangeReader, ByteExtent,
    ExtentCacheEntry, ExtentCacheKey,
};

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
    assert_eq!(reader.cached_extents().len(), 1);
    assert_eq!(
        reader.cached_extents()[0].extent,
        ByteExtent::new(0, 8).expect("valid extent")
    );
    assert_eq!(reader.metrics().bytes_fetched, 8);
    assert_eq!(reader.metrics().bytes_reused, 4);
}
