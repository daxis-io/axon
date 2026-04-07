use bytes::Bytes;
use wasm_http_object_store::{
    BrowserLocalObject, BrowserObject, BrowserObjectRangeReader, ByteExtent,
};

#[tokio::test]
async fn local_file_adapter_exposes_range_reads_from_blob_like_sources() {
    let mut reader = BrowserObjectRangeReader::new();
    let object = BrowserObject::local(BrowserLocalObject::from_bytes(
        "blob:fixture",
        Bytes::from_static(b"abcdefghij"),
    ));

    let result = reader
        .read_extent(&object, ByteExtent::new(2, 4).expect("valid extent"), None)
        .await
        .expect("local blob-like objects should use the same extent seam");

    assert_eq!(result.metadata.resource, "blob:fixture");
    assert_eq!(result.metadata.size_bytes, 10);
    assert!(!result.metadata.identity.is_empty());
    assert_eq!(result.bytes.as_ref(), b"cdef");
}

#[tokio::test]
async fn local_file_default_identity_distinguishes_distinct_contents() {
    let mut reader = BrowserObjectRangeReader::new();
    let first = BrowserObject::local(BrowserLocalObject::from_bytes(
        "blob:fixture",
        Bytes::from_static(b"abcd1111wxyz"),
    ));
    let second = BrowserObject::local(BrowserLocalObject::from_bytes(
        "blob:fixture",
        Bytes::from_static(b"abcd2222wxyz"),
    ));

    let original = reader
        .read_extent(&first, ByteExtent::new(4, 4).expect("valid extent"), None)
        .await
        .expect("first local object read should succeed");
    let updated = reader
        .read_extent(&second, ByteExtent::new(4, 4).expect("valid extent"), None)
        .await
        .expect("distinct local contents must not reuse stale cached extents");

    assert_eq!(original.bytes.as_ref(), b"1111");
    assert_eq!(updated.bytes.as_ref(), b"2222");
}
