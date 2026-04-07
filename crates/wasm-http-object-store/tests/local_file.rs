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
