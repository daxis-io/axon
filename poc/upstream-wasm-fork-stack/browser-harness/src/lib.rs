#[cfg(any(test, target_arch = "wasm32"))]
use arrow_array::{Array, Int64Array, StringArray};
#[cfg(any(test, target_arch = "wasm32"))]
use arrow_ipc::reader::StreamReader;
#[cfg(any(test, target_arch = "wasm32"))]
use browser_sdk::{ArrowIpcFormat, ArrowIpcResultEnvelope};
#[cfg(any(test, target_arch = "wasm32"))]
use deltalake_browser::{BrowserQueryResult, MAX_IPC_RESULT_BYTES};
#[cfg(any(test, target_arch = "wasm32"))]
use query_contract::ExecutionTarget;

#[cfg(any(test, target_arch = "wasm32"))]
#[derive(Debug)]
struct AxonPocQueryResult {
    executed_on: ExecutionTarget,
    snapshot_version: i64,
    row_count: usize,
    bytes_fetched: u64,
    request_count: u64,
    result: ArrowIpcResultEnvelope,
}

#[cfg(any(test, target_arch = "wasm32"))]
fn adapt_query_result(
    snapshot_version: i64,
    result: BrowserQueryResult,
) -> Result<AxonPocQueryResult, String> {
    if result.ipc_stream.len() > MAX_IPC_RESULT_BYTES {
        return Err(format!(
            "Arrow IPC result is {} bytes, exceeding the {}-byte browser POC budget",
            result.ipc_stream.len(),
            MAX_IPC_RESULT_BYTES
        ));
    }

    Ok(AxonPocQueryResult {
        executed_on: ExecutionTarget::BrowserWasm,
        snapshot_version,
        row_count: result.row_count,
        bytes_fetched: result.bytes_fetched,
        request_count: result.request_count,
        result: ArrowIpcResultEnvelope::new(ArrowIpcFormat::Stream, result.ipc_stream),
    })
}

#[cfg(any(test, target_arch = "wasm32"))]
fn canonical_aggregate_rows(bytes: &[u8]) -> Result<String, String> {
    let reader = StreamReader::try_new(bytes, None).map_err(|error| error.to_string())?;
    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|error| error.to_string())?;
        if batch.num_columns() != 2
            || batch.schema().field(0).name() != "category"
            || batch.schema().field(1).name() != "total"
        {
            return Err(format!(
                "expected aggregate IPC schema category,total; received {}",
                batch.schema()
            ));
        }
        let categories = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| "aggregate IPC category column is not Utf8".to_string())?;
        let totals = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| "aggregate IPC total column is not Int64".to_string())?;
        for index in 0..batch.num_rows() {
            if categories.is_null(index) || totals.is_null(index) {
                return Err("aggregate IPC unexpectedly contains a null row".to_string());
            }
            rows.push(format!(
                "{}={}",
                categories.value(index),
                totals.value(index)
            ));
        }
    }
    Ok(rows.join(","))
}

#[cfg(target_arch = "wasm32")]
fn ipc_row_count(bytes: &[u8]) -> Result<usize, String> {
    StreamReader::try_new(bytes, None)
        .map_err(|error| error.to_string())?
        .try_fold(0_usize, |rows, batch| {
            batch
                .map(|batch| rows.saturating_add(batch.num_rows()))
                .map_err(|error| error.to_string())
        })
}

#[cfg(target_arch = "wasm32")]
mod wasm {
    use std::sync::Arc;

    use js_sys::Uint8Array;
    use object_store::http::HttpBuilder;
    use object_store::path::Path;
    use object_store::{ClientOptions, ObjectStore, ObjectStoreExt, RetryConfig};
    use url::Url;
    use wasm_bindgen::prelude::*;

    use deltalake_browser::BrowserDeltaTable;

    use super::{
        AxonPocQueryResult, adapt_query_result, canonical_aggregate_rows, ipc_row_count,
    };

    #[wasm_bindgen]
    pub struct PocBrowserTable {
        table: BrowserDeltaTable,
    }

    #[wasm_bindgen]
    impl PocBrowserTable {
        #[wasm_bindgen(js_name = open)]
        pub async fn open(
            table_root: String,
            max_full_object_fallback_bytes: u32,
        ) -> Result<PocBrowserTable, JsValue> {
            let table_root = Url::parse(&table_root).map_err(js_error)?;
            let store = http_store(&table_root, max_full_object_fallback_bytes)?;
            let table = BrowserDeltaTable::open(table_root, store)
                .await
                .map_err(js_error)?;
            Ok(Self { table })
        }

        #[wasm_bindgen(getter, js_name = snapshotVersion)]
        pub fn snapshot_version(&self) -> i64 {
            self.table.snapshot_version()
        }

        pub async fn query(&self, sql: String) -> Result<PocQueryEnvelope, JsValue> {
            let result = self.table.query_ipc(&sql).await.map_err(js_error)?;
            adapt_query_result(self.table.snapshot_version(), result)
                .map(PocQueryEnvelope::from)
                .map_err(js_error)
        }
    }

    #[wasm_bindgen]
    pub struct PocQueryEnvelope {
        ipc_bytes: Uint8Array,
        row_count: u32,
        bytes_fetched: f64,
        request_count: f64,
        snapshot_version: i64,
        content_type: String,
    }

    #[wasm_bindgen]
    impl PocQueryEnvelope {
        #[wasm_bindgen(getter, js_name = ipcBytes)]
        pub fn ipc_bytes(&self) -> Uint8Array {
            self.ipc_bytes.clone()
        }

        #[wasm_bindgen(getter, js_name = ipcByteLength)]
        pub fn ipc_byte_length(&self) -> u32 {
            self.ipc_bytes.length()
        }

        #[wasm_bindgen(getter, js_name = rowCount)]
        pub fn row_count(&self) -> u32 {
            self.row_count
        }

        #[wasm_bindgen(getter, js_name = bytesFetched)]
        pub fn bytes_fetched(&self) -> f64 {
            self.bytes_fetched
        }

        #[wasm_bindgen(getter, js_name = requestCount)]
        pub fn request_count(&self) -> f64 {
            self.request_count
        }

        #[wasm_bindgen(getter, js_name = snapshotVersion)]
        pub fn snapshot_version(&self) -> i64 {
            self.snapshot_version
        }

        #[wasm_bindgen(getter, js_name = contentType)]
        pub fn content_type(&self) -> String {
            self.content_type.clone()
        }

        #[wasm_bindgen(getter, js_name = executedOn)]
        pub fn executed_on(&self) -> String {
            "browser_wasm".to_string()
        }

        #[wasm_bindgen(getter, js_name = usedNativeFallback)]
        pub fn used_native_fallback(&self) -> bool {
            false
        }
    }

    impl From<AxonPocQueryResult> for PocQueryEnvelope {
        fn from(value: AxonPocQueryResult) -> Self {
            debug_assert_eq!(
                value.executed_on,
                query_contract::ExecutionTarget::BrowserWasm
            );
            let bytes = value
                .result
                .bytes
                .expect("single-buffer Arrow IPC envelope must carry bytes");
            let length = u32::try_from(bytes.len())
                .expect("the 8 MiB result budget fits in a JavaScript typed array");
            let ipc_bytes = Uint8Array::new_with_length(length);
            ipc_bytes.copy_from(bytes.as_ref());
            Self {
                ipc_bytes,
                row_count: u32::try_from(value.row_count).unwrap_or(u32::MAX),
                bytes_fetched: value.bytes_fetched as f64,
                request_count: value.request_count as f64,
                snapshot_version: value.snapshot_version,
                content_type: value.result.content_type,
            }
        }
    }

    #[wasm_bindgen(js_name = validateIpcRows)]
    pub fn validate_ipc_rows(bytes: Uint8Array) -> Result<u32, JsValue> {
        let bytes = bytes.to_vec();
        ipc_row_count(&bytes)
            .and_then(|rows| u32::try_from(rows).map_err(|error| error.to_string()))
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = decodeAggregateRows)]
    pub fn decode_aggregate_rows(bytes: Uint8Array) -> Result<String, JsValue> {
        canonical_aggregate_rows(&bytes.to_vec()).map_err(js_error)
    }

    #[wasm_bindgen(js_name = probeRange)]
    pub async fn probe_range(
        object_url: String,
        max_full_object_fallback_bytes: u32,
    ) -> Result<Uint8Array, JsValue> {
        let object_url = Url::parse(&object_url).map_err(js_error)?;
        let path = Path::from_url_path(object_url.path()).map_err(js_error)?;
        let store = http_store(&object_url, max_full_object_fallback_bytes)?;
        let bytes = store.get_range(&path, 1..5).await.map_err(js_error)?;
        Ok(Uint8Array::from(bytes.as_ref()))
    }

    #[wasm_bindgen(js_name = probeStream)]
    pub async fn probe_stream(object_url: String) -> Result<Uint8Array, JsValue> {
        let object_url = Url::parse(&object_url).map_err(js_error)?;
        let path = Path::from_url_path(object_url.path()).map_err(js_error)?;
        let store = http_store(&object_url, 0)?;
        let bytes = store
            .get(&path)
            .await
            .map_err(js_error)?
            .bytes()
            .await
            .map_err(js_error)?;
        Ok(Uint8Array::from(bytes.as_ref()))
    }

    fn http_store(
        object_url: &Url,
        max_full_object_fallback_bytes: u32,
    ) -> Result<Arc<dyn ObjectStore>, JsValue> {
        let mut origin = object_url.clone();
        origin.set_path("/");
        origin.set_query(None);
        origin.set_fragment(None);
        let mut retry = RetryConfig::default();
        retry.max_retries = 3;
        let mut builder = HttpBuilder::new()
            .with_url(origin.as_str())
            .with_client_options(ClientOptions::new().with_allow_http(true))
            .with_retry(retry);
        if max_full_object_fallback_bytes != 0 {
            builder = builder
                .with_max_full_object_fallback_size(u64::from(max_full_object_fallback_bytes));
        }
        builder
            .build()
            .map(|store| Arc::new(store) as Arc<dyn ObjectStore>)
            .map_err(js_error)
    }

    fn js_error(error: impl std::fmt::Display) -> JsValue {
        JsValue::from_str(&error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
    use arrow_ipc::writer::StreamWriter;
    use bytes::Bytes;

    use super::*;

    #[test]
    fn adapter_uses_the_existing_axon_arrow_ipc_envelope() {
        let result = adapt_query_result(
            0,
            BrowserQueryResult {
                ipc_stream: vec![1, 2, 3, 4],
                row_count: 2,
                bytes_fetched: 4096,
                request_count: 3,
            },
        )
        .unwrap();

        assert_eq!(result.executed_on, ExecutionTarget::BrowserWasm);
        assert_eq!(result.snapshot_version, 0);
        assert_eq!(result.row_count, 2);
        assert_eq!(result.bytes_fetched, 4096);
        assert_eq!(result.request_count, 3);
        assert_eq!(
            result.result.content_type,
            browser_sdk::ARROW_IPC_STREAM_CONTENT_TYPE
        );
        assert_eq!(result.result.bytes, Some(Bytes::from_static(&[1, 2, 3, 4])));
        assert_eq!(result.result.byte_length, 4);
    }

    #[test]
    fn adapter_rejects_results_over_the_axon_budget() {
        let error = adapt_query_result(
            0,
            BrowserQueryResult {
                ipc_stream: vec![0; MAX_IPC_RESULT_BYTES + 1],
                row_count: 0,
                bytes_fetched: 0,
                request_count: 0,
            },
        )
        .unwrap_err();
        assert!(error.contains("8"));
        assert!(error.contains("browser POC budget"));
    }

    #[test]
    fn aggregate_ipc_decoder_returns_canonical_rows() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "category",
                Arc::new(StringArray::from(vec!["alpha", "beta"])) as Arc<dyn Array>,
            ),
            (
                "total",
                Arc::new(Int64Array::from(vec![7, 10])) as Arc<dyn Array>,
            ),
        ])
        .unwrap();
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, &batch.schema()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        assert_eq!(canonical_aggregate_rows(&ipc).unwrap(), "alpha=7,beta=10");
    }
}
