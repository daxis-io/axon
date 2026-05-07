#![cfg(not(target_arch = "wasm32"))]

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::data_type::Int64Type;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use query_contract::{ExecutionTarget, FallbackReason, QueryErrorCode};
use wasm_datafusion_poc::{
    BrowserQueryBudget, DeltaActiveFile, DeltaTableDescriptor, WasmDataFusionEngine,
};

#[tokio::test]
async fn query_budget_rejects_output_ipc_over_limit() {
    let mut engine = WasmDataFusionEngine::with_budget(BrowserQueryBudget {
        max_output_ipc_bytes: Some(128),
        ..Default::default()
    });
    let batch = controlled_batch();
    let schema = batch.schema();

    engine
        .register_record_batches("events", schema, vec![batch])
        .await
        .expect("record batches should register");

    let error = engine
        .sql_to_arrow_ipc("SELECT id, value, category FROM events ORDER BY id")
        .await
        .expect_err("output over budget should fail");

    assert_eq!(error.code, QueryErrorCode::FallbackRequired, "{error:?}");
    assert_eq!(
        error.fallback_reason,
        Some(FallbackReason::BrowserRuntimeConstraint)
    );
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
    assert!(error.message.contains("max_output_ipc_bytes"));
}

#[tokio::test]
async fn query_budget_rejects_planned_scan_over_limit() {
    let schema = descriptor_schema();
    let mut engine = WasmDataFusionEngine::with_budget(BrowserQueryBudget {
        max_scan_bytes: Some(1024),
        ..Default::default()
    });

    engine
        .open_delta_table_with_record_batch_partitions(
            delta_descriptor("events", Arc::clone(&schema)),
            controlled_partitions(schema),
        )
        .await
        .expect("descriptor-backed table should register");

    let error = engine
        .sql_to_arrow_ipc("SELECT id FROM events ORDER BY id")
        .await
        .expect_err("planned scan over budget should fail");

    assert_eq!(error.code, QueryErrorCode::FallbackRequired, "{error:?}");
    assert_eq!(
        error.fallback_reason,
        Some(FallbackReason::BrowserRuntimeConstraint)
    );
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
    assert!(error.message.contains("max_scan_bytes"));
    assert!(
        error.message.contains("3072"),
        "expected planned scan byte count in error: {}",
        error.message
    );
}

#[tokio::test]
async fn query_budget_rejects_zero_batch_budget_before_scanning() {
    let mut engine = WasmDataFusionEngine::with_budget(BrowserQueryBudget {
        max_batches_in_flight: Some(0),
        ..Default::default()
    });

    engine
        .open_delta_table(single_unreachable_parquet_descriptor())
        .await
        .expect("descriptor-backed table should register");

    let error = engine
        .sql_to_arrow_ipc("SELECT id FROM events")
        .await
        .expect_err("zero output batch budget should fail before object I/O");

    assert_eq!(error.code, QueryErrorCode::FallbackRequired);
    assert_eq!(
        error.fallback_reason,
        Some(FallbackReason::BrowserRuntimeConstraint)
    );
    assert!(error.message.contains("max_batches_in_flight"));
}

#[tokio::test]
async fn query_budget_rejects_row_budget_before_scanning_later_files() {
    let first_object = parquet_bytes_with_i64_columns(&[(1, 10), (2, 20), (3, 30)]);
    let first_object_size = u64::try_from(first_object.len()).expect("object size should fit");
    let first_server = RequestCapturingServer::new(first_object);
    let mut engine = WasmDataFusionEngine::with_budget(BrowserQueryBudget {
        max_rows_returned: Some(1),
        ..Default::default()
    });

    engine
        .open_delta_table(two_file_parquet_descriptor(
            first_server.url(),
            first_object_size,
        ))
        .await
        .expect("descriptor-backed table should register");

    let error = engine
        .sql_to_arrow_ipc("SELECT id, value FROM events")
        .await
        .expect_err("row budget should fail before scanning the unreachable second file");

    assert_eq!(error.code, QueryErrorCode::FallbackRequired);
    assert_eq!(
        error.fallback_reason,
        Some(FallbackReason::BrowserRuntimeConstraint)
    );
    assert!(error.message.contains("max_rows_returned"));
    assert!(
        !first_server.recorded_requests().is_empty(),
        "first file should be scanned before the row budget is exceeded"
    );
}

#[tokio::test]
async fn query_cancellation_reports_structured_shape() {
    let mut engine = WasmDataFusionEngine::with_budget(BrowserQueryBudget::default());
    let cancellation = engine.cancellation_token();
    let batch = controlled_batch();
    let schema = batch.schema();

    engine
        .register_record_batches("events", schema, vec![batch])
        .await
        .expect("record batches should register");
    cancellation.cancel();

    let error = engine
        .sql_to_arrow_ipc("SELECT id FROM events ORDER BY id")
        .await
        .expect_err("cancelled query should fail");

    assert_eq!(error.code, QueryErrorCode::ExecutionFailed);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
    assert!(error.message.contains("cancelled"));

    let result = engine
        .sql_to_arrow_ipc("SELECT id FROM events ORDER BY id")
        .await
        .expect("a cancelled query should not poison later queries");
    assert!(!result.is_empty());
}

fn controlled_batch() -> RecordBatch {
    RecordBatch::try_new(
        descriptor_schema(),
        vec![
            Arc::new(Int32Array::from(vec![3, 1, 4, 2, 5])),
            Arc::new(Int32Array::from(vec![25, 5, 20, 12, 8])),
            Arc::new(StringArray::from(vec!["B", "A", "C", "B", "B"])),
        ],
    )
    .expect("controlled batch should construct")
}

fn controlled_partitions(schema: SchemaRef) -> Vec<Vec<RecordBatch>> {
    vec![
        vec![RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![3, 1])),
                Arc::new(Int32Array::from(vec![25, 5])),
                Arc::new(StringArray::from(vec!["B", "A"])),
            ],
        )
        .expect("first scan partition should construct")],
        vec![RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![4, 2, 5])),
                Arc::new(Int32Array::from(vec![20, 12, 8])),
                Arc::new(StringArray::from(vec!["C", "B", "B"])),
            ],
        )
        .expect("second scan partition should construct")],
    ]
}

fn delta_descriptor(table_name: &str, schema: SchemaRef) -> DeltaTableDescriptor {
    DeltaTableDescriptor {
        table_name: table_name.to_string(),
        table_version: 9,
        schema,
        partition_columns: Vec::new(),
        partition_column_types: BTreeMap::new(),
        active_files: vec![
            DeltaActiveFile {
                path: "part-000.parquet".to_string(),
                url: "https://example.test/table/part-000.parquet".to_string(),
                size_bytes: 1024,
                partition_values: BTreeMap::new(),
                object_etag: Some("\"etag-1\"".to_string()),
                stats_json: Some(r#"{"numRecords":2}"#.to_string()),
                deletion_vector: None,
            },
            DeltaActiveFile {
                path: "part-001.parquet".to_string(),
                url: "https://example.test/table/part-001.parquet".to_string(),
                size_bytes: 2048,
                partition_values: BTreeMap::new(),
                object_etag: Some("\"etag-2\"".to_string()),
                stats_json: Some(r#"{"numRecords":3}"#.to_string()),
                deletion_vector: None,
            },
        ],
    }
}

fn single_unreachable_parquet_descriptor() -> DeltaTableDescriptor {
    DeltaTableDescriptor {
        table_name: "events".to_string(),
        table_version: 9,
        schema: i64_descriptor_schema(),
        partition_columns: Vec::new(),
        partition_column_types: BTreeMap::new(),
        active_files: vec![DeltaActiveFile {
            path: "part-000.parquet".to_string(),
            url: "http://127.0.0.1:9/part-000.parquet".to_string(),
            size_bytes: 1024,
            partition_values: BTreeMap::new(),
            object_etag: None,
            stats_json: Some(r#"{"numRecords":1}"#.to_string()),
            deletion_vector: None,
        }],
    }
}

fn two_file_parquet_descriptor(first_url: String, first_size_bytes: u64) -> DeltaTableDescriptor {
    DeltaTableDescriptor {
        table_name: "events".to_string(),
        table_version: 9,
        schema: i64_descriptor_schema(),
        partition_columns: Vec::new(),
        partition_column_types: BTreeMap::new(),
        active_files: vec![
            DeltaActiveFile {
                path: "part-000.parquet".to_string(),
                url: first_url,
                size_bytes: first_size_bytes,
                partition_values: BTreeMap::new(),
                object_etag: None,
                stats_json: Some(r#"{"numRecords":3}"#.to_string()),
                deletion_vector: None,
            },
            DeltaActiveFile {
                path: "part-001.parquet".to_string(),
                url: "http://127.0.0.1:9/part-001.parquet".to_string(),
                size_bytes: 1024,
                partition_values: BTreeMap::new(),
                object_etag: None,
                stats_json: Some(r#"{"numRecords":1}"#.to_string()),
                deletion_vector: None,
            },
        ],
    }
}

fn descriptor_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
    ]))
}

fn i64_descriptor_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn parquet_bytes_with_i64_columns(rows: &[(i64, i64)]) -> Vec<u8> {
    let schema = Arc::new(
        parse_message_type("message schema { REQUIRED INT64 id; REQUIRED INT64 value; }")
            .expect("parquet schema should parse"),
    );
    let mut bytes = Vec::new();
    let mut writer = SerializedFileWriter::new(
        &mut bytes,
        schema,
        Arc::new(WriterProperties::builder().build()),
    )
    .expect("parquet writer should construct");
    let ids = rows.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    let values = rows.iter().map(|(_, value)| *value).collect::<Vec<_>>();

    let mut row_group = writer
        .next_row_group()
        .expect("row-group writer should construct");
    if let Some(mut column) = row_group
        .next_column()
        .expect("id column writer should be returned")
    {
        column
            .typed::<Int64Type>()
            .write_batch(&ids, None, None)
            .expect("id values should write");
        column.close().expect("id column writer should close");
    }
    if let Some(mut column) = row_group
        .next_column()
        .expect("value column writer should be returned")
    {
        column
            .typed::<Int64Type>()
            .write_batch(&values, None, None)
            .expect("value values should write");
        column.close().expect("value column writer should close");
    }
    row_group.close().expect("row-group writer should close");
    writer.close().expect("file writer should close");
    bytes
}

#[derive(Clone, Debug)]
struct CapturedRequest {
    headers: BTreeMap<String, String>,
}

struct TestResponse {
    status_line: &'static str,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

fn read_request(stream: &mut std::net::TcpStream) -> CapturedRequest {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 512];
    loop {
        let read = stream
            .read(&mut chunk)
            .expect("test server should read request bytes");
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }

    let request = String::from_utf8_lossy(&buffer);
    let headers = request
        .lines()
        .skip(1)
        .take_while(|line| !line.is_empty())
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((name.trim().to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect();

    CapturedRequest { headers }
}

fn write_response(stream: &mut std::net::TcpStream, response: TestResponse) {
    write!(stream, "HTTP/1.1 {}\r\n", response.status_line).expect("status line should write");
    for (header, value) in response.headers {
        write!(stream, "{header}: {value}\r\n").expect("header should write");
    }
    write!(stream, "\r\n").expect("header terminator should write");
    stream
        .write_all(&response.body)
        .expect("response body should write");
    stream.flush().expect("response should flush");
}

fn full_or_ranged_response(request: &CapturedRequest, body: &[u8]) -> TestResponse {
    let Some(range_header) = request.headers.get("range") else {
        return TestResponse {
            status_line: "200 OK",
            headers: vec![("Content-Length".to_string(), body.len().to_string())],
            body: body.to_vec(),
        };
    };

    let (start, end) = resolve_range(range_header, body.len());
    if start > end || end >= body.len() {
        return TestResponse {
            status_line: "416 Range Not Satisfiable",
            headers: vec![
                ("Content-Length".to_string(), "0".to_string()),
                (
                    "Content-Range".to_string(),
                    format!("bytes */{}", body.len()),
                ),
            ],
            body: Vec::new(),
        };
    }

    let ranged = body[start..=end].to_vec();
    TestResponse {
        status_line: "206 Partial Content",
        headers: vec![
            ("Content-Length".to_string(), ranged.len().to_string()),
            (
                "Content-Range".to_string(),
                format!("bytes {start}-{end}/{}", body.len()),
            ),
        ],
        body: ranged,
    }
}

fn resolve_range(range_header: &str, object_len: usize) -> (usize, usize) {
    let range = range_header
        .strip_prefix("bytes=")
        .expect("test server expects byte ranges");
    if let Some(suffix) = range.strip_prefix('-') {
        let suffix_len = suffix.parse::<usize>().expect("suffix length should parse");
        let start = object_len.saturating_sub(suffix_len);
        return (start, object_len.saturating_sub(1));
    }
    let (start, end) = range
        .split_once('-')
        .expect("range should include '-' separator");
    let start = start.parse::<usize>().expect("range start should parse");
    if end.is_empty() {
        return (start, object_len.saturating_sub(1));
    }
    let end = end.parse::<usize>().expect("range end should parse");
    (start, end.min(object_len.saturating_sub(1)))
}

struct RequestCapturingServer {
    address: std::net::SocketAddr,
    stop: Arc<AtomicBool>,
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    thread: Option<JoinHandle<()>>,
}

impl RequestCapturingServer {
    fn new(body: Vec<u8>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral port should bind");
        listener
            .set_nonblocking(true)
            .expect("listener should allow nonblocking accept");
        let address = listener.local_addr().expect("listener addr should resolve");
        let stop = Arc::new(AtomicBool::new(false));
        let stop_for_thread = Arc::clone(&stop);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let requests_for_thread = Arc::clone(&requests);
        let body = Arc::new(body);

        let thread = thread::spawn(move || {
            while !stop_for_thread.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        if stop_for_thread.load(Ordering::SeqCst) {
                            break;
                        }
                        stream
                            .set_nonblocking(false)
                            .expect("accepted streams should allow blocking reads");
                        let body = Arc::clone(&body);
                        let requests = Arc::clone(&requests_for_thread);
                        thread::spawn(move || {
                            let request = read_request(&mut stream);
                            requests
                                .lock()
                                .expect("recorded requests should be writable")
                                .push(request.clone());
                            write_response(&mut stream, full_or_ranged_response(&request, &body));
                        });
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("test server accept should succeed: {error}"),
                }
            }
        });

        Self {
            address,
            stop,
            requests,
            thread: Some(thread),
        }
    }

    fn url(&self) -> String {
        format!("http://{}/object", self.address)
    }

    fn recorded_requests(&self) -> Vec<CapturedRequest> {
        self.requests
            .lock()
            .expect("recorded requests should be readable")
            .clone()
    }
}

impl Drop for RequestCapturingServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = std::net::TcpStream::connect(self.address);
        if let Some(thread) = self.thread.take() {
            thread.join().expect("test server should shut down cleanly");
        }
    }
}
