#![cfg(not(target_arch = "wasm32"))]

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use arrow_array::{cast::AsArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::data_type::Int64Type;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use query_contract::PartitionColumnType;
use wasm_datafusion_poc::{DeltaActiveFile, DeltaTableDescriptor, WasmDataFusionEngine};

static PARQUET_SCAN_TEST_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn delta_descriptor_scan_streams_parquet_batches_through_datafusion() {
    let _guard = PARQUET_SCAN_TEST_LOCK
        .lock()
        .expect("Parquet scan tests should serialize local HTTP servers");
    test_runtime().block_on(async {
        let object = parquet_bytes_with_i64_columns(&[(3, 25), (1, 5), (2, 12)]);
        let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
        let server = RequestCapturingServer::new(object);
        let mut engine = WasmDataFusionEngine::new();

        engine
            .open_delta_table(delta_descriptor(
                "events",
                descriptor_schema(),
                server.url(),
                object_size,
            ))
            .await
            .expect("Delta descriptor should register as a DataFusion table");

        let (schema, batches) = engine
            .sql_to_record_batches(
                "SELECT id, value \
             FROM events \
             WHERE category = 'B' AND value > 10 \
             ORDER BY id",
            )
            .await
            .expect("DataFusion should execute SQL over browser Parquet scan batches");

        assert_eq!(
            schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["id", "value"]
        );
        assert_eq!(int64_column_values(&batches, 0), vec![2, 3]);
        assert_eq!(int64_column_values(&batches, 1), vec![12, 25]);
        assert!(
            server
                .recorded_requests()
                .iter()
                .all(|request| request.headers.contains_key("range")),
            "DataFusion scan should delegate Parquet object reads to browser range I/O"
        );
    });
}

#[test]
fn delta_descriptor_scan_preserves_rows_for_count_star() {
    let _guard = PARQUET_SCAN_TEST_LOCK
        .lock()
        .expect("Parquet scan tests should serialize local HTTP servers");
    test_runtime().block_on(async {
        let object = parquet_bytes_with_i64_columns(&[(3, 25), (1, 5), (2, 12)]);
        let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
        let server = RequestCapturingServer::new(object);
        let mut engine = WasmDataFusionEngine::new();

        engine
            .open_delta_table(delta_descriptor(
                "events",
                descriptor_schema(),
                server.url(),
                object_size,
            ))
            .await
            .expect("Delta descriptor should register as a DataFusion table");

        let (schema, batches) = engine
            .sql_to_record_batches("SELECT COUNT(*) AS rows FROM events")
            .await
            .expect("DataFusion should count rows from browser Parquet scan batches");

        assert_eq!(schema.field(0).name(), "rows");
        assert_eq!(int64_column_values(&batches, 0), vec![3]);
    });
}

#[test]
fn delta_descriptor_scan_aligns_normalized_parquet_names_to_projected_schema() {
    let _guard = PARQUET_SCAN_TEST_LOCK
        .lock()
        .expect("Parquet scan tests should serialize local HTTP servers");
    test_runtime().block_on(async {
        let object = parquet_bytes_with_i64_columns(&[(3, 25), (1, 5), (2, 12)]);
        let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
        let server = RequestCapturingServer::new(object);
        let mut engine = WasmDataFusionEngine::new();

        engine
            .open_delta_table(delta_descriptor(
                "events",
                mixed_case_descriptor_schema(),
                server.url(),
                object_size,
            ))
            .await
            .expect("Delta descriptor should register as a DataFusion table");

        let (schema, batches) = engine
            .sql_to_record_batches(
                "SELECT \"Id\", \"Value\" \
             FROM events \
             WHERE \"Category\" = 'B' AND \"Value\" > 10 \
             ORDER BY \"Id\"",
            )
            .await
            .expect("DataFusion should align normalized Parquet output to projected schema");

        assert_eq!(
            schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["Id", "Value"]
        );
        assert_eq!(int64_column_values(&batches, 0), vec![2, 3]);
        assert_eq!(int64_column_values(&batches, 1), vec![12, 25]);
    });
}

fn test_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("test runtime should construct")
}

fn delta_descriptor(
    table_name: &str,
    schema: SchemaRef,
    url: String,
    size_bytes: u64,
) -> DeltaTableDescriptor {
    DeltaTableDescriptor {
        table_name: table_name.to_string(),
        table_version: 11,
        schema,
        partition_columns: vec!["category".to_string()],
        partition_column_types: BTreeMap::from([(
            "category".to_string(),
            PartitionColumnType::String,
        )]),
        active_files: vec![DeltaActiveFile {
            path: "category=B/part-000.parquet".to_string(),
            url,
            size_bytes,
            partition_values: BTreeMap::from([("category".to_string(), Some("B".to_string()))]),
            object_etag: None,
            stats_json: Some(r#"{"numRecords":3}"#.to_string()),
            deletion_vector: None,
        }],
    }
}

fn descriptor_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
        Field::new("category", DataType::Utf8, true),
    ]))
}

fn mixed_case_descriptor_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("Id", DataType::Int64, false),
        Field::new("Value", DataType::Int64, false),
        Field::new("Category", DataType::Utf8, true),
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

fn int64_column_values(batches: &[RecordBatch], column_index: usize) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(column_index)
                .as_primitive::<arrow_array::types::Int64Type>()
                .values()
                .to_vec()
        })
        .collect()
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
