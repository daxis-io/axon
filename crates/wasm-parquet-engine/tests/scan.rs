#![cfg(not(target_arch = "wasm32"))]

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use parquet::data_type::Int64Type;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use query_contract::PartitionColumnType;
use wasm_http_object_store::HttpRangeReader;
use wasm_parquet_engine::{
    read_parquet_metadata_for_target, scan_target_input_rows, ObjectSource, ParquetScalarValue,
    ScanTarget,
};

#[tokio::test]
async fn scan_target_reads_footer_and_row_groups_from_object_source() {
    let object = parquet_bytes_with_single_i64_column(&[7_i64, 11, 13]);
    let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
    let footer_length = parquet_footer_length(&object);
    let footer_offset = object_size
        .checked_sub(8)
        .and_then(|value| value.checked_sub(u64::from(footer_length)))
        .expect("footer should fit in object");
    let expected_trailer_range = format!("bytes={}-{}", object_size - 8, object_size - 1);
    let expected_footer_range = format!(
        "bytes={footer_offset}-{}",
        footer_offset + u64::from(footer_length) - 1
    );

    let server = RequestCapturingServer::new(object);
    let reader = HttpRangeReader::new();
    let scan_target = ScanTarget {
        object_source: ObjectSource::new(server.url()),
        object_etag: None,
        path: "part-000.parquet".to_string(),
        size_bytes: object_size,
        partition_values: BTreeMap::from([("category".to_string(), Some("A".to_string()))]),
    };

    let metadata = read_parquet_metadata_for_target(&reader, &scan_target, None)
        .await
        .expect("metadata reads should succeed");
    let rows = scan_target_input_rows(
        &reader,
        &scan_target,
        &["id".to_string(), "category".to_string()],
        &BTreeMap::from([("category".to_string(), PartitionColumnType::String)]),
        None,
    )
    .await
    .expect("scan reads should succeed");

    let requests = server.recorded_requests();
    assert!(requests.len() >= 4);
    assert_eq!(
        requests[0].headers.get("range"),
        Some(&expected_trailer_range)
    );
    assert_eq!(
        requests[1].headers.get("range"),
        Some(&expected_footer_range)
    );
    assert_eq!(
        requests[2].headers.get("range"),
        Some(&expected_trailer_range)
    );
    assert_eq!(
        requests[3].headers.get("range"),
        Some(&expected_footer_range)
    );
    assert!(requests
        .iter()
        .all(|request| request.headers.contains_key("range")));
    assert_eq!(metadata.row_group_count, 1);
    assert_eq!(metadata.row_count, 3);
    assert_eq!(
        rows.iter()
            .map(|row| (row.get("id").cloned(), row.get("category").cloned()))
            .collect::<Vec<_>>(),
        vec![
            (
                Some(ParquetScalarValue::Int64(7)),
                Some(ParquetScalarValue::String("A".to_string())),
            ),
            (
                Some(ParquetScalarValue::Int64(11)),
                Some(ParquetScalarValue::String("A".to_string())),
            ),
            (
                Some(ParquetScalarValue::Int64(13)),
                Some(ParquetScalarValue::String("A".to_string())),
            ),
        ]
    );
}

fn parquet_bytes_with_single_i64_column(values: &[i64]) -> Vec<u8> {
    let schema = Arc::new(
        parse_message_type("message schema { REQUIRED INT64 id; }")
            .expect("parquet schema should parse"),
    );
    let mut bytes = Vec::new();
    let mut writer = SerializedFileWriter::new(
        &mut bytes,
        schema,
        Arc::new(WriterProperties::builder().build()),
    )
    .expect("parquet writer should construct");

    let mut row_group = writer
        .next_row_group()
        .expect("row-group writer should construct");
    if let Some(mut column) = row_group
        .next_column()
        .expect("column writer should be returned")
    {
        column
            .typed::<Int64Type>()
            .write_batch(values, None, None)
            .expect("test parquet rows should write");
        column.close().expect("column writer should close");
    }
    row_group.close().expect("row-group writer should close");
    writer.close().expect("file writer should close");
    bytes
}

fn parquet_footer_length(object: &[u8]) -> u32 {
    let footer_length_offset = object
        .len()
        .checked_sub(8)
        .expect("parquet object should include trailer");
    let footer_length_slice = &object[footer_length_offset..footer_length_offset + 4];
    u32::from_le_bytes(
        footer_length_slice
            .try_into()
            .expect("parquet footer length slice should be 4 bytes"),
    )
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
    let (start, end) = range
        .split_once('-')
        .expect("range should include '-' separator");
    let start = start.parse::<usize>().expect("range start should parse");
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

        let thread = thread::spawn(move || {
            while !stop_for_thread.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let request = read_request(&mut stream);
                        requests_for_thread
                            .lock()
                            .expect("recorded requests should be writable")
                            .push(request.clone());
                        write_response(&mut stream, full_or_ranged_response(&request, &body));
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
