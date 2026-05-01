#![cfg(not(target_arch = "wasm32"))]

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use arrow_array::{Array, Int64Array, StringArray};
use futures_util::TryStreamExt;
use parquet::data_type::Int64Type;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use query_contract::PartitionColumnType;
use tokio::time::timeout;
use wasm_http_object_store::HttpRangeReader;
use wasm_parquet_engine::{
    stream_scan_target_batches, stream_scan_target_batches_with_row_group_pruning,
    stream_scan_targets, ObjectSource, ParquetIntegerComparison, ParquetRowGroupPruningPredicate,
    ScanTarget,
};

#[tokio::test]
async fn stream_scan_target_batches_yields_incremental_record_batches() {
    let values = (0_i64..1025).collect::<Vec<_>>();
    let object = parquet_bytes_with_single_i64_column(&values);
    let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
    let server = RequestCapturingServer::new(object.clone(), Duration::from_millis(200), true);

    let scan = timeout(
        Duration::from_millis(100),
        stream_scan_target_batches(
            &HttpRangeReader::new(),
            &ScanTarget {
                object_source: ObjectSource::new(server.url()),
                object_etag: None,
                path: "part-000.parquet".to_string(),
                size_bytes: object_size,
                partition_values: BTreeMap::from([("category".to_string(), Some("A".to_string()))]),
            },
            &["id".to_string(), "category".to_string()],
            &BTreeMap::from([("category".to_string(), PartitionColumnType::String)]),
            Some(Duration::from_millis(75)),
        ),
    )
    .await
    .expect("stream construction should finish before any slow full-object response")
    .expect("streaming scan should succeed");

    let metrics = Arc::clone(&scan.metrics);
    let batches = scan
        .batches
        .try_collect::<Vec<_>>()
        .await
        .expect("batch stream should decode");

    let requests = server.recorded_requests();
    assert!(
        requests
            .iter()
            .all(|request| request.headers.contains_key("range")),
        "stream construction and decoding should rely on range requests, not a full-object GET"
    );
    assert!(
        requests.len() >= 2,
        "known-size bootstrap should require at least trailer and footer range requests"
    );
    assert!(batches.len() >= 2, "expected more than one record batch");
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        values.len()
    );
    assert_eq!(
        batches[0]
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec!["id", "category"]
    );
    assert_eq!(metrics.snapshot().rows_emitted, values.len() as u64);
    assert_eq!(
        metrics.snapshot().bytes_fetched,
        requests
            .iter()
            .map(|request| requested_bytes(request, object_size))
            .sum::<u64>()
    );
}

#[tokio::test]
async fn known_size_scan_target_avoids_extra_metadata_probe_round_trips() {
    let object = parquet_bytes_with_single_i64_column(&[1_i64, 2, 3]);
    let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
    let server = RequestCapturingServer::new(object, Duration::from_millis(0), false);

    let scan = stream_scan_target_batches(
        &HttpRangeReader::new(),
        &ScanTarget {
            object_source: ObjectSource::new(server.url()),
            object_etag: None,
            path: "part-000.parquet".to_string(),
            size_bytes: object_size,
            partition_values: BTreeMap::new(),
        },
        &["id".to_string()],
        &BTreeMap::new(),
        None,
    )
    .await
    .expect("streaming scan should succeed");

    let metrics = Arc::clone(&scan.metrics);
    drop(scan);

    let requests = server.recorded_requests();
    assert_eq!(requests.len(), 2);
    assert!(requests
        .iter()
        .all(|request| request.headers.contains_key("range")));
    assert_eq!(metrics.snapshot().metadata_probe_round_trips, 2);
}

#[tokio::test]
async fn streamed_scan_reports_rows_and_bytes_per_target() {
    let object = parquet_bytes_with_single_i64_column(&[7_i64, 11, 13]);
    let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
    let server = RequestCapturingServer::new(object, Duration::from_millis(0), false);

    let scan = stream_scan_target_batches(
        &HttpRangeReader::new(),
        &ScanTarget {
            object_source: ObjectSource::new(server.url()),
            object_etag: None,
            path: "part-000.parquet".to_string(),
            size_bytes: object_size,
            partition_values: BTreeMap::new(),
        },
        &["id".to_string()],
        &BTreeMap::new(),
        None,
    )
    .await
    .expect("streaming scan should succeed");

    let metrics = Arc::clone(&scan.metrics);
    let batches = scan
        .batches
        .try_collect::<Vec<_>>()
        .await
        .expect("batch stream should decode");

    let requests = server.recorded_requests();
    assert_eq!(metrics.snapshot().files_touched, 1);
    assert_eq!(metrics.snapshot().files_skipped, 0);
    assert_eq!(metrics.snapshot().row_groups_touched, 1);
    assert_eq!(metrics.snapshot().row_groups_skipped, 0);
    assert_eq!(metrics.snapshot().footer_reads, 1);
    assert_eq!(metrics.snapshot().rows_emitted, 3);
    assert_eq!(
        metrics.snapshot().bytes_fetched,
        requests
            .iter()
            .map(|request| requested_bytes(request, object_size))
            .sum::<u64>()
    );
    assert_eq!(metrics.snapshot().metadata_probe_round_trips, 2);
    assert_eq!(batches.len(), 1);
}

#[tokio::test]
async fn row_group_stats_pruning_skips_ranges_after_footer_read() {
    let object = parquet_bytes_with_i64_row_groups(&[&[1_i64, 2, 3], &[10_i64, 11, 12]]);
    let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
    let first_row_group_range = first_row_group_byte_range(&object);
    let server = RequestCapturingServer::new(object, Duration::from_millis(0), false);

    let scan = stream_scan_target_batches_with_row_group_pruning(
        &HttpRangeReader::new(),
        &ScanTarget {
            object_source: ObjectSource::new(server.url()),
            object_etag: None,
            path: "part-000.parquet".to_string(),
            size_bytes: object_size,
            partition_values: BTreeMap::new(),
        },
        &["id".to_string()],
        &BTreeMap::new(),
        None,
        Some(&ParquetRowGroupPruningPredicate {
            column: "id".to_string(),
            comparison: ParquetIntegerComparison::Gte(10),
        }),
    )
    .await
    .expect("row-group-pruned stream should construct");

    let metrics = Arc::clone(&scan.metrics);
    let batches = scan
        .batches
        .try_collect::<Vec<_>>()
        .await
        .expect("row-group-pruned stream should decode");

    let requests = server.recorded_requests();
    assert_eq!(
        batches
            .iter()
            .flat_map(|batch| batch_i64_values(batch, "id"))
            .collect::<Vec<_>>(),
        vec![10, 11, 12]
    );
    assert_eq!(metrics.snapshot().files_touched, 1);
    assert_eq!(metrics.snapshot().files_skipped, 0);
    assert_eq!(metrics.snapshot().row_groups_touched, 1);
    assert_eq!(metrics.snapshot().row_groups_skipped, 1);
    assert_eq!(metrics.snapshot().footer_reads, 1);
    assert_eq!(metrics.snapshot().rows_emitted, 3);
    assert!(
        requests.iter().skip(2).all(|request| {
            request
                .headers
                .get("range")
                .and_then(|range| bounded_request_range(range, object_size))
                .is_none_or(|range| !ranges_overlap(range, first_row_group_range))
        }),
        "row group 0 data ranges should not be fetched after footer pruning"
    );
}

#[tokio::test]
async fn stream_scan_targets_yields_batches_across_files_with_aggregate_metrics() {
    let first_object = parquet_bytes_with_single_i64_column(&[1_i64, 2]);
    let second_object = parquet_bytes_with_single_i64_column(&[3_i64, 4, 5]);
    let first_size = u64::try_from(first_object.len()).expect("object size should fit in u64");
    let second_size = u64::try_from(second_object.len()).expect("object size should fit in u64");
    let first_server = RequestCapturingServer::new(first_object, Duration::from_millis(0), false);
    let second_server = RequestCapturingServer::new(second_object, Duration::from_millis(0), false);

    let scan = stream_scan_targets(
        &HttpRangeReader::new(),
        &[
            ScanTarget {
                object_source: ObjectSource::new(first_server.url()),
                object_etag: None,
                path: "category=A/part-000.parquet".to_string(),
                size_bytes: first_size,
                partition_values: BTreeMap::from([("category".to_string(), Some("A".to_string()))]),
            },
            ScanTarget {
                object_source: ObjectSource::new(second_server.url()),
                object_etag: None,
                path: "category=B/part-000.parquet".to_string(),
                size_bytes: second_size,
                partition_values: BTreeMap::from([("category".to_string(), Some("B".to_string()))]),
            },
        ],
        &["id".to_string(), "category".to_string()],
        &BTreeMap::from([("category".to_string(), PartitionColumnType::String)]),
        None,
    )
    .await
    .expect("multi-file streaming scan should construct");

    let metrics = Arc::clone(&scan.metrics);
    let batches = scan
        .batches
        .try_collect::<Vec<_>>()
        .await
        .expect("multi-file batches should decode");

    let first_requests = first_server.recorded_requests();
    let second_requests = second_server.recorded_requests();
    assert!(first_requests
        .iter()
        .chain(second_requests.iter())
        .all(|request| request.headers.contains_key("range")));
    assert_eq!(batches.len(), 2);
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        5
    );
    assert_eq!(batch_i64_values(&batches[0], "id"), vec![1, 2]);
    assert_eq!(
        batch_string_values(&batches[0], "category"),
        vec!["A".to_string(), "A".to_string()]
    );
    assert_eq!(batch_i64_values(&batches[1], "id"), vec![3, 4, 5]);
    assert_eq!(
        batch_string_values(&batches[1], "category"),
        vec!["B".to_string(), "B".to_string(), "B".to_string()]
    );
    assert_eq!(metrics.snapshot().files_touched, 2);
    assert_eq!(metrics.snapshot().rows_emitted, 5);
    assert_eq!(metrics.snapshot().row_groups_touched, 2);
    assert_eq!(metrics.snapshot().row_groups_skipped, 0);
    assert_eq!(metrics.snapshot().footer_reads, 2);
    assert_eq!(metrics.snapshot().metadata_probe_round_trips, 4);
    assert_eq!(
        metrics.snapshot().bytes_fetched,
        first_requests
            .iter()
            .map(|request| requested_bytes(request, first_size))
            .sum::<u64>()
            + second_requests
                .iter()
                .map(|request| requested_bytes(request, second_size))
                .sum::<u64>()
    );
}

fn parquet_bytes_with_single_i64_column(values: &[i64]) -> Vec<u8> {
    parquet_bytes_with_i64_row_groups(&[values])
}

fn parquet_bytes_with_i64_row_groups(row_groups: &[&[i64]]) -> Vec<u8> {
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

    for values in row_groups {
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
    }
    writer.close().expect("file writer should close");
    bytes
}

fn first_row_group_byte_range(object: &[u8]) -> (u64, u64) {
    let reader = SerializedFileReader::new(bytes::Bytes::copy_from_slice(object))
        .expect("parquet object should decode");
    reader.metadata().row_group(0).column(0).byte_range()
}

fn batch_i64_values(batch: &arrow_array::RecordBatch, column_name: &str) -> Vec<i64> {
    let column_index = batch
        .schema()
        .index_of(column_name)
        .expect("column should exist");
    let column = batch
        .column(column_index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("column should be int64");
    (0..column.len()).map(|index| column.value(index)).collect()
}

fn batch_string_values(batch: &arrow_array::RecordBatch, column_name: &str) -> Vec<String> {
    let column_index = batch
        .schema()
        .index_of(column_name)
        .expect("column should exist");
    let column = batch
        .column(column_index)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("column should be utf8");
    (0..column.len())
        .map(|index| column.value(index).to_string())
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

fn requested_bytes(request: &CapturedRequest, object_size: u64) -> u64 {
    let Some(range) = request.headers.get("range") else {
        return object_size;
    };
    let range = range
        .strip_prefix("bytes=")
        .expect("range headers should use bytes syntax");
    if let Some(length) = range.strip_prefix('-') {
        return length
            .parse::<u64>()
            .expect("suffix range lengths should parse");
    }
    let (start, end) = range
        .split_once('-')
        .expect("bounded range headers should include a dash");
    let start = start.parse::<u64>().expect("range start should parse");
    if end.is_empty() {
        return object_size
            .checked_sub(start)
            .expect("open-ended ranges should be within the object");
    }
    let end = end.parse::<u64>().expect("range end should parse");
    end.checked_sub(start)
        .and_then(|length| length.checked_add(1))
        .expect("bounded range length should not overflow")
}

fn bounded_request_range(range_header: &str, object_size: u64) -> Option<(u64, u64)> {
    let range = range_header.strip_prefix("bytes=")?;
    if let Some(length) = range.strip_prefix('-') {
        let length = length.parse::<u64>().ok()?;
        let start = object_size.checked_sub(length)?;
        return Some((start, object_size.checked_sub(1)?));
    }
    let (start, end) = range.split_once('-')?;
    let start = start.parse::<u64>().ok()?;
    if end.is_empty() {
        return Some((start, object_size.checked_sub(1)?));
    }
    Some((start, end.parse::<u64>().ok()?))
}

fn ranges_overlap(left: (u64, u64), right: (u64, u64)) -> bool {
    left.0 <= right.1 && right.0 <= left.1
}

struct RequestCapturingServer {
    address: std::net::SocketAddr,
    stop: Arc<AtomicBool>,
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    thread: Option<JoinHandle<()>>,
}

impl RequestCapturingServer {
    fn new(body: Vec<u8>, full_object_delay: Duration, delay_unranged: bool) -> Self {
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
                        stream
                            .set_nonblocking(false)
                            .expect("accepted streams should allow blocking reads");
                        let request = read_request(&mut stream);
                        requests_for_thread
                            .lock()
                            .expect("recorded requests should be writable")
                            .push(request);
                        let request = requests_for_thread
                            .lock()
                            .expect("recorded requests should be readable")
                            .last()
                            .cloned()
                            .expect("captured request should exist");
                        if delay_unranged && !request.headers.contains_key("range") {
                            thread::sleep(full_object_delay);
                        }
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
