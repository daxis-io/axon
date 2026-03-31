#![cfg(not(target_arch = "wasm32"))]

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc::{self, Receiver};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use parquet::data_type::Int64Type;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use wasm_http_object_store::HttpRangeReader;
use wasm_parquet_engine::{read_parquet_metadata_for_target, ObjectSource, ScanTarget};

#[tokio::test]
async fn known_file_size_avoids_extra_metadata_round_trip() {
    let object = parquet_bytes_with_single_i64_column(&[1_i64, 2, 3]);
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

    let object_for_server = object.clone();
    let (url, requests, server) = spawn_multi_request_server(2, move |request, _| {
        full_or_ranged_response(request, &object_for_server)
    });
    let metadata = read_parquet_metadata_for_target(
        &HttpRangeReader::new(),
        &ScanTarget {
            object_source: ObjectSource::new(url),
            object_etag: None,
            path: "part-000.parquet".to_string(),
            size_bytes: object_size,
            partition_values: BTreeMap::new(),
        },
        None,
    )
    .await
    .expect("known-size metadata reads should succeed");

    let requests = finish_requests(server, requests, 2);
    assert_eq!(
        requests[0].headers.get("range"),
        Some(&expected_trailer_range)
    );
    assert_eq!(
        requests[1].headers.get("range"),
        Some(&expected_footer_range)
    );
    assert_eq!(metadata.object_size_bytes, object_size);
    assert_eq!(metadata.row_group_count, 1);
    assert_eq!(metadata.row_count, 3);
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

#[derive(Debug)]
struct CapturedRequest {
    headers: BTreeMap<String, String>,
}

struct TestResponse {
    status_line: &'static str,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

fn spawn_multi_request_server<F>(
    request_count: usize,
    handler: F,
) -> (String, Receiver<CapturedRequest>, JoinHandle<()>)
where
    F: Fn(&CapturedRequest, usize) -> TestResponse + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral port should bind");
    let address = listener.local_addr().expect("listener addr should resolve");
    let url = format!("http://{address}/object");
    let (request_tx, request_rx) = mpsc::channel();

    let server = thread::spawn(move || {
        for index in 0..request_count {
            let (mut stream, _) = listener.accept().expect("test client should connect");
            let request = read_request(&mut stream);
            let response = handler(&request, index);
            write_response(&mut stream, response);
            let _ = request_tx.send(request);
        }
    });

    (url, request_rx, server)
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

fn finish_requests(
    server: JoinHandle<()>,
    requests: Receiver<CapturedRequest>,
    expected_count: usize,
) -> Vec<CapturedRequest> {
    let mut captured = Vec::with_capacity(expected_count);
    for _ in 0..expected_count {
        captured.push(requests.recv().expect("expected captured request"));
    }
    server.join().expect("test server should shut down cleanly");
    captured
}
