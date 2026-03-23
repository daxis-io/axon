use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc::{self, Receiver};
use std::thread::{self, JoinHandle};

use query_contract::QueryErrorCode;
use wasm_http_object_store::{HttpByteRange, HttpRangeReader};

const PARQUET_LIKE_BYTES: &[u8] = b"PAR1abcdefghijklmnoPAR1";

#[derive(Debug)]
struct CapturedRequest {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
}

struct TestResponse {
    status_line: &'static str,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

#[test]
fn full_reads_fetch_entire_object_without_range_header() {
    let (url, requests, server) = spawn_test_server(|request| {
        assert_eq!(request.method, "GET");
        assert_eq!(request.path, "/object");
        full_or_ranged_response(request, b"hello world")
    });

    let reader = HttpRangeReader::new();
    let result = runtime()
        .block_on(reader.read_range(&url, HttpByteRange::Full))
        .expect("full read should succeed");

    let request = finish_request(server, requests);
    assert!(!request.headers.contains_key("range"));
    assert_eq!(result.metadata.url, url);
    assert_eq!(result.metadata.size_bytes, Some(11));
    assert_eq!(result.bytes, b"hello world");
}

#[test]
fn full_reads_support_injected_reqwest_clients() {
    let (url, requests, server) = spawn_test_server(|request| {
        assert_eq!(request.method, "GET");
        assert_eq!(request.path, "/object");
        full_or_ranged_response(request, b"hello world")
    });

    let reader = HttpRangeReader::with_client(
        reqwest::Client::builder()
            .build()
            .expect("custom reqwest client should build"),
    );
    let result = runtime()
        .block_on(reader.read_range(&url, HttpByteRange::Full))
        .expect("full read should succeed");

    let request = finish_request(server, requests);
    assert!(!request.headers.contains_key("range"));
    assert_eq!(result.metadata.url, url);
    assert_eq!(result.metadata.size_bytes, Some(11));
    assert_eq!(result.bytes, b"hello world");
}

#[test]
fn bounded_reads_send_exact_range_header_and_capture_object_size() {
    let (url, requests, server) =
        spawn_test_server(|request| full_or_ranged_response(request, b"abcdefghij"));

    let reader = HttpRangeReader::new();
    let result = runtime()
        .block_on(reader.read_range(
            &url,
            HttpByteRange::Bounded {
                offset: 2,
                length: 4,
            },
        ))
        .expect("bounded read should succeed");

    let request = finish_request(server, requests);
    assert_eq!(request.headers.get("range"), Some(&"bytes=2-5".to_string()));
    assert_eq!(result.metadata.size_bytes, Some(10));
    assert_eq!(result.bytes, b"cdef");
}

#[test]
fn from_offset_reads_send_exact_range_header_and_capture_object_size() {
    let (url, requests, server) =
        spawn_test_server(|request| full_or_ranged_response(request, b"abcdefghij"));

    let reader = HttpRangeReader::new();
    let result = runtime()
        .block_on(reader.read_range(&url, HttpByteRange::FromOffset { offset: 6 }))
        .expect("from-offset read should succeed");

    let request = finish_request(server, requests);
    assert_eq!(request.headers.get("range"), Some(&"bytes=6-".to_string()));
    assert_eq!(result.metadata.size_bytes, Some(10));
    assert_eq!(result.bytes, b"ghij");
}

#[test]
fn suffix_reads_send_exact_range_header_and_capture_object_size() {
    let (url, requests, server) =
        spawn_test_server(|request| full_or_ranged_response(request, b"abcdefghij"));

    let reader = HttpRangeReader::new();
    let result = runtime()
        .block_on(reader.read_range(&url, HttpByteRange::Suffix { length: 3 }))
        .expect("suffix read should succeed");

    let request = finish_request(server, requests);
    assert_eq!(request.headers.get("range"), Some(&"bytes=-3".to_string()));
    assert_eq!(result.metadata.size_bytes, Some(10));
    assert_eq!(result.bytes, b"hij");
}

#[test]
fn parquet_footer_style_reads_return_last_eight_bytes() {
    let (url, _, server) =
        spawn_test_server(|request| full_or_ranged_response(request, PARQUET_LIKE_BYTES));

    let reader = HttpRangeReader::new();
    let result = runtime()
        .block_on(reader.read_range(&url, HttpByteRange::Suffix { length: 8 }))
        .expect("footer-style suffix read should succeed");

    server.join().expect("test server should shut down cleanly");
    assert_eq!(
        result.metadata.size_bytes,
        Some(PARQUET_LIKE_BYTES.len() as u64)
    );
    assert_eq!(result.bytes, b"lmnoPAR1");
}

#[test]
fn unauthorized_and_forbidden_map_to_access_denied() {
    for status_line in ["401 Unauthorized", "403 Forbidden"] {
        let (url, _, server) = spawn_test_server(move |_| TestResponse {
            status_line,
            headers: vec![("Content-Length".to_string(), "0".to_string())],
            body: Vec::new(),
        });

        let reader = HttpRangeReader::new();
        let error = runtime()
            .block_on(reader.read_range(&url, HttpByteRange::Full))
            .expect_err("access-denied statuses should fail");

        server.join().expect("test server should shut down cleanly");
        assert_eq!(error.code, QueryErrorCode::AccessDenied);
    }
}

#[test]
fn not_found_and_range_not_satisfiable_map_to_object_store_protocol() {
    let missing_cases = [
        TestResponse {
            status_line: "404 Not Found",
            headers: vec![("Content-Length".to_string(), "0".to_string())],
            body: Vec::new(),
        },
        TestResponse {
            status_line: "416 Range Not Satisfiable",
            headers: vec![
                ("Content-Length".to_string(), "0".to_string()),
                ("Content-Range".to_string(), "bytes */10".to_string()),
            ],
            body: Vec::new(),
        },
    ];

    for response in missing_cases {
        let (url, _, server) = spawn_test_server(move |_| TestResponse {
            status_line: response.status_line,
            headers: response.headers.clone(),
            body: response.body.clone(),
        });

        let reader = HttpRangeReader::new();
        let error = runtime()
            .block_on(reader.read_range(
                &url,
                HttpByteRange::Bounded {
                    offset: 0,
                    length: 4,
                },
            ))
            .expect_err("protocol statuses should fail");

        server.join().expect("test server should shut down cleanly");
        assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    }
}

#[test]
fn malformed_partial_responses_map_to_object_store_protocol() {
    let (url, _, server) = spawn_test_server(|request| {
        assert_eq!(request.headers.get("range"), Some(&"bytes=2-5".to_string()));
        TestResponse {
            status_line: "206 Partial Content",
            headers: vec![("Content-Length".to_string(), "4".to_string())],
            body: b"cdef".to_vec(),
        }
    });

    let reader = HttpRangeReader::new();
    let error = runtime()
        .block_on(reader.read_range(
            &url,
            HttpByteRange::Bounded {
                offset: 2,
                length: 4,
            },
        ))
        .expect_err("partial responses without Content-Range should fail");

    server.join().expect("test server should shut down cleanly");
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
}

#[test]
fn mismatched_content_ranges_are_rejected_even_when_body_length_matches() {
    let (url, _, server) = spawn_test_server(|request| {
        assert_eq!(request.headers.get("range"), Some(&"bytes=2-5".to_string()));
        TestResponse {
            status_line: "206 Partial Content",
            headers: vec![
                ("Content-Length".to_string(), "4".to_string()),
                ("Content-Range".to_string(), "bytes 0-3/10".to_string()),
            ],
            body: b"abcd".to_vec(),
        }
    });

    let reader = HttpRangeReader::new();
    let error = runtime()
        .block_on(reader.read_range(
            &url,
            HttpByteRange::Bounded {
                offset: 2,
                length: 4,
            },
        ))
        .expect_err("content ranges that do not match the requested bytes should fail");

    server.join().expect("test server should shut down cleanly");
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
}

#[test]
fn from_offset_reads_reject_partial_responses_that_do_not_extend_to_eof() {
    let (url, _, server) = spawn_test_server(|request| {
        assert_eq!(request.headers.get("range"), Some(&"bytes=6-".to_string()));
        TestResponse {
            status_line: "206 Partial Content",
            headers: vec![
                ("Content-Length".to_string(), "3".to_string()),
                ("Content-Range".to_string(), "bytes 6-8/10".to_string()),
            ],
            body: b"ghi".to_vec(),
        }
    });

    let reader = HttpRangeReader::new();
    let error = runtime()
        .block_on(reader.read_range(&url, HttpByteRange::FromOffset { offset: 6 }))
        .expect_err("from-offset reads should extend through the end of the object");

    server.join().expect("test server should shut down cleanly");
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
}

#[test]
fn suffix_reads_reject_partial_responses_that_do_not_cover_the_final_bytes() {
    let (url, _, server) = spawn_test_server(|request| {
        assert_eq!(request.headers.get("range"), Some(&"bytes=-3".to_string()));
        TestResponse {
            status_line: "206 Partial Content",
            headers: vec![
                ("Content-Length".to_string(), "3".to_string()),
                ("Content-Range".to_string(), "bytes 6-8/10".to_string()),
            ],
            body: b"ghi".to_vec(),
        }
    });

    let reader = HttpRangeReader::new();
    let error = runtime()
        .block_on(reader.read_range(&url, HttpByteRange::Suffix { length: 3 }))
        .expect_err("suffix reads should return the final requested bytes");

    server.join().expect("test server should shut down cleanly");
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
}

#[test]
fn range_requests_reject_ok_statuses() {
    let (url, _, server) = spawn_test_server(|request| {
        assert_eq!(request.headers.get("range"), Some(&"bytes=2-5".to_string()));
        TestResponse {
            status_line: "200 OK",
            headers: vec![("Content-Length".to_string(), "4".to_string())],
            body: b"cdef".to_vec(),
        }
    });

    let reader = HttpRangeReader::new();
    let error = runtime()
        .block_on(reader.read_range(
            &url,
            HttpByteRange::Bounded {
                offset: 2,
                length: 4,
            },
        ))
        .expect_err("range requests should require HTTP 206");

    server.join().expect("test server should shut down cleanly");
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
}

#[test]
fn full_requests_reject_partial_content_statuses() {
    let (url, _, server) = spawn_test_server(|_| TestResponse {
        status_line: "206 Partial Content",
        headers: vec![
            ("Content-Length".to_string(), "11".to_string()),
            ("Content-Range".to_string(), "bytes 0-10/11".to_string()),
        ],
        body: b"hello world".to_vec(),
    });

    let reader = HttpRangeReader::new();
    let error = runtime()
        .block_on(reader.read_range(&url, HttpByteRange::Full))
        .expect_err("full requests should require HTTP 200");

    server.join().expect("test server should shut down cleanly");
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
}

#[test]
fn body_length_mismatches_are_rejected() {
    let (url, _, server) = spawn_test_server(|request| {
        assert_eq!(request.headers.get("range"), Some(&"bytes=2-5".to_string()));
        TestResponse {
            status_line: "206 Partial Content",
            headers: vec![
                ("Content-Length".to_string(), "3".to_string()),
                ("Content-Range".to_string(), "bytes 2-5/10".to_string()),
            ],
            body: b"cde".to_vec(),
        }
    });

    let reader = HttpRangeReader::new();
    let error = runtime()
        .block_on(reader.read_range(
            &url,
            HttpByteRange::Bounded {
                offset: 2,
                length: 4,
            },
        ))
        .expect_err("partial bodies should match their declared content range length");

    server.join().expect("test server should shut down cleanly");
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
}

#[test]
fn invalid_schemes_map_to_invalid_request() {
    let reader = HttpRangeReader::new();
    let error = runtime()
        .block_on(reader.read_range("ftp://example.com/object", HttpByteRange::Full))
        .expect_err("unsupported schemes should fail validation");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
}

#[test]
fn metadata_and_errors_redact_query_strings_from_signed_urls() {
    let (base_url, _, server) = spawn_test_server(|_| TestResponse {
        status_line: "401 Unauthorized",
        headers: vec![("Content-Length".to_string(), "0".to_string())],
        body: Vec::new(),
    });
    let signed_url = format!("{base_url}?X-Goog-Signature=super-secret&X-Goog-Expires=30#fragment");

    let reader = HttpRangeReader::new();
    let error = runtime()
        .block_on(reader.read_range(&signed_url, HttpByteRange::Full))
        .expect_err("unauthorized signed URLs should fail");

    server.join().expect("test server should shut down cleanly");
    assert_eq!(error.code, QueryErrorCode::AccessDenied);
    assert!(error.message.contains(&base_url));
    assert!(!error.message.contains("X-Goog-Signature"));
    assert!(!error.message.contains("fragment"));

    let (base_url, _, server) = spawn_test_server(|_| TestResponse {
        status_line: "200 OK",
        headers: vec![("Content-Length".to_string(), "5".to_string())],
        body: b"hello".to_vec(),
    });
    let signed_url = format!("{base_url}?sig=secret");
    let result = runtime()
        .block_on(reader.read_range(&signed_url, HttpByteRange::Full))
        .expect("full reads should succeed");

    server.join().expect("test server should shut down cleanly");
    assert_eq!(result.metadata.url, base_url);
}

#[test]
fn transport_failures_map_to_execution_failed() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral port should bind");
    let url = format!(
        "http://{}/object",
        listener.local_addr().expect("addr should resolve")
    );
    drop(listener);

    let reader = HttpRangeReader::new();
    let error = runtime()
        .block_on(reader.read_range(&url, HttpByteRange::Full))
        .expect_err("connection-refused reads should fail");

    assert_eq!(error.code, QueryErrorCode::ExecutionFailed);
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("tokio runtime should be created for tests")
}

fn spawn_test_server<F>(handler: F) -> (String, Receiver<CapturedRequest>, JoinHandle<()>)
where
    F: FnOnce(&CapturedRequest) -> TestResponse + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral port should bind");
    let address = listener.local_addr().expect("listener addr should resolve");
    let url = format!("http://{address}/object");
    let (request_tx, request_rx) = mpsc::channel();

    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("test client should connect");
        let request = read_request(&mut stream);
        let response = handler(&request);
        write_response(&mut stream, response);
        let _ = request_tx.send(request);
    });

    (url, request_rx, server)
}

fn finish_request(server: JoinHandle<()>, requests: Receiver<CapturedRequest>) -> CapturedRequest {
    server.join().expect("test server should shut down cleanly");
    requests
        .recv()
        .expect("test should receive the captured request")
}

fn read_request(stream: &mut std::net::TcpStream) -> CapturedRequest {
    let mut buffer = [0_u8; 4096];
    let mut request = Vec::new();

    loop {
        let bytes_read = stream
            .read(&mut buffer)
            .expect("request bytes should be readable");
        if bytes_read == 0 {
            break;
        }
        request.extend_from_slice(&buffer[..bytes_read]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }

    let text = String::from_utf8(request).expect("request should be valid ASCII");
    let mut lines = text.split("\r\n");
    let request_line = lines.next().expect("request line should be present");
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .expect("method should be present")
        .to_string();
    let path = request_parts
        .next()
        .expect("path should be present")
        .to_string();

    let headers = lines
        .take_while(|line| !line.is_empty())
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_string()))
        .collect();

    CapturedRequest {
        method,
        path,
        headers,
    }
}

fn write_response(stream: &mut std::net::TcpStream, response: TestResponse) {
    let mut response_bytes = format!("HTTP/1.1 {}\r\n", response.status_line).into_bytes();
    for (name, value) in response.headers {
        response_bytes.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
    }
    response_bytes.extend_from_slice(b"Connection: close\r\n\r\n");
    response_bytes.extend_from_slice(&response.body);

    stream
        .write_all(&response_bytes)
        .expect("response should be written");
}

fn full_or_ranged_response(request: &CapturedRequest, object: &[u8]) -> TestResponse {
    let Some(range) = request.headers.get("range") else {
        return TestResponse {
            status_line: "200 OK",
            headers: vec![("Content-Length".to_string(), object.len().to_string())],
            body: object.to_vec(),
        };
    };

    let (start, end) = resolve_range(range, object.len());
    if start > end || end >= object.len() {
        return TestResponse {
            status_line: "416 Range Not Satisfiable",
            headers: vec![
                ("Content-Length".to_string(), "0".to_string()),
                (
                    "Content-Range".to_string(),
                    format!("bytes */{}", object.len()),
                ),
            ],
            body: Vec::new(),
        };
    }

    let body = object[start..=end].to_vec();
    TestResponse {
        status_line: "206 Partial Content",
        headers: vec![
            ("Content-Length".to_string(), body.len().to_string()),
            (
                "Content-Range".to_string(),
                format!("bytes {start}-{end}/{}", object.len()),
            ),
        ],
        body,
    }
}

fn resolve_range(range_header: &str, object_len: usize) -> (usize, usize) {
    let range = range_header
        .strip_prefix("bytes=")
        .expect("test server expects byte ranges");

    if let Some(suffix) = range.strip_prefix('-') {
        let suffix_len: usize = suffix.parse().expect("suffix length should be valid");
        let start = object_len.saturating_sub(suffix_len);
        return (start, object_len.saturating_sub(1));
    }

    let (start, end) = range
        .split_once('-')
        .expect("range should contain a start/end separator");
    let start: usize = start.parse().expect("range start should be valid");

    if end.is_empty() {
        return (start, object_len.saturating_sub(1));
    }

    let end: usize = end.parse().expect("range end should be valid");
    (start, end)
}
