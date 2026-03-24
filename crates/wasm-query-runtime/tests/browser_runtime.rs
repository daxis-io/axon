#![cfg(not(target_arch = "wasm32"))]

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc::{self, Receiver};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use query_contract::{
    CapabilityKey, CapabilityState, ExecutionTarget, FallbackReason, QueryErrorCode,
};
use wasm_http_object_store::{HttpByteRange, HttpRangeReader};
use wasm_query_runtime::{
    runtime_target, BrowserObjectAccessMode, BrowserObjectSource, BrowserRuntimeConfig,
    BrowserRuntimeSession,
};

#[test]
fn default_config_constructs_a_browser_runtime_session() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    assert_eq!(runtime_target(), ExecutionTarget::BrowserWasm);
    assert_eq!(session.config(), &BrowserRuntimeConfig::default());
}

#[test]
fn https_object_sources_are_constructible() {
    let source = BrowserObjectSource::from_url("https://example.com/object")
        .expect("https object sources should be supported");

    assert_eq!(source.url(), "https://example.com/object");
}

#[test]
fn multi_partition_configs_require_native_fallback() {
    let error = BrowserRuntimeSession::new(BrowserRuntimeConfig {
        target_partitions: 2,
        ..BrowserRuntimeConfig::default()
    })
    .expect_err("multi-partition configs should be rejected");

    assert_eq!(error.code, QueryErrorCode::FallbackRequired);
    assert_eq!(
        error.fallback_reason,
        Some(FallbackReason::CapabilityGate {
            capability: CapabilityKey::MultiPartitionExecution,
            required_state: CapabilityState::NativeOnly,
        })
    );
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn non_http_object_access_modes_fail_with_browser_runtime_constraint() {
    let error = BrowserRuntimeSession::new(BrowserRuntimeConfig {
        object_access_mode: BrowserObjectAccessMode::CloudObjectStore,
        ..BrowserRuntimeConfig::default()
    })
    .expect_err("cloud object store access should be rejected");

    assert_eq!(error.code, QueryErrorCode::FallbackRequired);
    assert_eq!(
        error.fallback_reason,
        Some(FallbackReason::BrowserRuntimeConstraint)
    );
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn cloud_credentials_are_rejected_as_a_security_policy_violation() {
    let error = BrowserRuntimeSession::new(BrowserRuntimeConfig {
        allow_cloud_credentials: true,
        ..BrowserRuntimeConfig::default()
    })
    .expect_err("cloud credentials should be rejected");

    assert_eq!(error.code, QueryErrorCode::SecurityPolicyViolation);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn request_timeouts_must_be_positive() {
    let error = BrowserRuntimeSession::new(BrowserRuntimeConfig {
        request_timeout_ms: 0,
        ..BrowserRuntimeConfig::default()
    })
    .expect_err("zero timeouts should be rejected");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn non_loopback_plain_http_sources_are_rejected() {
    let error = BrowserObjectSource::from_url("http://0.0.0.0:1/object")
        .expect_err("non-loopback plain HTTP should be rejected");

    assert_eq!(error.code, QueryErrorCode::SecurityPolicyViolation);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn unsupported_object_source_schemes_are_rejected() {
    for url in ["file:///tmp/object", "s3://bucket/object"] {
        let error =
            BrowserObjectSource::from_url(url).expect_err("unsupported schemes should be rejected");

        assert_eq!(error.code, QueryErrorCode::InvalidRequest, "url: {url}");
        assert_eq!(error.fallback_reason, None, "url: {url}");
        assert_eq!(error.target, ExecutionTarget::BrowserWasm, "url: {url}");
    }
}

#[test]
fn sessions_can_probe_loopback_http_sources_in_host_tests_through_an_injected_range_reader() {
    let (url, requests, server) =
        spawn_test_server(|request| full_or_ranged_response(request, b"abcdefghij"));
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session =
        BrowserRuntimeSession::with_reader(BrowserRuntimeConfig::default(), HttpRangeReader::new())
            .expect("default config should be supported");

    let result = runtime()
        .block_on(session.probe(
            &source,
            HttpByteRange::Bounded {
                offset: 2,
                length: 4,
            },
        ))
        .expect("probe should succeed");

    let request = finish_request(server, requests);
    assert_eq!(request.headers.get("range"), Some(&"bytes=2-5".to_string()));
    assert_eq!(result.metadata.url, url);
    assert_eq!(result.metadata.size_bytes, Some(10));
    assert_eq!(result.bytes.as_ref(), b"cdef");
}

#[test]
fn new_sessions_apply_request_timeouts_to_probe_requests() {
    let (url, server) = spawn_stalling_server(Duration::from_millis(250));
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig {
        request_timeout_ms: 25,
        ..BrowserRuntimeConfig::default()
    })
    .expect("short timeouts should be supported");

    let error = runtime()
        .block_on(session.probe(&source, HttpByteRange::Full))
        .expect_err("stalled probes should time out");

    server
        .join()
        .expect("stalling test server should shut down cleanly");
    assert_eq!(error.code, QueryErrorCode::ExecutionFailed);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn probe_preserves_http_range_reader_errors() {
    let (url, _, server) = spawn_test_server(|_| TestResponse {
        status_line: "401 Unauthorized",
        headers: vec![("Content-Length".to_string(), "0".to_string())],
        body: Vec::new(),
    });
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let error = runtime()
        .block_on(session.probe(&source, HttpByteRange::Full))
        .expect_err("unauthorized probes should fail");

    server.join().expect("test server should shut down cleanly");
    assert_eq!(error.code, QueryErrorCode::AccessDenied);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("tokio runtime should be created for tests")
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

fn spawn_stalling_server(delay: Duration) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral port should bind");
    let address = listener.local_addr().expect("listener addr should resolve");
    let url = format!("http://{address}/object");

    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("test client should connect");
        let mut buffer = [0_u8; 4096];
        let _ = stream.read(&mut buffer);
        thread::sleep(delay);
    });

    (url, server)
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
    let _request_line = lines.next().expect("request line should be present");
    let mut headers = BTreeMap::new();

    for line in lines.take_while(|line| !line.is_empty()) {
        let (name, value) = line
            .split_once(':')
            .expect("header line should contain a colon");
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }

    CapturedRequest { headers }
}

fn write_response(stream: &mut std::net::TcpStream, response: TestResponse) {
    write!(stream, "HTTP/1.1 {}\r\n", response.status_line)
        .expect("response status should be writable");
    for (header, value) in response.headers {
        write!(stream, "{header}: {value}\r\n").expect("response header should be writable");
    }
    write!(stream, "\r\n").expect("response separator should be writable");
    stream
        .write_all(&response.body)
        .expect("response body should be writable");
    stream.flush().expect("response should flush");
}

fn full_or_ranged_response(request: &CapturedRequest, body: &[u8]) -> TestResponse {
    if let Some(range_header) = request.headers.get("range") {
        let range_spec = range_header
            .strip_prefix("bytes=")
            .expect("range header should start with bytes=");
        let (start, end) = range_spec
            .split_once('-')
            .expect("range header should contain a dash");
        let start = start.parse::<usize>().expect("range start should parse");
        let end = end.parse::<usize>().expect("range end should parse");
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
    } else {
        TestResponse {
            status_line: "200 OK",
            headers: vec![("Content-Length".to_string(), body.len().to_string())],
            body: body.to_vec(),
        }
    }
}
