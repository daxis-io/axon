use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread::{self, JoinHandle};

use query_contract::{QueryErrorCode, SnapshotResolutionRequest};
use wasm_delta_snapshot::{
    BrowserDeltaLogManifest, BrowserDeltaLogObject, BrowserHttpDeltaLogStorageHandler,
    DefaultJsonHandler, DefaultParquetHandler, SnapshotResolver, StorageHandler,
};

#[test]
fn browser_delta_log_manifest_lists_delta_log_paths_in_sorted_order() {
    let manifest = BrowserDeltaLogManifest::new(
        "gs://bucket/table",
        vec![
            BrowserDeltaLogObject::new(
                "_delta_log/00000000000000000001.json",
                "https://example.com/table/_delta_log/1.json?sig=secret",
            ),
            BrowserDeltaLogObject::new(
                "_delta_log/00000000000000000000.json",
                "https://example.com/table/_delta_log/0.json?sig=secret",
            ),
        ],
    )
    .expect("manifest should validate");

    assert_eq!(
        manifest.list_paths("_delta_log"),
        vec![
            "_delta_log/00000000000000000000.json".to_string(),
            "_delta_log/00000000000000000001.json".to_string(),
        ]
    );
}

#[test]
fn browser_delta_log_manifest_rejects_duplicate_or_escaping_paths() {
    let duplicate = BrowserDeltaLogManifest::new(
        "gs://bucket/table",
        vec![
            BrowserDeltaLogObject::new(
                "_delta_log/00000000000000000000.json",
                "https://example.com/a",
            ),
            BrowserDeltaLogObject::new(
                "_delta_log/00000000000000000000.json",
                "https://example.com/b",
            ),
        ],
    )
    .expect_err("duplicate log paths should fail");
    assert!(duplicate.message.contains("duplicate"));

    let escaping = BrowserDeltaLogManifest::new(
        "gs://bucket/table",
        vec![BrowserDeltaLogObject::new(
            "../_delta_log/00000000000000000000.json",
            "https://example.com/a",
        )],
    )
    .expect_err("escaping paths should fail");
    assert!(escaping.message.contains("_delta_log"));
}

#[tokio::test]
async fn browser_http_delta_log_storage_replays_json_commits() {
    let server = StaticHttpServer::new([
        (
            "/_delta_log/00000000000000000000.json",
            concat!(
                "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n",
                "{\"metaData\":{\"id\":\"test\",\"format\":{\"provider\":\"parquet\",\"options\":{}},",
                "\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":false,\\\"metadata\\\":{}}]}\",",
                "\"partitionColumns\":[],\"configuration\":{}}}\n",
                "{\"add\":{\"path\":\"data/a.parquet\",\"size\":10,\"partitionValues\":{}}}\n"
            ),
        ),
        (
            "/_delta_log/00000000000000000001.json",
            "{\"remove\":{\"path\":\"data/a.parquet\"}}\n{\"add\":{\"path\":\"data/b.parquet\",\"size\":20,\"partitionValues\":{}}}\n",
        ),
    ]);
    let manifest = BrowserDeltaLogManifest::new(
        "gs://bucket/table",
        vec![
            BrowserDeltaLogObject::new(
                "_delta_log/00000000000000000000.json",
                server.url("/_delta_log/00000000000000000000.json"),
            ),
            BrowserDeltaLogObject::new(
                "_delta_log/00000000000000000001.json",
                server.url("/_delta_log/00000000000000000001.json"),
            ),
        ],
    )
    .expect("manifest should validate");

    let resolver = SnapshotResolver::new(
        BrowserHttpDeltaLogStorageHandler::new(manifest),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );
    let snapshot = resolver
        .resolve_snapshot(SnapshotResolutionRequest {
            table_uri: "gs://bucket/table".to_string(),
            snapshot_version: None,
        })
        .await
        .expect("HTTP Delta log should resolve");

    assert_eq!(snapshot.snapshot_version, 1);
    assert_eq!(snapshot.active_files[0].path, "data/b.parquet");
}

#[tokio::test]
async fn browser_http_storage_rejects_manifest_size_mismatches() {
    let server = StaticHttpServer::new([(
        "/_delta_log/00000000000000000000.json",
        "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n",
    )]);
    let manifest = BrowserDeltaLogManifest::new(
        "gs://bucket/table",
        vec![BrowserDeltaLogObject::new(
            "_delta_log/00000000000000000000.json",
            server.url("/_delta_log/00000000000000000000.json"),
        )
        .with_metadata(Some(999), None)],
    )
    .expect("manifest should validate");

    let storage = BrowserHttpDeltaLogStorageHandler::new(manifest);
    let error = storage
        .read_bytes("gs://bucket/table", "_delta_log/00000000000000000000.json")
        .await
        .expect_err("size mismatch should fail");

    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert!(error.message.contains("size changed"));
}

#[tokio::test]
async fn browser_http_storage_rejects_manifest_etag_mismatches() {
    let server = StaticHttpServer::with_responses([(
        "/_delta_log/00000000000000000000.json",
        TestHttpResponse::ok_with_headers(
            [("ETag", "\"v2\"")],
            "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n",
        ),
    )]);
    let manifest = BrowserDeltaLogManifest::new(
        "gs://bucket/table",
        vec![BrowserDeltaLogObject::new(
            "_delta_log/00000000000000000000.json",
            server.url("/_delta_log/00000000000000000000.json"),
        )
        .with_metadata(None, Some("\"v1\"".to_string()))],
    )
    .expect("manifest should validate");

    let storage = BrowserHttpDeltaLogStorageHandler::new(manifest);
    let error = storage
        .read_bytes("gs://bucket/table", "_delta_log/00000000000000000000.json")
        .await
        .expect_err("ETag mismatch should fail");

    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert!(error.message.contains("ETag"));
}

#[tokio::test]
async fn browser_http_storage_redacts_signed_url_secrets_from_status_errors() {
    let signed_path = "/_delta_log/00000000000000000000.json?X-Goog-Signature=secret";
    let server = StaticHttpServer::with_responses([(
        signed_path,
        TestHttpResponse::with_status("403 Forbidden", ""),
    )]);
    let manifest = BrowserDeltaLogManifest::new(
        "gs://bucket/table",
        vec![BrowserDeltaLogObject::new(
            "_delta_log/00000000000000000000.json",
            server.url(&format!("{signed_path}#frag")),
        )],
    )
    .expect("manifest should validate");

    let storage = BrowserHttpDeltaLogStorageHandler::new(manifest);
    let error = storage
        .read_bytes("gs://bucket/table", "_delta_log/00000000000000000000.json")
        .await
        .expect_err("HTTP status failures should surface");

    assert_eq!(error.code, QueryErrorCode::AccessDenied);
    assert!(!error.message.contains("secret"));
    assert!(!error.message.contains("X-Goog-Signature"));
    assert!(!error.message.contains("frag"));
}

#[test]
fn browser_delta_log_manifest_redacts_signed_url_query_strings() {
    let error = BrowserDeltaLogManifest::new(
        "gs://bucket/table",
        vec![BrowserDeltaLogObject::new(
            "_delta_log/00000000000000000000.json",
            "http://storage.example.com/object?X-Goog-Signature=secret#frag",
        )],
    )
    .expect_err("plain HTTP should fail outside loopback");

    assert_eq!(error.code, QueryErrorCode::SecurityPolicyViolation);
    assert!(!error.message.contains("secret"));
    assert!(!error.message.contains("X-Goog-Signature"));
    assert!(!error.message.contains("frag"));
}

struct StaticHttpServer {
    address: String,
    _server: JoinHandle<()>,
}

impl StaticHttpServer {
    fn new(routes: impl IntoIterator<Item = (&'static str, &'static str)>) -> Self {
        Self::with_responses(
            routes
                .into_iter()
                .map(|(path, body)| (path, TestHttpResponse::ok(body))),
        )
    }

    fn with_responses(routes: impl IntoIterator<Item = (&'static str, TestHttpResponse)>) -> Self {
        let bodies = routes
            .into_iter()
            .map(|(path, response)| (path.to_string(), response))
            .collect::<BTreeMap<_, _>>();
        let request_count = bodies.len();
        let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral port should bind");
        let address = listener.local_addr().expect("listener addr should resolve");
        let server = thread::spawn(move || {
            for _ in 0..request_count {
                let (mut stream, _) = listener.accept().expect("test client should connect");
                let path = read_request_path(&mut stream);
                let Some(body) = bodies.get(&path) else {
                    write_response(
                        &mut stream,
                        &TestHttpResponse::with_status("404 Not Found", ""),
                    );
                    continue;
                };
                write_response(&mut stream, body);
            }
        });

        Self {
            address: address.to_string(),
            _server: server,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{}", self.address, path)
    }
}

struct TestHttpResponse {
    status_line: &'static str,
    headers: Vec<(&'static str, &'static str)>,
    body: Vec<u8>,
}

impl TestHttpResponse {
    fn ok(body: &'static str) -> Self {
        Self::with_status("200 OK", body)
    }

    fn ok_with_headers(
        headers: impl IntoIterator<Item = (&'static str, &'static str)>,
        body: &'static str,
    ) -> Self {
        Self {
            status_line: "200 OK",
            headers: headers.into_iter().collect(),
            body: body.as_bytes().to_vec(),
        }
    }

    fn with_status(status_line: &'static str, body: &'static str) -> Self {
        Self {
            status_line,
            headers: Vec::new(),
            body: body.as_bytes().to_vec(),
        }
    }
}

fn read_request_path(stream: &mut std::net::TcpStream) -> String {
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
    let request_line = text.lines().next().expect("request line should exist");
    request_line
        .split_whitespace()
        .nth(1)
        .expect("request path should exist")
        .to_string()
}

fn write_response(stream: &mut std::net::TcpStream, response: &TestHttpResponse) {
    let headers = format!(
        "HTTP/1.1 {}\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n",
        response.status_line,
        response.body.len(),
        response
            .headers
            .iter()
            .map(|(name, value)| format!("{name}: {value}\r\n"))
            .collect::<String>()
    );
    stream
        .write_all(headers.as_bytes())
        .expect("response headers should be written");
    stream
        .write_all(&response.body)
        .expect("response body should be written");
}
