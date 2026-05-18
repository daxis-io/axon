use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};

use bytes::Bytes;
use query_contract::{
    BrokeredObjectAccess, FallbackReason, ObjectGrantBatchSignRequest,
    ObjectGrantBatchSignResponse, ObjectGrantHeadRequest, ObjectGrantListRequest,
    ObjectGrantListResponse, ObjectGrantObject, ObjectGrantRangeRequest, ObjectGrantSignedUrl,
    QueryError, QueryErrorCode,
};
use wasm_http_object_store::{
    BrokerFuture, BrokeredObjectStore, HttpRangeReader, ObjectGrantBrokerClient,
};

#[derive(Clone, Debug, Default)]
struct FakeBrokerClient {
    state: Arc<Mutex<FakeBrokerState>>,
}

#[derive(Debug, Default)]
struct FakeBrokerState {
    objects: BTreeMap<String, ObjectGrantObject>,
    signed_urls: BTreeMap<String, String>,
    signed_url_expirations: BTreeMap<String, u64>,
    proxy_ranges: BTreeMap<(String, u64, u64), Bytes>,
    batch_sign_error: Option<QueryError>,
    proxy_range_error: Option<QueryError>,
    list_calls: Vec<(String, String)>,
    head_calls: Vec<(String, String)>,
    batch_sign_calls: Vec<(String, Vec<String>)>,
    proxy_range_calls: Vec<(String, ObjectGrantRangeRequest)>,
}

impl FakeBrokerClient {
    fn with_state(state: FakeBrokerState) -> Self {
        Self {
            state: Arc::new(Mutex::new(state)),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, FakeBrokerState> {
        self.state.lock().expect("fake broker state lock")
    }
}

impl ObjectGrantBrokerClient for FakeBrokerClient {
    fn list<'a>(
        &'a self,
        grant_id: &'a str,
        request: ObjectGrantListRequest,
    ) -> BrokerFuture<'a, ObjectGrantListResponse> {
        Box::pin(async move {
            let mut state = self.state();
            state
                .list_calls
                .push((grant_id.to_string(), request.prefix.clone()));
            Ok(ObjectGrantListResponse {
                objects: state
                    .objects
                    .values()
                    .filter(|object| object.path.starts_with(&request.prefix))
                    .cloned()
                    .collect(),
            })
        })
    }

    fn head<'a>(
        &'a self,
        grant_id: &'a str,
        request: ObjectGrantHeadRequest,
    ) -> BrokerFuture<'a, ObjectGrantObject> {
        Box::pin(async move {
            let mut state = self.state();
            state
                .head_calls
                .push((grant_id.to_string(), request.path.clone()));
            state.objects.get(&request.path).cloned().ok_or_else(|| {
                QueryError::new(
                    QueryErrorCode::ObjectNotFound,
                    format!("object '{}' not found in fake grant", request.path),
                    query_contract::ExecutionTarget::BrowserWasm,
                )
            })
        })
    }

    fn batch_sign<'a>(
        &'a self,
        grant_id: &'a str,
        request: ObjectGrantBatchSignRequest,
    ) -> BrokerFuture<'a, ObjectGrantBatchSignResponse> {
        Box::pin(async move {
            let mut state = self.state();
            if let Some(error) = state.batch_sign_error.clone() {
                return Err(error);
            }
            state
                .batch_sign_calls
                .push((grant_id.to_string(), request.paths.clone()));
            Ok(ObjectGrantBatchSignResponse {
                signed_urls: request
                    .paths
                    .iter()
                    .map(|path| ObjectGrantSignedUrl {
                        path: path.clone(),
                        url: state
                            .signed_urls
                            .get(path)
                            .cloned()
                            .expect("fake signed URL should exist"),
                        expires_at_epoch_ms: state
                            .signed_url_expirations
                            .get(path)
                            .copied()
                            .unwrap_or(1_800_000_000_000),
                    })
                    .collect(),
            })
        })
    }

    fn proxy_range<'a>(
        &'a self,
        grant_id: &'a str,
        request: ObjectGrantRangeRequest,
    ) -> BrokerFuture<'a, Bytes> {
        Box::pin(async move {
            let mut state = self.state();
            if let Some(error) = state.proxy_range_error.clone() {
                return Err(error);
            }
            state
                .proxy_range_calls
                .push((grant_id.to_string(), request.clone()));
            state
                .proxy_ranges
                .get(&(request.path.clone(), request.start, request.end))
                .cloned()
                .ok_or_else(|| {
                    QueryError::new(
                        QueryErrorCode::ObjectStoreProtocol,
                        "fake proxy range was missing",
                        query_contract::ExecutionTarget::BrowserWasm,
                    )
                })
        })
    }
}

#[tokio::test]
async fn brokered_object_store_lists_and_heads_through_grant_client() {
    let client = FakeBrokerClient::with_state(FakeBrokerState {
        objects: BTreeMap::from([
            (
                "_delta_log/00000000000000000000.json".to_string(),
                ObjectGrantObject {
                    path: "_delta_log/00000000000000000000.json".to_string(),
                    size_bytes: 128,
                    etag: Some("\"log\"".to_string()),
                },
            ),
            (
                "part-000.parquet".to_string(),
                ObjectGrantObject {
                    path: "part-000.parquet".to_string(),
                    size_bytes: 512,
                    etag: Some("\"part\"".to_string()),
                },
            ),
        ]),
        ..FakeBrokerState::default()
    });
    let store = BrokeredObjectStore::new("grant-1", all_capabilities(), client.clone());

    let listed = store
        .list("_delta_log/")
        .await
        .expect("list should call the grant client");
    let metadata = store
        .head("part-000.parquet")
        .await
        .expect("head should call the grant client");

    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].path, "_delta_log/00000000000000000000.json");
    assert_eq!(metadata.size_bytes, 512);
    assert_eq!(
        client.state().list_calls,
        vec![("grant-1".to_string(), "_delta_log/".to_string())]
    );
    assert_eq!(
        client.state().head_calls,
        vec![("grant-1".to_string(), "part-000.parquet".to_string())]
    );
}

#[tokio::test]
async fn brokered_object_store_reads_ranges_via_batch_signed_urls() {
    let (url, requests, server) = spawn_test_server(|request| {
        assert_eq!(request.path.split('?').next(), Some("/part-000.parquet"));
        assert_eq!(request.headers.get("range"), Some(&"bytes=2-5".to_string()));
        TestResponse {
            status_line: "206 Partial Content",
            headers: vec![
                ("Content-Length".to_string(), "4".to_string()),
                ("Content-Range".to_string(), "bytes 2-5/10".to_string()),
                ("ETag".to_string(), "\"part\"".to_string()),
            ],
            body: b"cdef".to_vec(),
        }
    });
    let signed_url = format!("{url}?sig=super-secret#fragment");
    let client = FakeBrokerClient::with_state(FakeBrokerState {
        signed_urls: BTreeMap::from([("part-000.parquet".to_string(), signed_url)]),
        ..FakeBrokerState::default()
    });
    let store = BrokeredObjectStore::with_http_reader(
        "grant-1",
        BrokeredObjectAccess {
            batch_sign: true,
            range_get: true,
            ..no_capabilities()
        },
        client.clone(),
        HttpRangeReader::new(),
    );

    let bytes = store
        .get_range("part-000.parquet", 2, 6)
        .await
        .expect("range reads should use the signed URL returned by the broker");

    let request = finish_request(server, requests);
    assert_eq!(request.headers.get("range"), Some(&"bytes=2-5".to_string()));
    assert_eq!(bytes.as_ref(), b"cdef");
    assert_eq!(
        client.state().batch_sign_calls,
        vec![("grant-1".to_string(), vec!["part-000.parquet".to_string()])]
    );
}

#[tokio::test]
async fn brokered_object_store_reads_full_objects_through_proxy_ranges() {
    let client = FakeBrokerClient::with_state(FakeBrokerState {
        objects: BTreeMap::from([(
            "part-000.parquet".to_string(),
            ObjectGrantObject {
                path: "part-000.parquet".to_string(),
                size_bytes: 4,
                etag: Some("\"part\"".to_string()),
            },
        )]),
        proxy_ranges: BTreeMap::from([(
            ("part-000.parquet".to_string(), 0, 4),
            Bytes::from_static(b"PAR1"),
        )]),
        ..FakeBrokerState::default()
    });
    let store = BrokeredObjectStore::new(
        "grant-1",
        BrokeredObjectAccess {
            head: true,
            get: true,
            range_get: true,
            proxy_range: true,
            ..no_capabilities()
        },
        client.clone(),
    );

    let bytes = store
        .get("part-000.parquet")
        .await
        .expect("full reads should use a broker proxy range when signed URLs are unavailable");

    assert_eq!(bytes.as_ref(), b"PAR1");
    assert_eq!(
        client.state().head_calls,
        vec![("grant-1".to_string(), "part-000.parquet".to_string())]
    );
    assert_eq!(
        client.state().proxy_range_calls,
        vec![(
            "grant-1".to_string(),
            ObjectGrantRangeRequest {
                path: "part-000.parquet".to_string(),
                start: 0,
                end: 4
            }
        )]
    );
}

#[tokio::test]
async fn brokered_object_store_reads_ranges_through_proxy_ranges_when_signing_is_unavailable() {
    let client = FakeBrokerClient::with_state(FakeBrokerState {
        proxy_ranges: BTreeMap::from([(
            ("part-000.parquet".to_string(), 2, 6),
            Bytes::from_static(b"cdef"),
        )]),
        ..FakeBrokerState::default()
    });
    let store = BrokeredObjectStore::new(
        "grant-1",
        BrokeredObjectAccess {
            range_get: true,
            proxy_range: true,
            ..no_capabilities()
        },
        client.clone(),
    );

    let bytes = store
        .get_range("part-000.parquet", 2, 6)
        .await
        .expect("range reads should use broker proxy range when signing is unavailable");

    assert_eq!(bytes.as_ref(), b"cdef");
    assert_eq!(
        client.state().proxy_range_calls,
        vec![(
            "grant-1".to_string(),
            ObjectGrantRangeRequest {
                path: "part-000.parquet".to_string(),
                start: 2,
                end: 6
            }
        )]
    );
}

#[tokio::test]
async fn brokered_object_store_rejects_proxy_range_length_mismatches() {
    let client = FakeBrokerClient::with_state(FakeBrokerState {
        proxy_ranges: BTreeMap::from([(
            ("part-000.parquet".to_string(), 2, 6),
            Bytes::from_static(b"cd"),
        )]),
        ..FakeBrokerState::default()
    });
    let store = BrokeredObjectStore::new(
        "grant-1",
        BrokeredObjectAccess {
            range_get: true,
            proxy_range: true,
            ..no_capabilities()
        },
        client,
    );

    let error = store
        .get_range("part-000.parquet", 2, 6)
        .await
        .expect_err("proxy range reads must validate exact byte counts");

    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert!(error.message.contains("returned 2 bytes"));
    assert!(!error.message.contains("part-000.parquet"));
}

#[tokio::test]
async fn brokered_object_store_propagates_broker_auth_errors_without_retrying_as_secreted_client() {
    let client = FakeBrokerClient::with_state(FakeBrokerState {
        proxy_range_error: Some(QueryError::new(
            QueryErrorCode::AccessDenied,
            "object grant broker denied the proxy range request",
            query_contract::ExecutionTarget::BrowserWasm,
        )),
        ..FakeBrokerState::default()
    });
    let store = BrokeredObjectStore::new(
        "grant-1",
        BrokeredObjectAccess {
            range_get: true,
            proxy_range: true,
            ..no_capabilities()
        },
        client,
    );

    let error = store
        .get_range("part-000.parquet", 2, 6)
        .await
        .expect_err("broker auth failures should be surfaced to the runtime");

    assert_eq!(error.code, QueryErrorCode::AccessDenied);
    assert!(!error.message.contains("dapi"));
    assert!(!error.message.contains("secret"));
}

#[tokio::test]
async fn brokered_object_store_surfaces_mock_grant_expiry_as_fallback() {
    let client = FakeBrokerClient::with_state(FakeBrokerState {
        batch_sign_error: Some(
            QueryError::new(
                QueryErrorCode::FallbackRequired,
                "object grant expired before signing completed",
                query_contract::ExecutionTarget::BrowserWasm,
            )
            .with_fallback_reason(FallbackReason::SignedUrlExpired),
        ),
        ..FakeBrokerState::default()
    });
    let store = BrokeredObjectStore::new(
        "grant-1",
        BrokeredObjectAccess {
            batch_sign: true,
            range_get: true,
            ..no_capabilities()
        },
        client,
    );

    let error = store
        .get_range("part-000.parquet", 0, 4)
        .await
        .expect_err("mock brokers should be able to fake grant expiry");

    assert_eq!(error.code, QueryErrorCode::FallbackRequired);
    assert_eq!(
        error.fallback_reason,
        Some(FallbackReason::SignedUrlExpired)
    );
}

#[tokio::test]
async fn brokered_object_store_rejects_expired_signed_urls_before_http_read() {
    let client = FakeBrokerClient::with_state(FakeBrokerState {
        signed_urls: BTreeMap::from([(
            "part-000.parquet".to_string(),
            "http://127.0.0.1:9/part-000.parquet?sig=super-secret".to_string(),
        )]),
        signed_url_expirations: BTreeMap::from([("part-000.parquet".to_string(), 1)]),
        ..FakeBrokerState::default()
    });
    let store = BrokeredObjectStore::new(
        "grant-1",
        BrokeredObjectAccess {
            batch_sign: true,
            range_get: true,
            ..no_capabilities()
        },
        client,
    );

    let error = store
        .get_range("part-000.parquet", 0, 4)
        .await
        .expect_err("expired signed URLs should fail before the HTTP reader is invoked");

    assert_eq!(error.code, QueryErrorCode::FallbackRequired);
    assert_eq!(
        error.fallback_reason,
        Some(FallbackReason::SignedUrlExpired)
    );
    assert!(!error.message.contains("super-secret"));
}

#[tokio::test]
async fn brokered_object_store_reports_fallback_when_range_capability_is_missing() {
    let store = BrokeredObjectStore::new(
        "grant-1",
        BrokeredObjectAccess {
            batch_sign: true,
            ..no_capabilities()
        },
        FakeBrokerClient::default(),
    );

    let error = store
        .get_range("part-000.parquet", 0, 4)
        .await
        .expect_err("range reads should require an advertised broker range capability");

    assert_eq!(error.code, QueryErrorCode::FallbackRequired);
    assert!(!error.message.contains("part-000.parquet"));
}

#[tokio::test]
async fn brokered_object_store_rejects_invalid_signed_urls_without_leaking_secrets() {
    let client = FakeBrokerClient::with_state(FakeBrokerState {
        signed_urls: BTreeMap::from([(
            "part-000.parquet".to_string(),
            "ftp://objects.example.test/part-000.parquet?sig=super-secret#fragment".to_string(),
        )]),
        ..FakeBrokerState::default()
    });
    let store = BrokeredObjectStore::new(
        "grant-1",
        BrokeredObjectAccess {
            batch_sign: true,
            range_get: true,
            ..no_capabilities()
        },
        client,
    );

    let error = store
        .get_range("part-000.parquet", 0, 4)
        .await
        .expect_err("production brokered reads should validate signed URL schemes");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error
        .message
        .contains("ftp://objects.example.test/part-000.parquet"));
    assert!(!error.message.contains("super-secret"));
    assert!(!error.message.contains("fragment"));
}

fn all_capabilities() -> BrokeredObjectAccess {
    BrokeredObjectAccess {
        list: true,
        head: true,
        get: true,
        range_get: true,
        batch_sign: true,
        proxy_range: true,
    }
}

fn no_capabilities() -> BrokeredObjectAccess {
    BrokeredObjectAccess {
        list: false,
        head: false,
        get: false,
        range_get: false,
        batch_sign: false,
        proxy_range: false,
    }
}

#[derive(Debug)]
struct CapturedRequest {
    path: String,
    headers: BTreeMap<String, String>,
}

struct TestResponse {
    status_line: &'static str,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

fn spawn_test_server<F>(handler: F) -> (String, mpsc::Receiver<CapturedRequest>, JoinHandle<()>)
where
    F: FnOnce(CapturedRequest) -> TestResponse + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral port should bind");
    let addr = listener.local_addr().expect("local addr should resolve");
    let (request_tx, request_rx) = mpsc::channel();

    let server = thread::spawn(move || {
        let (mut stream, _) = listener
            .accept()
            .expect("test server should accept request");
        let mut buffer = [0_u8; 8192];
        let bytes = stream
            .read(&mut buffer)
            .expect("test server should read request");
        let request = parse_request(&String::from_utf8_lossy(&buffer[..bytes]));
        let response = handler(request);
        request_tx
            .send(parse_request(&String::from_utf8_lossy(&buffer[..bytes])))
            .expect("captured request should send");

        write!(
            stream,
            "HTTP/1.1 {}\r\nConnection: close\r\n",
            response.status_line
        )
        .expect("status line should write");
        for (name, value) in response.headers {
            write!(stream, "{name}: {value}\r\n").expect("header should write");
        }
        write!(stream, "\r\n").expect("header terminator should write");
        stream.write_all(&response.body).expect("body should write");
    });

    (
        format!("http://{addr}/part-000.parquet"),
        request_rx,
        server,
    )
}

fn finish_request(
    server: JoinHandle<()>,
    requests: mpsc::Receiver<CapturedRequest>,
) -> CapturedRequest {
    let request = requests.recv().expect("test server should capture request");
    server.join().expect("test server should shut down cleanly");
    request
}

fn parse_request(raw: &str) -> CapturedRequest {
    let mut lines = raw.lines();
    let request_line = lines.next().expect("request line should exist");
    let mut request_parts = request_line.split_whitespace();
    let _method = request_parts.next().expect("method should exist");
    let path = request_parts.next().expect("path should exist").to_string();
    let mut headers = BTreeMap::new();
    for line in lines {
        if line.trim().is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }

    CapturedRequest { path, headers }
}
