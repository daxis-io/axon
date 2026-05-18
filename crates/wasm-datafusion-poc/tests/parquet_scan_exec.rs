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
use bytes::Bytes;
use parquet::data_type::Int64Type;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use query_contract::{
    BrokeredDeltaAccessMode, BrokeredDeltaReadPlan, BrokeredObjectAccess, BrokeredPolicyAuthority,
    DirectExternalEngineReadSupport, ObjectGrantBatchSignRequest, ObjectGrantBatchSignResponse,
    ObjectGrantHeadRequest, ObjectGrantListRequest, ObjectGrantListResponse, ObjectGrantObject,
    ObjectGrantRangeRequest, ObjectGrantSignedUrl, PartitionColumnType, PolicyAuthorityKind,
    QueryError, QueryErrorCode, ResolvedFileDescriptor, ResolvedSnapshotDescriptor,
    SnapshotResolutionRequest,
};
use wasm_datafusion_poc::{
    DeltaActiveFile, DeltaTableDescriptor, DeltaTableFieldDataType, DeltaTableSchema,
    DeltaTableSchemaField, WasmDataFusionEngine,
};
use wasm_delta_snapshot::{
    BrokeredDeltaLogStorageHandler, DefaultJsonHandler, DefaultParquetHandler, SnapshotResolver,
};
use wasm_http_object_store::{BrokerFuture, BrokeredObjectStore, ObjectGrantBrokerClient};

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

#[test]
fn brokered_delta_read_plan_opens_datafusion_table_and_returns_fixture_rows() {
    let _guard = PARQUET_SCAN_TEST_LOCK
        .lock()
        .expect("Parquet scan tests should serialize local HTTP servers");
    test_runtime().block_on(async {
        let object = parquet_bytes_with_i64_columns(&[(3, 25), (1, 5), (2, 12)]);
        let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
        let server = RequestCapturingServer::new(object);
        let plan = brokered_delta_plan();
        let resolved_snapshot = ResolvedSnapshotDescriptor {
            table_uri: plan.table_root.clone(),
            snapshot_version: 11,
            partition_column_types: BTreeMap::from([(
                "category".to_string(),
                PartitionColumnType::String,
            )]),
            browser_compatibility: Default::default(),
            required_capabilities: Default::default(),
            active_files: vec![ResolvedFileDescriptor {
                path: "category=B/part-000.parquet".to_string(),
                size_bytes: object_size,
                partition_values: BTreeMap::from([("category".to_string(), Some("B".to_string()))]),
                stats: Some(r#"{"numRecords":3}"#.to_string()),
            }],
        };
        let snapshot = plan
            .to_browser_http_snapshot_descriptor(
                resolved_snapshot,
                &BTreeMap::from([("category=B/part-000.parquet".to_string(), server.url())]),
            )
            .expect("brokered Delta read plan should adapt to the browser descriptor");
        let descriptor = DeltaTableDescriptor::from_browser_http_snapshot(
            "events",
            &snapshot,
            brokered_delta_datafusion_schema(),
        )
        .expect("browser descriptor should convert to a DataFusion table descriptor");
        let mut engine = WasmDataFusionEngine::new();

        engine
            .open_delta_table(descriptor)
            .await
            .expect("brokered descriptor should register as a DataFusion table");

        let (schema, batches) = engine
            .sql_to_record_batches(
                "SELECT id, value \
             FROM events \
             WHERE category = 'B' \
             ORDER BY id",
            )
            .await
            .expect("DataFusion should query rows opened through the brokered plan path");

        assert_eq!(
            schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["id", "value"]
        );
        assert_eq!(int64_column_values(&batches, 0), vec![1, 2, 3]);
        assert_eq!(int64_column_values(&batches, 1), vec![5, 12, 25]);
    });
}

#[test]
fn brokered_delta_runtime_replays_log_signs_files_and_queries_datafusion() {
    let _guard = PARQUET_SCAN_TEST_LOCK
        .lock()
        .expect("Parquet scan tests should serialize local HTTP servers");
    test_runtime().block_on(async {
        let object = parquet_bytes_with_i64_columns(&[(3, 25), (1, 5), (2, 12)]);
        let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
        let server = RequestCapturingServer::new(object);
        let plan = brokered_delta_plan();
        let commit = delta_log_commit_for_data_file(object_size);
        let client = FakeBrokerClient::with_state(FakeBrokerState {
            objects: BTreeMap::from([(
                "_delta_log/00000000000000000000.json".to_string(),
                Bytes::from(commit),
            )]),
            signed_urls: BTreeMap::from([(
                "category=B/part-000.parquet".to_string(),
                server.url(),
            )]),
            ..FakeBrokerState::default()
        });
        let object_store = BrokeredObjectStore::new(
            plan.grant_id.clone(),
            BrokeredObjectAccess {
                list: true,
                head: true,
                get: true,
                range_get: true,
                batch_sign: false,
                proxy_range: true,
            },
            client.clone(),
        );
        let resolver = SnapshotResolver::new(
            BrokeredDeltaLogStorageHandler::new(plan.table_root.clone(), object_store),
            DefaultJsonHandler::default(),
            DefaultParquetHandler::default(),
        );

        let resolved_snapshot = resolver
            .resolve_snapshot(SnapshotResolutionRequest {
                table_uri: plan.table_root.clone(),
                snapshot_version: None,
            })
            .await
            .expect("brokered object grant should reconstruct the Delta snapshot");
        let signed_urls = client
            .batch_sign(
                &plan.grant_id,
                ObjectGrantBatchSignRequest {
                    paths: resolved_snapshot
                        .active_files
                        .iter()
                        .map(|file| file.path.clone())
                        .collect(),
                },
            )
            .await
            .expect("broker should sign active data files")
            .signed_urls
            .into_iter()
            .map(|signed| (signed.path, signed.url))
            .collect::<BTreeMap<_, _>>();
        let snapshot = plan
            .to_browser_http_snapshot_descriptor(resolved_snapshot, &signed_urls)
            .expect("brokered Delta read plan should adapt to the browser descriptor");
        let descriptor = DeltaTableDescriptor::from_browser_http_snapshot(
            "events",
            &snapshot,
            brokered_delta_datafusion_schema(),
        )
        .expect("browser descriptor should convert to a DataFusion table descriptor");
        let mut engine = WasmDataFusionEngine::new();

        engine
            .open_delta_table(descriptor)
            .await
            .expect("brokered descriptor should register as a DataFusion table");

        let (schema, batches) = engine
            .sql_to_record_batches(
                "SELECT id, value \
             FROM events \
             WHERE category = 'B' AND value > 10 \
             ORDER BY id",
            )
            .await
            .expect("DataFusion should query rows opened through the brokered runtime path");

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
        assert_eq!(
            client.state().list_calls,
            vec![("grant-456".to_string(), "_delta_log".to_string())]
        );
        assert!(
            client
                .state()
                .proxy_range_calls
                .iter()
                .any(|(_, request)| request.path == "_delta_log/00000000000000000000.json"),
            "snapshot replay should read Delta log bytes through the broker proxy"
        );
        assert_eq!(
            client.state().batch_sign_calls,
            vec![(
                "grant-456".to_string(),
                vec!["category=B/part-000.parquet".to_string()]
            )]
        );
        assert!(
            server
                .recorded_requests()
                .iter()
                .all(|request| request.headers.contains_key("range")),
            "DataFusion scan should read signed Parquet URLs through browser range I/O"
        );
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

fn brokered_delta_datafusion_schema() -> DeltaTableSchema {
    DeltaTableSchema::new(vec![
        DeltaTableSchemaField::new("id", DeltaTableFieldDataType::Int64, false),
        DeltaTableSchemaField::new("value", DeltaTableFieldDataType::Int64, false),
        DeltaTableSchemaField::new("category", DeltaTableFieldDataType::Utf8, true),
    ])
}

fn brokered_delta_plan() -> BrokeredDeltaReadPlan {
    BrokeredDeltaReadPlan {
        table_id: "tbl-123".to_string(),
        full_name: "main.sales.orders".to_string(),
        table_root: "s3://prod-bucket/tables/orders".to_string(),
        grant_id: "grant-456".to_string(),
        expires_at_epoch_ms: 1_800_000_000_000,
        delta_access_mode: BrokeredDeltaAccessMode::DeltaLog,
        policy_authority: BrokeredPolicyAuthority {
            authority: PolicyAuthorityKind::UnityCatalog,
            direct_external_engine_read: DirectExternalEngineReadSupport::Confirmed,
        },
        object_access: BrokeredObjectAccess {
            list: true,
            head: true,
            get: true,
            range_get: true,
            batch_sign: true,
            proxy_range: true,
        },
    }
}

fn delta_log_commit_for_data_file(size_bytes: u64) -> String {
    format!(
        "{}{}{}",
        "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n",
        "{\"metaData\":{\"id\":\"test\",\"format\":{\"provider\":\"parquet\",\"options\":{}},\
         \"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":false,\\\"metadata\\\":{}},{\\\"name\\\":\\\"value\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":false,\\\"metadata\\\":{}},{\\\"name\\\":\\\"category\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\",\
         \"partitionColumns\":[\"category\"],\"configuration\":{}}}\n",
        format!(
            "{{\"add\":{{\"path\":\"category=B/part-000.parquet\",\"size\":{size_bytes},\"partitionValues\":{{\"category\":\"B\"}},\"stats\":\"{{\\\"numRecords\\\":3}}\"}}}}\n"
        )
    )
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
    write!(stream, "Connection: close\r\n").expect("connection header should write");
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

#[derive(Clone, Debug, Default)]
struct FakeBrokerClient {
    state: Arc<Mutex<FakeBrokerState>>,
}

#[derive(Debug, Default)]
struct FakeBrokerState {
    objects: BTreeMap<String, Bytes>,
    signed_urls: BTreeMap<String, String>,
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
                    .iter()
                    .filter(|(path, _)| path.starts_with(&request.prefix))
                    .map(|(path, bytes)| ObjectGrantObject {
                        path: path.clone(),
                        size_bytes: bytes.len() as u64,
                        etag: None,
                    })
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
            state
                .objects
                .get(&request.path)
                .map(|bytes| ObjectGrantObject {
                    path: request.path,
                    size_bytes: bytes.len() as u64,
                    etag: None,
                })
                .ok_or_else(|| {
                    QueryError::new(
                        QueryErrorCode::ObjectNotFound,
                        "fake broker object was missing",
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
                            .expect("fake broker should have a signed URL for active files"),
                        expires_at_epoch_ms: 1_800_000_000_000,
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
            state
                .proxy_range_calls
                .push((grant_id.to_string(), request.clone()));
            let bytes = state.objects.get(&request.path).cloned().ok_or_else(|| {
                QueryError::new(
                    QueryErrorCode::ObjectNotFound,
                    "fake broker range object was missing",
                    query_contract::ExecutionTarget::BrowserWasm,
                )
            })?;
            Ok(bytes.slice(request.start as usize..request.end as usize))
        })
    }
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
