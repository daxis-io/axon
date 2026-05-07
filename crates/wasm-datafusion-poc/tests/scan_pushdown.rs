#![cfg(not(target_arch = "wasm32"))]

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use arrow_array::{cast::AsArray, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::SessionContext;
use parquet::data_type::Int64Type;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use query_contract::PartitionColumnType;
use wasm_datafusion_poc::{
    AxonDeltaTableProvider, AxonParquetScanExec, DeltaActiveFile, DeltaTableDescriptor,
};

#[tokio::test]
async fn scan_trace_records_projected_columns_and_limit_from_datafusion() {
    let ctx = context_with_events();
    let plan = physical_plan(&ctx, "SELECT id FROM events LIMIT 2").await;
    let scan = axon_scan(&plan);
    let trace = scan.pushdown_trace();

    assert_eq!(trace.projected_columns, vec!["id"]);
    assert_eq!(trace.limit, Some(2));
    assert_eq!(trace.files_skipped, 0);
    assert_eq!(trace.row_groups_skipped, 0);
    assert_eq!(trace.bytes_fetched, 0);
    assert_eq!(trace.rows_emitted, 0);
}

#[tokio::test]
async fn pushed_limit_bounds_rows_emitted_by_scan() {
    let ctx = context_with_single_partition_events();
    let plan = physical_plan(&ctx, "SELECT id FROM events LIMIT 1").await;
    let scan = axon_scan(&plan);

    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("query should execute");
    let trace = scan.pushdown_trace();

    assert_eq!(int32_column_values(&batches, 0), vec![1]);
    assert_eq!(trace.limit, Some(1));
    assert_eq!(trace.rows_emitted, 1);
}

#[tokio::test]
async fn exact_partition_filter_prunes_files_in_scan_trace() {
    let ctx = context_with_events();
    let plan = physical_plan(
        &ctx,
        "SELECT id FROM events WHERE event_date = '2026-01-02'",
    )
    .await;
    let scan = axon_scan(&plan);
    let trace = scan.pushdown_trace();

    assert_eq!(trace.projected_columns, vec!["id"]);
    assert_eq!(trace.files_total, 3);
    assert_eq!(trace.files_planned, 1);
    assert_eq!(trace.files_skipped, 2);
    assert_eq!(
        trace.planned_file_paths,
        vec!["event_date=2026-01-02/part-001.parquet"]
    );
}

#[tokio::test]
async fn null_equality_partition_filter_is_not_exact_pushdown() {
    let provider = AxonDeltaTableProvider::with_record_batch_partitions(
        nullable_delta_descriptor(),
        nullable_partitions(nullable_descriptor_schema()),
    );
    let filter = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(Expr::Column(Column::new_unqualified("event_date"))),
        Operator::Eq,
        Box::new(Expr::Literal(ScalarValue::Null, None)),
    ));

    let pushdown = provider
        .supports_filters_pushdown(&[&filter])
        .expect("pushdown support should be classified");

    assert_eq!(pushdown, vec![TableProviderFilterPushDown::Inexact]);
}

#[tokio::test]
async fn inexact_residual_filter_stays_above_partition_pruned_scan() {
    let ctx = context_with_events();
    let df = ctx
        .sql(
            "SELECT id \
             FROM events \
             WHERE event_date = '2026-01-02' AND value > 10 \
             ORDER BY id",
        )
        .await
        .expect("SQL should plan");
    let plan = df
        .create_physical_plan()
        .await
        .expect("physical plan should build");
    let scan = axon_scan(&plan);
    let before = scan.pushdown_trace();

    assert_eq!(before.files_skipped, 2);
    assert_eq!(before.projected_columns, vec!["id", "value"]);

    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("query should execute");
    let after = scan.pushdown_trace();

    assert_eq!(int32_column_values(&batches, 0), vec![4]);
    assert_eq!(after.files_skipped, 2);
    assert_eq!(after.rows_emitted, 2);
    assert_eq!(after.bytes_fetched, 0);
    assert_eq!(after.row_groups_skipped, 0);
}

#[test]
fn parquet_row_group_pruning_updates_scan_trace_and_keeps_residual_correctness() {
    test_runtime().block_on(async {
        let object = parquet_bytes_with_i64_row_groups(&[&[1_i64, 2, 3], &[10_i64, 11, 12]]);
        let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
        let server = RequestCapturingServer::new(object);
        let ctx = SessionContext::new();

        ctx.register_table(
            "events",
            Arc::new(AxonDeltaTableProvider::new(parquet_delta_descriptor(
                server.url(),
                object_size,
            ))),
        )
        .expect("table should register");

        let plan = physical_plan(&ctx, "SELECT id FROM events WHERE id >= 10 ORDER BY id").await;
        let scan = axon_scan(&plan);
        let batches = collect(Arc::clone(&plan), ctx.task_ctx())
            .await
            .expect("query should execute");
        let trace = scan.pushdown_trace();

        assert_eq!(int64_column_values(&batches, 0), vec![10, 11, 12]);
        assert_eq!(trace.row_groups_skipped, 1);
        assert!(trace.bytes_fetched > 0);
        assert_eq!(trace.rows_emitted, 3);
        assert!(
            server
                .recorded_requests()
                .iter()
                .all(|request| request.headers.contains_key("range")),
            "Parquet scan should use browser range I/O"
        );
    });
}

async fn physical_plan(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
    ctx.sql(sql)
        .await
        .expect("SQL should plan")
        .create_physical_plan()
        .await
        .expect("physical plan should build")
}

fn test_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("test runtime should construct")
}

fn context_with_events() -> SessionContext {
    let ctx = SessionContext::new();
    let schema = descriptor_schema();
    ctx.register_table(
        "events",
        Arc::new(AxonDeltaTableProvider::with_record_batch_partitions(
            delta_descriptor(Arc::clone(&schema)),
            controlled_partitions(schema),
        )),
    )
    .expect("table should register");
    ctx
}

fn context_with_single_partition_events() -> SessionContext {
    let ctx = SessionContext::new();
    let schema = descriptor_schema();
    ctx.register_table(
        "events",
        Arc::new(AxonDeltaTableProvider::with_record_batch_partitions(
            DeltaTableDescriptor {
                table_name: "events".to_string(),
                table_version: 12,
                schema: Arc::clone(&schema),
                partition_columns: Vec::new(),
                partition_column_types: BTreeMap::new(),
                active_files: vec![active_file("part-000.parquet", "", 3)],
            },
            vec![vec![record_batch(
                schema,
                &[1, 2, 3],
                &[5, 25, 7],
                &["", "", ""],
            )]],
        )),
    )
    .expect("table should register");
    ctx
}

fn axon_scan(plan: &Arc<dyn ExecutionPlan>) -> &AxonParquetScanExec {
    if let Some(scan) = plan.as_any().downcast_ref::<AxonParquetScanExec>() {
        return scan;
    }

    plan.children()
        .into_iter()
        .find_map(|child| axon_scan_optional(child.as_ref()))
        .expect("expected AxonParquetScanExec in physical plan")
}

fn axon_scan_optional(plan: &dyn ExecutionPlan) -> Option<&AxonParquetScanExec> {
    if let Some(scan) = plan.as_any().downcast_ref::<AxonParquetScanExec>() {
        return Some(scan);
    }

    plan.children()
        .into_iter()
        .find_map(|child| axon_scan_optional(child.as_ref()))
}

fn controlled_partitions(schema: SchemaRef) -> Vec<Vec<RecordBatch>> {
    vec![
        vec![record_batch(
            Arc::clone(&schema),
            &[1, 2],
            &[5, 25],
            &["2026-01-01", "2026-01-01"],
        )],
        vec![record_batch(
            Arc::clone(&schema),
            &[3, 4],
            &[7, 15],
            &["2026-01-02", "2026-01-02"],
        )],
        vec![record_batch(schema, &[5], &[50], &["2026-01-03"])],
    ]
}

fn record_batch(
    schema: SchemaRef,
    ids: &[i32],
    values: &[i32],
    event_dates: &[&str],
) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(Int32Array::from(values.to_vec())),
            Arc::new(StringArray::from(event_dates.to_vec())),
        ],
    )
    .expect("record batch should construct")
}

fn delta_descriptor(schema: SchemaRef) -> DeltaTableDescriptor {
    DeltaTableDescriptor {
        table_name: "events".to_string(),
        table_version: 12,
        schema,
        partition_columns: vec!["event_date".to_string()],
        partition_column_types: BTreeMap::from([(
            "event_date".to_string(),
            PartitionColumnType::String,
        )]),
        active_files: vec![
            active_file("event_date=2026-01-01/part-000.parquet", "2026-01-01", 2),
            active_file("event_date=2026-01-02/part-001.parquet", "2026-01-02", 2),
            active_file("event_date=2026-01-03/part-002.parquet", "2026-01-03", 1),
        ],
    }
}

fn nullable_delta_descriptor() -> DeltaTableDescriptor {
    DeltaTableDescriptor {
        table_name: "events".to_string(),
        table_version: 13,
        schema: nullable_descriptor_schema(),
        partition_columns: vec!["event_date".to_string()],
        partition_column_types: BTreeMap::from([(
            "event_date".to_string(),
            PartitionColumnType::String,
        )]),
        active_files: vec![
            DeltaActiveFile {
                path: "event_date=null/part-000.parquet".to_string(),
                url: "https://example.test/table/event_date=null/part-000.parquet".to_string(),
                size_bytes: 1024,
                partition_values: BTreeMap::from([("event_date".to_string(), None)]),
                object_etag: None,
                stats_json: Some(r#"{"numRecords":1}"#.to_string()),
                deletion_vector: None,
            },
            active_file("event_date=2026-01-02/part-001.parquet", "2026-01-02", 1),
        ],
    }
}

fn nullable_partitions(schema: SchemaRef) -> Vec<Vec<RecordBatch>> {
    vec![
        vec![nullable_record_batch(
            Arc::clone(&schema),
            &[1],
            &[5],
            &[None],
        )],
        vec![nullable_record_batch(
            schema,
            &[2],
            &[15],
            &[Some("2026-01-02")],
        )],
    ]
}

fn parquet_delta_descriptor(url: String, size_bytes: u64) -> DeltaTableDescriptor {
    DeltaTableDescriptor {
        table_name: "events".to_string(),
        table_version: 14,
        schema: parquet_descriptor_schema(),
        partition_columns: Vec::new(),
        partition_column_types: BTreeMap::new(),
        active_files: vec![DeltaActiveFile {
            path: "part-000.parquet".to_string(),
            url,
            size_bytes,
            partition_values: BTreeMap::new(),
            object_etag: None,
            stats_json: Some(r#"{"numRecords":6}"#.to_string()),
            deletion_vector: None,
        }],
    }
}

fn active_file(path: &str, event_date: &str, rows: u64) -> DeltaActiveFile {
    DeltaActiveFile {
        path: path.to_string(),
        url: format!("https://example.test/table/{path}"),
        size_bytes: 1024,
        partition_values: BTreeMap::from([(
            "event_date".to_string(),
            Some(event_date.to_string()),
        )]),
        object_etag: None,
        stats_json: Some(format!(r#"{{"numRecords":{rows}}}"#)),
        deletion_vector: None,
    }
}

fn nullable_record_batch(
    schema: SchemaRef,
    ids: &[i32],
    values: &[i32],
    event_dates: &[Option<&str>],
) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(Int32Array::from(values.to_vec())),
            Arc::new(StringArray::from(event_dates.to_vec())),
        ],
    )
    .expect("record batch should construct")
}

fn descriptor_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
        Field::new("event_date", DataType::Utf8, true),
    ]))
}

fn nullable_descriptor_schema() -> SchemaRef {
    descriptor_schema()
}

fn parquet_descriptor_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn int32_column_values(batches: &[RecordBatch], column_index: usize) -> Vec<i32> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(column_index)
                .as_primitive::<arrow_array::types::Int32Type>()
                .values()
                .to_vec()
        })
        .collect()
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
            .expect("id column writer should be returned")
        {
            column
                .typed::<Int64Type>()
                .write_batch(values, None, None)
                .expect("id values should write");
            column.close().expect("id column writer should close");
        }
        row_group.close().expect("row-group writer should close");
    }

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
