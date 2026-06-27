#[cfg(not(target_arch = "wasm32"))]
use std::collections::BTreeMap;

#[cfg(target_arch = "wasm32")]
use browser_engine_worker::{
    artifact_report, capabilities, runtime_sku, worker_target, BrowserResultTransport,
};
#[cfg(not(target_arch = "wasm32"))]
use browser_engine_worker::{BrowserWorker, DEFAULT_SESSION_CACHE_BYTES};
#[cfg(not(target_arch = "wasm32"))]
use browser_sdk::{
    ArrowIpcDelivery, ArrowIpcFormat, BrowserWorkerCommand, BrowserWorkerEventEnvelope,
    BrowserWorkerEventPhase, BrowserWorkerProgressStage, BrowserWorkerResponseEnvelope,
    BrowserWorkerSqlDelivery,
};
#[cfg(target_arch = "wasm32")]
use query_contract::{BrowserAccessMode, ExecutionTarget};
#[cfg(not(target_arch = "wasm32"))]
use query_contract::{
    BrowserHttpFileDescriptor, BrowserHttpParquetDatasetDescriptor, BrowserHttpSnapshotDescriptor,
    CapabilityReport, ExecutionTarget, QueryErrorCode, QueryExecutionOptions, QueryRequest,
    QueryResultPage, QueryRuntimeLimits,
};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::wasm_bindgen_test;
#[cfg(not(target_arch = "wasm32"))]
use wasm_query_runtime::BrowserRuntimeConfig;

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen_test]
fn worker_reports_cold_start_and_memory_baseline_in_wasm() {
    let report = artifact_report().expect("worker artifact report should be produced in wasm");

    println!(
        "{}",
        serde_json::to_string(&report).expect("worker artifact report should serialize in wasm")
    );
    assert_eq!(worker_target(), ExecutionTarget::BrowserWasm);
    assert_eq!(report.startup.target, ExecutionTarget::BrowserWasm);
    assert_eq!(report.runtime_sku, runtime_sku());
    assert_eq!(report.result_transport, BrowserResultTransport::ArrowIpc);
    assert_eq!(report.capabilities, capabilities());
    assert!(!report.capabilities.browser_datafusion);
    assert_eq!(
        report.startup.access_mode,
        BrowserAccessMode::BrowserSafeHttp
    );
    assert_eq!(report.identity.package_name, "browser-engine-worker");
    assert_eq!(report.identity.package_version, env!("CARGO_PKG_VERSION"));
    assert_eq!(report.identity.wasm_artifact, "browser_engine_worker.wasm");
    assert!(report.startup.command_envelope_bytes > 0);
    assert!(report.startup.error_envelope_bytes > 0);
    assert!(report.memory.runtime_session_bytes > 0);
    assert!(report.memory.query_session_bytes > 0);
    assert!(report.memory.command_envelope_json_bytes > 0);
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn worker_rejects_sql_before_open_delta_table() {
    let mut worker =
        BrowserWorker::new(BrowserRuntimeConfig::default(), DEFAULT_SESSION_CACHE_BYTES)
            .expect("worker should construct");

    let response = test_runtime().block_on(worker.handle_command(BrowserWorkerCommand::sql(
        "req-sql-before-open",
        "events",
        query_with_explicit_browser_limits(QueryRequest::new(
            "gs://axon-fixtures/empty",
            "SELECT COUNT(*) AS rows FROM events",
            ExecutionTarget::BrowserWasm,
        )),
    )));

    match response {
        BrowserWorkerResponseEnvelope::Error(error) => {
            assert_eq!(error.request_id, "req-sql-before-open");
            assert_eq!(error.error.code, QueryErrorCode::InvalidRequest);
            assert!(error.error.message.contains("open table named 'events'"));
        }
        other => panic!("expected SQL-before-open to fail, got {other:?}"),
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn worker_rejects_raw_sql_without_browser_safe_defaults_or_explicit_limits() {
    let mut worker =
        BrowserWorker::new(BrowserRuntimeConfig::default(), DEFAULT_SESSION_CACHE_BYTES)
            .expect("worker should construct");
    let descriptor = empty_delta_descriptor();

    test_runtime().block_on(
        worker.handle_command(BrowserWorkerCommand::open_delta_table(
            "req-open-delta",
            "events",
            descriptor.clone(),
        )),
    );

    let response = test_runtime().block_on(worker.handle_command(BrowserWorkerCommand::sql(
        "req-sql-raw-missing-limits",
        "events",
        QueryRequest::new(
            descriptor.table_uri,
            "SELECT COUNT(*) AS rows FROM axon_table",
            ExecutionTarget::BrowserWasm,
        ),
    )));

    match response {
        BrowserWorkerResponseEnvelope::Error(error) => {
            assert_eq!(error.request_id, "req-sql-raw-missing-limits");
            assert_eq!(error.error.code, QueryErrorCode::InvalidRequest);
            assert!(
                error.error.message.contains("browser_safe_defaults"),
                "error should identify the browser-safe default contract: {}",
                error.error.message
            );
        }
        other => panic!("expected raw SQL without browser-safe limits to fail, got {other:?}"),
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn worker_instantiation_event_stream_reports_instantiate_phase() {
    let mut events = Vec::new();
    BrowserWorker::new_with_event_sink(
        BrowserRuntimeConfig::default(),
        DEFAULT_SESSION_CACHE_BYTES,
        |event| events.push(event),
    )
    .expect("worker should construct");

    assert!(
        events.iter().all(|event| {
            let context = event.context();
            context.phase == BrowserWorkerEventPhase::Instantiate
                && context.request_id.is_none()
                && context.query_id.is_none()
                && context.table_name.is_none()
        }),
        "instantiate events should not invent command identity: {events:?}"
    );
    assert!(
        events.iter().any(|event| matches!(
            event,
            BrowserWorkerEventEnvelope::Progress(progress)
                if progress.stage == BrowserWorkerProgressStage::Started
        )),
        "instantiate event stream should include a started progress event"
    );
    assert!(
        events.iter().any(|event| matches!(
            event,
            BrowserWorkerEventEnvelope::Progress(progress)
                if progress.stage == BrowserWorkerProgressStage::Finished
        )),
        "instantiate event stream should include a finished progress event"
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn worker_opens_delta_table_and_returns_sql_arrow_ipc_stream() {
    let mut worker =
        BrowserWorker::new(BrowserRuntimeConfig::default(), DEFAULT_SESSION_CACHE_BYTES)
            .expect("worker should construct");
    let descriptor = empty_delta_descriptor();

    let opened = test_runtime().block_on(worker.handle_command(
        BrowserWorkerCommand::open_delta_table("req-open-delta", "events", descriptor.clone()),
    ));
    assert_eq!(
        opened.opened_envelope().expect("opened response").name,
        "events"
    );

    let sql = "SELECT COUNT(*) AS rows FROM axon_table";
    let response = test_runtime().block_on(worker.handle_command(browser_safe_sql_command(
        "req-sql-ipc",
        "events",
        QueryRequest::new(descriptor.table_uri, sql, ExecutionTarget::BrowserWasm),
    )));

    let success = response
        .success_envelope()
        .expect("SQL should return a success envelope");
    assert_eq!(success.request_id, "req-sql-ipc");
    assert_eq!(success.result.format, ArrowIpcFormat::Stream);
    assert_eq!(
        success.result.content_type,
        "application/vnd.apache.arrow.stream"
    );
    assert!(success
        .result
        .bytes
        .as_ref()
        .is_some_and(|bytes| !bytes.is_empty()));
    assert_eq!(success.response.metrics.rows_emitted, 0);
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn worker_opens_parquet_dataset_and_returns_sql_arrow_ipc_stream() {
    let mut worker =
        BrowserWorker::new(BrowserRuntimeConfig::default(), DEFAULT_SESSION_CACHE_BYTES)
            .expect("worker should construct");
    let dataset = empty_parquet_dataset_descriptor();

    let opened = test_runtime().block_on(worker.handle_command(
        BrowserWorkerCommand::open_parquet_dataset(
            "req-open-parquet",
            "plain_events",
            dataset.clone(),
        ),
    ));
    assert_eq!(
        opened.opened_envelope().expect("opened response").name,
        "plain_events"
    );

    let sql = "SELECT COUNT(*) AS rows FROM axon_table";
    let response = test_runtime().block_on(worker.handle_command(browser_safe_sql_command(
        "req-sql-parquet",
        "plain_events",
        QueryRequest::new(dataset.table_uri, sql, ExecutionTarget::BrowserWasm),
    )));

    let success = response
        .success_envelope()
        .expect("SQL should return a success envelope");
    assert_eq!(success.request_id, "req-sql-parquet");
    assert_eq!(success.result.format, ArrowIpcFormat::Stream);
    assert_eq!(
        success.result.content_type,
        "application/vnd.apache.arrow.stream"
    );
    assert!(success
        .result
        .bytes
        .as_ref()
        .is_some_and(|bytes| !bytes.is_empty()));
    assert_eq!(success.response.metrics.files_touched, 0);
    assert_eq!(success.response.metrics.rows_emitted, 0);
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn worker_event_stream_preserves_request_and_query_identity() {
    let mut worker =
        BrowserWorker::new(BrowserRuntimeConfig::default(), DEFAULT_SESSION_CACHE_BYTES)
            .expect("worker should construct");
    let descriptor = empty_delta_descriptor();

    test_runtime().block_on(
        worker.handle_command(BrowserWorkerCommand::open_delta_table(
            "req-open-delta",
            "events",
            descriptor.clone(),
        )),
    );

    let sql = "SELECT COUNT(*) AS rows FROM axon_table";
    let mut events = Vec::new();
    let response = test_runtime().block_on(worker.handle_command_streaming_events(
        browser_safe_sql_command(
            "req-sql-events",
            "events",
            QueryRequest::new(descriptor.table_uri, sql, ExecutionTarget::BrowserWasm),
        ),
        |event| events.push(event),
    ));

    assert_eq!(response.request_id(), "req-sql-events");
    assert!(
        response.success_envelope().is_some(),
        "query should still return the final success envelope"
    );
    assert!(
        events.iter().all(|event| {
            let context = event.context();
            context.phase == BrowserWorkerEventPhase::Query
                && context.request_id.as_deref() == Some("req-sql-events")
                && context.query_id.as_deref() == Some("req-sql-events")
                && context.table_name.as_deref() == Some("events")
        }),
        "all query events must preserve request and query identity: {events:?}"
    );
    assert!(
        events.iter().any(|event| matches!(
            event,
            BrowserWorkerEventEnvelope::Progress(progress)
                if progress.stage == BrowserWorkerProgressStage::Started
        )),
        "query event stream should include a started progress event"
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, BrowserWorkerEventEnvelope::RangeReadMetrics(_))),
        "query event stream should include live range-read metrics"
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn worker_chunked_sql_emits_arrow_ipc_chunk_events_with_metadata() {
    let mut worker =
        BrowserWorker::new(BrowserRuntimeConfig::default(), DEFAULT_SESSION_CACHE_BYTES)
            .expect("worker should construct");
    let descriptor = empty_delta_descriptor();

    test_runtime().block_on(
        worker.handle_command(BrowserWorkerCommand::open_delta_table(
            "req-open-delta",
            "events",
            descriptor.clone(),
        )),
    );

    let sql = "SELECT COUNT(*) AS rows FROM axon_table";
    let mut events = Vec::new();
    let response = test_runtime().block_on(worker.handle_command_streaming_events(
        browser_safe_chunked_sql_command(
            "req-sql-chunked-events",
            "events",
            QueryRequest::new(descriptor.table_uri, sql, ExecutionTarget::BrowserWasm),
        ),
        |event| events.push(event),
    ));

    let success = response
        .success_envelope()
        .expect("chunked SQL should return a success envelope");
    assert_eq!(success.request_id, "req-sql-chunked-events");
    assert_eq!(success.result.delivery, ArrowIpcDelivery::ChunkedBuffers);
    assert_eq!(success.result.bytes, None);
    assert!(success.result.byte_length > 0);
    assert_eq!(success.result.chunk_count, 1);
    assert_eq!(success.response.metrics.arrow_ipc_chunk_count, Some(1));

    let chunk_events = events
        .iter()
        .filter_map(|event| match event {
            BrowserWorkerEventEnvelope::ArrowIpcChunk(chunk) => Some(chunk),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(chunk_events.len(), 1, "expected one Arrow IPC chunk event");

    let chunk = chunk_events[0];
    assert_eq!(chunk.request_id, "req-sql-chunked-events");
    assert_eq!(chunk.sequence, 0);
    assert_eq!(chunk.byte_offset, 0);
    assert_eq!(chunk.byte_length, success.result.byte_length);
    assert_eq!(u64::try_from(chunk.bytes.len()).unwrap(), chunk.byte_length);
    assert!(!chunk.bytes.is_empty());
    assert_eq!(
        events.iter().position(|event| matches!(
            event,
            BrowserWorkerEventEnvelope::Progress(progress)
                if progress.stage == BrowserWorkerProgressStage::ArrowIpcReady
        )),
        Some(
            events
                .iter()
                .position(|event| matches!(event, BrowserWorkerEventEnvelope::ArrowIpcChunk(_)))
                .expect("chunk event should be present")
                - 1
        ),
        "chunk should be emitted immediately after arrow_ipc_ready for this empty fixture"
    );
    assert!(
        events
            .iter()
            .position(|event| matches!(event, BrowserWorkerEventEnvelope::ArrowIpcChunk(_)))
            < events.iter().position(|event| matches!(
                event,
                BrowserWorkerEventEnvelope::Progress(progress)
                    if progress.stage == BrowserWorkerProgressStage::Finished
            )),
        "chunk events should precede the finished progress event: {events:?}"
    );
}

#[cfg(not(target_arch = "wasm32"))]
fn browser_safe_sql_command(
    request_id: &str,
    name: &str,
    request: QueryRequest,
) -> BrowserWorkerCommand {
    let mut command = BrowserWorkerCommand::sql(request_id, name, request);
    if let BrowserWorkerCommand::Sql(command) = &mut command {
        command.browser_safe_defaults = true;
    }
    command
}

#[cfg(not(target_arch = "wasm32"))]
fn browser_safe_chunked_sql_command(
    request_id: &str,
    name: &str,
    request: QueryRequest,
) -> BrowserWorkerCommand {
    let mut command = browser_safe_sql_command(request_id, name, request);
    if let BrowserWorkerCommand::Sql(command) = &mut command {
        command.delivery = BrowserWorkerSqlDelivery::ChunkedBuffers;
    }
    command
}

#[cfg(not(target_arch = "wasm32"))]
fn query_with_explicit_browser_limits(request: QueryRequest) -> QueryRequest {
    request.with_options(QueryExecutionOptions {
        result_page: Some(QueryResultPage {
            limit: 501,
            offset: 0,
        }),
        runtime_limits: Some(QueryRuntimeLimits {
            max_result_rows: Some(501),
            max_arrow_ipc_bytes: Some(8 * 1024 * 1024),
            max_preview_string_bytes: Some(256 * 1024),
            max_scan_bytes: None,
        }),
        ..QueryExecutionOptions::default()
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn empty_delta_descriptor() -> BrowserHttpSnapshotDescriptor {
    BrowserHttpSnapshotDescriptor {
        table_uri: "gs://axon-fixtures/empty".to_string(),
        snapshot_version: 1,
        partition_column_types: BTreeMap::new(),
        browser_compatibility: CapabilityReport::default(),
        required_capabilities: CapabilityReport::default(),
        active_files: Vec::<BrowserHttpFileDescriptor>::new(),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn empty_parquet_dataset_descriptor() -> BrowserHttpParquetDatasetDescriptor {
    BrowserHttpParquetDatasetDescriptor {
        table_uri: "https://example.invalid/datasets/plain-parquet-empty".to_string(),
        partition_column_types: BTreeMap::new(),
        browser_compatibility: CapabilityReport::default(),
        required_capabilities: CapabilityReport::default(),
        files: Vec::<BrowserHttpFileDescriptor>::new(),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn test_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("test runtime should construct")
}
