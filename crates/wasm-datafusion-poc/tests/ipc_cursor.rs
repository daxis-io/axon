use std::io::Cursor;
use std::sync::Arc;

use arrow_array::builder::StringDictionaryBuilder;
use arrow_array::types::Int8Type;
use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{Field, Schema};
use query_contract::QueryErrorCode;
use wasm_datafusion_poc::{
    ArrowIpcPhase, IpcCursorItem, IpcStreamLimits, QueryTerminalStatus, WasmDataFusionEngine,
    DEFAULT_BROWSER_DATAFUSION_MEMORY_POOL_BYTES,
};

#[tokio::test]
async fn cursor_streams_schema_data_and_eos_before_success() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "id",
        arrow_schema::DataType::Int32,
        false,
    )]));
    let batches = vec![
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap(),
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![4, 5, 6]))],
        )
        .unwrap(),
    ];
    let expected_bytes = encode_arrow_ipc(&schema, &batches);
    let mut engine = WasmDataFusionEngine::new();
    engine
        .register_record_batches("events", Arc::clone(&schema), batches)
        .await
        .unwrap();
    let complete_result_bytes = engine
        .sql_to_arrow_ipc("SELECT id FROM events")
        .await
        .unwrap();
    assert_eq!(complete_result_bytes, expected_bytes);

    let mut cursor = engine
        .start_arrow_ipc_cursor(
            "SELECT id FROM events",
            IpcStreamLimits {
                max_transport_chunk_bytes: 31,
                max_pending_encoded_batch_bytes: 1024 * 1024,
                max_total_encoded_bytes: None,
                preview_rows: 2,
                max_preview_string_bytes: None,
            },
            false,
        )
        .await
        .unwrap();
    let mut chunks = Vec::new();
    let mut terminal = None;

    while let Some(item) = cursor.next().await.unwrap() {
        match item {
            IpcCursorItem::Chunk(chunk) => chunks.push(chunk),
            IpcCursorItem::Terminal(value) => terminal = Some(value),
        }
    }

    assert!(!chunks.is_empty());
    assert_eq!(chunks[0].phase, ArrowIpcPhase::Schema);
    assert!(chunks
        .iter()
        .any(|chunk| chunk.phase == ArrowIpcPhase::Data));
    assert!(chunks
        .iter()
        .any(|chunk| chunk.phase == ArrowIpcPhase::EndOfStream));
    assert_eq!(
        chunks
            .iter()
            .map(|chunk| chunk.sequence)
            .collect::<Vec<_>>(),
        (0..u64::try_from(chunks.len()).unwrap()).collect::<Vec<_>>()
    );
    assert!(chunks.iter().all(|chunk| chunk.bytes.len() <= 31));

    let bytes = chunks
        .into_iter()
        .flat_map(|chunk| chunk.bytes)
        .collect::<Vec<_>>();
    assert_eq!(bytes, expected_bytes);
    let decoded = StreamReader::try_new(Cursor::new(bytes), None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded.iter().map(RecordBatch::num_rows).sum::<usize>(), 6);

    let terminal = terminal.expect("cursor must deliver one terminal outcome");
    assert_eq!(terminal.status, QueryTerminalStatus::Succeeded);
    assert_eq!(terminal.row_count, 6);
    assert_eq!(terminal.preview.rows.len(), 2);
    assert_eq!(
        terminal.memory_metrics.limit_bytes,
        u64::try_from(DEFAULT_BROWSER_DATAFUSION_MEMORY_POOL_BYTES).unwrap(),
    );
    assert_eq!(terminal.memory_metrics.reserved_bytes, 0);
    assert!(terminal.memory_metrics.peak_bytes <= terminal.memory_metrics.limit_bytes);
    assert!(cursor.next().await.unwrap().is_none());
}

#[tokio::test]
async fn cursor_cancellation_discards_pending_bytes_and_terminates_once() {
    let engine = engine_with_string_rows(vec!["pending payload".repeat(1024)]).await;
    let mut cursor = engine
        .start_arrow_ipc_cursor("SELECT value FROM events", limits(64), false)
        .await
        .unwrap();

    let first = cursor.next().await.unwrap().unwrap();
    assert!(matches!(
        first,
        IpcCursorItem::Chunk(ref chunk) if chunk.phase == ArrowIpcPhase::Schema
    ));
    cursor.cancel_handle().cancel();

    let terminal = match cursor.next().await.unwrap().unwrap() {
        IpcCursorItem::Terminal(terminal) => terminal,
        item => panic!("expected cancellation terminal, got {item:?}"),
    };
    assert_eq!(terminal.status, QueryTerminalStatus::Cancelled);
    assert_eq!(terminal.row_count, 0);
    assert!(cursor.next().await.unwrap().is_none());
}

#[tokio::test]
async fn engine_cancel_generation_reaches_every_live_cursor_and_not_later_queries() {
    let engine = engine_with_string_rows(vec!["payload".repeat(1024)]).await;
    let mut first = engine
        .start_arrow_ipc_cursor("SELECT value FROM events", limits(64), false)
        .await
        .unwrap();
    let mut second = engine
        .start_arrow_ipc_cursor("SELECT value FROM events", limits(64), false)
        .await
        .unwrap();

    assert!(matches!(
        first.next().await.unwrap().unwrap(),
        IpcCursorItem::Chunk(ref chunk) if chunk.phase == ArrowIpcPhase::Schema
    ));
    assert!(matches!(
        second.next().await.unwrap().unwrap(),
        IpcCursorItem::Chunk(ref chunk) if chunk.phase == ArrowIpcPhase::Schema
    ));

    engine.cancel_running_queries();

    let first_terminal = match first.next().await.unwrap().unwrap() {
        IpcCursorItem::Terminal(terminal) => terminal,
        item => panic!("first live cursor missed cancel-all: {item:?}"),
    };
    let second_terminal = match second.next().await.unwrap().unwrap() {
        IpcCursorItem::Terminal(terminal) => terminal,
        item => panic!("second live cursor missed cancel-all: {item:?}"),
    };
    assert_eq!(first_terminal.status, QueryTerminalStatus::Cancelled);
    assert_eq!(second_terminal.status, QueryTerminalStatus::Cancelled);

    let mut later = engine
        .start_arrow_ipc_cursor("SELECT value FROM events", limits(64), false)
        .await
        .expect("a later query must capture the new cancellation generation");
    let later_terminal = loop {
        match later.next().await.unwrap().unwrap() {
            IpcCursorItem::Chunk(_) => {}
            IpcCursorItem::Terminal(terminal) => break terminal,
        }
    };
    assert_eq!(later_terminal.status, QueryTerminalStatus::Succeeded);
}

#[tokio::test]
async fn cursor_total_budget_counts_end_of_stream_and_terminates_once() {
    let engine = engine_with_string_rows(Vec::new()).await;
    let mut measuring_cursor = engine
        .start_arrow_ipc_cursor("SELECT value FROM events", limits(1024 * 1024), false)
        .await
        .unwrap();
    let mut schema_bytes = 0_u64;
    let mut data_bytes = 0_u64;
    while let Some(item) = measuring_cursor.next().await.unwrap() {
        if let IpcCursorItem::Chunk(chunk) = item {
            match chunk.phase {
                ArrowIpcPhase::Schema => {
                    schema_bytes += u64::try_from(chunk.bytes.len()).unwrap();
                }
                ArrowIpcPhase::Data => {
                    data_bytes += u64::try_from(chunk.bytes.len()).unwrap();
                }
                ArrowIpcPhase::EndOfStream => {}
            }
        }
    }

    let mut budgeted_limits = limits(1024 * 1024);
    budgeted_limits.max_total_encoded_bytes = Some(schema_bytes + data_bytes + 7);
    let mut cursor = engine
        .start_arrow_ipc_cursor("SELECT value FROM events", budgeted_limits, false)
        .await
        .unwrap();
    let mut terminal_count = 0;
    let mut terminal_status = None;
    while let Some(item) = cursor.next().await.unwrap() {
        if let IpcCursorItem::Terminal(terminal) = item {
            terminal_count += 1;
            terminal_status = Some(terminal.status);
            assert!(
                terminal
                    .error
                    .as_ref()
                    .is_some_and(|error| error.message.contains("max_output_ipc_bytes")),
                "{terminal:?}"
            );
        }
    }

    assert_eq!(terminal_count, 1);
    assert_eq!(terminal_status, Some(QueryTerminalStatus::Failed));
}

#[tokio::test]
async fn cursor_rejects_oversized_retained_batch_before_data_encoding() {
    let engine = engine_with_string_rows(vec!["x".repeat(4096)]).await;
    let mut stream_limits = limits(1024);
    stream_limits.max_pending_encoded_batch_bytes = 1024;
    let mut cursor = engine
        .start_arrow_ipc_cursor("SELECT value FROM events", stream_limits, false)
        .await
        .unwrap();
    let mut phases = Vec::new();
    let mut terminal = None;

    while let Some(item) = cursor.next().await.unwrap() {
        match item {
            IpcCursorItem::Chunk(chunk) => phases.push(chunk.phase),
            IpcCursorItem::Terminal(value) => terminal = Some(value),
        }
    }

    assert!(phases.iter().all(|phase| *phase == ArrowIpcPhase::Schema));
    let terminal = terminal.unwrap();
    assert_eq!(terminal.status, QueryTerminalStatus::Failed);
    assert!(terminal
        .error
        .as_ref()
        .is_some_and(|error| error.message.contains("retained input batch")));
    assert!(terminal.cursor_metrics.peak_input_batch_bytes > 1024);
    assert!(terminal.cursor_metrics.peak_pending_encoded_bytes <= 1024);
    assert!(terminal.cursor_metrics.peak_pending_encoded_capacity <= 1024);
}

#[tokio::test]
async fn cursor_grows_pending_storage_lazily_under_separate_total_and_batch_caps() {
    const PENDING_CAP: usize = 8 * 1024 * 1024;
    const TOTAL_CAP: u64 = 16 * 1024 * 1024;
    let engine = engine_with_string_rows(vec!["small".to_string()]).await;
    let mut stream_limits = limits(1024 * 1024);
    stream_limits.max_pending_encoded_batch_bytes = PENDING_CAP;
    stream_limits.max_total_encoded_bytes = Some(TOTAL_CAP);
    let mut cursor = engine
        .start_arrow_ipc_cursor("SELECT value FROM events", stream_limits, false)
        .await
        .unwrap();
    let mut terminal = None;

    while let Some(item) = cursor.next().await.unwrap() {
        if let IpcCursorItem::Terminal(value) = item {
            terminal = Some(value);
        }
    }

    let terminal = terminal.expect("cursor should report one terminal outcome");
    assert_eq!(terminal.status, QueryTerminalStatus::Succeeded);
    assert!(terminal.encoded_bytes <= TOTAL_CAP);
    assert!(terminal.cursor_metrics.peak_pending_encoded_bytes <= PENDING_CAP as u64);
    assert!(terminal.cursor_metrics.peak_pending_encoded_capacity <= PENDING_CAP as u64);
    assert!(
        terminal.cursor_metrics.peak_pending_encoded_capacity < PENDING_CAP as u64,
        "a small query must not reserve the entire per-batch cap"
    );
}

#[tokio::test]
async fn cursor_allows_output_above_pending_cap_when_each_batch_stays_bounded() {
    const BATCH_COUNT: usize = 18;
    const PAYLOAD_BYTES: usize = 512 * 1024;
    const PENDING_CAP: usize = 8 * 1024 * 1024;
    const TOTAL_CAP: u64 = 16 * 1024 * 1024;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        arrow_schema::DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec!["x".repeat(PAYLOAD_BYTES)]))],
    )
    .unwrap();
    let mut engine = WasmDataFusionEngine::new();
    engine
        .register_record_batches("events", schema, vec![batch; BATCH_COUNT])
        .await
        .unwrap();
    let mut cursor = engine
        .start_arrow_ipc_cursor(
            "SELECT value FROM events",
            IpcStreamLimits {
                max_transport_chunk_bytes: 1024 * 1024,
                max_pending_encoded_batch_bytes: PENDING_CAP,
                max_total_encoded_bytes: Some(TOTAL_CAP),
                preview_rows: 0,
                max_preview_string_bytes: None,
            },
            false,
        )
        .await
        .unwrap();
    let terminal = loop {
        match cursor.next().await.unwrap().unwrap() {
            IpcCursorItem::Chunk(_) => {}
            IpcCursorItem::Terminal(terminal) => break terminal,
        }
    };

    assert_eq!(terminal.status, QueryTerminalStatus::Succeeded);
    assert!(terminal.encoded_bytes > PENDING_CAP as u64);
    assert!(terminal.encoded_bytes <= TOTAL_CAP);
    assert!(terminal.cursor_metrics.peak_pending_encoded_bytes <= PENDING_CAP as u64);
    assert!(terminal.cursor_metrics.peak_pending_encoded_capacity <= PENDING_CAP as u64);
}

#[tokio::test]
async fn cursor_rejects_all_dictionary_modes_before_writer_creation() {
    let modes = [
        ("static", vec![vec!["alpha", "beta"]]),
        ("replacement", vec![vec!["alpha"], vec!["bravo"]]),
        ("delta", vec![vec!["alpha"], vec!["alpha", "bravo"]]),
        (
            "growing",
            vec![
                vec!["alpha"],
                vec!["alpha", "bravo"],
                vec!["alpha", "bravo", "charlie"],
            ],
        ),
    ];

    for (mode, values_by_batch) in modes {
        let batches = values_by_batch
            .into_iter()
            .map(dictionary_batch)
            .collect::<Vec<_>>();
        let schema = batches[0].schema();
        let mut engine = WasmDataFusionEngine::new();
        engine
            .register_record_batches("events", schema, batches)
            .await
            .unwrap();

        let error = engine
            .start_arrow_ipc_cursor("SELECT value FROM events", limits(1024), false)
            .await
            .unwrap_err();

        assert_eq!(error.code, QueryErrorCode::UnsupportedFeature, "{mode}");
        assert!(
            error.message.contains("dictionary output is unsupported"),
            "{mode}: {error:?}"
        );
    }
}

#[tokio::test]
async fn cursor_preserves_empty_logical_batches_and_their_ipc_bytes() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "id",
        arrow_schema::DataType::Int32,
        false,
    )]));
    let batches = vec![
        RecordBatch::new_empty(Arc::clone(&schema)),
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )
        .unwrap(),
        RecordBatch::new_empty(Arc::clone(&schema)),
    ];
    let expected_bytes = encode_arrow_ipc(&schema, &batches);
    let mut engine = WasmDataFusionEngine::new();
    engine
        .register_record_batches("events", Arc::clone(&schema), batches)
        .await
        .unwrap();
    let mut cursor = engine
        .start_arrow_ipc_cursor("SELECT id FROM events", limits(1024 * 1024), false)
        .await
        .unwrap();
    let mut bytes = Vec::new();
    let mut completed_data_batches = 0;

    while let Some(item) = cursor.next().await.unwrap() {
        if let IpcCursorItem::Chunk(chunk) = item {
            if chunk.phase == ArrowIpcPhase::Data && chunk.end_of_logical_batch {
                completed_data_batches += 1;
            }
            bytes.extend_from_slice(&chunk.bytes);
        }
    }

    assert_eq!(completed_data_batches, 3);
    assert_eq!(bytes, expected_bytes);
    let decoded_rows = StreamReader::try_new(Cursor::new(bytes), None)
        .unwrap()
        .map(|batch| batch.unwrap().num_rows())
        .collect::<Vec<_>>();
    assert_eq!(decoded_rows, vec![0, 2, 0]);
}

#[tokio::test]
async fn empty_query_result_still_streams_schema_and_end_of_stream() {
    let engine = engine_with_string_rows(vec!["unused".to_string()]).await;
    let mut cursor = engine
        .start_arrow_ipc_cursor(
            "SELECT value FROM events WHERE 1 = 0",
            limits(1024 * 1024),
            false,
        )
        .await
        .unwrap();
    let mut phases = Vec::new();
    let mut terminal = None;

    while let Some(item) = cursor.next().await.unwrap() {
        match item {
            IpcCursorItem::Chunk(chunk) => phases.push(chunk.phase),
            IpcCursorItem::Terminal(value) => terminal = Some(value),
        }
    }

    assert_eq!(phases.first(), Some(&ArrowIpcPhase::Schema));
    assert_eq!(phases.last(), Some(&ArrowIpcPhase::EndOfStream));
    assert_eq!(terminal.unwrap().row_count, 0);
}

#[tokio::test]
async fn cursor_peak_memory_metrics_do_not_grow_with_total_batch_count() {
    let one_batch = collect_metrics_for_batch_count(1).await;
    let many_batches = collect_metrics_for_batch_count(64).await;

    assert!(many_batches.0 > one_batch.0);
    assert_eq!(
        many_batches.1.peak_input_batch_bytes,
        one_batch.1.peak_input_batch_bytes
    );
    assert_eq!(
        many_batches.1.peak_pending_encoded_capacity,
        one_batch.1.peak_pending_encoded_capacity
    );
    assert_eq!(
        many_batches.1.peak_pending_encoded_bytes,
        one_batch.1.peak_pending_encoded_bytes
    );
    assert_eq!(
        many_batches.1.peak_transport_chunk_bytes,
        one_batch.1.peak_transport_chunk_bytes
    );
}

fn limits(max_transport_chunk_bytes: usize) -> IpcStreamLimits {
    IpcStreamLimits {
        max_transport_chunk_bytes,
        max_pending_encoded_batch_bytes: 1024 * 1024,
        max_total_encoded_bytes: None,
        preview_rows: 2,
        max_preview_string_bytes: None,
    }
}

async fn engine_with_string_rows(values: Vec<String>) -> WasmDataFusionEngine {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        arrow_schema::DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(values))],
    )
    .unwrap();
    let mut engine = WasmDataFusionEngine::new();
    engine
        .register_record_batches("events", schema, vec![batch])
        .await
        .unwrap();
    engine
}

fn dictionary_batch(values: Vec<&str>) -> RecordBatch {
    let mut builder = StringDictionaryBuilder::<Int8Type>::new();
    for value in values {
        builder.append(value).unwrap();
    }
    let dictionary = Arc::new(builder.finish()) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        dictionary.data_type().clone(),
        false,
    )]));
    RecordBatch::try_new(schema, vec![dictionary]).unwrap()
}

fn encode_arrow_ipc(schema: &Arc<Schema>, batches: &[RecordBatch]) -> Vec<u8> {
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, schema.as_ref()).unwrap();
        for batch in batches {
            writer.write(batch).unwrap();
        }
        writer.finish().unwrap();
    }
    bytes
}

async fn collect_metrics_for_batch_count(
    batch_count: usize,
) -> (u64, wasm_datafusion_poc::IpcCursorMetrics) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        arrow_schema::DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec!["x".repeat(1024)]))],
    )
    .unwrap();
    let mut engine = WasmDataFusionEngine::new();
    engine
        .register_record_batches("events", schema, vec![batch; batch_count])
        .await
        .unwrap();
    let mut cursor = engine
        .start_arrow_ipc_cursor("SELECT value FROM events", limits(1024 * 1024), false)
        .await
        .unwrap();
    while let Some(item) = cursor.next().await.unwrap() {
        if let IpcCursorItem::Terminal(terminal) = item {
            return (terminal.encoded_bytes, terminal.cursor_metrics);
        }
    }
    panic!("cursor closed without a terminal outcome")
}
