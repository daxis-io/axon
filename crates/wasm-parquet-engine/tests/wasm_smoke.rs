#![cfg(target_arch = "wasm32")]

use std::collections::BTreeMap;
use std::sync::Arc;

use parquet::data_type::Int64Type;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use query_contract::{ExecutionTarget, PartitionColumnType};
use wasm_bindgen_test::wasm_bindgen_test;
use wasm_http_object_store::HttpRangeReader;
use wasm_parquet_engine::{
    decode_parquet_input_rows, read_parquet_footer, read_parquet_footer_for_target,
    read_parquet_metadata_for_target, runtime_target, scan_target_input_rows, ObjectSource,
    ParquetScalarValue, ScanTarget,
};

#[wasm_bindgen_test]
fn parquet_engine_runtime_target_and_targets_construct_in_wasm() {
    let target = synthetic_scan_target();

    assert_eq!(runtime_target(), ExecutionTarget::BrowserWasm);
    assert_eq!(
        target.object_source,
        ObjectSource::new("https://example.com/object.parquet")
    );
    assert_eq!(target.path, "part-000.parquet");
}

#[wasm_bindgen_test]
fn parquet_engine_network_api_surface_constructs_futures_in_wasm() {
    let reader = HttpRangeReader::new();
    let source = ObjectSource::new("https://example.com/object.parquet");
    let target = synthetic_scan_target();
    let required_columns = ["id".to_string(), "category".to_string()];
    let partition_column_types =
        BTreeMap::from([("category".to_string(), PartitionColumnType::String)]);

    let footer_read = read_parquet_footer(&reader, &source, None);
    let footer_read_for_target = read_parquet_footer_for_target(&reader, &target, None);
    let metadata_read = read_parquet_metadata_for_target(&reader, &target, None);
    let scan = scan_target_input_rows(
        &reader,
        &target,
        &required_columns,
        &partition_column_types,
        None,
    );

    drop(footer_read);
    drop(footer_read_for_target);
    drop(metadata_read);
    drop(scan);
}

#[wasm_bindgen_test]
fn parquet_engine_decodes_rows_from_synthetic_parquet_in_wasm() {
    let rows = decode_parquet_input_rows(
        &synthetic_scan_target(),
        parquet_bytes_with_single_i64_column(&[7_i64, 11, 13]).into(),
        &["id".to_string()],
    )
    .expect("synthetic parquet bytes should decode in wasm");

    assert_eq!(
        rows,
        vec![
            BTreeMap::from([("id".to_string(), ParquetScalarValue::Int64(7))]),
            BTreeMap::from([("id".to_string(), ParquetScalarValue::Int64(11))]),
            BTreeMap::from([("id".to_string(), ParquetScalarValue::Int64(13))]),
        ]
    );
}

fn synthetic_scan_target() -> ScanTarget {
    ScanTarget {
        object_source: ObjectSource::new("https://example.com/object.parquet"),
        object_etag: Some("\"etag-1\"".to_string()),
        path: "part-000.parquet".to_string(),
        size_bytes: 128,
        partition_values: BTreeMap::from([("category".to_string(), Some("A".to_string()))]),
    }
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
