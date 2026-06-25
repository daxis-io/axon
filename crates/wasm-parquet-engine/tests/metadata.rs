#![cfg(not(target_arch = "wasm32"))]

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc::{self, Receiver};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use parquet::basic::Compression;
use parquet::data_type::Int64Type;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use wasm_http_object_store::HttpRangeReader;
use wasm_parquet_engine::{
    inspect_parquet_target, read_parquet_metadata_for_target,
    read_parquet_metadata_for_target_with_cache, ObjectSource, ParquetMetadataCache, ScanTarget,
};

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

#[tokio::test]
async fn inspect_parquet_target_returns_file_column_and_index_diagnostics() {
    let object = parquet_bytes_with_two_i64_columns(&[(1_i64, 10_i64), (2, 20), (3, 30)]);
    let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
    let footer_length = parquet_footer_length(&object);
    let object_for_server = object.clone();
    let (url, requests, server) = spawn_multi_request_server(2, move |request, _| {
        full_or_ranged_response(request, &object_for_server)
    });

    let inspection = inspect_parquet_target(
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
    .expect("parquet inspection should succeed");

    let _requests = finish_requests(server, requests, 2);
    assert_eq!(inspection.path, "part-000.parquet");
    assert_eq!(inspection.object_size_bytes, object_size);
    assert_eq!(inspection.footer_length_bytes, footer_length);
    assert!(inspection.metadata_memory_size_bytes > 0);
    assert_eq!(
        inspection.created_by.as_deref(),
        Some("axon inspection test")
    );
    assert_eq!(inspection.file_version, 2);
    assert_eq!(inspection.row_group_count, 1);
    assert_eq!(inspection.row_count, 3);
    assert_eq!(inspection.column_count, 2);
    assert!(inspection.compression.compressed_size_bytes > 0);
    assert!(inspection.compression.uncompressed_size_bytes > 0);
    assert!(inspection.compression.ratio_basis_points > 0);

    let id = inspection
        .columns
        .iter()
        .find(|column| column.name == "id")
        .expect("id column should be inspected");
    assert_eq!(id.physical_type, "Int64");
    assert_eq!(id.logical_type, None);
    assert_eq!(id.null_count, Some(0));
    assert!(id.has_statistics);
    assert!(id.has_column_index);
    assert!(id.has_offset_index);
    assert!(!id.has_bloom_filter);
    assert!(id.encodings.iter().any(|encoding| encoding == "PLAIN"));
    assert_eq!(id.compressions, vec!["UNCOMPRESSED"]);

    let row_group = inspection
        .row_groups
        .first()
        .expect("row-group diagnostics should be present");
    assert_eq!(row_group.index, 0);
    assert_eq!(row_group.row_count, 3);
    assert_eq!(row_group.columns.len(), 2);
    assert_eq!(row_group.columns[0].column_name, "id");
    assert_eq!(row_group.columns[0].compression, "UNCOMPRESSED");
    assert!(row_group.columns[0]
        .encodings
        .iter()
        .any(|encoding| encoding == "PLAIN"));
}

#[tokio::test]
async fn metadata_cache_reuses_footer_across_signed_url_rotation_with_same_identity() {
    let object = parquet_bytes_with_single_i64_column(&[1_i64, 2, 3]);
    let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
    let object_for_server = object.clone();
    let (url, requests, server) = spawn_multi_request_server(3, move |request, _| {
        full_or_ranged_response_with_etag(request, &object_for_server, Some("\"part-000-v1\""))
    });
    let cache = ParquetMetadataCache::default();
    let first_target = ScanTarget {
        object_source: ObjectSource::new(format!("{url}?X-Goog-Signature=first")),
        object_etag: Some("\"part-000-v1\"".to_string()),
        path: "part-000.parquet".to_string(),
        size_bytes: object_size,
        partition_values: BTreeMap::new(),
    };
    let rotated_target = ScanTarget {
        object_source: ObjectSource::new(format!("{url}?X-Goog-Signature=rotated")),
        object_etag: Some("\"part-000-v1\"".to_string()),
        path: "part-000.parquet".to_string(),
        size_bytes: object_size,
        partition_values: BTreeMap::new(),
    };

    let first = read_parquet_metadata_for_target_with_cache(
        &HttpRangeReader::new(),
        &first_target,
        None,
        Some(&cache),
        None,
    )
    .await
    .expect("first metadata read should fetch and cache the footer");
    let second = read_parquet_metadata_for_target_with_cache(
        &HttpRangeReader::new(),
        &rotated_target,
        None,
        Some(&cache),
        None,
    )
    .await
    .expect("rotated signed URL should reuse the footer for the same object identity");

    let requests = finish_requests(server, requests, 3);
    let cache_metrics = cache.snapshot();
    assert_eq!(first.row_count, second.row_count);
    assert_eq!(requests.len(), 3);
    assert_eq!(cache_metrics.footer_cache_misses, 1);
    assert_eq!(cache_metrics.footer_cache_hits, 1);
    assert_eq!(cache_metrics.footer_range_reads_avoided, 2);
}

#[tokio::test]
async fn metadata_cache_does_not_reuse_without_strong_etag_identity() {
    let object = parquet_bytes_with_single_i64_column(&[1_i64, 2, 3]);
    let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
    let object_for_server = object.clone();
    let (url, requests, server) = spawn_multi_request_server(4, move |request, _| {
        full_or_ranged_response(request, &object_for_server)
    });
    let cache = ParquetMetadataCache::default();
    let target = ScanTarget {
        object_source: ObjectSource::new(url),
        object_etag: None,
        path: "part-000.parquet".to_string(),
        size_bytes: object_size,
        partition_values: BTreeMap::new(),
    };

    read_parquet_metadata_for_target_with_cache(
        &HttpRangeReader::new(),
        &target,
        None,
        Some(&cache),
        None,
    )
    .await
    .expect("first metadata read should succeed");
    read_parquet_metadata_for_target_with_cache(
        &HttpRangeReader::new(),
        &target,
        None,
        Some(&cache),
        None,
    )
    .await
    .expect("second metadata read should succeed without identity reuse");

    let requests = finish_requests(server, requests, 4);
    let cache_metrics = cache.snapshot();
    assert_eq!(requests.len(), 4);
    assert_eq!(cache_metrics.footer_cache_hits, 0);
    assert_eq!(cache_metrics.footer_cache_misses, 2);
    assert_eq!(cache_metrics.footer_cache_degraded_identity_reads, 2);
}

#[tokio::test]
async fn metadata_cache_bypasses_cached_footer_when_etag_changes() {
    let object = parquet_bytes_with_single_i64_column(&[1_i64, 2, 3]);
    let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
    let object_for_server = object.clone();
    let (url, requests, server) = spawn_multi_request_server(4, move |request, index| {
        let etag = if index < 2 {
            "\"part-000-v1\""
        } else {
            "\"part-000-v2\""
        };
        full_or_ranged_response_with_etag(request, &object_for_server, Some(etag))
    });
    let cache = ParquetMetadataCache::default();
    let first_target = ScanTarget {
        object_source: ObjectSource::new(url.clone()),
        object_etag: Some("\"part-000-v1\"".to_string()),
        path: "part-000.parquet".to_string(),
        size_bytes: object_size,
        partition_values: BTreeMap::new(),
    };
    let changed_target = ScanTarget {
        object_source: ObjectSource::new(url),
        object_etag: Some("\"part-000-v2\"".to_string()),
        path: "part-000.parquet".to_string(),
        size_bytes: object_size,
        partition_values: BTreeMap::new(),
    };

    read_parquet_metadata_for_target_with_cache(
        &HttpRangeReader::new(),
        &first_target,
        None,
        Some(&cache),
        None,
    )
    .await
    .expect("first metadata read should cache ETag v1");
    read_parquet_metadata_for_target_with_cache(
        &HttpRangeReader::new(),
        &changed_target,
        None,
        Some(&cache),
        None,
    )
    .await
    .expect("changed ETag should fetch and cache a separate footer");

    let requests = finish_requests(server, requests, 4);
    let cache_metrics = cache.snapshot();
    assert_eq!(requests.len(), 4);
    assert_eq!(cache_metrics.footer_cache_hits, 0);
    assert_eq!(cache_metrics.footer_cache_misses, 2);
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

fn parquet_bytes_with_two_i64_columns(values: &[(i64, i64)]) -> Vec<u8> {
    let schema = Arc::new(
        parse_message_type("message schema { REQUIRED INT64 id; REQUIRED INT64 value; }")
            .expect("parquet schema should parse"),
    );
    let ids = values.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    let values = values.iter().map(|(_, value)| *value).collect::<Vec<_>>();
    let mut bytes = Vec::new();
    let mut writer = SerializedFileWriter::new(
        &mut bytes,
        schema,
        Arc::new(
            WriterProperties::builder()
                .set_created_by("axon inspection test".to_string())
                .set_writer_version(WriterVersion::PARQUET_2_0)
                .set_compression(Compression::UNCOMPRESSED)
                .set_statistics_enabled(EnabledStatistics::Page)
                .build(),
        ),
    )
    .expect("parquet writer should construct");

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
            .expect("test parquet id rows should write");
        column.close().expect("id column writer should close");
    }
    if let Some(mut column) = row_group
        .next_column()
        .expect("value column writer should be returned")
    {
        column
            .typed::<Int64Type>()
            .write_batch(&values, None, None)
            .expect("test parquet value rows should write");
        column.close().expect("value column writer should close");
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

fn full_or_ranged_response_with_etag(
    request: &CapturedRequest,
    body: &[u8],
    etag: Option<&str>,
) -> TestResponse {
    let mut response = full_or_ranged_response(request, body);
    if let Some(etag) = etag {
        response
            .headers
            .push(("ETag".to_string(), etag.to_string()));
    }
    response
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
