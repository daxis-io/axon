#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use deltalake::arrow::array::{Int32Array, StringArray};
use deltalake::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::kernel::scalars::ScalarExt;
use deltalake::kernel::{DataType, PrimitiveType, StructField};
use deltalake::DeltaTable;
use query_contract::ResolvedFileDescriptor;
use tempfile::TempDir;

pub struct TestTableFixture {
    _tempdir: TempDir,
    pub table_uri: String,
}

impl TestTableFixture {
    pub fn create_multi_version() -> Self {
        let fixture = Self::create_with_partition_columns(vec![]);

        tokio::runtime::Runtime::new()
            .expect("runtime should be created")
            .block_on(async {
                let table = DeltaTable::try_from_url(
                    deltalake::ensure_table_uri(&fixture.table_uri)
                        .expect("table uri should parse"),
                )
                .await
                .expect("table handle should be created");

                let table = table
                    .write(vec![fixture_batch(
                        &[1, 2, 3],
                        &["A", "B", "A"],
                        &[10, 20, 30],
                    )])
                    .await
                    .expect("first batch should be written");

                table
                    .write(vec![fixture_batch(
                        &[4, 5, 6],
                        &["B", "B", "C"],
                        &[40, 50, 60],
                    )])
                    .await
                    .expect("second batch should be written");
            });

        fixture
    }

    pub fn create_partitioned() -> Self {
        let fixture = Self::create_with_partition_columns(vec!["category"]);

        tokio::runtime::Runtime::new()
            .expect("runtime should be created")
            .block_on(async {
                let table = DeltaTable::try_from_url(
                    deltalake::ensure_table_uri(&fixture.table_uri)
                        .expect("table uri should parse"),
                )
                .await
                .expect("table handle should be created");

                table
                    .write(vec![fixture_batch(
                        &[1, 2, 3, 4, 5, 6],
                        &["A", "B", "A", "B", "B", "C"],
                        &[10, 20, 30, 40, 50, 60],
                    )])
                    .await
                    .expect("partitioned batch should be written");
            });

        fixture
    }

    fn create_with_partition_columns(partition_columns: Vec<&str>) -> Self {
        let tempdir = TempDir::new().expect("tempdir should be created");
        let table_uri = deltalake::ensure_table_uri(tempdir.path().to_string_lossy())
            .expect("table uri should be normalized")
            .to_string();

        tokio::runtime::Runtime::new()
            .expect("runtime should be created")
            .block_on(async {
                let table = DeltaTable::try_from_url(
                    deltalake::ensure_table_uri(&table_uri).expect("table uri should parse"),
                )
                .await
                .expect("table handle should be created");

                table
                    .create()
                    .with_columns(default_table_columns())
                    .with_partition_columns(partition_columns)
                    .with_table_name("axon_fixture")
                    .await
                    .expect("table should be created");
            });

        Self {
            _tempdir: tempdir,
            table_uri,
        }
    }

    pub fn data_file_paths(&self) -> Vec<PathBuf> {
        let mut paths = Vec::new();
        collect_matching_paths(self._tempdir.path(), &mut paths, |path| {
            path.extension()
                .is_some_and(|extension| extension == "parquet")
        });
        paths.sort();
        paths
    }

    pub fn raw_table_path(&self) -> String {
        self._tempdir.path().display().to_string()
    }

    pub fn table_root(&self) -> &Path {
        self._tempdir.path()
    }

    pub fn expected_active_files(
        &self,
        snapshot_version: Option<i64>,
    ) -> Vec<ResolvedFileDescriptor> {
        let table = tokio::runtime::Runtime::new()
            .expect("runtime should be created")
            .block_on(async {
                let mut table = DeltaTable::try_from_url(
                    deltalake::ensure_table_uri(&self.table_uri).expect("table uri should parse"),
                )
                .await
                .expect("table handle should be created");

                if let Some(version) = snapshot_version {
                    table
                        .load_version(version)
                        .await
                        .expect("historical snapshot should load");
                }

                table
            });

        let mut active_files = table
            .snapshot()
            .expect("snapshot should be available")
            .log_data()
            .iter()
            .map(|file| ResolvedFileDescriptor {
                path: file.path().into_owned(),
                size_bytes: u64::try_from(file.size())
                    .expect("fixture file sizes should be non-negative"),
                partition_values: file
                    .partition_values()
                    .map(|data| {
                        data.fields()
                            .iter()
                            .zip(data.values().iter())
                            .map(|(field, value)| {
                                (
                                    field.name().to_string(),
                                    if value.is_null() {
                                        None
                                    } else {
                                        Some(value.serialize())
                                    },
                                )
                            })
                            .collect::<BTreeMap<_, _>>()
                    })
                    .unwrap_or_default(),
            })
            .collect::<Vec<_>>();

        active_files.sort_by(|left, right| left.path.cmp(&right.path));
        active_files
    }
}

fn default_table_columns() -> Vec<StructField> {
    vec![
        StructField::new(
            "id".to_string(),
            DataType::Primitive(PrimitiveType::Integer),
            false,
        ),
        StructField::new(
            "category".to_string(),
            DataType::Primitive(PrimitiveType::String),
            false,
        ),
        StructField::new(
            "value".to_string(),
            DataType::Primitive(PrimitiveType::Integer),
            false,
        ),
    ]
}

fn fixture_batch(ids: &[i32], categories: &[&str], values: &[i32]) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowDataType::Int32, false),
        Field::new("category", ArrowDataType::Utf8, false),
        Field::new("value", ArrowDataType::Int32, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(categories.to_vec())),
            Arc::new(Int32Array::from(values.to_vec())),
        ],
    )
    .expect("fixture batch should be created")
}

fn collect_matching_paths<F>(root: &Path, matches: &mut Vec<PathBuf>, predicate: F)
where
    F: Fn(&Path) -> bool + Copy,
{
    for entry in fs::read_dir(root).expect("directory should be readable") {
        let entry = entry.expect("directory entry should load");
        let path = entry.path();
        if path.is_dir() {
            collect_matching_paths(&path, matches, predicate);
        } else if predicate(&path) {
            matches.push(path);
        }
    }
}

pub struct LoopbackObjectServer {
    address: std::net::SocketAddr,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl LoopbackObjectServer {
    pub fn from_fixture_paths(
        fixture: &TestTableFixture,
        relative_paths: impl IntoIterator<Item = String>,
    ) -> Self {
        let objects_by_path = relative_paths
            .into_iter()
            .map(|relative_path| {
                let absolute_path = fixture.table_root().join(&relative_path);
                let bytes = fs::read(&absolute_path).unwrap_or_else(|error| {
                    panic!(
                        "fixture object '{}' should be readable: {error}",
                        absolute_path.display()
                    )
                });
                (format!("/{}", relative_path), bytes)
            })
            .collect::<BTreeMap<_, _>>();

        Self::from_objects(objects_by_path)
    }

    pub fn url_for_path(&self, relative_path: &str) -> String {
        format!("http://{}/{}", self.address, relative_path)
    }

    fn from_objects(objects_by_path: BTreeMap<String, Vec<u8>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("loopback listener should bind");
        listener
            .set_nonblocking(true)
            .expect("listener should allow nonblocking accept");
        let address = listener
            .local_addr()
            .expect("listener should expose a local address");
        let stop = Arc::new(AtomicBool::new(false));
        let stop_for_thread = Arc::clone(&stop);

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
                        handle_connection(&mut stream, &objects_by_path);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => {
                        panic!("loopback object server accept should succeed: {error}")
                    }
                }
            }
        });

        Self {
            address,
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for LoopbackObjectServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.address);
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .expect("loopback object server should shut down cleanly");
        }
    }
}

fn handle_connection(stream: &mut TcpStream, objects_by_path: &BTreeMap<String, Vec<u8>>) {
    let request = read_request(stream);
    let response = objects_by_path
        .get(&request.path)
        .map(|object| full_or_ranged_response(request.range_header.as_deref(), object))
        .unwrap_or_else(not_found_response);
    write_response(stream, response);
}

struct CapturedRequest {
    path: String,
    range_header: Option<String>,
}

struct TestResponse {
    status_line: &'static str,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

fn read_request(stream: &mut TcpStream) -> CapturedRequest {
    let mut buffer = [0_u8; 8192];
    let mut request = Vec::new();

    loop {
        let read = stream
            .read(&mut buffer)
            .expect("request bytes should be readable");
        if read == 0 {
            break;
        }
        request.extend_from_slice(&buffer[..read]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }

    let request = String::from_utf8(request).expect("request should be valid utf8");
    let mut lines = request.split("\r\n");
    let request_line = lines.next().expect("request line should be present");
    let path = request_line
        .split_whitespace()
        .nth(1)
        .expect("request line should include a path")
        .to_string();
    let range_header = lines.find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("range")
            .then(|| value.trim().to_string())
    });

    CapturedRequest { path, range_header }
}

fn full_or_ranged_response(range_header: Option<&str>, object: &[u8]) -> TestResponse {
    if let Some(range_header) = range_header {
        let (start, end) = resolve_range(range_header, object.len());
        let body = object[start..=end].to_vec();
        return TestResponse {
            status_line: "206 Partial Content",
            headers: vec![
                ("Content-Length".to_string(), body.len().to_string()),
                (
                    "Content-Range".to_string(),
                    format!("bytes {start}-{end}/{}", object.len()),
                ),
            ],
            body,
        };
    }

    TestResponse {
        status_line: "200 OK",
        headers: vec![("Content-Length".to_string(), object.len().to_string())],
        body: object.to_vec(),
    }
}

fn not_found_response() -> TestResponse {
    TestResponse {
        status_line: "404 Not Found",
        headers: vec![("Content-Length".to_string(), "0".to_string())],
        body: Vec::new(),
    }
}

fn resolve_range(range_header: &str, object_len: usize) -> (usize, usize) {
    let range_spec = range_header
        .strip_prefix("bytes=")
        .expect("range header should use bytes= syntax");

    if let Some(suffix) = range_spec.strip_prefix('-') {
        let suffix = suffix.parse::<usize>().expect("suffix length should parse");
        let start = object_len
            .checked_sub(suffix)
            .expect("suffix should be shorter than the object");
        return (start, object_len - 1);
    }

    let (start, end) = range_spec
        .split_once('-')
        .expect("range header should contain a single dash");
    let start = start.parse::<usize>().expect("range start should parse");
    let end = if end.is_empty() {
        object_len - 1
    } else {
        end.parse::<usize>().expect("range end should parse")
    };

    (start, end)
}

fn write_response(stream: &mut TcpStream, response: TestResponse) {
    let mut bytes = format!("HTTP/1.1 {}\r\n", response.status_line).into_bytes();
    for (name, value) in response.headers {
        bytes.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
    }
    bytes.extend_from_slice(b"Connection: close\r\n\r\n");
    bytes.extend_from_slice(&response.body);
    stream
        .write_all(&bytes)
        .expect("response bytes should be writable");
}
