# Browser DataFusion POC Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add an isolated experimental browser DataFusion crate that proves one query path from browser-safe object bytes to DataFusion SQL results without changing Axon's default browser runtime or worker capability claim.

**Architecture:** Create `crates/wasm-datafusion-poc` as a workspace member that no existing runtime or worker depends on. The POC reads Parquet bytes through `wasm-http-object-store`, decodes them with `wasm-parquet-engine`, converts the supported scalar subset into Arrow `RecordBatch` values, registers those batches with DataFusion `MemTable`, and executes one narrow SQL profile. The shipped worker remains `BrowserRuntimeSku::Narrow` with `browser_datafusion = false`.

**Tech Stack:** Rust workspace, `datafusion = 52.4.0` from the existing lockfile, Arrow 57.3.0, `wasm-http-object-store`, `wasm-parquet-engine`, `wasm-bindgen-test`, host loopback HTTP tests, `wasm32-unknown-unknown` compile and smoke checks.

---

## Ground Rules

- Do not add DataFusion to `wasm-query-runtime`, `wasm-query-session`, `browser-sdk`, or `browser-engine-worker`.
- Do not change `BrowserWorkerCapabilities.browser_datafusion`; the default worker must continue reporting `false`.
- Do not add cloud SDKs, OpenDAL, signing code, or credential dependencies to browser-target packages.
- Keep the POC explicitly experimental in names, docs, tests, and artifact reporting.
- The first query path is intentionally narrow: one Parquet object, one in-memory DataFusion table named `axon_table`, simple projection/filter/order/limit SQL.
- Use `wasm-http-object-store` for bytes and identity/cache metrics; do not bypass it with ad hoc fetch logic.
- If DataFusion itself fails to compile for `wasm32-unknown-unknown`, stop after the compile-spike task, capture the blocker in docs, and do not wire the crate into any shipped path.

---

### Task 1: Scaffold The Experimental Crate

**Files:**
- Modify: `Cargo.toml`
- Create: `crates/wasm-datafusion-poc/Cargo.toml`
- Create: `crates/wasm-datafusion-poc/src/lib.rs`
- Create: `crates/wasm-datafusion-poc/tests/guardrails.rs`

**Step 1: Write the failing guardrail test**

Create `crates/wasm-datafusion-poc/tests/guardrails.rs`:

```rust
use query_contract::ExecutionTarget;

#[test]
fn poc_reports_browser_wasm_target_and_experimental_status() {
    assert_eq!(wasm_datafusion_poc::runtime_target(), ExecutionTarget::BrowserWasm);
    assert!(wasm_datafusion_poc::is_experimental());
    assert_eq!(wasm_datafusion_poc::DEFAULT_TABLE_NAME, "axon_table");
}
```

**Step 2: Run the test to verify it fails**

Run:

```bash
cargo test -p wasm-datafusion-poc --locked --test guardrails
```

Expected: FAIL because the crate is not in the workspace yet.

**Step 3: Add the workspace member**

In the root `Cargo.toml`, add the crate to `[workspace].members`:

```toml
  "crates/wasm-datafusion-poc",
```

Keep it out of every other crate's dependency list.

**Step 4: Create the crate manifest**

Create `crates/wasm-datafusion-poc/Cargo.toml`:

```toml
[package]
name = "wasm-datafusion-poc"
edition.workspace = true
license.workspace = true
version.workspace = true
publish = false

[dependencies]
arrow-array = "=57.3.0"
arrow-schema = "=57.3.0"
bytes = "=1.11.1"
datafusion = { version = "=52.4.0", default-features = false, features = ["sql"] }
futures-util = "=0.3.32"
query-contract = { path = "../query-contract" }
wasm-http-object-store = { path = "../wasm-http-object-store" }
wasm-parquet-engine = { path = "../wasm-parquet-engine" }

[dev-dependencies]
parquet = { version = "=57.3.0", default-features = false }
serde_json.workspace = true

[target.'cfg(not(target_arch = "wasm32"))'.dev-dependencies]
tokio = { version = "=1.50.0", features = ["macros", "rt-multi-thread"] }

[target.'cfg(target_arch = "wasm32")'.dev-dependencies]
wasm-bindgen-test = "=0.3.64"
```

**Step 5: Create the minimal library**

Create `crates/wasm-datafusion-poc/src/lib.rs`:

```rust
//! Experimental browser DataFusion proof-of-concept.
//!
//! This crate is intentionally isolated from Axon's default browser runtime and worker artifact.

use query_contract::ExecutionTarget;

pub const OWNER: &str = "Runtime / engine team";
pub const RESPONSIBILITY: &str =
    "Experimental browser DataFusion proof over browser-safe object bytes.";
pub const DEFAULT_TABLE_NAME: &str = "axon_table";

pub fn runtime_target() -> ExecutionTarget {
    ExecutionTarget::BrowserWasm
}

pub fn is_experimental() -> bool {
    true
}
```

**Step 6: Run the test to verify it passes**

Run:

```bash
cargo test -p wasm-datafusion-poc --locked --test guardrails
```

Expected: PASS.

**Step 7: Prove the default worker still has no POC dependency**

Run:

```bash
cargo tree -p browser-engine-worker --target wasm32-unknown-unknown --locked | rg "wasm-datafusion-poc|datafusion" || true
```

Expected: no output. If there is output, remove the accidental dependency before continuing.

**Step 8: Commit**

```bash
git status --short
git add Cargo.toml crates/wasm-datafusion-poc
git commit -m "feat: scaffold isolated browser datafusion poc"
```

---

### Task 2: Prove DataFusion Compiles For The Browser Target

**Files:**
- Modify: `crates/wasm-datafusion-poc/src/lib.rs`
- Create: `crates/wasm-datafusion-poc/tests/wasm_smoke.rs`
- Modify if needed: `crates/wasm-datafusion-poc/Cargo.toml`

**Step 1: Write the failing host and wasm tests**

Append to `crates/wasm-datafusion-poc/tests/guardrails.rs`:

```rust
#[test]
fn datafusion_session_context_constructs_on_host() {
    let marker = wasm_datafusion_poc::datafusion_compile_marker();
    assert_eq!(marker.table_name, "axon_table");
    assert!(marker.datafusion_version.starts_with("52."));
}
```

Create `crates/wasm-datafusion-poc/tests/wasm_smoke.rs`:

```rust
#![cfg(target_arch = "wasm32")]

use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen_test]
fn datafusion_session_context_constructs_in_wasm() {
    let marker = wasm_datafusion_poc::datafusion_compile_marker();
    assert_eq!(marker.table_name, "axon_table");
    assert!(marker.datafusion_version.starts_with("52."));
}
```

**Step 2: Run the tests to verify they fail**

Run:

```bash
cargo test -p wasm-datafusion-poc --locked --test guardrails
cargo test -p wasm-datafusion-poc --target wasm32-unknown-unknown --locked --test wasm_smoke
```

Expected: FAIL because `datafusion_compile_marker` does not exist.

If the wasm command fails before test execution due to a DataFusion compile error, capture the exact error and follow `superpowers:systematic-debugging`. Do not proceed to Task 3 until `SessionContext` can at least construct under `wasm32-unknown-unknown`, or until a blocker doc is written.

**Step 3: Implement the compile marker**

In `src/lib.rs`, add:

```rust
use datafusion::prelude::SessionContext;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataFusionCompileMarker {
    pub table_name: &'static str,
    pub datafusion_version: &'static str,
}

pub fn datafusion_compile_marker() -> DataFusionCompileMarker {
    let _context = SessionContext::new();
    DataFusionCompileMarker {
        table_name: DEFAULT_TABLE_NAME,
        datafusion_version: "52.4.0",
    }
}
```

If `datafusion = { default-features = false, features = ["sql"] }` does not expose `SessionContext`, update the dependency minimally. Prefer adding `parquet` only if compiler output requires it; do not enable default features as the first move.

**Step 4: Run the tests to verify they pass**

Run:

```bash
cargo test -p wasm-datafusion-poc --locked --test guardrails
cargo test -p wasm-datafusion-poc --target wasm32-unknown-unknown --locked --test wasm_smoke
```

Expected: PASS.

**Step 5: Commit**

```bash
git status --short
git add crates/wasm-datafusion-poc
git commit -m "test: prove datafusion constructs in wasm poc"
```

---

### Task 3: Query A DataFusion MemTable From Arrow RecordBatches

**Files:**
- Modify: `crates/wasm-datafusion-poc/src/lib.rs`
- Create: `crates/wasm-datafusion-poc/tests/memtable_query.rs`
- Modify: `crates/wasm-datafusion-poc/tests/wasm_smoke.rs`

**Step 1: Write the failing host test**

Create `crates/wasm-datafusion-poc/tests/memtable_query.rs`:

```rust
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

#[tokio::test]
async fn datafusion_queries_registered_record_batches() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["A", "B", "B"])),
        ],
    )
    .expect("batch should construct");

    let result = wasm_datafusion_poc::query_record_batch(
        "SELECT id, category FROM axon_table WHERE id > 1 ORDER BY id",
        batch,
    )
    .await
    .expect("DataFusion query should run");

    assert_eq!(result.row_count, 2);
    assert_eq!(result.column_names, vec!["id", "category"]);
}
```

**Step 2: Add a failing wasm test**

Append to `wasm_smoke.rs`:

```rust
#[wasm_bindgen_test]
async fn datafusion_queries_record_batches_in_wasm() {
    let batch = wasm_datafusion_poc::synthetic_record_batch()
        .expect("synthetic batch should construct");
    let result = wasm_datafusion_poc::query_record_batch(
        "SELECT id FROM axon_table WHERE id >= 2 ORDER BY id",
        batch,
    )
    .await
    .expect("DataFusion query should run in wasm");

    assert_eq!(result.row_count, 2);
    assert_eq!(result.column_names, vec!["id"]);
}
```

**Step 3: Run tests to verify they fail**

Run:

```bash
cargo test -p wasm-datafusion-poc --locked --test memtable_query
cargo test -p wasm-datafusion-poc --target wasm32-unknown-unknown --locked --test wasm_smoke
```

Expected: FAIL because `query_record_batch`, `synthetic_record_batch`, and `ExperimentalQueryResult` do not exist.

**Step 4: Implement MemTable query execution**

In `src/lib.rs`, add:

```rust
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use query_contract::{QueryError, QueryErrorCode};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExperimentalQueryResult {
    pub row_count: usize,
    pub column_names: Vec<String>,
}

pub async fn query_record_batch(
    sql: &str,
    batch: RecordBatch,
) -> Result<ExperimentalQueryResult, QueryError> {
    let schema = batch.schema();
    let table = MemTable::try_new(schema, vec![vec![batch]]).map_err(map_datafusion_error)?;
    let context = SessionContext::new();
    context
        .register_table(DEFAULT_TABLE_NAME, Arc::new(table))
        .map_err(map_datafusion_error)?;
    let frame = context.sql(sql).await.map_err(map_datafusion_error)?;
    let batches = frame.collect().await.map_err(map_datafusion_error)?;
    let row_count = batches.iter().map(RecordBatch::num_rows).sum();
    let column_names = batches
        .first()
        .map(|batch| {
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().to_string())
                .collect()
        })
        .unwrap_or_default();

    Ok(ExperimentalQueryResult {
        row_count,
        column_names,
    })
}

pub fn synthetic_record_batch() -> Result<RecordBatch, QueryError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["A", "B", "B"])),
        ],
    )
    .map_err(|error| {
        QueryError::new(
            QueryErrorCode::ExecutionFailed,
            format!("experimental DataFusion batch construction failed: {error}"),
            runtime_target(),
        )
    })
}

fn map_datafusion_error(error: datafusion::error::DataFusionError) -> QueryError {
    QueryError::new(
        QueryErrorCode::ExecutionFailed,
        format!("experimental browser DataFusion query failed: {error}"),
        runtime_target(),
    )
}
```

If `datafusion::datasource::MemTable` moved, find the local 52.4.0 export with:

```bash
rg -n "pub struct MemTable|pub use .*MemTable" ~/.cargo/registry/src/*/datafusion-52.4.0 ~/.cargo/registry/src/*/datafusion-catalog-52.4.0
```

**Step 5: Run tests to verify they pass**

Run:

```bash
cargo test -p wasm-datafusion-poc --locked --test memtable_query
cargo test -p wasm-datafusion-poc --target wasm32-unknown-unknown --locked --test wasm_smoke
```

Expected: PASS.

**Step 6: Commit**

```bash
git status --short
git add crates/wasm-datafusion-poc
git commit -m "feat: query record batches with browser datafusion poc"
```

---

### Task 4: Convert Parquet Engine Rows Into Arrow Batches

**Files:**
- Modify: `crates/wasm-datafusion-poc/src/lib.rs`
- Create: `crates/wasm-datafusion-poc/tests/parquet_rows.rs`

**Step 1: Write the failing test**

Create `crates/wasm-datafusion-poc/tests/parquet_rows.rs`:

```rust
use std::collections::BTreeMap;
use wasm_parquet_engine::ParquetScalarValue;

#[tokio::test]
async fn parquet_rows_convert_to_arrow_and_query_with_datafusion() {
    let rows = vec![
        BTreeMap::from([
            ("id".to_string(), ParquetScalarValue::Int64(1)),
            ("category".to_string(), ParquetScalarValue::String("A".to_string())),
        ]),
        BTreeMap::from([
            ("id".to_string(), ParquetScalarValue::Int64(2)),
            ("category".to_string(), ParquetScalarValue::String("B".to_string())),
        ]),
        BTreeMap::from([
            ("id".to_string(), ParquetScalarValue::Int64(3)),
            ("category".to_string(), ParquetScalarValue::String("B".to_string())),
        ]),
    ];
    let schema = vec![
        wasm_datafusion_poc::ExperimentalColumn::int64("id"),
        wasm_datafusion_poc::ExperimentalColumn::utf8("category"),
    ];

    let batch = wasm_datafusion_poc::record_batch_from_parquet_rows(&schema, &rows)
        .expect("rows should convert");
    let result = wasm_datafusion_poc::query_record_batch(
        "SELECT category FROM axon_table WHERE id = 2",
        batch,
    )
    .await
    .expect("DataFusion query should run");

    assert_eq!(result.row_count, 1);
    assert_eq!(result.column_names, vec!["category"]);
}
```

**Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p wasm-datafusion-poc --locked --test parquet_rows
```

Expected: FAIL because `ExperimentalColumn` and `record_batch_from_parquet_rows` do not exist.

**Step 3: Implement the narrow row bridge**

In `src/lib.rs`, add:

```rust
use arrow_array::{ArrayRef, BooleanArray};
use wasm_parquet_engine::{ParquetInputRow, ParquetScalarValue};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExperimentalColumnType {
    Int64,
    Utf8,
    Boolean,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExperimentalColumn {
    pub name: String,
    pub column_type: ExperimentalColumnType,
}

impl ExperimentalColumn {
    pub fn int64(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            column_type: ExperimentalColumnType::Int64,
        }
    }

    pub fn utf8(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            column_type: ExperimentalColumnType::Utf8,
        }
    }

    pub fn boolean(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            column_type: ExperimentalColumnType::Boolean,
        }
    }
}

pub fn record_batch_from_parquet_rows(
    columns: &[ExperimentalColumn],
    rows: &[ParquetInputRow],
) -> Result<RecordBatch, QueryError> {
    let fields = columns
        .iter()
        .map(|column| {
            Field::new(
                &column.name,
                match column.column_type {
                    ExperimentalColumnType::Int64 => DataType::Int64,
                    ExperimentalColumnType::Utf8 => DataType::Utf8,
                    ExperimentalColumnType::Boolean => DataType::Boolean,
                },
                true,
            )
        })
        .collect::<Vec<_>>();

    let arrays = columns
        .iter()
        .map(|column| column_array(column, rows))
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|error| {
        QueryError::new(
            QueryErrorCode::ExecutionFailed,
            format!("experimental DataFusion Arrow batch conversion failed: {error}"),
            runtime_target(),
        )
    })
}

fn column_array(
    column: &ExperimentalColumn,
    rows: &[ParquetInputRow],
) -> Result<ArrayRef, QueryError> {
    match column.column_type {
        ExperimentalColumnType::Int64 => {
            let values = rows
                .iter()
                .map(|row| match row.get(&column.name) {
                    Some(ParquetScalarValue::Int64(value)) => Some(*value),
                    Some(ParquetScalarValue::Null) | None => None,
                    Some(other) => return_type_error(&column.name, "Int64", other),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(Int64Array::from(values)))
        }
        ExperimentalColumnType::Utf8 => {
            let values = rows
                .iter()
                .map(|row| match row.get(&column.name) {
                    Some(ParquetScalarValue::String(value)) => Ok(Some(value.as_str())),
                    Some(ParquetScalarValue::Null) | None => Ok(None),
                    Some(other) => return_type_error(&column.name, "Utf8", other),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(StringArray::from(values)))
        }
        ExperimentalColumnType::Boolean => {
            let values = rows
                .iter()
                .map(|row| match row.get(&column.name) {
                    Some(ParquetScalarValue::Boolean(value)) => Some(*value),
                    Some(ParquetScalarValue::Null) | None => None,
                    Some(other) => return_type_error(&column.name, "Boolean", other),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(BooleanArray::from(values)))
        }
    }
}

fn return_type_error<T>(
    column: &str,
    expected: &str,
    actual: &ParquetScalarValue,
) -> Result<T, QueryError> {
    Err(QueryError::new(
        QueryErrorCode::UnsupportedFeature,
        format!(
            "experimental DataFusion POC column '{column}' expected {expected}, got {actual:?}"
        ),
        runtime_target(),
    ))
}
```

Keep the supported scalar set this small until the POC proves the end-to-end path.

**Step 4: Run test to verify it passes**

Run:

```bash
cargo test -p wasm-datafusion-poc --locked --test parquet_rows
```

Expected: PASS.

**Step 5: Commit**

```bash
git status --short
git add crates/wasm-datafusion-poc
git commit -m "feat: bridge parquet rows into datafusion batches"
```

---

### Task 5: Prove Browser-Local Object Bytes To DataFusion SQL In WASM

**Files:**
- Modify: `crates/wasm-datafusion-poc/src/lib.rs`
- Modify: `crates/wasm-datafusion-poc/tests/wasm_smoke.rs`
- Create: `crates/wasm-datafusion-poc/tests/support/parquet_fixture.rs` if needed

**Step 1: Write the failing wasm test**

Append to `wasm_smoke.rs`:

```rust
#[wasm_bindgen_test]
async fn browser_local_object_parquet_queries_with_datafusion_in_wasm() {
    let parquet_bytes = wasm_datafusion_poc::test_parquet_bytes_with_single_i64_column(&[1, 2, 3]);
    let result = wasm_datafusion_poc::query_browser_local_parquet_object(
        "SELECT id FROM axon_table WHERE id > 1 ORDER BY id",
        "memory://synthetic/part-000.parquet",
        parquet_bytes.into(),
        vec![wasm_datafusion_poc::ExperimentalColumn::int64("id")],
    )
    .await
    .expect("browser-local object DataFusion query should run in wasm");

    assert_eq!(result.row_count, 2);
    assert_eq!(result.column_names, vec!["id"]);
    assert!(result.object_bytes_read > 0);
    assert!(result.transport_bytes_reused <= result.object_bytes_read);
}
```

**Step 2: Run the wasm test to verify it fails**

Run:

```bash
cargo test -p wasm-datafusion-poc --target wasm32-unknown-unknown --locked --test wasm_smoke
```

Expected: FAIL because `query_browser_local_parquet_object`, the extended result metrics, and the test Parquet helper do not exist.

**Step 3: Implement the local-object query path**

In `src/lib.rs`, add:

```rust
use bytes::Bytes;
use wasm_http_object_store::{
    BrowserLocalObject, BrowserObject, BrowserObjectRangeReader, ByteExtent,
};
use wasm_parquet_engine::{decode_parquet_input_rows, ObjectSource, ScanTarget};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExperimentalObjectQueryResult {
    pub row_count: usize,
    pub column_names: Vec<String>,
    pub object_bytes_read: u64,
    pub transport_bytes_reused: u64,
}

pub async fn query_browser_local_parquet_object(
    sql: &str,
    resource: impl Into<String>,
    parquet_bytes: Bytes,
    columns: Vec<ExperimentalColumn>,
) -> Result<ExperimentalObjectQueryResult, QueryError> {
    let resource = resource.into();
    let object_len = parquet_bytes.len() as u64;
    let local = BrowserLocalObject::from_bytes(resource.clone(), parquet_bytes);
    let object = BrowserObject::local(local);
    let mut reader = BrowserObjectRangeReader::new();
    let extent = ByteExtent::new(0, object_len)?;
    let read = reader.read_extent(&object, extent, None).await?;
    let target = ScanTarget {
        object_source: ObjectSource::new(resource.clone()),
        object_etag: Some(read.metadata.identity.clone()),
        path: resource,
        size_bytes: read.metadata.size_bytes,
        partition_values: Default::default(),
    };
    let required_columns = columns.iter().map(|column| column.name.clone()).collect::<Vec<_>>();
    let rows = decode_parquet_input_rows(&target, read.bytes, &required_columns)?;
    let batch = record_batch_from_parquet_rows(&columns, &rows)?;
    let query = query_record_batch(sql, batch).await?;
    let metrics = reader.metrics();

    Ok(ExperimentalObjectQueryResult {
        row_count: query.row_count,
        column_names: query.column_names,
        object_bytes_read: metrics.bytes_fetched,
        transport_bytes_reused: metrics.bytes_reused,
    })
}
```

Add a `#[cfg(any(test, target_arch = "wasm32"))]` helper only if tests need it:

```rust
#[cfg(any(test, target_arch = "wasm32"))]
pub fn test_parquet_bytes_with_single_i64_column(values: &[i64]) -> Vec<u8> {
    // Reuse the writer pattern from crates/wasm-parquet-engine/tests/wasm_smoke.rs.
}
```

Use the same small Parquet writer pattern from `crates/wasm-parquet-engine/tests/wasm_smoke.rs`; keep it test-only.

**Step 4: Run the wasm test to verify it passes**

Run:

```bash
cargo test -p wasm-datafusion-poc --target wasm32-unknown-unknown --locked --test wasm_smoke
```

Expected: PASS.

**Step 5: Commit**

```bash
git status --short
git add crates/wasm-datafusion-poc
git commit -m "feat: prove browser local object datafusion query"
```

---

### Task 6: Prove HTTP Range Bytes To DataFusion SQL On Host

**Files:**
- Modify: `crates/wasm-datafusion-poc/src/lib.rs`
- Create: `crates/wasm-datafusion-poc/tests/http_object_query.rs`

**Step 1: Write the failing host test**

Create `crates/wasm-datafusion-poc/tests/http_object_query.rs`:

```rust
#[tokio::test]
async fn loopback_http_parquet_queries_with_datafusion_using_object_store_substrate() {
    let parquet_bytes = wasm_datafusion_poc::test_parquet_bytes_with_single_i64_column(&[1, 2, 3]);
    let server = StaticHttpServer::new("/part-000.parquet", parquet_bytes.clone());

    let result = wasm_datafusion_poc::query_http_parquet_object(
        "SELECT id FROM axon_table WHERE id >= 2 ORDER BY id",
        server.url("/part-000.parquet"),
        parquet_bytes.len() as u64,
        vec![wasm_datafusion_poc::ExperimentalColumn::int64("id")],
    )
    .await
    .expect("HTTP object DataFusion query should run");

    assert_eq!(result.row_count, 2);
    assert_eq!(result.column_names, vec!["id"]);
    assert_eq!(server.range_request_count(), 1);
    assert!(result.object_bytes_read > 0);
}
```

Add a tiny `StaticHttpServer` helper that supports one `Range: bytes=0-N` request and returns `206 Partial Content` with `Content-Range`, `Content-Length`, and `ETag`.

**Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p wasm-datafusion-poc --locked --test http_object_query
```

Expected: FAIL because `query_http_parquet_object` does not exist.

**Step 3: Implement HTTP object query path**

In `src/lib.rs`, add:

```rust
pub async fn query_http_parquet_object(
    sql: &str,
    url: impl Into<String>,
    object_size_bytes: u64,
    columns: Vec<ExperimentalColumn>,
) -> Result<ExperimentalObjectQueryResult, QueryError> {
    let url = url.into();
    let object = BrowserObject::http(url.clone());
    let mut reader = BrowserObjectRangeReader::new();
    let extent = ByteExtent::new(0, object_size_bytes)?;
    let read = reader.read_extent(&object, extent, None).await?;
    let target = ScanTarget {
        object_source: ObjectSource::new(url.clone()),
        object_etag: read.metadata.identity.clone().into(),
        path: url,
        size_bytes: read.metadata.size_bytes,
        partition_values: Default::default(),
    };
    let required_columns = columns.iter().map(|column| column.name.clone()).collect::<Vec<_>>();
    let rows = decode_parquet_input_rows(&target, read.bytes, &required_columns)?;
    let batch = record_batch_from_parquet_rows(&columns, &rows)?;
    let query = query_record_batch(sql, batch).await?;
    let metrics = reader.metrics();

    Ok(ExperimentalObjectQueryResult {
        row_count: query.row_count,
        column_names: query.column_names,
        object_bytes_read: metrics.bytes_fetched,
        transport_bytes_reused: metrics.bytes_reused,
    })
}
```

If `object_etag` assignment does not compile because `identity` is not optional, use `Some(read.metadata.identity.clone())`.

**Step 4: Run test to verify it passes**

Run:

```bash
cargo test -p wasm-datafusion-poc --locked --test http_object_query
```

Expected: PASS.

**Step 5: Commit**

```bash
git status --short
git add crates/wasm-datafusion-poc
git commit -m "feat: prove http object datafusion query poc"
```

---

### Task 7: Add Explicit Experimental Artifact Reporting

**Files:**
- Modify: `crates/wasm-datafusion-poc/src/lib.rs`
- Create: `crates/wasm-datafusion-poc/tests/artifact_report.rs`
- Modify: `docs/program/browser-lakehouse-release-handoff-examples/browser-worker-artifact-report.narrow.json` only if needed to prove it stays unchanged

**Step 1: Write the failing report test**

Create `crates/wasm-datafusion-poc/tests/artifact_report.rs`:

```rust
#[test]
fn datafusion_poc_report_is_explicitly_experimental() {
    let report = wasm_datafusion_poc::experimental_artifact_report();

    assert_eq!(report.runtime_sku, "experimental_datafusion_poc");
    assert!(report.browser_datafusion);
    assert!(!report.default_worker_runtime);
    assert_eq!(report.result_transport, "record_batches");
}
```

**Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p wasm-datafusion-poc --locked --test artifact_report
```

Expected: FAIL because no report exists.

**Step 3: Implement the report**

In `src/lib.rs`, add:

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExperimentalDataFusionArtifactReport {
    pub runtime_sku: &'static str,
    pub browser_datafusion: bool,
    pub default_worker_runtime: bool,
    pub result_transport: &'static str,
    pub object_substrate: &'static str,
}

pub fn experimental_artifact_report() -> ExperimentalDataFusionArtifactReport {
    ExperimentalDataFusionArtifactReport {
        runtime_sku: "experimental_datafusion_poc",
        browser_datafusion: true,
        default_worker_runtime: false,
        result_transport: "record_batches",
        object_substrate: "wasm-http-object-store",
    }
}
```

Do not modify `browser-engine-worker::artifact_report()` in this task.

**Step 4: Add a default worker guard**

Run:

```bash
cargo test -p browser-engine-worker --locked worker_artifact_reports_session_capability_without_claiming_browser_datafusion -- --exact
```

Expected: PASS and still asserts `browser_datafusion == false`.

**Step 5: Commit**

```bash
git status --short
git add crates/wasm-datafusion-poc
git commit -m "feat: report experimental datafusion poc artifact"
```

---

### Task 8: Add Optional CI And Release Evidence Without Changing Default Gates

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `docs/release-gates/browser-wasm-delta-gcs-release-evidence.md`
- Modify: `docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md`

**Step 1: Add CI commands as an experimental section**

In `.github/workflows/ci.yml`, add separate steps after the default browser wasm checks:

```yaml
      - name: Check experimental browser DataFusion POC
        run: cargo check -p wasm-datafusion-poc --target wasm32-unknown-unknown --locked

      - name: Test experimental browser DataFusion POC
        run: cargo test -p wasm-datafusion-poc --locked

      - name: Test experimental browser DataFusion POC wasm smoke
        run: cargo test -p wasm-datafusion-poc --target wasm32-unknown-unknown --locked --test wasm_smoke
```

If the compile spike exposed upstream DataFusion wasm blockers, do not add these CI steps. Instead, add a docs-only blocker entry with the failing command and error class.

**Step 2: Update release evidence**

Add this section to `docs/release-gates/browser-wasm-delta-gcs-release-evidence.md`:

```markdown
## Experimental Browser DataFusion POC

These commands are evidence for the isolated POC only. They do not change the shipped worker SKU or release claim.

```bash
cargo test -p wasm-datafusion-poc --locked
cargo check -p wasm-datafusion-poc --target wasm32-unknown-unknown --locked
cargo test -p wasm-datafusion-poc --target wasm32-unknown-unknown --locked --test wasm_smoke
```
```

**Step 3: Update launch checklist wording**

In `docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md`, keep the default V1 claim unchanged and add:

```markdown
- [ ] Experimental browser DataFusion POC evidence is tracked separately from the shipped browser V1 SKU.
```

**Step 4: Run the docs/CI syntax check**

Run:

```bash
rg -n "experimental browser DataFusion|wasm-datafusion-poc|browser_datafusion = false|broad browser DataFusion" .github/workflows/ci.yml docs/release-gates docs/program README.md
```

Expected: Output shows the POC is experimental and the default worker claim remains narrow.

**Step 5: Commit**

```bash
git status --short
git add .github/workflows/ci.yml docs/release-gates/browser-wasm-delta-gcs-release-evidence.md docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md
git commit -m "ci: track experimental browser datafusion poc"
```

---

### Task 9: Update Program Docs And Dependency Guardrails

**Files:**
- Modify: `README.md`
- Modify: `docs/program/browser-lakehouse-engine-strategy.md`
- Modify: `docs/program/browser-release-integration-runbook.md`
- Modify: `tests/security/README.md`

**Step 1: Update README**

Add a new section after the Browser Runtime Envelope:

```markdown
## Experimental Browser DataFusion POC

`crates/wasm-datafusion-poc` is an isolated experiment that proves one browser-safe object-to-DataFusion path. It reads Parquet bytes through `wasm-http-object-store`, decodes the narrow supported scalar subset through `wasm-parquet-engine`, registers Arrow batches in a DataFusion `MemTable`, and executes a small SQL profile over `axon_table`.

This crate is not a dependency of `wasm-query-runtime`, `wasm-query-session`, `browser-sdk`, or `browser-engine-worker`. The shipped worker still reports `browser_datafusion = false`; broad browser DataFusion remains outside the default V1 SKU.
```

**Step 2: Update strategy docs**

In `docs/program/browser-lakehouse-engine-strategy.md`, under "Browser DataFusion Is Deferred For The Default SKU", add:

```markdown
The isolated `wasm-datafusion-poc` crate may exist as experimental evidence. It must not be treated as the default runtime until it has separate size, runtime, SQL breadth, parity, and fallback gates.
```

**Step 3: Update runbook**

In `docs/program/browser-release-integration-runbook.md`, add an experimental troubleshooting note:

```markdown
The DataFusion POC is not part of the shipped worker. A failure in `wasm-datafusion-poc` blocks only experimental DataFusion evidence unless a release explicitly opts into that SKU.
```

**Step 4: Update security docs**

In `tests/security/README.md`, add:

```markdown
The dependency guardrail command remains scoped to `browser-engine-worker`. `wasm-datafusion-poc` is allowed to carry DataFusion as an experimental dependency, but it must not introduce cloud credential SDKs, OpenDAL, signing libraries, or service-account markers.
```

**Step 5: Run doc checks**

Run:

```bash
rg -n "wasm-datafusion-poc|browser_datafusion = false|default V1 SKU|cloud credential|OpenDAL" README.md docs/program tests/security/README.md
```

Expected: Docs identify the POC as isolated and preserve the default worker claim.

**Step 6: Commit**

```bash
git status --short
git add README.md docs/program/browser-lakehouse-engine-strategy.md docs/program/browser-release-integration-runbook.md tests/security/README.md
git commit -m "docs: document experimental browser datafusion poc"
```

---

### Task 10: Final Verification

**Files:**
- No code changes unless verification exposes a real issue.

**Step 1: Run POC verification**

Run:

```bash
cargo test -p wasm-datafusion-poc --locked
cargo check -p wasm-datafusion-poc --target wasm32-unknown-unknown --locked
cargo test -p wasm-datafusion-poc --target wasm32-unknown-unknown --locked --test wasm_smoke
```

Expected: PASS.

**Step 2: Run default browser verification**

Run:

```bash
cargo test -p browser-engine-worker --locked worker_artifact_reports_session_capability_without_claiming_browser_datafusion -- --exact
cargo check -p wasm-query-runtime -p wasm-query-session -p browser-sdk -p browser-engine-worker --target wasm32-unknown-unknown --locked
```

Expected: PASS, and the worker still reports `browser_datafusion = false`.

**Step 3: Run object substrate verification**

Run:

```bash
cargo test -p wasm-http-object-store --locked
cargo test -p wasm-parquet-engine --locked
```

Expected: PASS.

**Step 4: Run dependency guardrail check**

First build the worker artifact if needed:

```bash
cargo build -p browser-engine-worker --target wasm32-unknown-unknown --release --locked
bash tests/security/verify_browser_dependency_guardrails.sh
```

Expected: PASS. The guardrail checks `browser-engine-worker`, not the experimental POC crate.

**Step 5: Review dependency isolation**

Run:

```bash
cargo tree -p browser-engine-worker --target wasm32-unknown-unknown --locked | rg "wasm-datafusion-poc|datafusion" || true
cargo tree -p wasm-datafusion-poc --target wasm32-unknown-unknown --locked | rg "datafusion|opendal|aws|azure|gcp|google|credential|oauth" || true
```

Expected:

- first command: no output
- second command: DataFusion is present; cloud credential/signing dependencies are not present

**Step 6: Review final diff**

Run:

```bash
git status --short
git diff --stat
git diff -- Cargo.toml crates/wasm-datafusion-poc README.md docs/program docs/release-gates tests/security/README.md .github/workflows/ci.yml
```

Expected:

- POC crate is isolated.
- Default worker code is unchanged except for tests/docs if explicitly touched.
- No default runtime or SDK crate depends on `wasm-datafusion-poc`.
- Docs avoid claiming broad browser DataFusion as shipped.

**Step 7: Final commit if verification required fixes**

```bash
git add <changed-files>
git commit -m "fix: stabilize experimental browser datafusion poc"
```

Skip this if Task 10 made no changes.

---

## Done Criteria

- `crates/wasm-datafusion-poc` exists and is a workspace member.
- The POC constructs DataFusion under host and `wasm32-unknown-unknown`.
- One WASM smoke test executes DataFusion SQL over an Arrow batch.
- One WASM smoke test reads browser-local object bytes through `wasm-http-object-store`, decodes Parquet through `wasm-parquet-engine`, and queries the result through DataFusion.
- One host test proves the loopback HTTP range-read path into DataFusion SQL.
- `browser-engine-worker` still reports `browser_datafusion = false`.
- `wasm-query-runtime`, `wasm-query-session`, `browser-sdk`, and `browser-engine-worker` do not depend on the POC crate.
- Docs and release evidence clearly label the work as experimental and separate from the default V1 SKU.
