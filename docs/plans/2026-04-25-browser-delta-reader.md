# Browser Delta Reader Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make `crates/wasm-delta-snapshot` a browser-safe Delta reader over supplied `_delta_log` object descriptors, preserving listing, checkpoint selection, sidecar reads, JSON replay, active-file facts, protocol classification, and `wasm32-unknown-unknown` compatibility.

**Architecture:** Keep the existing `SnapshotResolver<S, J, P>` core and add a browser-safe HTTP manifest storage handler backed by `crates/wasm-http-object-store`. Browser code must not perform credentialed cloud listing; `_delta_log` listing comes from a trusted manifest or read-proxy response containing exact relative paths and HTTPS object URLs. `wasm-query-runtime` remains an orchestrator and consumer of `ResolvedSnapshotDescriptor`; this plan does not add browser DataFusion.

**Tech Stack:** Rust, `async_trait`, `bytes`, `query-contract`, `wasm-http-object-store`, `parquet`, `wasm-bindgen-test`, host loopback HTTP tests, `wasm32-unknown-unknown` checks.

---

## Ground Rules

- Do not add `deltalake`, DataFusion, cloud SDKs, or credential-bearing dependencies to browser-target dependencies.
- Treat browser `_delta_log` listing as a manifest/proxy contract, not direct GCS/S3 listing.
- Keep `LocalFileStorageHandler` host-only and keep the resolver generic over `StorageHandler`.
- Preserve signed URL redaction: test errors must not leak query strings or fragments.
- Use TDD for each behavior: write the failing test, run it, implement the minimum, then rerun.
- Before committing, check `git status --short` and commit only files touched by the task.

---

### Task 1: Add Browser Delta Log Manifest Types

**Files:**
- Modify: `crates/wasm-delta-snapshot/src/lib.rs`
- Test: `crates/wasm-delta-snapshot/tests/http_storage.rs`

**Step 1: Write the failing tests**

Create `crates/wasm-delta-snapshot/tests/http_storage.rs` with tests for manifest validation and deterministic listing:

```rust
use wasm_delta_snapshot::{BrowserDeltaLogManifest, BrowserDeltaLogObject};

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
```

**Step 2: Run the test to verify it fails**

Run:

```bash
cargo test -p wasm-delta-snapshot --locked --test http_storage
```

Expected: FAIL because `BrowserDeltaLogManifest` and `BrowserDeltaLogObject` do not exist.

**Step 3: Implement the manifest types**

In `crates/wasm-delta-snapshot/src/lib.rs`, add public manifest types near the existing `StorageHandler` declarations:

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserDeltaLogObject {
    relative_path: String,
    url: String,
    size_bytes: Option<u64>,
    etag: Option<String>,
}

impl BrowserDeltaLogObject {
    pub fn new(relative_path: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            relative_path: relative_path.into(),
            url: url.into(),
            size_bytes: None,
            etag: None,
        }
    }

    pub fn with_metadata(
        mut self,
        size_bytes: Option<u64>,
        etag: Option<String>,
    ) -> Self {
        self.size_bytes = size_bytes;
        self.etag = etag;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserDeltaLogManifest {
    table_uri: String,
    objects: BTreeMap<String, BrowserDeltaLogObject>,
}
```

Add methods:

```rust
impl BrowserDeltaLogManifest {
    pub fn new(
        table_uri: impl Into<String>,
        objects: Vec<BrowserDeltaLogObject>,
    ) -> Result<Self, QueryError> {
        let mut by_path = BTreeMap::new();
        for object in objects {
            validate_delta_log_relative_path(&object.relative_path)?;
            validate_browser_object_url(
                &object.url,
                runtime_target(),
                BrowserObjectUrlPolicy::HttpsOrLoopbackHttpForHostTests,
                "Delta log object URL",
            )?;
            if by_path.insert(object.relative_path.clone(), object).is_some() {
                return Err(invalid_request("duplicate Delta log object path in manifest".to_string()));
            }
        }
        Ok(Self { table_uri: table_uri.into(), objects: by_path })
    }

    pub fn table_uri(&self) -> &str {
        &self.table_uri
    }

    pub fn list_paths(&self, prefix: &str) -> Vec<String> {
        self.objects
            .keys()
            .filter(|path| path.starts_with(prefix))
            .cloned()
            .collect()
    }

    fn object(&self, relative_path: &str) -> Option<&BrowserDeltaLogObject> {
        self.objects.get(relative_path)
    }
}
```

Import `BrowserObjectUrlPolicy` and `validate_browser_object_url` from `query_contract`. Add a target-independent `validate_delta_log_relative_path` helper that rejects empty paths, absolute URIs, native absolute paths, `..`, and anything outside `_delta_log/`.

**Step 4: Run the test to verify it passes**

Run:

```bash
cargo test -p wasm-delta-snapshot --locked --test http_storage
```

Expected: PASS.

**Step 5: Commit**

```bash
git status --short
git add crates/wasm-delta-snapshot/src/lib.rs crates/wasm-delta-snapshot/tests/http_storage.rs
git commit -m "feat: add browser delta log manifest"
```

---

### Task 2: Implement HTTP-Backed Delta Log Storage

**Files:**
- Modify: `crates/wasm-delta-snapshot/Cargo.toml`
- Modify: `crates/wasm-delta-snapshot/src/lib.rs`
- Test: `crates/wasm-delta-snapshot/tests/http_storage.rs`

**Step 1: Write the failing test**

Extend `http_storage.rs` with a loopback HTTP server helper and this resolver-level test:

```rust
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
```

The helper can be a minimal `TcpListener` thread that returns exact bodies and `Content-Length`; keep it local to this test file.

**Step 2: Run the test to verify it fails**

Run:

```bash
cargo test -p wasm-delta-snapshot --locked --test http_storage
```

Expected: FAIL because `BrowserHttpDeltaLogStorageHandler` and the `wasm-http-object-store` dependency do not exist in this crate.

**Step 3: Add the dependency**

In `crates/wasm-delta-snapshot/Cargo.toml`, add:

```toml
wasm-http-object-store = { path = "../wasm-http-object-store" }
```

Do not add `reqwest` directly unless `wasm-http-object-store` cannot expose the needed constructor.

**Step 4: Implement the storage handler**

In `crates/wasm-delta-snapshot/src/lib.rs`, import:

```rust
use wasm_http_object_store::{HttpByteRange, HttpRangeReader, HttpRangeValidation};
```

Add:

```rust
pub struct BrowserHttpDeltaLogStorageHandler {
    manifest: BrowserDeltaLogManifest,
    reader: HttpRangeReader,
    request_timeout: Option<Duration>,
}

impl BrowserHttpDeltaLogStorageHandler {
    pub fn new(manifest: BrowserDeltaLogManifest) -> Self {
        Self {
            manifest,
            reader: HttpRangeReader::new(),
            request_timeout: None,
        }
    }

    pub fn with_reader(manifest: BrowserDeltaLogManifest, reader: HttpRangeReader) -> Self {
        Self {
            manifest,
            reader,
            request_timeout: None,
        }
    }

    pub fn with_request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = Some(request_timeout);
        self
    }
}
```

Implement `StorageHandler`:

```rust
#[async_trait(?Send)]
impl StorageHandler for BrowserHttpDeltaLogStorageHandler {
    async fn list_paths(&self, table_uri: &str, prefix: &str) -> Result<Vec<String>, QueryError> {
        if table_uri != self.manifest.table_uri() {
            return Err(invalid_request("Delta log manifest table_uri did not match request".to_string()));
        }
        Ok(self.manifest.list_paths(prefix))
    }

    async fn read_bytes(
        &self,
        table_uri: &str,
        relative_path: &str,
    ) -> Result<Option<Bytes>, QueryError> {
        if table_uri != self.manifest.table_uri() {
            return Err(invalid_request("Delta log manifest table_uri did not match request".to_string()));
        }
        let Some(object) = self.manifest.object(relative_path) else {
            return Ok(None);
        };
        let validation = object
            .etag
            .clone()
            .map(HttpRangeValidation::if_range_etag);
        let result = self
            .reader
            .read_range_with_validation(
                &object.url,
                HttpByteRange::Full,
                validation,
                self.request_timeout,
            )
            .await?;
        validate_manifest_object_metadata(object, &result.metadata)?;
        Ok(Some(result.bytes))
    }
}
```

Add `validate_manifest_object_metadata` to reject size mismatches when `size_bytes` is present and identity drift when `etag` is present. Use `QueryErrorCode::ObjectStoreProtocol` for protocol mismatches.

**Step 5: Run the test to verify it passes**

Run:

```bash
cargo test -p wasm-delta-snapshot --locked --test http_storage
```

Expected: PASS.

**Step 6: Commit**

```bash
git status --short
git add crates/wasm-delta-snapshot/Cargo.toml crates/wasm-delta-snapshot/src/lib.rs crates/wasm-delta-snapshot/tests/http_storage.rs
git commit -m "feat: read delta log objects over browser-safe HTTP"
```

---

### Task 3: Prove Checkpoint And Sidecar Resolution Over HTTP

**Files:**
- Modify: `crates/wasm-delta-snapshot/tests/checkpoints.rs`

**Step 1: Write the failing test**

Add an HTTP-backed variant beside the existing checkpoint tests, reusing the checkpoint file writers already in `checkpoints.rs`:

```rust
#[tokio::test]
async fn browser_http_storage_loads_v2_checkpoint_sidecars_and_replays_json() {
    let fixture = TempDir::new().expect("tempdir should be created");
    write_json_commit(
        fixture.path(),
        0,
        &[r#"{"add":{"path":"data/a.parquet","size":10,"partitionValues":{"category":"A"}}}"#],
    );
    write_uuid_checkpoint(
        fixture.path(),
        1,
        "11111111-1111-1111-1111-111111111111",
        &[CheckpointRow::sidecar("part-00001.parquet")],
    );
    write_sidecar(
        fixture.path(),
        "part-00001.parquet",
        &[CheckpointRow::add(
            "data/b.parquet",
            20,
            BTreeMap::from([("category".to_string(), Some("B".to_string()))]),
        )],
    );
    write_last_checkpoint(fixture.path(), 1, None);
    write_json_commit(
        fixture.path(),
        2,
        &[r#"{"add":{"path":"data/c.parquet","size":30,"partitionValues":{"category":"C"}}}"#],
    );

    let server = StaticDirectoryHttpServer::serve(fixture.path());
    let manifest = BrowserDeltaLogManifest::new(
        "gs://bucket/table",
        vec![
            server.object("_delta_log/00000000000000000000.json"),
            server.object("_delta_log/00000000000000000001.checkpoint.11111111-1111-1111-1111-111111111111.parquet"),
            server.object("_delta_log/_sidecars/part-00001.parquet"),
            server.object("_delta_log/_last_checkpoint"),
            server.object("_delta_log/00000000000000000002.json"),
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
        .expect("HTTP checkpoint should resolve");

    assert_eq!(snapshot.snapshot_version, 2);
    assert_eq!(
        snapshot.active_files.iter().map(|file| file.path.as_str()).collect::<Vec<_>>(),
        vec!["data/b.parquet", "data/c.parquet"]
    );
}
```

If `StaticDirectoryHttpServer` would make the file too noisy, implement the smallest helper needed at the bottom of `checkpoints.rs`. It should serve only files under the temp table root and return 404 otherwise.

**Step 2: Run the test to verify it fails**

Run:

```bash
cargo test -p wasm-delta-snapshot --locked --test checkpoints browser_http_storage_loads_v2_checkpoint_sidecars_and_replays_json -- --exact
```

Expected: FAIL until the helper and manifest object construction are finished.

**Step 3: Implement the test helper**

Add helper methods:

```rust
impl StaticDirectoryHttpServer {
    fn serve(root: &Path) -> Self { /* loopback server */ }

    fn object(&self, relative_path: &str) -> BrowserDeltaLogObject {
        BrowserDeltaLogObject::new(relative_path, self.url(relative_path))
    }
}
```

Keep query strings out of error assertions unless testing redaction.

**Step 4: Run the test to verify it passes**

Run:

```bash
cargo test -p wasm-delta-snapshot --locked --test checkpoints browser_http_storage_loads_v2_checkpoint_sidecars_and_replays_json -- --exact
```

Expected: PASS.

**Step 5: Commit**

```bash
git status --short
git add crates/wasm-delta-snapshot/tests/checkpoints.rs
git commit -m "test: prove browser http delta checkpoint replay"
```

---

### Task 4: Harden HTTP Storage Error Mapping And Secret Redaction

**Files:**
- Modify: `crates/wasm-delta-snapshot/src/lib.rs`
- Modify: `crates/wasm-delta-snapshot/tests/http_storage.rs`

**Step 1: Write failing tests**

Add tests for invalid URLs, status failures, size mismatch, ETag mismatch, and redaction:

```rust
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

    assert!(!error.message.contains("secret"));
    assert!(!error.message.contains("X-Goog-Signature"));
    assert!(!error.message.contains("frag"));
}
```

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p wasm-delta-snapshot --locked --test http_storage
```

Expected: FAIL until metadata validation and any missing redaction path are implemented.

**Step 3: Implement the minimum fixes**

Add `validate_manifest_object_metadata`:

```rust
fn validate_manifest_object_metadata(
    object: &BrowserDeltaLogObject,
    metadata: &wasm_http_object_store::HttpObjectMetadata,
) -> Result<(), QueryError> {
    if let (Some(expected), Some(actual)) = (object.size_bytes, metadata.size_bytes) {
        if expected != actual {
            return Err(QueryError::new(
                QueryErrorCode::ObjectStoreProtocol,
                format!(
                    "Delta log object '{}' size changed between manifest and read",
                    object.relative_path
                ),
                runtime_target(),
            ));
        }
    }
    if let (Some(expected), Some(actual)) = (object.etag.as_deref(), metadata.etag.as_deref()) {
        if expected != actual {
            return Err(QueryError::new(
                QueryErrorCode::ObjectStoreProtocol,
                format!(
                    "Delta log object '{}' identity changed between manifest and read",
                    object.relative_path
                ),
                runtime_target(),
            ));
        }
    }
    Ok(())
}
```

Rely on `validate_browser_object_url` and `wasm-http-object-store` for URL redaction.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p wasm-delta-snapshot --locked --test http_storage
```

Expected: PASS.

**Step 5: Commit**

```bash
git status --short
git add crates/wasm-delta-snapshot/src/lib.rs crates/wasm-delta-snapshot/tests/http_storage.rs
git commit -m "fix: harden browser delta log http storage"
```

---

### Task 5: Extend Protocol Classification Coverage

**Files:**
- Modify: `crates/wasm-delta-snapshot/tests/compatibility.rs`
- Modify if needed: `crates/query-contract/src/delta_protocol_features.rs`
- Modify if needed: `crates/wasm-delta-snapshot/src/lib.rs`

**Step 1: Write failing coverage tests**

Add tests that make classification expectations explicit for the browser Delta reader:

```rust
#[test]
fn every_known_delta_protocol_feature_has_an_explicit_browser_classification() {
    let names = query_contract::delta_protocol_feature_names().collect::<Vec<_>>();
    assert!(names.contains(&"v2Checkpoint"));
    assert!(names.contains(&"deletionVectors"));
    assert!(names.contains(&"columnMapping"));
    assert!(names.contains(&"timestampNtz"));

    for name in names {
        let feature = query_contract::delta_protocol_feature(name)
            .expect("feature should resolve by its own name");
        match feature.class {
            query_contract::DeltaProtocolFeatureClass::SupportedInBrowser
            | query_contract::DeltaProtocolFeatureClass::NativeOnly
            | query_contract::DeltaProtocolFeatureClass::TerminalUnsupported => {}
        }
    }
}
```

Add a fixture test that proves `v2Checkpoint` alone is supported, while native-only features produce `CapabilityState::NativeOnly` rather than terminal errors.

**Step 2: Run tests to verify behavior**

Run:

```bash
cargo test -p wasm-delta-snapshot --locked --test compatibility
```

Expected: PASS if the existing matrix is complete. If this unexpectedly fails, update only the classification table or resolver classification logic needed by the failure.

**Step 3: Commit if files changed**

```bash
git status --short
git add crates/wasm-delta-snapshot/tests/compatibility.rs crates/query-contract/src/delta_protocol_features.rs crates/wasm-delta-snapshot/src/lib.rs
git commit -m "test: cover browser delta protocol classification matrix"
```

---

### Task 6: Prove Runtime Can Consume The Browser HTTP Delta Reader

**Files:**
- Modify: `crates/wasm-query-runtime/tests/browser_runtime.rs`

**Step 1: Write the failing integration test**

Add a test near the existing `resolve_delta_snapshot` coverage:

```rust
#[test]
fn runtime_resolves_delta_snapshot_from_browser_http_log_manifest() {
    let rt = tokio::runtime::Runtime::new().expect("runtime should be created");
    let server = StaticHttpServer::new([(
        "/_delta_log/00000000000000000000.json",
        concat!(
            "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n",
            "{\"metaData\":{\"id\":\"test\",\"format\":{\"provider\":\"parquet\",\"options\":{}},",
            "\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":false,\\\"metadata\\\":{}}]}\",",
            "\"partitionColumns\":[],\"configuration\":{}}}\n",
            "{\"add\":{\"path\":\"data/a.parquet\",\"size\":10,\"partitionValues\":{}}}\n"
        ),
    )]);
    let manifest = BrowserDeltaLogManifest::new(
        "gs://bucket/table",
        vec![BrowserDeltaLogObject::new(
            "_delta_log/00000000000000000000.json",
            server.url("/_delta_log/00000000000000000000.json"),
        )],
    )
    .expect("manifest should validate");
    let resolver = SnapshotResolver::new(
        BrowserHttpDeltaLogStorageHandler::new(manifest),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("runtime config should validate");

    let snapshot = rt
        .block_on(session.resolve_delta_snapshot(
            &resolver,
            SnapshotResolutionRequest {
                table_uri: "gs://bucket/table".to_string(),
                snapshot_version: None,
            },
        ))
        .expect("runtime should resolve browser HTTP Delta snapshot");

    assert_eq!(snapshot.snapshot_version, 0);
    assert_eq!(snapshot.active_files[0].path, "data/a.parquet");
}
```

If `StaticHttpServer` already exists in `browser_runtime.rs`, reuse it. Otherwise add the smallest helper needed for this test.

**Step 2: Run the test to verify it fails**

Run:

```bash
cargo test -p wasm-query-runtime --locked runtime_resolves_delta_snapshot_from_browser_http_log_manifest -- --exact
```

Expected: FAIL until imports and helper wiring are complete.

**Step 3: Implement imports and helper wiring**

Import the new types from `wasm_delta_snapshot`:

```rust
use wasm_delta_snapshot::{
    BrowserDeltaLogManifest, BrowserDeltaLogObject, BrowserHttpDeltaLogStorageHandler,
    DefaultJsonHandler, DefaultParquetHandler, SnapshotResolver,
};
```

Do not add browser DataFusion or change runtime execution behavior.

**Step 4: Run the test to verify it passes**

Run:

```bash
cargo test -p wasm-query-runtime --locked runtime_resolves_delta_snapshot_from_browser_http_log_manifest -- --exact
```

Expected: PASS.

**Step 5: Commit**

```bash
git status --short
git add crates/wasm-query-runtime/tests/browser_runtime.rs
git commit -m "test: consume browser http delta reader from runtime"
```

---

### Task 7: Add WASM Smoke Coverage For The Browser Delta Reader API

**Files:**
- Modify: `crates/wasm-delta-snapshot/tests/wasm_smoke.rs`

**Step 1: Write the failing WASM smoke test**

Add a construction-only smoke test that does not require network:

```rust
#[wasm_bindgen_test]
fn browser_delta_log_manifest_and_http_storage_construct_in_wasm() {
    let manifest = BrowserDeltaLogManifest::new(
        "gs://axon-fixtures/synthetic",
        vec![BrowserDeltaLogObject::new(
            "_delta_log/00000000000000000000.json",
            "https://example.com/table/_delta_log/00000000000000000000.json?sig=redacted",
        )],
    )
    .expect("HTTPS manifest should validate in wasm");

    let paths = manifest.list_paths("_delta_log");
    assert_eq!(paths, vec!["_delta_log/00000000000000000000.json"]);

    let _storage = BrowserHttpDeltaLogStorageHandler::new(manifest);
}
```

**Step 2: Run the smoke test to verify it fails**

Run:

```bash
cargo test -p wasm-delta-snapshot --target wasm32-unknown-unknown --locked --test wasm_smoke
```

Expected: FAIL until the new types are imported and compile for `wasm32-unknown-unknown`.

**Step 3: Implement imports**

Update the existing import block in `wasm_smoke.rs`:

```rust
use wasm_delta_snapshot::{
    runtime_target, BrowserDeltaLogManifest, BrowserDeltaLogObject,
    BrowserHttpDeltaLogStorageHandler, DefaultJsonHandler, DefaultParquetHandler,
    SnapshotResolver, StorageHandler,
};
```

**Step 4: Run the smoke test to verify it passes**

Run:

```bash
cargo test -p wasm-delta-snapshot --target wasm32-unknown-unknown --locked --test wasm_smoke
```

Expected: PASS.

**Step 5: Commit**

```bash
git status --short
git add crates/wasm-delta-snapshot/tests/wasm_smoke.rs
git commit -m "test: smoke browser delta reader api in wasm"
```

---

### Task 8: Update Documentation And Release Evidence

**Files:**
- Modify: `README.md`
- Modify: `docs/program/browser-lakehouse-engine-strategy.md`
- Modify: `docs/release-gates/browser-wasm-delta-gcs-release-evidence.md`

**Step 1: Write the doc change**

Update the browser runtime section in `README.md` to state:

```markdown
`crates/wasm-delta-snapshot` can reconstruct browser-safe Delta snapshots from either injected test storage, host-only local-file storage, or a browser-safe HTTP `_delta_log` manifest backed by `wasm-http-object-store`. Browser `_delta_log` listing remains an explicit manifest/proxy input; the browser runtime still does not receive cloud credentials or perform credentialed bucket listing.
```

In `browser-lakehouse-engine-strategy.md`, update the Delta layer bullet to include HTTP manifest storage. In release evidence, add:

```bash
cargo test -p wasm-delta-snapshot --locked --test http_storage
cargo test -p wasm-delta-snapshot --target wasm32-unknown-unknown --locked --test wasm_smoke
```

**Step 2: Run doc grep to catch accidental overclaiming**

Run:

```bash
rg -n "browser DataFusion|cloud credentials|credentialed bucket listing|manifest" README.md docs/program/browser-lakehouse-engine-strategy.md docs/release-gates/browser-wasm-delta-gcs-release-evidence.md
```

Expected: Docs still say browser DataFusion is not the default V1 execution path, and browser listing is manifest/proxy based.

**Step 3: Commit**

```bash
git status --short
git add README.md docs/program/browser-lakehouse-engine-strategy.md docs/release-gates/browser-wasm-delta-gcs-release-evidence.md
git commit -m "docs: document browser delta log reader"
```

---

### Task 9: Final Verification

**Files:**
- No code changes unless verification exposes a real issue.

**Step 1: Run focused host tests**

Run:

```bash
cargo test -p wasm-delta-snapshot --locked
cargo test -p wasm-query-runtime --locked runtime_resolves_delta_snapshot_from_browser_http_log_manifest -- --exact
```

Expected: PASS.

**Step 2: Run browser-target checks**

Run:

```bash
cargo check -p wasm-http-object-store -p wasm-delta-snapshot -p wasm-query-runtime --target wasm32-unknown-unknown --locked
cargo test -p wasm-delta-snapshot --target wasm32-unknown-unknown --locked --test wasm_smoke
```

Expected: PASS.

**Step 3: Run adjacent safety checks**

Run:

```bash
cargo test -p wasm-http-object-store --locked
bash tests/security/verify_browser_dependency_guardrails.sh
```

Expected: PASS.

**Step 4: Review the final diff**

Run:

```bash
git status --short
git diff --stat
git diff -- crates/wasm-delta-snapshot crates/wasm-query-runtime README.md docs/program/browser-lakehouse-engine-strategy.md docs/release-gates/browser-wasm-delta-gcs-release-evidence.md
```

Expected:

- Browser Delta reader uses `wasm-http-object-store`.
- No browser-target dependency on DataFusion, `deltalake`, or cloud credential SDKs.
- `_delta_log` listing is supplied by manifest/proxy input.
- Checkpoints, sidecars, JSON replay, active-file facts, and protocol classification remain covered.

**Step 5: Final commit**

If Task 9 required any fixes:

```bash
git add <changed-files>
git commit -m "fix: stabilize browser delta reader verification"
```

If no fixes were needed, do not create an empty commit.

---

## Done Criteria

- `wasm-delta-snapshot` exposes a browser-safe HTTP manifest storage handler.
- `_delta_log` listing is deterministic and manifest-driven.
- JSON replay works over HTTP-backed log objects.
- Classic checkpoints, V2/UUID checkpoints, and sidecars work over HTTP-backed log objects.
- Active file descriptors preserve path, size, partition values, and stats before Parquet data files are opened.
- Protocol classification remains explicit for supported, native-only, terminal unsupported, and unknown features.
- `wasm-query-runtime` can consume the browser HTTP Delta reader through `resolve_delta_snapshot`.
- `wasm32-unknown-unknown` check and wasm smoke tests pass.
- Docs do not claim broad browser DataFusion or credentialed browser cloud listing.
