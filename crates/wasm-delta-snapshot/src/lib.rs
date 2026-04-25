//! Browser-safe Delta snapshot reconstruction.

use std::collections::{BTreeMap, BTreeSet};
#[cfg(not(target_arch = "wasm32"))]
use std::fs;
#[cfg(all(not(target_arch = "wasm32"), unix))]
use std::io::Read;
use std::path::{Component, Path};
#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;
#[cfg(all(not(target_arch = "wasm32"), unix))]
use std::{
    ffi::CString,
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::ffi::OsStrExt,
    },
};

use async_trait::async_trait;
use bytes::Bytes;
use parquet::file::reader::{FileReader as ParquetFileReader, SerializedFileReader};
use parquet::record::Field as ParquetField;
use query_contract::{
    delta_protocol_feature, validate_browser_object_url, BrowserObjectUrlPolicy, CapabilityKey,
    CapabilityReport, CapabilityState, DeltaProtocolFeature as KnownDeltaFeature,
    DeltaProtocolFeatureClass as KnownFeatureClass,
    DeltaProtocolFeatureEnablement as KnownFeatureEnablement,
    DeltaProtocolFeatureKind as KnownFeatureKind, ExecutionTarget, PartitionColumnType,
    QueryError, QueryErrorCode, ResolvedFileDescriptor, ResolvedSnapshotDescriptor,
    SnapshotResolutionRequest, KNOWN_DELTA_PROTOCOL_FEATURES as KNOWN_DELTA_FEATURES,
};
use serde::Deserialize;
use serde_json::Value as JsonValue;

pub const OWNER: &str = "Runtime / engine team";
pub const RESPONSIBILITY: &str = "Browser-safe Delta snapshot reconstruction.";

const DELTA_LOG_DIR: &str = "_delta_log";
const LAST_CHECKPOINT_PATH: &str = "_delta_log/_last_checkpoint";

pub fn runtime_target() -> ExecutionTarget {
    ExecutionTarget::BrowserWasm
}

#[async_trait(?Send)]
pub trait StorageHandler {
    async fn list_paths(&self, table_uri: &str, prefix: &str) -> Result<Vec<String>, QueryError>;

    async fn read_bytes(
        &self,
        table_uri: &str,
        relative_path: &str,
    ) -> Result<Option<Bytes>, QueryError>;
}

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

    pub fn with_metadata(mut self, size_bytes: Option<u64>, etag: Option<String>) -> Self {
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
                return Err(invalid_request(
                    "duplicate Delta log object path in manifest".to_string(),
                ));
            }
        }
        Ok(Self {
            table_uri: table_uri.into(),
            objects: by_path,
        })
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

pub trait JsonHandler {
    fn parse_last_checkpoint_hint(
        &self,
        bytes: &[u8],
    ) -> Result<Option<LastCheckpointHint>, QueryError>;

    fn parse_log_actions(&self, bytes: &[u8]) -> Result<Vec<SnapshotLogAction>, QueryError>;
}

pub trait ParquetHandler {
    fn parse_checkpoint_actions(
        &self,
        relative_path: &str,
        bytes: Bytes,
    ) -> Result<Vec<SnapshotLogAction>, QueryError>;
}

trait CompatibilityHandler {
    fn classify(
        &self,
        replay_state: &SnapshotReplayState,
        schema_derivations: &SchemaDerivations,
    ) -> Result<CapabilityReport, QueryError>;
}

#[derive(Clone, Debug, Default)]
pub struct DefaultJsonHandler;

#[derive(Clone, Debug, Default)]
pub struct DefaultParquetHandler;

#[derive(Clone, Debug, Default)]
struct DefaultCompatibilityHandler;

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug, Default)]
pub struct LocalFileStorageHandler;

#[derive(Clone, Debug)]
pub struct SnapshotResolver<S, J, P> {
    storage: S,
    json: J,
    parquet: P,
    compatibility: DefaultCompatibilityHandler,
}

impl<S, J, P> SnapshotResolver<S, J, P> {
    pub fn new(storage: S, json: J, parquet: P) -> Self {
        Self {
            storage,
            json,
            parquet,
            compatibility: DefaultCompatibilityHandler,
        }
    }
}

impl<S, J, P> SnapshotResolver<S, J, P>
where
    S: StorageHandler,
    J: JsonHandler,
    P: ParquetHandler,
{
    pub async fn resolve_snapshot(
        &self,
        request: SnapshotResolutionRequest,
    ) -> Result<ResolvedSnapshotDescriptor, QueryError> {
        validate_snapshot_version(request.snapshot_version)?;
        let log_paths = self
            .storage
            .list_paths(&request.table_uri, DELTA_LOG_DIR)
            .await?;
        let log_entries = parse_log_entries(&log_paths);
        let latest_version = latest_table_version(&log_entries)?;
        let target_version = request.snapshot_version.unwrap_or(latest_version);
        if target_version > latest_version {
            return Err(invalid_request(format!(
                "requested snapshot version {} exceeds the latest available version {}",
                target_version, latest_version
            )));
        }

        let selected_checkpoint = self
            .select_checkpoint(&request.table_uri, target_version, &log_entries)
            .await?;
        let mut replay_state = SnapshotReplayState::default();
        let replay_start_version = if let Some(checkpoint) = &selected_checkpoint {
            self.apply_checkpoint(&request.table_uri, checkpoint, &mut replay_state)
                .await?;
            checkpoint.version + 1
        } else {
            0
        };

        if replay_start_version <= target_version {
            for version in replay_start_version..=target_version {
                let commit_path = format!("{DELTA_LOG_DIR}/{version:020}.json");
                let bytes = self
                    .storage
                    .read_bytes(&request.table_uri, &commit_path)
                    .await?
                    .ok_or_else(|| {
                        invalid_request(format!(
                            "delta log replay expected commit file '{}'",
                            commit_path
                        ))
                    })?;
                let actions = self.json.parse_log_actions(bytes.as_ref())?;
                apply_actions(&mut replay_state, actions)?;
            }
        }
        let schema_derivations = derive_schema_derivations(replay_state.current_metadata.as_ref())?;
        let browser_compatibility = self
            .compatibility
            .classify(&replay_state, &schema_derivations)?;

        Ok(ResolvedSnapshotDescriptor {
            table_uri: request.table_uri,
            snapshot_version: target_version,
            partition_column_types: schema_derivations.partition_column_types,
            browser_compatibility: browser_compatibility.clone(),
            required_capabilities: browser_compatibility,
            active_files: replay_state.active_files.into_values().collect(),
        })
    }

    async fn select_checkpoint(
        &self,
        table_uri: &str,
        target_version: i64,
        log_entries: &[LogEntry],
    ) -> Result<Option<SelectedCheckpoint>, QueryError> {
        let grouped = group_checkpoints(log_entries);

        if let Some(last_checkpoint) = self.read_last_checkpoint_hint(table_uri).await? {
            if last_checkpoint.version <= target_version {
                if let Some(checkpoint) = selected_checkpoint_from_hint(&grouped, &last_checkpoint)
                {
                    return Ok(Some(checkpoint));
                }
            }
        }

        Ok(grouped
            .into_values()
            .filter(|candidate| candidate.version <= target_version)
            .max_by_key(|candidate| candidate.version))
    }

    async fn read_last_checkpoint_hint(
        &self,
        table_uri: &str,
    ) -> Result<Option<LastCheckpointHint>, QueryError> {
        let Some(bytes) = self
            .storage
            .read_bytes(table_uri, LAST_CHECKPOINT_PATH)
            .await?
        else {
            return Ok(None);
        };
        self.json.parse_last_checkpoint_hint(bytes.as_ref())
    }

    async fn apply_checkpoint(
        &self,
        table_uri: &str,
        checkpoint: &SelectedCheckpoint,
        replay_state: &mut SnapshotReplayState,
    ) -> Result<(), QueryError> {
        let mut actions = Vec::new();
        for part_path in &checkpoint.part_paths {
            let bytes = self
                .storage
                .read_bytes(table_uri, part_path)
                .await?
                .ok_or_else(|| {
                    invalid_request(format!("missing checkpoint part '{}'", part_path))
                })?;
            actions.extend(self.parse_checkpoint_actions(part_path, bytes)?);
        }

        let sidecar_paths = actions
            .iter()
            .filter_map(|action| match action {
                SnapshotLogAction::Sidecar { path } => Some(path.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        apply_actions(replay_state, actions)?;

        for sidecar_path in sidecar_paths {
            let resolved_sidecar_path = resolve_sidecar_path(&sidecar_path);
            let bytes = self
                .storage
                .read_bytes(table_uri, &resolved_sidecar_path)
                .await?
                .ok_or_else(|| {
                    invalid_request(format!(
                        "checkpoint sidecar '{}' referenced by '{}' was missing",
                        resolved_sidecar_path, checkpoint.display_path
                    ))
                })?;
            let actions = self.parse_checkpoint_actions(&resolved_sidecar_path, bytes)?;
            apply_actions(replay_state, actions)?;
        }

        Ok(())
    }

    fn parse_checkpoint_actions(
        &self,
        relative_path: &str,
        bytes: Bytes,
    ) -> Result<Vec<SnapshotLogAction>, QueryError> {
        if relative_path.ends_with(".json") {
            return self.json.parse_log_actions(bytes.as_ref());
        }

        self.parquet.parse_checkpoint_actions(relative_path, bytes)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LastCheckpointHint {
    pub version: i64,
    pub parts: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotLogAction {
    Add {
        file: ResolvedFileDescriptor,
        uses_deletion_vector: bool,
    },
    Remove {
        path: String,
    },
    Sidecar {
        path: String,
    },
    Protocol {
        min_reader_version: i32,
        min_writer_version: i32,
        reader_features: BTreeSet<String>,
        writer_features: BTreeSet<String>,
    },
    Metadata {
        schema_string: String,
        partition_columns: Vec<String>,
        configuration: BTreeMap<String, String>,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct SnapshotReplayState {
    active_files: BTreeMap<String, ResolvedFileDescriptor>,
    active_deletion_vector_paths: BTreeSet<String>,
    current_protocol: Option<SnapshotProtocol>,
    current_metadata: Option<SnapshotMetadata>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SnapshotProtocol {
    min_reader_version: i32,
    min_writer_version: i32,
    reader_features: BTreeSet<String>,
    writer_features: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SnapshotMetadata {
    schema_string: String,
    partition_columns: Vec<String>,
    configuration: BTreeMap<String, String>,
}

#[derive(Default)]
struct SchemaDerivations {
    partition_column_types: BTreeMap<String, PartitionColumnType>,
    uses_column_mapping: bool,
    uses_generated_columns: bool,
    uses_identity_columns: bool,
    uses_invariants: bool,
    uses_timestamp_ntz: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum LogKind {
    Commit,
    SinglePartCheckpoint,
    UuidCheckpoint,
    MultiPartCheckpoint { part: u32, total_parts: u32 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LogEntry {
    version: i64,
    path: String,
    kind: LogKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SelectedCheckpoint {
    version: i64,
    display_path: String,
    part_paths: Vec<String>,
}

#[derive(Default)]
struct CheckpointAccumulator {
    single_path: Option<String>,
    uuid_path: Option<String>,
    multipart_paths: BTreeMap<u32, String>,
    multipart_total_parts: Option<u32>,
}

impl CheckpointAccumulator {
    fn into_selected(self, version: i64) -> Option<SelectedCheckpoint> {
        if let Some(path) = self.uuid_path.or(self.single_path) {
            return Some(SelectedCheckpoint {
                version,
                display_path: path.clone(),
                part_paths: vec![path],
            });
        }

        let total_parts = self.multipart_total_parts?;
        if total_parts == 0 {
            return None;
        }

        let mut part_paths = Vec::with_capacity(total_parts as usize);
        for part in 1..=total_parts {
            let path = self.multipart_paths.get(&part)?.clone();
            part_paths.push(path);
        }

        Some(SelectedCheckpoint {
            version,
            display_path: part_paths
                .first()
                .cloned()
                .unwrap_or_else(|| format!("{DELTA_LOG_DIR}/{version:020}.checkpoint.parquet")),
            part_paths,
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LastCheckpointHintJson {
    version: i64,
    #[serde(default)]
    parts: Option<u32>,
}

#[derive(Deserialize)]
struct JsonActionEnvelope {
    #[serde(default)]
    add: Option<JsonAddAction>,
    #[serde(default)]
    remove: Option<JsonRemoveAction>,
    #[serde(default)]
    sidecar: Option<JsonSidecarAction>,
    #[serde(default)]
    protocol: Option<JsonProtocolAction>,
    #[serde(default, rename = "metaData")]
    metadata: Option<JsonMetadataAction>,
}

#[derive(Deserialize)]
struct JsonAddAction {
    path: String,
    size: i64,
    #[serde(default, rename = "partitionValues")]
    partition_values: BTreeMap<String, Option<String>>,
    #[serde(default)]
    stats: Option<String>,
    #[serde(default, rename = "deletionVector")]
    deletion_vector: Option<JsonValue>,
}

#[derive(Deserialize)]
struct JsonRemoveAction {
    path: String,
}

#[derive(Deserialize)]
struct JsonSidecarAction {
    path: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonProtocolAction {
    min_reader_version: i32,
    min_writer_version: i32,
    #[serde(default)]
    reader_features: Vec<String>,
    #[serde(default)]
    writer_features: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonMetadataAction {
    schema_string: String,
    #[serde(default)]
    partition_columns: Vec<String>,
    #[serde(default)]
    configuration: BTreeMap<String, String>,
}

impl JsonHandler for DefaultJsonHandler {
    fn parse_last_checkpoint_hint(
        &self,
        bytes: &[u8],
    ) -> Result<Option<LastCheckpointHint>, QueryError> {
        let parsed = serde_json::from_slice::<LastCheckpointHintJson>(bytes);
        match parsed {
            Ok(parsed) => Ok(Some(LastCheckpointHint {
                version: parsed.version,
                parts: parsed.parts,
            })),
            Err(_) => Ok(None),
        }
    }

    fn parse_log_actions(&self, bytes: &[u8]) -> Result<Vec<SnapshotLogAction>, QueryError> {
        let content = std::str::from_utf8(bytes).map_err(|error| {
            execution_error(format!(
                "delta log commit bytes were not valid UTF-8: {error}"
            ))
        })?;
        let mut actions = Vec::new();
        for line in content.lines().filter(|line| !line.trim().is_empty()) {
            let envelope: JsonActionEnvelope = serde_json::from_str(line).map_err(|error| {
                execution_error(format!("delta log action JSON failed to parse: {error}"))
            })?;
            if let Some(action) = snapshot_action_from_json(envelope)? {
                actions.push(action);
            }
        }
        Ok(actions)
    }
}

impl ParquetHandler for DefaultParquetHandler {
    fn parse_checkpoint_actions(
        &self,
        relative_path: &str,
        bytes: Bytes,
    ) -> Result<Vec<SnapshotLogAction>, QueryError> {
        let reader = SerializedFileReader::new(bytes).map_err(|error| {
            execution_error(format!(
                "delta checkpoint '{}' could not be opened: {error}",
                relative_path
            ))
        })?;
        let row_iter = reader.get_row_iter(None).map_err(|error| {
            execution_error(format!(
                "delta checkpoint '{}' rows could not be iterated: {error}",
                relative_path
            ))
        })?;
        let mut actions = Vec::new();
        for row in row_iter {
            let row = row.map_err(|error| {
                execution_error(format!(
                    "delta checkpoint '{}' row decode failed: {error}",
                    relative_path
                ))
            })?;
            if let Some(action) = snapshot_action_from_checkpoint_row(&row, relative_path)? {
                actions.push(action);
            }
        }
        Ok(actions)
    }
}

impl CompatibilityHandler for DefaultCompatibilityHandler {
    fn classify(
        &self,
        replay_state: &SnapshotReplayState,
        schema_derivations: &SchemaDerivations,
    ) -> Result<CapabilityReport, QueryError> {
        let terminally_unsupported =
            terminally_unsupported_features(replay_state, schema_derivations);
        if !terminally_unsupported.is_empty() {
            return Err(unsupported_feature(format!(
                "browser snapshot reconstruction classifies Delta feature(s) as terminal unsupported for browser routing: {}",
                terminally_unsupported.join(", ")
            )));
        }

        let unknown_protocol_features =
            unknown_protocol_features(replay_state.current_protocol.as_ref());
        if !unknown_protocol_features.is_empty() {
            return Err(unsupported_feature(format!(
                "browser snapshot reconstruction does not support unknown Delta protocol feature(s): {}",
                unknown_protocol_features.join(", ")
            )));
        }

        Ok(required_capabilities_from_replay_state(
            replay_state,
            schema_derivations,
        ))
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait(?Send)]
impl StorageHandler for LocalFileStorageHandler {
    async fn list_paths(&self, table_uri: &str, prefix: &str) -> Result<Vec<String>, QueryError> {
        let root = root_path_from_table_uri(table_uri)?;
        let list_root = root.join(prefix);
        if !list_root.exists() {
            return Err(invalid_request(format!(
                "delta log directory '{}' did not exist for table '{}'",
                list_root.display(),
                table_uri
            )));
        }

        let mut paths = Vec::new();
        collect_relative_paths(&root, &list_root, &mut paths)?;
        paths.sort();
        Ok(paths)
    }

    async fn read_bytes(
        &self,
        table_uri: &str,
        relative_path: &str,
    ) -> Result<Option<Bytes>, QueryError> {
        let root = root_path_from_table_uri(table_uri)?;
        #[cfg(unix)]
        {
            return read_confined_bytes_unix(&root, relative_path);
        }

        #[cfg(not(unix))]
        {
            let path = table_relative_path(&root, relative_path)?;
            match fs::read(path) {
                Ok(bytes) => Ok(Some(Bytes::from(bytes))),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(error) => Err(execution_error(format!(
                    "failed to read snapshot file '{}': {error}",
                    relative_path
                ))),
            }
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn root_path_from_table_uri(table_uri: &str) -> Result<PathBuf, QueryError> {
    let trimmed = table_uri.trim();
    if let Some(path) = trimmed.strip_prefix("file://") {
        return Ok(PathBuf::from(path));
    }
    Ok(PathBuf::from(trimmed))
}

#[cfg(not(target_arch = "wasm32"))]
fn validated_relative_path(relative_path: &str) -> Result<PathBuf, QueryError> {
    if relative_path.is_empty()
        || is_absolute_uri(relative_path)
        || is_absolute_native_path(relative_path)
    {
        return Err(invalid_request(format!(
            "snapshot path '{}' must stay under the table root",
            relative_path
        )));
    }

    if Path::new(relative_path).components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return Err(invalid_request(format!(
            "snapshot path '{}' must stay under the table root",
            relative_path
        )));
    }

    Ok(PathBuf::from(relative_path))
}

#[cfg(all(not(target_arch = "wasm32"), not(unix)))]
fn table_relative_path(table_root: &Path, relative_path: &str) -> Result<PathBuf, QueryError> {
    let parsed = validated_relative_path(relative_path)?;
    let joined = table_root.join(parsed);
    if joined.exists() {
        let canonical_root = fs::canonicalize(table_root).map_err(|error| {
            execution_error(format!(
                "failed to canonicalize table root '{}': {error}",
                table_root.display()
            ))
        })?;
        let canonical_joined = fs::canonicalize(&joined).map_err(|error| {
            execution_error(format!(
                "failed to canonicalize snapshot file '{}': {error}",
                joined.display()
            ))
        })?;
        if !canonical_joined.starts_with(&canonical_root) {
            return Err(invalid_request(format!(
                "snapshot path '{}' must stay under the table root",
                relative_path
            )));
        }
    }

    Ok(joined)
}

#[cfg(all(not(target_arch = "wasm32"), unix))]
fn read_confined_bytes_unix(
    table_root: &Path,
    relative_path: &str,
) -> Result<Option<Bytes>, QueryError> {
    let parsed = validated_relative_path(relative_path)?;
    let mut components = parsed
        .components()
        .filter_map(|component| match component {
            Component::CurDir => None,
            Component::Normal(part) => Some(path_component_cstring(part, relative_path)),
            _ => Some(Err(invalid_request(format!(
                "snapshot path '{}' must stay under the table root",
                relative_path
            )))),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let Some(final_component) = components.pop() else {
        return Err(invalid_request(format!(
            "snapshot path '{}' must stay under the table root",
            relative_path
        )));
    };

    let mut directory = fs::File::open(table_root).map_err(|error| {
        execution_error(format!(
            "failed to open table root '{}': {error}",
            table_root.display()
        ))
    })?;
    for component in components {
        let Some(next_directory) = open_directory_nofollow_unix(
            directory.as_raw_fd(),
            component.as_c_str(),
            relative_path,
        )?
        else {
            return Ok(None);
        };
        directory = next_directory;
    }

    let Some(mut file) = open_file_nofollow_unix(
        directory.as_raw_fd(),
        final_component.as_c_str(),
        relative_path,
    )?
    else {
        return Ok(None);
    };

    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(|error| {
        execution_error(format!(
            "failed to read snapshot file '{}': {error}",
            relative_path
        ))
    })?;
    Ok(Some(Bytes::from(bytes)))
}

#[cfg(all(not(target_arch = "wasm32"), unix))]
fn path_component_cstring(
    component: &std::ffi::OsStr,
    relative_path: &str,
) -> Result<CString, QueryError> {
    CString::new(component.as_bytes()).map_err(|_| {
        invalid_request(format!(
            "snapshot path '{}' must stay under the table root",
            relative_path
        ))
    })
}

#[cfg(all(not(target_arch = "wasm32"), unix))]
fn open_directory_nofollow_unix(
    directory_fd: std::os::fd::RawFd,
    component: &std::ffi::CStr,
    relative_path: &str,
) -> Result<Option<fs::File>, QueryError> {
    open_nofollow_unix(
        directory_fd,
        component,
        libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW,
        relative_path,
    )
}

#[cfg(all(not(target_arch = "wasm32"), unix))]
fn open_file_nofollow_unix(
    directory_fd: std::os::fd::RawFd,
    component: &std::ffi::CStr,
    relative_path: &str,
) -> Result<Option<fs::File>, QueryError> {
    open_nofollow_unix(
        directory_fd,
        component,
        libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        relative_path,
    )
}

#[cfg(all(not(target_arch = "wasm32"), unix))]
fn open_nofollow_unix(
    directory_fd: std::os::fd::RawFd,
    component: &std::ffi::CStr,
    flags: libc::c_int,
    relative_path: &str,
) -> Result<Option<fs::File>, QueryError> {
    let opened_fd = unsafe { libc::openat(directory_fd, component.as_ptr(), flags) };
    if opened_fd >= 0 {
        let owned_fd = unsafe { std::os::fd::OwnedFd::from_raw_fd(opened_fd) };
        return Ok(Some(fs::File::from(owned_fd)));
    }

    let error = std::io::Error::last_os_error();
    match error.raw_os_error() {
        Some(code) if code == libc::ENOENT || code == libc::ENOTDIR => Ok(None),
        Some(code) if code == libc::ELOOP => Err(invalid_request(format!(
            "snapshot path '{}' must stay under the table root",
            relative_path
        ))),
        _ => Err(execution_error(format!(
            "failed to open snapshot file '{}': {error}",
            relative_path
        ))),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn collect_relative_paths(
    table_root: &Path,
    current: &Path,
    paths: &mut Vec<String>,
) -> Result<(), QueryError> {
    for entry in fs::read_dir(current).map_err(|error| {
        execution_error(format!(
            "failed to list delta log directory '{}': {error}",
            current.display()
        ))
    })? {
        let entry = entry.map_err(|error| {
            execution_error(format!(
                "failed to enumerate delta log directory '{}': {error}",
                current.display()
            ))
        })?;
        let path = entry.path();
        if path.is_dir() {
            collect_relative_paths(table_root, &path, paths)?;
        } else {
            let relative = path
                .strip_prefix(table_root)
                .map_err(|error| {
                    execution_error(format!(
                        "failed to derive a relative snapshot path for '{}': {error}",
                        path.display()
                    ))
                })?
                .to_string_lossy()
                .replace('\\', "/");
            paths.push(relative);
        }
    }
    Ok(())
}

fn validate_snapshot_version(snapshot_version: Option<i64>) -> Result<(), QueryError> {
    if snapshot_version.is_some_and(|version| version < 0) {
        return Err(invalid_request(
            "snapshot_version must be zero or greater".to_string(),
        ));
    }
    Ok(())
}

fn validate_delta_log_relative_path(relative_path: &str) -> Result<(), QueryError> {
    if relative_path.is_empty()
        || is_absolute_uri(relative_path)
        || is_absolute_native_path(relative_path)
        || !relative_path.starts_with("_delta_log/")
    {
        return Err(invalid_request(format!(
            "Delta log object path '{}' must stay under _delta_log/",
            relative_path
        )));
    }

    if Path::new(relative_path).components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return Err(invalid_request(format!(
            "Delta log object path '{}' must stay under _delta_log/",
            relative_path
        )));
    }

    Ok(())
}

fn latest_table_version(log_entries: &[LogEntry]) -> Result<i64, QueryError> {
    log_entries
        .iter()
        .map(|entry| entry.version)
        .max()
        .ok_or_else(|| {
            invalid_request("delta log did not contain any commits or checkpoints".to_string())
        })
}

fn parse_log_entries(paths: &[String]) -> Vec<LogEntry> {
    paths
        .iter()
        .filter_map(|path| parse_log_entry(path))
        .collect()
}

fn parse_log_entry(path: &str) -> Option<LogEntry> {
    let filename = path.strip_prefix(&format!("{DELTA_LOG_DIR}/"))?;
    if filename.contains('/') {
        return None;
    }

    if let Some(version) = parse_commit_version(filename) {
        return Some(LogEntry {
            version,
            path: path.to_string(),
            kind: LogKind::Commit,
        });
    }

    parse_checkpoint_kind(filename).map(|(version, kind)| LogEntry {
        version,
        path: path.to_string(),
        kind,
    })
}

fn parse_commit_version(filename: &str) -> Option<i64> {
    let digits = filename.strip_suffix(".json")?;
    parse_version_digits(digits)
}

fn parse_checkpoint_kind(filename: &str) -> Option<(i64, LogKind)> {
    let checkpoint_suffix = ".checkpoint.";
    let version_digits = filename.split(checkpoint_suffix).next()?;
    let version = parse_version_digits(version_digits)?;
    let remainder = filename.strip_prefix(&format!("{version_digits}{checkpoint_suffix}"))?;

    if remainder == "parquet" {
        return Some((version, LogKind::SinglePartCheckpoint));
    }

    if let Some(uuid) = remainder.strip_suffix(".parquet") {
        let parts = uuid.split('.').collect::<Vec<_>>();
        if parts.len() == 2 {
            let part = parse_ten_digit_number(parts[0])?;
            let total_parts = parse_ten_digit_number(parts[1])?;
            return Some((version, LogKind::MultiPartCheckpoint { part, total_parts }));
        }

        if parts.len() == 1 && is_uuid_like(parts[0]) {
            return Some((version, LogKind::UuidCheckpoint));
        }
    }

    if let Some(uuid) = remainder.strip_suffix(".json") {
        if is_uuid_like(uuid) {
            return Some((version, LogKind::UuidCheckpoint));
        }
    }

    None
}

fn parse_version_digits(value: &str) -> Option<i64> {
    if value.len() != 20 || !value.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

fn parse_ten_digit_number(value: &str) -> Option<u32> {
    if value.len() != 10 || !value.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

fn is_uuid_like(value: &str) -> bool {
    let mut groups = value.split('-');
    let expected_lengths = [8, 4, 4, 4, 12];

    for expected_length in expected_lengths {
        let Some(group) = groups.next() else {
            return false;
        };
        if group.len() != expected_length || !group.chars().all(|ch| ch.is_ascii_hexdigit()) {
            return false;
        }
    }

    groups.next().is_none()
}

fn group_checkpoints(entries: &[LogEntry]) -> BTreeMap<i64, SelectedCheckpoint> {
    let mut grouped = BTreeMap::new();
    for entry in entries {
        let accumulator = grouped
            .entry(entry.version)
            .or_insert_with(CheckpointAccumulator::default);
        match &entry.kind {
            LogKind::SinglePartCheckpoint => accumulator.single_path = Some(entry.path.clone()),
            LogKind::UuidCheckpoint => accumulator.uuid_path = Some(entry.path.clone()),
            LogKind::MultiPartCheckpoint { part, total_parts } => {
                accumulator
                    .multipart_paths
                    .insert(*part, entry.path.clone());
                accumulator.multipart_total_parts = Some(*total_parts);
            }
            LogKind::Commit => {}
        }
    }

    grouped
        .into_iter()
        .filter_map(|(version, candidate)| {
            candidate
                .into_selected(version)
                .map(|candidate| (version, candidate))
        })
        .collect()
}

fn selected_checkpoint_from_hint(
    grouped: &BTreeMap<i64, SelectedCheckpoint>,
    hint: &LastCheckpointHint,
) -> Option<SelectedCheckpoint> {
    let checkpoint = grouped.get(&hint.version)?.clone();
    if let Some(parts) = hint.parts {
        if checkpoint.part_paths.len() != parts as usize {
            return None;
        }
    }
    Some(checkpoint)
}

fn apply_actions(
    replay_state: &mut SnapshotReplayState,
    actions: Vec<SnapshotLogAction>,
) -> Result<(), QueryError> {
    for action in actions {
        match action {
            SnapshotLogAction::Add {
                file,
                uses_deletion_vector,
            } => {
                let path = file.path.clone();
                replay_state.active_files.insert(path.clone(), file);
                if uses_deletion_vector {
                    replay_state.active_deletion_vector_paths.insert(path);
                } else {
                    replay_state.active_deletion_vector_paths.remove(&path);
                }
            }
            SnapshotLogAction::Remove { path } => {
                replay_state.active_files.remove(&path);
                replay_state.active_deletion_vector_paths.remove(&path);
            }
            SnapshotLogAction::Sidecar { .. } => {}
            SnapshotLogAction::Protocol {
                min_reader_version,
                min_writer_version,
                reader_features,
                writer_features,
            } => {
                replay_state.current_protocol = Some(SnapshotProtocol {
                    min_reader_version,
                    min_writer_version,
                    reader_features,
                    writer_features,
                });
            }
            SnapshotLogAction::Metadata {
                schema_string,
                partition_columns,
                configuration,
            } => {
                replay_state.current_metadata = Some(SnapshotMetadata {
                    schema_string,
                    partition_columns,
                    configuration,
                });
            }
        }
    }
    Ok(())
}

fn snapshot_action_from_json(
    envelope: JsonActionEnvelope,
) -> Result<Option<SnapshotLogAction>, QueryError> {
    if let Some(add) = envelope.add {
        return Ok(Some(SnapshotLogAction::Add {
            file: ResolvedFileDescriptor {
                path: add.path,
                size_bytes: u64::try_from(add.size).map_err(|_| {
                    execution_error("delta add action size must be non-negative".to_string())
                })?,
                partition_values: add.partition_values,
                stats: add.stats,
            },
            uses_deletion_vector: add.deletion_vector.is_some(),
        }));
    }

    if let Some(remove) = envelope.remove {
        return Ok(Some(SnapshotLogAction::Remove { path: remove.path }));
    }

    if let Some(sidecar) = envelope.sidecar {
        return Ok(Some(SnapshotLogAction::Sidecar { path: sidecar.path }));
    }

    if let Some(protocol) = envelope.protocol {
        return Ok(Some(SnapshotLogAction::Protocol {
            min_reader_version: protocol.min_reader_version,
            min_writer_version: protocol.min_writer_version,
            reader_features: protocol.reader_features.into_iter().collect(),
            writer_features: protocol.writer_features.into_iter().collect(),
        }));
    }

    if let Some(metadata) = envelope.metadata {
        return Ok(Some(SnapshotLogAction::Metadata {
            schema_string: metadata.schema_string,
            partition_columns: metadata.partition_columns,
            configuration: metadata.configuration,
        }));
    }

    Ok(None)
}

fn snapshot_action_from_checkpoint_row(
    row: &parquet::record::Row,
    relative_path: &str,
) -> Result<Option<SnapshotLogAction>, QueryError> {
    let fields = row
        .get_column_iter()
        .map(|(name, field)| (name.as_str(), field))
        .collect::<BTreeMap<_, _>>();

    if let Some(field) = fields.get("add") {
        if let Some(action) = checkpoint_add_action(field, relative_path)? {
            return Ok(Some(action));
        }
    }

    if let Some(field) = fields.get("remove") {
        if let Some(action) = checkpoint_remove_action(field)? {
            return Ok(Some(action));
        }
    }

    if let Some(field) = fields.get("sidecar") {
        if let Some(action) = checkpoint_sidecar_action(field)? {
            return Ok(Some(action));
        }
    }

    if let Some(field) = fields.get("protocol") {
        if let Some(action) = checkpoint_protocol_action(field)? {
            return Ok(Some(action));
        }
    }

    if let Some(field) = fields.get("metaData") {
        if let Some(action) = checkpoint_metadata_action(field)? {
            return Ok(Some(action));
        }
    }

    Ok(None)
}

fn checkpoint_add_action(
    field: &ParquetField,
    relative_path: &str,
) -> Result<Option<SnapshotLogAction>, QueryError> {
    let Some(group) = as_group(field) else {
        return Ok(None);
    };
    let path = group_string(group, "path")?;
    if path.is_none() {
        return Ok(None);
    }
    let size = group_i64(group, "size")?.ok_or_else(|| {
        execution_error(format!(
            "checkpoint add action in '{}' was missing a size",
            relative_path
        ))
    })?;
    let partition_values = group_string_map(group, "partitionValues")?;
    let stats = group_string(group, "stats")?;

    Ok(Some(SnapshotLogAction::Add {
        file: ResolvedFileDescriptor {
            path: path.expect("path should exist when action is present"),
            size_bytes: u64::try_from(size).map_err(|_| {
                execution_error(format!(
                    "checkpoint add action in '{}' reported a negative size",
                    relative_path
                ))
            })?,
            partition_values,
            stats,
        },
        uses_deletion_vector: group_has_non_null_field(group, "deletionVector")?,
    }))
}

fn checkpoint_remove_action(field: &ParquetField) -> Result<Option<SnapshotLogAction>, QueryError> {
    let Some(group) = as_group(field) else {
        return Ok(None);
    };
    let Some(path) = group_string(group, "path")? else {
        return Ok(None);
    };
    Ok(Some(SnapshotLogAction::Remove { path }))
}

fn checkpoint_sidecar_action(
    field: &ParquetField,
) -> Result<Option<SnapshotLogAction>, QueryError> {
    let Some(group) = as_group(field) else {
        return Ok(None);
    };
    let Some(path) = group_string(group, "path")? else {
        return Ok(None);
    };
    Ok(Some(SnapshotLogAction::Sidecar { path }))
}

fn checkpoint_protocol_action(
    field: &ParquetField,
) -> Result<Option<SnapshotLogAction>, QueryError> {
    let Some(group) = as_group(field) else {
        return Ok(None);
    };
    let Some(min_reader_version) = group_i64(group, "minReaderVersion")? else {
        return Ok(None);
    };
    let min_writer_version = group_i64(group, "minWriterVersion")?.ok_or_else(|| {
        execution_error("checkpoint protocol action was missing minWriterVersion".to_string())
    })?;

    Ok(Some(SnapshotLogAction::Protocol {
        min_reader_version: i32::try_from(min_reader_version).map_err(|_| {
            execution_error("checkpoint protocol minReaderVersion overflowed i32".to_string())
        })?,
        min_writer_version: i32::try_from(min_writer_version).map_err(|_| {
            execution_error("checkpoint protocol minWriterVersion overflowed i32".to_string())
        })?,
        reader_features: group_string_list(group, "readerFeatures")?
            .into_iter()
            .collect(),
        writer_features: group_string_list(group, "writerFeatures")?
            .into_iter()
            .collect(),
    }))
}

fn checkpoint_metadata_action(
    field: &ParquetField,
) -> Result<Option<SnapshotLogAction>, QueryError> {
    let Some(group) = as_group(field) else {
        return Ok(None);
    };
    let Some(schema_string) = group_string(group, "schemaString")? else {
        return Ok(None);
    };

    Ok(Some(SnapshotLogAction::Metadata {
        schema_string,
        partition_columns: group_string_list(group, "partitionColumns")?,
        configuration: group_string_map(group, "configuration")?
            .into_iter()
            .filter_map(|(key, value)| value.map(|value| (key, value)))
            .collect(),
    }))
}

fn as_group(field: &ParquetField) -> Option<&parquet::record::Row> {
    match field {
        ParquetField::Group(group) => Some(group),
        ParquetField::Null => None,
        _ => None,
    }
}

fn group_field<'a>(
    group: &'a parquet::record::Row,
    name: &str,
) -> Result<Option<&'a ParquetField>, QueryError> {
    let mut matches = group
        .get_column_iter()
        .filter(|(field_name, _)| field_name.as_str() == name);
    let field = matches.next().map(|(_, field)| field);
    if matches.next().is_some() {
        return Err(execution_error(format!(
            "delta checkpoint struct contained duplicate '{}' fields",
            name
        )));
    }
    Ok(field)
}

fn group_string(group: &parquet::record::Row, name: &str) -> Result<Option<String>, QueryError> {
    match group_field(group, name)? {
        Some(ParquetField::Str(value)) => Ok(Some(value.clone())),
        Some(ParquetField::Null) | None => Ok(None),
        Some(other) => Err(execution_error(format!(
            "delta checkpoint field '{}' had unexpected type '{:?}'",
            name, other
        ))),
    }
}

fn group_i64(group: &parquet::record::Row, name: &str) -> Result<Option<i64>, QueryError> {
    match group_field(group, name)? {
        Some(ParquetField::Long(value)) => Ok(Some(*value)),
        Some(ParquetField::Int(value)) => Ok(Some(i64::from(*value))),
        Some(ParquetField::Null) | None => Ok(None),
        Some(other) => Err(execution_error(format!(
            "delta checkpoint field '{}' had unexpected type '{:?}'",
            name, other
        ))),
    }
}

fn group_string_list(group: &parquet::record::Row, name: &str) -> Result<Vec<String>, QueryError> {
    match group_field(group, name)? {
        Some(ParquetField::ListInternal(list)) => list
            .elements()
            .iter()
            .map(|element| match element {
                ParquetField::Str(value) => Ok(value.clone()),
                ParquetField::Null => Err(execution_error(format!(
                    "delta checkpoint list field '{}' contained a null element",
                    name
                ))),
                other => Err(execution_error(format!(
                    "delta checkpoint list field '{}' had a non-string element '{:?}'",
                    name, other
                ))),
            })
            .collect(),
        Some(ParquetField::Null) | None => Ok(Vec::new()),
        Some(other) => Err(execution_error(format!(
            "delta checkpoint field '{}' had unexpected type '{:?}'",
            name, other
        ))),
    }
}

fn group_has_non_null_field(group: &parquet::record::Row, name: &str) -> Result<bool, QueryError> {
    Ok(match group_field(group, name)? {
        Some(ParquetField::Null) | None => false,
        Some(_) => true,
    })
}

fn group_string_map(
    group: &parquet::record::Row,
    name: &str,
) -> Result<BTreeMap<String, Option<String>>, QueryError> {
    match group_field(group, name)? {
        Some(ParquetField::MapInternal(map)) => {
            let mut values = BTreeMap::new();
            for (key, value) in map.entries() {
                let ParquetField::Str(key) = key else {
                    return Err(execution_error(format!(
                        "delta checkpoint map field '{}' had a non-string key",
                        name
                    )));
                };
                let value = match value {
                    ParquetField::Str(value) => Some(value.clone()),
                    ParquetField::Null => None,
                    other => {
                        return Err(execution_error(format!(
                            "delta checkpoint map field '{}' had a non-string value '{:?}'",
                            name, other
                        )))
                    }
                };
                values.insert(key.clone(), value);
            }
            Ok(values)
        }
        Some(ParquetField::Null) | None => Ok(BTreeMap::new()),
        Some(other) => Err(execution_error(format!(
            "delta checkpoint field '{}' had unexpected type '{:?}'",
            name, other
        ))),
    }
}

fn derive_schema_derivations(
    metadata: Option<&SnapshotMetadata>,
) -> Result<SchemaDerivations, QueryError> {
    let Some(metadata) = metadata else {
        return Ok(SchemaDerivations::default());
    };
    let schema = parse_schema_json(&metadata.schema_string)?;
    let fields = schema_fields(schema.as_ref(), "Delta schema root")?;
    let mut partition_column_types = BTreeMap::new();
    for partition_column in &metadata.partition_columns {
        let field = schema_field_by_name(fields, partition_column).ok_or_else(|| {
            execution_error(format!(
                "partition column '{}' was absent from the Delta schema during browser snapshot reconstruction",
                partition_column
            ))
        })?;
        partition_column_types.insert(
            partition_column.clone(),
            partition_column_type_from_schema_field(field)?,
        );
    }

    Ok(SchemaDerivations {
        partition_column_types,
        uses_column_mapping: schema_fields_use_column_mapping(fields)?,
        uses_generated_columns: schema_fields_use_generated_columns(fields)?,
        uses_identity_columns: schema_fields_use_identity_columns(fields)?,
        uses_invariants: schema_fields_use_invariants(fields)?,
        uses_timestamp_ntz: schema_fields_use_timestamp_ntz(fields)?,
    })
}

fn required_capabilities_from_replay_state(
    replay_state: &SnapshotReplayState,
    schema_derivations: &SchemaDerivations,
) -> CapabilityReport {
    let mut report = CapabilityReport::default();

    if metadata_flag_enabled(
        replay_state.current_metadata.as_ref(),
        "delta.enableChangeDataFeed",
    ) && protocol_supports_change_data_feed(replay_state.current_protocol.as_ref())
    {
        report.insert(CapabilityKey::ChangeDataFeed, CapabilityState::NativeOnly);
    }

    if schema_derivations.uses_column_mapping {
        report.insert(CapabilityKey::ColumnMapping, CapabilityState::NativeOnly);
    }

    if !replay_state.active_deletion_vector_paths.is_empty()
        || (metadata_flag_enabled(
            replay_state.current_metadata.as_ref(),
            "delta.enableDeletionVectors",
        ) && protocol_supports_deletion_vectors(replay_state.current_protocol.as_ref()))
    {
        report.insert(CapabilityKey::DeletionVectors, CapabilityState::NativeOnly);
    }

    if schema_derivations.uses_timestamp_ntz {
        report.insert(CapabilityKey::TimestampNtz, CapabilityState::NativeOnly);
    }

    report
}

fn metadata_flag_enabled(metadata: Option<&SnapshotMetadata>, key: &str) -> bool {
    metadata
        .and_then(|metadata| metadata.configuration.get(key))
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

fn protocol_supports_change_data_feed(protocol: Option<&SnapshotProtocol>) -> bool {
    let Some(protocol) = protocol else {
        return false;
    };

    if protocol.min_writer_version == 7 {
        return protocol.writer_features.contains("changeDataFeed");
    }

    protocol.min_writer_version >= 4
}

fn protocol_supports_deletion_vectors(protocol: Option<&SnapshotProtocol>) -> bool {
    let Some(protocol) = protocol else {
        return false;
    };

    protocol.reader_features.contains("deletionVectors")
        || protocol.writer_features.contains("deletionVectors")
}

fn terminally_unsupported_features(
    replay_state: &SnapshotReplayState,
    schema_derivations: &SchemaDerivations,
) -> Vec<String> {
    enabled_delta_features(
        replay_state.current_protocol.as_ref(),
        replay_state.current_metadata.as_ref(),
        schema_derivations,
    )
    .into_iter()
    .filter(|feature| feature.class == KnownFeatureClass::TerminalUnsupported)
    .map(|feature| feature.name.to_string())
    .collect::<BTreeSet<_>>()
    .into_iter()
    .collect()
}

fn unknown_protocol_features(protocol: Option<&SnapshotProtocol>) -> Vec<String> {
    protocol_feature_set(protocol)
        .into_iter()
        .filter(|feature| !protocol_feature_is_known(feature))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn protocol_feature_set(protocol: Option<&SnapshotProtocol>) -> Vec<String> {
    let Some(protocol) = protocol else {
        return Vec::new();
    };

    protocol
        .reader_features
        .iter()
        .chain(protocol.writer_features.iter())
        .cloned()
        .collect()
}

fn enabled_delta_features(
    protocol: Option<&SnapshotProtocol>,
    metadata: Option<&SnapshotMetadata>,
    schema_derivations: &SchemaDerivations,
) -> Vec<&'static KnownDeltaFeature> {
    KNOWN_DELTA_FEATURES
        .iter()
        .filter(|feature| delta_feature_is_enabled(protocol, metadata, schema_derivations, feature))
        .collect()
}

fn delta_feature_is_enabled(
    protocol: Option<&SnapshotProtocol>,
    metadata: Option<&SnapshotMetadata>,
    schema_derivations: &SchemaDerivations,
    feature: &KnownDeltaFeature,
) -> bool {
    if !delta_feature_is_supported_in_protocol(protocol, feature) {
        return false;
    }

    match feature.enablement {
        KnownFeatureEnablement::AlwaysIfSupported => true,
        KnownFeatureEnablement::BoolFlag(key) => metadata_flag_enabled(metadata, key),
        KnownFeatureEnablement::ColumnMappingMode => metadata
            .and_then(|metadata| metadata.configuration.get("delta.columnMapping.mode"))
            .is_some_and(|value| !value.eq_ignore_ascii_case("none")),
        KnownFeatureEnablement::ConfigurationPrefix(prefix) => metadata.is_some_and(|metadata| {
            metadata
                .configuration
                .keys()
                .any(|key| key.starts_with(prefix))
        }),
        KnownFeatureEnablement::RowTracking => {
            metadata_flag_enabled(metadata, "delta.enableRowTracking")
                && !metadata_flag_enabled(metadata, "delta.rowTrackingSuspended")
        }
        KnownFeatureEnablement::SchemaGeneratedColumns => schema_derivations.uses_generated_columns,
        KnownFeatureEnablement::SchemaIdentityColumns => schema_derivations.uses_identity_columns,
        KnownFeatureEnablement::SchemaInvariants => schema_derivations.uses_invariants,
    }
}

fn delta_feature_is_supported_in_protocol(
    protocol: Option<&SnapshotProtocol>,
    feature: &KnownDeltaFeature,
) -> bool {
    let Some(protocol) = protocol else {
        return false;
    };

    match feature.kind {
        KnownFeatureKind::Writer => {
            if protocol.min_writer_version < 7 {
                protocol.min_writer_version >= feature.min_writer_version
            } else {
                protocol.writer_features.contains(feature.name)
            }
        }
        KnownFeatureKind::ReaderWriter => {
            let reader_supported = if protocol.min_reader_version < 3 {
                protocol.min_reader_version >= feature.min_reader_version
            } else {
                protocol.reader_features.contains(feature.name)
            };
            let writer_supported = if protocol.min_writer_version < 7 {
                protocol.min_writer_version >= feature.min_writer_version
            } else {
                protocol.writer_features.contains(feature.name)
            };

            reader_supported && writer_supported
        }
    }
}

fn protocol_feature_is_known(feature_name: &str) -> bool {
    delta_protocol_feature(feature_name).is_some()
}

fn parse_schema_json(schema_string: &str) -> Result<Box<JsonValue>, QueryError> {
    serde_json::from_str(schema_string).map_err(|error| {
        execution_error(format!(
            "Delta schemaString could not be parsed during browser snapshot reconstruction: {error}"
        ))
    })
}

fn schema_fields<'a>(schema: &'a JsonValue, context: &str) -> Result<&'a [JsonValue], QueryError> {
    let type_name = schema
        .get("type")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| execution_error(format!("{context} was missing a string 'type' field")))?;
    if type_name != "struct" {
        return Err(execution_error(format!(
            "{context} expected 'type' == 'struct', found '{type_name}'"
        )));
    }

    schema
        .get("fields")
        .and_then(JsonValue::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| execution_error(format!("{context} was missing a 'fields' array")))
}

fn schema_field_by_name<'a>(fields: &'a [JsonValue], name: &str) -> Option<&'a JsonValue> {
    fields.iter().find(|field| {
        field
            .get("name")
            .and_then(JsonValue::as_str)
            .is_some_and(|field_name| field_name == name)
    })
}

fn partition_column_type_from_schema_field(
    field: &JsonValue,
) -> Result<PartitionColumnType, QueryError> {
    let data_type = field.get("type").ok_or_else(|| {
        execution_error("Delta schema field was missing a 'type' value".to_string())
    })?;
    Ok(partition_column_type_from_schema_type(data_type))
}

fn partition_column_type_from_schema_type(data_type: &JsonValue) -> PartitionColumnType {
    match data_type {
        JsonValue::String(value) => match value.as_str() {
            "string" => PartitionColumnType::String,
            "boolean" => PartitionColumnType::Boolean,
            "byte" | "short" | "integer" | "long" => PartitionColumnType::Int64,
            _ => PartitionColumnType::Unsupported,
        },
        JsonValue::Object(_) => PartitionColumnType::Unsupported,
        _ => PartitionColumnType::Unsupported,
    }
}

fn schema_fields_use_column_mapping(fields: &[JsonValue]) -> Result<bool, QueryError> {
    schema_fields_have_matching_metadata(fields, &|key| key.starts_with("delta.columnMapping."))
}

fn schema_fields_use_generated_columns(fields: &[JsonValue]) -> Result<bool, QueryError> {
    schema_fields_have_matching_metadata(fields, &|key| key == "delta.generationExpression")
}

fn schema_fields_use_identity_columns(fields: &[JsonValue]) -> Result<bool, QueryError> {
    schema_fields_have_matching_metadata(fields, &|key| key.starts_with("delta.identity."))
}

fn schema_fields_use_invariants(fields: &[JsonValue]) -> Result<bool, QueryError> {
    schema_fields_have_matching_metadata(fields, &|key| key == "delta.invariants")
}

fn schema_fields_have_matching_metadata(
    fields: &[JsonValue],
    predicate: &dyn Fn(&str) -> bool,
) -> Result<bool, QueryError> {
    for field in fields {
        if schema_field_has_matching_metadata(field, predicate)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn schema_field_has_matching_metadata(
    field: &JsonValue,
    predicate: &dyn Fn(&str) -> bool,
) -> Result<bool, QueryError> {
    if let Some(metadata) = field.get("metadata").and_then(JsonValue::as_object) {
        if metadata.keys().any(|key| predicate(key)) {
            return Ok(true);
        }
    }

    let data_type = field.get("type").ok_or_else(|| {
        execution_error("Delta schema field was missing a 'type' value".to_string())
    })?;
    schema_type_has_matching_metadata(data_type, predicate)
}

fn schema_type_has_matching_metadata(
    data_type: &JsonValue,
    predicate: &dyn Fn(&str) -> bool,
) -> Result<bool, QueryError> {
    match data_type {
        JsonValue::String(_) => Ok(false),
        JsonValue::Object(object) => match object.get("type").and_then(JsonValue::as_str) {
            Some("array") => schema_type_has_matching_metadata(
                object.get("elementType").ok_or_else(|| {
                    execution_error("Delta array type was missing elementType".to_string())
                })?,
                predicate,
            ),
            Some("map") => Ok(schema_type_has_matching_metadata(
                object.get("keyType").ok_or_else(|| {
                    execution_error("Delta map type was missing keyType".to_string())
                })?,
                predicate,
            )? || schema_type_has_matching_metadata(
                object.get("valueType").ok_or_else(|| {
                    execution_error("Delta map type was missing valueType".to_string())
                })?,
                predicate,
            )?),
            Some("struct") | Some("variant") => {
                let fields = object
                    .get("fields")
                    .and_then(JsonValue::as_array)
                    .map(Vec::as_slice)
                    .ok_or_else(|| {
                        execution_error("Delta nested struct type was missing fields".to_string())
                    })?;
                schema_fields_have_matching_metadata(fields, predicate)
            }
            Some(_) => Ok(false),
            None => Err(execution_error(
                "Delta complex type was missing a string 'type' field".to_string(),
            )),
        },
        _ => Err(execution_error(
            "Delta schema type was neither a string nor an object".to_string(),
        )),
    }
}

fn schema_fields_use_timestamp_ntz(fields: &[JsonValue]) -> Result<bool, QueryError> {
    for field in fields {
        if schema_field_uses_timestamp_ntz(field)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn schema_field_uses_timestamp_ntz(field: &JsonValue) -> Result<bool, QueryError> {
    let data_type = field.get("type").ok_or_else(|| {
        execution_error("Delta schema field was missing a 'type' value".to_string())
    })?;
    schema_type_uses_timestamp_ntz(data_type)
}

fn schema_type_uses_timestamp_ntz(data_type: &JsonValue) -> Result<bool, QueryError> {
    match data_type {
        JsonValue::String(value) => Ok(value == "timestamp_ntz"),
        JsonValue::Object(object) => match object.get("type").and_then(JsonValue::as_str) {
            Some("array") => {
                schema_type_uses_timestamp_ntz(object.get("elementType").ok_or_else(|| {
                    execution_error("Delta array type was missing elementType".to_string())
                })?)
            }
            Some("map") => Ok(
                schema_type_uses_timestamp_ntz(object.get("keyType").ok_or_else(|| {
                    execution_error("Delta map type was missing keyType".to_string())
                })?)?
                    || schema_type_uses_timestamp_ntz(object.get("valueType").ok_or_else(
                        || execution_error("Delta map type was missing valueType".to_string()),
                    )?)?,
            ),
            Some("struct") | Some("variant") => {
                let fields = object
                    .get("fields")
                    .and_then(JsonValue::as_array)
                    .map(Vec::as_slice)
                    .ok_or_else(|| {
                        execution_error("Delta nested struct type was missing fields".to_string())
                    })?;
                schema_fields_use_timestamp_ntz(fields)
            }
            Some(_) => Ok(false),
            None => Err(execution_error(
                "Delta complex type was missing a string 'type' field".to_string(),
            )),
        },
        _ => Err(execution_error(
            "Delta schema type was neither a string nor an object".to_string(),
        )),
    }
}

fn resolve_sidecar_path(path: &str) -> String {
    if path.starts_with("_delta_log/") || is_absolute_uri(path) || is_absolute_native_path(path) {
        path.to_string()
    } else {
        format!("{DELTA_LOG_DIR}/_sidecars/{path}")
    }
}

fn is_absolute_native_path(path: &str) -> bool {
    Path::new(path).is_absolute() || path.starts_with('\\')
}

fn is_absolute_uri(path: &str) -> bool {
    let Some((scheme, _)) = path.split_once(':') else {
        return false;
    };

    !scheme.is_empty()
        && scheme.chars().enumerate().all(|(index, ch)| match ch {
            'a'..='z' | 'A'..='Z' => true,
            '0'..='9' | '+' | '-' | '.' => index > 0,
            _ => false,
        })
}

fn invalid_request(message: String) -> QueryError {
    QueryError::new(QueryErrorCode::InvalidRequest, message, runtime_target())
}

fn execution_error(message: String) -> QueryError {
    QueryError::new(QueryErrorCode::ExecutionFailed, message, runtime_target())
}

fn unsupported_feature(message: String) -> QueryError {
    QueryError::new(
        QueryErrorCode::UnsupportedFeature,
        message,
        runtime_target(),
    )
}
