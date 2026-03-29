//! Browser-safe Delta snapshot reconstruction.

use std::collections::BTreeMap;
#[cfg(not(target_arch = "wasm32"))]
use std::fs;
#[cfg(not(target_arch = "wasm32"))]
use std::path::{Component, Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use parquet::file::reader::{FileReader as ParquetFileReader, SerializedFileReader};
use parquet::record::Field as ParquetField;
use query_contract::{
    ExecutionTarget, QueryError, QueryErrorCode, ResolvedFileDescriptor,
    ResolvedSnapshotDescriptor, SnapshotResolutionRequest,
};
use serde::Deserialize;

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

#[derive(Clone, Debug, Default)]
pub struct DefaultJsonHandler;

#[derive(Clone, Debug, Default)]
pub struct DefaultParquetHandler;

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug, Default)]
pub struct LocalFileStorageHandler;

#[derive(Clone, Debug)]
pub struct SnapshotResolver<S, J, P> {
    storage: S,
    json: J,
    parquet: P,
}

impl<S, J, P> SnapshotResolver<S, J, P> {
    pub fn new(storage: S, json: J, parquet: P) -> Self {
        Self {
            storage,
            json,
            parquet,
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
        let mut active_files = BTreeMap::new();
        let replay_start_version = if let Some(checkpoint) = &selected_checkpoint {
            self.apply_checkpoint(&request.table_uri, checkpoint, &mut active_files)
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
                apply_actions(&mut active_files, actions)?;
            }
        }

        Ok(ResolvedSnapshotDescriptor {
            table_uri: request.table_uri,
            snapshot_version: target_version,
            partition_column_types: BTreeMap::new(),
            active_files: active_files.into_values().collect(),
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
        active_files: &mut BTreeMap<String, ResolvedFileDescriptor>,
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
        apply_actions(active_files, actions)?;

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
            apply_actions(active_files, actions)?;
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
    Add(ResolvedFileDescriptor),
    Remove { path: String },
    Sidecar { path: String },
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
}

#[derive(Deserialize)]
struct JsonAddAction {
    path: String,
    size: i64,
    #[serde(default, rename = "partitionValues")]
    partition_values: BTreeMap<String, Option<String>>,
}

#[derive(Deserialize)]
struct JsonRemoveAction {
    path: String,
}

#[derive(Deserialize)]
struct JsonSidecarAction {
    path: String,
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

#[cfg(not(target_arch = "wasm32"))]
fn root_path_from_table_uri(table_uri: &str) -> Result<PathBuf, QueryError> {
    let trimmed = table_uri.trim();
    if let Some(path) = trimmed.strip_prefix("file://") {
        return Ok(PathBuf::from(path));
    }
    Ok(PathBuf::from(trimmed))
}

#[cfg(not(target_arch = "wasm32"))]
fn table_relative_path(table_root: &Path, relative_path: &str) -> Result<PathBuf, QueryError> {
    if relative_path.is_empty()
        || is_absolute_uri(relative_path)
        || Path::new(relative_path).is_absolute()
    {
        return Err(invalid_request(format!(
            "snapshot path '{}' must stay under the table root",
            relative_path
        )));
    }

    let parsed = Path::new(relative_path);
    if parsed.components().any(|component| {
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
    active_files: &mut BTreeMap<String, ResolvedFileDescriptor>,
    actions: Vec<SnapshotLogAction>,
) -> Result<(), QueryError> {
    for action in actions {
        match action {
            SnapshotLogAction::Add(file) => {
                active_files.insert(file.path.clone(), file);
            }
            SnapshotLogAction::Remove { path } => {
                active_files.remove(&path);
            }
            SnapshotLogAction::Sidecar { .. } => {}
        }
    }
    Ok(())
}

fn snapshot_action_from_json(
    envelope: JsonActionEnvelope,
) -> Result<Option<SnapshotLogAction>, QueryError> {
    if let Some(add) = envelope.add {
        return Ok(Some(SnapshotLogAction::Add(ResolvedFileDescriptor {
            path: add.path,
            size_bytes: u64::try_from(add.size).map_err(|_| {
                execution_error("delta add action size must be non-negative".to_string())
            })?,
            partition_values: add.partition_values,
        })));
    }

    if let Some(remove) = envelope.remove {
        return Ok(Some(SnapshotLogAction::Remove { path: remove.path }));
    }

    if let Some(sidecar) = envelope.sidecar {
        return Ok(Some(SnapshotLogAction::Sidecar { path: sidecar.path }));
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

    Ok(Some(SnapshotLogAction::Add(ResolvedFileDescriptor {
        path: path.expect("path should exist when action is present"),
        size_bytes: u64::try_from(size).map_err(|_| {
            execution_error(format!(
                "checkpoint add action in '{}' reported a negative size",
                relative_path
            ))
        })?,
        partition_values,
    })))
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

fn resolve_sidecar_path(path: &str) -> String {
    if path.starts_with("_delta_log/") || is_absolute_uri(path) {
        path.to_string()
    } else {
        format!("{DELTA_LOG_DIR}/_sidecars/{path}")
    }
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
