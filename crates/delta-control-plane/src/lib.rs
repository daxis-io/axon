//! Trusted control-plane skeleton for snapshot resolution and browser-safe access descriptors.

use std::collections::BTreeMap;
use std::future::Future;

use deltalake::kernel::scalars::ScalarExt;
use deltalake::table::state::DeltaTableState;
use deltalake::{open_table, open_table_with_version, DeltaTableError, ObjectStoreError};
use query_contract::{
    ExecutionTarget, QueryError, QueryErrorCode, ResolvedFileDescriptor,
    ResolvedSnapshotDescriptor, SnapshotResolutionRequest,
};
use url::Url;

pub const OWNER: &str = "Storage platform team";
pub const RESPONSIBILITY: &str =
    "Resolve Delta snapshots and mint browser-safe access descriptors.";

pub fn control_plane_target() -> ExecutionTarget {
    ExecutionTarget::Native
}

pub fn resolve_snapshot(
    request: SnapshotResolutionRequest,
) -> Result<ResolvedSnapshotDescriptor, QueryError> {
    run_on_runtime(async move { resolve_snapshot_async(request).await })
}

async fn resolve_snapshot_async(
    request: SnapshotResolutionRequest,
) -> Result<ResolvedSnapshotDescriptor, QueryError> {
    validate_snapshot_version(request.snapshot_version)?;

    let normalized_uri = normalize_table_uri(&request.table_uri)?;
    let table = match request.snapshot_version {
        Some(snapshot_version) => open_table_with_version(normalized_uri.clone(), snapshot_version)
            .await
            .map_err(map_delta_error)?,
        None => open_table(normalized_uri.clone())
            .await
            .map_err(map_delta_error)?,
    };
    let snapshot = table.snapshot().map_err(map_delta_error)?;
    let active_files = collect_active_files(snapshot)?;

    Ok(ResolvedSnapshotDescriptor {
        table_uri: normalized_uri.to_string(),
        snapshot_version: snapshot.version(),
        active_files,
    })
}

fn validate_snapshot_version(snapshot_version: Option<i64>) -> Result<(), QueryError> {
    if let Some(snapshot_version) = snapshot_version {
        if snapshot_version < 0 {
            return Err(QueryError::new(
                QueryErrorCode::InvalidRequest,
                format!(
                    "snapshot_version must be greater than or equal to 0, got {snapshot_version}"
                ),
                control_plane_target(),
            ));
        }
    }

    Ok(())
}

fn normalize_table_uri(table_uri: &str) -> Result<Url, QueryError> {
    let table_uri = table_uri.trim();
    if table_uri.is_empty() {
        return Err(QueryError::new(
            QueryErrorCode::InvalidRequest,
            "table_uri must not be empty",
            control_plane_target(),
        ));
    }

    if table_uri.contains("://") {
        let url = Url::parse(table_uri).map_err(|error| {
            QueryError::new(
                QueryErrorCode::InvalidRequest,
                format!("invalid table location: {error}"),
                control_plane_target(),
            )
        })?;

        if url.scheme() == "file" {
            return normalize_local_path(url.to_file_path().map_err(|_| {
                QueryError::new(
                    QueryErrorCode::InvalidRequest,
                    format!("invalid file table location: {table_uri}"),
                    control_plane_target(),
                )
            })?);
        }

        Ok(url)
    } else {
        normalize_local_path(std::path::PathBuf::from(table_uri))
    }
}

fn normalize_local_path(path: std::path::PathBuf) -> Result<Url, QueryError> {
    let canonical_path = std::fs::canonicalize(&path).map_err(|error| {
        QueryError::new(
            QueryErrorCode::InvalidRequest,
            format!("invalid table location '{}': {error}", path.display()),
            control_plane_target(),
        )
    })?;

    Url::from_directory_path(canonical_path).map_err(|_| {
        QueryError::new(
            QueryErrorCode::InvalidRequest,
            format!("table location '{}' must be a directory", path.display()),
            control_plane_target(),
        )
    })
}

fn collect_active_files(
    snapshot: &DeltaTableState,
) -> Result<Vec<ResolvedFileDescriptor>, QueryError> {
    let mut active_files = snapshot
        .log_data()
        .iter()
        .map(|file| {
            let size_bytes = u64::try_from(file.size()).map_err(|_| {
                QueryError::new(
                    QueryErrorCode::ExecutionFailed,
                    format!("file '{}' reported an invalid negative size", file.path()),
                    control_plane_target(),
                )
            })?;

            Ok(ResolvedFileDescriptor {
                path: file.path().into_owned(),
                size_bytes,
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
        })
        .collect::<Result<Vec<_>, QueryError>>()?;

    active_files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(active_files)
}

fn run_on_runtime<F, T>(future: F) -> Result<T, QueryError>
where
    F: Future<Output = Result<T, QueryError>> + Send + 'static,
    T: Send + 'static,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::spawn(move || {
            tokio::runtime::Runtime::new()
                .map_err(map_runtime_creation_error)?
                .block_on(future)
        })
        .join()
        .map_err(|_| {
            QueryError::new(
                QueryErrorCode::ExecutionFailed,
                "control-plane snapshot resolution thread panicked",
                control_plane_target(),
            )
        })?
    } else {
        tokio::runtime::Runtime::new()
            .map_err(map_runtime_creation_error)?
            .block_on(future)
    }
}

fn map_runtime_creation_error(error: std::io::Error) -> QueryError {
    QueryError::new(
        QueryErrorCode::ExecutionFailed,
        format!("failed to create tokio runtime: {error}"),
        control_plane_target(),
    )
}

fn map_delta_error(error: DeltaTableError) -> QueryError {
    match error {
        DeltaTableError::InvalidTableLocation(message) => QueryError::new(
            QueryErrorCode::InvalidRequest,
            format!("invalid table location: {message}"),
            control_plane_target(),
        ),
        DeltaTableError::NotATable(message) => QueryError::new(
            QueryErrorCode::InvalidRequest,
            format!("not a delta table: {message}"),
            control_plane_target(),
        ),
        DeltaTableError::NotInitialized => QueryError::new(
            QueryErrorCode::InvalidRequest,
            "table location is not initialized as a Delta table",
            control_plane_target(),
        ),
        DeltaTableError::InvalidVersion(version) => QueryError::new(
            QueryErrorCode::InvalidRequest,
            format!("snapshot version {version} is not available for this table"),
            control_plane_target(),
        ),
        DeltaTableError::MissingFeature { feature, url } => QueryError::new(
            QueryErrorCode::UnsupportedFeature,
            format!("missing runtime feature '{feature}' for table location {url}"),
            control_plane_target(),
        ),
        DeltaTableError::ObjectStore { source } => map_object_store_error(source),
        DeltaTableError::KernelError(source) => {
            if let Some(mapped) = query_error_from_unavailable_snapshot_message(&source.to_string())
            {
                mapped
            } else {
                QueryError::new(
                    QueryErrorCode::ExecutionFailed,
                    format!("Kernel error: {source}"),
                    control_plane_target(),
                )
            }
        }
        DeltaTableError::Generic(message) => {
            if let Some(mapped) = query_error_from_unavailable_snapshot_message(&message) {
                mapped
            } else {
                QueryError::new(
                    QueryErrorCode::ExecutionFailed,
                    format!("Generic DeltaTable error: {message}"),
                    control_plane_target(),
                )
            }
        }
        other => QueryError::new(
            QueryErrorCode::ExecutionFailed,
            other.to_string(),
            control_plane_target(),
        ),
    }
}

fn query_error_from_unavailable_snapshot_message(message: &str) -> Option<QueryError> {
    let normalized = message.to_ascii_lowercase();
    let snapshot_unavailable = normalized.contains("specified end version")
        || normalized.contains("provided snapshot version does not match the requested version")
        || normalized.contains("snapshot version is not available");

    snapshot_unavailable.then(|| {
        QueryError::new(
            QueryErrorCode::InvalidRequest,
            format!("requested snapshot version is not available: {message}"),
            control_plane_target(),
        )
    })
}

fn map_object_store_error(error: ObjectStoreError) -> QueryError {
    match error {
        ObjectStoreError::PermissionDenied { .. } | ObjectStoreError::Unauthenticated { .. } => {
            QueryError::new(
                QueryErrorCode::AccessDenied,
                error.to_string(),
                control_plane_target(),
            )
        }
        ObjectStoreError::NotFound { .. } => QueryError::new(
            QueryErrorCode::ObjectStoreProtocol,
            error.to_string(),
            control_plane_target(),
        ),
        ObjectStoreError::InvalidPath { .. } | ObjectStoreError::UnknownConfigurationKey { .. } => {
            QueryError::new(
                QueryErrorCode::InvalidRequest,
                error.to_string(),
                control_plane_target(),
            )
        }
        ObjectStoreError::NotSupported { .. } | ObjectStoreError::NotImplemented => {
            QueryError::new(
                QueryErrorCode::UnsupportedFeature,
                error.to_string(),
                control_plane_target(),
            )
        }
        _ => QueryError::new(
            QueryErrorCode::ExecutionFailed,
            error.to_string(),
            control_plane_target(),
        ),
    }
}
