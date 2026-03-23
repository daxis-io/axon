use std::error::Error as StdError;
use std::future::Future;
use std::path::PathBuf;

use deltalake::datafusion::common::DataFusionError;
use deltalake::{DeltaTableError, ObjectStoreError};
use query_contract::{ExecutionTarget, QueryError, QueryErrorCode};
use url::Url;

pub fn run_on_runtime<F, T>(
    future: F,
    panic_message: &'static str,
    target: ExecutionTarget,
) -> Result<T, QueryError>
where
    F: Future<Output = Result<T, QueryError>> + Send + 'static,
    T: Send + 'static,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::spawn(move || {
            tokio::runtime::Runtime::new()
                .map_err(|error| map_runtime_creation_error(error, target))?
                .block_on(future)
        })
        .join()
        .map_err(|_| QueryError::new(QueryErrorCode::ExecutionFailed, panic_message, target))?
    } else {
        tokio::runtime::Runtime::new()
            .map_err(|error| map_runtime_creation_error(error, target))?
            .block_on(future)
    }
}

pub fn validate_snapshot_version(
    snapshot_version: Option<i64>,
    target: ExecutionTarget,
) -> Result<(), QueryError> {
    if let Some(snapshot_version) = snapshot_version {
        if snapshot_version < 0 {
            return Err(QueryError::new(
                QueryErrorCode::InvalidRequest,
                format!(
                    "snapshot_version must be greater than or equal to 0, got {snapshot_version}"
                ),
                target,
            ));
        }
    }

    Ok(())
}

pub fn normalize_table_uri(table_uri: &str, target: ExecutionTarget) -> Result<Url, QueryError> {
    let table_uri = table_uri.trim();
    if table_uri.is_empty() {
        return Err(QueryError::new(
            QueryErrorCode::InvalidRequest,
            "table_uri must not be empty",
            target,
        ));
    }

    if table_uri.contains("://") {
        let url = Url::parse(table_uri).map_err(|error| {
            QueryError::new(
                QueryErrorCode::InvalidRequest,
                format!("invalid table location: {error}"),
                target,
            )
        })?;

        if url.scheme() == "file" {
            return normalize_local_path(
                url.to_file_path().map_err(|_| {
                    QueryError::new(
                        QueryErrorCode::InvalidRequest,
                        format!("invalid file table location: {table_uri}"),
                        target,
                    )
                })?,
                target,
            );
        }

        Ok(url)
    } else {
        normalize_local_path(PathBuf::from(table_uri), target)
    }
}

pub fn map_delta_error(error: DeltaTableError, target: ExecutionTarget) -> QueryError {
    match error {
        DeltaTableError::InvalidTableLocation(message) => QueryError::new(
            QueryErrorCode::InvalidRequest,
            format!("invalid table location: {message}"),
            target,
        ),
        DeltaTableError::NotATable(message) => QueryError::new(
            QueryErrorCode::InvalidRequest,
            format!("not a delta table: {message}"),
            target,
        ),
        DeltaTableError::NotInitialized => QueryError::new(
            QueryErrorCode::InvalidRequest,
            "table location is not initialized as a Delta table",
            target,
        ),
        DeltaTableError::InvalidVersion(version) => QueryError::new(
            QueryErrorCode::InvalidRequest,
            format!("snapshot version {version} is not available for this table"),
            target,
        ),
        DeltaTableError::MissingFeature { feature, url } => QueryError::new(
            QueryErrorCode::UnsupportedFeature,
            format!("missing runtime feature '{feature}' for table location {url}"),
            target,
        ),
        DeltaTableError::ObjectStore { source } => map_object_store_error(source, target),
        DeltaTableError::KernelError(source) => {
            if let Some(mapped) =
                query_error_from_unavailable_snapshot_message(&source.to_string(), target)
            {
                mapped
            } else {
                QueryError::new(
                    QueryErrorCode::ExecutionFailed,
                    format!("Kernel error: {source}"),
                    target,
                )
            }
        }
        DeltaTableError::Generic(message) => {
            if let Some(mapped) = query_error_from_unavailable_snapshot_message(&message, target) {
                mapped
            } else {
                QueryError::new(
                    QueryErrorCode::ExecutionFailed,
                    format!("Generic DeltaTable error: {message}"),
                    target,
                )
            }
        }
        other => QueryError::new(QueryErrorCode::ExecutionFailed, other.to_string(), target),
    }
}

pub fn query_error_from_object_store_error(
    error: &ObjectStoreError,
    target: ExecutionTarget,
) -> QueryError {
    match error {
        ObjectStoreError::PermissionDenied { .. } | ObjectStoreError::Unauthenticated { .. } => {
            QueryError::new(QueryErrorCode::AccessDenied, error.to_string(), target)
        }
        ObjectStoreError::NotFound { .. } => QueryError::new(
            QueryErrorCode::ObjectStoreProtocol,
            error.to_string(),
            target,
        ),
        ObjectStoreError::InvalidPath { .. } | ObjectStoreError::UnknownConfigurationKey { .. } => {
            QueryError::new(QueryErrorCode::InvalidRequest, error.to_string(), target)
        }
        ObjectStoreError::NotSupported { .. } | ObjectStoreError::NotImplemented => {
            QueryError::new(
                QueryErrorCode::UnsupportedFeature,
                error.to_string(),
                target,
            )
        }
        _ => QueryError::new(QueryErrorCode::ExecutionFailed, error.to_string(), target),
    }
}

pub fn map_object_store_error(error: ObjectStoreError, target: ExecutionTarget) -> QueryError {
    query_error_from_object_store_error(&error, target)
}

pub fn map_datafusion_error(error: DataFusionError, target: ExecutionTarget) -> QueryError {
    if let Some(object_store_error) = find_object_store_error(&error) {
        return query_error_from_object_store_error(object_store_error, target);
    }

    if let Some(mapped) = query_error_from_datafusion_error(&error, target) {
        return mapped;
    }

    match error {
        DataFusionError::ObjectStore(source) => map_object_store_error(*source, target),
        DataFusionError::SQL(..)
        | DataFusionError::Plan(_)
        | DataFusionError::SchemaError(..)
        | DataFusionError::Configuration(_) => {
            QueryError::new(QueryErrorCode::InvalidRequest, error.to_string(), target)
        }
        DataFusionError::NotImplemented(_) => QueryError::new(
            QueryErrorCode::UnsupportedFeature,
            error.to_string(),
            target,
        ),
        _ => QueryError::new(QueryErrorCode::ExecutionFailed, error.to_string(), target),
    }
}

pub fn query_error_from_unavailable_snapshot_message(
    message: &str,
    target: ExecutionTarget,
) -> Option<QueryError> {
    let normalized = message.to_ascii_lowercase();
    let snapshot_unavailable = normalized.contains("specified end version")
        || normalized.contains("provided snapshot version does not match the requested version")
        || normalized.contains("snapshot version is not available");

    snapshot_unavailable.then(|| {
        QueryError::new(
            QueryErrorCode::InvalidRequest,
            format!("requested snapshot version is not available: {message}"),
            target,
        )
    })
}

fn find_object_store_error<'a>(
    error: &'a (dyn StdError + 'static),
) -> Option<&'a ObjectStoreError> {
    let mut current = Some(error);
    while let Some(candidate) = current {
        if let Some(object_store_error) = candidate.downcast_ref::<ObjectStoreError>() {
            return Some(object_store_error);
        }
        current = candidate.source();
    }
    None
}

fn query_error_from_datafusion_error(
    error: &DataFusionError,
    target: ExecutionTarget,
) -> Option<QueryError> {
    match error {
        DataFusionError::Execution(message) => query_error_from_execution_message(message, target),
        DataFusionError::ParquetError(error) => {
            query_error_from_execution_message(&error.to_string(), target)
        }
        DataFusionError::IoError(error) => query_error_from_io_error(error, target),
        DataFusionError::Context(_, inner) => query_error_from_datafusion_error(inner, target),
        DataFusionError::Diagnostic(_, inner) => query_error_from_datafusion_error(inner, target),
        DataFusionError::Shared(inner) => query_error_from_datafusion_error(inner, target),
        DataFusionError::Collection(errors) => errors
            .iter()
            .find_map(|error| query_error_from_datafusion_error(error, target)),
        _ => None,
    }
}

fn query_error_from_io_error(
    error: &std::io::Error,
    target: ExecutionTarget,
) -> Option<QueryError> {
    match error.kind() {
        std::io::ErrorKind::NotFound => Some(QueryError::new(
            QueryErrorCode::ObjectStoreProtocol,
            error.to_string(),
            target,
        )),
        std::io::ErrorKind::PermissionDenied => Some(QueryError::new(
            QueryErrorCode::AccessDenied,
            error.to_string(),
            target,
        )),
        _ => query_error_from_execution_message(&error.to_string(), target),
    }
}

fn query_error_from_execution_message(
    message: &str,
    target: ExecutionTarget,
) -> Option<QueryError> {
    let normalized = message.to_ascii_lowercase();

    if normalized.contains("not found")
        || normalized.contains("404")
        || normalized.contains("no such file or directory")
        || normalized.contains("os error 2")
    {
        return Some(QueryError::new(
            QueryErrorCode::ObjectStoreProtocol,
            message.to_string(),
            target,
        ));
    }

    if normalized.contains("permission denied")
        || normalized.contains("access denied")
        || normalized.contains("unauthenticated")
        || normalized.contains("unauthorized")
        || normalized.contains("operation not permitted")
        || normalized.contains("os error 1")
        || normalized.contains("os error 13")
        || normalized.contains("401")
        || normalized.contains("403")
    {
        return Some(QueryError::new(
            QueryErrorCode::AccessDenied,
            message.to_string(),
            target,
        ));
    }

    None
}

fn normalize_local_path(path: PathBuf, target: ExecutionTarget) -> Result<Url, QueryError> {
    let canonical_path = std::fs::canonicalize(&path).map_err(|error| {
        QueryError::new(
            QueryErrorCode::InvalidRequest,
            format!("invalid table location '{}': {error}", path.display()),
            target,
        )
    })?;

    Url::from_directory_path(canonical_path).map_err(|_| {
        QueryError::new(
            QueryErrorCode::InvalidRequest,
            format!("table location '{}' must be a directory", path.display()),
            target,
        )
    })
}

fn map_runtime_creation_error(error: std::io::Error, target: ExecutionTarget) -> QueryError {
    QueryError::new(
        QueryErrorCode::ExecutionFailed,
        format!("failed to create tokio runtime: {error}"),
        target,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use deltalake::datafusion::common::{Column, DataFusionError, SchemaError};
    use tempfile::TempDir;

    #[test]
    fn normalize_table_uri_accepts_trimmed_raw_paths_and_file_urls() {
        let tempdir = TempDir::new().expect("tempdir should be created");
        let expected = Url::from_directory_path(
            std::fs::canonicalize(tempdir.path()).expect("tempdir should canonicalize"),
        )
        .expect("canonical tempdir should be a directory")
        .to_string();

        let raw_path = format!("  {}  ", tempdir.path().display());
        let file_url = format!("  {}  ", Url::from_directory_path(tempdir.path()).unwrap());

        assert_eq!(
            normalize_table_uri(&raw_path, ExecutionTarget::Native)
                .expect("raw path should normalize")
                .to_string(),
            expected
        );
        assert_eq!(
            normalize_table_uri(&file_url, ExecutionTarget::Native)
                .expect("file url should normalize")
                .to_string(),
            expected
        );
    }

    #[test]
    fn validate_snapshot_version_rejects_negative_versions() {
        let error = validate_snapshot_version(Some(-1), ExecutionTarget::Native)
            .expect_err("negative versions should be rejected");

        assert_eq!(error.code, QueryErrorCode::InvalidRequest);
        assert_eq!(error.target, ExecutionTarget::Native);
        assert!(error.message.contains("snapshot_version"));
    }

    #[test]
    fn invalid_snapshot_versions_map_to_invalid_request() {
        let error = map_delta_error(DeltaTableError::InvalidVersion(42), ExecutionTarget::Native);

        assert_eq!(error.code, QueryErrorCode::InvalidRequest);
        assert_eq!(error.target, ExecutionTarget::Native);
    }

    #[test]
    fn permission_denied_object_store_errors_map_to_access_denied() {
        let error = map_object_store_error(
            ObjectStoreError::PermissionDenied {
                path: "_delta_log/00000000000000000000.json".to_string(),
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "permission denied",
                )),
            },
            ExecutionTarget::Native,
        );

        assert_eq!(error.code, QueryErrorCode::AccessDenied);
        assert_eq!(error.target, ExecutionTarget::Native);
    }

    #[test]
    fn datafusion_execution_errors_with_os_error_13_map_to_access_denied() {
        let error = map_datafusion_error(
            DataFusionError::Execution(
                "failed to open parquet file: Permission denied (os error 13)".to_string(),
            ),
            ExecutionTarget::Native,
        );

        assert_eq!(error.code, QueryErrorCode::AccessDenied);
        assert_eq!(error.target, ExecutionTarget::Native);
    }

    #[test]
    fn datafusion_execution_errors_with_operation_not_permitted_map_to_access_denied() {
        let error = map_datafusion_error(
            DataFusionError::Execution("read failed: Operation not permitted (os error 1)".into()),
            ExecutionTarget::Native,
        );

        assert_eq!(error.code, QueryErrorCode::AccessDenied);
        assert_eq!(error.target, ExecutionTarget::Native);
    }

    #[test]
    fn datafusion_execution_errors_with_missing_file_messages_map_to_object_store_protocol() {
        let error = map_datafusion_error(
            DataFusionError::Execution(
                "failed to open parquet file: No such file or directory (os error 2)".into(),
            ),
            ExecutionTarget::Native,
        );

        assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
        assert_eq!(error.target, ExecutionTarget::Native);
    }

    #[test]
    fn nested_datafusion_execution_errors_preserve_access_denied_classification() {
        let error = map_datafusion_error(
            DataFusionError::Context(
                "while scanning parquet".into(),
                Box::new(DataFusionError::Execution(
                    "failed to open parquet file: Permission denied (os error 13)".into(),
                )),
            ),
            ExecutionTarget::Native,
        );

        assert_eq!(error.code, QueryErrorCode::AccessDenied);
        assert_eq!(error.target, ExecutionTarget::Native);
    }

    #[test]
    fn schema_errors_containing_status_like_field_names_stay_invalid_request() {
        let error = map_datafusion_error(
            DataFusionError::SchemaError(
                Box::new(SchemaError::FieldNotFound {
                    field: Box::new(Column::from_name("unauthorized")),
                    valid_fields: vec![],
                }),
                Box::new(None),
            ),
            ExecutionTarget::Native,
        );

        assert_eq!(error.code, QueryErrorCode::InvalidRequest);
        assert_eq!(error.target, ExecutionTarget::Native);
    }

    #[test]
    fn unavailable_snapshot_messages_map_to_invalid_request() {
        let error = query_error_from_unavailable_snapshot_message(
            "Generic delta kernel error: LogSegment end version 2 not the same as the specified end version 99",
            ExecutionTarget::Native,
        )
        .expect("snapshot-version mismatch should be recognized");

        assert_eq!(error.code, QueryErrorCode::InvalidRequest);
        assert_eq!(error.target, ExecutionTarget::Native);
        assert!(error.message.contains("requested snapshot version"));
    }

    #[test]
    fn run_on_runtime_is_safe_inside_existing_runtime() {
        let value = tokio::runtime::Runtime::new()
            .expect("runtime should be created")
            .block_on(async {
                run_on_runtime(
                    async { Ok::<u64, QueryError>(7) },
                    "shared runtime helper thread panicked",
                    ExecutionTarget::Native,
                )
            })
            .expect("runtime helper should work inside an existing runtime");

        assert_eq!(value, 7);
    }
}
