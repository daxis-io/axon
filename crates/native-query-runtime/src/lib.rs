//! Native reference runtime for trusted Delta + DataFusion execution.

use std::future::Future;
use std::time::Instant;

use deltalake::arrow::record_batch::RecordBatch;
use deltalake::datafusion::common::DataFusionError;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::errors::DeltaTableError;
use deltalake::kernel::{DataType, PrimitiveType, StructField};
use deltalake::table::config::TablePropertiesExt;
use deltalake::{open_table, DeltaTable, ObjectStoreError};
use query_contract::{
    CapabilityKey, CapabilityReport, CapabilityState, ExecutionTarget, QueryError, QueryErrorCode,
    QueryMetricsSummary, QueryRequest,
};
use url::Url;

pub const OWNER: &str = "Runtime / engine team";
pub const RESPONSIBILITY: &str = "Authoritative native execution path and fallback runtime.";
pub const DEFAULT_TABLE_NAME: &str = "axon_table";

pub fn runtime_target() -> ExecutionTarget {
    ExecutionTarget::Native
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeTableBootstrap {
    pub table_uri: String,
    pub version: i64,
    pub active_files: u64,
}

#[derive(Debug)]
pub struct NativeQueryResult {
    pub batches: Vec<RecordBatch>,
    pub explain_lines: Option<Vec<String>>,
    pub metrics: QueryMetricsSummary,
    pub capabilities: CapabilityReport,
}

pub fn bootstrap_table(table_uri: &str) -> Result<NativeTableBootstrap, QueryError> {
    let table_uri = table_uri.to_string();

    run_on_runtime(async move {
        let prepared = PreparedTable::load(&table_uri).await?;

        Ok(NativeTableBootstrap {
            table_uri: prepared.table_uri,
            version: prepared.version,
            active_files: prepared.files_touched,
        })
    })
}

pub fn execute_query(request: QueryRequest) -> Result<NativeQueryResult, QueryError> {
    run_on_runtime(async move { execute_query_async(request).await })
}

async fn execute_query_async(request: QueryRequest) -> Result<NativeQueryResult, QueryError> {
    if request.sql.trim().is_empty() {
        return Err(QueryError::new(
            QueryErrorCode::InvalidRequest,
            "sql must not be empty",
            ExecutionTarget::Native,
        ));
    }

    let prepared = PreparedTable::load(&request.table_uri).await?;
    let query_started_at = Instant::now();

    let batches = prepared
        .session
        .sql(&request.sql)
        .await
        .map_err(map_datafusion_error)?
        .collect()
        .await
        .map_err(map_datafusion_error)?;

    let explain_lines = if request.options.include_explain {
        Some(collect_explain_lines(&prepared.session, &request.sql).await?)
    } else {
        None
    };

    let metrics = if request.options.collect_metrics {
        QueryMetricsSummary {
            bytes_fetched: prepared.bytes_fetched,
            duration_ms: query_started_at.elapsed().as_millis().max(1) as u64,
            files_touched: prepared.files_touched,
            files_skipped: 0,
        }
    } else {
        QueryMetricsSummary::default()
    };

    Ok(NativeQueryResult {
        batches,
        explain_lines,
        metrics,
        capabilities: prepared.capabilities,
    })
}

async fn collect_explain_lines(
    session: &SessionContext,
    sql: &str,
) -> Result<Vec<String>, QueryError> {
    let explain_sql = format!("EXPLAIN VERBOSE {sql}");
    let explain_batches = session
        .sql(&explain_sql)
        .await
        .map_err(map_datafusion_error)?
        .collect()
        .await
        .map_err(map_datafusion_error)?;

    Ok(
        deltalake::arrow::util::pretty::pretty_format_batches(&explain_batches)
            .map_err(map_arrow_error)?
            .to_string()
            .lines()
            .map(ToOwned::to_owned)
            .collect(),
    )
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
                "native query runtime thread panicked",
                ExecutionTarget::Native,
            )
        })?
    } else {
        tokio::runtime::Runtime::new()
            .map_err(map_runtime_creation_error)?
            .block_on(future)
    }
}

struct PreparedTable {
    capabilities: CapabilityReport,
    bytes_fetched: u64,
    files_touched: u64,
    session: SessionContext,
    table_uri: String,
    version: i64,
}

impl PreparedTable {
    async fn load(table_uri: &str) -> Result<Self, QueryError> {
        let normalized_uri = normalize_table_uri(table_uri)?;
        let table = open_table(normalized_uri.clone())
            .await
            .map_err(map_delta_error)?;
        let capabilities = detect_capabilities(&table)?;
        ensure_supported_table_shape(&table, &capabilities)?;

        let session = SessionContext::new();
        table
            .update_datafusion_session(&session.state())
            .map_err(map_delta_error)?;
        let provider = table.table_provider().await.map_err(map_datafusion_error)?;
        session
            .register_table(DEFAULT_TABLE_NAME, provider)
            .map_err(map_datafusion_error)?;

        let snapshot = table.snapshot().map_err(map_delta_error)?;
        let files_touched = snapshot.log_data().num_files() as u64;
        let bytes_fetched = snapshot
            .log_data()
            .iter()
            .map(|file| file.size().max(0) as u64)
            .sum();

        Ok(Self {
            capabilities,
            bytes_fetched,
            files_touched,
            session,
            table_uri: normalized_uri.to_string(),
            version: snapshot.version(),
        })
    }
}

fn normalize_table_uri(table_uri: &str) -> Result<Url, QueryError> {
    let table_uri = table_uri.trim();
    if table_uri.is_empty() {
        return Err(QueryError::new(
            QueryErrorCode::InvalidRequest,
            "table_uri must not be empty",
            ExecutionTarget::Native,
        ));
    }

    if table_uri.contains("://") {
        let url = Url::parse(table_uri).map_err(|error| {
            QueryError::new(
                QueryErrorCode::InvalidRequest,
                format!("invalid table location: {error}"),
                ExecutionTarget::Native,
            )
        })?;

        if url.scheme() == "file" {
            return normalize_local_path(url.to_file_path().map_err(|_| {
                QueryError::new(
                    QueryErrorCode::InvalidRequest,
                    format!("invalid file table location: {table_uri}"),
                    ExecutionTarget::Native,
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
            ExecutionTarget::Native,
        )
    })?;

    Url::from_directory_path(canonical_path).map_err(|_| {
        QueryError::new(
            QueryErrorCode::InvalidRequest,
            format!("table location '{}' must be a directory", path.display()),
            ExecutionTarget::Native,
        )
    })
}

fn detect_capabilities(table: &DeltaTable) -> Result<CapabilityReport, QueryError> {
    let snapshot = table.snapshot().map_err(map_delta_error)?;
    let schema = snapshot.schema();
    let deletion_vectors_present = snapshot
        .log_data()
        .iter()
        .any(|file| file.deletion_vector_descriptor().is_some());
    let change_data_feed_enabled = snapshot.table_config().enable_change_data_feed();
    let column_mapping_present = schema.fields().any(field_uses_column_mapping);
    let timestamp_ntz_present = schema.fields().any(field_uses_timestamp_ntz);

    let mut report = CapabilityReport::from_pairs([
        (CapabilityKey::ChangeDataFeed, CapabilityState::Unsupported),
        (CapabilityKey::ColumnMapping, CapabilityState::Unsupported),
        (CapabilityKey::DeletionVectors, CapabilityState::Unsupported),
        (
            CapabilityKey::MultiPartitionExecution,
            CapabilityState::Experimental,
        ),
        (CapabilityKey::ProxyAccess, CapabilityState::Unsupported),
        (CapabilityKey::RangeReads, CapabilityState::Supported),
        (CapabilityKey::SignedUrlAccess, CapabilityState::Unsupported),
        (CapabilityKey::TimeTravel, CapabilityState::Unsupported),
        (CapabilityKey::TimestampNtz, CapabilityState::Unsupported),
    ]);

    if change_data_feed_enabled {
        report.insert(CapabilityKey::ChangeDataFeed, CapabilityState::Experimental);
    }

    if column_mapping_present {
        report.insert(CapabilityKey::ColumnMapping, CapabilityState::Experimental);
    }

    if deletion_vectors_present {
        report.insert(
            CapabilityKey::DeletionVectors,
            CapabilityState::Experimental,
        );
    }

    if timestamp_ntz_present {
        report.insert(CapabilityKey::TimestampNtz, CapabilityState::Experimental);
    }

    Ok(report)
}

fn ensure_supported_table_shape(
    table: &DeltaTable,
    capabilities: &CapabilityReport,
) -> Result<(), QueryError> {
    let snapshot = table.snapshot().map_err(map_delta_error)?;
    let mut unsupported_capabilities = Vec::new();

    if snapshot.table_config().enable_change_data_feed() {
        unsupported_capabilities.push(CapabilityKey::ChangeDataFeed);
    }

    if snapshot.schema().fields().any(field_uses_column_mapping) {
        unsupported_capabilities.push(CapabilityKey::ColumnMapping);
    }

    if snapshot
        .log_data()
        .iter()
        .any(|file| file.deletion_vector_descriptor().is_some())
    {
        unsupported_capabilities.push(CapabilityKey::DeletionVectors);
    }

    if snapshot.schema().fields().any(field_uses_timestamp_ntz) {
        unsupported_capabilities.push(CapabilityKey::TimestampNtz);
    }

    if let Some(capability) = unsupported_capabilities.into_iter().next() {
        let state = capabilities
            .state(capability)
            .unwrap_or(CapabilityState::Unsupported);

        return Err(QueryError::new(
            QueryErrorCode::UnsupportedFeature,
            format!(
                "table requires {:?} support, but Sprint 1 only allows capabilities marked supported; current state is {:?}",
                capability, state
            ),
            ExecutionTarget::Native,
        ));
    }

    Ok(())
}

fn field_uses_timestamp_ntz(field: &StructField) -> bool {
    data_type_uses_timestamp_ntz(&field.data_type)
}

fn data_type_uses_timestamp_ntz(data_type: &DataType) -> bool {
    match data_type {
        DataType::Primitive(PrimitiveType::TimestampNtz) => true,
        DataType::Array(array_type) => data_type_uses_timestamp_ntz(array_type.element_type()),
        DataType::Struct(struct_type) | DataType::Variant(struct_type) => {
            struct_type.fields().any(field_uses_timestamp_ntz)
        }
        DataType::Map(map_type) => {
            data_type_uses_timestamp_ntz(map_type.key_type())
                || data_type_uses_timestamp_ntz(map_type.value_type())
        }
        DataType::Primitive(_) => false,
    }
}

fn field_uses_column_mapping(field: &StructField) -> bool {
    field
        .metadata
        .keys()
        .any(|key| key.starts_with("delta.columnMapping."))
        || data_type_uses_column_mapping(&field.data_type)
}

fn data_type_uses_column_mapping(data_type: &DataType) -> bool {
    match data_type {
        DataType::Array(array_type) => data_type_uses_column_mapping(array_type.element_type()),
        DataType::Struct(struct_type) | DataType::Variant(struct_type) => {
            struct_type.fields().any(field_uses_column_mapping)
        }
        DataType::Map(map_type) => {
            data_type_uses_column_mapping(map_type.key_type())
                || data_type_uses_column_mapping(map_type.value_type())
        }
        DataType::Primitive(_) => false,
    }
}

fn map_arrow_error(error: deltalake::arrow::error::ArrowError) -> QueryError {
    QueryError::new(
        QueryErrorCode::ExecutionFailed,
        format!("failed to format arrow batches: {error}"),
        ExecutionTarget::Native,
    )
}

fn map_runtime_creation_error(error: std::io::Error) -> QueryError {
    QueryError::new(
        QueryErrorCode::ExecutionFailed,
        format!("failed to create tokio runtime: {error}"),
        ExecutionTarget::Native,
    )
}

fn map_delta_error(error: DeltaTableError) -> QueryError {
    match error {
        DeltaTableError::InvalidTableLocation(message) => QueryError::new(
            QueryErrorCode::InvalidRequest,
            format!("invalid table location: {message}"),
            ExecutionTarget::Native,
        ),
        DeltaTableError::NotATable(message) => QueryError::new(
            QueryErrorCode::InvalidRequest,
            format!("not a delta table: {message}"),
            ExecutionTarget::Native,
        ),
        DeltaTableError::NotInitialized => QueryError::new(
            QueryErrorCode::InvalidRequest,
            "table location is not initialized as a Delta table",
            ExecutionTarget::Native,
        ),
        DeltaTableError::MissingFeature { feature, url } => QueryError::new(
            QueryErrorCode::UnsupportedFeature,
            format!("missing runtime feature '{feature}' for table location {url}"),
            ExecutionTarget::Native,
        ),
        DeltaTableError::ObjectStore { source } => map_object_store_error(source),
        other => QueryError::new(
            QueryErrorCode::ExecutionFailed,
            other.to_string(),
            ExecutionTarget::Native,
        ),
    }
}

fn map_datafusion_error(error: DataFusionError) -> QueryError {
    match error {
        DataFusionError::ObjectStore(source) => map_object_store_error(*source),
        DataFusionError::SQL(..)
        | DataFusionError::Plan(_)
        | DataFusionError::SchemaError(..)
        | DataFusionError::Configuration(_) => QueryError::new(
            QueryErrorCode::InvalidRequest,
            error.to_string(),
            ExecutionTarget::Native,
        ),
        DataFusionError::NotImplemented(_) => QueryError::new(
            QueryErrorCode::UnsupportedFeature,
            error.to_string(),
            ExecutionTarget::Native,
        ),
        _ => QueryError::new(
            QueryErrorCode::ExecutionFailed,
            error.to_string(),
            ExecutionTarget::Native,
        ),
    }
}

fn map_object_store_error(error: ObjectStoreError) -> QueryError {
    match error {
        ObjectStoreError::PermissionDenied { .. } | ObjectStoreError::Unauthenticated { .. } => {
            QueryError::new(
                QueryErrorCode::AccessDenied,
                error.to_string(),
                ExecutionTarget::Native,
            )
        }
        ObjectStoreError::NotFound { .. } => QueryError::new(
            QueryErrorCode::ObjectStoreProtocol,
            error.to_string(),
            ExecutionTarget::Native,
        ),
        ObjectStoreError::InvalidPath { .. } | ObjectStoreError::UnknownConfigurationKey { .. } => {
            QueryError::new(
                QueryErrorCode::InvalidRequest,
                error.to_string(),
                ExecutionTarget::Native,
            )
        }
        ObjectStoreError::NotSupported { .. } | ObjectStoreError::NotImplemented { .. } => {
            QueryError::new(
                QueryErrorCode::UnsupportedFeature,
                error.to_string(),
                ExecutionTarget::Native,
            )
        }
        _ => QueryError::new(
            QueryErrorCode::ExecutionFailed,
            error.to_string(),
            ExecutionTarget::Native,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permission_denied_object_store_errors_map_to_access_denied() {
        let error = map_object_store_error(ObjectStoreError::PermissionDenied {
            path: "_delta_log/00000000000000000000.json".to_string(),
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "permission denied",
            )),
        });

        assert_eq!(error.code, QueryErrorCode::AccessDenied);
        assert_eq!(error.target, ExecutionTarget::Native);
    }

    #[test]
    fn invalid_table_locations_map_to_invalid_request() {
        let error = map_delta_error(DeltaTableError::InvalidTableLocation(
            "bad gs uri".to_string(),
        ));

        assert_eq!(error.code, QueryErrorCode::InvalidRequest);
        assert_eq!(error.target, ExecutionTarget::Native);
    }
}
