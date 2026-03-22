//! Trusted control-plane skeleton for snapshot resolution and browser-safe access descriptors.

use std::collections::BTreeMap;

use delta_runtime_support::{
    map_delta_error, normalize_table_uri, run_on_runtime, validate_snapshot_version,
};
use deltalake::kernel::scalars::ScalarExt;
use deltalake::table::state::DeltaTableState;
use deltalake::{open_table, open_table_with_version};
use query_contract::{
    ExecutionTarget, QueryError, QueryErrorCode, ResolvedFileDescriptor,
    ResolvedSnapshotDescriptor, SnapshotResolutionRequest,
};

pub const OWNER: &str = "Storage platform team";
pub const RESPONSIBILITY: &str =
    "Resolve Delta snapshots and mint browser-safe access descriptors.";

pub fn control_plane_target() -> ExecutionTarget {
    ExecutionTarget::Native
}

pub fn resolve_snapshot(
    request: SnapshotResolutionRequest,
) -> Result<ResolvedSnapshotDescriptor, QueryError> {
    run_on_runtime(
        async move { resolve_snapshot_async(request).await },
        "control-plane snapshot resolution thread panicked",
        control_plane_target(),
    )
}

async fn resolve_snapshot_async(
    request: SnapshotResolutionRequest,
) -> Result<ResolvedSnapshotDescriptor, QueryError> {
    validate_snapshot_version(request.snapshot_version, control_plane_target())?;

    let normalized_uri = normalize_table_uri(&request.table_uri, control_plane_target())?;
    let table = match request.snapshot_version {
        Some(snapshot_version) => open_table_with_version(normalized_uri.clone(), snapshot_version)
            .await
            .map_err(|error| map_delta_error(error, control_plane_target()))?,
        None => open_table(normalized_uri.clone())
            .await
            .map_err(|error| map_delta_error(error, control_plane_target()))?,
    };
    let snapshot = table
        .snapshot()
        .map_err(|error| map_delta_error(error, control_plane_target()))?;
    let active_files = collect_active_files(snapshot)?;

    Ok(ResolvedSnapshotDescriptor {
        table_uri: normalized_uri.to_string(),
        snapshot_version: snapshot.version(),
        active_files,
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
