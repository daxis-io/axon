//! Trusted control-plane skeleton for snapshot resolution and browser-safe access descriptors.

use std::collections::{BTreeMap, BTreeSet};

use delta_runtime_support::{
    canonical_table_policy_key_from_url, map_delta_error, normalize_table_uri, run_on_runtime,
    validate_snapshot_version,
};
use deltalake::kernel::scalars::ScalarExt;
use deltalake::table::state::DeltaTableState;
use deltalake::{open_table, open_table_with_version};
use query_contract::{
    validate_browser_object_url, BrowserHttpFileDescriptor, BrowserHttpSnapshotDescriptor,
    BrowserObjectUrlPolicy, ExecutionTarget, QueryError, QueryErrorCode, ResolvedFileDescriptor,
    ResolvedSnapshotDescriptor, SnapshotResolutionRequest,
};

pub const OWNER: &str = "Storage platform team";
pub const RESPONSIBILITY: &str =
    "Resolve Delta snapshots and mint browser-safe access descriptors.";

/// Exact-match allow/deny rules for normalized Delta table locators.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SnapshotAccessPolicy {
    allowed_table_keys: BTreeSet<String>,
    denied_table_keys: BTreeSet<String>,
}

impl SnapshotAccessPolicy {
    /// Adds an allow-list rule for the provided table locator.
    pub fn allow_table(&mut self, table_uri: &str) -> Result<(), QueryError> {
        self.allowed_table_keys
            .insert(delta_runtime_support::canonical_table_policy_key(
                table_uri,
                control_plane_target(),
            )?);
        Ok(())
    }

    /// Adds a deny-list rule for the provided table locator.
    pub fn deny_table(&mut self, table_uri: &str) -> Result<(), QueryError> {
        self.denied_table_keys
            .insert(delta_runtime_support::canonical_table_policy_key(
                table_uri,
                control_plane_target(),
            )?);
        Ok(())
    }

    fn enforce(&self, table_key: &str) -> Result<(), QueryError> {
        if self.denied_table_keys.contains(table_key) {
            return Err(QueryError::new(
                QueryErrorCode::SecurityPolicyViolation,
                "table access denied by control-plane policy",
                control_plane_target(),
            ));
        }

        if !self.allowed_table_keys.is_empty() && !self.allowed_table_keys.contains(table_key) {
            return Err(QueryError::new(
                QueryErrorCode::SecurityPolicyViolation,
                "table access is not allowed by control-plane policy",
                control_plane_target(),
            ));
        }

        Ok(())
    }
}

pub fn control_plane_target() -> ExecutionTarget {
    ExecutionTarget::Native
}

pub fn resolve_snapshot(
    request: SnapshotResolutionRequest,
) -> Result<ResolvedSnapshotDescriptor, QueryError> {
    resolve_snapshot_with_policy(request, &SnapshotAccessPolicy::default())
}

pub fn resolve_snapshot_with_policy(
    request: SnapshotResolutionRequest,
    policy: &SnapshotAccessPolicy,
) -> Result<ResolvedSnapshotDescriptor, QueryError> {
    validate_snapshot_version(request.snapshot_version, control_plane_target())?;

    let normalized_uri = normalize_table_uri(&request.table_uri, control_plane_target())?;
    let table_key = canonical_table_policy_key_from_url(&normalized_uri);
    policy.enforce(&table_key)?;
    let snapshot_version = request.snapshot_version;

    run_on_runtime(
        async move {
            let table = match snapshot_version {
                Some(snapshot_version) => {
                    open_table_with_version(normalized_uri.clone(), snapshot_version)
                        .await
                        .map_err(|error| map_delta_error(error, control_plane_target()))?
                }
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
        },
        "control-plane snapshot resolution thread panicked",
        control_plane_target(),
    )
}

pub fn attach_browser_http_urls(
    resolved_snapshot: ResolvedSnapshotDescriptor,
    object_urls_by_path: &BTreeMap<String, String>,
) -> Result<BrowserHttpSnapshotDescriptor, QueryError> {
    let mut resolved_paths = BTreeSet::new();
    let mut duplicate_paths = BTreeSet::new();
    for file in &resolved_snapshot.active_files {
        if !resolved_paths.insert(file.path.clone()) {
            duplicate_paths.insert(file.path.clone());
        }
    }
    let missing_paths = resolved_snapshot
        .active_files
        .iter()
        .filter(|file| !object_urls_by_path.contains_key(&file.path))
        .map(|file| file.path.clone())
        .collect::<Vec<_>>();
    let unexpected_paths = object_urls_by_path
        .keys()
        .filter(|path| !resolved_paths.contains(*path))
        .cloned()
        .collect::<Vec<_>>();

    if !duplicate_paths.is_empty() || !missing_paths.is_empty() || !unexpected_paths.is_empty() {
        let mut reasons = Vec::new();
        if !duplicate_paths.is_empty() {
            reasons.push(format!(
                "resolved snapshot contained duplicate paths [{}]",
                duplicate_paths
                    .iter()
                    .map(|path| format!("'{path}'"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if !missing_paths.is_empty() {
            reasons.push(format!(
                "missing URLs for [{}]",
                missing_paths
                    .iter()
                    .map(|path| format!("'{path}'"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if !unexpected_paths.is_empty() {
            reasons.push(format!(
                "unexpected URLs for [{}]",
                unexpected_paths
                    .iter()
                    .map(|path| format!("'{path}'"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }

        return Err(QueryError::new(
            QueryErrorCode::InvalidRequest,
            format!(
                "browser HTTP object URL coverage did not match the resolved snapshot: {}",
                reasons.join("; ")
            ),
            control_plane_target(),
        ));
    }

    let active_files = resolved_snapshot
        .active_files
        .into_iter()
        .map(|file| {
            let url = object_urls_by_path
                .get(&file.path)
                .expect("missing URL coverage should be rejected before mapping");
            validate_browser_http_url(url)?;

            Ok(BrowserHttpFileDescriptor {
                path: file.path,
                url: url.clone(),
                size_bytes: file.size_bytes,
                partition_values: file.partition_values,
            })
        })
        .collect::<Result<Vec<_>, QueryError>>()?;

    Ok(BrowserHttpSnapshotDescriptor {
        table_uri: resolved_snapshot.table_uri,
        snapshot_version: resolved_snapshot.snapshot_version,
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

fn validate_browser_http_url(url: &str) -> Result<(), QueryError> {
    validate_browser_object_url(
        url,
        control_plane_target(),
        BrowserObjectUrlPolicy::HttpsOnly,
        "browser HTTP object URL",
    )
    .map(|_| ())
}
