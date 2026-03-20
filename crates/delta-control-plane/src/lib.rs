//! Trusted control-plane skeleton for snapshot resolution and browser-safe access descriptors.

use query_contract::ExecutionTarget;

pub const OWNER: &str = "Storage platform team";
pub const RESPONSIBILITY: &str =
    "Resolve Delta snapshots and mint browser-safe access descriptors.";

pub fn control_plane_target() -> ExecutionTarget {
    ExecutionTarget::Native
}
