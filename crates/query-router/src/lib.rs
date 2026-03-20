//! Route selection and fallback recording for query execution.

use query_contract::ExecutionTarget;

pub const OWNER: &str = "Runtime / engine team";
pub const RESPONSIBILITY: &str = "Choose browser vs native execution and record fallback reasons.";

pub fn default_target() -> ExecutionTarget {
    ExecutionTarget::Native
}
