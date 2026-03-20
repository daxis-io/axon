//! Native query runtime skeleton for trusted Delta + DataFusion execution.

use query_contract::ExecutionTarget;

pub const OWNER: &str = "Runtime / engine team";
pub const RESPONSIBILITY: &str = "Authoritative native execution path and fallback runtime.";

pub fn runtime_target() -> ExecutionTarget {
    ExecutionTarget::Native
}
