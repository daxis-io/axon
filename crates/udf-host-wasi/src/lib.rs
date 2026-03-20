//! Hosted WASI UDF runtime skeleton.

use query_contract::ExecutionTarget;

pub const OWNER: &str = "Runtime / engine team";
pub const RESPONSIBILITY: &str = "Execute hosted UDFs using a separate ABI/runtime track.";

pub fn host_target() -> ExecutionTarget {
    ExecutionTarget::Native
}
