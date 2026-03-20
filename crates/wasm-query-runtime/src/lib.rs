//! Browser-oriented runtime skeleton for supported DataFusion WASM queries.

use query_contract::ExecutionTarget;

pub const OWNER: &str = "Runtime / engine team";
pub const RESPONSIBILITY: &str = "Browser-safe query execution for the supported SQL envelope.";

pub fn runtime_target() -> ExecutionTarget {
    ExecutionTarget::BrowserWasm
}
