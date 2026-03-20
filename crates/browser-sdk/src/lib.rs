//! Browser SDK skeleton that will eventually expose the public query session API.

use query_contract::ExecutionTarget;

pub const OWNER: &str = "Web platform team";
pub const RESPONSIBILITY: &str = "Public browser-facing SDK surface for query execution.";

pub fn preferred_target() -> ExecutionTarget {
    ExecutionTarget::BrowserWasm
}
