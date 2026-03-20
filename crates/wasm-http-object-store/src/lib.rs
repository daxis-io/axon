//! HTTP range-read adapter skeleton for signed URL and proxy-backed browser object access.

use query_contract::ExecutionTarget;

pub const OWNER: &str = "Runtime / engine team";
pub const RESPONSIBILITY: &str = "Browser-safe object reads over HTTP range requests.";

pub fn supported_target() -> ExecutionTarget {
    ExecutionTarget::BrowserWasm
}
