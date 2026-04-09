//! Route selection and fallback recording for query execution.

use query_contract::{ExecutionTarget, FallbackReason, QueryError, QueryRequest};

pub const OWNER: &str = "Runtime / engine team";
pub const RESPONSIBILITY: &str = "Choose browser vs native execution and record fallback reasons.";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouteDecision {
    pub target: ExecutionTarget,
    pub fallback_reason: Option<FallbackReason>,
}

pub fn default_target() -> ExecutionTarget {
    ExecutionTarget::Native
}

pub fn initial_route(request: &QueryRequest) -> RouteDecision {
    RouteDecision {
        target: request.preferred_target,
        fallback_reason: None,
    }
}

pub fn reroute_browser_failure(
    request: &QueryRequest,
    error: QueryError,
) -> Result<RouteDecision, QueryError> {
    if request.preferred_target == ExecutionTarget::BrowserWasm && error.requires_fallback() {
        return Ok(RouteDecision {
            target: ExecutionTarget::Native,
            fallback_reason: error.fallback_reason,
        });
    }

    Err(error)
}

#[cfg(test)]
mod tests {
    use query_contract::{ExecutionTarget, FallbackReason, QueryError, QueryErrorCode};

    use super::{initial_route, reroute_browser_failure, RouteDecision};

    #[test]
    fn initial_route_uses_the_request_preference() {
        let request = query_contract::QueryRequest::new(
            "gs://axon-fixtures/table",
            "SELECT id FROM axon_table",
            ExecutionTarget::BrowserWasm,
        );

        assert_eq!(
            initial_route(&request),
            RouteDecision {
                target: ExecutionTarget::BrowserWasm,
                fallback_reason: None,
            }
        );
    }

    #[test]
    fn reroute_browser_failure_preserves_structured_fallback_reason() {
        let request = query_contract::QueryRequest::new(
            "gs://axon-fixtures/table",
            "SELECT id FROM axon_table",
            ExecutionTarget::BrowserWasm,
        );
        let error = QueryError::new(
            QueryErrorCode::FallbackRequired,
            "browser runtime requires native fallback",
            ExecutionTarget::BrowserWasm,
        )
        .with_fallback_reason(FallbackReason::NativeRequired);

        assert_eq!(
            reroute_browser_failure(&request, error).expect("fallback errors should reroute"),
            RouteDecision {
                target: ExecutionTarget::Native,
                fallback_reason: Some(FallbackReason::NativeRequired),
            }
        );
    }

    #[test]
    fn reroute_browser_failure_preserves_capability_gate_fallback_reason() {
        let request = query_contract::QueryRequest::new(
            "gs://axon-fixtures/table",
            "SELECT id FROM axon_table",
            ExecutionTarget::BrowserWasm,
        );
        let error = QueryError::new(
            QueryErrorCode::FallbackRequired,
            "browser runtime hit a capability gate",
            ExecutionTarget::BrowserWasm,
        )
        .with_fallback_reason(FallbackReason::CapabilityGate {
            capability: query_contract::CapabilityKey::SignedUrlAccess,
            required_state: query_contract::CapabilityState::Supported,
        });

        assert_eq!(
            reroute_browser_failure(&request, error).expect("fallback errors should reroute"),
            RouteDecision {
                target: ExecutionTarget::Native,
                fallback_reason: Some(FallbackReason::CapabilityGate {
                    capability: query_contract::CapabilityKey::SignedUrlAccess,
                    required_state: query_contract::CapabilityState::Supported,
                }),
            }
        );
    }

    #[test]
    fn reroute_browser_failure_preserves_browser_runtime_constraint_fallback_reason() {
        let request = query_contract::QueryRequest::new(
            "gs://axon-fixtures/table",
            "SELECT id FROM axon_table",
            ExecutionTarget::BrowserWasm,
        );
        let error = QueryError::new(
            QueryErrorCode::FallbackRequired,
            "browser runtime exceeded the configured execution budget",
            ExecutionTarget::BrowserWasm,
        )
        .with_fallback_reason(FallbackReason::BrowserRuntimeConstraint);

        assert_eq!(
            reroute_browser_failure(&request, error).expect("fallback errors should reroute"),
            RouteDecision {
                target: ExecutionTarget::Native,
                fallback_reason: Some(FallbackReason::BrowserRuntimeConstraint),
            }
        );
    }

    #[test]
    fn reroute_browser_failure_keeps_non_fallback_errors_terminal() {
        let request = query_contract::QueryRequest::new(
            "gs://axon-fixtures/table",
            "SELECT id FROM axon_table",
            ExecutionTarget::BrowserWasm,
        );
        let error = QueryError::new(
            QueryErrorCode::InvalidRequest,
            "bad query",
            ExecutionTarget::BrowserWasm,
        );

        let returned = reroute_browser_failure(&request, error.clone())
            .expect_err("non-fallback errors should remain terminal");
        assert_eq!(returned, error);
    }
}
