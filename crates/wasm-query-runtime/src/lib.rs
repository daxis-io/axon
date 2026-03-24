//! Constrained browser runtime envelope for future DataFusion WASM integration.

use std::net::IpAddr;
use std::time::Duration;

use query_contract::{
    CapabilityKey, CapabilityState, ExecutionTarget, FallbackReason, QueryError, QueryErrorCode,
};
use reqwest::Url;
use wasm_http_object_store::{HttpByteRange, HttpRangeReadResult, HttpRangeReader};

pub const OWNER: &str = "Runtime / engine team";
pub const RESPONSIBILITY: &str = "Browser-safe query execution for the supported SQL envelope.";
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;

pub fn runtime_target() -> ExecutionTarget {
    ExecutionTarget::BrowserWasm
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum BrowserObjectAccessMode {
    #[default]
    BrowserSafeHttp,
    CloudObjectStore,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserRuntimeConfig {
    pub target_partitions: usize,
    pub object_access_mode: BrowserObjectAccessMode,
    pub allow_cloud_credentials: bool,
    pub request_timeout_ms: u64,
}

impl Default for BrowserRuntimeConfig {
    fn default() -> Self {
        Self {
            target_partitions: 1,
            object_access_mode: BrowserObjectAccessMode::BrowserSafeHttp,
            allow_cloud_credentials: false,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
        }
    }
}

impl BrowserRuntimeConfig {
    pub fn validate(&self) -> Result<(), QueryError> {
        if self.request_timeout_ms == 0 {
            return Err(QueryError::new(
                QueryErrorCode::InvalidRequest,
                "browser runtime request timeout must be at least 1 ms",
                runtime_target(),
            ));
        }

        if self.allow_cloud_credentials {
            return Err(QueryError::new(
                QueryErrorCode::SecurityPolicyViolation,
                "browser runtime must not accept cloud credentials",
                runtime_target(),
            ));
        }

        if self.target_partitions != 1 {
            return Err(QueryError::new(
                QueryErrorCode::FallbackRequired,
                format!(
                    "browser runtime only supports single-partition execution; received {} partition(s)",
                    self.target_partitions
                ),
                runtime_target(),
            )
            .with_fallback_reason(FallbackReason::CapabilityGate {
                capability: CapabilityKey::MultiPartitionExecution,
                required_state: CapabilityState::NativeOnly,
            }));
        }

        if self.object_access_mode != BrowserObjectAccessMode::BrowserSafeHttp {
            return Err(QueryError::new(
                QueryErrorCode::FallbackRequired,
                "browser runtime only supports browser-safe HTTP object access",
                runtime_target(),
            )
            .with_fallback_reason(FallbackReason::BrowserRuntimeConstraint));
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserObjectSource {
    url: String,
}

impl BrowserObjectSource {
    pub fn from_url(url: impl Into<String>) -> Result<Self, QueryError> {
        let url = url.into();
        validate_source_url(&url)?;

        Ok(Self { url })
    }

    pub fn url(&self) -> &str {
        &self.url
    }
}

#[derive(Clone, Debug)]
pub struct BrowserRuntimeSession {
    config: BrowserRuntimeConfig,
    reader: HttpRangeReader,
}

impl BrowserRuntimeSession {
    pub fn new(config: BrowserRuntimeConfig) -> Result<Self, QueryError> {
        Self::with_reader(config, HttpRangeReader::new())
    }

    pub fn with_reader(
        config: BrowserRuntimeConfig,
        reader: HttpRangeReader,
    ) -> Result<Self, QueryError> {
        config.validate()?;

        Ok(Self { config, reader })
    }

    pub fn config(&self) -> &BrowserRuntimeConfig {
        &self.config
    }

    pub async fn probe(
        &self,
        source: &BrowserObjectSource,
        range: HttpByteRange,
    ) -> Result<HttpRangeReadResult, QueryError> {
        self.reader
            .read_range_with_timeout(
                source.url(),
                range,
                Some(Duration::from_millis(self.config.request_timeout_ms)),
            )
            .await
    }
}

fn validate_source_url(url: &str) -> Result<(), QueryError> {
    let parsed = Url::parse(url).map_err(|error| {
        QueryError::new(
            QueryErrorCode::InvalidRequest,
            format!(
                "invalid browser object URL '{}': {error}",
                redacted_input_url(url)
            ),
            runtime_target(),
        )
    })?;

    if parsed.scheme() == "http" && !allows_loopback_http_for_host_tests(&parsed) {
        return Err(QueryError::new(
            QueryErrorCode::SecurityPolicyViolation,
            format!(
                "browser runtime only permits HTTPS object URLs; plain HTTP is reserved for loopback host-side tests: '{}'",
                redacted_url(&parsed)
            ),
            runtime_target(),
        ));
    }

    Ok(())
}

fn allows_loopback_http_for_host_tests(url: &Url) -> bool {
    cfg!(not(target_arch = "wasm32"))
        && match url.host_str() {
            Some("localhost") => true,
            Some(host) => host
                .parse::<IpAddr>()
                .map(|address| address.is_loopback())
                .unwrap_or(false),
            None => false,
        }
}

fn redacted_input_url(url: &str) -> String {
    let end = url.find(['?', '#']).unwrap_or(url.len());
    url[..end].to_string()
}

fn redacted_url(url: &Url) -> String {
    let mut redacted = url.clone();
    let _ = redacted.set_username("");
    let _ = redacted.set_password(None);
    redacted.set_query(None);
    redacted.set_fragment(None);
    redacted.to_string()
}
