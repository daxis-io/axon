//! HTTP range-read adapter for browser-safe object access over exact HTTP byte ranges.

use query_contract::{ExecutionTarget, QueryError, QueryErrorCode};
use reqwest::header::{CONTENT_LENGTH, CONTENT_RANGE, RANGE};
use reqwest::{StatusCode, Url};

pub const OWNER: &str = "Runtime / engine team";
pub const RESPONSIBILITY: &str = "Browser-safe object reads over HTTP range requests.";

pub fn supported_target() -> ExecutionTarget {
    ExecutionTarget::BrowserWasm
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpByteRange {
    Full,
    Bounded { offset: u64, length: u64 },
    FromOffset { offset: u64 },
    Suffix { length: u64 },
}

impl HttpByteRange {
    fn header_value(self) -> Result<Option<String>, QueryError> {
        match self {
            Self::Full => Ok(None),
            Self::Bounded { offset, length } => {
                if length == 0 {
                    return Err(invalid_request(
                        "bounded byte ranges must request at least one byte",
                    ));
                }

                let end = offset
                    .checked_add(length - 1)
                    .ok_or_else(|| invalid_request("bounded byte range overflowed u64"))?;
                Ok(Some(format!("bytes={offset}-{end}")))
            }
            Self::FromOffset { offset } => Ok(Some(format!("bytes={offset}-"))),
            Self::Suffix { length } => {
                if length == 0 {
                    return Err(invalid_request(
                        "suffix byte ranges must request at least one byte",
                    ));
                }

                Ok(Some(format!("bytes=-{length}")))
            }
        }
    }

    fn expects_partial_response(self) -> bool {
        !matches!(self, Self::Full)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpObjectMetadata {
    pub url: String,
    pub size_bytes: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpRangeReadResult {
    pub metadata: HttpObjectMetadata,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct HttpRangeReader {
    client: reqwest::Client,
}

impl Default for HttpRangeReader {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpRangeReader {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    pub async fn read_range(
        &self,
        url: &str,
        range: HttpByteRange,
    ) -> Result<HttpRangeReadResult, QueryError> {
        let url = parse_url(url)?;
        let range_header = range.header_value()?;

        let mut request = self.client.get(url.clone());
        if let Some(range_header) = &range_header {
            request = request.header(RANGE, range_header);
        }

        let response = request.send().await.map_err(|error| {
            QueryError::new(
                QueryErrorCode::ExecutionFailed,
                format!("http request to '{url}' failed: {error}"),
                supported_target(),
            )
        })?;

        if let Some(error) = map_status_error(response.status(), &url) {
            return Err(error);
        }

        let object_size = if range.expects_partial_response() {
            if response.status() != StatusCode::PARTIAL_CONTENT {
                return Err(protocol_error(format!(
                    "range request to '{url}' expected HTTP 206 Partial Content, got {}",
                    response.status()
                )));
            }

            Some(parse_content_range(response.headers(), &url)?.total_size)
        } else {
            if response.status() != StatusCode::OK {
                return Err(protocol_error(format!(
                    "full-object request to '{url}' expected HTTP 200 OK, got {}",
                    response.status()
                )));
            }

            parse_optional_content_length(response.headers(), &url)?
        };

        let content_range = if range.expects_partial_response() {
            Some(parse_content_range(response.headers(), &url)?)
        } else {
            None
        };

        let bytes = response.bytes().await.map_err(|error| {
            QueryError::new(
                QueryErrorCode::ExecutionFailed,
                format!("http response body from '{url}' could not be read: {error}"),
                supported_target(),
            )
        })?;

        if let Some(content_range) = content_range {
            let expected_length = content_range
                .end
                .checked_sub(content_range.start)
                .and_then(|delta| delta.checked_add(1))
                .ok_or_else(|| protocol_error("content-range length overflowed u64"))?;

            if bytes.len() as u64 != expected_length {
                return Err(protocol_error(format!(
                    "http response body from '{url}' returned {} bytes, but Content-Range declared {expected_length}",
                    bytes.len()
                )));
            }
        }

        Ok(HttpRangeReadResult {
            metadata: HttpObjectMetadata {
                url: url.to_string(),
                size_bytes: object_size.or(Some(bytes.len() as u64)),
            },
            bytes: bytes.to_vec(),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ParsedContentRange {
    start: u64,
    end: u64,
    total_size: u64,
}

fn parse_url(url: &str) -> Result<Url, QueryError> {
    Url::parse(url).map_err(|error| {
        QueryError::new(
            QueryErrorCode::InvalidRequest,
            format!("invalid HTTP object URL '{url}': {error}"),
            supported_target(),
        )
    })
}

fn map_status_error(status: StatusCode, url: &Url) -> Option<QueryError> {
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Some(QueryError::new(
            QueryErrorCode::AccessDenied,
            format!("http request to '{url}' was denied with status {status}"),
            supported_target(),
        )),
        StatusCode::NOT_FOUND | StatusCode::RANGE_NOT_SATISFIABLE => Some(protocol_error(format!(
            "http request to '{url}' failed with status {status}"
        ))),
        status if status.is_client_error() => Some(protocol_error(format!(
            "http request to '{url}' failed with client error status {status}"
        ))),
        status if status.is_server_error() => Some(QueryError::new(
            QueryErrorCode::ExecutionFailed,
            format!("http request to '{url}' failed with server error status {status}"),
            supported_target(),
        )),
        _ => None,
    }
}

fn parse_optional_content_length(
    headers: &reqwest::header::HeaderMap,
    url: &Url,
) -> Result<Option<u64>, QueryError> {
    headers
        .get(CONTENT_LENGTH)
        .map(|value| parse_header_u64(value, CONTENT_LENGTH.as_str(), url))
        .transpose()
}

fn parse_content_range(
    headers: &reqwest::header::HeaderMap,
    url: &Url,
) -> Result<ParsedContentRange, QueryError> {
    let content_range = headers
        .get(CONTENT_RANGE)
        .ok_or_else(|| {
            protocol_error(format!(
                "partial response from '{url}' did not include a Content-Range header"
            ))
        })?
        .to_str()
        .map_err(|error| {
            protocol_error(format!(
                "partial response from '{url}' returned a non-UTF8 Content-Range header: {error}"
            ))
        })?;

    let (unit, spec) = content_range.split_once(' ').ok_or_else(|| {
        protocol_error(format!(
            "partial response from '{url}' returned an invalid Content-Range header: {content_range}"
        ))
    })?;
    if unit != "bytes" {
        return Err(protocol_error(format!(
            "partial response from '{url}' returned unsupported Content-Range units: {content_range}"
        )));
    }

    let (range_spec, total_size) = spec.split_once('/').ok_or_else(|| {
        protocol_error(format!(
            "partial response from '{url}' returned an invalid Content-Range header: {content_range}"
        ))
    })?;

    if range_spec == "*" || total_size == "*" {
        return Err(protocol_error(format!(
            "partial response from '{url}' returned an unsatisfied Content-Range header: {content_range}"
        )));
    }

    let (start, end) = range_spec.split_once('-').ok_or_else(|| {
        protocol_error(format!(
            "partial response from '{url}' returned an invalid Content-Range header: {content_range}"
        ))
    })?;

    let start = start.parse::<u64>().map_err(|error| {
        protocol_error(format!(
            "partial response from '{url}' returned an invalid Content-Range start: {error}"
        ))
    })?;
    let end = end.parse::<u64>().map_err(|error| {
        protocol_error(format!(
            "partial response from '{url}' returned an invalid Content-Range end: {error}"
        ))
    })?;
    let total_size = total_size.parse::<u64>().map_err(|error| {
        protocol_error(format!(
            "partial response from '{url}' returned an invalid Content-Range size: {error}"
        ))
    })?;

    if end < start {
        return Err(protocol_error(format!(
            "partial response from '{url}' returned a descending Content-Range: {content_range}"
        )));
    }
    if end >= total_size {
        return Err(protocol_error(format!(
            "partial response from '{url}' returned an out-of-bounds Content-Range: {content_range}"
        )));
    }

    Ok(ParsedContentRange {
        start,
        end,
        total_size,
    })
}

fn parse_header_u64(
    value: &reqwest::header::HeaderValue,
    header_name: &str,
    url: &Url,
) -> Result<u64, QueryError> {
    let text = value.to_str().map_err(|error| {
        protocol_error(format!(
            "response from '{url}' returned a non-UTF8 {header_name} header: {error}"
        ))
    })?;

    text.parse::<u64>().map_err(|error| {
        protocol_error(format!(
            "response from '{url}' returned an invalid {header_name} header: {error}"
        ))
    })
}

fn invalid_request(message: impl Into<String>) -> QueryError {
    QueryError::new(QueryErrorCode::InvalidRequest, message, supported_target())
}

fn protocol_error(message: impl Into<String>) -> QueryError {
    QueryError::new(
        QueryErrorCode::ObjectStoreProtocol,
        message,
        supported_target(),
    )
}
