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

    fn validate_content_range(
        self,
        content_range: ParsedContentRange,
        display_url: &str,
    ) -> Result<(), QueryError> {
        match self {
            Self::Full => Ok(()),
            Self::Bounded { offset, length } => {
                let expected_end = offset
                    .checked_add(length - 1)
                    .ok_or_else(|| protocol_error("bounded byte range overflowed u64"))?;
                if content_range.start != offset || content_range.end != expected_end {
                    return Err(protocol_error(format!(
                        "partial response from '{display_url}' returned bytes {}-{}, but the request asked for bytes {offset}-{expected_end}",
                        content_range.start, content_range.end
                    )));
                }
                Ok(())
            }
            Self::FromOffset { offset } => {
                let expected_end = content_range.total_size.checked_sub(1).ok_or_else(|| {
                    protocol_error("content-range total size must be at least one byte")
                })?;
                if content_range.start != offset || content_range.end != expected_end {
                    return Err(protocol_error(format!(
                        "partial response from '{display_url}' returned bytes {}-{}, but the request asked for bytes {offset}-",
                        content_range.start, content_range.end
                    )));
                }
                Ok(())
            }
            Self::Suffix { length } => {
                let expected_start = content_range.total_size.saturating_sub(length);
                let expected_end = content_range.total_size.checked_sub(1).ok_or_else(|| {
                    protocol_error("content-range total size must be at least one byte")
                })?;
                if content_range.start != expected_start || content_range.end != expected_end {
                    return Err(protocol_error(format!(
                        "partial response from '{display_url}' returned bytes {}-{}, but the request asked for the final {length} bytes",
                        content_range.start, content_range.end
                    )));
                }
                Ok(())
            }
        }
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
        Self::with_client(reqwest::Client::new())
    }

    pub fn with_client(client: reqwest::Client) -> Self {
        Self { client }
    }

    pub async fn read_range(
        &self,
        url: &str,
        range: HttpByteRange,
    ) -> Result<HttpRangeReadResult, QueryError> {
        let url = parse_url(url)?;
        let display_url = redacted_url(&url);
        let range_header = range.header_value()?;

        let mut request = self.client.get(url.clone());
        if let Some(range_header) = &range_header {
            request = request.header(RANGE, range_header);
        }

        let response = request.send().await.map_err(|error| {
            QueryError::new(
                QueryErrorCode::ExecutionFailed,
                format!("http request to '{display_url}' failed: {error}"),
                supported_target(),
            )
        })?;

        if let Some(error) = map_status_error(response.status(), &display_url) {
            return Err(error);
        }

        let content_range = if range.expects_partial_response() {
            if response.status() != StatusCode::PARTIAL_CONTENT {
                return Err(protocol_error(format!(
                    "range request to '{display_url}' expected HTTP 206 Partial Content, got {}",
                    response.status()
                )));
            }

            let content_range = parse_content_range(response.headers(), &display_url)?;
            range.validate_content_range(content_range, &display_url)?;
            Some(content_range)
        } else {
            if response.status() != StatusCode::OK {
                return Err(protocol_error(format!(
                    "full-object request to '{display_url}' expected HTTP 200 OK, got {}",
                    response.status()
                )));
            }

            None
        };
        let object_size = match content_range {
            Some(content_range) => Some(content_range.total_size),
            None => parse_optional_content_length(response.headers(), &display_url)?,
        };

        let bytes = response.bytes().await.map_err(|error| {
            QueryError::new(
                QueryErrorCode::ExecutionFailed,
                format!("http response body from '{display_url}' could not be read: {error}"),
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
                    "http response body from '{display_url}' returned {} bytes, but Content-Range declared {expected_length}",
                    bytes.len()
                )));
            }
        }

        Ok(HttpRangeReadResult {
            metadata: HttpObjectMetadata {
                url: display_url,
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
    let display_url = redacted_input_url(url);
    let parsed = Url::parse(url).map_err(|error| {
        QueryError::new(
            QueryErrorCode::InvalidRequest,
            format!("invalid HTTP object URL '{display_url}': {error}"),
            supported_target(),
        )
    })?;

    match parsed.scheme() {
        "http" | "https" => Ok(parsed),
        scheme => Err(invalid_request(format!(
            "invalid HTTP object URL '{}': unsupported scheme '{scheme}'",
            redacted_url(&parsed)
        ))),
    }
}

fn map_status_error(status: StatusCode, display_url: &str) -> Option<QueryError> {
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Some(QueryError::new(
            QueryErrorCode::AccessDenied,
            format!("http request to '{display_url}' was denied with status {status}"),
            supported_target(),
        )),
        StatusCode::NOT_FOUND | StatusCode::RANGE_NOT_SATISFIABLE => Some(protocol_error(format!(
            "http request to '{display_url}' failed with status {status}"
        ))),
        status if status.is_client_error() => Some(protocol_error(format!(
            "http request to '{display_url}' failed with client error status {status}"
        ))),
        status if status.is_server_error() => Some(QueryError::new(
            QueryErrorCode::ExecutionFailed,
            format!("http request to '{display_url}' failed with server error status {status}"),
            supported_target(),
        )),
        _ => None,
    }
}

fn parse_optional_content_length(
    headers: &reqwest::header::HeaderMap,
    display_url: &str,
) -> Result<Option<u64>, QueryError> {
    headers
        .get(CONTENT_LENGTH)
        .map(|value| parse_header_u64(value, CONTENT_LENGTH.as_str(), display_url))
        .transpose()
}

fn parse_content_range(
    headers: &reqwest::header::HeaderMap,
    display_url: &str,
) -> Result<ParsedContentRange, QueryError> {
    let content_range = headers
        .get(CONTENT_RANGE)
        .ok_or_else(|| {
            protocol_error(format!(
                "partial response from '{display_url}' did not include a Content-Range header"
            ))
        })?
        .to_str()
        .map_err(|error| {
            protocol_error(format!(
                "partial response from '{display_url}' returned a non-UTF8 Content-Range header: {error}"
            ))
        })?;

    let (unit, spec) = content_range.split_once(' ').ok_or_else(|| {
        protocol_error(format!(
            "partial response from '{display_url}' returned an invalid Content-Range header: {content_range}"
        ))
    })?;
    if unit != "bytes" {
        return Err(protocol_error(format!(
            "partial response from '{display_url}' returned unsupported Content-Range units: {content_range}"
        )));
    }

    let (range_spec, total_size) = spec.split_once('/').ok_or_else(|| {
        protocol_error(format!(
            "partial response from '{display_url}' returned an invalid Content-Range header: {content_range}"
        ))
    })?;

    if range_spec == "*" || total_size == "*" {
        return Err(protocol_error(format!(
            "partial response from '{display_url}' returned an unsatisfied Content-Range header: {content_range}"
        )));
    }

    let (start, end) = range_spec.split_once('-').ok_or_else(|| {
        protocol_error(format!(
            "partial response from '{display_url}' returned an invalid Content-Range header: {content_range}"
        ))
    })?;

    let start = start.parse::<u64>().map_err(|error| {
        protocol_error(format!(
            "partial response from '{display_url}' returned an invalid Content-Range start: {error}"
        ))
    })?;
    let end = end.parse::<u64>().map_err(|error| {
        protocol_error(format!(
            "partial response from '{display_url}' returned an invalid Content-Range end: {error}"
        ))
    })?;
    let total_size = total_size.parse::<u64>().map_err(|error| {
        protocol_error(format!(
            "partial response from '{display_url}' returned an invalid Content-Range size: {error}"
        ))
    })?;

    if end < start {
        return Err(protocol_error(format!(
            "partial response from '{display_url}' returned a descending Content-Range: {content_range}"
        )));
    }
    if end >= total_size {
        return Err(protocol_error(format!(
            "partial response from '{display_url}' returned an out-of-bounds Content-Range: {content_range}"
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
    display_url: &str,
) -> Result<u64, QueryError> {
    let text = value.to_str().map_err(|error| {
        protocol_error(format!(
            "response from '{display_url}' returned a non-UTF8 {header_name} header: {error}"
        ))
    })?;

    text.parse::<u64>().map_err(|error| {
        protocol_error(format!(
            "response from '{display_url}' returned an invalid {header_name} header: {error}"
        ))
    })
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
