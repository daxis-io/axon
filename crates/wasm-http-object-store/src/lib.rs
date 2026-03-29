//! HTTP range-read adapter for browser-safe object access over exact HTTP byte ranges.

use std::time::Duration;

use bytes::Bytes;
use query_contract::{ExecutionTarget, QueryError, QueryErrorCode};
use reqwest::header::{CONTENT_LENGTH, CONTENT_RANGE, ETAG, IF_RANGE, RANGE};
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
    pub etag: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpRangeReadResult {
    pub metadata: HttpObjectMetadata,
    pub bytes: Bytes,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HttpMetadataProbeRequirements {
    pub require_size: bool,
    pub require_etag: bool,
}

impl HttpMetadataProbeRequirements {
    pub fn is_satisfied_by(self, metadata: &HttpObjectMetadata) -> bool {
        (!self.require_size || metadata.size_bytes.is_some())
            && (!self.require_etag || metadata.etag.is_some())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HttpRangeValidation {
    IfRangeEtag(String),
}

impl HttpRangeValidation {
    pub fn if_range_etag(etag: String) -> Self {
        Self::IfRangeEtag(etag)
    }

    fn apply_request(
        self,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder, QueryError> {
        match self {
            Self::IfRangeEtag(etag) => {
                if etag.trim().is_empty() {
                    return Err(invalid_request("if-range validators must not be empty"));
                }
                Ok(request.header(IF_RANGE, etag))
            }
        }
    }

    fn validate_response_identity(
        self,
        response_etag: Option<&str>,
        display_url: &str,
    ) -> Result<(), QueryError> {
        match self {
            Self::IfRangeEtag(expected) => match response_etag {
                Some(actual) if actual == expected => Ok(()),
                Some(actual) => Err(protocol_error(format!(
                    "range validation for '{display_url}' expected ETag {expected}, but the response returned {actual}"
                ))),
                None => Err(protocol_error(format!(
                    "range validation for '{display_url}' required an exposed ETag header, but none was returned"
                ))),
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ByteExtent {
    pub offset: u64,
    pub length: u64,
}

impl ByteExtent {
    pub fn new(offset: u64, length: u64) -> Result<Self, QueryError> {
        if length == 0 {
            return Err(invalid_request(
                "byte extents must request at least one byte",
            ));
        }

        let _ = offset
            .checked_add(length)
            .ok_or_else(|| invalid_request("byte extent overflowed u64"))?;

        Ok(Self { offset, length })
    }

    fn end_exclusive(self) -> Result<u64, QueryError> {
        self.offset
            .checked_add(self.length)
            .ok_or_else(|| protocol_error("byte extent overflowed u64"))
    }

    fn merge(self, other: Self) -> Result<Self, QueryError> {
        let start = self.offset.min(other.offset);
        let end = self.end_exclusive()?.max(other.end_exclusive()?);
        Self::new(start, end - start)
    }

    pub fn contains(self, other: Self) -> bool {
        match (self.end_exclusive(), other.end_exclusive()) {
            (Ok(self_end), Ok(other_end)) => self.offset <= other.offset && self_end >= other_end,
            _ => false,
        }
    }

    fn touches_or_overlaps(self, other: Self) -> bool {
        match (self.end_exclusive(), other.end_exclusive()) {
            (Ok(self_end), Ok(other_end)) => self_end >= other.offset && other_end >= self.offset,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ExtentCacheKey {
    pub resource: String,
    pub identity: Option<String>,
}

impl ExtentCacheKey {
    pub fn new(resource: impl Into<String>, identity: Option<String>) -> Self {
        Self {
            resource: resource.into(),
            identity,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtentCacheEntry {
    pub key: ExtentCacheKey,
    pub extent: ByteExtent,
    pub bytes: Bytes,
}

impl ExtentCacheEntry {
    pub fn new(key: ExtentCacheKey, extent: ByteExtent, bytes: Bytes) -> Result<Self, QueryError> {
        if extent.length == 0 {
            return Err(invalid_request(
                "cached extents must request at least one byte",
            ));
        }
        let _ = extent
            .offset
            .checked_add(extent.length)
            .ok_or_else(|| invalid_request("cached extent overflowed u64"))?;

        let expected_len = usize::try_from(extent.length)
            .map_err(|_| invalid_request("byte extent length overflowed usize"))?;
        if bytes.len() != expected_len {
            return Err(invalid_request(format!(
                "cached extent length mismatch: extent declared {} bytes, but payload had {} bytes",
                extent.length,
                bytes.len()
            )));
        }

        Ok(Self { key, extent, bytes })
    }

    pub fn can_satisfy(&self, request_key: &ExtentCacheKey, request_extent: ByteExtent) -> bool {
        &self.key == request_key && self.extent.contains(request_extent)
    }
}

pub fn coalesce_extents(mut extents: Vec<ByteExtent>) -> Result<Vec<ByteExtent>, QueryError> {
    if extents.is_empty() {
        return Ok(Vec::new());
    }

    extents.sort_by(|left, right| {
        left.offset
            .cmp(&right.offset)
            .then(left.length.cmp(&right.length))
    });

    let mut coalesced = Vec::with_capacity(extents.len());
    let mut current = extents[0];
    for extent in extents.into_iter().skip(1) {
        if current.touches_or_overlaps(extent) {
            current = current.merge(extent)?;
            continue;
        }
        coalesced.push(current);
        current = extent;
    }
    coalesced.push(current);

    Ok(coalesced)
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
        self.read_range_with_timeout(url, range, None).await
    }

    pub async fn read_range_with_timeout(
        &self,
        url: &str,
        range: HttpByteRange,
        timeout: Option<Duration>,
    ) -> Result<HttpRangeReadResult, QueryError> {
        self.read_range_with_validation(url, range, None, timeout)
            .await
    }

    pub async fn read_range_with_validation(
        &self,
        url: &str,
        range: HttpByteRange,
        validation: Option<HttpRangeValidation>,
        timeout: Option<Duration>,
    ) -> Result<HttpRangeReadResult, QueryError> {
        let url = parse_url(url)?;
        let display_url = redacted_url(&url);
        let range_header = range.header_value()?;

        let mut request = self.client.get(url.clone());
        if let Some(range_header) = &range_header {
            request = request.header(RANGE, range_header);
        }
        if let Some(validation) = validation.clone() {
            request = validation.apply_request(request)?;
        }
        if let Some(timeout) = timeout {
            request = request.timeout(timeout);
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
                if validation.is_some() {
                    return Err(protocol_error(format!(
                        "range request to '{display_url}' expected HTTP 206 Partial Content, got {}; If-Range validation likely failed due object identity drift",
                        response.status()
                    )));
                }
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
        let etag = parse_optional_header_string(response.headers(), ETAG.as_str(), &display_url)?;
        if let Some(validation) = validation {
            validation.validate_response_identity(etag.as_deref(), &display_url)?;
        }

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
                etag,
            },
            bytes,
        })
    }

    pub async fn probe_metadata(
        &self,
        url: &str,
        requirements: HttpMetadataProbeRequirements,
    ) -> Result<HttpObjectMetadata, QueryError> {
        self.probe_metadata_with_timeout(url, requirements, None)
            .await
    }

    pub async fn probe_metadata_with_timeout(
        &self,
        url: &str,
        requirements: HttpMetadataProbeRequirements,
        timeout: Option<Duration>,
    ) -> Result<HttpObjectMetadata, QueryError> {
        let url = parse_url(url)?;
        let display_url = redacted_url(&url);
        let mut request = self.client.get(url.clone()).header(RANGE, "bytes=0-0");
        if let Some(timeout) = timeout {
            request = request.timeout(timeout);
        }
        let response = request.send().await.map_err(|error| {
            QueryError::new(
                QueryErrorCode::ExecutionFailed,
                format!("http request to '{display_url}' failed: {error}"),
                supported_target(),
            )
        })?;

        if response.status() == StatusCode::RANGE_NOT_SATISFIABLE {
            let unsatisfied_size =
                parse_unsatisfied_content_range_total_size(response.headers(), &display_url)?;
            if unsatisfied_size == Some(0) {
                let etag =
                    parse_optional_header_string(response.headers(), ETAG.as_str(), &display_url)?;
                let metadata = HttpObjectMetadata {
                    url: display_url,
                    size_bytes: Some(0),
                    etag,
                };
                validate_metadata_probe_requirements(&metadata, requirements)?;
                return Ok(metadata);
            }
        }

        if let Some(error) = map_status_error(response.status(), &display_url) {
            return Err(error);
        }
        if response.status() != StatusCode::PARTIAL_CONTENT {
            return Err(protocol_error(format!(
                "metadata probe to '{display_url}' expected HTTP 206 Partial Content, got {}",
                response.status()
            )));
        }

        let content_range = parse_content_range(response.headers(), &display_url)?;
        HttpByteRange::Bounded {
            offset: 0,
            length: 1,
        }
        .validate_content_range(content_range, &display_url)?;
        let etag = parse_optional_header_string(response.headers(), ETAG.as_str(), &display_url)?;
        let bytes = response.bytes().await.map_err(|error| {
            QueryError::new(
                QueryErrorCode::ExecutionFailed,
                format!("http response body from '{display_url}' could not be read: {error}"),
                supported_target(),
            )
        })?;
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

        let metadata = HttpObjectMetadata {
            url: display_url,
            size_bytes: Some(content_range.total_size),
            etag,
        };
        validate_metadata_probe_requirements(&metadata, requirements)?;

        Ok(metadata)
    }

    pub async fn resolve_metadata(
        &self,
        url: &str,
        known_metadata: Option<HttpObjectMetadata>,
        requirements: HttpMetadataProbeRequirements,
    ) -> Result<HttpObjectMetadata, QueryError> {
        self.resolve_metadata_with_timeout(url, known_metadata, requirements, None)
            .await
    }

    pub async fn resolve_metadata_with_timeout(
        &self,
        url: &str,
        known_metadata: Option<HttpObjectMetadata>,
        requirements: HttpMetadataProbeRequirements,
        timeout: Option<Duration>,
    ) -> Result<HttpObjectMetadata, QueryError> {
        let requested_url = parse_url(url)?;
        let requested_display_url = redacted_url(&requested_url);
        if let Some(known_metadata) = known_metadata {
            if known_metadata.url == requested_display_url
                && requirements.is_satisfied_by(&known_metadata)
            {
                return Ok(known_metadata);
            }
        }

        self.probe_metadata_with_timeout(url, requirements, timeout)
            .await
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

fn parse_unsatisfied_content_range_total_size(
    headers: &reqwest::header::HeaderMap,
    display_url: &str,
) -> Result<Option<u64>, QueryError> {
    let Some(content_range) = headers.get(CONTENT_RANGE) else {
        return Ok(None);
    };
    let content_range = content_range.to_str().map_err(|error| {
        protocol_error(format!(
            "response from '{display_url}' returned a non-UTF8 Content-Range header: {error}"
        ))
    })?;

    let (unit, spec) = content_range.split_once(' ').ok_or_else(|| {
        protocol_error(format!(
            "response from '{display_url}' returned an invalid Content-Range header: {content_range}"
        ))
    })?;
    if unit != "bytes" {
        return Err(protocol_error(format!(
            "response from '{display_url}' returned unsupported Content-Range units: {content_range}"
        )));
    }
    let (range_spec, total_size) = spec.split_once('/').ok_or_else(|| {
        protocol_error(format!(
            "response from '{display_url}' returned an invalid Content-Range header: {content_range}"
        ))
    })?;
    if range_spec != "*" {
        return Ok(None);
    }
    if total_size == "*" {
        return Err(protocol_error(format!(
            "response from '{display_url}' returned an invalid unsatisfied Content-Range header: {content_range}"
        )));
    }

    let total_size = total_size.parse::<u64>().map_err(|error| {
        protocol_error(format!(
            "response from '{display_url}' returned an invalid Content-Range size: {error}"
        ))
    })?;
    Ok(Some(total_size))
}

fn validate_metadata_probe_requirements(
    metadata: &HttpObjectMetadata,
    requirements: HttpMetadataProbeRequirements,
) -> Result<(), QueryError> {
    if requirements.require_size && metadata.size_bytes.is_none() {
        return Err(protocol_error(format!(
            "metadata probe for '{}' required object size metadata, but no browser-visible size headers were returned",
            metadata.url
        )));
    }
    if requirements.require_etag && metadata.etag.is_none() {
        return Err(protocol_error(format!(
            "metadata probe for '{}' required an exposed ETag header, but none was returned",
            metadata.url
        )));
    }

    Ok(())
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

fn parse_optional_header_string(
    headers: &reqwest::header::HeaderMap,
    header_name: &str,
    display_url: &str,
) -> Result<Option<String>, QueryError> {
    headers
        .get(header_name)
        .map(|value| {
            value.to_str().map(|text| text.to_string()).map_err(|error| {
                protocol_error(format!(
                    "response from '{display_url}' returned a non-UTF8 {header_name} header: {error}"
                ))
            })
        })
        .transpose()
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
