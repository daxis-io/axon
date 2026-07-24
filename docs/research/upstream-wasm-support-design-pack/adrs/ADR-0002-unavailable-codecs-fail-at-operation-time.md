# ADR-0002: Unavailable Codecs Fail At Operation Time

- Status: Proposed
- Date: 2026-07-23
- Scope: Parquet and Arrow IPC codecs

## Decision

A logical codec feature may compile on a target without an implementation. The crate returns a
target-unavailable error at the first codec-instantiation point for the operation.

The failure occurs before compressed output is committed or compressed input is decoded.

The error distinguishes:

- feature disabled;
- feature enabled with a supported backend;
- feature enabled with no backend for the target.

## Context

A file can expose useful metadata before the application requests compressed pages or record
batches. Compile failure prevents schema inspection, column selection, and a precise application
fallback.

Existing writer builders include infallible methods. Requiring configuration-time failure would
force unrelated public API changes.

## Consequences

- Parquet footer and schema inspection can succeed for zstd files on WASM.
- Applications receive a codec, operation, and target error.
- Writers validate before committing compressed bytes.
- Tests need fixtures that separate metadata from compressed data.
- Documentation must state that feature enablement does not guarantee a backend on every target.

## Rejected Options

### `compile_error!`

Feature unification would make an otherwise usable WASM graph fail before the application selects a
codec or column.

### Silent fallback to uncompressed output

The result would violate the requested file or IPC format and could corrupt interoperability.

### Change every builder to return `Result`

The first implementation can preserve public APIs and fail at codec construction or first use.

## References

- [Arrow and Parquet codec architecture](../docs/03-arrow-parquet-codec-architecture.md)
