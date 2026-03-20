# EPIC-02: Native Delta-on-GCS Reference Runtime

- Type: Epic
- Accountable: Query platform lead
- Delivery DRI: Runtime / engine team
- Depends on: EPIC-01
- Milestone: `M1`

## Goal

Build the authoritative native read path using `deltalake_core` + GCS + DataFusion.

## Packages In Scope

- `crates/native-query-runtime`
- `tests/conformance`
- `tests/perf`

## Deliverables

- open Delta tables from GCS
- register the object store into the DataFusion session
- execute a read-only SQL corpus
- emit explain plans and execution metrics
- expose a native fallback API

## Child Issues

1. Implement table load from a GCS Delta URI.
2. Implement session preparation via Delta object-store registration.
3. Implement the SQL execution API.
4. Implement capability detection for advanced table features.
5. Implement native metrics export.
6. Create GCS fixture tables.
7. Add time-travel smoke tests.
8. Add failure-path tests for missing files, stale snapshots, and access errors.

## Acceptance Criteria

- runs representative read-only SQL against at least one real Delta table in GCS
- compares results across at least 25 fixture queries
- advanced or unsupported table features return explicit capability results
- metrics include bytes read, files scanned, files skipped, and duration
- the native path is callable independently of browser code

## Definition Of Done

The native runtime is fit to serve as oracle and fallback.
