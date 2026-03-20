# ADR-0005: Read-Only MVP First, With Explicit Delta Compatibility Policy

- Status: Proposed
- Date: 2026-03-20
- Decision owner: Runtime / engine team
- Delivery DRI: Storage platform team
- Approver: Executive sponsor

## Decision

Start with a read-only MVP:

- projection
- filters
- aggregates
- limited joins inside tested bounds
- explain and metrics
- time travel only after base snapshot reads are stable

MVP non-goals:

- writes
- browser-side Delta commits
- browser-side credentialed `gs://` discovery
- silent support for all Delta features

## Compatibility Policy

Every advanced Delta feature must be classified as one of:

- supported
- supported only in native runtime
- unsupported / hard fail
- experimental / feature-flagged

## Deletion Vectors

Deletion-vector support should not be assumed from a single doc page or a single release note. Official best-practice docs still warn about a lack of support, while more recent 2026 release notes and current bindings or code indicate support has evolved in parts of the stack. The enforced engineering policy is therefore capability-gated support proven by tests in the exact runtime.

## Consequences

- the browser scope stays small enough to verify rigorously
- advanced Delta features become an explicit compatibility matrix rather than tribal knowledge
- the project avoids shipping accidental partial support

## References

- [delta-rs best practices](https://delta-io.github.io/delta-rs/delta-lake-best-practices/)
