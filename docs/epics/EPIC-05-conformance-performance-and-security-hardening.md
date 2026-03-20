# EPIC-05: Conformance, Performance, And Security Hardening

- Type: Epic
- Accountable: QA / performance engineering
- Delivery DRI: QA / performance engineering
- Depends on: EPIC-02, EPIC-03, EPIC-04
- Milestone: `M4`

## Goal

Turn the implementation into a production system rather than a demo.

## Packages In Scope

- `tests/conformance`
- `tests/perf`
- `tests/security`
- instrumentation additions in runtime packages

## Deliverables

- semantic parity suite
- Delta feature compatibility matrix
- benchmark datasets and repeatable runs
- regression thresholds
- secret leakage checks
- observability dashboards
- runbooks and release-gate checklist

## Child Issues

1. Build a parity harness executing the same query on browser and native.
2. Add a fixture matrix covering partitioned, compacted, z-ordered, and advanced-feature tables.
3. Add a browser and network benchmark suite.
4. Add a metrics pipeline for bytes fetched, files touched, skipped files, footer reads, cache hits, and fallback reasons.
5. Add secret scanning and browser bundle inspection.
6. Add chaos tests for expired signed URLs and intermittent network failures.
7. Add release thresholds and CI blockers.
8. Create an oncall playbook for fallback failure or control-plane outage.

## Acceptance Criteria

- at least 95% of supported-query fixtures match byte-for-byte or value-equivalent semantics across runtimes
- unsupported and advanced features are cataloged and tested
- perf dashboards show baseline and regression trends for native and browser
- security tests show no cloud secrets in browser bundle or runtime payloads
- the release checklist exists and is enforced

## Definition Of Done

The system has measurable correctness, measurable performance, measurable observability, and a reviewed security posture.
