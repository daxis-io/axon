# EPIC-04: Browser DataFusion WASM Runtime And HTTP Object-Store Hardening

- Type: Epic
- Accountable: Query platform lead
- Delivery DRI: Runtime / engine team
- Depends on: EPIC-01, EPIC-02, EPIC-03
- Milestone: `M3`

## Goal

Build the browser runtime for supported SQL workloads over signed HTTPS or proxy access.

## Packages In Scope

- `crates/wasm-query-runtime`
- `crates/wasm-http-object-store`
- `crates/browser-sdk`
- `apps/web-console`
- `tests/conformance`
- `tests/perf`

## Deliverables

- SQL-first browser API
- descriptor-based table registration
- HTTP range-read adapter
- deterministic fallback to native runtime
- explain and metrics in browser
- single-partition execution by default
- size and startup budgets

## Child Issues

1. Wire `wasm-query-runtime` to descriptor-based registration.
2. Implement the HTTP range adapter in `wasm-http-object-store`.
3. Add range tests for footer, bounded, suffix, and offset reads.
4. Add the SQL API and JS/TS wrapper in `browser-sdk`.
5. Add structured fallback routing.
6. Add bundle-size reporting and cold-start benchmark.
7. Add browser memory benchmark.
8. Add explain-plan rendering and debug metrics.
9. Add deterministic error taxonomy for auth, range, and protocol failures.
10. Add a sample web console.

## Acceptance Criteria

- the browser runtime executes the supported SQL corpus with parity to native
- the browser defaults to one partition and reports this clearly
- range-read correctness tests pass
- bundle size and startup budgets are captured in CI
- the SDK returns structured fallback reasons
- no browser package depends on signing or service-account code

## Definition Of Done

A supported browser query can read a Delta-backed table through signed HTTPS or proxy descriptors and return the same result as native.
