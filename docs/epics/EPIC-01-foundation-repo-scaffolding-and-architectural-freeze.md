# EPIC-01: Foundation, Repo Scaffolding, and Architectural Freeze

- Type: Epic
- Accountable: Query platform lead
- Delivery DRI: Runtime / engine team
- Depends on: none
- Milestone: `M0`

## Goal

Create the package structure, governance, labels, contracts, and CI required to build safely.

## Packages In Scope

- `crates/query-contract`
- `crates/query-router` skeleton
- `crates/native-query-runtime` skeleton
- `crates/delta-control-plane` skeleton
- `crates/wasm-query-runtime` skeleton
- `crates/wasm-http-object-store` skeleton
- `crates/browser-sdk` skeleton
- `crates/udf-abi` skeleton
- `crates/udf-host-wasi` skeleton
- `tests/conformance`
- `tests/perf`
- `tests/security`

## Deliverables

- monorepo layout
- CI for native and wasm builds
- ADRs checked into repo
- labels, milestones, and templates
- shared error taxonomy and capability model

## Implementation Snapshot

The initial EPIC-01 scaffold now exists in-repo:

- workspace manifest: [Cargo.toml](../../Cargo.toml)
- shared contract types: [crates/query-contract/src/lib.rs](../../crates/query-contract/src/lib.rs)
- native and wasm CI: [.github/workflows/ci.yml](../../.github/workflows/ci.yml)
- ownership map: [docs/program/package-owners.md](../program/package-owners.md)
- patch inventory template: [docs/program/upstream-patch-inventory.md](../program/upstream-patch-inventory.md)
- workspace layout verifier: [tests/conformance/verify_workspace_layout.sh](../../tests/conformance/verify_workspace_layout.sh)

## Child Issues

1. Create workspace skeleton.
2. Add native and wasm CI.
3. Create ADR, epic, and runbook docs structure.
4. Define `query-contract` types.
5. Add code owners.
6. Add release-gate checklist template.
7. Add upstream patch inventory template.

## Acceptance Criteria

- every package exists with an owner
- CI builds native and wasm targets
- `query-contract` has no cloud or browser dependencies
- ADRs are merged and linked
- issue templates exist for epics and release gates

## Definition Of Done

EPIC-02 and EPIC-03 can start without reopening package-boundary debates.
