# GitHub Project Setup

- Date: 2026-03-20
- Purpose: turn the architecture bundle into actionable GitHub metadata

## Labels

- `area/runtime-native`
- `area/runtime-wasm`
- `area/control-plane`
- `area/storage-gcs`
- `area/security`
- `area/perf`
- `area/conformance`
- `area/oss-upstream`
- `type/adr-followup`
- `type/epic`
- `type/release-gate`
- `risk/high`
- `risk/medium`
- `risk/low`

## Milestones

- `M0` Architecture frozen
- `M1` Native reference reads Delta on GCS
- `M2` Control plane signs browser-safe read access
- `M3` Browser runtime executes supported SQL
- `M4` Production hardening complete
- `M5` Hosted WASI UDF path available
- `M6` Upstreamed / maintainable

## Recommended Approval Sequence

Approve first:

- ADR-0001
- ADR-0002
- ADR-0003
- ADR-0004
- ADR-0005

Open first:

- EPIC-01
- EPIC-02
- EPIC-03
- EPIC-04

Harden next:

- EPIC-05

Separate extension track and maintenance:

- EPIC-06
- EPIC-07

## Paste Order

1. Add the ADR files from [docs/adr](../adr/README.md) to the repo and land ADR-0001 through ADR-0005 first.
2. Open the issue bodies from [docs/epics](../epics/README.md) in milestone order.
3. Apply the labels above before triage so ownership and risk stay visible from day one.
4. Use the release-gate template only after EPIC-05 work begins.
