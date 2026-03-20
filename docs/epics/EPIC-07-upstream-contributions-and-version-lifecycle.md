# EPIC-07: Upstream Contributions And Version Lifecycle

- Type: Epic
- Accountable: DX / OSS maintainer
- Delivery DRI: Runtime / engine team
- Depends on: EPIC-02, EPIC-04, EPIC-05
- Milestone: `M6`

## Goal

Shrink the private fork surface area and normalize upgrades.

## Deliverables

- private patch inventory
- quarterly upgrade SOP
- candidate upstream PR list
- compatibility review checklist
- dependency dashboard

## Likely Upstream Targets

- DataFusion WASM support improvements
- browser-safe runtime and task abstractions
- better release and upgrade SOP for wasm bindings
- range-read and object-store correctness work
- docs and examples for browser execution
- safer abstractions reducing `unsafe` workarounds

## Child Issues

1. Inventory all downstream patches.
2. Create an upgrade rehearsal CI job.
3. Open upstream issues or PRs for the highest-value changes.
4. Add a changelog review checklist for DataFusion and delta-rs upgrades.
5. Add a fork divergence report to quarterly review.
6. Document policy for temporary vendoring and removal.

## Acceptance Criteria

- every private patch is tracked with owner and disposition
- upgrade rehearsal runs in CI
- at least one high-value patch is proposed upstream
- release notes include upstream compatibility review
- no mystery forks remain

## Definition Of Done

Maintenance becomes a repeatable engineering process instead of heroics.
