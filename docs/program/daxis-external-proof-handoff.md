# Daxis External Proof Handoff

- Date: 2026-05-30
- Scope: Daxis-owned evidence required before Axon is treated as the production default browser read-compute path
- Owner: Daxis product, platform, storage, catalog, web, security, and SRE owners
- Related:
  - [Daxis first-class integration strategy](./daxis-first-class-integration-strategy.md)
  - [Daxis operational maturity contract](./daxis-operational-maturity.md)
  - [Daxis external proof packet](../release-gates/daxis-external-proof-packet.json)
  - [Daxis strategy traceability matrix](../release-gates/daxis-strategy-traceability.json)

This handoff turns the Axon-side strategy, traceability, rollout, and operational-readiness contracts into the proof packet Daxis must attach during rollout review. It does not claim the external Daxis production systems exist in this repository.

## Required Packet

The machine-readable packet is [`../release-gates/daxis-external-proof-packet.json`](../release-gates/daxis-external-proof-packet.json), checked by `bash tests/conformance/verify_daxis_external_proof_packet.sh`.

Each packet item names:

- the milestone it blocks
- the Daxis owner for the external work
- the proof artifacts Daxis must attach
- the acceptance checks reviewers should apply
- the rollback evidence that keeps server fallback available
- the Axon references that define the contract boundary

## Review Flow

1. Run the Axon release evidence runner and attach the output.
2. Attach Daxis service endpoint tests for descriptor resolution and read-access-plan outcomes.
3. Attach Daxis frontend open/query/cancel evidence for the supported rollout browsers.
4. Attach storage CORS, proxy, object-scope, TTL, and audit evidence for the first rollout segment.
5. Attach Daxis dashboard, oncall, rollout-control, and compatibility-dashboard proof.
6. Confirm the rollback path can force `server_fallback` without shipping a new Axon release.

## Stable Default Boundary

Axon can supply repo-owned contracts, SDK examples, fixtures, browser matrix coverage, runtime-budget evidence, and release-evidence automation. Daxis must supply production proof for architecture docs, service endpoints, UI integration, storage access, rollout controls, dashboards, runbooks, and current table compatibility.

Until every item in the packet has attached Daxis evidence, the release state should remain at `integration` or `candidate`; it should not be treated as `stable_default`.
