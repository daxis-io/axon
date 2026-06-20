#!/usr/bin/env bash

set -euo pipefail

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

repo_root="$tmpdir/repo"
strategy="$repo_root/docs/integrations/daxis/daxis-first-class-integration-strategy.md"
engine_strategy="$repo_root/docs/program/browser-lakehouse-engine-strategy.md"
sprint_board="$repo_root/docs/program/browser-lakehouse-sprint-board.md"

mkdir -p \
	"$repo_root/.github" \
	"$repo_root/apps/axon-web/examples" \
	"$repo_root/docs/adr" \
	"$repo_root/docs/integrations/daxis/daxis-first-class-integration-examples" \
	"$repo_root/docs/release-gates" \
	"$repo_root/tests/conformance"

for doc in \
	.github/pull_request_template.md \
	apps/axon-web/examples/daxis-descriptor-resolver.ts \
	apps/axon-web/examples/daxis-headless-query.ts \
	apps/axon-web/examples/daxis-object-grant-adapter.ts \
	apps/axon-web/examples/daxis-read-access-plan.ts \
	docs/adr/ADR-0008-daxis-browser-read-compute-contract.md \
	docs/program/browser-lakehouse-engine-strategy.md \
	docs/program/browser-lakehouse-sprint-board.md \
	docs/program/browser-delta-compatibility-matrix.md \
	docs/program/browser-embedding-deployment.md \
	docs/program/browser-uc-brokered-runtime-contract.md \
	docs/program/browser-observability-contract.md \
	docs/program/browser-datafusion-runtime-parity.md \
	docs/integrations/daxis/daxis-operational-maturity.md \
	docs/integrations/daxis/daxis-external-proof-handoff.md \
	docs/integrations/daxis/daxis-first-class-integration-examples/approved-axon-read-descriptor.saved-query.json \
	docs/integrations/daxis/daxis-first-class-integration-examples/object-grants.audit-event.range.json \
	docs/release-gates/daxis-production-rollout-decisions.json \
	docs/release-gates/daxis-strategy-traceability.json \
	docs/release-gates/daxis-external-proof-packet.json \
	docs/release-gates/daxis-operational-readiness.json \
	docs/release-gates/daxis-external-proof-attachment-template.md \
	docs/release-gates/daxis-release-bundle-manifest.json \
	docs/release-gates/daxis-release-attachment-template.md \
	docs/release-gates/daxis-release-notes-template.md \
	docs/release-gates/daxis-release-migration-notes-template.md \
	docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md \
	docs/release-gates/browser-wasm-delta-gcs-external-blockers.md \
	docs/release-gates/daxis-browser-datafusion-budget-profile.json \
	docs/release-gates/daxis-browser-runtime-isolation-plan.json \
	docs/release-gates/daxis-contract-artifacts.sha256; do
	mkdir -p "$repo_root/$(dirname "$doc")"
	printf '# test fixture\n' >"$repo_root/$doc"
done

cat >"$repo_root/tests/conformance/verify_daxis_release_evidence.sh" <<'RUNNER'
#!/usr/bin/env bash
set -euo pipefail
case "${1:-}" in
  --list)
    cat <<'COMMANDS'
bash tests/conformance/verify_daxis_strategy_document_test.sh
bash tests/conformance/verify_daxis_strategy_document.sh
bash tests/conformance/verify_daxis_query_corpus_coverage_test.sh
bash tests/conformance/verify_daxis_query_corpus_coverage.sh
bash tests/conformance/verify_daxis_contract_artifacts.sh
bash tests/conformance/verify_daxis_rollout_decisions.sh
bash tests/conformance/verify_daxis_operational_readiness.sh
bash tests/conformance/verify_daxis_strategy_traceability.sh
bash tests/conformance/verify_daxis_external_proof_packet.sh
bash tests/conformance/verify_daxis_architecture_adr.sh
bash tests/conformance/verify_daxis_release_bundle_manifest.sh
bash tests/conformance/verify_daxis_pr_checklist.sh
bash tests/conformance/verify_daxis_release_evidence.sh
COMMANDS
    ;;
  *)
    ;;
esac
RUNNER
chmod +x "$repo_root/tests/conformance/verify_daxis_release_evidence.sh"

write_valid_companion_docs() {
	cat >"$engine_strategy" <<'ENGINE'
# Browser Lakehouse Engine Strategy

- Scope: make browser DataFusion the default Axon browser execution engine while keeping legacy narrow execution isolated for compatibility

The Axon app worker is browser DataFusion-backed.
Legacy narrow runtime and session shell remain compatibility-only.
ENGINE

	cat >"$sprint_board" <<'SPRINT'
# Browser Lakehouse Sprint Board

- Axon DataFusion runtime SKU
- legacy narrow compatibility SKU
- worker artifact reports enabled feature set, SKU identity, and browser DataFusion availability
SPRINT
}

write_valid_strategy() {
	write_valid_companion_docs
	cat >"$strategy" <<'STRATEGY'
# Daxis First-Class Integration Strategy

- Date: 2026-05-30
- Status: Working strategy
- Audience: Axon runtime, SDK, web platform, storage platform, and Daxis control-plane developers
- Scope: how Axon should evolve into the default browser read-compute engine for Daxis
- Related:
  - [Browser lakehouse engine strategy](../../program/browser-lakehouse-engine-strategy.md)
  - [Browser Delta compatibility matrix](../../program/browser-delta-compatibility-matrix.md)
  - [Browser embedding and deployment](../../program/browser-embedding-deployment.md)
  - [Browser Unity Catalog brokered runtime contract](../../program/browser-uc-brokered-runtime-contract.md)
  - [Browser observability contract](../../program/browser-observability-contract.md)
  - [Browser DataFusion runtime parity](../../program/browser-datafusion-runtime-parity.md)
  - [ADR-0008: Daxis browser read compute contract](../../adr/ADR-0008-daxis-browser-read-compute-contract.md)
  - [Daxis operational maturity contract](./daxis-operational-maturity.md)
  - [Daxis external proof handoff](./daxis-external-proof-handoff.md)
  - [Daxis production rollout decisions](../../release-gates/daxis-production-rollout-decisions.json)
  - [Daxis strategy traceability matrix](../../release-gates/daxis-strategy-traceability.json)
  - [Daxis external proof packet](../../release-gates/daxis-external-proof-packet.json)
  - [Daxis external proof attachment template](../../release-gates/daxis-external-proof-attachment-template.md)
  - [Daxis release bundle manifest](../../release-gates/daxis-release-bundle-manifest.json)
  - [Daxis release attachment template](../../release-gates/daxis-release-attachment-template.md)
  - [Daxis release notes template](../../release-gates/daxis-release-notes-template.md)
  - [Daxis release migration notes template](../../release-gates/daxis-release-migration-notes-template.md)
  - [Browser WASM + Delta on GCS launch checklist](../../release-gates/browser-wasm-delta-gcs-launch-checklist.md)
  - [External blockers](../../release-gates/browser-wasm-delta-gcs-external-blockers.md)

## Executive Summary

Axon should be developed as the default browser read-compute engine for Daxis.
Daxis owns the headless query gateway, catalog policy, access grants, audit, request correlation, routing, and server fallback.
Axon owns browser SQL execution, Delta/Parquet read behavior, worker and SDK contracts, Arrow IPC result transport, structured runtime metrics, and deterministic fallback vocabulary.
The browser runtime must never become authoritative for Daxis policy, cloud credentials, catalog identity, signing, or audit.
The long-term product behavior should be "browser first, deterministic fallback".

## Product Vision

Axon needs stable contracts, compatibility discipline, release evidence, security boundaries, and operational visibility.

## Non-Goals

Axon should not own Daxis user authentication, catalog governance, raw cloud credential handling, production audit pipelines, authoritative server query fallback decisions, or Delta writes.

## Integration Boundary

Daxis asks the governance question and Axon asks the execution question. If policy is unknown, Daxis must return `blocked` or `sql_fallback_required`.

## Default Read-Compute Model

Daxis should make Axon the default read compute path for interactive browser analytics when table support, policy, descriptor safety, Delta features, SQL shape, and browser budgets allow it.

## Headless Query Gateway

Daxis query surfaces all enter one gateway path. Daxis owns every state through `routed`; Axon owns only browser execution after it receives a Daxis-approved Axon read descriptor.

## Target Architecture

Layer 1 is Daxis product and trusted services. Layer 2 is Axon public integration surface. Layer 3 is Axon browser runtime.

## Contract Strategy

Primary contracts include `QueryRequest`, `QueryResponse`, `QueryError`, `FallbackReason`, `DaxisResultEnvelope`, `DaxisApprovedAxonReadDescriptor`, `BrowserHttpSnapshotDescriptor`, `DeltaLocationResolveResponse`, `ReadAccessPlan`, `BrokeredDeltaReadPlan`, `BrowserWorkerEventEnvelope`, and Arrow IPC result envelope.

## Daxis-Facing API Shapes

The descriptor endpoint is `POST /v1/query/delta/snapshot-descriptor`; the read-access-plan endpoint is `POST /v1/catalog/read-access-plan`.
The Axon browser SDK example lives in [`../../../apps/axon-web/examples/daxis-descriptor-resolver.ts`](../../../apps/axon-web/examples/daxis-descriptor-resolver.ts).
The headless gateway example lives in [`../../../apps/axon-web/examples/daxis-headless-query.ts`](../../../apps/axon-web/examples/daxis-headless-query.ts).
The M2 read-access-plan SDK example lives in [`../../../apps/axon-web/examples/daxis-read-access-plan.ts`](../../../apps/axon-web/examples/daxis-read-access-plan.ts).
The M2 object-grant helper example lives in [`../../../apps/axon-web/examples/daxis-object-grant-adapter.ts`](../../../apps/axon-web/examples/daxis-object-grant-adapter.ts). It keeps grant identifiers out of worker command payloads.
The M2 audit fixture [`object-grants.audit-event.range.json`](./daxis-first-class-integration-examples/object-grants.audit-event.range.json) defines the Axon-side shape Daxis can use.
The Daxis contract artifact hash manifest [`../../release-gates/daxis-contract-artifacts.sha256`](../../release-gates/daxis-contract-artifacts.sha256) is verified with `bash tests/conformance/verify_daxis_contract_artifacts.sh`.
The M3 browser DataFusion budget profile [`../../release-gates/daxis-browser-datafusion-budget-profile.json`](../../release-gates/daxis-browser-datafusion-budget-profile.json) is checked by `cargo test -p wasm-datafusion-poc --test daxis_budget_profile`.
The M3 runtime isolation plan [`../../release-gates/daxis-browser-runtime-isolation-plan.json`](../../release-gates/daxis-browser-runtime-isolation-plan.json) is checked by `cargo test -p wasm-datafusion-poc --test daxis_runtime_isolation_plan`.

## Daxis Integration Responsibility Plan

Daxis product platform, gateway, catalog, storage broker, and query service own policy, auth, audit, routing, and fallback. Axon SDK and worker own descriptor validation, browser SQL, structured metrics, fallback/error taxonomy, and worker events.

## Runtime Strategy

The default browser runtime must use browser DataFusion and have no hidden server calls from the default browser runtime.

## Query Flow

The query path should include stable request IDs from Daxis and worker-side query IDs from Axon.

## Fallback And Error Semantics

Fallback reasons include unsupported SQL, unsupported Delta protocol features, browser budget exceeded, signed URL expired, CORS blocked, policy requires server execution, warehouse/server fallback required, and cancellation.

## Security And Governance

Browser code must never receive raw credentials, signing keys, service-account JSON, OAuth tokens, HMAC keys, broad SAS tokens, or direct catalog tokens.

## Observability

Axon should emit bytes fetched, files touched, files skipped, row groups touched, row groups skipped, footer reads, rows emitted, query duration, snapshot bootstrap duration, selected bundle tier, browser runtime SKU, fallback reason, worker progress events, worker terminal errors, Arrow IPC byte length, session cache bytes, and table count.
Daxis should add tenant ID, workspace ID, policy decision, access mode decision, request ID, server-side correlation ID, audit evidence for read access, and fallback routing target.

## Testing And Quality Gates

Required gates for Axon engine changes include Rust formatting with `cargo fmt --check`, Rust contract tests, WASM target checks, dependency guardrails, browser matrix tests, public object-storage smoke, and release evidence.
Daxis integration gates should include descriptor resolver tests, read-access-plan tests, object-grant tests, fallback-required tests, request and correlation ID propagation test, and production rollout evidence.
For the Daxis browser query corpus, `bash tests/conformance/verify_daxis_query_corpus_coverage.sh` enforces that runtime cases, declared SQL classes, and browser DataFusion parity statements stay aligned.
The strategy document itself is checked by `bash tests/conformance/verify_daxis_strategy_document.sh`.

## Release And Compatibility Policy

The machine-readable bundle checklist is [`../../release-gates/daxis-release-bundle-manifest.json`](../../release-gates/daxis-release-bundle-manifest.json), checked by `bash tests/conformance/verify_daxis_release_bundle_manifest.sh`.
It distinguishes repo-verified artifacts from release-process attachments such as the exact commit SHA, worker artifact size output, public GCS live-smoke output or skip-safe blocker record, Daxis-facing release notes, and migration notes or a no-breaking-change statement.
Its `releaseAttachmentSchema` names [`../../release-gates/daxis-release-attachment-template.md`](../../release-gates/daxis-release-attachment-template.md) and the metadata each release-process attachment must carry, including item ID, release commit, release ref, Daxis release channel, rollout segment, owner, capture time, artifact URI, artifact SHA-256 digest, verification command or statement, exit or review status, and rollback or migration-note URI.
The `releaseAttachmentSchema.allowedReleaseChannels` contract records the allowed `experimental`, `integration`, `candidate`, and `stable` channel values; stable default promotion requires `stable` through `stableDefaultPromotionGate.requiredReleaseChannel`.
The `releaseAttachmentSchema.checksumFormat` contract records that SHA-256 attachment fields use 64-character lowercase hexadecimal digests generated from the exact attached artifact bytes.
The `releaseAttachmentSchema.requiredReviewerRoles` contract records the release, runtime, product, query, catalog/storage, security, and SRE owner roles required for attachment review.
The `releaseAttachmentSchema.stableDefaultValidationCommand` and `releaseAttachmentSchema.stableDefaultDirectoryValidationCommand` fields record the single-attachment and complete-set validators for accepted stable release-process evidence.
The `releaseAttachmentSchema.stableDefaultArtifactValidationCommand` and `releaseAttachmentSchema.stableDefaultArtifactDirectoryValidationCommand` fields record the local-packet validators for accepted stable release-process evidence.
The `stableDefaultPromotionPacketValidationCommand` field records `bash tests/conformance/verify_daxis_stable_default_promotion_packet.sh` as the combined stable-default promotion packet validator.
Release owners should complete [`../../release-gates/daxis-release-notes-template.md`](../../release-gates/daxis-release-notes-template.md) for release notes that document query-result semantics, Daxis result metrics and observability fields, fallback behavior, supported SQL and Delta feature claims, descriptor validation, public error taxonomy, runtime budgets, worker artifact changes, security-boundary impact, external proof packet status, and `stableDefaultPromotionGate` `currentPromotionState`.
Recommended release channels are `experimental`, `integration`, `candidate`, and `stable`.

## Development Operating Model

Treat docs, tests, and release evidence as part of the change, not cleanup after the fact.

## Roadmap

The milestone deliverables and exit criteria below are mapped to current evidence in [`../../release-gates/daxis-strategy-traceability.json`](../../release-gates/daxis-strategy-traceability.json), checked by `bash tests/conformance/verify_daxis_strategy_traceability.sh`.
The external proof handoff is recorded in [`daxis-external-proof-handoff.md`](./daxis-external-proof-handoff.md) and [`../../release-gates/daxis-external-proof-packet.json`](../../release-gates/daxis-external-proof-packet.json), checked by `bash tests/conformance/verify_daxis_external_proof_packet.sh`.
It carries the `stableDefaultPromotionGate` and `currentPromotionState` `blocked_external_proof_required` value that ties accepted external proof to accepted release-process attachments, full release-evidence output, the external blocker register, and `server_fallback` rollback evidence.
The stable-default gate also carries `requiredReleaseAttachmentSchemaFields` and `requiredProofAttachmentSchemaFields` so promotion depends on digest-pinned, channel-scoped, segment-scoped, production-environment-scoped, Axon-release-identity-pinned, owner-reviewed release attachments and Daxis proof attachments.
Those field lists include `releaseAttachmentSchema.allowedReleaseChannels`, `releaseAttachmentSchema.stableDefaultValidationCommand`, `releaseAttachmentSchema.stableDefaultDirectoryValidationCommand`, `releaseAttachmentSchema.stableDefaultArtifactValidationCommand`, `releaseAttachmentSchema.stableDefaultArtifactDirectoryValidationCommand`, `proofAttachmentSchema.stableDefaultValidationCommand`, `proofAttachmentSchema.stableDefaultDirectoryValidationCommand`, `proofAttachmentSchema.stableDefaultArtifactValidationCommand`, `proofAttachmentSchema.stableDefaultArtifactDirectoryValidationCommand`, `requiredReleaseEvidenceArtifactCommand`, `stableDefaultPromotionPacketValidationCommand`, `verify_daxis_stable_default_promotion_packet.sh`, `proofAttachmentSchema.allowedReleaseChannels`, `proofAttachmentSchema.allowedDaxisWorktreeStatuses`, `proofAttachmentSchema.allowedDaxisWorktreeReviews`, `proofAttachmentSchema.acceptedDaxisWorktreeReviews`, `proofAttachmentSchema.dirtyWorktreeReviewChecksumFormat`, and `proofAttachmentSchema.dirtyWorktreeReviewTemplatePath`, so stable default promotion requires `stable`, rejects arbitrary release-channel strings, and stable default promotion accepts only `clean` or digest-pinned `dirty_reviewed` Daxis checkout evidence.

### M0: Daxis Alignment

Daxis architecture names Axon as the default browser read engine.

### M1: Server-Resolved Descriptor Integration

Server fallback remains available and reason-preserving.

### M2: Brokered Object Grants

Daxis can choose descriptor, grant, proxy, Delta Sharing, server fallback, or blocked states per table.

### M3: Browser DataFusion As Primary Runtime

DataFusion-backed SQL execution is the default app/runtime path.

### M4: Operational Maturity

Goal: make Axon safe to operate as default read compute. The repo-owned M4 contract is recorded in [`daxis-operational-maturity.md`](./daxis-operational-maturity.md) and [`../../release-gates/daxis-operational-readiness.json`](../../release-gates/daxis-operational-readiness.json), checked by `bash tests/conformance/verify_daxis_operational_readiness.sh`, and the release evidence runner is `bash tests/conformance/verify_daxis_release_evidence.sh`.

## Success Metrics

Interactive read queries use browser execution by default where policy allows. Daxis can explain why each query used browser execution, server fallback, or policy block. Axon release evidence is sufficient for rollout decisions. Elevated fallback rates, CORS failures, expired descriptors, and worker failures are observable.

## Rollout Decision Register

The first Daxis production rollout decisions are recorded in [`../../release-gates/daxis-production-rollout-decisions.json`](../../release-gates/daxis-production-rollout-decisions.json) and checked by `bash tests/conformance/verify_daxis_rollout_decisions.sh`.
External signoffs still required before production default routing include Daxis security approval, Daxis product/query platform approval, Daxis storage approval, Daxis SRE approval, and Daxis owner attachment of the proof artifacts listed in the external proof packet.

## PR Checklist For Axon Developers

Before merging a Daxis-relevant Axon change, verify the checked-in pull request template at [`../../../.github/pull_request_template.md`](../../../.github/pull_request_template.md). The Daxis checklist is enforced by `bash tests/conformance/verify_daxis_pr_checklist.sh`.
Promotion requires passing release evidence, no broadened browser trust boundary, and Daxis-owned rollout controls.
Stable default routing is gated on `docs/release-gates/daxis-external-proof-packet.json` `stableDefaultPromotionGate` acceptance, `requiredReleaseChannel` `stable`, `requiredReleaseEvidenceArtifactCommand`, and `stableDefaultPromotionPacketValidationCommand`.
Release-process evidence uses `docs/release-gates/daxis-release-attachment-template.md` for git SHA, worker size, public GCS live smoke, release notes, and migration notes, including `release_channel`, `rollout_segment`, and `releaseAttachmentSchema.allowedReleaseChannels`.
Daxis-facing release notes use `docs/release-gates/daxis-release-notes-template.md` for semantic, Daxis result metrics and observability fields, fallback, compatibility, descriptor, error-taxonomy, runtime-budget, worker-artifact, and trust-boundary changes.
Daxis-facing migration notes use `docs/release-gates/daxis-release-migration-notes-template.md` for breaking changes or explicit no-breaking-change statements.
Daxis-owned production proof uses `docs/release-gates/daxis-external-proof-attachment-template.md` and `docs/release-gates/daxis-dirty-worktree-review-template.json` for dirty-checkout reviews, and attaches `release_channel`, production `environment_class`, `axon_release_commit_sha`, `axon_release_ref`, `proofAttachmentSchema.allowedReleaseChannels`, `proofAttachmentSchema.acceptedDaxisWorktreeReviews`, the `daxis.external_state.v1` JSON summary, its SHA-256 digest, and clean or digest-pinned dirty-reviewed Daxis worktree classification before stable default routing.

## Handoff Summary

Make the browser safe. Make fallback deterministic. Make release evidence repeatable.
STRATEGY
}

verify_fixture() {
	AXON_DAXIS_STRATEGY_DOCUMENT_REPO_ROOT="$repo_root" \
		bash tests/conformance/verify_daxis_strategy_document.sh >/dev/null 2>&1
}

write_valid_strategy
verify_fixture

expect_missing_text_rejected() {
	local needle="$1"
	local description="$2"

	write_valid_strategy

	python3 - "$strategy" "$needle" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
needle = sys.argv[2]
text = path.read_text(encoding="utf-8")
text = text.replace(needle, "")
path.write_text(text, encoding="utf-8")
PY

	if verify_fixture; then
		echo "expected missing Daxis strategy $description to be rejected" >&2
		exit 1
	fi
}

expect_missing_text_rejected "## Default Read-Compute Model" "default read-compute section"
expect_missing_text_rejected "stableDefaultPromotionGate" "stable default gate"
expect_missing_text_rejected "requiredReleaseAttachmentSchemaFields" "stable default release attachment schema fields"
expect_missing_text_rejected "requiredProofAttachmentSchemaFields" "stable default proof attachment schema fields"
expect_missing_text_rejected 'currentPromotionState` `blocked_external_proof_required' "stable default current promotion state"
expect_missing_text_rejected "cargo fmt --check" "Rust formatting command"
expect_missing_text_rejected "bash tests/conformance/verify_daxis_query_corpus_coverage.sh" "query corpus coverage verifier command"
expect_missing_text_rejected "bash tests/conformance/verify_daxis_release_bundle_manifest.sh" "release bundle gate"
expect_missing_text_rejected "artifact SHA-256 digest" "release attachment artifact checksum metadata"
expect_missing_text_rejected "Daxis release channel" "release attachment release-channel metadata"
expect_missing_text_rejected "allowedReleaseChannels" "allowed release-channel policy"
expect_missing_text_rejected "requiredReleaseChannel" "stable default required release channel"
expect_missing_text_rejected 'stable default promotion requires `stable`' "stable default stable-channel policy"
expect_missing_text_rejected "allowedDaxisWorktreeStatuses" "allowed Daxis worktree status policy"
expect_missing_text_rejected "allowedDaxisWorktreeReviews" "allowed Daxis worktree review policy"
expect_missing_text_rejected "acceptedDaxisWorktreeReviews" "accepted Daxis worktree review policy"
expect_missing_text_rejected 'stable default promotion accepts only `clean` or digest-pinned `dirty_reviewed`' "stable default accepted worktree review policy"
expect_missing_text_rejected "rollout segment" "release attachment rollout-segment metadata"
expect_missing_text_rejected "releaseAttachmentSchema.checksumFormat" "release attachment checksum format"
expect_missing_text_rejected "releaseAttachmentSchema.requiredReviewerRoles" "release attachment reviewer-role schema"
expect_missing_text_rejected "channel-scoped" "stable default channel-scoped release attachment guidance"
expect_missing_text_rejected "segment-scoped" "stable default segment-scoped release attachment guidance"
expect_missing_text_rejected "production-environment-scoped" "stable default production-environment-scoped proof guidance"
expect_missing_text_rejected "Axon-release-identity-pinned" "stable default Axon release identity proof guidance"
expect_missing_text_rejected "../../release-gates/daxis-release-notes-template.md" "release notes artifact path"
expect_missing_text_rejected "../../release-gates/daxis-release-migration-notes-template.md" "release migration notes artifact path"
expect_missing_text_rejected 'Release-process evidence uses `docs/release-gates/daxis-release-attachment-template.md` for git SHA, worker size, public GCS live smoke, release notes, and migration notes, including `release_channel`, `rollout_segment`, and `releaseAttachmentSchema.allowedReleaseChannels`.' "scoped release attachment checklist guidance"
expect_missing_text_rejected "The M2 object-grant helper example lives in [\`../../../apps/axon-web/examples/daxis-object-grant-adapter.ts\`](../../../apps/axon-web/examples/daxis-object-grant-adapter.ts)." "M2 object-grant adapter strategy guidance"
expect_missing_text_rejected 'query-result semantics, Daxis result metrics and observability fields, fallback behavior' "Daxis result metrics release-policy guidance"
expect_missing_text_rejected 'Daxis-facing release notes use `docs/release-gates/daxis-release-notes-template.md` for semantic, Daxis result metrics and observability fields, fallback, compatibility, descriptor, error-taxonomy, runtime-budget, worker-artifact, and trust-boundary changes.' "Daxis-facing release notes checklist guidance"
expect_missing_text_rejected 'Daxis-facing migration notes use `docs/release-gates/daxis-release-migration-notes-template.md` for breaking changes or explicit no-breaking-change statements.' "Daxis-facing migration notes checklist guidance"
expect_missing_text_rejected 'Daxis-owned production proof uses `docs/release-gates/daxis-external-proof-attachment-template.md` and `docs/release-gates/daxis-dirty-worktree-review-template.json` for dirty-checkout reviews, and attaches `release_channel`, production `environment_class`, `axon_release_commit_sha`, `axon_release_ref`, `proofAttachmentSchema.allowedReleaseChannels`, `proofAttachmentSchema.acceptedDaxisWorktreeReviews`, the `daxis.external_state.v1` JSON summary, its SHA-256 digest, and clean or digest-pinned dirty-reviewed Daxis worktree classification before stable default routing.' "Daxis-owned external-state proof checklist guidance"
expect_missing_text_rejected "### M3: Browser DataFusion As Primary Runtime" "M3 roadmap section"
expect_missing_text_rejected "Daxis owner attachment of the proof artifacts listed in the external proof packet" "external proof signoff"

write_valid_strategy
cat >>"$engine_strategy" <<'ENGINE'
Stale scope: turn Axon's current browser preflight and narrow execution slice into a production-oriented browser engine.
ENGINE

if verify_fixture; then
	echo "expected stale linked browser lakehouse engine strategy scope to be rejected" >&2
	exit 1
fi

write_valid_strategy
cat >>"$engine_strategy" <<'ENGINE'
Signed URL issuance stays outside the repo-owned V1 success claims.
ENGINE

if verify_fixture; then
	echo "expected stale linked browser lakehouse engine strategy V1 boundary to be rejected" >&2
	exit 1
fi

write_valid_strategy
cat >>"$sprint_board" <<'SPRINT'
- default narrow runtime SKU
- opt-in larger SQL SKU
SPRINT

if verify_fixture; then
	echo "expected stale browser lakehouse sprint-board SKU labels to be rejected" >&2
	exit 1
fi

write_valid_strategy
rm -f "$repo_root/docs/release-gates/daxis-external-proof-packet.json"

if verify_fixture; then
	echo "expected missing linked external proof packet to be rejected" >&2
	exit 1
fi

write_valid_strategy
python3 - "$strategy" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
text = text.replace(
    "[Daxis external proof packet](../../release-gates/daxis-external-proof-packet.json)",
    "[Daxis external proof packet](../../outside.json)",
)
path.write_text(text, encoding="utf-8")
PY

if verify_fixture; then
	echo "expected unsafe linked strategy path to be rejected" >&2
	exit 1
fi

rm -f "$strategy"

if verify_fixture; then
	echo "expected missing Daxis strategy document to be rejected" >&2
	exit 1
fi

echo "Daxis strategy document verifier regression coverage passed"
