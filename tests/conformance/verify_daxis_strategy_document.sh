#!/usr/bin/env bash

set -euo pipefail

repo_root="${AXON_DAXIS_STRATEGY_DOCUMENT_REPO_ROOT:-$(pwd)}"
strategy_file="${AXON_DAXIS_STRATEGY_DOCUMENT_FILE:-docs/program/daxis-first-class-integration-strategy.md}"

resolve_path() {
	local path="$1"
	if [[ "$path" = /* ]]; then
		printf "%s\n" "$path"
		return
	fi

	printf "%s/%s\n" "$repo_root" "$path"
}

strategy_path="$(resolve_path "$strategy_file")"
if [[ ! -f "$strategy_path" ]]; then
	echo "missing Daxis first-class integration strategy document: $strategy_file" >&2
	exit 1
fi

python3 - "$repo_root" "$strategy_path" "$strategy_file" <<'PY'
import re
import subprocess
import sys
from pathlib import Path

repo_root = Path(sys.argv[1])
strategy_path = Path(sys.argv[2])
strategy_file = sys.argv[3]
text = strategy_path.read_text(encoding="utf-8")


def fail(message: str) -> None:
    print(message, file=sys.stderr)
    sys.exit(1)


def expect(condition: bool, message: str) -> None:
    if not condition:
        fail(message)


def expect_text(needle: str, description: str) -> None:
    expect(needle in text, f"{strategy_file} missing {description}: {needle}")


def check_strategy_link(link: str) -> None:
    if link.startswith(("http://", "https://", "mailto:", "#")):
        return
    path_part = link.split("#", 1)[0]
    if not path_part:
        return
    candidate = Path(path_part)
    expect(not candidate.is_absolute(), f"{strategy_file} has absolute link: {link}")
    resolved = (strategy_path.parent / candidate).resolve()
    repo_resolved = repo_root.resolve()
    expect(
        resolved == repo_resolved or repo_resolved in resolved.parents,
        f"{strategy_file} has unsafe link outside repo: {link}",
    )
    expect(resolved.exists(), f"{strategy_file} links missing artifact: {link}")


def read_required_program_doc(filename: str, description: str) -> str:
    path = (strategy_path.parent / filename).resolve()
    repo_resolved = repo_root.resolve()
    expect(
        path == repo_resolved or repo_resolved in path.parents,
        f"{strategy_file} has unsafe companion artifact path: {filename}",
    )
    expect(path.is_file(), f"missing {description}: {filename}")
    return path.read_text(encoding="utf-8")


def expect_companion_text(doc_text: str, doc_name: str, needle: str, description: str) -> None:
    expect(needle in doc_text, f"{doc_name} missing {description}: {needle}")


def reject_companion_text(doc_text: str, doc_name: str, needle: str, description: str) -> None:
    expect(needle not in doc_text, f"{doc_name} must not contain stale {description}: {needle}")


expect(text.startswith("# Daxis First-Class Integration Strategy"), "unexpected strategy title")
for required in [
    "- Date:",
    "- Status: Working strategy",
    "- Audience: Axon runtime, SDK, web platform, storage platform, and Daxis control-plane developers",
    "- Scope: how Axon should evolve into the default browser read-compute engine for Daxis",
    "- Related:",
]:
    expect_text(required, "frontmatter field")

for section in [
    "## Executive Summary",
    "## Product Vision",
    "## Non-Goals",
    "## Integration Boundary",
    "## Default Read-Compute Model",
    "## Headless Query Gateway",
    "## Target Architecture",
    "## Contract Strategy",
    "## Daxis-Facing API Shapes",
    "## Daxis Integration Responsibility Plan",
    "## Runtime Strategy",
    "## Query Flow",
    "## Fallback And Error Semantics",
    "## Security And Governance",
    "## Observability",
    "## Testing And Quality Gates",
    "## Release And Compatibility Policy",
    "## Development Operating Model",
    "## Roadmap",
    "### M0: Daxis Alignment",
    "### M1: Server-Resolved Descriptor Integration",
    "### M2: Brokered Object Grants",
    "### M3: Browser DataFusion As Primary Runtime",
    "### M4: Operational Maturity",
    "## Success Metrics",
    "## Rollout Decision Register",
    "## PR Checklist For Axon Developers",
    "## Handoff Summary",
]:
    expect_text(section, "strategy section")

for required_path in [
    "./browser-lakehouse-engine-strategy.md",
    "./browser-delta-compatibility-matrix.md",
    "./browser-embedding-deployment.md",
    "./browser-uc-brokered-runtime-contract.md",
    "./browser-observability-contract.md",
    "./browser-datafusion-runtime-parity.md",
    "../adr/ADR-0008-daxis-browser-read-compute-contract.md",
    "./daxis-operational-maturity.md",
    "./daxis-external-proof-handoff.md",
    "../release-gates/daxis-production-rollout-decisions.json",
    "../release-gates/daxis-strategy-traceability.json",
    "../release-gates/daxis-external-proof-packet.json",
    "../release-gates/daxis-external-proof-attachment-template.md",
    "../release-gates/daxis-release-bundle-manifest.json",
    "../release-gates/daxis-release-attachment-template.md",
    "../release-gates/daxis-release-notes-template.md",
    "../release-gates/daxis-release-migration-notes-template.md",
    "../release-gates/browser-wasm-delta-gcs-launch-checklist.md",
    "../release-gates/browser-wasm-delta-gcs-external-blockers.md",
]:
    expect(required_path in text, f"{strategy_file} missing required related artifact link: {required_path}")

for link in re.findall(r"\[[^\]]+\]\(([^)]+)\)", text):
    check_strategy_link(link)

engine_strategy = read_required_program_doc(
    "browser-lakehouse-engine-strategy.md",
    "browser lakehouse engine strategy",
)
engine_strategy_doc = "docs/program/browser-lakehouse-engine-strategy.md"
for forbidden_text, description in [
    (
        "current browser preflight and narrow execution slice",
        "narrow-runtime scope",
    ),
    (
        "repo-owned V1 success claims",
        "V1 success-claim boundary",
    ),
]:
    reject_companion_text(engine_strategy, engine_strategy_doc, forbidden_text, description)
for required_text, description in [
    (
        "- Scope: make browser DataFusion the Daxis-facing default read engine while keeping legacy narrow execution isolated for compatibility",
        "DataFusion-default scope",
    ),
    (
        "Daxis-facing app worker is browser DataFusion-backed.",
        "Daxis-facing DataFusion worker default",
    ),
    (
        "Legacy narrow runtime and session shell remain compatibility-only.",
        "legacy narrow compatibility boundary",
    ),
]:
    expect_companion_text(engine_strategy, engine_strategy_doc, required_text, description)

sprint_board = read_required_program_doc(
    "browser-lakehouse-sprint-board.md",
    "browser lakehouse sprint board",
)
sprint_board_doc = "docs/program/browser-lakehouse-sprint-board.md"
for forbidden_text, description in [
    ("default narrow runtime SKU", "default narrow SKU label"),
    ("opt-in larger SQL SKU", "opt-in SQL SKU label"),
]:
    reject_companion_text(sprint_board, sprint_board_doc, forbidden_text, description)
for required_text, description in [
    ("Daxis-facing DataFusion runtime SKU", "Daxis-facing DataFusion SKU label"),
    ("legacy narrow compatibility SKU", "legacy narrow compatibility SKU label"),
    (
        "worker artifact reports enabled feature set, SKU identity, and browser DataFusion availability",
        "DataFusion artifact reporting scope",
    ),
]:
    expect_companion_text(sprint_board, sprint_board_doc, required_text, description)

for required_text in [
    "Axon should be developed as the default browser read-compute engine for Daxis.",
    "Daxis owns the headless query gateway",
    "Axon owns browser SQL execution",
    "browser runtime must never become authoritative for Daxis policy",
    "\"browser first, deterministic fallback\"",
    "Daxis should make Axon the default read compute path for interactive browser analytics",
    "Daxis owns every state through `routed`",
    "Axon owns only browser execution",
    "`BrowserHttpSnapshotDescriptor`",
    "`DeltaLocationResolveResponse`",
    "`ReadAccessPlan`",
    "`BrokeredDeltaReadPlan`",
    "`BrowserWorkerEventEnvelope`",
    "POST /v1/query/delta/snapshot-descriptor",
    "POST /v1/catalog/read-access-plan",
    "The M2 object-grant helper example lives in [`../../apps/axon-web/examples/daxis-object-grant-adapter.ts`](../../apps/axon-web/examples/daxis-object-grant-adapter.ts).",
    "keeps grant identifiers out of worker command payloads",
    "Daxis product platform, gateway, catalog, storage broker, and query service own policy, auth, audit, routing, and fallback",
    "DataFusion-backed SQL execution",
    "no hidden server calls from the default browser runtime",
    "stable request IDs from Daxis and worker-side query IDs from Axon",
    "unsupported SQL",
    "unsupported Delta protocol features",
    "browser budget exceeded",
    "signed URL expired",
    "CORS blocked",
    "warehouse/server fallback required",
    "Browser code must never receive raw credentials",
    "service-account JSON",
    "OAuth tokens",
    "HMAC keys",
    "fallback reason",
    "Arrow IPC byte length",
    "server-side correlation ID",
    "audit evidence for read access",
    "release-process attachments",
    "artifact SHA-256 digest",
    "Daxis release channel",
    "allowedReleaseChannels",
    "requiredReleaseChannel",
    "stable default promotion requires `stable`",
    "allowedDaxisWorktreeStatuses",
    "allowedDaxisWorktreeReviews",
    "acceptedDaxisWorktreeReviews",
    "stable default promotion accepts only `clean` or digest-pinned `dirty_reviewed`",
    "rollout segment",
    "releaseAttachmentSchema.checksumFormat",
    "releaseAttachmentSchema.requiredReviewerRoles",
    "releaseAttachmentSchema.stableDefaultValidationCommand",
    "releaseAttachmentSchema.stableDefaultDirectoryValidationCommand",
    "releaseAttachmentSchema.stableDefaultArtifactValidationCommand",
    "releaseAttachmentSchema.stableDefaultArtifactDirectoryValidationCommand",
    "stableDefaultPromotionPacketValidationCommand",
    "verify_daxis_stable_default_promotion_packet.sh",
    "query-result semantics, Daxis result metrics and observability fields, fallback behavior",
    "public GCS live-smoke output or skip-safe blocker record",
    "Daxis-facing release notes use `docs/release-gates/daxis-release-notes-template.md` for semantic, Daxis result metrics and observability fields, fallback, compatibility, descriptor, error-taxonomy, runtime-budget, worker-artifact, and trust-boundary changes.",
    "Daxis-facing migration notes use `docs/release-gates/daxis-release-migration-notes-template.md` for breaking changes or explicit no-breaking-change statements.",
    "Release-process evidence uses `docs/release-gates/daxis-release-attachment-template.md` for git SHA, worker size, public GCS live smoke, release notes, and migration notes, including `release_channel`, `rollout_segment`, and `releaseAttachmentSchema.allowedReleaseChannels`.",
    "Daxis-owned production proof uses `docs/release-gates/daxis-external-proof-attachment-template.md` and `docs/release-gates/daxis-dirty-worktree-review-template.json` for dirty-checkout reviews, and attaches `release_channel`, production `environment_class`, `axon_release_commit_sha`, `axon_release_ref`, `proofAttachmentSchema.allowedReleaseChannels`, `proofAttachmentSchema.acceptedDaxisWorktreeReviews`, the `daxis.external_state.v1` JSON summary, its SHA-256 digest, and clean or digest-pinned dirty-reviewed Daxis worktree classification before stable default routing.",
    "Recommended release channels",
    "Treat docs, tests, and release evidence as part of the change",
    "stableDefaultPromotionGate",
    "requiredReleaseAttachmentSchemaFields",
    "requiredProofAttachmentSchemaFields",
    "requiredReleaseEvidenceArtifactCommand",
    "releaseAttachmentSchema.allowedReleaseChannels",
    "releaseAttachmentSchema.stableDefaultValidationCommand",
    "releaseAttachmentSchema.stableDefaultDirectoryValidationCommand",
    "releaseAttachmentSchema.stableDefaultArtifactValidationCommand",
    "releaseAttachmentSchema.stableDefaultArtifactDirectoryValidationCommand",
    "proofAttachmentSchema.stableDefaultValidationCommand",
    "proofAttachmentSchema.stableDefaultDirectoryValidationCommand",
    "proofAttachmentSchema.stableDefaultArtifactValidationCommand",
    "proofAttachmentSchema.stableDefaultArtifactDirectoryValidationCommand",
    "requiredReleaseEvidenceArtifactCommand",
    "stableDefaultPromotionPacketValidationCommand",
    "verify_daxis_stable_default_promotion_packet.sh",
    "proofAttachmentSchema.allowedReleaseChannels",
    "proofAttachmentSchema.allowedDaxisWorktreeStatuses",
    "proofAttachmentSchema.allowedDaxisWorktreeReviews",
    "proofAttachmentSchema.acceptedDaxisWorktreeReviews",
    "proofAttachmentSchema.dirtyWorktreeReviewTemplatePath",
    "channel-scoped",
    "segment-scoped",
    "production-environment-scoped",
    "Axon-release-identity-pinned",
    "currentPromotionState",
    "blocked_external_proof_required",
    "full release-evidence output",
    "external blocker register",
    "`server_fallback` rollback evidence",
    "Daxis architecture names Axon as the default browser read engine.",
    "Server fallback remains available and reason-preserving.",
    "Daxis can choose descriptor, grant, proxy, Delta Sharing, server fallback, or blocked states per table.",
    "Goal: make Axon safe to operate as default read compute.",
    "Interactive read queries use browser execution by default where policy allows.",
    "Daxis can explain why each query used browser execution, server fallback, or policy block.",
    "Daxis owner attachment of the proof artifacts listed in the external proof packet",
    "Stable default routing is gated on `docs/release-gates/daxis-external-proof-packet.json` `stableDefaultPromotionGate` acceptance, `requiredReleaseChannel` `stable`, `requiredReleaseEvidenceArtifactCommand`, and `stableDefaultPromotionPacketValidationCommand`.",
    "Make fallback deterministic.",
    "Make release evidence repeatable.",
]:
    expect_text(required_text, "strategy requirement")

required_commands = [
    "bash tests/conformance/verify_daxis_strategy_document.sh",
    "cargo fmt --check",
    "bash tests/conformance/verify_daxis_query_corpus_coverage.sh",
    "bash tests/conformance/verify_daxis_contract_artifacts.sh",
    "cargo test -p wasm-datafusion-poc --test daxis_budget_profile",
    "cargo test -p wasm-datafusion-poc --test daxis_runtime_isolation_plan",
    "bash tests/conformance/verify_daxis_release_bundle_manifest.sh",
    "bash tests/conformance/verify_daxis_strategy_traceability.sh",
    "bash tests/conformance/verify_daxis_external_proof_packet.sh",
    "bash tests/conformance/verify_daxis_operational_readiness.sh",
    "bash tests/conformance/verify_daxis_release_evidence.sh",
    "bash tests/conformance/verify_daxis_rollout_decisions.sh",
    "bash tests/conformance/verify_daxis_pr_checklist.sh",
]
for command in required_commands:
    expect_text(command, "strategy command reference")

release_evidence_runner = repo_root / "tests/conformance/verify_daxis_release_evidence.sh"
expect(release_evidence_runner.is_file(), "missing Daxis release evidence runner")
try:
    listed_release_commands = subprocess.check_output(
        ["bash", str(release_evidence_runner), "--list"],
        cwd=repo_root,
        stderr=subprocess.PIPE,
        text=True,
    ).splitlines()
except subprocess.CalledProcessError as error:
    fail("Daxis release evidence runner --list failed: " + error.stderr.strip())

listed_release_command_set = {
    command.strip()
    for command in listed_release_commands
    if command.strip()
}
for command in [
    "bash tests/conformance/verify_daxis_strategy_document_test.sh",
    "bash tests/conformance/verify_daxis_strategy_document.sh",
    "bash tests/conformance/verify_daxis_query_corpus_coverage_test.sh",
    "bash tests/conformance/verify_daxis_query_corpus_coverage.sh",
    "bash tests/conformance/verify_daxis_contract_artifacts.sh",
    "bash tests/conformance/verify_daxis_rollout_decisions.sh",
    "bash tests/conformance/verify_daxis_operational_readiness.sh",
    "bash tests/conformance/verify_daxis_strategy_traceability.sh",
    "bash tests/conformance/verify_daxis_external_proof_packet.sh",
    "bash tests/conformance/verify_daxis_architecture_adr.sh",
    "bash tests/conformance/verify_daxis_release_bundle_manifest.sh",
    "bash tests/conformance/verify_daxis_pr_checklist.sh",
]:
    expect(command in listed_release_command_set, f"release evidence runner missing strategy command: {command}")

print("Daxis strategy document verified")
PY
