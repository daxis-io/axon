#!/usr/bin/env bash

set -euo pipefail

repo_root="${AXON_DAXIS_RELEASE_EVIDENCE_REPO_ROOT:-$(pwd)}"

commands=(
	"cargo check --workspace --locked"
	"cargo fmt --check"
	"cargo test -p wasm-datafusion-poc -- --test-threads=1"
	"cargo test -p query-contract"
	"cargo test -p wasm-datafusion-poc --test daxis_query_corpus"
	"bash tests/conformance/verify_daxis_query_corpus_coverage_test.sh"
	"bash tests/conformance/verify_daxis_query_corpus_coverage.sh"
	"cargo test -p wasm-datafusion-session"
	"cargo test -p axon-web-wasm"
	"cargo test -p query-router -p native-query-runtime -p delta-control-plane -p wasm-query-runtime -p wasm-query-session -p wasm-http-object-store -p wasm-parquet-engine -p wasm-delta-snapshot -p browser-sdk -p browser-engine-worker"
	"bash tests/conformance/verify_daxis_contract_artifacts_test.sh"
	"bash tests/conformance/verify_daxis_contract_artifacts.sh"
	"bash tests/conformance/verify_browser_worker_dependency_boundary.sh"
	"bash tests/conformance/verify_browser_observability_contract_test.sh"
	"bash tests/conformance/verify_browser_observability_contract.sh"
	"bash tests/conformance/verify_axon_web_datafusion_runtime.sh"
	"bash tests/conformance/verify_daxis_strategy_document_test.sh"
	"bash tests/conformance/verify_daxis_strategy_document.sh"
	"bash tests/conformance/verify_daxis_rollout_decisions_test.sh"
	"bash tests/conformance/verify_daxis_rollout_decisions.sh"
	"bash tests/conformance/verify_daxis_operational_readiness_test.sh"
	"bash tests/conformance/verify_daxis_operational_readiness.sh"
	"bash tests/conformance/verify_daxis_strategy_traceability_test.sh"
	"bash tests/conformance/verify_daxis_strategy_traceability.sh"
	"bash tests/conformance/verify_daxis_external_state_test.sh"
	"bash tests/conformance/verify_daxis_external_proof_attachment_test.sh"
	"bash tests/conformance/verify_daxis_external_proof_packet_test.sh"
	"bash tests/conformance/verify_daxis_external_proof_packet.sh"
	"bash tests/conformance/verify_daxis_architecture_adr_test.sh"
	"bash tests/conformance/verify_daxis_architecture_adr.sh"
	"bash tests/conformance/verify_daxis_release_attachment_test.sh"
	"bash tests/conformance/verify_daxis_stable_default_promotion_packet_test.sh"
	"bash tests/conformance/verify_daxis_release_bundle_manifest_test.sh"
	"bash tests/conformance/verify_daxis_release_bundle_manifest.sh"
	"bash tests/conformance/verify_daxis_pr_checklist_test.sh"
	"bash tests/conformance/verify_daxis_pr_checklist.sh"
	"bash tests/conformance/verify_daxis_release_evidence_test.sh"
	"bash tests/perf/report_datafusion_wasm_size_test.sh"
	"cargo check -p wasm-http-object-store -p wasm-parquet-engine -p wasm-delta-snapshot -p wasm-query-runtime -p wasm-query-session -p browser-sdk -p browser-engine-worker --target wasm32-unknown-unknown --locked"
	"npm run build:fixture"
	"npm run build:wasm"
	"npm exec -- tsc --noEmit"
	"npm run test:sdk"
	"env AXON_BROWSER_DEPENDENCY_PACKAGE=axon-web-wasm bash tests/security/verify_browser_dependency_guardrails.sh target/wasm32-unknown-unknown/release/axon_web_wasm.wasm"
	"npm exec -- playwright test --config=playwright.config.ts --grep \"Daxis descriptor-resolver|preserves cancellation errors|surfaces unsupported feature errors\""
	"npm run test:browser:public-gcs-live -- --reporter=line"
)

list_commands() {
	printf "%s\n" "${commands[@]}"
}

run_step() {
	printf '+'
	local arg
	for arg in "$@"; do
		printf ' %q' "$arg"
	done
	printf '\n'
	"$@"
}

run_release_evidence() {
	cd "$repo_root"
	run_step cargo check --workspace --locked
	run_step cargo fmt --check
	run_step cargo test -p wasm-datafusion-poc -- --test-threads=1
	run_step cargo test -p query-contract
	run_step cargo test -p wasm-datafusion-poc --test daxis_query_corpus
	run_step bash tests/conformance/verify_daxis_query_corpus_coverage_test.sh
	run_step bash tests/conformance/verify_daxis_query_corpus_coverage.sh
	run_step cargo test -p wasm-datafusion-session
	run_step cargo test -p axon-web-wasm
	run_step cargo test -p query-router -p native-query-runtime -p delta-control-plane -p wasm-query-runtime -p wasm-query-session -p wasm-http-object-store -p wasm-parquet-engine -p wasm-delta-snapshot -p browser-sdk -p browser-engine-worker
	run_step bash tests/conformance/verify_daxis_contract_artifacts_test.sh
	run_step bash tests/conformance/verify_daxis_contract_artifacts.sh
	run_step bash tests/conformance/verify_browser_worker_dependency_boundary.sh
	run_step bash tests/conformance/verify_browser_observability_contract_test.sh
	run_step bash tests/conformance/verify_browser_observability_contract.sh
	run_step bash tests/conformance/verify_axon_web_datafusion_runtime.sh
	run_step bash tests/conformance/verify_daxis_strategy_document_test.sh
	run_step bash tests/conformance/verify_daxis_strategy_document.sh
	run_step bash tests/conformance/verify_daxis_rollout_decisions_test.sh
	run_step bash tests/conformance/verify_daxis_rollout_decisions.sh
	run_step bash tests/conformance/verify_daxis_operational_readiness_test.sh
	run_step bash tests/conformance/verify_daxis_operational_readiness.sh
	run_step bash tests/conformance/verify_daxis_strategy_traceability_test.sh
	run_step bash tests/conformance/verify_daxis_strategy_traceability.sh
	run_step bash tests/conformance/verify_daxis_external_state_test.sh
	run_step bash tests/conformance/verify_daxis_external_proof_attachment_test.sh
	run_step bash tests/conformance/verify_daxis_external_proof_packet_test.sh
	run_step bash tests/conformance/verify_daxis_external_proof_packet.sh
	run_step bash tests/conformance/verify_daxis_architecture_adr_test.sh
	run_step bash tests/conformance/verify_daxis_architecture_adr.sh
	run_step bash tests/conformance/verify_daxis_release_attachment_test.sh
	run_step bash tests/conformance/verify_daxis_stable_default_promotion_packet_test.sh
	run_step bash tests/conformance/verify_daxis_release_bundle_manifest_test.sh
	run_step bash tests/conformance/verify_daxis_release_bundle_manifest.sh
	run_step bash tests/conformance/verify_daxis_pr_checklist_test.sh
	run_step bash tests/conformance/verify_daxis_pr_checklist.sh
	run_step bash tests/conformance/verify_daxis_release_evidence_test.sh
	run_step bash tests/perf/report_datafusion_wasm_size_test.sh
	run_step cargo check -p wasm-http-object-store -p wasm-parquet-engine -p wasm-delta-snapshot -p wasm-query-runtime -p wasm-query-session -p browser-sdk -p browser-engine-worker --target wasm32-unknown-unknown --locked
	(
		cd "$repo_root/apps/axon-web"
		run_step npm run build:fixture
		run_step npm run build:wasm
		run_step npm exec -- tsc --noEmit
		run_step npm run test:sdk
	)
	run_step env AXON_BROWSER_DEPENDENCY_PACKAGE=axon-web-wasm bash tests/security/verify_browser_dependency_guardrails.sh target/wasm32-unknown-unknown/release/axon_web_wasm.wasm
	(
		cd "$repo_root/apps/axon-web"
		run_step npm exec -- playwright test --config=playwright.config.ts --grep "Daxis descriptor-resolver|preserves cancellation errors|surfaces unsupported feature errors"
		run_step npm run test:browser:public-gcs-live -- --reporter=line
	)
}

sha256_file() {
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$1" | awk '{print $1}'
		return
	fi

	shasum -a 256 "$1" | awk '{print $1}'
}

write_log_artifact() {
	local output_path="$1"
	if [[ -z "$output_path" ]]; then
		echo "usage: $0 --write-log <path>" >&2
		exit 2
	fi

	local output_dir
	local output_name
	output_dir="$(dirname "$output_path")"
	output_name="$(basename "$output_path")"
	mkdir -p "$output_dir"

	local output_dir_abs
	output_dir_abs="$(cd "$output_dir" && pwd -P)"
	local output_path_abs="$output_dir_abs/$output_name"
	local temp_path="$output_dir_abs/.release-evidence.$output_name.$$.tmp"
	local status=0

	if run_release_evidence >"$temp_path" 2>&1; then
		status=0
	else
		status=$?
	fi
	# Copy instead of rename so async descendants that inherited stdout/stderr
	# cannot mutate the published artifact after its digest is printed.
	cp "$temp_path" "$output_path_abs"
	rm -f "$temp_path"

	printf 'daxis_release_evidence_log_path=%s\n' "$output_path"
	printf 'daxis_release_evidence_log_sha256=%s\n' "$(sha256_file "$output_path_abs")"
	printf 'daxis_release_evidence_exit_status=%s\n' "$status"
	return "$status"
}

case "${1:-}" in
--list)
	list_commands
	;;
--write-log)
	if [[ $# -ne 2 ]]; then
		echo "usage: $0 --write-log <path>" >&2
		exit 2
	fi
	write_log_artifact "$2"
	;;
"")
	run_release_evidence
	;;
*)
	echo "usage: $0 [--list|--write-log <path>]" >&2
	exit 2
	;;
esac
