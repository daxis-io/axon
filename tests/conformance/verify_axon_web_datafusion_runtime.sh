#!/usr/bin/env bash

set -euo pipefail

tree="$(cargo tree -p axon-web-wasm --target wasm32-unknown-unknown --locked)"
artifact_report="docs/program/browser-lakehouse-release-handoff-examples/browser-worker-artifact-report.datafusion.json"
release_handoff="docs/program/browser-lakehouse-release-handoff.md"
embedding_deployment="docs/program/browser-embedding-deployment.md"
observability_contract="docs/program/browser-observability-contract.md"
security_readme="tests/security/README.md"
external_blockers="docs/release-gates/browser-wasm-delta-gcs-external-blockers.md"
launch_checklist="docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md"
release_evidence="docs/release-gates/browser-wasm-delta-gcs-release-evidence.md"
ci_workflow=".github/workflows/ci.yml"
root_readme="README.md"
perf_readme="tests/perf/README.md"
size_audit="docs/program/browser-datafusion-size-audit.md"

if ! rg -n '(^|[[:space:]])wasm-datafusion-session v' <<<"$tree"; then
  echo "axon-web-wasm production wasm runtime must use the dedicated DataFusion session crate" >&2
  exit 1
fi

if ! rg -n '(^|[[:space:]])wasm-datafusion-poc v' <<<"$tree"; then
  echo "axon-web-wasm production wasm runtime must include the DataFusion session path" >&2
  exit 1
fi

if ! rg -n '(^|[[:space:]])datafusion v' <<<"$tree"; then
  echo "axon-web-wasm production wasm runtime must include DataFusion" >&2
  exit 1
fi

if rg -n '(^|[[:space:]])wasm-query-session v' <<<"$tree"; then
  echo "axon-web-wasm production wasm runtime must not depend on the legacy narrow query session" >&2
  exit 1
fi

if rg -n 'Browser execution V1 is the narrow runtime' "$release_handoff"; then
  echo "release handoff must not lead with the legacy narrow runtime as the current browser execution model" >&2
  exit 1
fi

if ! rg -n 'Daxis-facing app worker .*`browser_datafusion`.*default runtime SKU' "$release_handoff"; then
  echo "release handoff must name browser_datafusion as the Daxis-facing default runtime SKU" >&2
  exit 1
fi

if ! rg -n 'legacy narrow worker remains compatibility-only' "$release_handoff"; then
  echo "release handoff must keep the legacy narrow worker compatibility-only" >&2
  exit 1
fi

if rg -n 'packaged browser output is TBD until the JS worker bootstrap exists|/workers/browser-engine-worker\.js|/workers/browser_engine_worker\.wasm|/vendor/axon/browser-engine-worker\.js|/vendor/axon/browser_engine_worker\.wasm|@axon/browser/worker/browser-engine-worker\.js' "$embedding_deployment"; then
  echo "embedding deployment guide must not present legacy narrow worker artifacts as the Daxis-facing default bundle" >&2
  exit 1
fi

if ! rg -n 'Daxis-facing default bundle .*`axon-web-worker\.js`.*`axon_web_wasm_bg\.wasm`' "$embedding_deployment"; then
  echo "embedding deployment guide must name the Daxis-facing default bundle artifacts" >&2
  exit 1
fi

if ! rg -n '`apps/axon-web/src/sandbox-query-worker\.ts`.*`axon_web_wasm_bg\.wasm`' "$embedding_deployment"; then
  echo "embedding deployment guide must tie the Daxis-facing worker entrypoint to axon_web_wasm_bg.wasm" >&2
  exit 1
fi

python3 - "$artifact_report" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    report = json.load(handle)

expectations = {
    "runtime_sku": "browser_datafusion",
    "default_worker_sku": True,
    "result_transport": "arrow_ipc",
}
for key, expected in expectations.items():
    actual = report.get(key)
    if actual != expected:
        raise SystemExit(f"{sys.argv[1]} {key} must be {expected!r}, found {actual!r}")

package_name = report.get("identity", {}).get("package_name")
if package_name != "axon-web-wasm":
    raise SystemExit(f"{sys.argv[1]} identity.package_name must be 'axon-web-wasm', found {package_name!r}")
PY

if rg -n '`runtime_sku`: `narrow` for the shipped browser runtime' "$observability_contract"; then
  echo "browser observability contract must not describe the shipped Daxis runtime SKU as narrow" >&2
  exit 1
fi

if ! rg -n '`runtime_sku`: `browser_datafusion`' "$observability_contract"; then
  echo "browser observability contract must name browser_datafusion as the Daxis-facing default runtime SKU" >&2
  exit 1
fi

if rg -n 'current browser V1 remains narrow runtime' "$security_readme"; then
  echo "security README must not describe the current Daxis-facing browser runtime as narrow-only" >&2
  exit 1
fi

if ! rg -n 'Daxis-facing .*browser DataFusion' "$security_readme"; then
  echo "security README must name the Daxis-facing browser DataFusion default path" >&2
  exit 1
fi

if rg -n 'outside repo-owned success claims' "$security_readme" "$ci_workflow"; then
  echo "security docs and CI validation must qualify external service proof against repo-owned browser-engine success claims" >&2
  exit 1
fi

if ! rg -n 'outside repo-owned browser-engine success claims' "$security_readme"; then
  echo "security README must qualify external service proof against repo-owned browser-engine success claims" >&2
  exit 1
fi

if rg -n 'shipped V1 remains narrow runtime|browser DataFusion as the default execution SKU' "$external_blockers"; then
  echo "external blockers must not list browser DataFusion default execution as deferred repo scope" >&2
  exit 1
fi

if rg -n 'outside repo-owned success claims' "$external_blockers" "$launch_checklist"; then
  echo "release-gate docs must qualify external service proof as outside repo-owned browser-engine success claims" >&2
  exit 1
fi

if ! rg -n 'outside repo-owned browser-engine success claims' "$external_blockers"; then
  echo "external blockers must qualify external service proof against repo-owned browser-engine success claims" >&2
  exit 1
fi

if ! rg -n 'outside repo-owned browser-engine success claims' "$launch_checklist"; then
  echo "launch checklist must qualify external service proof against repo-owned browser-engine success claims" >&2
  exit 1
fi

if rg -n 'Browser release-artifact size reporting is enforced in CI on the real `browser-engine-worker\.wasm` artifact' "$launch_checklist"; then
  echo "launch checklist must not describe the legacy narrow worker size gate as the whole browser release-artifact size gate" >&2
  exit 1
fi

if ! rg -n 'Legacy narrow worker artifact size reporting remains enforced in CI on the real `browser-engine-worker\.wasm` artifact' "$launch_checklist"; then
  echo "launch checklist must keep legacy narrow worker artifact size reporting scoped to compatibility evidence" >&2
  exit 1
fi

if ! rg -n 'Daxis DataFusion default worker size reporting remains release-process evidence .*`axon-web-wasm`' "$launch_checklist"; then
  echo "launch checklist must name axon-web-wasm size reporting as Daxis default-worker release-process evidence" >&2
  exit 1
fi

if rg -n '`wasm-parquet-engine` / `wasm-query-session` split' "$launch_checklist"; then
  echo "launch checklist must not describe Delta compatibility around the legacy narrow session split" >&2
  exit 1
fi

if ! rg -n '`wasm-parquet-engine`, `wasm-datafusion-session`, and legacy `wasm-query-session` compatibility isolation' "$launch_checklist"; then
  echo "launch checklist must name wasm-datafusion-session as the Daxis default session boundary and legacy wasm-query-session as isolated compatibility evidence" >&2
  exit 1
fi

if rg -n 'A shipped worker artifact with broad browser DataFusion|not broad browser DataFusion|browser_datafusion = false` on purpose|browser DataFusion remains deferred|browser DataFusion as the default execution SKU' "$root_readme" "$perf_readme" "$ci_workflow"; then
  echo "README, perf docs, and CI validation must not describe browser DataFusion default-worker evidence as deferred or absent" >&2
  exit 1
fi

if ! rg -n 'Daxis-facing .*`axon-web-wasm`.*browser DataFusion.*default worker' "$root_readme"; then
  echo "README must name axon-web-wasm as the Daxis-facing browser DataFusion default worker" >&2
  exit 1
fi

if ! rg -n 'legacy `browser-engine-worker`.*compatibility' "$root_readme"; then
  echo "README must keep browser-engine-worker scoped to legacy compatibility evidence" >&2
  exit 1
fi

if ! rg -n 'Daxis-facing app worker is browser DataFusion-backed' "$perf_readme"; then
  echo "perf README must name the Daxis-facing browser DataFusion-backed app worker" >&2
  exit 1
fi

if ! rg -n 'legacy narrow worker artifact.*`browser_datafusion = false`' "$perf_readme"; then
  echo "perf README must scope browser_datafusion=false to the legacy narrow worker artifact" >&2
  exit 1
fi

if ! rg -n 'DataFusion default worker.*`browser_datafusion = true`' "$perf_readme"; then
  echo "perf README must name browser_datafusion=true for the DataFusion default worker artifact" >&2
  exit 1
fi

if rg -n 'Validate repo-grounded browser V1 docs' "$ci_workflow"; then
  echo "CI doc validation step must not describe Daxis-facing browser docs as browser V1" >&2
  exit 1
fi

if ! rg -n 'Validate browser DataFusion and legacy compatibility docs' "$ci_workflow"; then
  echo "CI doc validation step must name the browser DataFusion and legacy compatibility boundary" >&2
  exit 1
fi

if rg -F -n 'rg -q "narrow runtime \+ streaming scan \+ in-memory session shell"' "$ci_workflow"; then
  echo "CI doc validation must not only check the old narrow runtime component phrase" >&2
  exit 1
fi

if ! rg -n 'legacy \.\*compatibility\|legacy narrow \.\*browser_datafusion = false' "$ci_workflow"; then
  echo "CI doc validation must require legacy runtime wording to be scoped to compatibility evidence" >&2
  exit 1
fi

if ! rg -n 'Daxis-facing \.\*browser DataFusion\|Daxis-facing \.\*`browser_datafusion`\.\*default runtime SKU\|browser DataFusion-backed\|DataFusion default worker' "$ci_workflow"; then
  echo "CI doc validation must require the Daxis-facing browser DataFusion default boundary" >&2
  exit 1
fi

if rg -n '\bV1\b' "$size_audit"; then
  echo "DataFusion size audit must use explicit Daxis default-profile wording instead of ambiguous browser V1 labels" >&2
  exit 1
fi

if ! rg -n 'Outside Browser DataFusion Default Profile' "$size_audit"; then
  echo "DataFusion size audit must name the out-of-profile list against the browser DataFusion default profile" >&2
  exit 1
fi

if rg -n 'The in-memory session shell and session-backed worker contract stay repo-owned and testable' "$release_evidence"; then
  echo "release evidence must scope the session-backed worker row as legacy compatibility evidence" >&2
  exit 1
fi

if ! rg -n 'The legacy in-memory session shell and compatibility worker contract stay repo-owned and testable' "$release_evidence"; then
  echo "release evidence must name the legacy session shell as compatibility worker evidence" >&2
  exit 1
fi

if ! rg -n 'The app WASM DataFusion runtime is the Daxis-facing default worker SKU' "$release_evidence"; then
  echo "release evidence must keep the app WASM DataFusion runtime as the Daxis-facing default worker SKU" >&2
  exit 1
fi

echo "axon-web WASM DataFusion runtime verified"
