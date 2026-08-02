#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../../.." && pwd)"
web_root="${repo_root}/apps/axon-web"
fixture_root="${AXON_BROWSER_EXTERNAL_MEMORY_FIXTURE_ROOT:-${repo_root}/target/fixtures/browser-external-memory-v1}"
manifest="${fixture_root}/fixture-manifest.json"
oracle="${fixture_root}/native-oracle.json"
table_root="${fixture_root}/table"

verify_metadata() {
  for required in "${manifest}" "${oracle}" "${table_root}/_delta_log/00000000000000000000.json"; do
    if [[ ! -f "${required}" ]]; then
      echo "missing required browser external-memory fixture input: ${required}" >&2
      exit 1
    fi
  done

  jq -e '
    .schema_version == 1 and
    .fixture_revision == "browser-external-memory-v1" and
    .row_count == 1600000 and
    .group_count == 800000 and
    .active_file_count == 16 and
    ([.queries[].id] | sort) == ["aggregate", "aggregate_states", "external_sort"] and
    ([.queries[].expected_operator] | sort) ==
      ["EXTERNAL_SORT", "GROUPED_AGGREGATE", "GROUPED_AGGREGATE"] and
    (.objects | length) > 0 and
    ([.objects[].sha256 | test("^[0-9a-f]{64}$")] | all)
  ' "${manifest}" >/dev/null

  jq -e '
    .schema_version == 1 and
    .fixture_revision == "browser-external-memory-v1" and
    ([.queries[].id] | sort) == ["aggregate", "aggregate_states", "external_sort"]
  ' "${oracle}" >/dev/null

  while IFS=$'\t' read -r relative_path expected_sha; do
    case "${relative_path}" in
      /*|*../*)
        echo "unsafe fixture object path: ${relative_path}" >&2
        exit 1
        ;;
    esac
    object="${table_root}/${relative_path}"
    if [[ ! -f "${object}" ]]; then
      echo "missing fixture object: ${object}" >&2
      exit 1
    fi
    actual_sha="$(shasum -a 256 "${object}" | awk '{print $1}')"
    if [[ "${actual_sha}" != "${expected_sha}" ]]; then
      echo "fixture checksum mismatch: ${relative_path}" >&2
      exit 1
    fi
  done < <(jq -r '.objects[] | [.relative_path, .sha256] | @tsv' "${manifest}")

  echo "Verified browser external-memory fixture metadata at ${fixture_root}"
}

if [[ "${1:-}" == "--metadata-only" ]]; then
  verify_metadata
  exit 0
fi

if [[ $# -ne 0 ]]; then
  echo "usage: $0 [--metadata-only]" >&2
  exit 2
fi

stress_table="${AXON_STRESS_DELTA_PATH:-}"
if [[ -z "${stress_table}" || ! -d "${stress_table}" ]]; then
  echo "AXON_STRESS_DELTA_PATH is required for the complete browser external-memory gate" >&2
  exit 1
fi

cd "${web_root}"
npm run build:spill-conformance-fixture
verify_metadata
npm run build:wasm

server_port="${AXON_BROWSER_EXTERNAL_MEMORY_PORT:-5174}"
base_url="https://127.0.0.1:${server_port}"
server_log="${repo_root}/target/browser-external-memory-vite.log"
npm run dev:server -- --port "${server_port}" >"${server_log}" 2>&1 &
server_pid=$!
cleanup() {
  kill "${server_pid}" 2>/dev/null || true
  wait "${server_pid}" 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 60); do
  if curl --silent --insecure --fail "${base_url}/" >/dev/null; then
    break
  fi
  if ! kill -0 "${server_pid}" 2>/dev/null; then
    cat "${server_log}" >&2
    exit 1
  fi
  sleep 1
done
curl --silent --insecure --fail "${base_url}/" >/dev/null

projects="${AXON_BROWSER_EXTERNAL_MEMORY_PROJECTS:-chromium,firefox,webkit}"
for profile in 64 128; do
  IFS=',' read -r -a project_list <<<"${projects}"
  for project in "${project_list[@]}"; do
    webkit_persistent=0
    if [[ "${project}" == "webkit" ]]; then
      webkit_persistent=1
    fi
    AXON_EDITOR_BROWSER_MATRIX=1 \
    AXON_WEBKIT_PERSISTENT="${webkit_persistent}" \
    AXON_SPILL_CONFORMANCE_PATH="${table_root}" \
    AXON_BROWSER_MEMORY_PROFILE_MIB="${profile}" \
    PLAYWRIGHT_BASE_URL="${base_url}" \
      npm run test:browser:editor-smoke -- \
        --project="${project}" \
        --grep "browser external-memory conformance corpus"
  done
done

IFS=',' read -r -a project_list <<<"${projects}"
for project in "${project_list[@]}"; do
  webkit_persistent=0
  if [[ "${project}" == "webkit" ]]; then
    webkit_persistent=1
  fi
  AXON_EDITOR_BROWSER_MATRIX=1 \
  AXON_WEBKIT_PERSISTENT="${webkit_persistent}" \
  AXON_SPILL_CONFORMANCE_PATH="${table_root}" \
  AXON_BROWSER_MEMORY_PROFILE_MIB=128 \
  AXON_SPILL_WARM_REPEAT_COUNT=10 \
  PLAYWRIGHT_BASE_URL="${base_url}" \
    npm run test:browser:editor-smoke -- \
      --project="${project}" \
      --grep "browser external-memory conformance corpus"
done

if [[ ",${projects}," == *,webkit,* ]]; then
  AXON_EDITOR_BROWSER_MATRIX=1 \
  AXON_WEBKIT_PRIVATE_OPFS=1 \
  PLAYWRIGHT_BASE_URL="${base_url}" \
    npm run test:browser:editor-smoke -- \
      --project=webkit \
      --grep "keeps non-spilling queries available when private WebKit cannot use OPFS"
fi

AXON_EDITOR_BROWSER_MATRIX=1 \
AXON_STRESS_DELTA_PATH="${stress_table}" \
AXON_BROWSER_MEMORY_PROFILE_MIB=64 \
PLAYWRIGHT_BASE_URL="${base_url}" \
  npm run test:browser:editor-smoke -- \
    --project=chromium \
    --grep "spills the original high-cardinality stress aggregate"

AXON_EDITOR_BROWSER_MATRIX=1 \
AXON_SPILL_CONCURRENT_TABS=1 \
AXON_STRESS_DELTA_PATH="${stress_table}" \
AXON_BROWSER_MEMORY_PROFILE_MIB=128 \
PLAYWRIGHT_BASE_URL="${base_url}" \
  npm run test:browser:editor-smoke -- \
    --project=chromium \
    --grep "isolates simultaneous OPFS spill scopes across two same-origin tabs"

echo "Browser external-memory conformance passed at 64 MiB and 128 MiB"
