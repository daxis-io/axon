#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
app_dir="${repo_root}/apps/axon-web"
artifact_path="${AXON_BROWSER_QUERY_PERF_ARTIFACT:-${repo_root}/target/perf/browser-query-performance.json}"

case "$artifact_path" in
  "${repo_root}"/target/perf/*.json)
    ;;
  *)
    echo "AXON_BROWSER_QUERY_PERF_ARTIFACT must be a JSON file under target/perf" >&2
    exit 1
    ;;
esac

mkdir -p "$(dirname "$artifact_path")"
export AXON_BROWSER_QUERY_PERF_ARTIFACT="$artifact_path"
export AXON_BROWSER_QUERY_PERF_GIT_SHA="${AXON_BROWSER_QUERY_PERF_GIT_SHA:-$(git rev-parse HEAD)}"

if [[ "${AXON_BROWSER_QUERY_PERF_SKIP_BUILD:-0}" != "1" ]]; then
  (
    cd "$app_dir"
    npm run build:wasm
  )
fi

(
  cd "$app_dir"
  npm run test:browser:query-performance -- "$@"
)

if [[ ! -s "$artifact_path" ]]; then
  echo "browser query performance artifact was not written: $artifact_path" >&2
  exit 1
fi

echo "Browser query performance artifact: $artifact_path"
