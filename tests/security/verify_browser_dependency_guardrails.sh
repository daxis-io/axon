#!/usr/bin/env bash

set -euo pipefail

artifact="${1:-target/wasm32-unknown-unknown/release/browser_engine_worker.wasm}"
dependency_package="${AXON_BROWSER_DEPENDENCY_PACKAGE:-browser-engine-worker}"
tree_file="$(mktemp)"
trap 'rm -f "$tree_file"' EXIT

if [ -n "${AXON_BROWSER_WORKER_TREE_FILE:-}" ]; then
  cp "$AXON_BROWSER_WORKER_TREE_FILE" "$tree_file"
else
  cargo tree -p "$dependency_package" --target wasm32-unknown-unknown --locked --prefix none > "$tree_file"
fi

denylist='^(aws-config|aws-credential-types|aws-sdk-[^[:space:]]*|aws-smithy-[^[:space:]]*|azure_core|azure_identity|azure_storage|gcp_auth|google-cloud-[^[:space:]]*|jsonwebtoken|opendal|yup-oauth2)([[:space:]]|$)'

if grep -E -n "$denylist" "$tree_file" >/dev/null; then
  echo "browser dependency guardrails failed: denylisted dependency detected" >&2
  grep -E -n "$denylist" "$tree_file" >&2
  exit 1
fi

browser_package_denylist='(@aws-sdk/|aws-sdk|google-auth-library|@google-cloud/storage|azure-identity|@azure/storage-blob|opendal)'
browser_package_files="${AXON_BROWSER_PACKAGE_FILES:-apps/axon-web/package.json apps/axon-web/package-lock.json}"
package_file_args=()
for package_file in $browser_package_files; do
  if [ -f "$package_file" ]; then
    package_file_args+=("$package_file")
  fi
done

if [ "${#package_file_args[@]}" -gt 0 ] &&
  grep -E -n "$browser_package_denylist" "${package_file_args[@]}" >/dev/null; then
  echo "browser dependency guardrails failed: denylisted browser package detected" >&2
  grep -E -n "$browser_package_denylist" "${package_file_args[@]}" >&2
  exit 1
fi

if [ ! -f "$artifact" ]; then
  echo "browser bundle guardrails failed: missing artifact $artifact" >&2
  exit 1
fi

secret_markers='GOOGLE_APPLICATION_CREDENTIALS|AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AZURE_STORAGE_KEY|AZURE_CLIENT_SECRET|client_secret|refresh_token|service_account|private_key|client_email'

if grep -aE -n "$secret_markers" "$artifact" >/dev/null; then
  echo "browser bundle guardrails failed: secret-like marker detected in artifact" >&2
  grep -aE -n "$secret_markers" "$artifact" >&2
  exit 1
fi

echo "browser dependency and bundle guardrails passed"
