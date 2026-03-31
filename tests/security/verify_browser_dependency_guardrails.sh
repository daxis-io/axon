#!/usr/bin/env bash

set -euo pipefail

artifact="${1:-target/wasm32-unknown-unknown/release/browser_engine_worker.wasm}"
tree_file="$(mktemp)"
trap 'rm -f "$tree_file"' EXIT

if [ -n "${AXON_BROWSER_WORKER_TREE_FILE:-}" ]; then
  cp "$AXON_BROWSER_WORKER_TREE_FILE" "$tree_file"
else
  cargo tree -p browser-engine-worker --target wasm32-unknown-unknown --locked --prefix none > "$tree_file"
fi

denylist='^(aws-config|aws-credential-types|aws-sdk-[^[:space:]]*|aws-smithy-[^[:space:]]*|azure_core|azure_identity|azure_storage|gcp_auth|google-cloud-[^[:space:]]*|jsonwebtoken|opendal|yup-oauth2)([[:space:]]|$)'

if grep -E -n "$denylist" "$tree_file" >/dev/null; then
  echo "browser dependency guardrails failed: denylisted dependency detected" >&2
  grep -E -n "$denylist" "$tree_file" >&2
  exit 1
fi

if [ ! -f "$artifact" ]; then
  echo "browser bundle guardrails failed: missing artifact $artifact" >&2
  exit 1
fi

secret_markers='GOOGLE_APPLICATION_CREDENTIALS|AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AZURE_STORAGE_KEY|service_account|private_key|client_email'

if grep -aE -n "$secret_markers" "$artifact" >/dev/null; then
  echo "browser bundle guardrails failed: secret-like marker detected in artifact" >&2
  grep -aE -n "$secret_markers" "$artifact" >&2
  exit 1
fi

echo "browser dependency and bundle guardrails passed"
