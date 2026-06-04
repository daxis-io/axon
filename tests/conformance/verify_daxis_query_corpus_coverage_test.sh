#!/usr/bin/env bash

set -euo pipefail

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

repo_root="$tmpdir/repo"
mkdir -p "$repo_root/docs/program" "$repo_root/tests/conformance"

write_valid_fixture() {
  cat >"$repo_root/tests/conformance/daxis-browser-datafusion-query-corpus.json" <<'JSON'
[
  {
    "name": "recent_open_orders",
    "sql": "SELECT order_id, customer_tier, amount_cents FROM orders WHERE order_date = '2026-05-30' AND status <> 'cancelled' ORDER BY amount_cents DESC LIMIT 2",
    "covered_sql_classes": ["projection", "filtering", "ordering", "limit", "descriptor_backed_scan"],
    "expected_columns": ["order_id", "customer_tier", "amount_cents"],
    "expected_rows": [[1001, "gold", 12500], [1002, "silver", 8750]]
  },
  {
    "name": "tier_revenue_summary",
    "sql": "SELECT customer_tier, COUNT(*) AS order_count, SUM(amount_cents) AS gross_cents FROM orders WHERE status <> 'cancelled' GROUP BY customer_tier ORDER BY customer_tier",
    "covered_sql_classes": ["projection", "filtering", "grouped_aggregate", "ordering", "descriptor_backed_scan"],
    "expected_columns": ["customer_tier", "order_count", "gross_cents"],
    "expected_rows": [["gold", 3, 49500], ["silver", 1, 8750]]
  },
  {
    "name": "priority_net_amounts",
    "sql": "SELECT order_id, CAST(amount_cents - CASE WHEN discount_cents IS NULL THEN 0 ELSE discount_cents END AS BIGINT) AS net_cents, CASE WHEN is_priority THEN 'priority' ELSE 'standard' END AS priority_bucket FROM orders ORDER BY order_id",
    "covered_sql_classes": ["projection", "ordering", "arithmetic", "cast", "case_expression", "descriptor_backed_scan"],
    "expected_columns": ["order_id", "net_cents", "priority_bucket"],
    "expected_rows": [[1001, 12000, "priority"], [1002, 8750, "standard"]]
  }
]
JSON

  cat >"$repo_root/docs/program/browser-datafusion-runtime-parity.md" <<'EOF'
# Browser DataFusion Runtime Parity

## Supported SQL Classes

- projection
- filtering
- ordering
- limit
- grouped aggregates
- arithmetic
- `CAST`
- `CASE` expressions
- descriptor-backed `AxonParquetScanExec`

## Daxis Query Corpus Coverage

| Case | Covered SQL classes |
| ---- | ------------------- |
| `recent_open_orders` | projection, filtering, ordering, limit, descriptor-backed `AxonParquetScanExec` |
| `tier_revenue_summary` | projection, filtering, grouped aggregates, ordering, descriptor-backed `AxonParquetScanExec` |
| `priority_net_amounts` | projection, ordering, arithmetic, `CAST`, `CASE` expressions, descriptor-backed `AxonParquetScanExec` |
EOF
}

verify_fixture() {
  AXON_DAXIS_QUERY_CORPUS_REPO_ROOT="$repo_root" \
    bash tests/conformance/verify_daxis_query_corpus_coverage.sh >/dev/null 2>&1
}

write_valid_fixture
verify_fixture

python3 - "$repo_root/tests/conformance/daxis-browser-datafusion-query-corpus.json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    corpus = json.load(handle)
corpus[0].pop("covered_sql_classes")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(corpus, handle)
PY

if verify_fixture; then
  echo "expected corpus case missing covered_sql_classes to be rejected" >&2
  exit 1
fi

write_valid_fixture
python3 - "$repo_root/tests/conformance/daxis-browser-datafusion-query-corpus.json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    corpus = json.load(handle)
corpus[0]["covered_sql_classes"].append("window_functions")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(corpus, handle)
PY

if verify_fixture; then
  echo "expected unknown corpus SQL class to be rejected" >&2
  exit 1
fi

write_valid_fixture
python3 - "$repo_root/tests/conformance/daxis-browser-datafusion-query-corpus.json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    corpus = json.load(handle)
corpus[1]["covered_sql_classes"].remove("grouped_aggregate")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(corpus, handle)
PY

if verify_fixture; then
  echo "expected missing required grouped aggregate corpus coverage to be rejected" >&2
  exit 1
fi

write_valid_fixture
python3 - "$repo_root/docs/program/browser-datafusion-runtime-parity.md" <<'PY'
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    text = handle.read()
text = text.replace("| `priority_net_amounts` | projection, ordering, arithmetic, `CAST`, `CASE` expressions, descriptor-backed `AxonParquetScanExec` |\n", "")
with open(path, "w", encoding="utf-8") as handle:
    handle.write(text)
PY

if verify_fixture; then
  echo "expected parity doc missing corpus case statement to be rejected" >&2
  exit 1
fi

write_valid_fixture
python3 - "$repo_root/docs/program/browser-datafusion-runtime-parity.md" <<'PY'
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    text = handle.read()
text = text.replace("descriptor-backed `AxonParquetScanExec`", "descriptor-backed scan")
with open(path, "w", encoding="utf-8") as handle:
    handle.write(text)
PY

if verify_fixture; then
  echo "expected parity doc missing descriptor-backed compatibility statement to be rejected" >&2
  exit 1
fi

echo "Daxis query corpus compatibility coverage verifier regression coverage passed"
