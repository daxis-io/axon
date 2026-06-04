#!/usr/bin/env bash

set -euo pipefail

repo_root="${AXON_DAXIS_QUERY_CORPUS_REPO_ROOT:-$(pwd)}"
corpus_file="${AXON_DAXIS_QUERY_CORPUS_FILE:-tests/conformance/daxis-browser-datafusion-query-corpus.json}"
parity_doc="${AXON_DAXIS_QUERY_CORPUS_PARITY_DOC:-docs/program/browser-datafusion-runtime-parity.md}"

resolve_path() {
  local path="$1"
  if [[ "$path" = /* ]]; then
    printf "%s\n" "$path"
    return
  fi

  printf "%s/%s\n" "$repo_root" "$path"
}

corpus_path="$(resolve_path "$corpus_file")"
parity_doc_path="$(resolve_path "$parity_doc")"

if [[ ! -f "$corpus_path" ]]; then
  echo "missing Daxis query corpus: $corpus_file" >&2
  exit 1
fi

if [[ ! -f "$parity_doc_path" ]]; then
  echo "missing browser DataFusion parity document: $parity_doc" >&2
  exit 1
fi

python3 - "$corpus_path" "$parity_doc_path" "$corpus_file" "$parity_doc" <<'PY'
import json
import sys
from pathlib import Path

corpus_path = Path(sys.argv[1])
parity_doc_path = Path(sys.argv[2])
corpus_file = sys.argv[3]
parity_doc = sys.argv[4]

with open(corpus_path, encoding="utf-8") as handle:
    corpus = json.load(handle)
parity_text = parity_doc_path.read_text(encoding="utf-8")


def fail(message: str) -> None:
    print(message, file=sys.stderr)
    sys.exit(1)


def expect(condition: bool, message: str) -> None:
    if not condition:
        fail(message)


allowed_classes = {
    "projection": "projection",
    "filtering": "filtering",
    "ordering": "ordering",
    "limit": "limit",
    "grouped_aggregate": "grouped aggregates",
    "arithmetic": "arithmetic",
    "cast": "`CAST`",
    "case_expression": "`CASE` expressions",
    "descriptor_backed_scan": "descriptor-backed `AxonParquetScanExec`",
}
required_classes = set(allowed_classes)

expect(isinstance(corpus, list) and corpus, f"{corpus_file} must be a non-empty JSON array")
case_names: list[str] = []
covered_classes: set[str] = set()

for index, case in enumerate(corpus):
    expect(isinstance(case, dict), f"{corpus_file} case #{index + 1} must be an object")
    name = case.get("name")
    expect(isinstance(name, str) and name.strip(), f"{corpus_file} case #{index + 1} missing name")
    expect(name not in case_names, f"{corpus_file} has duplicate case name: {name}")
    case_names.append(name)

    sql = case.get("sql")
    expect(isinstance(sql, str) and sql.strip(), f"{corpus_file} case {name} missing SQL")
    expect(
        sql.strip().lower().startswith("select "),
        f"{corpus_file} case {name} must stay in the read-only SELECT envelope",
    )

    classes = case.get("covered_sql_classes")
    expect(
        isinstance(classes, list) and classes,
        f"{corpus_file} case {name} missing covered_sql_classes",
    )
    normalized_classes: list[str] = []
    for class_name in classes:
        expect(
            isinstance(class_name, str) and class_name.strip(),
            f"{corpus_file} case {name} has invalid covered SQL class",
        )
        expect(
            class_name in allowed_classes,
            f"{corpus_file} case {name} declares unsupported SQL class: {class_name}",
        )
        expect(
            class_name not in normalized_classes,
            f"{corpus_file} case {name} repeats covered SQL class: {class_name}",
        )
        normalized_classes.append(class_name)
    covered_classes.update(normalized_classes)

    for field in ["expected_columns", "expected_rows"]:
        expect(field in case, f"{corpus_file} case {name} missing {field}")

missing_required_classes = sorted(required_classes - covered_classes)
expect(
    not missing_required_classes,
    f"{corpus_file} does not cover required Daxis SQL classes: {', '.join(missing_required_classes)}",
)

for class_name, doc_phrase in allowed_classes.items():
    expect(
        doc_phrase in parity_text,
        f"{parity_doc} missing compatibility statement for {class_name}: {doc_phrase}",
    )

expect(
    "## Daxis Query Corpus Coverage" in parity_text,
    f"{parity_doc} missing Daxis query corpus coverage section",
)
for name in case_names:
    expect(
        f"`{name}`" in parity_text,
        f"{parity_doc} missing Daxis query corpus case statement: {name}",
    )

print("Daxis query corpus compatibility coverage verified")
PY
