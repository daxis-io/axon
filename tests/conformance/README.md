# Conformance Tests

This directory holds checks that keep browser and native behavior aligned, plus native-only corpora for execution metrics and pruning coverage.

Current contents:

- `verify_workspace_layout.sh`: verifies the EPIC-01 scaffold exists before feature work begins.
- `native-runtime-sql-corpus.json`: unpartitioned native reference-runtime SQL corpus with golden result tables for projection/filter, aggregate, group-by, and order/limit coverage.
- `native-runtime-partitioned-sql-corpus.json`: partitioned latest-snapshot SQL corpus with golden results plus expected scanned/skipped file counts for pruning-visible cases.
