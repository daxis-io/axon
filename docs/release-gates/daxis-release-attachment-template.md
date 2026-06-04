# Daxis Release Attachment Template

Use this template when attaching release-process-required evidence to a Daxis Axon release packet. Complete one copy for each release-process item in `docs/release-gates/daxis-release-bundle-manifest.json`.

Do not attach browser-visible secrets, signed URLs, raw credentials, service-account material, private customer identifiers, or unredacted tenant data.

## Attachment Metadata

| Field                               | Value |
| ----------------------------------- | ----- |
| `item_id`                           |       |
| `release_commit_sha`                |       |
| `release_ref`                       |       |
| `owner`                             |       |
| `captured_at`                       |       |
| `artifact_uri`                      |       |
| `verification_command_or_statement` |       |
| `exit_status_or_review_status`      |       |
| `rollback_or_migration_note_uri`    |       |

`exit_status_or_review_status` must be one of:

- attached
- reviewed
- accepted
- rejected

## Release-Process Items

Use the matching guidance for `item_id`.

### `git_sha`

- Attach the exact Axon release commit SHA.
- Attach the branch or tag name in `release_ref`.
- Use the release packet URI, commit permalink, or tag permalink as `artifact_uri`.
- Use the rollback or prior-release reference as `rollback_or_migration_note_uri`.

### `worker_artifact_size`

- Attach output from `AXON_DF_SIZE_PACKAGE=axon-web-wasm AXON_DF_SIZE_WASM_STEM=axon_web_wasm AXON_DF_BROTLI_BUDGET_BYTES=6291456 bash tests/perf/report_datafusion_wasm_size.sh` when the optional size toolchain is available.
- If the optional size toolchain is unavailable, attach the release-owner statement that records why size evidence is deferred and where the blocker is tracked.
- Link the generated size output, release log, or blocker record in `artifact_uri`.

### `public_gcs_live_smoke`

- Attach output from `AXON_LIVE_PUBLIC_GCS_TABLE_URI=gs://... npm run test:browser:public-gcs-live -- --reporter=line`.
- If `AXON_LIVE_PUBLIC_GCS_TABLE_URI` is not configured, attach the skip-safe output and the external blocker record for fixture ownership, runner identity, and CI variable provisioning.
- Link the live-smoke output, CI run, or blocker record in `artifact_uri`.

### `release_notes`

- Attach a completed copy of `docs/release-gates/daxis-release-notes-template.md`.
- Cover query-result semantics, Daxis result metrics and observability fields, fallback behavior, supported SQL and Delta feature claims, descriptor validation, public error taxonomy, runtime budgets, worker artifact changes, security-boundary impact, external proof packet status, and `currentPromotionState`.
- For unchanged releases, include the unchanged statement from that template.
- Link the completed release notes in `artifact_uri`.

### `migration_notes`

- Attach a completed copy of `docs/release-gates/daxis-release-migration-notes-template.md`.
- Include external proof packet status and stable default promotion state.
- For no-breaking releases, include the no-breaking-change statement from that template.
- Link the completed migration notes in `rollback_or_migration_note_uri`.

## Owner Review

| Role                        | Name | Review state | Notes |
| --------------------------- | ---- | ------------ | ----- |
| Release owner               |      |              |       |
| Runtime / engine owner      |      |              |       |
| Daxis product owner         |      |              |       |
| Daxis query platform owner  |      |              |       |
| Daxis catalog/storage owner |      |              |       |
| Daxis security owner        |      |              |       |
| Daxis SRE owner             |      |              |       |
