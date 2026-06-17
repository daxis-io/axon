# Daxis Release Notes Template

Use this template when assembling Daxis-facing release notes for an Axon release. Attach a completed copy to the release packet for every candidate and stable Daxis rollout review. Do not attach browser-visible secrets, signed URLs, raw credentials, service-account material, private customer identifiers, or unredacted tenant data.

## Release Identity

- Axon commit SHA:
- Branch or tag:
- Release channel: `experimental` | `integration` | `candidate` | `stable`
- Rollout segment:
- Release date:
- Release owner:
- Daxis rollout decision link:
- Migration notes link:

## Compatibility Summary

State whether this release changes any Daxis-facing runtime behavior. If there are no Daxis-facing changes, include this statement:

```text
This Axon release does not change Daxis-facing query results, fallback behavior, supported SQL claims, Delta feature handling, descriptor validation, or public error taxonomy.
```

## Required Release Note Sections

Complete every section. Use `unchanged` for areas that did not change.

### Query Result Semantics

- Summary:
- Evidence:
- Daxis action required:
- Rollback impact:

### Query Metrics And Observability

- Summary:
- Daxis result metrics changed:
- Fields affected: `rows_returned`, `arrow_ipc_bytes`, `scan_bytes`, `duration_ms`, `files_touched`, `files_skipped`, `row_groups_touched`, `row_groups_skipped`, `footer_reads`, `snapshot_bootstrap_duration_ms`, and `access_mode`
- Daxis dashboard or telemetry action required:
- Evidence:
- Rollback impact:

### Fallback Behavior

- Summary:
- Evidence:
- Daxis action required:
- Rollback impact:

### Supported SQL And Delta Features

- Summary:
- Evidence:
- Daxis action required:
- Rollback impact:

### Descriptor Validation

- Summary:
- Evidence:
- Daxis action required:
- Rollback impact:

### Error And Fallback Taxonomy

- Summary:
- Evidence:
- Daxis action required:
- Rollback impact:

### Runtime Budgets And Worker Artifact

- Summary:
- Evidence:
- Daxis action required:
- Rollback impact:

### Security And Browser Trust Boundary

- Summary:
- Evidence:
- Daxis action required:
- Rollback impact:

## Required Evidence

- Daxis release evidence runner:
- Daxis release bundle manifest verifier:
- Daxis compatibility notes:
- External proof packet status:
- Stable default promotion state:
- Migration notes or no-breaking-change statement:
- Browser matrix result when worker behavior changed:
- Contract artifact verifier when public contracts changed:

## Owner Signoff

- Release owner:
- Runtime / engine owner:
- Daxis product owner:
- Daxis query platform owner:
- Daxis catalog/storage owner:
- Daxis security owner:
- Daxis SRE owner:
