# Upstream Patch Inventory

Current state: no private downstream patches are checked into this repository.

## Verification

- No `vendor/` directory exists in this repository.
- No workspace manifest contains a `[patch]` section.
- The browser lakehouse release baseline at git commit `0e19f1d` is built from workspace crates plus upstream registry dependencies pinned in `Cargo.lock`.

If a downstream patch becomes necessary, add it here before merge and keep the row current until removal.

| Patch | Local Path | Owner | Upstream Disposition | Removal Condition | Tracking Issue |
| --- | --- | --- | --- | --- | --- |

Allowed values for `Upstream Disposition`:

- `proposed`
- `opened`
- `merged`
- `wontfix`
- `temporary`
