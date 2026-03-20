# Upstream Patch Inventory

Track every downstream patch that diverges from upstream dependencies.

| Patch | Local Path | Owner | Upstream Disposition | Removal Condition | Tracking Issue |
| --- | --- | --- | --- | --- | --- |
| Example: browser range-read fix | `vendor/datafusion-wasm-bindings` | Runtime / engine team | `proposed` | Remove after upstream release with fix | `TBD` |

Allowed values for `Upstream Disposition`:

- `proposed`
- `opened`
- `merged`
- `wontfix`
- `temporary`
