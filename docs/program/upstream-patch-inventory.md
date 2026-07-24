# Upstream Patch Inventory

Current state: no private downstream patches are checked into this repository.

The isolated Daxis upstream-WASM POC tracks candidate patches in public forks and keeps Daxis-only
dependency wiring on separate stack branches.

## Verification

- No `vendor/` directory exists in this repository.
- No workspace manifest contains a `[patch]` section.
- The browser lakehouse release baseline at git commit `0e19f1d` is built from workspace crates plus upstream registry dependencies pinned in `Cargo.lock`.

If a downstream patch becomes necessary, add it here before merge and keep the row current until
removal.

| Patch | Fork | Compatible Base | Candidate Branch | Stack Branch | Owner | Upstream Disposition | Removal Condition | Tracking Issue |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Arrow and Parquet target-safe codec backends | [`daxis-io/arrow-rs`](https://github.com/daxis-io/arrow-rs) | `poc/base/arrow-58.3.0` | `poc/wasm32-browser-candidate` | `poc/wasm32-browser-stack` | Runtime / engine team | `opened` | Adopt an upstream Arrow release containing the accepted codec fixes, pass Axon's locked browser graph rehearsal, and remove the fork revision. | [#2](https://github.com/daxis-io/axon/issues/2) |
| `object_store` target-safe HTTP, retry, and range protocol | [`daxis-io/arrow-rs-object-store`](https://github.com/daxis-io/arrow-rs-object-store) | `poc/base/object-store-0.13.2` | `poc/wasm32-browser-candidate` | `poc/wasm32-browser-stack` | Runtime / engine team | `proposed` | Adopt an upstream `object_store` release containing the accepted browser HTTP slices, pass Axon's locked browser graph rehearsal, and remove the fork revision. | [#2](https://github.com/daxis-io/axon/issues/2) |
| DataFusion browser feature ownership and runtime profile | [`daxis-io/datafusion`](https://github.com/daxis-io/datafusion) | `poc/base/datafusion-53.1.0` | `poc/wasm32-browser-candidate` | `poc/wasm32-browser-stack` | Runtime / engine team | `proposed` | Adopt an upstream DataFusion release containing the accepted feature and runtime slices, pass Axon's locked browser graph rehearsal, and remove the fork revision. | [#2](https://github.com/daxis-io/axon/issues/2) |
| Delta Kernel read-only core target safety | [`daxis-io/delta-kernel-rs`](https://github.com/daxis-io/delta-kernel-rs) | `poc/base/buoyant-kernel-0.22.2` | `poc/wasm32-browser-candidate` | `poc/wasm32-browser-stack` | Runtime / engine team | `proposed` | Adopt a compatible upstream Delta Kernel release containing the accepted target-safety fixes, pass the downstream browser-engine rehearsal, and remove the fork revision. | [#2](https://github.com/daxis-io/axon/issues/2) |
| delta-rs browser-engine incubation | [`daxis-io/delta-rs`](https://github.com/daxis-io/delta-rs) | `poc/base/delta-rs-0.32.4` | `poc/wasm32-browser-candidate` | `poc/wasm32-browser-stack` | Runtime / engine team | `proposed` | Land the reviewed browser-engine boundary in its canonical home, adopt a compatible release, pass Axon's locked browser proof, and remove the fork revision. | [#2](https://github.com/daxis-io/axon/issues/2) |

Allowed values for `Upstream Disposition`:

- `proposed`
- `opened`
- `merged`
- `wontfix`
- `temporary`
