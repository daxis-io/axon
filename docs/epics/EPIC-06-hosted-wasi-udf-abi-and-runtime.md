# EPIC-06: Hosted WASI UDF ABI And Runtime

- Type: Epic
- Accountable: Query platform lead
- Delivery DRI: Runtime / engine team
- Depends on: EPIC-01, optionally EPIC-02
- Milestone: `M5`

## Goal

Turn UDF/WASM into a versioned hosted extension path that stays separate from browser execution.

## Packages In Scope

- `crates/udf-abi`
- `crates/udf-host-wasi`
- host and guest compatibility tests
- optional starter SDKs

## Deliverables

- WIT-defined ABI
- ABI versioning policy
- hosted runtime using WASIp2 / wasmtime
- example scalar UDF
- example aggregate UDF or an explicit roadmap for why not yet
- compatibility tests across ABI versions

## Child Issues

1. Define the UDF WIT package and version rules.
2. Implement the host runtime with capability-limited IO.
3. Implement the Arrow or columnar boundary strategy.
4. Build a sample Rust guest.
5. Build a second-language starter SDK or template.
6. Add compatibility tests for version skew.
7. Write the hosted-UDF security model.
8. Document why the hosted ABI is separate from the browser runtime.

## Acceptance Criteria

- the ABI is versioned and documented
- an example guest executes in the hosted runtime
- compatibility tests pass
- browser packages do not depend on `udf-host-wasi`
- security review approves the capability limits

## Definition Of Done

Teams can safely extend the platform with hosted UDFs without coupling that design to browser execution.
