# Upstream WASM Support Design Pack

- Status: Draft for upstream architecture review
- Date: 2026-07-23
- Scope: long-term `wasm32-unknown-unknown` support across Arrow Rust, Parquet, `object_store`,
  DataFusion, delta-kernel, and delta-rs
- Target policy: exactly `wasm32-unknown-unknown`
- Source policy: primary project source, project documentation, Cargo and Rust documentation, Fetch,
  and HTTP specifications

## Purpose

This pack defines a cross-project design that keeps native defaults intact while making a
feature-unified `wasm32-unknown-unknown` graph compile and behave predictably.

The target does not provide native threads, a filesystem, blocking I/O, or a browser API. A browser
host can supply Fetch, timers, entropy, and local task scheduling through JavaScript. The design
keeps those host services separate from target detection.

## Document Map

| File                                                                                       | Purpose                                                                                         |
| ------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------- |
| [00-executive-brief.md](./docs/00-executive-brief.md)                                      | Recommendation, support levels, and work to defer.                                              |
| [01-architecture-and-ownership.md](./docs/01-architecture-and-ownership.md)                | End-to-end component model, target policy, Cargo model, and ownership.                          |
| [02-dependency-graph-and-failure-map.md](./docs/02-dependency-graph-and-failure-map.md)    | Blocker graph, enablers, failure phases, and evidence.                                          |
| [03-arrow-parquet-codec-architecture.md](./docs/03-arrow-parquet-codec-architecture.md)    | Target-selected codec backends, errors, tests, and codec policy matrix.                         |
| [04-object-store-browser-architecture.md](./docs/04-object-store-browser-architecture.md)  | Host-neutral HTTP core, existing Fetch adapter, runtime services, and browser protocol rules.   |
| [05-datafusion-delta-integration.md](./docs/05-datafusion-delta-integration.md)            | DataFusion feature ownership, browser tier, delta-kernel engine split, and delta-rs provenance. |
| [06-verification-and-upstream-rollout.md](./docs/06-verification-and-upstream-rollout.md)  | CI matrix, graph policy, golden fixtures, and PR stack.                                         |
| [07-risk-register.md](./docs/07-risk-register.md)                                          | Compatibility, performance, support, and delivery risks.                                        |
| [08-research-snapshot.md](./docs/08-research-snapshot.md)                                  | Commits, lockfiles, tools, probes, resolved versions, and source index.                         |
| [ADR-0001](./adrs/ADR-0001-logical-features-use-target-selected-backends.md)               | Logical features use target-selected implementations.                                           |
| [ADR-0002](./adrs/ADR-0002-unavailable-codecs-fail-at-operation-time.md)                   | Unavailable codecs compile and fail at operation time.                                          |
| [ADR-0003](./adrs/ADR-0003-browser-services-are-explicit-host-capabilities.md)             | Browser services are explicit host capabilities.                                                |
| [ADR-0004](./adrs/ADR-0004-native-and-browser-delta-engines-remain-separate.md)            | Native and browser Delta engines remain separate.                                               |
| [Implementation plan](../../plans/2026-07-23-upstream-wasm-support-implementation-plan.md) | Test-first, reviewable PR sequence across the owning repositories.                              |
| [Daxis fork POC plan](../../plans/2026-07-23-daxis-upstream-wasm-fork-poc.md)              | Revision-pinned fork topology and end-to-end browser proof.                                     |

## Recommendation

The durable design separates a logical Cargo feature from the implementation available on a target:

1. Preserve Arrow and Parquet's public codec features and native defaults.
2. Remove C-backed codec dependencies from the active `wasm32-unknown-unknown` graph with
   target-specific dependency tables and matching source `cfg` expressions.
3. Compile a target-unavailable stub when Cargo enables a logical codec feature but the target has no
   backend.
4. Distinguish feature-disabled errors from target-unavailable errors.
5. Keep `object_store/http-base` host-neutral and make `web` the browser-host capability.
6. Formalize the existing reqwest-on-WASM Fetch adapter instead of creating a parallel transport.
7. Require DataFusion and Delta crates to declare the exact dependency features they own.
8. Test graph compilation, browser execution, protocol interoperability, and downstream integration
   as separate guarantees.

Pure-Rust zstd decode, a public codec-provider API, cloud credential discovery in browsers,
general DataFusion browser execution, browser spill storage, and zstd encoding remain follow-up work.

## Non-Negotiable Invariants

- Native default behavior and codec performance remain unchanged.
- A downstream crate does not need `default-features = false` to keep the graph compilable.
- `parquet/zstd` and `arrow-ipc/zstd` may be unified into a WASM build.
- No C-backed codec library enters the supported browser graph.
- `wasm32-unknown-unknown` does not imply JavaScript or a browser.
- Browser range reads validate status, range metadata, object identity, and content representation.
- Unsupported execution paths return errors instead of panicking or reaching filesystem and runtime
  assumptions.
- Upstream crates contain no Axon or delta-rs product policy.

## Source Snapshot

| Project                         | Pinned revision                                                                                                                             |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| Apache Arrow Rust and Parquet   | [`b46e7334b3d4233b35868d7d22b0807d3c38e147`](https://github.com/apache/arrow-rs/tree/b46e7334b3d4233b35868d7d22b0807d3c38e147)              |
| Apache `object_store`           | [`84d24eb8efcec9448566de09e94d2d4b74b21ebe`](https://github.com/apache/arrow-rs-object-store/tree/84d24eb8efcec9448566de09e94d2d4b74b21ebe) |
| Apache DataFusion               | [`17634176a4f406a50b31a3711514831c9d3ecfa0`](https://github.com/apache/datafusion/tree/17634176a4f406a50b31a3711514831c9d3ecfa0)            |
| Upstream delta-kernel-rs        | [`03e2537d5a6203872d9785e31cc00f503ddeb8ec`](https://github.com/delta-io/delta-kernel-rs/tree/03e2537d5a6203872d9785e31cc00f503ddeb8ec)     |
| delta-rs                        | [`3f562682c5a9dd55693b7f7bbd2a2f749fdf38e5`](https://github.com/delta-io/delta-rs/tree/3f562682c5a9dd55693b7f7bbd2a2f749fdf38e5)            |
| delta-rs kernel fork resolution | [`8ba063f8f84fec222000f66d40d70911d7c79675`](https://github.com/buoyant-data/delta-kernel-rs/tree/8ba063f8f84fec222000f66d40d70911d7c79675) |

The [research snapshot](./docs/08-research-snapshot.md) records the toolchain, lockfile hashes, probe
commands, and evidence limits.
