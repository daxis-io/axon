# Browser Lakehouse Engine Strategy

- Status: **Canonical design — implementation in progress**
- Revision date: 2026-08-02
- Decision owner: Runtime / engine team
- Authored against Axon `origin/main`: [`9ad43ce72fc8235128c5fa604eecd95aabf1bc29`](https://github.com/daxis-io/axon/commit/9ad43ce72fc8235128c5fa604eecd95aabf1bc29)
- Scope: normative browser-engine architecture, compatibility policy, migration, and promotion gates
- Related:
  - [Axon workbench and query-engine architecture](./axon-workbench-architecture.md)
  - [Provider and host integration model](./provider-model.md)
  - [Browser-owned descriptor materialization](./browser-owned-descriptor-materialization.md)
  - [Browser Unity Catalog brokered runtime contract](./browser-uc-brokered-runtime-contract.md)
  - [Browser DataFusion runtime parity](./browser-datafusion-runtime-parity.md)
  - [Upstream WebAssembly support strategy](./upstream-wasm-support-strategy.md)
  - [Upstream WASM fork POC evidence](../release-gates/upstream-wasm-fork-poc-evidence.md)
  - [Browser WASM Delta/GCS release evidence](../release-gates/browser-wasm-delta-gcs-release-evidence.md)

## Authority, Language, And Precedence

This document is Axon's single normative browser-engine design. It preserves the program's
read-only product outcomes, authority boundaries, native correctness oracle, and release-proof
standards while replacing the superseded mechanics listed below.

The terms **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** are normative. Lowercase
terms and descriptions of current code or evidence are informative unless a sentence explicitly
says otherwise. A target interface in this document is a design contract, not proof that the
interface has landed.

When documents disagree, apply this precedence:

1. Existing security, authority, and native-oracle ADRs remain binding, especially
   [ADR-0002](../adr/ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md),
   [ADR-0004](../adr/ADR-0004-native-runtime-is-correctness-oracle-and-mandatory-fallback.md),
   [ADR-0005](../adr/ADR-0005-read-only-mvp-and-delta-compatibility-policy.md), and
   [ADR-0008](../adr/ADR-0008-daxis-browser-read-compute-contract.md).
2. This strategy governs browser-engine architecture and migration.
3. Upstream plans govern dependency-publication mechanics and ordering, not Axon product
   architecture.
4. Historical implementation plans remain evidence of intent and prior work, but lose authority
   wherever the [supersession ledger](#compatibility-and-supersession-ledger) says so.
5. Release-evidence documents determine what has been proven. Authors MUST NOT use this design to
   promote local, POC, or design-review evidence into a remotely reproducible release, shipping
   adoption, or production-default claim.

## Maturity Ledger: 2026-08-02

Maturity is recorded by evidence class rather than by a single "done" label.

| Maturity                             | Exact state                                                                                                                                                                                                                                                                                                                                                                                                                                                              | What it permits                                                                                                                             |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------- |
| Landed on current Axon main          | `9ad43ce72fc8235128c5fa604eecd95aabf1bc29` contains the compatibility DataFusion provider and custom scan, persistent worker/session path, pull-driven private Arrow IPC cursor with atomic public `sql()`, typed budgets and cancellation, identity-aware HTTP range handling, and bounded path-free OPFS aggregate spill with structured `resource_exhausted` and cleanup metrics. Delta snapshot reconstruction is already repo-owned through the compatibility path. | Maintain and use the qualified compatibility provider. It does not prove the target Kernel-native provider or progressive API.              |
| Locally verified but unpublished     | The exact Axon and upstream heads in the immutable-head ledger below passed their stated local verifier scope.                                                                                                                                                                                                                                                                                                                                                           | Review and publication preparation only. They are not remotely reproducible releases or valid shipping dependency pins.                     |
| Accepted only for Axon design review | `BrowserDeltaAccessDescriptor`, `AxonTableAccess`, `KernelTaskDriver`, `AxonKernelTableProvider`, `BrowserScanPredicates`, `BrowserDataFusionProfile`, the prefix router, and `sqlProgressive()` are the selected target seams.                                                                                                                                                                                                                                          | Axon preparation may implement the independent pieces called out in the roadmap. Kernel-dependent work still waits for upstream acceptance. |
| Experimental / non-authoritative     | `poc/upstream-wasm-fork-stack`, the historical prefetch/cached-callback integration, the permanent-custom-scan POC shape, and the [Mangrove comparison source at `601be3c`](https://github.com/open-lakehouse/mangrove/commit/601be3cddbe68a676c7740d75cbce26190ad4279).                                                                                                                                                                                                 | Comparison, fixture, and risk evidence only. None defines Axon's production architecture.                                                   |
| Planned                              | Publication, isolated shipping workspace, standard ObjectStore adapter, Kernel K1-K5, parallel provider, access convergence, progressive delivery, and promotion.                                                                                                                                                                                                                                                                                                        | Execution only in the dependency order and behind the gates below.                                                                          |

### Immutable local-head ledger

These are supplied local evidence heads. Record them exactly; do not cite them as remotely
reproducible releases:

| Evidence                       | Exact local head                           | Local GO                                                   | Remotely reproducible release | Maintainer acceptance                                                                  | Shipping adoption | Production default |
| ------------------------------ | ------------------------------------------ | ---------------------------------------------------------- | ----------------------------- | -------------------------------------------------------------------------------------- | ----------------- | ------------------ |
| Axon verifier                  | `7df911beb0e4f77a8280be212ee4d6d50400fcf5` | Yes, for its recorded verifier scope                       | No; local/unpublished head    | Not applicable                                                                         | No                | No                 |
| `object_store`                 | `bd5c4ed1789602ce90f2e2dd718e545be8ab5197` | Yes, for the local browser contract                        | No                            | No upstream acceptance recorded here                                                   | No                | No                 |
| Arrow / Parquet                | `ee2cfeb8ef353683e8c49bcd48b2ce13afe1de60` | Yes, for the local browser contract                        | No                            | No upstream acceptance recorded here                                                   | No                | No                 |
| DataFusion                     | `eb00a115c9caf4abc66c9ca9209ad83b3b1fcc83` | Yes, for the local browser contract                        | No                            | No upstream acceptance recorded here                                                   | No                | No                 |
| Kernel operation-task contract | `4223fa43039d418238f6c4a1304d23e9f3764aa6` | Design packet reviewed locally; implementation GO is gated | No                            | Pending on [Kernel issue #252](https://github.com/delta-io/delta-kernel-rs/issues/252) | No                | No                 |

The current compatibility provider is shipping code and is the Daxis-facing browser DataFusion
default runtime SKU where its release gates are satisfied. The Kernel-native provider described
here has neither shipping adoption nor production-default status. A local GO on an upstream head
does not change either fact.

## Canonical Decision

Axon will converge on Delta Kernel for Delta protocol and scan semantics, DataFusion for standard
SQL and Parquet execution, and Axon-owned browser access and runtime policy. The engine MUST use
one access-resolution seam, one persistent worker/session, bounded asynchronous Kernel tasks, a
standard DataFusion Parquet source over an Axon `ObjectStore`, and the existing pull-driven Arrow
IPC cursor.

The end-to-end target is:

```text
CanonicalResourceRef
  → existing DataAccessResolver
  → one execution-local BrowserDeltaAccess
  → persistent Axon worker/session
  → capability and expiry validation
  → root-scoped or per-file object access
  → async bounded Delta Kernel SnapshotTask / ScanTask
  → Kernel DataFusionPlanCompiler
  → DataFusion logical and physical planning
  → standard DataFusion Parquet over Axon ObjectStore
  → existing pull-driven continuous Arrow IPC cursor
  → atomic sql() or consumer-credited sqlProgressive()
```

Product APIs MAY compose resolve, open, and query into one call. Internally, resolution remains a
separate pre-admission operation: the engine MUST NOT call a catalog, choose another source, or
mint credentials. When metadata is sufficient to reject a table capability before vending a
credential or object grant, the resolver MUST reject first.

## Ownership

### Delta Kernel owns Delta semantics

Delta Kernel owns protocol validation, snapshot discovery and replay, scan construction,
transforms, deletion-vector and file-constant semantics, partition interpretation, and Delta
file-skipping semantics. Axon MUST NOT create a second private Delta protocol or reinterpret a
Kernel scan plan.

### DataFusion owns query and Parquet semantics

DataFusion owns SQL parsing, logical and physical planning, optimization, Parquet decode,
row-group and page pruning, operators, expression evaluation, memory accounting, and execution.
The target provider MUST use standard DataFusion Parquet execution. `AxonParquetScanExec` and the
`wasm-parquet-engine` compatibility scanner are not the permanent physical scan engine.

### Axon owns browser policy and isolation

Axon owns access authority, capability and identity validation, browser object I/O, strong
ETag/range/`If-Range` policy, request coalescing and caches, metadata/data concurrency budgets,
deadlines, cancellation, output and memory budgets, path-free bounded OPFS spill, worker
isolation, Arrow IPC delivery, explicit fallback contracts, and observability.

`CatalogProvider` remains discovery-only. `DataAccessResolver` remains the only resolver seam.
There MUST NOT be a parallel `BrowserTableResolver`. Native execution remains the correctness
oracle and mandatory supported route, but an accepted browser failure never transparently becomes
native execution.

## Access And Deep Module Seams

The names below are conceptual target interfaces. Exact Rust, TypeScript, and protobuf spelling is
settled when each vertical slice lands, without changing the responsibilities.

`BrowserDeltaAccess` is the execution-local internal binding that joins one validated
`ResolvedBrowserRead` Delta arm to one `AxonTableAccess`, plus its deadline, cancellation, and
correlation state. The worker creates it after admission validation and disposes it after rejection
or terminal execution. Sessions may retain non-secret table identity and eligible cached bytes,
but never reuse this capability-bearing binding for another execution.

### `BrowserDeltaAccessDescriptor`

`ResolvedBrowserRead` already contains an openable descriptor union. The target extends that
existing union with a Delta-specific `BrowserDeltaAccessDescriptor`; it does not replace
`ResolvedBrowserRead` or add a provider seam.

```text
BrowserDeltaAccessDescriptor
  RootScopedDelta
    table root
    requested latest/exact or pinned snapshot selection
    access capabilities
    opaque grant or store-factory reference
    earliest expiry and non-secret provenance

  PerFileSnapshot
    existing BrowserHttpSnapshotDescriptor
```

`RootScopedDelta` supports browser-safe list/head/range access to a table root. Its grant or
factory reference carries an execution-local capability handle. It contains neither a cloud
credential nor an `ObjectStore` instance. `PerFileSnapshot` preserves the existing active-file
descriptor for signed-file, manifest, Delta Sharing URL-mode, and compatibility paths.

Plain Parquet remains its existing openable descriptor arm. The Delta union is an extension, not a
reason to make every source look like Delta or to encode internal objects on the wire.

### `AxonTableAccess`

`AxonTableAccess` is the internal deep module that consumes a validated Delta access descriptor
and produces exactly one of:

- a root-scoped `Arc<dyn ObjectStore>` plus normalized table prefix; or
- the per-file compatibility representation used by the current provider and, later, by a
  manifest-backed standard DataFusion Parquet source.

Object-store instances, callbacks, JavaScript objects, tokens, and secrets MUST NOT cross
protobuf or JSON boundaries. Only opaque, bounded, validated references cross the boundary; the
worker constructs the store inside its execution authority and disposes it no later than the
binding lifetime.

### Path-segment-aware prefix router

One worker/session MAY open multiple authorized tables. A router selects stores by normalized
authority and the longest matching path-segment prefix, not raw string prefix. It MUST reject
ambiguous duplicate roots, traversal, encoded-separator confusion, authority mismatch, and a path
that matches no registered root. A store registered for `bucket/a/table` MUST NOT authorize
`bucket/a/table-two` or another table in the same bucket. Credentials and cache namespaces remain
isolated per routed root.

### `KernelTaskDriver`

`KernelTaskDriver` asynchronously drives the proposed Kernel `OperationTask` protocol. It owns
browser awaits, request correlation, cancellation, deadlines, driver cursor progress, and the
per-page and cumulative budgets accepted by the task.

It MUST:

- accept at most one outstanding `OperationRequest` and return the matching owned result;
- validate monotonically issued request IDs and the exact result variant before resumption;
- bound pages, entries, input slices, output chunks, descriptor bytes, payload bytes, plan nodes,
  batches, rows, parsed footer structure, continuation length, retained log state, and total work;
- preserve the task's typed protocol, cancellation, malformed-response, and
  `ResourceExhausted` failures;
- abort or drop driver-owned I/O on cancellation and reject late results without reviving a task;
- await browser Fetch and storage promises without blocking the worker.

There is no `block_on`, `Atomics.wait`, thread parking, hidden Fetch inside synchronous Kernel
callbacks, unbounded iterator result, mandatory full-log prefetch, or browser-only replacement
protocol.

The upstream seam is still a proposal at local head
`4223fa43039d418238f6c4a1304d23e9f3764aa6`. K1-K5 MUST NOT start until maintainers explicitly
accept or revise it. If the seam is rejected or materially revised, stop K1-K5 and revise this
design; do not create an Axon-private substitute by default.

### `AxonKernelTableProvider`

`AxonKernelTableProvider` is the one-table root-scoped bridge into the caller's DataFusion
session. It:

1. validates the registered access and `BrowserDataFusionProfile`;
2. lowers supported DataFusion filters conservatively into Kernel expressions;
3. awaits `ScanTask` through the worker-safe async bridge;
4. compiles the accepted Kernel plan with `DataFusionPlanCompiler`; and
5. asks the caller's DataFusion session to create the physical plan.

The provider does not own a second DataFusion session, object access, a native executor, or a
fallback decision. Unsupported predicate lowering is an optimization miss, not a fallback or a
correctness error.

### `BrowserScanPredicates`

`BrowserScanPredicates` carries two related but distinct forms:

- the conservatively lowered Kernel predicate used for Delta file skipping; and
- the complete DataFusion predicate retained for Parquet pruning and residual evaluation.

Initial pushdown MUST report `Inexact`. DataFusion retains the full residual filter even when
Kernel accepted a lowering. Kernel may omit files only under its semantics; DataFusion remains
responsible for row-level correctness. Null, partition, cast, collation, timestamp, decimal, or
other unsupported lowering returns "not lowered" and continues through DataFusion.

### `BrowserDataFusionProfile`

One authoritative `BrowserDataFusionProfile` centrally owns:

- supported view/table-type policy;
- target partitions and repartition rules;
- required optimizer settings and extensions;
- bounded memory pool and structured `resource_exhausted` policy;
- path-free spill selection, storage cap, cleanup, and terminal metrics;
- metadata HTTP concurrency and data HTTP concurrency as separate budgets; and
- bundle capabilities needed by the selected worker SKU.

Provider registration MUST validate this profile and reject incompatible state. It MUST NOT
silently repair session settings. I/O concurrency MUST NOT be derived from CPU target partitions;
metadata and data requests have different latency, fanout, and authority costs.

## Worker, Result, And Fallback Contracts

### Persistent execution

The default remains one persistent Axon worker/session with explicit table registration and cache
reuse. A worker-per-query model is not the default. Worker isolation, deadlines, cancellation,
memory/output budgets, and typed terminal metrics continue across both providers.

### Atomic delivery remains atomic

Existing `sql()` and its `single_buffer` / `chunked_buffers` delivery modes remain atomic. The
private pull-driven cursor may transfer multiple exact-sized buffers, but the SDK does not expose
them until the terminal success is known. `chunked_buffers` is a transport and reassembly choice,
not user-visible progressive streaming.

Atomic execution MUST stage within its output and coordinator budgets. Failure or cancellation
before terminal success discards the staged public result. This is atomic rollback, not an
implicit retry.

### `sqlProgressive()` is a separate API

Progressive delivery is a new `sqlProgressive()` async iterator or `ReadableStream` contract. It
MUST carry consumer credit across every boundary:

```text
SDK consumer
  → public worker
  → coordinator
  → child worker
  → Rust Arrow IPC cursor
```

No layer may pull, encode, transfer, or retain bytes beyond its granted window. The first public
chunk is irrevocable: after it is emitted, a late failure produces a typed terminal stream error
and MUST NOT be represented as an atomic rollback, a successful partial result, or an automatic
native retry. Slow-consumer, cancellation, deadline, and expiry behavior must remain bounded and
observable.

### Explicit native retry only

Before browser admission, resolution or capability policy may return `remote_required` and admit a
native/server execution instead. After browser acceptance, that execution has exactly one
terminal result. Any later native attempt is an explicit new admission with a new execution ID,
separately identified execution target, and correlation to the failed browser attempt. The engine
MUST NOT switch targets invisibly.

This rule satisfies the mandatory native fallback ADR by keeping native execution available and
deterministic without converting an accepted failure into two executions under one identity.

## Compatibility And Supersession Ledger

### Retained architectural assets

The target retains:

- persistent worker/session lifetime and cache reuse;
- the pull-driven continuous Arrow IPC cursor and exact-sized transferable buffers;
- cancellation, deadlines, memory/output budgets, typed errors, and terminal metrics;
- strong ETag, exact-range, `Content-Range`, and `If-Range` validation;
- bounded path-free OPFS spill with cleanup before terminal metrics;
- discovery-only `CatalogProvider`, the sole `DataAccessResolver` seam, and provider-neutral
  access envelopes; and
- the native runtime as correctness oracle and explicit supported route.

### Superseded mechanics

This strategy supersedes:

| Historical mechanic                                                           | Canonical replacement                                                                                         |
| ----------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| Async prefetch followed by synchronous cached Kernel callbacks                | Bounded `SnapshotTask` / `ScanTask` driven asynchronously through `KernelTaskDriver`                          |
| `BrowserHttpSnapshotDescriptor.active_files` as the universal execution input | `BrowserDeltaAccessDescriptor` with root-scoped and per-file variants inside the existing resolved-read union |
| `AxonParquetScanExec` and `wasm-parquet-engine` as the permanent scan engine  | Standard DataFusion Parquet over an Axon `ObjectStore`; compatibility scan remains during migration           |
| Renaming the historical POC into the production engine                        | A fresh independently locked `engines/kernel-datafusion/` shipping workspace and optional bundle              |
| Treating `chunked_buffers` as progressive delivery                            | Atomic reassembly remains; `sqlProgressive()` is a separate credited API                                      |

Historical documents may still describe those mechanics accurately for their implementation
date. They MUST carry a supersession notice when readers could mistake them for the target.

### Compatibility crate roles

`wasm-parquet-engine` converges on browser I/O policy, object identity, validated extents,
coalescing, cache behavior, request/byte metrics, and a temporary compatibility scanner. The
per-file path remains supported: it first uses the current descriptor provider, then migrates to
a manifest-backed standard DataFusion Parquet source without forcing root access.

`wasm-delta-snapshot` is frozen as a compatibility oracle. It receives correctness and security
maintenance needed by the active compatibility provider, but no new long-term protocol
ownership. Remove it only after Kernel snapshot/scan parity, shipping adoption, and rollback
requirements are satisfied.

### Mangrove comparison boundary

The exact [Mangrove source at
`601be3cddbe68a676c7740d75cbce26190ad4279`](https://github.com/open-lakehouse/mangrove/commit/601be3cddbe68a676c7740d75cbce26190ad4279)
is comparison evidence for async browser object-store execution, standard
DataFusion integration, and path-segment-aware routing of multiple table stores
under one authority. Axon adopts those bounded lessons, not Mangrove's whole
runtime. Its worker-per-run lifecycle, unconstrained batch callback, independent
self-contained IPC stream per batch, transparent fallback classification, and
manifest-specific snapshot assumptions do not override Axon's persistent
session, continuous credited cursor, explicit admission, or dual root/per-file
access contract.

### Non-goals

- worker-per-query as the default;
- unconstrained push callbacks or independent Arrow IPC streams per batch by default;
- string-classified errors;
- synchronous Kernel driving, hidden browser Fetch, or unbounded prefetch;
- I/O concurrency derived from CPU partitions;
- automatic post-admission native fallback;
- browser writes or Delta commit/checkpoint creation;
- raw or long-lived browser cloud secrets;
- an Axon-private clone of a rejected Kernel operation protocol;
- treating local GO evidence as a published release or production promotion.

OPFS / IndexedDB may be used only under the explicit cache and spill authority policies. This
design does not make capability-bearing descriptors or grants persistable.

## Shipping Topology

The historical `poc/upstream-wasm-fork-stack` workspace and its evidence remain unchanged. It is
immutable compatibility evidence, not the directory to rename or evolve into production.

Create `engines/kernel-datafusion/` afresh as the shipping engine workspace. It has:

- an independent `Cargo.lock` and reproducible dependency report;
- a separate optional WASM bundle selected by the existing capability/asset policy;
- permission to reuse Axon source crates through deliberate workspace interfaces; and
- exact external dependency revisions that are remotely reachable, with no tracked local path
  overlays.

The supplied local fork heads cannot enter that lock until their commits are published at stable,
reviewable remote refs. Keep the current compatibility bundle, provider, and worker protocol
active until promotion; do not make the isolated shell itself evidence of adoption.

## Productization And Migration Roadmap

### 1. Publication and review gates

Publish in dependency order: `object_store`, Arrow/Parquet, then DataFusion. Publish and tag the
Axon verifier evidence. Circulate Kernel contract head
`4223fa43039d418238f6c4a1304d23e9f3764aa6` on
[issue #252](https://github.com/delta-io/delta-kernel-rs/issues/252). Publication requires its own
authorization and upstream review; this document grants neither.

### 2. Independent Axon preparation

Axon may proceed before Kernel acceptance with:

- the standard brokered `ObjectStore` adapter;
- the isolated `engines/kernel-datafusion/` shell and optional bundle;
- authoritative `BrowserDataFusionProfile` validation;
- the path-segment-aware prefix router; and
- a host parity harness that can run compatibility-provider, target-provider, and native-oracle
  lanes over the same fixture/query identity.

These changes MUST NOT invent the Kernel protocol or claim target-provider parity.

### 3. Kernel and delta-rs

After explicit Kernel maintainer acceptance, implement these slices in order:

1. **K1, exact browser target policy:** select the target-safe randomness and
   feature closure without weakening native defaults.
2. **K2, bounded operation protocol and native adapter:** land the paged
   `Operation` / `PlanResult` forms, accounting contracts, misuse semantics,
   cancellation input, and synchronous reference driver without changing
   snapshot behavior.
3. **K3, `SnapshotTask`:** port snapshot discovery/replay one I/O edge at a
   time and prove checkpoint, tail, CRC, catalog, and time-travel parity under
   finite limits.
4. **K4, `ScanTask`:** resume checkpoint-shape probes and return the accepted
   scan-owned plan without a browser-only operator.
5. **K5, `DataFusionPlanCompiler`:** split target-neutral logical lowering
   from execution so native and browser runners reuse the same compiler.

K4 also waits for the accepted successors of the still-open declarative-plan
work:

- [#3015: scan metadata output options](https://github.com/delta-io/delta-kernel-rs/pull/3015)
- [#3024: `Load` to `DynamicScan`](https://github.com/delta-io/delta-kernel-rs/pull/3024)
- [#3039: scan-owned plan construction](https://github.com/delta-io/delta-kernel-rs/pull/3039)

Issue [#252](https://github.com/delta-io/delta-kernel-rs/issues/252) and all three
PRs were open when this revision was authored.

Only after K1-K5 are complete may delta-rs add the production browser/WASM facade. If maintainers
reject or materially revise the operation-task seam, stop and revise this strategy before doing
Kernel or delta-rs implementation.

### 4. Parallel Axon provider

Add one-table root-scoped Kernel execution over standard DataFusion Parquet. Keep the descriptor
provider active as compatibility. Every supported query and fixture runs differential snapshot,
plan, result, error, and metric checks across the compatibility provider, Kernel provider, and
native oracle where applicable.

### 5. Access and delivery convergence

Extend `ResolvedBrowserRead` with the root-scoped/per-file Delta access union, then enable multiple
routed roots. Move per-file grants to the manifest-backed standard Parquet source. Add
`sqlProgressive()` only after the full credit path and irreversible-first-chunk semantics pass
their own browser tests.

### 6. Promotion and retirement

Make the Kernel-native provider the default only for qualified root-scoped access after parity,
memory, request-byte, cancellation, spill-cleanup, artifact-size, and browser gates pass. Per-file
access remains a supported route. After a rollback window and release-evidence review, freeze and
eventually remove the bespoke replay and physical-scan layers.

Promotion is per access class and browser. A Chrome pass cannot promote Firefox, and a root-scoped
pass cannot promote per-file access. Local, public-object, canary, and production-default evidence
remain separate.

## Implementation Acceptance Matrix

| Area                          | Required evidence before promotion                                                                                                                                                                                                                                                                                      |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Kernel task protocol          | Start/resume state machine; monotonically matched IDs; wrong IDs and result variants; empty/final pages; pagination at sizes 1, 2, and configured maximum; malformed, reordered, overlapping, or oversized responses; every per-page and cumulative counter; typed resource exhaustion; exact one-shot error ownership. |
| Cancellation and deadlines    | Before first I/O, between pages, during pending Fetch, after completion, and late-result rejection; driver-owned I/O is released and no request occurs after terminal state.                                                                                                                                            |
| Snapshot and scan parity      | Commit-only, V1 single/multipart checkpoints, inline V2 and manifest/sidecars, checkpoint plus tail, exact/latest/time travel, catalog tails, CRC policy, no-match scans, and unsupported protocol features across compatibility provider, Kernel provider, and native oracle.                                          |
| Predicate correctness         | Projection; null and partition semantics; supported/unsupported lowering; Kernel file skipping; Parquet row-group and page pruning; casts and transforms; full residual retention; no false exclusion.                                                                                                                  |
| Access and identity           | Root-scoped and per-file access; local/public/signed/proxy classes; earliest expiry; capability mismatch; ETag drift; exact ranges; `If-Range`; normalized longest-prefix routing; same-bucket multi-table isolation; no credential or cache leakage.                                                                   |
| DataFusion profile and memory | View-type policy; partition/repartition configuration; optimizer requirements; independent metadata/data concurrency; bounded memory pool; path-free OPFS spill; storage exhaustion; cancellation; cleanup; zero active spill files/scopes at terminal metrics; structured `resource_exhausted`.                        |
| Atomic results                | `single_buffer` and `chunked_buffers`; coordinator and output budgets; exact-sized transfers; rollback on cancellation, expiry, output exhaustion, and late execution failure; no public bytes before terminal success.                                                                                                 |
| Progressive results           | End-to-end consumer credit; slow and stopped consumers; bounded buffering at every hop; cancel/deadline/expiry; first public chunk irrevocability; typed late failure; no automatic native replay.                                                                                                                      |
| Dependency and artifact       | Independent lock; every exact revision remotely reachable; no tracked local path overlays; denied dependency graph; reproducible source report; raw, gzip, and Brotli bundle size.                                                                                                                                      |
| Browser and efficiency        | Real worker I/O in Chrome and Firefox; exact request count, requested bytes, response bytes, overfetch, cache provenance, peak operator/process memory as separately named measures, startup and steady-state latency, cancellation request cutoff, and spill cleanup.                                                  |

Every matrix result records provider, access class, browser/version, Axon commit, engine bundle hash,
dependency lock hash, fixture provenance, query corpus hash, budget profile, and whether the proof is
local, remotely reproducible, canary, or production-default evidence.

## Decision Stop Conditions

Stop promotion or implementation at the applicable boundary when:

- Kernel maintainers have not accepted the operation-task seam;
- K4's declarative-plan dependencies have not landed in accepted form;
- an exact external revision is not remotely reachable;
- the shipping workspace requires a tracked local path overlay;
- result, error, request-byte, or snapshot parity diverges without a classified cause;
- a budget can truncate correctness rather than return a typed terminal error;
- spill cleanup, cancellation, identity validation, or route isolation is unproven;
- progressive credit can be bypassed at any hop; or
- release evidence cannot distinguish local GO, remote reproducibility, maintainer acceptance,
  shipping adoption, and production default.

## Primary References

- [Delta Kernel issue #252](https://github.com/delta-io/delta-kernel-rs/issues/252)
- [Delta Kernel PR #3015](https://github.com/delta-io/delta-kernel-rs/pull/3015)
- [Delta Kernel PR #3024](https://github.com/delta-io/delta-kernel-rs/pull/3024)
- [Delta Kernel PR #3039](https://github.com/delta-io/delta-kernel-rs/pull/3039)
- [Mangrove comparison source at `601be3cddbe68a676c7740d75cbce26190ad4279`](https://github.com/open-lakehouse/mangrove/commit/601be3cddbe68a676c7740d75cbce26190ad4279)
- [May 2026 Kernel/DataFusion execution plan](../plans/2026-05-07-browser-delta-kernel-datafusion-engine-execution-plan.md)
- [Upstream WASM support implementation plan](../plans/2026-07-23-upstream-wasm-support-implementation-plan.md)
- [Upstream WASM fork POC evidence](../release-gates/upstream-wasm-fork-poc-evidence.md)
