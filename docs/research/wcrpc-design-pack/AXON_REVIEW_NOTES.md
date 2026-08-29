# WCRPC design pack review notes

Started: 2026-07-15

Source: `/Users/ethanurbanski/Downloads/wcrpc_design_pack`

Imported copy: `docs/research/wcrpc-design-pack/`

Review status: full first-pass review of the 30 substantive files in the pack. The two `.DS_Store` files were Finder metadata, so the import excludes them.

Axon grounding: I compared the pack with the current checkout and the local `origin/main` ref at `b11a44b`. The root checkout was dirty, four commits ahead of and 55 commits behind that ref, so claims about landed contracts use `origin/main` rather than stale root files.

External-source status: this pass reviews the design pack as supplied. It does not revalidate the linked Quack, Ballista, or Arrow claims against their current upstream documentation.

## Corpus notes

- The corpus contains 28 Markdown files, one TypeScript contract sketch, and one protobuf sketch, totaling 5,743 lines.
- [WCRPC-full-design-spec.md](WCRPC-full-design-spec.md) contains the normalized content of [docs/00-executive-brief.md](docs/00-executive-brief.md) through [docs/12-risk-register.md](docs/12-risk-register.md), separated by horizontal rules. A normalized comparison produced the same SHA-256 for the modular chapters and the consolidated file.
- The pack does not declare whether the modular chapters, consolidated file, ADRs, or reference contracts control when they disagree. A future edit needs one normative source and a generated consolidation step.
- Every ADR has `Proposed` status. The pack records a design proposal, not accepted Axon architecture.

## Working vocabulary

- **Public worker protocol:** the SDK-facing command, event, and terminal-response surface. WCRPC leaves this surface in place.
- **Coordinator worker:** the one visible worker that classifies a query, chooses single-worker or pool execution, builds the shard plan, allocates budgets, fans out cancellation, and rolls up metrics.
- **Child worker:** one Web Worker with its own Wasm module, `BrowserDataFusionSession`, `WasmDataFusionEngine`, shard descriptor, and local execution budget.
- **WCRPC:** an internal browser-local protocol for schema-first columnar streams between the coordinator, child workers, and a later merge worker.
- **Payload codec:** the representation of analytical data carried by a WCRPC stream. The stream lifecycle and payload representation can change without forcing one another to change.
- **Exchange mode:** the routing rule between stage partitions. The proposal implements gather first and reserves hash, range, broadcast, round-robin, and top-k.
- **MergePlan:** the correctness contract that explains how shard-local outputs become one result with DataFusion-equivalent SQL semantics.
- **Query budget:** the global cap across the pool. A stream budget applies to one child stream and must fit inside the query budget.

## Current mental model

```text
Active SDK worker command/event protocol
(axon.exec.v1 is a declared contract without runtime adoption)
        |
        v
Visible coordinator worker
  - existing public command and response interface
  - query eligibility and cost decision
  - deterministic candidate-file sharding
  - global budget, cancellation, and metrics
        |
        v
BrowserDataFusionWorkerPool
        |
        +-- WCRPC --> child WebWorker + Wasm/DataFusion + shard A
        +-- WCRPC --> child WebWorker + Wasm/DataFusion + shard B
        +-- WCRPC --> child WebWorker + Wasm/DataFusion + shard C
        |
        v
Coordinator merge in the first slice
Dedicated Wasm merge worker in a later slice
        |
        v
One final Arrow IPC result through the existing public protocol
```

WCRPC deepens the coordinator-plus-child-worker proposal in [the workspace-only worker-pool design](../../plans/2026-07-05-browser-datafusion-worker-pool-design.md). That design file is untracked in this checkout and absent from the local `origin/main`, so it does not represent accepted or landed architecture. WCRPC adds a stream lifecycle, payload codecs, byte and batch credits, schemas, terminal status, stage identity, cancellation, and per-stream metrics.

The portable path uses `MessagePort` or `postMessage` with transferable `ArrayBuffer` values. It does not require cross-origin isolation. The proposed first release supports projection and filter queries plus a controlled set of mergeable aggregates. It rejects joins, windows, unsafe aggregates, global ordering, and other shapes that lack a proven merge plan.

Later work adds a dedicated merge worker, direct worker-to-worker ports, Arrow buffer descriptors, native aggregate-state payloads, top-k, shared slabs, and richer exchange. Those capabilities need separate evidence and negotiation.

## Intended invariants I see across the pack

1. WCRPC stays behind the active public worker protocol. If Axon adopts `axon.exec.v1` at runtime, WCRPC also stays behind that outer provider contract.
2. DataFusion owns shard-local planning and execution. Axon owns browser-level eligibility, sharding, orchestration, merge, budgets, cancellation, and path evidence.
3. The coordinator starts from the same pruned candidate-file set that single-worker execution would scan.
4. The prose expects each worker-pool decision to record a reason, deterministic shard assignment, output schema, codec, and metrics plan. The TypeScript sketch does not enforce the reason or metrics-plan parts for its `worker_pool` arm.
5. Every non-append query carries an explicit `MergePlan` that preserves DataFusion semantics and final Arrow types.
6. Each stream starts with coordinator headers and initial credit. The producer sends a schema before data, follows ordered frame rules, spends byte and batch credit, and ends with one terminal outcome.
7. A child failure cancels its siblings. The first version returns no successful partial result.
8. The sum of in-flight payload bytes across child streams stays inside one query-level cap.
9. The portable baseline uses transferable buffers. Shared memory remains an optional deployment tier.
10. Browser-Wasm success needs evidence. A native fallback cannot appear as worker-pool success.

## Axon fit

| Area | Axon today | WCRPC proposal |
|---|---|---|
| Public execution boundary | SDK callers use the existing worker command, event, and response protocol. The local `origin/main` declares `axon.exec.v1` with directly openable descriptors and service descriptors, but it has no adopted E9 provider, Connect transport, client, server, or runtime implementation. | WCRPC belongs behind the active worker protocol and any later `axon.exec.v1` adoption. The proposed `WasmColumnarExecutor` proto must not become a competing public service. |
| Visible worker | [sandbox-query-worker.ts](../../../apps/axon-web/src/sandbox-query-worker.ts) owns one session, serializes normal commands, and tracks one active query. | The visible worker becomes a coordinator that hides a child pool behind the same caller-facing commands. |
| Query engine | [BrowserDataFusionSession](../../../crates/wasm-datafusion-session/src/lib.rs) wraps `WasmDataFusionEngine`; the browser baseline remains single-partition per Wasm instance. | Each child keeps that honest single-instance path. Browser Web Workers provide parallelism above it. |
| Arrow delivery | The worker can post exact-sized transferable chunks, but it waits for `session.sql()` to return a complete byte array before slicing it. | Child workers need a new Rust/Wasm producer seam that emits batches or IPC messages while execution runs. Credits around the current completed buffer would not bound encoder memory. |
| Query limits | Axon caps rows, Arrow bytes, preview strings, and scan bytes. Callers supply `result_page`; `BrowserDataFusionSession` injects `LIMIT/OFFSET` before execution. | The coordinator adds global workers, intermediate state, in-flight bytes, and deadlines. It must apply paging once after merge rather than once per shard. |
| Object access | Axon uses typed openable descriptors, object identity, typed partition values, signed HTTPS or a narrow proxy, and browser-safe range reads. | Shard plans should reuse those descriptors. `descriptor_json` and reduced path-only `CandidateFile` values would create a weaker parallel vocabulary. |
| Caches | Each browser session owns its runtime, metadata cache, and range cache. | Child workers duplicate those in-memory caches unless a separate shared substrate coordinates them. Shared result slabs do not solve duplicate metadata or range reads. |
| Shared memory | [The browser deployment guidance](../../program/browser-embedding-deployment.md) keeps cross-origin isolation optional and gates threaded tiers by capability. | The pack follows the same posture: transferables first; shared slabs require COOP/COEP and browser proof. |
| Correctness oracle | Native DataFusion and the host UAT corpus remain comparison targets. | Every supported merge shape needs single-worker, native, host-UAT, and randomized-sharding parity. |

The pack aligns with Axon’s browser-first execution boundary. WCRPC can deepen internal worker exchange without replacing the active public worker envelope, preempting later `axon.exec.v1` runtime adoption, or weakening the trusted descriptor model.

## Strong parts of the proposal

- The coordinator module forms a deep seam. Callers do not need shard IDs, child lifetimes, codecs, or exchange topology.
- The proposal keeps row-oriented JSON and protobuf out of the hot data path.
- The query classifier limits distributed execution to shapes with explicit merge semantics.
- The design treats schema, dictionary, null, decimal, timestamp, grouping, and final-cast behavior as correctness work.
- The global budget and credit model addresses the browser failure mode where all children emit full results at once.
- The proposal distinguishes portable transferables from optional shared memory.
- The test plan combines state-machine tests, browser proof, oracle parity, property tests, fuzzing, UAT, and measured performance.
- The rollout uses opt-in and cost gates. It asks whether parallelism pays for worker startup, table open, transfer, merge, and final emission.

## Design gates before implementation

### 1. Choose the normative artifacts

The pack repeats core types in the architecture chapter, planning chapter, protocol chapter, runtime chapters, TypeScript sketch, and proto sketch. Those definitions already drift. Pick one normative v0 contract, generate the consolidated spec and reference bindings where possible, and label every other example as explanatory.

### 2. Define the first executable slice

The executive brief calls grouped aggregates part of the first version. The rollout places projection/filter in Phase 2 and aggregates in Phase 3. Treat the first browser proof as gather plus projection/filter. Add aggregate merge after the transport, planner, and safety invariants work. Assert that every child keeps `target_partitions == 1`, including shards with several files, so the proof does not reintroduce intra-Wasm multi-partition execution.

### 3. Preserve the outer execution contract

Map WCRPC requests, cancellation, errors, events, Arrow chunks, metrics, and terminal responses into the active worker protocol. Keep WCRPC child methods and frame details internal. Treat `axon.exec.v1` as a declared outer contract that still lacks runtime adoption. The optional `axon.wcrpc.v1.WasmColumnarExecutor` sketch should not enter public codegen in its current form.

### 4. Make control bidirectional

The stream lifecycle sends headers and credits from coordinator to child, then schemas, data, metrics, and trailers from child to coordinator. The proto defines `ExecuteShard` as a server stream, so the coordinator cannot grant more credit after the request starts. A `MessagePort` transport can carry both directions. The normative contract must model that duplex behavior.

### 5. Define the data carrier

Both reference files define `BufferRef`, but neither defines the message object that carries the frame and its attached `ArrayBuffer[]`. Specify an atomic envelope such as `{ frame, buffers }`, attachment ordering, bounds checks, transfer-list behavior, ownership after send, and release or crash cleanup.

### 6. Write the complete stream state machine

Specify legal frame order, initial credit, schema and dictionary rules, data-after-cancel behavior, late frames, transport close, one terminal outcome, and cancellation precedence. The current strict `seq` rule is ambiguous because credits and data travel in opposite directions. Use one sequence space per direction or separate control and data stream identities.

The standalone `error` frame also overlaps with `TrailersFrame.error`. Choose one terminal model. A useful rule would let diagnostic errors precede trailers while trailers remain the sole terminal frame.

### 7. Define an IPC chunk

The pack uses “Arrow IPC chunk” for several possible units: arbitrary bytes from one stream, one IPC message, one record batch, or one self-contained stream segment. Incremental parsing, schema handling, dictionaries, credit accounting, and final-stream assembly depend on this choice.

Current Axon chunks split a completed byte array into fixed-size pieces. WCRPC needs an engine-side emission unit if it intends to overlap execution, transfer, and merge.

Internal streaming also creates a public-result atomicity choice. If the coordinator forwards final IPC chunks before every child succeeds, a late failure leaks a partial result despite the v0 no-partial-result rule. If the coordinator waits for all children, it must buffer or spool the complete final result and retains much of the materialization pressure. The design needs an explicit staging, commit, or discard model for public chunks.

### 8. Put eligibility, pruning, and paging behind the Rust authority

The coordinator needs a serializable decision, but TypeScript should not reimplement DataFusion query classification or candidate pruning. Expose one narrow Rust interface that returns eligibility, the pruned candidate descriptors, deterministic shard inputs, final schema, merge plan, budgets, and structured reason.

Global paging needs explicit handling in the first append slice. Axon adds `LIMIT/OFFSET` for browser-safe result pages. Sending the same rewritten SQL to every child would apply the page per shard and return the wrong global result. The planner must separate child work from final paging.

### 9. Complete the merge contract

The reference `MergePlan` carries little beyond a kind and schemas. A usable contract needs aggregate state columns, group-key rules, final casts, order keys, limit and offset, dictionary policy, engine and codec versions, overflow rules, and final-schema fingerprints.

The pack gives good truth tables for `COUNT`, `SUM`, `MIN`, `MAX`, and `AVG`. It still needs numeric widening, decimal overflow, floating aggregates, collation, tie handling, and unordered `LIMIT/OFFSET` semantics.

### 10. Make budgets composable

The proposed default names 32 MiB of initial byte credit but does not define whether that value applies to a query or a stream. Axon’s browser Arrow output cap is lower, and the query-level in-flight cap remains optional. Assigning 32 MiB to each child could violate the global invariant at stream start.

Define concrete production caps, the query-to-stream split, payload versus allocation accounting, frame overhead, credit refunds, fairness, oversized-frame handling, deadlock detection, and forced termination when a worker ignores cooperative cancellation.

### 11. Specify schema and version identity

Define canonical Arrow schema serialization, the fingerprint algorithm, metadata inclusion, dictionary lifecycle, schema-ID scope, and aggregate-state version coupling. Advertise supported protocol and codec ranges rather than one exact version if old and new workers must interoperate.

Make dictionary rejection the v0 rule unless the first slice includes a complete normalization design and parity suite.

### 12. Use safe numeric types

The TypeScript sketch uses `number` for sequence values, rows, bytes, budgets, and metrics. The proto uses `uint64`, and Axon’s generated TypeScript represents those fields as `bigint`. WCRPC should follow the generated-contract convention and use decimal strings at JSON boundaries.

### 13. Design worker and cache lifecycle

Specify warm-pool size, worker reuse, table registration lifetime, descriptor refresh, crash replacement, forced termination, concurrent-query admission, and memory eviction. Scope warm workers, table registrations, caches, descriptors, and ports by tenant or session identity. Include duplicate table-open, metadata, and range-read costs in the benchmark model.

Preserve [ADR-0002](../../adr/ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md): validate descriptor expiry and origin, keep cloud credentials out of workers, and use typed browser-safe descriptors. Reuse [`redactUrlSecrets`](../../../apps/axon-web/src/axon-browser-sdk.ts) at every error and log boundary. Do not place raw descriptors, signed URLs, original SQL, or uncontrolled error `details` in metrics labels, traces, or public worker logs.

### 14. Map internal evidence to Axon evidence

The pack uses `execution_target = "BrowserWasm"`; Axon uses `QueryResponse.executed_on` and generated enum values. Define one total mapping for WCRPC statuses to `QueryErrorCode` and `FallbackReason`, plus an additive worker-pool metrics message. Free-form reason strings will make gates and dashboards brittle.

### 15. Move minimum safety and evidence into the first fanout slice

The rollout postpones the global credit allocator, deadline propagation, and full metrics until after projection and aggregate fanout. The first real two-worker execution should include a bounded global window, sibling cancellation, a deadline, path metrics, and one terminal result. Later phases can tune those mechanisms.

The first browser proof should also assert `target_partitions == 1` inside each child. Web Worker fanout must remain the source of parallelism.

Before the 25 percent speedup and 95 percent selection gates control rollout, define the “eligible scan-heavy” thresholds, warmups, sample count, browser and hardware pins, variance or confidence rule, cache state, and treatment of transfer and merge overlap. Summing overlapped phases can double-count the critical path.

## Contract drift found in the pack

| Contract | Drift |
|---|---|
| `WcrpcCapabilities` | The protocol, codec, and deployment chapters define different fields. The TypeScript reference omits the complete capability contract. |
| `ShardedQueryPlan` | The architecture version adds `fallbackPolicy`; the planning version adds candidate files, shard descriptors, and `metricsPlan`. The TypeScript sketch includes candidate files and shard descriptors but omits `fallbackPolicy` and `metricsPlan`. |
| `StagePlan` | One version uses `inputs` and `outputs`; another uses tasks plus input/output schemas; the reference reduces `kind` to `string`. |
| `QueryBudget` | The runtime chapter includes output rows and intermediate limits; the TypeScript sketch omits them. Several later invariants treat optional limits as required. |
| `MergePlan` | The merge chapter requires group keys, aggregate states, casts, ordering, paging, and versions. The TypeScript sketch lacks them, and the proto has no merge plan. |
| Frame payloads | The generic TypeScript envelope does not tie `kind` to payload. It also permits contradictory pairs such as envelope `data` with payload `record_batch` and envelope `aggregate_state` with payload `aggregate_state_batch`. It lists dictionary, release, metrics, and other kinds without defining all payload types. The proto uses `string kind` plus opaque bytes. |
| Cancellation | One `CancelFrame` relies on the envelope query ID; another repeats `queryId` in the payload. The text allows narrower scope but defines no scope field. |
| Task and partition identity | ADR-0007 requires stage, source-task, input-partition, and output-partition IDs. The proto request and frame carry stage and stream IDs but omit task and partition identity. |
| Method set | TypeScript headers allow `OpenTable`, `ExecuteShard`, `ExecuteStage`, `Merge`, and `GetMetrics` but omit `Cancel`. The proto service exposes `OpenTable`, `ExecuteShard`, `Cancel`, and `GetMetrics` but omits `ExecuteStage` and `Merge`. |
| Data attachments | `bufferIndex` has no normative attached-buffer list in either reference. The proto cannot express transferable browser ownership. Its enum defaults to `TRANSFERABLE` with no unspecified value, and flat fields can represent transferable and shared-slab state at the same time. Use a validated discriminated union or proto `oneof`. |
| Metrics | The TypeScript sketch carries rich stream metrics; the proto carries only completed streams, bytes, and rows. The public Axon metric shape uses different field names. |
| Versioning | The prose calls the protocol `0.1-draft`; the TypeScript wire version is `1`; the proto package is `axon.wcrpc.v1`; compatibility behavior remains undefined. |

## Initial recommendation

Before writing production code, accept or revise the ten ADRs and write one normative internal v0 transport contract with this scope:

1. gather exchange for v0;
2. projection/filter append for the first proof;
3. Arrow IPC plus transferable buffers;
4. typed bidirectional `MessagePort` control;
5. a complete state machine and attachment envelope;
6. one Rust-owned eligibility and shard-plan result;
7. bounded global credits, cancellation, deadline, and proof metrics;
8. an adapter that leaves the active public worker commands and the declared `axon.exec.v1` contract intact.

Then run a two-worker browser proof against the current single-worker and native oracles. Add aggregate state and Wasm merge after that slice proves correctness, memory bounds, and enough speedup to justify the extra protocol surface.

## Questions to carry forward

- Does the first two-worker proof need a named WCRPC protocol, or should a smaller typed stream adapter prove the performance premise before the team freezes a protocol?
- Which file or generated schema will own frame definitions?
- Does each direction get its own sequence counter?
- Which IPC unit can the Rust/Wasm engine emit before full query completion?
- Which Rust interface can expose eligibility and candidate pruning without exposing DataFusion internals to TypeScript?
- How will the coordinator apply global result paging and preview limits?
- How will child workers refresh expiring signed descriptors without changing immutable shard identity?
- How will the query budget divide credit under skew when one child produces far more output?
- Which worker-pool metrics belong in the public contract, and which stay in debug traces?
- How will the pool account for duplicated Wasm memory, table state, metadata caches, and range caches?
- How will public chunk delivery preserve all-or-nothing results after a late child failure?
- Which tenant or session identity scopes warm workers, table registrations, descriptors, caches, and ports?
- Which browser and hardware matrix determines the 25 percent warm-query speedup gate?
- Who owns each risk, and when does the team revisit it?

## File-by-file reading log

### Pack entry points

- [README.md](README.md): Defines the scope, document map, design thesis, portable topology, future topology, and prior-art links. It makes WCRPC internal and frames the pack as an expansion of the existing worker-pool design.
- [WCRPC-full-design-spec.md](WCRPC-full-design-spec.md): Consolidates the 13 modular chapters. Its normalized content matches those chapters, so it adds packaging value but no separate design decisions.

### Core design chapters

- [docs/00-executive-brief.md](docs/00-executive-brief.md): Explains the move from opaque child result blobs to streamed schema, data, credit, cancellation, metrics, and trailers. It defines the broad first-version query set and public-boundary rule.
- [docs/01-research-brief-quack-ballista-arrow.md](docs/01-research-brief-quack-ballista-arrow.md): Borrows codec flexibility from Quack, exchange identity from Ballista, compatibility from Arrow IPC, and metadata/body separation from Dissociated IPC. The lessons remain hypotheses until Axon benchmarks them.
- [docs/02-architecture-spec.md](docs/02-architecture-spec.md): Defines the SDK, coordinator, deep worker-pool module, child workers, merge worker, endpoints, topologies, planning layers, failure model, and performance equation.
- [docs/03-wcrpc-protocol-spec.md](docs/03-wcrpc-protocol-spec.md): Defines protocol identity, lifecycle, envelope, headers, schemas, data frames, buffer refs, codecs, credits, release, cancellation, errors, trailers, status, metrics, sequencing, and capabilities. It needs a normative duplex state machine and typed payload union.
- [docs/04-planning-execution-model.md](docs/04-planning-execution-model.md): Defines pool, single-worker, and unsupported outcomes; candidate pruning; deterministic size-aware sharding; stages; tasks; exchanges; merge plans; and planner invariants. It flags the Rust-versus-TypeScript pruning seam as unresolved.
- [docs/05-payload-codecs-and-memory.md](docs/05-payload-codecs-and-memory.md): Defines IPC chunks, Arrow buffer descriptors, native aggregate state, and shared slabs. It accepts one portable Wasm-to-JS copy and proposes 8,192-row batches, 32 MiB initial credit, eight batches, exact schemas, and dictionary rejection.
- [docs/06-budgets-backpressure-cancellation.md](docs/06-budgets-backpressure-cancellation.md): Defines query and stream budgets, credit spending, query-scoped cancellation, sibling failure, limited startup retry, deadlines, memory protection, and one terminal SDK result.
- [docs/07-merge-semantics.md](docs/07-merge-semantics.md): Defines append rules, aggregate truth tables, grouped-key encoding, final schema preservation, future top-k and distinct, fail-closed errors, and parity requirements.
- [docs/08-observability-benchmarks.md](docs/08-observability-benchmarks.md): Defines worker-pool and WCRPC metrics, benchmark dimensions, trace names, report fields, a 25 percent warm-query speedup target, and a 95 percent small-query single-worker selection target.
- [docs/09-security-compatibility-deployment.md](docs/09-security-compatibility-deployment.md): Treats frames as untrusted, defines portable and shared-memory deployment, capability checks, frame validation, resource caps, explicit fallback, layered versioning, compatibility tests, and disabled-by-default rollout.
- [docs/10-rollout-plan.md](docs/10-rollout-plan.md): Sequences design, protocol core, projection/filter fanout, aggregate merge, safety hardening, codec experiments, direct exchange, top-k, shared slabs, and broader exchange. Minimum safety needs to move into the first real fanout slice.
- [docs/11-test-plan.md](docs/11-test-plan.md): Covers lifecycle, credit, codec, schema, planner, merge, browser, Wasm, property, fuzz, performance, and UAT tests. It should add worker-hang, port-close, lost-credit, double-release, forced-termination, and race coverage.
- [docs/12-risk-register.md](docs/12-risk-register.md): Captures merge, startup, memory, schema, dictionary, aggregate, fallback, shared-memory, codec, coordinator, ordering, grouping, cancellation, browser, and complexity risks. It needs named owners, review dates, and cache, paging, deadlock, expiry, contention, and telemetry risks.

### Proposed ADRs

- [adrs/ADR-0001-browser-worker-runtime.md](adrs/ADR-0001-browser-worker-runtime.md): Chooses Web Workers as the parallel runtime, with one independent Wasm/DataFusion session per child.
- [adrs/ADR-0002-wcrpc-internal-protocol.md](adrs/ADR-0002-wcrpc-internal-protocol.md): Introduces WCRPC between coordinator, child, and merge workers while preserving the public protocol.
- [adrs/ADR-0003-control-data-plane-split.md](adrs/ADR-0003-control-data-plane-split.md): Separates small control frames from Arrow/DataFusion data and bans row-wise JSON or protobuf on the hot path.
- [adrs/ADR-0004-payload-codec-strategy-quack-inspired.md](adrs/ADR-0004-payload-codec-strategy-quack-inspired.md): Separates stream semantics from payload codecs and reserves engine-aware formats.
- [adrs/ADR-0005-baseline-transferable-arraybuffer.md](adrs/ADR-0005-baseline-transferable-arraybuffer.md): Selects `MessagePort` or `postMessage` with transferable `ArrayBuffer` values for v0.
- [adrs/ADR-0006-optional-shared-slab-fastpath.md](adrs/ADR-0006-optional-shared-slab-fastpath.md): Keeps shared slabs optional and deployment-gated.
- [adrs/ADR-0007-stage-partition-exchange-model-ballista-inspired.md](adrs/ADR-0007-stage-partition-exchange-model-ballista-inspired.md): Adds stage, task, input-partition, and output-partition identity while implementing gather first.
- [adrs/ADR-0008-merge-in-wasm.md](adrs/ADR-0008-merge-in-wasm.md): Keeps TypeScript in orchestration and moves hot aggregate, grouped, and top-k merge loops into Wasm.
- [adrs/ADR-0009-stable-public-protocol.md](adrs/ADR-0009-stable-public-protocol.md): Hides WCRPC and child topology from SDK callers, exposing reviewed metrics and fallback evidence.
- [adrs/ADR-0010-opt-in-rollout-and-cost-gating.md](adrs/ADR-0010-opt-in-rollout-and-cost-gating.md): Starts behind an opt-in flag and can enable automatically when shape, file count, bytes, output, and browser capability predict a gain.

### Implementation aids

- [implementation/checklist.md](implementation/checklist.md): Lists build work and completion evidence. It needs dependency order, owners, acceptance details, compatibility, lifecycle, buffer ownership, and descriptor-security work.
- [implementation/decision-log-template.md](implementation/decision-log-template.md): Provides a useful decision record shell. It should add an ID, owner, approvers, affected interfaces, supersession, evidence date, compatibility impact, and revisit trigger.
- [implementation/epics-and-milestones.md](implementation/epics-and-milestones.md): Groups the work into protocol, credit, worker integration, planner, aggregate merge, observability, codec experiments, direct exchange, and shared slabs. Planner and baseline observability need to precede or accompany worker integration.

### Reference sketches

- [reference/typescript/wcrpc-types.ts](reference/typescript/wcrpc-types.ts): Supplies useful names but does not define an implementation-safe contract. Its frame envelope is not a discriminated union, future capabilities are constructible in v0, numeric types conflict with generated Axon conventions, and planner and merge types omit required semantics.
- [reference/proto/wcrpc.proto](reference/proto/wcrpc.proto): Sketches a control service and buffer references. Its server stream cannot carry ongoing credits from coordinator to worker, its frames and status use untyped strings and bytes, and its descriptor, budget, metric, and optional-presence choices do not match the richer design or current Axon contracts. Its `uint64` fields also require explicit `bigint` and JSON-boundary handling in TypeScript.
