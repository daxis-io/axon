# E9 — Execution Provider and Data-Access Vertical Slices

- Date: 2026-07-15
- Audit revision: 2026-07-16
- Status: Proposed
- Audited baseline: `origin/main` at
  `7681f1dfa5bdaaae3ff2ccff79cc8be76ec1503a`
- Per-slice dependencies: Slice 1 needs E0 state; one intentional E3A correction
  PI follows Slice 1 and gates Slice 2; Slice 3 needs E1 logical table references
  and E6 session-proxied access
- Feeds: E2 editor runs, E5 results, E7 insights, and E8 file preview
- Related: [workbench strategy](../program/rich-lakehouse-workbench-strategy.md),
  [provider model](../program/provider-model.md),
  [ADR-0002](../adr/ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md),
  [ADR-0010](../adr/ADR-0010-pluggable-catalog-providers.md), and the
  [E3A exec handoff](./2026-07-11-e3a-exec-contract-pi-session.md)

## Outcome

E9 makes resource selection, read authorization, and execution compose without
letting any layer silently choose a different table or move credentials across
the wrong trust boundary.

The roadmap uses this exact gated sequence:

1. E9 Slice 1 fixes selected-source integrity and lifecycle without protobuf
   changes and maps domain `execution_id` to existing worker correlation fields.
2. One intentional E3A correction PI updates the unadopted wire contracts.
3. E9 Slice 2 adopts local Delta and public-object resolver/executor seams.
4. E9 Slice 3 composes E1 and E6 for one session-proxied Unity Catalog browser
   query.
5. Governed remote execution waits for a concrete host.
6. E8 runtime adoption waits for a read-only volume consumer.

The last two extensions are independently gated and neither blocks the other.
Each started slice goes from a selected resource to a visible terminal outcome
with security, cancellation, cache, and audit behavior exercised end to end. No
milestone exists solely to add another provider interface or protobuf package.

## Current implementation

The audited baseline already has working browser execution for local Delta,
public object storage, and a sample manifest. It also has:

- landed `axon/common/v1`, `axon/catalog/v1`, `axon/dataaccess/v1`,
  `axon/exec/v1`, and `axon/fs/v1` message packages;
- `axon/dataaccess/v1` read outcomes and browser-safe descriptors;
- `axon/exec/v1` query, worker event, response, preview, and cancel messages;
- `axon/fs/v1` filesystem roots, entries, list/stat, and read-resolution values
  as messages-only substrate, with no E8 provider, adapter, UI, or runtime
  consumer;
- a TypeScript browser worker and a Rust/WASM query runtime;
- generated TypeScript for all five packages and Buffa Rust contract proof for
  common, data-access, execution, and filesystem messages; and
- local session reuse, identity-safe bounded Parquet readahead, range-read
  metrics, Arrow IPC results, and structured worker errors.

`30d7e8bdc729902a7a5f50cd3d30feb7efdd2348` added cache and bounded-readahead
worker metrics. `7681f1dfa5bdaaae3ff2ccff79cc8be76ec1503a` then landed bounded,
strong-identity Parquet scan readahead. Neither change establishes
selected-source integrity, one execution lifecycle, capability disposal, or the
provider seams in this plan.

The current runtime does not yet implement the E9 provider seams. In
`services/query-source.ts`, a missing or invalid active selection can fall
through to another queryable table and ultimately `SAMPLE_QUERY_SOURCE`.
Execution also uses multiple identifiers across open, query, and cancel paths,
and cancellation acknowledgement is not yet the lifecycle authority for one
accepted execution.

The landed `QueryEngine` declaration is a browser-worker compatibility surface.
There is no approved remote execution service on the baseline.

## Target modules

### `DataAccessResolver`

The resolver answers one question: how may this caller read this selected
resource now?

Its input is a kinded logical resource reference plus execution context. Its
output is one closed result:

- `browser_read` with one `ResolvedBrowserRead`;
- `remote_required` with the enforcement owner and reason;
- `denied` with a policy reason; or
- a typed operational error such as invalid input, session expiry, or
  unavailability.

It does not execute SQL, browse catalogs, or choose an alternate resource.
Unknown authorization or capability state fails closed.

### `ExecutionProvider`

The initiator creates an opaque `execution_id`, then asks the executor to admit
immutable input: that ID, the binding appropriate to its enforcement location,
the query, deadline, and budgets. The executor atomically rejects or accepts the
input and owns the authoritative record after acceptance. The first
implementation is browser WASM and a governed remote executor follows only with
a concrete host.

It does not discover resources or mint browser access. It may reject an input it
cannot enforce; it may not reinterpret that input as a different resource.

The three phases have disjoint closed results:

- resolution: `browser_read`, `remote_required`, `denied`, or
  `resolution_error`;
- admission: `accepted(execution_id)` or
  `rejected(execution_id, reason)`, with no execution event stream on rejection;
- accepted execution: authoritative `completed`, `failed`, or `cancelled`.

### Source profile

A source profile composes compatible catalog, resolver, executor, and optional
filesystem implementations. This composition is configuration, not another
catch-all provider interface.

The profile makes the enforcement owner explicit:

| Profile                       | Discovery                         | Access decision                   | Enforcement and execution                        |
| ----------------------------- | --------------------------------- | --------------------------------- | ------------------------------------------------ |
| Local Delta                   | Browser                           | User grant and browser validation | Browser WASM                                     |
| Public object storage         | Browser                           | Public-read validation            | Browser WASM                                     |
| Session-proxied Unity Catalog | E1 provider through session proxy | Server policy and grant service   | Browser validates binding; browser WASM executes |
| Governed remote               | Host catalog                      | Host policy service               | Host remote executor                             |

Browser-side Cedar may make early UI decisions for local resources and may
explain server decisions. It is not the enforcement point for a remote catalog,
remote object store, or remote executor.

## Non-negotiable invariants

### Selected resource integrity

An explicit selection is authoritative. If it is missing, stale, unqueryable, or
cannot be resolved, execution returns a typed unavailable or invalid-selection
outcome. It never substitutes the first available table, the sample table, or a
previous session table.

Sample data is an explicit source profile used by demos and tests. It is not an
error fallback.

### One binding per execution

An accepted request contains exactly one binding arm:

- browser execution receives a `ResolvedBrowserRead` that it can open directly;
- remote governed execution receives a logical resource reference and resolves
  access at the server enforcement point;
- no request carries both as independent authorities.

A resolved browser read is self-identifying. It contains the canonical tuple
defined by ADR-0010: `connection_id`, a versioned `provider_namespace`, resource
`kind`, and exactly one non-empty `provider_object_id` or `canonical_locator`
identity arm. Provider IDs are scoped to the other tuple fields. Locator-based
providers must produce capability-free identity with their versioned,
fixture-tested canonicalizer. The binding also carries the openable descriptor,
access class, conditionally required `not_after`, correlation ID, and resolution
provenance. It contains no cloud credential or refresh token.

### Capability lifetime follows the work

The browser binding is execution-local, not a reusable memory cache. The resolver
passes it directly to one admission attempt; rejection or the accepted
execution's terminal state disposes it. Every new execution resolves a new
binding.

Signed, proxy, grant-backed, and other expiring access classes require
`not_after`, set to the earliest session, grant, descriptor, or signed-object
expiry. A resolver that cannot determine a finite bound for capability-bearing
access does not return `browser_read`. Before admission and immediately before
table open, the consumer verifies the bound with a safety margin. Expired or
near-expiry access is re-resolved for the same still-pending intent; it is never
patched by reusing old URLs.

Expiry during an accepted execution produces one `failed` terminal outcome with
an access-expired code. E9
does not silently retry the query with refreshed access because the first
execution may have consumed resources or emitted result bytes.

### One execution lifecycle

The initiator creates one `execution_id` before admission. It identifies the
immutable admission input, progress, Arrow result delivery, cancellation,
metrics, audit evidence, and authoritative terminal state. Open-table setup may
have an internal span ID, but it is not a second public query identity.

Admission is idempotent by ID. An identical repeated request returns the recorded
admission state and cannot launch duplicate work. The same ID with a different
canonical resource, query, deadline, or budget is rejected as ID reuse.

After acceptance, the executor atomically records exactly one terminal outcome:

- completed;
- failed;
- cancelled.

Deadline exhaustion is a typed `failed` code, not a fourth terminal state.

The first successful transition from running to a terminal state wins completion,
cancellation, and deadline races. Later terminal signals are ignored and recorded
as invariant violations.

A live event stream exposes zero or more ordered non-terminal frames and at most
one frame for the recorded terminal outcome. No frame follows a terminal frame.
The local in-process path must observe its terminal frame. A remote disconnect can
leave the caller's observation unknown even though the executor has one state;
without a separate status or resume contract, the caller does not infer failure
or automatically replay work.

Cancel is idempotent and keyed by `execution_id`. Repeated cancellation returns
the known state. Cancellation that wins before admission records a bounded
tombstone through the request deadline, preventing a late admission from starting
work. If admission wins the race, cancellation requests termination and the first
terminal transition wins. Cancellation after a terminal outcome returns that
outcome and does not start new work.

### Deadline, budgets, and retry

The caller supplies or derives one deadline and bounded result, Arrow-byte, and
scan budgets. Resolution and execution consume the same remaining deadline.
The executor rejects unsupported or impossible limits before acceptance.

Metadata and access resolution may retry only idempotent operations before
execution acceptance, within the deadline, with a bounded policy. Admission may
be retried only with the same ID and identical immutable input. Do not retry
access denied, invalid input, cancelled work, or accepted execution work.

### Streaming and backpressure

Arrow IPC remains the data plane. Protobuf carries opaque bytes and metadata,
not rows.

Slices 1 through 3 use one byte-budgeted Arrow IPC result buffer because the
landed browser-worker protocol has no credit or acknowledgement mechanism. The
collector and encoder stop at the byte budget plus documented fixed overhead;
the executor fails with a resource-limit code and never publishes an oversized
buffer. Chunked browser delivery is deferred until an explicit credit/acknowledge
contract and bounded-queue test exist. A streaming remote extension uses its
transport's flow control and enforces an explicit maximum of buffered Arrow
bytes. Both forms preserve event ordering and idempotent cancellation; they do
not pretend a worker queue already provides backpressure.

### Errors and fail-closed behavior

Errors are structured by domain, not transport, and their phase is explicit:

| Condition                   | Resolution                               | Admission                                             | After acceptance                                                        |
| --------------------------- | ---------------------------------------- | ----------------------------------------------------- | ----------------------------------------------------------------------- |
| Invalid or mismatched input | `resolution_error(invalid)`              | `rejected(invalid)`                                   | `failed(invariant_violation)` only if an executor bug discovers it late |
| Access denied               | `denied`                                 | `rejected(access_denied)` for server-side enforcement | `failed(access_revoked)` only at a required continuation check          |
| Session or access expired   | `resolution_error(expired)`              | `rejected(expired)`                                   | `failed(expired)`                                                       |
| Unsupported in this target  | `remote_required` when an owner is known | `rejected(unsupported)`                               | `failed(unsupported)` only if capability discovery was incomplete       |
| Dependency unavailable      | `resolution_error(unavailable)`          | `rejected(unavailable)`                               | `failed(unavailable)`                                                   |
| Cancellation                | caller abort before a result             | `rejected(cancelled)`                                 | `cancelled`                                                             |
| Deadline                    | `resolution_error(deadline)`             | `rejected(deadline)`                                  | `failed(deadline)`                                                      |

HTTP, worker, WASM, object-store, and RPC details are attached as safe cause
metadata at adapters. Unknown access state never becomes a browser descriptor.
Error messages and logs redact signed URL query strings and credential-like
values.

## Cache, persistence, and audit ownership

| Data                           | Browser cache                                        | Persistence                                                                | Audit owner                                          |
| ------------------------------ | ---------------------------------------------------- | -------------------------------------------------------------------------- | ---------------------------------------------------- |
| Public catalog metadata        | Query cache                                          | Allowed within E0 policy                                                   | Browser telemetry                                    |
| Local handles and metadata     | Connection/session cache                             | Handle persistence only with explicit browser grant                        | Browser telemetry                                    |
| Resolved browser access        | One admission/execution only                         | Never                                                                      | Resolution service plus browser correlation evidence |
| Public object bytes            | Shared cache when identity and validators are stable | Allowed within size and eviction policy                                    | Browser metrics                                      |
| Governed/session-proxied bytes | Memory-only by default                               | Deferred until principal/session namespacing and logout invalidation exist | Enforcement service                                  |
| Remote execution state/results | Server-owned                                         | Server policy                                                              | Remote executor                                      |

The component that enforces access owns the authoritative audit record. Browser
telemetry is diagnostic evidence, not proof that a remote policy was enforced.
Correlation IDs join discovery, resolution, execution, and audit without
putting secrets in logs.

## Vertical slices

### Slice 1 — Correct-source execution and lifecycle

Deliver:

- remove implicit first-table and sample fallback when a user selection exists;
- represent no selection, invalid selection, and sample selection separately;
- create one domain execution ID in the run controller before admission and
  carry it through the query service and browser client by mapping it to the
  existing worker request, query, and cancellation correlation fields;
- make no protobuf or generated-contract changes in this slice;
- make admission idempotent for identical input and reject mismatched ID reuse;
- make cancel idempotent, including a cancel-before-admit tombstone;
- atomically record one terminal outcome and expose at most one terminal event;
- make resolution, admission, and accepted execution results distinct;
- carry one deadline and the existing result/byte budgets end to end;
- retain one Arrow IPC result buffer and fail before publishing it when the byte
  budget would be exceeded; do not introduce chunk delivery without credits.

Validation:

- property or state-machine tests cover duplicate identical admission,
  mismatched ID reuse, acceptance-response loss/retry, cancel-before-admit,
  acceptance/cancel/timeout/success races, repeated cancel, and worker error;
- an invalid selected table never calls `getSession`, `openDeltaTable`, or the
  worker;
- an empty catalog does not execute the sample source;
- the first terminal transition wins, no event is observed after a terminal
  frame, and the stream exposes at most one terminal frame;
- result-buffer tests prove collection and encoding stay within the configured
  Arrow byte limit plus fixed overhead and never post an oversized buffer;
- local editor smoke remains usable.

Exit gate: Axon cannot return correct-looking results from the wrong table, one
execution has one observable lifecycle, and the slice has no protobuf or
generated-contract diff.

### Mandatory E3A correction PI between Slices 1 and 2

The current `axon.exec.v1.ExecuteRequest` combines a `table_ref` and openable
descriptor, while worker messages use request and query identifiers in several
places. After Slice 1 proves the domain lifecycle, one intentional E3A PI may
correct the unadopted compatibility surface. That correction is a mandatory gate
before Slice 2.

The correction is limited to:

- the exact `CanonicalResourceRef` tuple and identity `oneof` from ADR-0010,
  including provider namespace and canonicalization fixtures;
- a oneof or distinct request type that enforces one binding;
- one resolution algebra: `browser_read`, `remote_required`, `denied`, or a typed
  operational error;
- capability lifetime on the resolved browser binding;
- caller-created `execution_id`, idempotent admission, and mismatched-ID-reuse
  rejection;
- cancel-before-admit tombstones and first-terminal-transition semantics;
- authoritative terminal state separated from at-most-once terminal-frame
  delivery;
- one byte-budgeted browser result buffer until credit-based chunking exists;
- deadline and budget fields exercised by Slice 1; and
- removal of duplicate fallback/error states needed to enforce the above.

Run `buf breaking`, document every accepted break, update generated TypeScript
and Rust output still used by consumers, and establish the corrected files as
the new Buf `FILE` baseline. Do not use the correction window for speculative
remote-service or E8 fields.

### Slice 2 — Local Delta and public-object browser providers

Deliver:

- define the minimal local `DataAccessResolver` and `ExecutionProvider`
  interfaces from the two existing paths;
- have local Delta and public object storage return a
  `ResolvedBrowserRead` rather than a generic source union;
- validate the resolved binding at the executor boundary;
- preserve existing session reuse and range-read behavior;
- classify public and local cache identities explicitly.

Validation:

- existing local Delta and public-object live tests pass through the new seams;
- descriptor validation, mandatory expiry for capability-bearing access classes,
  allowed expiry absence for non-expiring local/public access, cancellation,
  deadlines, and structured errors have focused tests;
- no second path can open the same resource from an unvalidated source union;
- generated contract adoption removes equivalent hand-written types instead of
  adding a parallel model.

Exit gate: two shipping profiles use the provider seams without changing their
observable query behavior.

### Slice 3 — One session-proxied Unity Catalog browser query

Deliver:

- consume one `TableRef` produced by E1's session-proxied Unity Catalog provider;
- ask the server enforcement point for one read decision;
- materialize an expiring browser-safe binding containing signed HTTPS or narrow
  proxy access;
- keep the binding and its URLs in memory only;
- execute one browser query and join the result to the server correlation ID;
- surface denied, session-expired, unsupported, and access-expired outcomes.

Validation:

- network and persistence tests prove no upstream catalog or cloud credential,
  signed URL, or grant is stored in browser state;
- denied or unknown policy never reaches `openDeltaTable`;
- an expired pending binding re-resolves before acceptance, every new execution
  obtains a new binding, and accepted work never auto-retries;
- logout clears metadata that requires the session and all resolved bindings;
- server audit and browser correlation evidence agree on resource and outcome.

Exit gate: one real table can be discovered, authorized, resolved, and queried
in the browser without moving the enforcement credential into the browser.

### Extension A — Governed remote execution contract

Deliver only after Slice 3 identifies a concrete host consumer:

- define a remote execution request that carries the caller-created execution ID,
  logical resource reference, SQL, deadline, and budgets;
- make identical admission retries return the recorded state and reject ID reuse
  with different immutable input;
- resolve and enforce access on the server;
- map remote progress, Arrow stream, cancel, and terminal outcomes to E9's
  observable lifecycle;
- make the server the owner of credential use, retries, caches, and audit.

The remote contract may use protobuf, but it is not generated mechanically from
the browser-worker `QueryEngine` service. Reuse domain messages only where the
semantics match.

Validation:

- contract tests run the same lifecycle cases against browser and remote
  providers;
- remote cancellation reaches the executor and is idempotent;
- transport flow-control tests bound buffered Arrow bytes;
- disconnect tests distinguish the executor's authoritative terminal state from
  an unknown caller observation and never infer failure or replay automatically;
- server authorization is checked at acceptance and any required continuation
  boundary;
- browser logs and state contain no remote credential or resolved server access.

Exit gate: local and remote providers have semantic parity for outcomes and
cancellation, without pretending they share authority or implementation.

### Extension B — E8 read-only volume consumer

Start only when E8 has a concrete read-only volume consumer. Map E1's logical
volume reference to the landed `axon/fs/v1` `FsRootRef` and use `FsEntry` paths
as logical file references. The package's existence is not runtime adoption.

Deliver:

- select a file through a `FileSystemProvider`;
- resolve browser access through the same access-lifetime rules;
- preview one bounded supported format through the appropriate execution
  provider;
- keep catalog discovery out of the filesystem seam;
- keep writes and editing out of this slice.

Validation:

- directory cancellation and pagination do not leak stale entries into the
  selected path;
- preview enforces byte, row, and deadline budgets;
- expired or denied file access fails before reading bytes;
- governed file bytes remain memory-only unless principal/session-partitioned
  persistent caching and logout invalidation have been designed and tested.

Exit gate: a volume can be selected, browsed, and previewed read-only with the
same binding, cancellation, error, cache, and audit invariants as a table.

## Explicit non-goals

- More provider types before a slice consumes them.
- A universal provider interface spanning catalog, access, execution, and files.
- Automatic browser-to-remote fallback after execution acceptance.
- Reusing the browser-worker `QueryEngine` surface as the remote API without a
  concrete semantic review.
- Persisting signed URLs, grants, access envelopes, or governed bytes by
  default.
- Modeling Arrow rows in protobuf.
- Query queues, resumable results, distributed transactions, or exactly-once
  execution claims.
- Function/model execution, writes, or volume editing.
- Rust code generation for packages with no Rust consumer.

## Verification bundle

Each slice adds focused tests at its authority boundaries. The common minimum is:

```bash
cd apps/axon-web && npm run codegen:contracts:check
cd apps/axon-web && npm test
cd apps/axon-web && npm exec -- tsc --noEmit
cd apps/axon-web && npm run lint
cargo test -p browser-sdk --locked
cargo test -p axon-contract-proto --locked
bash tests/security/verify_browser_dependency_guardrails.sh
```

Run browser-worker WASM and artifact-size checks when the worker or generated
Rust it imports changes. Run Buf lint, format, and breaking checks whenever a
proto changes. Add live cloud or host tests only where the slice requires proof
that deterministic fixtures cannot provide.

## Exit criteria

The initial E9 release is complete only after Slice 1, the mandatory E3A
correction PI, Slice 2, and Slice 3 pass in that order. Extension A begins only
when a governed-host consumer exists; Extension B begins only when E8 has a
concrete read-only volume consumer. They may land in either order. Every
completed slice or extension must enforce selected-resource integrity,
capability lifetime, explicit cache and audit ownership, and one observable
lifecycle. Browser and remote execution share those semantics without sharing
authority. No unused transport or provider abstraction is added in anticipation
of future work.
