# E3A — Provider Contract Surfaces — Revised Execution Plan

- Original date: 2026-06-20
- Revised: 2026-07-23
- Status: Pre-adoption correction complete locally; consumer adoption remains
- Audited baseline: `origin/main` at
  `7681f1dfa5bdaaae3ff2ccff79cc8be76ec1503a`
- Current local integration base: `origin/main` at
  `6cca364465fc4fa714ff7403b6df7e3f229c6e8f`
- Related: [workbench strategy](../program/rich-lakehouse-workbench-strategy.md),
  [provider ADR](../adr/ADR-0010-pluggable-catalog-providers.md), and
  [E9 vertical slices](./2026-07-15-e9-execution-provider-vertical-slice-plan.md)

## Purpose

E3A gives Axon one generated vocabulary for stable values that cross package,
language, or worker boundaries. It does not define every provider interface and
it does not create a transport merely because protobuf can describe one.

The four data-path interfaces in E3A's scope remain local interfaces. Identity
sessions and early authorization decisions are separate source-profile seams but
do not need new E3A messages for the first slices:

- `CatalogProvider` discovers catalog objects.
- `DataAccessResolver` resolves whether and how a selected resource can be read.
- `ExecutionProvider` executes or previews a query.
- `FileSystemProvider` lists and stats workspace files and returns logical
  references when E8 proves the read-only consumer. Access resolution and
  preview remain in their existing seams.

Generated messages carry values between those seams. The interfaces own
lifecycle, authority, caching, and cancellation.

## Current implementation

The following is landed on the audited baseline:

| Package              | Landed surface                                                                                 | Current consumers                                                       |
| -------------------- | ---------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| `axon/common/v1`     | Resource references, pagination, capabilities, and provider errors                             | Generated TypeScript and Buffa Rust contract tests                      |
| `axon/catalog/v1`    | Normalized discovery messages                                                                  | Generated TypeScript contract tests; provider adoption remains          |
| `axon/dataaccess/v1` | Read outcomes, browser-safe descriptors, grants, and capability reports                        | Generated TypeScript and Buffa Rust contract tests                      |
| `axon/exec/v1`       | Query values, browser-worker commands/events/responses, and a `QueryEngine` service descriptor | Generated TypeScript; Buffa Rust message proof; no service transport    |
| `axon/fs/v1`         | Filesystem roots, entries, list/stat requests, and read-resolution values                      | Generated TypeScript and Buffa Rust contract tests; E8 adoption remains |

The module uses Buf `FILE` compatibility. TypeScript generation is checked in
and drift-gated. Buffa-generated Rust exists for common, data-access, exec, and
filesystem messages and has passed host and `wasm32-unknown-unknown` proof.

This current state is contract substrate. Catalog, data-access, execution, and
filesystem messages are not complete provider implementations, and the
`QueryEngine` declaration is not a deployed remote execution API. No E8
provider, adapter, UI, or runtime consumer has landed.

On the historical audited baseline,
`axon.common.v1.ProviderCapabilities` included a generic `authority` field. It
was unadopted and too broad: discovery capability does not establish who
decides access or who enforces it. The 2026-07-23 correction removes that
message and field before E1 or E9 adoption.

## Contract classification

### Domain messages after the correction

Keep protobuf for values whose corrected meaning should remain stable across
callers:

- resource identity and pagination;
- normalized catalog nodes and table metadata;
- fail-closed read-resolution outcomes;
- directly openable browser descriptors;
- structured error codes and capability reports;
- opaque Arrow IPC metadata and bytes.

These messages must not contain browser-held cloud credentials, refresh tokens,
signing material, or provider-specific SDK objects.

The historical `ObjectRef`, resolution variants, and execution lifecycle were
not yet in that category. E9 Slice 1 first established the domain identity and
lifecycle without protobuf changes. The 2026-07-23 intentional E3A correction
then aligned their wire identity, result algebra, access lifetime, and
cancellation semantics before E9 Slice 2 adoption.

### Browser-worker compatibility protocol

Treat the landed `axon.exec.v1` command, event, response, and `QueryEngine`
surface as the compatibility protocol between Axon's TypeScript runtime and its
browser worker. The service descriptor records method cardinality for that
boundary: `Execute` streams; `Preview` and `Cancel` are unary.

It is not a portable remote service contract. Its requests carry directly
openable browser descriptors and its events expose browser-worker concepts. A
governed remote executor has different authority, credential, retry, audit, and
caching rules and must not inherit this surface by default.

### Local interfaces, not protobuf services

Catalog discovery, data-access resolution, and filesystem browsing remain local
TypeScript or Rust interfaces until bytes demonstrably cross a process boundary.
Do not add services for uniformity. If a remote boundary appears, define the
smallest transport contract from that consumer's requirements.

## Binding rule

An execution request has exactly one resource binding:

- browser execution receives a resolved, directly openable binding;
- remote governed execution receives a logical resource reference and resolves
  access at the enforcement point;
- a request never carries both as independent sources of truth.

The browser binding is self-identifying and carries ADR-0010's exact canonical
tuple: connection ID, versioned provider namespace, resource kind, and exactly
one provider-object-ID or canonical-locator identity arm. Locator identity is
capability-free and comes from that namespace's versioned, fixture-tested
canonicalizer. The binding also carries its descriptor, access class,
correlation ID, resolution provenance, and `not_after` whenever the access class
is capability-bearing. It is passed directly to one admission attempt, discarded
after rejection or terminal execution, and never becomes a catalog-cache entry.

The pre-correction `ExecuteRequest` contained both `table_ref` and
`descriptor`. The completed pre-adoption correction replaces that ambiguous
pairing with exactly one `browser_read` or `logical_resource` binding arm
before production consumers depend on it.

Authority is contextual rather than a catalog capability. The source profile
identifies the access-decision owner and enforcement owner. If those values do
not cross a real boundary, keep them out of protobuf. Remove the unused generic
provider-authority field during the correction window instead of adding more
authority enums.

## Evolution policy

Buf `FILE` compatibility is the default now. Keep it enabled.

One intentional pre-adoption correction is allowed for the unconsumed E3A
surfaces. That correction must:

1. be tied to the first E9 consumer;
2. remove a demonstrated ambiguity rather than add speculative flexibility;
3. update proto, generated output, adapters, fixtures, and docs together;
4. run `buf breaking` against the old baseline and record each accepted break;
5. establish the replacement as the new compatibility baseline immediately.

After that correction, changes are additive under Buf `FILE` rules. Do not keep
a generally relaxed "living draft" period.

ProtoJSON is an edge representation, not a storage schema. Tests may normalize
legacy JSON at adapters, but durable data must not depend on enum spellings,
unknown-field preservation, default-value emission, or 64-bit JSON number
coercions.

## Rust generation policy

Do not generate every package into Rust by symmetry. Generate Rust only when a
real Rust consumer imports the messages or when a bounded compatibility proof
is required for an imminent consumer.

The existing common, data-access, exec, and filesystem Buffa output proves that
the selected toolchain works on host and WASM. It is not a commitment to
generate `axon/catalog/v1`, a decision package, or a remote service stub into
Rust before those consumers exist.

## Remaining work

### M0 — E9 Slice 1 establishes the domain lifecycle (complete)

Before changing protobuf, E9 Slice 1 removes implicit first-table and sample
fallback, makes the selected resource authoritative, and establishes one
caller-created domain `execution_id`, idempotent admission and cancellation,
and one terminal state. Compatibility adapters map that ID to the existing
worker request, query, and cancellation correlation fields.

Gate: selected-source and lifecycle tests pass without proto, generated-output,
or compatibility-baseline changes.

### M1 — Intentional pre-adoption contract correction (complete locally)

One intentional E3A correction PI follows E9 Slice 1 and aligns the execution
binding and lifecycle wire shape:

- one `CanonicalResourceRef` containing connection ID, versioned provider
  namespace, resource kind, and exactly one non-empty provider-object-ID or
  canonical-locator arm, with locator canonicalization fixtures;
- one caller-created `execution_id` before admission, with identical retries
  idempotent and mismatched ID reuse rejected;
- exactly one binding arm;
- disjoint closed algebras for resolution, admission, and accepted terminal
  state;
- conditionally mandatory `not_after` for capability-bearing browser access and
  execution-local binding disposal;
- removal of generic provider authority from discovery capabilities;
- idempotent cancellation, including cancel-before-admit tombstones;
- first-terminal-transition semantics and authoritative state separated from
  at-most-one terminal stream frame;
- one byte-budgeted browser Arrow buffer until a credit/acknowledgement protocol
  proves bounded chunk delivery;
- explicit deadline and budget fields only where the first consumer uses them.

The same correction removes contradictory or unused substrate before E9 Slice 2
and E1 adoption:

- `PageInfo` uses an optional next cursor as its sole continuation signal;
  remove `has_more`;
- remove the unused generic `ProviderCapabilities` and `ProviderAuthority`
  messages; method and result shapes express discovery support;
- defer function and model nodes and list responses; retain the minimal volume
  reference because E8 has a named consumer;
- keep ordered `partition_columns` as the sole partition-membership source;
- replace string-keyed runtime capability maps with one typed capability key and
  repeated key/state entries that reject duplicates; and
- map any legacy request/query audit identifiers to the domain
  `execution_id` at the compatibility adapter.

Gate: Buf lint and formatting pass; the intentional break report is reviewed;
generated output and normalized boundary tests pass; identity mismatch,
canonicalization, duplicate admission, mismatched ID reuse, cancel/admit race,
terminal race, conditional-expiry, and result-byte-budget tests pass; no transport
is added.

### M2 — Local and public browser adoption

Adopt the generated data-access and exec values in the existing local Delta and
public object-storage query path. Remove hand-written mirrors only when the
consumer has equivalent tests.

Gate: the selected resource can never fall through to a sample or different
table; local Delta and public-object-storage browser tests pass; idempotent
admission, cancellation, bounded-buffer, and terminal-state/frame tests pass.

### M3 — One session-proxied Unity Catalog browser query

Use `axon/catalog/v1` for discovery and resolve one selected table into an
expiring, execution-local browser binding. This is the first proof that the three
seams compose without moving catalog or cloud credentials into the browser.

Gate: expiry, denied, and session-loss paths fail closed; the binding is absent
from persistent stores, logs, and query caches; correlation IDs join discovery,
resolution, and execution evidence.

### M4 — Adopt landed filesystem messages only with the E8 consumer

`axon/fs/v1` is landed messages-only substrate with generated TypeScript and
Buffa Rust proof. E8 must still supply one read-only volume-browsing consumer
before Axon adds a filesystem provider, adapter, UI, or runtime path. The E1
volume reference maps to an `FsRootRef`; listing and stat return logical
`FsEntry` values; selected-file access and preview continue through the
data-access and execution seams.

Gate: no E8 runtime-adoption claim exists until a real read-only volume consumer
passes its authority, pagination, path-normalization, cancellation, preview
limit, persistence, and audit tests.

## Removed or deferred work

- No generic Connect, gRPC, or HTTP execution service in E3A.
- No remote executor generated from the browser-worker `QueryEngine` surface.
- No service-stub generation until a concrete transport consumer exists.
- No additional Rust package generation without a Rust consumer or bounded
  compatibility proof.
- No function or model discovery messages before a concrete consumer.
- No duplicate JSON Schema or OpenAPI representation for Axon-owned messages.
- No protobuf representation of Arrow rows or record batches.
- No E8 provider, adapter, UI, or runtime adoption before its read-only volume
  consumer proves the boundary.

## Validation

Run the checks appropriate to any contract change:

```bash
cd apps/axon-web && buf lint
cd apps/axon-web && buf format --diff --exit-code
cd apps/axon-web && npm run codegen:contracts:check
cd apps/axon-web && npm test -- src/generated/contracts
cd apps/axon-web && npm exec -- tsc --noEmit
cargo test -p axon-contract-proto --locked
cargo check -p axon-contract-proto --target wasm32-unknown-unknown --locked
```

Authority-crossing tests reject missing required messages, unspecified or
unknown enum values, unset/conflicting variants, empty canonical identities,
mismatched resource and descriptor identity, invalid half-open ranges,
non-positive limits, expired capabilities with safety skew, and events after a
terminal outcome. TypeScript and Rust adapters use the same negative matrix.

Add `buf breaking --against ...` whenever a proto changes. Run Rust checks only
for packages that still have a generated Rust consumer. The acceptance gate is
consumer behavior, not the number of generated packages.

## Exit criteria

E3A is complete when E9 Slice 1 has established the domain lifecycle, the
intentional correction window is closed, E9 Slice 2 consumes the corrected
values, Buf `FILE` compatibility is enforced from the corrected baseline, and no
additional unused package or transport remains on the roadmap merely to make
the contract tree look complete. Landed `axon/fs/v1` remains messages-only until
the separately gated E8 consumer adopts it.

## 2026-07-23 correction handoff

The completed local stack is based directly on
`6cca364465fc4fa714ff7403b6df7e3f229c6e8f`. Its rewritten E9 commits are
`59df620`, `d2fc39e`, `8424e6d`, `4519d42`, `b78deb5`, and `0572b32`; the
correction commits before this documentation handoff are `1a57235`, `fe15caf`,
`0569dce`, and `51873a3`.

M1 now supplies the corrected value substrate:

- `CanonicalResourceRef` is the sole cross-seam resource identity;
- `ReadResolution` has exactly four disjoint outcomes and
  `ResolvedBrowserRead` is self-identifying;
- capability reports use typed unique entries, and capability-bearing browser
  access expires at the earliest finite capability boundary;
- `ExecuteRequest` has one binding, one caller-created `execution_id`, one
  absolute deadline, and explicit runtime budgets;
- admission is accepted or rejected, accepted execution has seven lifecycle
  states and exactly three authoritative terminal outcomes, and cancellation is
  keyed only by `execution_id`; and
- the public contract carries one bounded Arrow IPC buffer. The landed private
  child/coordinator chunk protocol remains an internal TypeScript/Rust detail.

The reviewed `buf breaking` report contains exactly 95 pre-adoption findings:
11 catalog, 5 common, 18 data-access, and 61 execution. There is no filesystem,
unrelated package, service-cardinality, or unreviewed field-type break. This is
the one authorized compatibility window. Future protobuf changes must run Buf
`FILE` compatibility against the corrected commit, not against the historical
pre-correction schema, and must be additive unless separately reviewed.

M2/E9 Slice 2 planning is now unblocked, but M2 implementation has not started.
The correction adds no provider interface, remote executor, Connect/Tauri/Cedar
transport, E8 consumer, or application-layer generated-message adoption. The
branch remains local-only until separately reviewed.

The fresh independent review added semantic-negative TypeScript and Buffa
fixtures for incomplete resource tuples, nested browser-read bindings,
capability enums/duplicates, response oneofs, and rejected-admission ordering.
It also hardened the existing internal E9 worker lifecycle with bounded
nonresponsive-child cleanup and editor-session invalidation. These are contract
proof and existing-runtime corrections only; they do not begin M2 provider
adoption.
