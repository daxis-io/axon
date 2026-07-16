# Axon Workbench And Query Engine Architecture

- Status: Working architecture
- Date: 2026-06-20
- Revised: 2026-07-15
- Scope: Axon's query-engine, runtime, and workbench architecture
- Related:
  - [Provider model](./provider-model.md)
  - [Browser lakehouse engine strategy](./browser-lakehouse-engine-strategy.md)
  - [Browser Delta compatibility matrix](./browser-delta-compatibility-matrix.md)
  - [ADR-0009: Axon Is The Lakehouse Query Engine And Workbench Runtime](../adr/ADR-0009-axon-is-the-lakehouse-workbench.md)

## North Star

Axon is an embeddable lakehouse workbench and query engine. It discovers a selected resource, obtains one authority-preserving resource binding, runs supported reads where the deployment permits, and returns Arrow data or a deterministic outcome. The browser runtime is useful on its own for local Delta and public object storage. Authenticated catalog access and remote execution are deployment profiles built by composing the same narrow seams.

Axon owns browser-safe descriptor validation, byte-range reads, Delta/Parquet planning, DataFusion execution, Arrow IPC result transport, budgets, cancellation, metrics, and structured terminal outcomes. The enforcement point serving a protected catalog, object, or compute resource owns authorization and audit for that resource. Host products may also own identity, tenants, rollout, billing, and server fallback policy.

## Current Implementation And Proposed Architecture

Current implementation:

- Local Delta and public object-storage sources execute through the browser worker.
- Generated `axon/common/v1`, `axon/catalog/v1`, `axon/dataaccess/v1`,
  `axon/exec/v1`, and `axon/fs/v1` messages are landed. The filesystem package
  is contract substrate only; no E8 provider, adapter, UI, or runtime consumer
  has landed.
- `axon.exec.v1` currently describes compatibility IPC between the browser client and worker. It does not define a remote service.
- Query-source selection and the app query service still fuse source lookup, read construction, worker session management, and execution.
- Unity Catalog discovery, protected browser reads, remote execution, and volume filesystem browsing are not end-to-end implementations.

Proposed architecture:

- A `SourceProfile` composes identity/session, discovery, authorization decision, read resolution, execution, and optional filesystem seams.
- `CatalogProvider` is discovery only. It returns logical references and metadata.
- `DataAccessResolver` is the only browser-side seam that turns a logical reference into a short-lived access envelope.
- The run controller creates one `execution_id` before admission;
  `ExecutionProvider` atomically owns its state after acceptance.
- `FileSystemProvider` lists and stats file-like resources and returns logical
  references. Access resolution and preview stay in the resolver and executor
  seams.

## Runtime Flow

```text
user selects catalog object or file
        |
        v
CatalogProvider or FileSystemProvider
  returns logical reference + metadata
        |
        v
AuthorizationDecisionProvider
  early allow / deny / unknown decision
  (remote enforcement still belongs to the resource owner)
        |
        +---------------- remote/native execution ----------------+
        |                                                          |
        v                                                          v
DataAccessResolver                                      ExecutionProvider
  logical ref -> resolved browser envelope              logical ref -> server-side resolution
        |                                                          |
        v                                                          |
BrowserWasm ExecutionProvider                                      |
        +-------------------------+--------------------------------+
                                  v
       bounded Arrow IPC result or flow-controlled stream + progress + metrics
```

Every resource uses the canonical identity tuple from ADR-0010: connection ID,
versioned provider namespace, kind, and exactly one provider-object-ID or
canonical-locator arm. Equality is exact after provider canonicalization;
display names and access capabilities are not identity.

Browser resolution produces one execution-local envelope containing that
identity, an openable descriptor, access class, conditional expiry, and
correlation/provenance identifiers. Capability-bearing access requires an expiry
bounded by the earliest session, grant, descriptor, or object-URL limit. The
envelope is discarded after one rejection or terminal execution. Remote/native
execution receives only a logical reference and resolves it inside its own trust
boundary. No execution request carries both forms.

The runtime keeps three result algebras separate. Resolution returns
`browser_read`, `remote_required`, `denied`, or `resolution_error`. Admission
returns `accepted(execution_id)` or `rejected(execution_id, reason)` and creates
no stream when rejected. An accepted executor atomically records exactly one
`completed`, `failed`, or `cancelled` state; deadline expiry is a structured
failure. A live stream carries at most one corresponding terminal frame, so a
remote disconnect may leave caller observation unknown without changing the
executor's state.

The run controller creates `execution_id` before admission. Identical admission
retries return the recorded state; the same ID with different immutable input is
invalid. Cancellation is idempotent, targets that ID, and prevents late admission
when it wins the race. The first terminal transition wins later completion,
cancellation, or deadline signals. Discovery or resolution may retry before
acceptance within capability lifetime; accepted work is not automatically
replayed.

## Runtime Boundaries

```text
Workbench surfaces and SDK
        |
        v
SourceProfile
  identity | discovery | decision | access | execution | optional filesystem
        |
        +--> BrowserWasm: resolved browser envelope
        |
        +--> Native/remote: logical reference
        |
        v
bounded Arrow IPC result or flow-controlled stream, progress, metrics, outcome
```

The browser target owns browser-safe range reads, descriptor materialization,
DataFusion-backed SQL execution, and Arrow IPC delivery. The initial worker path
returns one byte-budgeted Arrow IPC buffer; chunking waits for explicit credits.
Known unsupported work routes before admission. A failure discovered after
acceptance terminates that execution; any native/server attempt is a new,
correlated admission. The native target remains the correctness oracle and
mandatory route for query shapes, table features, or runtime budgets outside the
browser contract.

Protobuf is the stable control-contract representation where multiple language or process boundaries need it. Arrow IPC remains the result data plane. TypeScript interfaces are appropriate for in-process discovery and composition. Transport-specific worker commands remain explicitly worker-scoped; a remote host contract will be designed from a real remote consumer rather than inferred from browser IPC.

## Cache, Persistence, And Audit

Caching follows the authority and lifetime of the data:

- Public immutable object ranges may be persisted under stable object identity and validation metadata.
- Catalog metadata may be cached within the principal/session namespace that authorized discovery.
- Signed URLs, grants, resolved access envelopes, and governed descriptors are
  execution-local and expire no later than their earliest capability; a new
  execution resolves a new envelope.
- Protected object bytes are memory-only by default. Persistent storage requires a principal/session namespace, revocation model, logout invalidation, and an explicit decision by the enforcement owner.
- Remote execution caches and durable operation records belong to the remote executor.

Axon owns local diagnostics for browser execution. The service that enforces remote metadata, byte, or compute access owns the authoritative audit record. Correlation identifiers cross the seams so browser diagnostics can be joined with that record without duplicating authority.

## Host Integration Boundary

Integrations can provide session access, catalog adapters, approved read-plan sources, governed descriptors, or remote execution. They do not become one broad provider. Consumer-specific documentation stays under `docs/integrations/` because it describes host-owned gateway, policy, rollout, and production-proof responsibilities around Axon.

## Compatibility Rules

- Browser execution fails closed when identity, authority, descriptor safety, capability expiry, table support, SQL shape, or runtime budgets are unknown.
- Browser Cedar can make early product decisions but cannot replace enforcement at a remote resource boundary.
- Catalog discovery never returns credentials, grants, signed URLs, or executable descriptors.
- An executor receives one resource-binding form and owns cancellation and its single terminal outcome.
- Native execution remains the correctness reference for unsupported browser behavior.
- Authority-crossing JSON and protobuf adapters validate required fields, enum values, oneof exclusivity, expiry, and URL policy before constructing domain values.
- Core success claims remain tied to repo-owned runtime evidence; external integrations own their deployment proof.
