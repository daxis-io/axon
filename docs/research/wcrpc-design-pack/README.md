# WCRPC: Wasm Columnar RPC Design Pack

**Status:** Draft for architecture review  
**Date:** 2026-07-06  
**Owner:** Runtime / Engine Team  
**Scope:** Browser-native, Wasm-first columnar exchange protocol for multi-worker analytical execution.

This documentation pack expands the existing Browser DataFusion Worker Pool design into a broader execution vision built around **WCRPC: Wasm Columnar RPC**.

WCRPC is not a public SDK protocol and not a replacement for DataFusion. It is an internal browser-local exchange protocol that connects coordinator workers, child Wasm DataFusion workers, merge workers, and future exchange workers using stream semantics, Arrow/DataFusion-native payloads, budgets, cancellation, backpressure, and observability.

## Document map

| File | Purpose |
|---|---|
| `docs/00-executive-brief.md` | Product and architecture summary. |
| `docs/01-research-brief-quack-ballista-arrow.md` | Prior art and lessons from DuckDB Quack, DataFusion Ballista, Arrow IPC, and Dissociated IPC. |
| `docs/02-architecture-spec.md` | End-to-end architecture and component boundaries. |
| `docs/03-wcrpc-protocol-spec.md` | WCRPC stream, frame, status, and lifecycle specification. |
| `docs/04-planning-execution-model.md` | Distributed planning model, query classification, stages, partitions, and exchange modes. |
| `docs/05-payload-codecs-and-memory.md` | Payload codec strategy: Arrow IPC, record-batch buffers, aggregate-state batches, and shared slabs. |
| `docs/06-budgets-backpressure-cancellation.md` | Resource accounting, flow control, cancellation, and failure semantics. |
| `docs/07-merge-semantics.md` | Correctness rules for projection/filter, aggregates, grouped aggregates, order/limit, and future distinct. |
| `docs/08-observability-benchmarks.md` | Metrics, tracing, benchmark matrix, and performance gates. |
| `docs/09-security-compatibility-deployment.md` | Browser security model, deployment constraints, compatibility, and fallback policy. |
| `docs/10-rollout-plan.md` | Milestones, acceptance gates, and staffing sequence. |
| `docs/11-test-plan.md` | Unit, Wasm, browser, UAT, fuzz/property, and performance tests. |
| `docs/12-risk-register.md` | Risks, mitigations, and owners. |
| `adrs/*.md` | Architecture Decision Records. |
| `reference/typescript/wcrpc-types.ts` | TypeScript reference types for frames and planner contracts. |
| `reference/proto/wcrpc.proto` | Optional protobuf-style control-plane schema sketch. |
| `implementation/epics-and-milestones.md` | Implementation epics and phase-by-phase plan. |
| `implementation/checklist.md` | Build checklist and definition of done. |
| `implementation/decision-log-template.md` | Template for future decisions. |
| `WCRPC-full-design-spec.md` | Consolidated single-file version of the core design. |

## Design thesis

> WCRPC is **Flight-like in stream semantics**, **Quack-inspired in payload strategy**, **Ballista-inspired in exchange model**, **Arrow/DataFusion-native in execution format**, and **browser-local in transport**.

The first implementation should remain simple:

```text
Coordinator Worker
  -> WCRPC ExecuteShard streams over MessagePort/postMessage
  -> Child Wasm DataFusion workers
  -> transferable Arrow IPC / RecordBatch buffers
  -> coordinator or merge worker
  -> final existing BrowserWorkerResponseEnvelope
```

The future implementation can add:

```text
shared-memory slabs
stage DAGs
hash/range exchange
direct worker-to-worker channels
Wasm merge kernels
top-k and distinct support
```

## Primary external references

- DuckDB Quack announcement, May 12, 2026: https://duckdb.org/2026/05/12/quack-remote-protocol
- DuckDB Quack docs: https://duckdb.org/docs/current/quack/overview
- MotherDuck Quack explanation: https://motherduck.com/blog/duckdb-client-server/
- DataFusion Ballista architecture: https://datafusion.apache.org/ballista/contributors-guide/architecture.html
- DataFusion Ballista tuning/shuffle: https://datafusion.apache.org/ballista/user-guide/tuning-guide.html
- Arrow Dissociated IPC: https://arrow.apache.org/docs/format/DissociatedIPC.html
- Arrow IPC API/docs: https://arrow.apache.org/docs/cpp/api/ipc.html
- Arrow FAQ: https://arrow.apache.org/faq/
