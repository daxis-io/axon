# Build Checklist

## Before coding

- [ ] ADRs reviewed.
- [ ] v0 supported SQL shapes approved.
- [ ] fallback policy approved.
- [ ] metrics extension approved.
- [ ] benchmark matrix approved.
- [ ] feature flags named.

## Protocol core

- [ ] Frame envelope.
- [ ] Stream state machine.
- [ ] Sequence validation.
- [ ] Headers frame.
- [ ] Schema frame.
- [ ] Data frame.
- [ ] Aggregate-state frame.
- [ ] Credit frame.
- [ ] Cancel frame.
- [ ] Error frame.
- [ ] Trailers frame.
- [ ] Loopback tests.
- [ ] MessagePort transport.

## Worker integration

- [ ] Coordinator endpoint.
- [ ] Child endpoint.
- [ ] Worker lifecycle.
- [ ] Table open through shard descriptors.
- [ ] ExecuteShard over WCRPC.
- [ ] Cancellation fanout.
- [ ] Late frame rejection.

## Planning

- [ ] WorkerPoolDecision.
- [ ] Query eligibility.
- [ ] Candidate file pruning source.
- [ ] Deterministic sharding.
- [ ] MergePlan.
- [ ] Structured fallback reasons.

## Payloads

- [ ] Arrow IPC chunk codec.
- [ ] Transferable buffer handling.
- [ ] Schema fingerprinting.
- [ ] Dictionary policy.
- [ ] Record-batch buffer codec experiment.

## Merge

- [ ] Projection/filter append.
- [ ] Global aggregate merge.
- [ ] Grouped aggregate merge.
- [ ] Canonical group-key encoder.
- [ ] Final schema validation.
- [ ] Wasm merge API.

## Budgets

- [ ] Query budgets.
- [ ] Stream budgets.
- [ ] Intermediate-state budgets.
- [ ] Final output budgets.
- [ ] Credit allocator.
- [ ] Deadline propagation.

## Tests

- [ ] Unit protocol tests.
- [ ] Planner tests.
- [ ] Merge truth-table tests.
- [ ] Browser Playwright proof.
- [ ] UAT row.
- [ ] Fuzz malformed frames.
- [ ] Performance benchmarks.

## Done

- [ ] `worker_count >= 2` proof.
- [ ] `BrowserWasm` proof.
- [ ] `fallback_reason = None` proof.
- [ ] Single-worker/native parity.
- [ ] No partial-result path.
- [ ] Metrics explain overhead and speedup.
