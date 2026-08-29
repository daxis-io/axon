# Risk Register

| Risk | Impact | Likelihood | Mitigation | Owner |
|---|---:|---:|---|---|
| Distributed SQL merge correctness bug | High | Medium | Limit v0 shapes, require MergePlan, parity/property tests | Engine |
| Worker startup overhead erases speedup | Medium | High | Opt-in, warm pool, cost heuristic, benchmark gates | Runtime |
| Wasm module duplication causes high memory use | High | Medium | Bound worker count, measure memory, consider shared module strategies later | Runtime |
| Browser memory spike from parallel outputs | High | High | Credit-based backpressure and global in-flight budget | Runtime |
| Schema mismatch discovered late | Medium | Medium | Schema-first streams and fingerprints | Protocol |
| Dictionary arrays merge incorrectly | High | Medium | Reject or normalize explicitly in v0 | Engine |
| Aggregate state truncation corrupts results | High | Medium | Separate final output and intermediate-state budgets; fail closed | Engine |
| Hidden native fallback masks browser failure | High | Medium | Structured fallback reasons, metrics, test assertions | Runtime |
| Transport protocol leaks into public SDK | Medium | Low | Keep WCRPC internal; expose only metrics extension | SDK |
| SharedArrayBuffer deployment unavailable | Medium | High | Portable transferable mode v0; shared slab only optional | Platform |
| Record-batch buffer codec diverges from Arrow semantics | High | Medium | Keep IPC as baseline; feature flag experiments; extensive schema tests | Protocol |
| Coordinator becomes bottleneck | Medium | Medium | Direct worker-to-worker exchange and merge worker in later phases | Runtime |
| Top-k ordering semantics differ from DataFusion | High | Medium | Defer until focused top-k module and oracle tests | Engine |
| Group-key canonicalization bug | High | Medium | Restrict key types; canonical encoder tests | Engine |
| Cancellation race returns partial data | High | Medium | Query state machine, late-frame rejection, no partial mode | Runtime |
| Browser differences across Chrome/Firefox/Safari | Medium | Medium | Playwright matrix, feature detection, conservative defaults | Platform |
| Quack-inspired native codec overfits internals | Medium | Medium | Version codecs, keep Arrow IPC fallback | Protocol |
| Ballista-inspired stage model adds too much v0 complexity | Medium | Low | Implement only gather v0; reserve fields for future | Architecture |
