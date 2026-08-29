# Test Plan

## 1. Unit tests

### 1.1 Protocol lifecycle

- headers before data;
- schema before data;
- trailers exactly once;
- sequence numbers strictly increasing;
- duplicate data frame rejected;
- duplicate cancel ignored;
- data after trailers rejected;
- data after cancel ignored and counted.

### 1.2 Credit/backpressure

- producer cannot send without byte credit;
- producer cannot send without batch credit;
- credit grants are additive;
- blocked time metric increments;
- global in-flight bytes never exceed query budget.

### 1.3 Codec negotiation

- common codec selected;
- unsupported codec rejects before execution;
- fallback to IPC chunk when experimental codec disabled;
- shared slab only enabled under capability and deployment checks.

### 1.4 Schema validation

- exact schema match passes;
- field type mismatch fails;
- nullability mismatch fails;
- metadata mismatch fails under exact policy;
- dictionary policy reject/unify behavior tested.

## 2. Planner tests

- eligible projection/filter accepted;
- eligible aggregate accepted;
- grouped aggregate accepted only for safe key types;
- joins rejected from worker pool;
- windows rejected;
- distinct rejected in v0;
- order/limit rejected until top-k module enabled;
- fallback reasons deterministic;
- sharding deterministic across runs.

## 3. Merge tests

### 3.1 Aggregates

- `COUNT(*)` empty input;
- `COUNT(expr)` all-null input;
- `SUM` all-null returns null;
- `MIN`/`MAX` all-null returns null;
- `AVG` count zero returns null;
- decimal cases rejected or exactly matched;
- empty shard neutral behavior;
- missing group behavior.

### 3.2 Group keys

- null group keys;
- string keys;
- integer keys;
- timestamp metadata;
- decimal precision/scale;
- dictionary keys normalized or rejected;
- float keys rejected or fully specified;
- complex keys rejected.

## 4. Browser tests

Use Playwright.

Required tests:

- start coordinator worker;
- open table with at least two files;
- run eligible projection/filter;
- assert more than one child worker executed;
- assert `BrowserWasm` execution target;
- assert `fallback_reason = None`;
- assert result parity;
- run grouped aggregate;
- assert parity with host oracle;
- run unsupported query;
- assert documented single-worker fallback or structured unsupported reason;
- cancel during scan;
- cancel during merge;
- timeout one child and assert siblings cancelled.

## 5. Wasm tests

- keep existing single-worker Wasm UAT proof;
- add worker-pool contract test only when bridge is stable;
- test Wasm merge kernel directly with Arrow batches;
- test aggregate-state codec round trips.

## 6. Property tests

For supported query shapes:

```text
same table + same SQL + different deterministic shard assignments
=> same final result set, modulo unspecified order
```

Properties:

- random file partitioning;
- random null distribution;
- random group-key distribution;
- random empty shards;
- random batch sizes;
- random cancellation timing for no-partial-results invariant.

## 7. Fuzz tests

Fuzz WCRPC frames:

- malformed envelopes;
- invalid buffer refs;
- unknown schema IDs;
- sequence gaps;
- oversized frames;
- corrupted payload lengths;
- trailers before data;
- schema after data.

Expected result: structured protocol error, no crash, no memory leak.

## 8. Performance tests

Benchmark matrix:

- cold vs warm workers;
- 1/2/4/8 workers;
- 2/4/8/16/64 files;
- uniform/skewed file sizes;
- projection/filter output sizes;
- global aggregates;
- grouped low/high cardinality;
- Arrow IPC vs experimental record-batch buffer codec.

## 9. UAT integration

Add a separate UAT row:

```text
actual browser worker-pool WCRPC query slice
```

Keep it separate from:

- host-engine UAT query corpus;
- actual wasm32 single-worker query slice;
- native oracle corpora;
- browser/native parity;
- performance smoke.

## 10. Definition of done

A feature slice is done only when:

- correctness tests pass;
- browser proof passes;
- no hidden native fallback;
- metrics prove worker-pool behavior;
- budgets fail closed;
- cancellation is deterministic;
- performance report explains overhead and benefit.
