# Merge Semantics Specification

## 1. Core principle

WCRPC moves data, but the merge plan defines SQL semantics. Every distributed query must have either:

```text
append-only semantics
```

or an explicit:

```text
MergePlan
```

If no merge plan can prove equivalence with single-worker DataFusion semantics, worker-pool execution is rejected.

## 2. Projection/filter append

Supported when query has no global ordering, aggregation, distinct, or grouping.

Rules:

1. All child schemas must match final schema exactly.
2. Output order is unspecified unless SQL requests an order.
3. Final output is a logical Arrow IPC stream of all child batches.
4. Do not byte-concatenate full IPC streams blindly. Decode/validate stream schema and emit a single valid final stream.
5. Dictionary arrays require explicit normalize/unify/reject policy.

## 3. Global aggregates

Supported v0 aggregate functions:

- `COUNT(*)`
- `COUNT(expr)`
- `SUM(expr)`
- `MIN(expr)`
- `MAX(expr)`
- `AVG(expr)` as sum + count

### 3.1 Truth table

| Function | Partial state | Merge | Empty/all-null behavior |
|---|---|---|---|
| `COUNT(*)` | row count | sum counts | zero on empty input |
| `COUNT(expr)` | non-null count | sum counts | zero on empty/all-null input |
| `SUM(expr)` | sum + non-null count | sum partial sums | null if total non-null count is zero |
| `MIN(expr)` | min + non-null count | min non-null partials | null if total non-null count is zero |
| `MAX(expr)` | max + non-null count | max non-null partials | null if total non-null count is zero |
| `AVG(expr)` | sum + non-null count | sum sums / sum counts | null if total non-null count is zero |

### 3.2 Unsupported v0 aggregate variants

Reject from worker pool unless separately designed:

- `COUNT(DISTINCT ...)`;
- approximate aggregates;
- non-deterministic aggregates;
- user-defined aggregates;
- decimal overflow behavior not proven equivalent;
- aggregate filter clauses unless planner rewrites them safely;
- aggregates over unsupported complex types.

## 4. Grouped aggregates

Each worker returns one row per group key with partial aggregate state.

Rules:

1. Missing group on a shard means zero rows for that group, not a group with null values.
2. Null group keys follow SQL grouping semantics: nulls group together.
3. Group keys must use canonical key encoding.
4. Aggregate-state schema must match merge plan exactly.
5. Intermediate group count budget failure fails the query, never truncates.

## 5. Canonical group-key encoding

v0 supported key types should be limited to tested primitive types.

Required behavior:

| Type | Requirement |
|---|---|
| Boolean | canonical byte representation |
| Signed/unsigned integer | width-aware representation |
| UTF-8 string | dictionary/plain encodings compare equal after normalization or dictionary rejected |
| Null | encoded explicitly and groups together |
| Date/time/timestamp | unit/timezone metadata included |
| Decimal | precision/scale included |
| Float | define/reject `NaN`, `-0.0`, `0.0` semantics |
| Binary | byte-exact, not UTF-8 converted |
| List/struct/map | reject in v0 unless explicitly supported |

Preferred v0 policy:

```text
Support booleans, integers, UTF-8 strings, dates, timestamps with exact metadata, and nulls.
Reject floating group keys, complex keys, and dictionary-encoded group keys unless normalized.
```

## 6. Final schema preservation

The final output schema is produced by the merge plan, not inferred opportunistically from child output.

```ts
type FinalCastSpec = {
  inputColumn: string;
  outputColumn: string;
  outputType: ArrowDataTypeDescriptor;
  nullable: boolean;
};
```

If final casts cannot reproduce DataFusion output exactly, reject worker-pool execution.

## 7. Order and limit

Deferred until top-k module is implemented.

Required future plan:

1. Each child runs local `ORDER BY` with `LIMIT global_limit + offset`.
2. Children emit sorted streams.
3. Merge worker performs k-way merge.
4. Apply offset and limit globally.
5. Preserve null sort semantics and collation semantics.
6. Validate parity against native oracle across skewed shards.

## 8. Distinct

Deferred until output cardinality budgets are enforced.

Potential plan:

```text
worker local distinct
  -> hash/range exchange by distinct columns
  -> reducer global distinct
  -> final gather
```

Do not implement as naive gather-and-dedup for large outputs without intermediate budgets.

## 9. Error rules

Fail closed on:

- schema mismatch;
- unsupported aggregate;
- unsupported group key type;
- ambiguous decimal/float semantics;
- aggregate intermediate budget exceeded;
- dictionary policy violation;
- final schema mismatch;
- merge kernel error.

## 10. Parity requirement

Every supported merge shape must have parity tests against:

- single-worker browser path where possible;
- native DataFusion oracle;
- host UAT corpus;
- randomized file partitioning property tests.
