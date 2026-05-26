# Explain Plan Plumbing — Browser → Worker → Contract → UI

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Thread the existing `QueryRequest.options.include_explain` flag through to a populated explain string in `QueryResponse`, surface it in the browser SDK, and render it in the editor's Plan tab so users can see the executed query plan.

**Architecture:** Add `explain: Option<String>` to `QueryResponse` in `query-contract`. In `wasm-query-session::sql()`, when the flag is set, render the already-built `BrowserExecutionPlan` to a string via its existing `Debug` derive and attach it. Mirror the field in the TS SDK type, lift it through the editor's `services/query.ts` into the existing `PlanSummary.tree` slot that the Plan tab already renders.

**Tech Stack:** Rust (query-contract, wasm-query-session, wasm-query-runtime), TypeScript (axon-browser-sdk, services/query, App), Playwright (smoke).

**Why a string and not a struct:** `BrowserExecutionPlan` is Axon's own structured plan (not DataFusion's). It already derives `Debug`. A single readable string is the simplest wire format and lets the UI render it in the existing `<pre class="plan-tree">` without any structural assumptions. A nicer hand-rolled `Display` impl is a follow-up polish task — kept out of scope here to land a working end-to-end slice first.

**Out of scope:**
- Custom `Display` impl on `BrowserExecutionPlan` (mentioned in Task 6 as an optional follow-up)
- Native execution-target explain (this plan covers `BrowserWasm`; native runs go through a different code path)
- Per-stage timing alongside the plan tree

**Non-blocking dependencies:** None. All file paths exist as referenced.

---

## Task 1: Contract — add `explain` to `QueryResponse`

**Files:**
- Modify: [crates/query-contract/src/lib.rs:443-450](../../crates/query-contract/src/lib.rs)

**Step 1: Write the failing serde round-trip test**

Add this test at the bottom of `crates/query-contract/src/lib.rs` (look for an existing `#[cfg(test)] mod tests` block — append there; if none exists, add one):

```rust
#[cfg(test)]
mod explain_field_tests {
    use super::*;

    #[test]
    fn query_response_round_trips_explain_field() {
        let resp = QueryResponse {
            executed_on: ExecutionTarget::BrowserWasm,
            capabilities: CapabilityReport::default(),
            fallback_reason: None,
            metrics: QueryMetricsSummary::default(),
            explain: Some("Scan: events\n  Filter: status = 'fulfilled'".into()),
        };
        let wire = serde_json::to_string(&resp).expect("serialize");
        assert!(wire.contains("\"explain\""), "wire form: {wire}");
        let back: QueryResponse = serde_json::from_str(&wire).expect("deserialize");
        assert_eq!(back.explain.as_deref(), Some("Scan: events\n  Filter: status = 'fulfilled'"));
    }

    #[test]
    fn query_response_omits_explain_when_none() {
        let resp = QueryResponse {
            executed_on: ExecutionTarget::BrowserWasm,
            capabilities: CapabilityReport::default(),
            fallback_reason: None,
            metrics: QueryMetricsSummary::default(),
            explain: None,
        };
        let wire = serde_json::to_string(&resp).expect("serialize");
        assert!(!wire.contains("\"explain\""), "expected explain to be omitted; wire: {wire}");
    }
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test -p query-contract explain_field_tests
```

Expected: FAIL — `no field 'explain' on type 'QueryResponse'`.

**Step 3: Add the field**

Edit `crates/query-contract/src/lib.rs:444-450` so the struct reads:

```rust
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct QueryResponse {
    pub executed_on: ExecutionTarget,
    pub capabilities: CapabilityReport,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_reason: Option<FallbackReason>,
    pub metrics: QueryMetricsSummary,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub explain: Option<String>,
}
```

The `#[serde(default, skip_serializing_if = "Option::is_none")]` pattern mirrors `fallback_reason` exactly and keeps existing wire payloads forward-compatible.

**Step 4: Run test + cargo check to verify the rest of the workspace still compiles**

```bash
cargo test -p query-contract explain_field_tests
cargo check --workspace
```

Expected: tests PASS. `cargo check` will fail in any crate that constructs `QueryResponse` by positional/struct-literal without `explain` — fix each by adding `explain: None`. Likely callers:

```bash
grep -rn "QueryResponse {" crates/ --include="*.rs"
```

Add `explain: None` to every match. Re-run `cargo check --workspace` until green.

**Step 5: Commit**

```bash
git add crates/query-contract/src/lib.rs $(grep -rln "QueryResponse {" crates/ --include="*.rs")
git commit -m "contract: add optional QueryResponse.explain field"
```

---

## Task 2: Session — populate `explain` when `include_explain` is set

**Files:**
- Modify: [crates/wasm-query-session/src/lib.rs:169-211](../../crates/wasm-query-session/src/lib.rs)
- Test: [crates/wasm-query-session/tests/session.rs:343](../../crates/wasm-query-session/tests/session.rs) (extend `query_request` helper)

**Step 1: Write the failing test**

Append to `crates/wasm-query-session/tests/session.rs`:

```rust
#[test]
fn session_sql_returns_explain_string_when_requested() {
    let fixture = TestTableFixture::create_partitioned();
    let resolved_snapshot = resolve_latest_snapshot(&fixture);
    let server = LoopbackObjectServer::from_fixture_paths(
        &fixture,
        resolved_snapshot.active_files.iter().map(|file| file.path.clone()),
    );
    let materialized = materialize_loopback_snapshot(&resolved_snapshot, &server);
    let mut session = BrowserQuerySession::new(BrowserRuntimeConfig::default(), u64::MAX)
        .expect("default browser runtime config should be supported");
    session
        .cache_table("events", materialized, None)
        .expect("session should cache snapshot");

    let mut request = query_request(
        &resolved_snapshot.table_uri,
        resolved_snapshot.snapshot_version,
        "SELECT id FROM axon_table WHERE category = 'A' LIMIT 2",
    );
    request.options.include_explain = true;

    let result = runtime()
        .block_on(session.sql("events", &request))
        .expect("query should execute");

    let explain = result.response.explain.as_deref().expect("explain should be populated");
    // The Debug rendering of BrowserExecutionPlan must include the scan + filter shape.
    assert!(explain.contains("BrowserExecutionPlan"), "explain text: {explain}");
    assert!(explain.contains("table_uri"), "explain text: {explain}");
}

#[test]
fn session_sql_omits_explain_by_default() {
    let fixture = TestTableFixture::create_partitioned();
    let resolved_snapshot = resolve_latest_snapshot(&fixture);
    let server = LoopbackObjectServer::from_fixture_paths(
        &fixture,
        resolved_snapshot.active_files.iter().map(|file| file.path.clone()),
    );
    let materialized = materialize_loopback_snapshot(&resolved_snapshot, &server);
    let mut session = BrowserQuerySession::new(BrowserRuntimeConfig::default(), u64::MAX)
        .expect("default browser runtime config should be supported");
    session
        .cache_table("events", materialized, None)
        .expect("session should cache snapshot");

    let request = query_request(
        &resolved_snapshot.table_uri,
        resolved_snapshot.snapshot_version,
        "SELECT id FROM axon_table LIMIT 1",
    );

    let result = runtime()
        .block_on(session.sql("events", &request))
        .expect("query should execute");

    assert!(
        result.response.explain.is_none(),
        "explain must be None when include_explain is false; got: {:?}",
        result.response.explain
    );
}
```

**Step 2: Run tests to verify they fail**

```bash
cargo test -p wasm-query-session session_sql_returns_explain_string_when_requested
cargo test -p wasm-query-session session_sql_omits_explain_by_default
```

Expected: both FAIL — `explain` field exists (after Task 1) but `sql()` never populates it, so the first test fails on `expect("explain should be populated")` and the second technically already passes (returns None by default), but the first is the load-bearing one.

**Step 3: Populate `explain` in `sql()`**

Edit `crates/wasm-query-session/src/lib.rs:202-210`. Replace the `Ok(BrowserSessionQueryResult { ... })` block with:

```rust
        let explain = if request.options.include_explain {
            Some(format!("{plan:#?}"))
        } else {
            None
        };

        Ok(BrowserSessionQueryResult {
            response: QueryResponse {
                executed_on: ExecutionTarget::BrowserWasm,
                capabilities: materialized.required_capabilities().clone(),
                fallback_reason: None,
                metrics: execution_metrics(&plan, &runtime_result, started_at)?,
                explain,
            },
            runtime_result,
        })
```

Note: `plan` is the `BrowserExecutionPlan` already in scope from the `if/else` block on lines 181-195. It derives `Debug` (verified at `crates/wasm-query-runtime/src/lib.rs:1034`), so `format!("{plan:#?}")` works without any new trait impl.

**Step 4: Run tests to verify they pass**

```bash
cargo test -p wasm-query-session session_sql_returns_explain_string_when_requested
cargo test -p wasm-query-session session_sql_omits_explain_by_default
cargo test -p wasm-query-session  # full crate test to confirm no regressions
```

Expected: both new tests PASS, existing tests still PASS.

**Step 5: Commit**

```bash
git add crates/wasm-query-session/src/lib.rs crates/wasm-query-session/tests/session.rs
git commit -m "session: populate QueryResponse.explain when include_explain is set"
```

---

## Task 3: TS SDK — mirror `explain` on `QueryResponse`

**Files:**
- Modify: [examples/browser-delta-sandbox/src/axon-browser-sdk.ts:118-123](../../examples/browser-delta-sandbox/src/axon-browser-sdk.ts)

**Step 1: Add the field**

Edit `examples/browser-delta-sandbox/src/axon-browser-sdk.ts:118-123`:

```typescript
export type QueryResponse = {
  executed_on: ExecutionTarget;
  capabilities: CapabilityReport;
  fallback_reason?: FallbackReason;
  metrics: QueryMetricsSummary;
  explain?: string;
};
```

**Step 2: Run typecheck**

```bash
cd examples/browser-delta-sandbox && npx tsc --noEmit
```

Expected: clean (no callers depend on the shape of `QueryResponse` beyond reading existing fields, and the new field is optional).

**Step 3: Commit**

```bash
git add examples/browser-delta-sandbox/src/axon-browser-sdk.ts
git commit -m "sdk(ts): mirror QueryResponse.explain optional field"
```

---

## Task 4: Editor wiring — request explain and route it to PlanSummary

**Files:**
- Modify: [examples/browser-delta-sandbox/src/services/query.ts](../../examples/browser-delta-sandbox/src/services/query.ts) — set `include_explain: true`, surface explain in the outcome
- Modify: [examples/browser-delta-sandbox/src/services/types.ts](../../examples/browser-delta-sandbox/src/services/types.ts) — extend `QueryRunResult` with explain
- Modify: [examples/browser-delta-sandbox/src/editor/App.tsx](../../examples/browser-delta-sandbox/src/editor/App.tsx) — feed explain into `plan` state

**Step 1: Extend `QueryRunResult` type**

Edit `examples/browser-delta-sandbox/src/services/types.ts` — find the `QueryRunResult` type (search for `status: 'done'`) and add `explain?: string;` next to `fallback_reason`:

```typescript
export type QueryRunResult = {
  status: 'done';
  result: QueryResultData;
  metrics: QueryMetricsSummary;
  executed_on: ExecutionTarget;
  capabilities: CapabilityReport;
  fallback_reason?: FallbackReason;
  explain?: string;
  elapsed_ms: number;
};
```

**Step 2: Request explain + forward it in `services/query.ts`**

Edit `examples/browser-delta-sandbox/src/services/query.ts`. Find the `state.client.query(...)` call (around the middle of `runQuery`). Change the `options` block:

```typescript
        options: { collect_metrics: true, include_explain: true },
```

(previously `include_explain: false`)

In the same function, in the `return { status: 'done', ... }` block right below, add:

```typescript
        explain: result.response.explain,
```

right after `fallback_reason:`.

**Step 3: Feed explain into plan state in `App.tsx`**

Edit `examples/browser-delta-sandbox/src/editor/App.tsx`. The `plan` state is currently:

```typescript
const [plan] = useState<PlanSummary | undefined>(undefined);
```

Change to:

```typescript
const [plan, setPlan] = useState<PlanSummary | undefined>(undefined);
```

Then, in the `runActive` callback, in the branch where `outcome.status === 'done'`, add right after `setMetrics(outcome.metrics);`:

```typescript
      if (outcome.explain) {
        setPlan({ tree: outcome.explain, files: [] });
      } else {
        setPlan(undefined);
      }
```

`PlanSummary` already has a `tree: string` field that the Plan tab renders in `<div className="plan-tree">{plan.tree}</div>` (see `src/editor/components/Results.tsx`). No UI change needed.

**Step 4: Verify typecheck + lint**

```bash
cd examples/browser-delta-sandbox && npx tsc --noEmit && npx eslint . --max-warnings=0
```

Expected: both clean.

**Step 5: Extend the editor smoke test to assert explain renders**

Edit `examples/browser-delta-sandbox/tests/editor-smoke.spec.ts`. After the existing `await expect(page.locator('table.grid tbody tr')).toHaveCount(1);` assertion, add:

```typescript
    // Plan tab renders the explain string from the worker.
    await page.locator('.res-tab', { hasText: 'Plan' }).click();
    await expect(page.locator('.plan-tree')).toContainText('BrowserExecutionPlan', { timeout: 5_000 });
```

**Step 6: Run the smoke test**

In one terminal:

```bash
cd examples/browser-delta-sandbox && npx vite --host 127.0.0.1 --port 5174 --strictPort
```

In another:

```bash
cd examples/browser-delta-sandbox && npx playwright test --config=playwright.editor-smoke.config.ts --reporter=line
```

Expected: PASS.

**Step 7: Commit**

```bash
git add \
  examples/browser-delta-sandbox/src/services/types.ts \
  examples/browser-delta-sandbox/src/services/query.ts \
  examples/browser-delta-sandbox/src/editor/App.tsx \
  examples/browser-delta-sandbox/tests/editor-smoke.spec.ts
git commit -m "editor: request explain on every query and render it in the Plan tab"
```

---

## Task 5: Rebuild WASM so the change ships end-to-end

The editor smoke test in Task 4 used a pre-built WASM that does not have the new explain field. The Playwright smoke will fail at the Plan-tree assertion (the JS SDK parses `result.response.explain`, but the Rust side hasn't been rebuilt). This task makes it real.

**Step 1: Rebuild the WASM sandbox bundle**

```bash
cd examples/browser-delta-sandbox
npm run build:wasm
```

This is the same script Vite uses; it runs `cargo build -p browser-delta-sandbox --target wasm32-unknown-unknown --release --locked` then `wasm-bindgen`. Expected: completes without errors and refreshes `src/wasm/`.

**Step 2: Re-run the editor smoke test**

```bash
cd examples/browser-delta-sandbox && npx playwright test --config=playwright.editor-smoke.config.ts --reporter=line
```

Expected: PASS. The Plan tab now contains `BrowserExecutionPlan` because the rebuilt WASM populates the field end-to-end.

**Step 3: Run the existing sandbox suite to confirm no regressions**

```bash
cd examples/browser-delta-sandbox && npx playwright test --reporter=line tests/browser-delta-sandbox.spec.ts
```

Expected: PASS (the sandbox doesn't read `explain`, but this confirms the wire format change didn't break parsing).

**Step 4: Commit (only if `src/wasm/` is tracked; it usually isn't — confirm with `git status`)**

```bash
git status examples/browser-delta-sandbox/src/wasm/
```

If files are tracked: `git add` them and commit `"wasm: rebuild sandbox bundle with explain field"`. If untracked (the typical case — they're built artifacts), skip the commit.

---

## Task 6 (Optional polish): Custom `Display` impl for `BrowserExecutionPlan`

The Debug formatting reads like a Rust struct dump. The design's Plan tab mockup expects a tree-style render. This task is **optional** — the previous tasks ship a working explain string; this one makes it pretty.

**Files:**
- Modify: [crates/wasm-query-runtime/src/lib.rs:1050-1098](../../crates/wasm-query-runtime/src/lib.rs)
- Modify: [crates/wasm-query-session/src/lib.rs](../../crates/wasm-query-session/src/lib.rs) — swap `format!("{plan:#?}")` for `format!("{plan}")`

**Step 1: Write a snapshot-style test in wasm-query-runtime**

Add to `crates/wasm-query-runtime/tests/` (create new file `explain_format.rs`):

```rust
use wasm_query_runtime::BrowserExecutionPlan;

// Adapt this builder to whatever ctor or test helper already exists for
// BrowserExecutionPlan; check `crates/wasm-query-runtime/src/lib.rs` for an
// existing constructor or test fixture before fabricating fields by hand.
fn sample_plan() -> BrowserExecutionPlan {
    unimplemented!("use the runtime's existing plan builder/fixture")
}

#[test]
fn display_renders_tree_with_scan_filter_and_limit() {
    let plan = sample_plan();
    let rendered = format!("{plan}");
    assert!(rendered.contains("Scan"));
    assert!(rendered.contains("Filter") || rendered.contains("Limit"));
    // Indentation conveys depth: lines after the first should start with ws or '└'.
    let lines: Vec<&str> = rendered.lines().collect();
    assert!(lines.len() >= 2, "expected multi-line tree: {rendered}");
}
```

**Step 2: Implement `Display` on `BrowserExecutionPlan`**

In `crates/wasm-query-runtime/src/lib.rs` after the `impl BrowserExecutionPlan` block (around line 1098), add:

```rust
impl std::fmt::Display for BrowserExecutionPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Scan: {} (v{})", self.table_uri, self.snapshot_version)?;
        if let Some(filter) = self.filter.as_ref() {
            writeln!(f, "  Filter: {filter:?}")?;
        }
        if let Some(agg) = self.aggregation.as_ref() {
            writeln!(f, "  Aggregate: {agg:?}")?;
        }
        if !self.order_by.is_empty() {
            writeln!(f, "  Sort: {:?}", self.order_by)?;
        }
        if let Some(limit) = self.limit {
            writeln!(f, "  Limit: {limit}")?;
        }
        Ok(())
    }
}
```

(Refine the format based on what the components actually print well; this is a starting point.)

**Step 3: Switch the session to use Display**

In `crates/wasm-query-session/src/lib.rs` (the line from Task 2):

```rust
            Some(format!("{plan}"))
```

instead of `format!("{plan:#?}")`.

**Step 4: Update the wasm-query-session test from Task 2**

The `explain.contains("BrowserExecutionPlan")` assertion expects the Debug format. Change to `explain.contains("Scan:")` to match the new Display.

Also update the editor smoke test's Plan-tree assertion to look for `Scan:` instead of `BrowserExecutionPlan`.

**Step 5: Run tests + rebuild WASM**

```bash
cargo test -p wasm-query-runtime display_renders_tree
cargo test -p wasm-query-session session_sql_returns_explain_string_when_requested
cd examples/browser-delta-sandbox && npm run build:wasm && npx playwright test --config=playwright.editor-smoke.config.ts
```

Expected: all PASS.

**Step 6: Commit**

```bash
git add crates/wasm-query-runtime/src/lib.rs crates/wasm-query-runtime/tests/explain_format.rs \
        crates/wasm-query-session/src/lib.rs crates/wasm-query-session/tests/session.rs \
        examples/browser-delta-sandbox/tests/editor-smoke.spec.ts
git commit -m "runtime: render BrowserExecutionPlan as a tree-style explain"
```

---

## Verification Checklist

After Task 5 (or Task 6 if pursued):

- [ ] `cargo test -p query-contract` green
- [ ] `cargo test -p wasm-query-session` green
- [ ] `cargo check --workspace` green
- [ ] `cd examples/browser-delta-sandbox && npx tsc --noEmit && npx eslint . --max-warnings=0` green
- [ ] `npx playwright test --config=playwright.editor-smoke.config.ts` green (Plan tab shows the explain string)
- [ ] `npx playwright test tests/browser-delta-sandbox.spec.ts` still green (no regression on the existing sandbox suite)
- [ ] Manual: open `https://127.0.0.1:5174/`, run a query, click the Plan tab — verify the rendered tree is sensible.

## Files Touched (cumulative)

| File | Purpose |
|---|---|
| `crates/query-contract/src/lib.rs` | Add `explain: Option<String>` to `QueryResponse` |
| `crates/wasm-query-session/src/lib.rs` | Populate `explain` in `sql()` when flag set |
| `crates/wasm-query-session/tests/session.rs` | Cover the flag-on and flag-off paths |
| `examples/browser-delta-sandbox/src/axon-browser-sdk.ts` | Mirror the new field on the TS contract |
| `examples/browser-delta-sandbox/src/services/types.ts` | Carry `explain` through `QueryRunResult` |
| `examples/browser-delta-sandbox/src/services/query.ts` | Request `include_explain: true`, forward `explain` |
| `examples/browser-delta-sandbox/src/editor/App.tsx` | Push `explain` into `plan` state |
| `examples/browser-delta-sandbox/tests/editor-smoke.spec.ts` | Assert the Plan tab renders the explain string |
| `crates/wasm-query-runtime/src/lib.rs` (Task 6 only) | Custom `Display` impl |
| `crates/wasm-query-runtime/tests/explain_format.rs` (Task 6 only) | Cover the Display impl |
