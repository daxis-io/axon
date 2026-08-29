# Deterministic contract gate

Accept the plan only when every item is true.

## Required shape

- [ ] `PLAN STATUS` is present.
- [ ] `IMPLEMENTER` is `terra` or `luna`.
- [ ] Goal states observable behavior.
- [ ] Current behavior is grounded in repository evidence.
- [ ] Root cause is confirmed/strongly supported, or the first step is a bounded experiment.
- [ ] Current invariants are stated.
- [ ] Exactly one chosen approach is present.
- [ ] Exact implementation targets are present.
- [ ] Implementation sequence has no more than seven steps.
- [ ] Acceptance criteria contain three to six testable items.
- [ ] Validation contains targeted commands/checks.
- [ ] Complexity budget is explicit.
- [ ] Non-goals are explicit.
- [ ] Stop conditions are explicit.
- [ ] Remaining risks contain no more than three items.

## Scope limits

- [ ] No alternatives section.
- [ ] No future-work program.
- [ ] No unrelated cleanup.
- [ ] New dependencies default to zero.
- [ ] New public APIs/configuration default to zero.
- [ ] New abstractions default to zero.
- [ ] No speculative retry, fallback, cache, compatibility shim, feature flag, registry, migration, or parallel path.
- [ ] Every nonzero exception names the current requirement or demonstrated failure that requires it.

## Luna route limits

Luna is valid only when:

- [ ] edit/root cause is clear
- [ ] local existing pattern
- [ ] roughly two source files plus tests
- [ ] no public/persistence/concurrency/security/compatibility boundary
- [ ] no new dependency or abstraction

When any Luna item is false, route to Terra without replanning.
