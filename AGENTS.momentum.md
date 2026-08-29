# Momentum progress-first engineering policy

Merge the relevant sections into the repository's applicable `AGENTS.md`.
Keep repository-specific build/test commands accurate.

## Change discipline

- Deliver the smallest complete change that satisfies observable requirements.
- Prefer extending the existing owning path over adding a parallel path.
- Reuse existing types, dependencies, error handling, and test style.
- Do not add a dependency, public API/configuration, abstraction, feature flag, registry, retry, fallback, cache, compatibility layer, migration, or generalized framework without a current demonstrated need.
- Keep unrelated cleanup, formatting, renaming, and refactoring out of the patch.
- Generalize from multiple concrete present uses, not imagined future consumers.
- Every safeguard must name the current failure mode or invariant it protects.
- Preserve all pre-existing user changes.

## Planning

For nontrivial or ambiguous implementation work, invoke `$momentum`.

The Sol planner may investigate deeply, but its final Execution Contract must contain one bounded path, explicit non-goals, a complexity budget, targeted validation, and stop conditions.

## Implementation

- Exactly one agent owns source edits.
- Use Terra Max for normal/hard work.
- Use Luna Max only for clear local work with no public, persistence, transaction, concurrency, security, recovery, compatibility, or architectural boundary.
- A builder must return `CONTRACT_MISMATCH` instead of silently widening scope.
- Run the narrowest meaningful tests first.
- Stop when acceptance criteria pass.

## Review

- Correctness review reports only reachable evidence-backed defects.
- Simplicity review reports only code that can be removed or reduced while preserving acceptance criteria.
- Style preferences and hypothetical future concerns are not blockers.
- Allow at most one bounded repair pass.

## Validation

Replace this section with the repository's real commands.

1. formatter/check for affected language
2. targeted test proving changed behavior
3. affected package/crate/module checks
4. broader workspace tests only when shared behavior or repository policy requires them

## Definition of done

The task is done when acceptance criteria pass, no accepted blocker remains, the final diff stays inside approved scope, and pre-existing user changes remain intact.
