# Review and adjudication policy

## Correctness finding eligibility

A finding must include:

1. exact file:line or symbol
2. reachable scenario
3. concrete evidence
4. requirement or invariant violated
5. material impact
6. smallest local repair
7. confidence

No evidence means no finding.

## Accept

- BLOCKER with concrete evidence
- IMPORTANT with concrete evidence
- MINOR only when a failing test proves it or both reviewers independently identify the same material defect
- SIMPLIFY only when it removes or shrinks code and preserves all acceptance criteria

## Reject

- style, naming, formatting, taste
- speculative future requirements
- unreachable theoretical edge cases
- unrelated pre-existing issues
- generic hardening or defense in depth
- broader refactor than the current defect requires
- more abstraction/configuration/retry/fallback/cache/compatibility without a demonstrated current need
- duplicate findings
- low-confidence concern without evidence
- suggestions whose implementation surface exceeds the problem they claim to solve

## Limits

- maximum five accepted findings
- one repair pass
- no second full review
- narrow recheck only for a materially changed BLOCKER/IMPORTANT repair

Reviewers submit evidence. The conductor adjudicates. Reviewer consensus is not required.
