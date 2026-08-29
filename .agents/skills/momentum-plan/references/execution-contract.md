# Execution Contract format

The approved contract must use this structure.

```markdown
# Execution Contract

PLAN STATUS: READY | EXPERIMENT-FIRST | BLOCKED
IMPLEMENTER: terra | luna

## Goal
One precise observable outcome.

## Current behavior and evidence
Concrete repository evidence with files and symbols.

## Root cause and confidence
The mechanism plus: confirmed | strongly supported | uncertain.
When uncertain, name the bounded first experiment.

## Invariants to preserve
Only current invariants that materially constrain this change.

## Chosen approach
Exactly one approach and why it is the smallest sufficient path.

## Exact implementation targets
Expected files/modules/symbols and intended change.

## Implementation sequence
Maximum seven numbered steps.

## Acceptance criteria
Three to six observable, testable criteria.

## Validation
Exact targeted commands or the narrowest repository-appropriate checks.

## Complexity budget
- expected source files
- expected test files
- approximate diff tripwire
- new dependencies
- new public APIs/configuration
- new abstractions
- new compatibility/fallback paths
- unrelated cleanup

## Non-goals
Adjacent work explicitly excluded.

## Stop conditions
Conditions requiring CONTRACT_MISMATCH rather than scope expansion.

## Remaining material risks
Maximum three current risks.
```

The contract is an implementation boundary, not an architecture essay.

The builder receives this contract, not the planner's hidden exploration.
