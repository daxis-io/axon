# Daxis Release Migration Notes Template

Use this template when assembling the Daxis release evidence bundle for an Axon release. Attach a completed copy to the release packet. Do not attach browser-visible secrets, signed URLs, raw credentials, service-account material, or private customer identifiers.

## Release Identity

- Axon commit SHA:
- Branch or tag:
- Release channel: `experimental` | `integration` | `candidate` | `stable`
- Release date:
- Release owner:
- Daxis platform owners reviewed:
- Daxis rollout decision link:

## Compatibility Classification

Select exactly one classification.

- [ ] No Daxis-facing breaking change.
- [ ] Breaking Daxis-facing change with migration required before rollout.

For a no-breaking release, include this statement:

```text
This Axon release does not introduce breaking changes to Daxis-facing contracts, runtime semantics, fallback vocabulary, compatibility claims, browser artifact selection, or rollout requirements.
```

## Change Inventory

Record every Daxis-facing change. Mark unchanged areas as `unchanged`; do not leave them blank.

| Area                                                    | Changed? | Evidence | Daxis action required | Rollback impact |
| ------------------------------------------------------- | -------- | -------- | --------------------- | --------------- |
| Public Rust contracts and JSON Schema                   |          |          |                       |                 |
| TypeScript SDK examples and worker envelopes            |          |          |                       |                 |
| Descriptor resolver or read-access-plan behavior        |          |          |                       |                 |
| Object-grant route envelopes or audit fixtures          |          |          |                       |                 |
| Fallback, block, or runtime-error vocabulary            |          |          |                       |                 |
| Supported SQL, Delta features, or table eligibility     |          |          |                       |                 |
| Runtime budgets, worker artifact, or default worker SKU |          |          |                       |                 |
| Browser dependency, credential, or signing guardrails   |          |          |                       |                 |
| Release channel, rollout controls, or external blockers |          |          |                       |                 |

## Required Evidence

- Contract artifact verifier:
- Daxis release evidence runner:
- Browser matrix result:
- Worker artifact size output:
- Daxis production rollout decision:
- External proof packet status:
- Stable default promotion state:

## Migration Plan

Complete this section when the compatibility classification is `Breaking Daxis-facing change with migration required before rollout`.

- Affected Daxis services, surfaces, or owners:
- Old contract or runtime behavior:
- New contract or runtime behavior:
- Required Daxis code or configuration changes:
- Backward compatibility window:
- Staged rollout sequence:
- Required dashboard or alert updates:
- User or operator communication:
- Rollback plan:

## Owner Signoff

- Release owner:
- Runtime / engine owner:
- Daxis product owner:
- Daxis query platform owner:
- Daxis catalog/storage owner:
- Daxis security owner:
- Daxis SRE owner:
