# Browser WASM + Delta on GCS External Blockers

- Date: 2026-03-31
- Scope: work required for launch that is not implemented in this repository

The repository can prove the browser engine, control-plane descriptor seam, worker artifact, and repo-level guardrails. It cannot prove the service and operational items below because the required code or infrastructure is not present here.

| Blocker | Owner | Current repo state | Required external proof |
| --- | --- | --- | --- |
| `services/query-api` or equivalent trusted service | Storage platform team | No `services/` tree exists in this repository | Service code merged and exercised end to end |
| Signed URL issuance | Storage platform team | No in-repo signing code exists | Trusted service returns browser-safe object URLs |
| Proxy-mode request issuance | Storage platform team | No in-repo read proxy exists | Trusted service terminates browser requests and enforces policy |
| Signed URL TTL policy approval | Security engineering | No approval artifact exists in repo | Reviewed TTL policy and security signoff |
| Object-scoped signed URLs | Storage platform team | Not implemented in repo | Verified object-level scoping in service responses |
| Audit logging and request correlation | Storage platform team + SRE / production engineering | No audit or correlation pipeline in repo | Logs correlate read access to user, session, and request id |
| Production XML-endpoint CORS/origin validation | Security engineering | No production-shape CORS validation in repo | Validated against the exact production endpoint path |
| Native/browser regression dashboards | SRE / production engineering + QA / performance engineering | Metrics contract exists, dashboards do not | Live trend dashboards with alert ownership |
| Production oncall / control-plane outage playbook | SRE / production engineering | Repo runbook is local-only and service-agnostic | Service-aware incident playbook with oncall ownership |
| Env-gated GCS fixture provisioning, IAM, and CI variables | Storage platform team | Smokes are env-gated and skip when unset | Fixture tables, runner identity, and CI variables configured |

Explicit repo deferrals that must not be mistaken for shipped launch scope:

- OPFS / IndexedDB persistent-cache backends
- browser DataFusion as the default execution SKU
