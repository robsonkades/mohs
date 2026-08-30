# 8. Security

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository

| Document | What it covers |
| --- | --- |
| [security-overview.md](security-overview.md) | The trust model, what an unauthenticated caller can do, how to secure it, input validation, injection surface, data exposure, recommendations |

## The one paragraph that matters

**The Mohs REST API has no authentication and no authorization.** If `/api/mohs/v1/**` is reachable
from an untrusted network, anyone can invoke any job with any payload, pause jobs cluster-wide,
cancel and retry executions, change schedules, and change rate limits. The host application's
`SecurityFilterChain` — or a gateway in front of it — is the only protection, and Mohs is designed
so that protection is possible: the API and the dashboard are served on the **host's own server**,
inside the host's filter chain, deliberately and as the only mode.

The API is **off by default**, and enabling it logs a WARN naming exactly what it can do.

## Quick checklist before enabling the API

- [ ] `SecurityFilterChain` covering `/api/mohs/**` and `/mohs-ui/**`
- [ ] Read (`GET`) and write (`POST`/`PATCH`) separated by role
- [ ] A custom `ActorResolver` backed by real authentication, so the audit trail carries identity
- [ ] TLS terminated in front
- [ ] Alerting on `PATCH` calls — they are emergency levers and should be rare
- [ ] A decision about payload retention: payloads are stored as plaintext JSON **forever** today
