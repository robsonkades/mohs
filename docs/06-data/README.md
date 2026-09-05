# 6. Data

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository

| Document | What it covers |
| --- | --- |
| [data-model.md](data-model.md) | The nine tables, the four write profiles, column-by-column semantics, key strategy, the temporal contract |
| [schema.md](schema.md) | The canonical DDL, the dialect type map, the constraints inventory |
| [indexes.md](indexes.md) | Every index, the query it serves, and the measurement behind it |
| [migrations.md](migrations.md) | **You install the schema**: the installer, the V1–V8 delta chain, and the operational cost of `V5` |
| [dialects.md](dialects.md) | Support tiers and the exact divergences per database |
| [data-lifecycle.md](data-lifecycle.md) | Growth profiles, automatic cleanup and history-retention options |

## In one screen

```
CONTROL PLANE (read often, written rarely)
  mohs_job_definitions   one row per job; definitional vs. operational state separated
  mohs_rate_limits       one row per limit; token bucket state
  mohs_nodes             one row per node incarnation; the LEASE lives here
  mohs_batches           one row per batch; pending is derived

HOT PATH (split by WRITE PROFILE)
  mohs_ready             THE QUEUE      — size = backlog       — INSERT/DELETE churn
  mohs_lease             OWNERSHIP      — size = in-flight     — INSERT/DELETE churn
  mohs_execution         HISTORY        — retained by default  — 1 INSERT + 1 UPDATE
  mohs_attempt           ATTEMPT LOG    — follows history TTL  — append only
  mohs_idempotency       DEDUP          — pruned automatically — INSERT, PK conflict IS the check

FLYWAY
```

**The consequence that matters**: the claim statement references only `mohs_ready` and `mohs_lease`,
so **history size does not affect claim cost** — measured flat between ~0 and 2 M history rows.

**The lifecycle choice that matters**: execution history and payloads are retained unless
`mohs.engine.history-retention` is configured. Idempotency records are pruned automatically. See
[data-lifecycle.md](data-lifecycle.md).
