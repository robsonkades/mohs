# DR-004: Derive execution state from the queue and the lease

## Status

Accepted

## Context

Splitting the hot path ([DR-003](DR-003-split-the-hot-path-by-write-profile.md)) moved the truth about
an in-flight execution out of the history row and into two other tables. A `state` column in
`mohs_execution` could still have been maintained on every transition — but that would put an update
on the hot path for every state change, which is exactly what the split removed.

At the same time, a dashboard polling every two seconds cannot afford a three-way join for a count.

## Decision

`mohs_execution.state` is **advisory**: `PENDING` from birth until a single terminal write. Reads that
need the truth derive it:

| Observed | Derived state |
| --- | --- |
| A row in `mohs_lease` | `RUNNING`, with `owner` = its `node_id` and `firedAt` = its `claimed_at` |
| A row in `mohs_ready` with `attempt > 1` and `visible_at > now` | `RETRY_WAITING` |
| Any other row in `mohs_ready` | `ENQUEUED` |
| A terminal value in the column | Itself |
| `PENDING` with neither queue nor lease | `ENQUEUED` — a bounded, documented staleness window |

Reads that need **speed** use the column and accept that bounded staleness; reads that need **truth**
join.

## Consequences

### Positive

- **One update per execution in history**, at the terminal transition — the tuple-version figure the
  split was after.
- The `GET /overview` counts are proportional to live work by construction: `RUNNING` is the size of
  `mohs_lease`; `ENQUEUED` and `RETRY_WAITING` are `mohs_ready` split by the visibility rule. **No
  count read touches history.**
- `RETRY_WAITING` stops being a claimable state, which is what allowed the queue to have exactly one
  admission rule (`visible_at <= now`) — and that removed a two-value `IN` predicate responsible for a
  measured 3× regression on MySQL.

### Negative

- **The read model is a join**, and `findPage`'s `status` filter targets different storage per value:
  a terminal value filters the column, `RUNNING` filters ownership, `ENQUEUED` and `RETRY_WAITING`
  filter the queue.
- **A staleness window exists and is documented**: a `PENDING` row with neither queue nor lease is a
  completion flush in progress, and reads as `ENQUEUED`.
- Anyone querying `mohs_execution.state` directly, outside the library, reads `PENDING` for live work
  and must be told why.

## Alternatives considered

| Alternative | Why not |
| --- | --- |
| Maintain `state` on every transition | Puts an update on the hot path per transition — the cost the split removed |
| A database view over the three tables | Works for reads, but does not remove the maintenance cost, and pushes dialect-specific SQL into a place four sets of migrations must keep in step |
| Expose the raw advisory column in the API | The API would report `PENDING` for running work, which is simply wrong |

## Evidence

- `mohs-engine/src/main/java/io/mohs/engine/HistoryStore.java` — the derivation is specified in the
  port's Javadoc, staleness window included.
- `mohs-store-jdbc/src/main/java/io/mohs/store/jdbc/JdbcHistoryStore.java` — `READ_MODEL_COLUMNS` and
  the two `LEFT JOIN`s.
- `io.mohs.core.execution.ExecutionState` — the rename from `RETRY_SCHEDULED` to `RETRY_WAITING`
  carries exactly this change in meaning.
