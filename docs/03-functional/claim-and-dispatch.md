# Claim and dispatch

The coordinator reads a keyset page of up to 128 visible job identities per shard, then bulk-loads those policies. It advances past excluded pages, so a large closed-window prefix cannot monopolize candidate selection. A nonempty positive job-key filter reaches the claim transaction; the engine never uses the store's explicitly unrestricted empty filter.

The claim locks the process session, observes database time, locks candidate jobs in canonical order, verifies active definitions and computes positive per-job concurrency headroom from current grants. Zero cap is unlimited. A fresh observation after admission locks validates the session again and supplies claimed_at. The caller's horizon can restrict eligibility but cannot move it beyond database time.

The work row changes atomically from READY to LEASED. PostgreSQL uses a picked CTE and UPDATE RETURNING; SQL Server uses UPDATE OUTPUT; MySQL/H2 use a locking pick and guarded UPDATE in the same READ_COMMITTED transaction. There is no queue-delete/lease-insert transition. See the [dialects](../06-data/dialects.md) for actual differences and [transactions](../04-engineering/transactions.md) for lock order.

The returned grant contains identity, policy key, attempt, priority and token. Input snapshots are read in bounded groups. Dispatcher selects the local executable and runner, reconstructs the input with InputCodec and invokes user code after the database transaction has ended.

## Resource admission

| Rule | Authority | Deferral |
|---|---|---|
| Session active | Database session lock and fresh clock | No claim |
| Job active and positive concurrency cap | Canonically ordered job locks and grant count | No grant beyond headroom |
| Named execution window | Local predicate before and after claim | Fenced READY transition, same attempt |
| Shared rate bucket | Database-time full-state CAS | Excess group members return READY, same attempt |
| Runner / process dispatch capacity | Local permits and tracked tasks | Bounded submission; failure follows explicit dispatch path |

Window and availability checks are cached per resource within their admission phase. Post-claim charging groups jobs sharing a rate name. A successful charge authorizes that group; unknown or failed charge does not dispatch. Charged tokens are not refunded after a later failure, since the outcome may be uncertain. A cap bounds authorized grants; stale handlers outside that authority may still be performing external effects.

## Progress and ordering

Priority, visibility and execution ID order the selected candidate set. Pages and shard rotation do not provide global FIFO or global priority. Repeatedly closed windows and exhausted rates can defer work indefinitely. Retired definitions cannot acquire more work; a grant already acquired may finish against its snapshot.

One TTL/4 monotonic budget covers the entire round, with a rotating stage cursor. SQL deadlines are separate and cannot interrupt every pool/driver operation precisely at a checkpoint. max-poll-interval bounds sleep, not queue or dispatch latency. Idle existence checks avoid redundant shard laps once emptiness is established.

Candidate pagination bounds returned policy metadata. DISTINCT over a deep backlog can still inspect many index entries. Use actual plans and arrival/backlog measurements before claiming bounded database work or capacity.
