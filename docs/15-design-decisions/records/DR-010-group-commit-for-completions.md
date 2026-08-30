# DR-010: Group commit for completions, with the durability window declared

## Status

Accepted

## Context

Every finished attempt writes a completion: a fenced lease delete, an attempt insert, a terminal
update or a retry insert, plus batch counting and any fixed-delay rearm. With one transaction per
execution, that is one commit — and therefore one fsync — per execution.

Under load, the measured wait profile put `LWLock:WALWrite` at the top. Commits per execution measured
**3.9** in the single-table era and did not fall far below 2 with the split alone.

## Decision

Completions accumulate in a bounded queue and flush in **one `LeaseStore#complete` transaction** when
either **256 results** or **5 ms** has elapsed since the first pending one.

The semantic cost is declared in the class's own Javadoc rather than discovered:

> The window between "the handler finished" and "the result is durable" grows from about 1 ms to at
> most `flushInterval`; a crash in that window re-executes up to `flushSize` results more than those
> in flight. **The contract was already at-least-once — this changes the exposure to duplicates, not
> the guarantee.**

The opt-out is `mohs.engine.completion-flush-on-every-result=true`, and it is the **only** knob the
decision adds.

## Consequences

### Positive

- **Commits per execution fell to 0.037–0.048** — fewer than one commit per twenty executions.
- Throughput after the split reached 12.2–14.5 k/s on a single node, where the synchronous-commit
  control arm sat around 4.7 k/s in the comparable earlier round.
- **Backpressure is structural**: a full queue (`4 × flushSize`) **blocks `submit` on the handler's
  thread**. The dispatch stays in flight, the claim sees reduced headroom, and the node stops claiming
  beyond what it can persist. No configuration is needed to make that work.
- Failure is graded: a batch flush failure falls back to **one transaction per result**, and an
  individual failure leaves that execution for the reaper without killing the flusher or its
  neighbours.

### Negative

- **A duplicate-exposure window of up to 5 ms.** Declared, not hidden.
- **The stray-lease reconcile needed a state-based guard.** A completion in transit has a lease with no
  in-flight incarnation, which is exactly the shape of a stray lease. `completionInTransit` is that
  guard, and it exists only because of this decision.
- **The reconcile also needed a temporal grace**, and its floor is not theoretical: during a cold start
  the flusher fell more than 500 ms behind and the reconcile requeued whole batches of completions in
  transit — blocks of 256 and 512, **10,700 lost fences in one cold round**. An earlier version
  produced 199,000 WARNs and phantom requeues that contended with the flush until deadlock.
- **The shutdown order became load-bearing.** The batcher must drain **before** the final `STOPPED`
  heartbeat: that heartbeat zeroes `expires_at`, and from then on every peer considers this node dead
  — a result still in the queue would be reclaimed as an orphan.
- **Two cold paths must bypass the batcher.** `failBeforeDispatch` and `abandonOwnership` depend on
  their callers' "it threw, so it did not happen" contract; routing them through the queue would trade
  a synchronous exception for a log line in the flusher that nobody retries.
- The flusher is a service thread and needs an explicit failure policy: an `Error` killing it with
  `closed` false would fill the queue, block every submit forever, and strand in-flight executions
  with their leases held.

## Alternatives considered

| Alternative | Why not |
| --- | --- |
| One transaction per completion | The behaviour being replaced. Measured at the top of the wait profile |
| Asynchronous commit at the database level (`synchronous_commit = off`) | Trades durability **globally**, including the host application's own writes. This decision trades it locally, for a bounded window, and only for Mohs |
| A larger flush window | Grows the duplicate exposure linearly. 5 ms was chosen as small enough to be uninteresting and large enough to batch |
| Making size and interval configurable | Would be two knobs for a decision that has one meaningful question: on or off |

## Evidence

- `mohs-engine/src/main/java/io/mohs/engine/CompletionBatcher.java` — the mechanism, the declared
  cost, the failure policy, and the drain protocol.
- `mohs-engine/src/main/java/io/mohs/engine/Engine.java` — `stop`, where `drainCompletions` precedes
  the final heartbeat, and `reconcileOwnStrayLeases`, whose grace exists because of this.
- `mohs-spring-boot-starter/src/main/java/io/mohs/autoconfigure/MohsAutoConfiguration.java` — 256/5 ms
  fixed by decision.
- Group-commit measurements recorded 2026-08-22.
