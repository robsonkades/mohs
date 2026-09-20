# Glossary

| Term | Meaning |
|---|---|
| Job | Durable definition naming an executable, input contract, schedule and immutable policy value |
| JobRef<T> | Technical key, local input class and explicit persisted input contract |
| Registration | Local handler plus definition, published atomically with the executable catalog |
| Trigger | Definition revision and next_fire_at; NULL disarms a fixed-delay series while its occurrence runs |
| Occurrence | SCHEDULED execution tied to the trigger revision that created it |
| Execution | Immutable accepted identity/input snapshot; survives retries until a terminal state |
| Attempt | One authorized try; only its latest completed result is retained |
| Payload | JSON snapshot plus input_contract; no persisted Java class name |
| Actor | Required Unicode audit label; origin, not this label, decides scheduled behavior |
| Work | Single active row, READY or LEASED; terminal work has no active row |
| READY | Waiting for visibility and admission; attempt number is already assigned |
| LEASED / grant | Current session/token authority to settle an execution; not proof that only one external handler exists |
| Claim | Atomic guarded READY-to-LEASED transition after session and per-job admission locks |
| Fence | Execution ID, session ID, fresh claim token and attempt checked by a write; stale owners lose |
| Session | UUIDv7 process identity with database expiry and irreversible revocation |
| Heartbeat | Renewal of an unexpired, unrevoked session; expiry cannot be undone by renewal |
| Recovery | Revoke expired session, then settle its grants through remaining retry/cancellation policy |
| Stray grant | This session owns work with no local handler or pending completion after a grace period |
| Requeue | Fenced admission deferral returning the same attempt to READY |
| Retry | A new attempt of the same execution; scheduled lineage remains for automatic retry |
| Manual retry | Reactivates a retained failed execution as MANUAL, without rearming its former recurring trigger |
| Candidate page | Up to 128 visible job keys per shard; positive eligible keys constrain claim |
| Shard | Deterministic execution-ID partition among 64 buckets; membership suggests ownership, database locks arbitrate actual work |
| Rate limit | Shared token bucket with fractional refill; charges admission, not successful external effects |
| Concurrency cap | Positive per-job ceiling on authorized grants; zero means unlimited |
| Window | Local named exclusion predicate checked at dispatch admission; can defer work without consuming a retry |
| Fixed rate | Recurrence anchored to scheduled instants |
| Fixed delay | Matching scheduled terminal completion rearms its revision from finish time |
| Misfire | Overdue occurrence beyond the configured threshold, handled by IGNORE, FIRE_NOW or bounded FIRE_ALL_MISSED |
| Idempotency receipt | Acceptance result keyed by job/key, with fixed expiry independent of history retention |
| Batch | Flat set of accepted executions with durable terminal counters; no persisted workflow or local callback registry |
| Drain | Stop acquiring new work and await current handlers within a grace period |
| Zombie | Handler continues after authority is lost; its result is fenced out |
| Unknown outcome | Connection/timeout leaves commit uncertain; an exception is not proof of rollback |
