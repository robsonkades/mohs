# Definitions and registration

JobDefinition and JobOptions are immutable values. A definition contains its technical identity, executable identity and input contract, schedule/misfire policy, resource references and operational state. JobRef<T> carries a local input class and a stable format identifier; NoInput represents an explicit no-input handler.

Register handlers programmatically or with the supported annotations. Both routes normalize into the same registration/catalog publication. Annotated job methods must
return `void` and complete synchronously. A non-void return (including Future/CompletionStage) or
Spring `@Async` on the method/class fails startup; otherwise the invocation could be acknowledged
before the work finishes. Await asynchronous operations inside the synchronous handler. Direct enqueue, scheduleAt, scheduleAfter and recurring scheduling methods replace the former staged ScheduleCommand/JobSpec/PolicySpec APIs. See the compilable forms in [programmatic jobs](../05-api/programmatic-jobs.md).

A named recurrence binds in one hop to a registered executable and stores its automatic input snapshot in the definition. Every accepted execution retains its own input snapshot. Editing a recurrence cannot change an existing execution's input. Incompatible formats require another job identity; a retained identity cannot silently change input contract.

## Publication and operational changes

The scanner prepares the complete local executable catalog and definitions before publication.
Known singleton job/observer declarations marked lazy are initialized for this catalog scan;
unrelated lazy beans remain lazy. Declarations discovered after publication fail explicitly
instead of being silently left out. A singleton catalog row arbitrates even when there are no jobs or sessions. Incompatible live sessions, including paused/draining ones, prevent publication or entry. The system does not use retries to mask nodes missing an executable during rollout.

OVERRIDE, PRESERVE and FAIL govern definition conflicts in one atomic publication. They are explicit startup policy choices, not legacy API paths. Retirement survives restart. Reactivation is explicit and validates the installed catalog; it preserves pause state and creates a new trigger revision where appropriate.

Retirement marks the definition, disarms its trigger and cancels queued work in bounded ordered chunks. Work already granted may finish its immutable snapshot. New acceptance/claim checks the active definition under the appropriate locks. Retention is separate: retiring a definition does not erase its execution history or receipts.

Schedule changes increment schedule_revision. Firing compares that revision and the observed next_fire_at. Only a scheduled terminal completion matching the current fixed-delay revision rearms the series. Manual execution and manual retry cannot rearm it. Actor is an audit label, not the control flag for these decisions.

A positive maxConcurrentExecutions is a strict cluster ceiling on authorized grants; zero is unlimited. Job pause suppresses recurring triggers; engine/node pause suppresses new claims for that node. Existing grants remain subject to normal completion and cancellation rules.

See [catalog protocol](../04-engineering/executable-catalog.md), [data model](../06-data/data-model.md) and [configuration](../07-configuration/configuration-reference.md).
