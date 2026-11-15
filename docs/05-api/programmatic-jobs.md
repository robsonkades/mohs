# Programmatic jobs

Status: Active · Last Reviewed: 2026-09-10 · Source of Truth: Repository

Register executable code once per application context, then submit data through `Mohs`.
The same executable serves any number of customers, requests and recurring schedules.
This API requires the existing Java 25 / Spring Boot 4 starter and an installed schema;
follow [Getting started](../01-overview/getting-started.md) for the dependency, DataSource and dialect.

## One-time application setup

The following are application types in `com.example.tax`, each in its own file.
`TaxCalculator` represents your existing Spring service implementing tax calculation; its
implementation and dependencies belong to your application. Mohs needs only its invocation contract.

```java
package com.example.tax;

public record TaxInput(String companyId, int year, boolean includeAdjustments) {}
```

```java
package com.example.tax;

// Implement this interface as a Spring bean using your application's tax logic.
public interface TaxCalculator {
    void calculate(TaxInput input);
}
```

```java
package com.example.tax;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import io.mohs.core.definition.JobRegistration;
import io.mohs.core.job.JobRef;

@Configuration(proxyBeanMethods = false)
public class TaxJobs {
    public static final JobRef<TaxInput> CALCULATE_TAX =
            JobRef.of("calculate-tax", TaxInput.class);

    @Bean
    JobRegistration<TaxInput> calculateTax(TaxCalculator calculator) {
        return JobRegistration.of(CALCULATE_TAX,
                (input, context) -> calculator.calculate(input));
    }
}
```

Spring discovers this singleton declaration before workers start. It registers the action locally and
stores its stable executable identity. No execution or recurring trigger is created. Every worker
must declare the same executable. Missing code for an active persisted binding fails startup with
the affected IDs. Do submissions after singleton initialization, for example from an
`ApplicationRunner`, a service handling a request, or another job; constructors and `@PostConstruct`
are too early.

The action can use `context` for cancellation and the existing execution context facilities.
Calling an injected service retains its Spring proxy and advice. The lambda itself is never serialized.
A worker reloads persisted input and dispatches by the stable executable key to its local action.
Never capture a company's per-submission parameters in the action: put them in `TaxInput`.

## Per-submission application code

This service uses that setup unchanged for both Company A and Company B:

```java
package com.example.tax;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Service;
import io.mohs.core.Mohs;
import io.mohs.core.SubmissionOptions;
import io.mohs.core.event.Enqueued;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.Priority;
import io.mohs.core.schedule.CronSpec;
import io.mohs.core.schedule.RecurringSchedule;
import static com.example.tax.TaxJobs.CALCULATE_TAX;

@Service
public class TaxSubmissions {
    private final Mohs mohs;

    public TaxSubmissions(Mohs mohs) { this.mohs = mohs; }

    public List<Enqueued> runBothCompanies() {
        return List.of(
                mohs.enqueue(CALCULATE_TAX, new TaxInput("company-a", 2026, true)),
                mohs.enqueue(CALCULATE_TAX, new TaxInput("company-b", 2025, false)));
    }

    public Enqueued runLater(TaxInput input, Instant when) {
        return mohs.scheduleAt(CALCULATE_TAX, input, when);
    }

    public Enqueued runAfterTenMinutes(TaxInput input) {
        return mohs.scheduleAfter(CALCULATE_TAX, input, Duration.ofMinutes(10));
    }

    public List<RecurringSchedule> scheduleBothCompanies() {
        ZoneId zone = ZoneId.of("America/Sao_Paulo");
        return List.of(
                mohs.scheduleRecurring("tax-company-a", CALCULATE_TAX,
                        new TaxInput("company-a", 2026, true),
                        new CronSpec("0 0 9 * * MON", zone)),
                mohs.scheduleRecurring("tax-company-b", CALCULATE_TAX,
                        new TaxInput("company-b", 2025, false),
                        new CronSpec("0 0 10 * * TUE", zone)));
    }

    public Enqueued runUrgently(TaxInput input, String requestId) {
        return mohs.enqueue(CALCULATE_TAX, input, SubmissionOptions.defaults()
                .withPriority(Priority.HIGH)
                .withActor("tax-service")
                .withIdempotencyKey(requestId));
    }

    public Optional<Execution> track(Enqueued receipt) {
        return mohs.findExecution(receipt.executionId());
    }
}
```

| Call | What it creates | When it becomes eligible |
| --- | --- | --- |
| `enqueue` | One execution and its queue entry | After commit, due immediately |
| `scheduleAt` | One execution and its queue entry | After commit, at the specified instant; a past instant is already due |
| `scheduleAfter` | One execution and its queue entry | At Mohs's clock plus a nonnegative delay; zero means due now |
| `scheduleRecurring` | One named recurring schedule and its input snapshot | After commit, at its next cron/interval firing |

Eligibility is subject to the running engine, workers, policies and polling. No method executes the
action inline or promises an exact wall-clock start. Submissions join the application's transaction:
a returned receipt is provisional until the enclosing commit; rollback removes all new work.
Execution events are published after commit.

A named recurrence does not return an execution ID because its future occurrences each get their own.
Use `recurrence.key()` with `mohs.findJob`, `pause`, `resume`, `reschedule` and `remove`.
Filter execution history by that key using the existing `ExecutionQuery`.
`RecurringSchedule.nextFireAt()` is nullable and a snapshot, as are its recurrence and pause state.

Repeating `scheduleRecurring` with the same ID updates its parameters, recurrence and policy atomically.
An unchanged recurrence preserves its next firing and every update preserves the operator's pause
decision. Already accepted executions retain their original input. Changing the executable behind
an existing recurring ID is rejected; use a new ID. Different companies never repeat code registration.

Both interval modes also work:

```java
import java.time.Duration;
import io.mohs.core.schedule.IntervalSpec;

// In an application method with the same mohs instance and CALCULATE_TAX reference:
var fixedRate = new IntervalSpec(Duration.ofHours(1), false);
var fixedDelay = new IntervalSpec(Duration.ofHours(1), true);
mohs.scheduleRecurring("tax-company-a-hourly", CALCULATE_TAX,
        new TaxInput("company-a", 2026, true), fixedRate);
mohs.scheduleRecurring("tax-company-b-after-finish", CALCULATE_TAX,
        new TaxInput("company-b", 2025, false), fixedDelay);
```

The first interval firing is after the interval. Fixed delay then measures from completion.
Cron uses the existing seconds-first grammar and requires an explicit `ZoneId`.

## Optional policy without mandatory builders

Default registration needs only a reference and an action. To change reusable execution policy,
replace the return expression of the registration bean:

```java
import java.time.Duration;
import io.mohs.core.definition.JobOptions;

return JobRegistration.of(CALCULATE_TAX, (input, context) -> calculator.calculate(input))
        .withOptions(JobOptions.defaults()
                .withName("Calculate tax")
                .withRetries(3)
                .withTimeout(Duration.ofMinutes(2))
                .withMaxConcurrentExecutions(1));
```

`withRunner`, `withWindow`, `withRateLimit` and `withRetryPolicy` refer to existing named
resources, configured once through Mohs's existing integration. Unknown names fail validation
before registration or recurrence writes. The default runner is the configured default (normally
`io`); defining a runner or window is unnecessary for the common case.

| Options | Defaults | Scope |
| --- | --- | --- |
| `JobOptions` | One retry, default runner, no job-specific timeout, no window/rate limit/custom retry policy, unlimited concurrency | Reusable executable policy |
| `SubmissionOptions` | `NORMAL` priority, `application` actor, no deduplication key | One deliberate submission |
| `RecurringOptions` | Copy executable policy now, `Misfire.IGNORE`, initially unpaused | One named recurring schedule |

All options are immutable; `with...` returns a copy. `maxConcurrentExecutions(0)` means unlimited;
one prevents overlap. Ceilings apply **per job key**: each named recurrence has its own ceiling,
and standalone invocations share the executable key. Named shared rate limits retain their
existing cross-job meaning. A policy copied into a recurrence is a snapshot; later registration
changes do not rewrite existing schedules until they are explicitly updated.

For a paused recurrence with an explicit policy, use the overload:

```java
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.RecurringOptions;

var options = RecurringOptions.defaults()
        .withStartPaused(true)
        .withMisfire(Misfire.FIRE_NOW)
        .withJobOptions(JobOptions.defaults().withRetries(3).withMaxConcurrentExecutions(1));
var recurrence = mohs.scheduleRecurring("tax-company-a-review", CALCULATE_TAX,
        new TaxInput("company-a", 2026, true),
        new CronSpec("0 0 9 * * MON", ZoneId.of("America/Sao_Paulo")), options);
mohs.resume(recurrence.key());
```

An explicit `JobOptions` is a **complete replacement**, not a partial merge. `startPaused` applies
only when the recurring ID is first created. Pausing controls automatic firings; explicit submissions
remain possible. Retiring an executable requires removing its active named recurrences first.
Retiring a recurrence keeps its binding so accepted work and retained history remain resolvable.

## Inputs, errors and receipts

Use concrete JSON-compatible records or DTOs containing data. Mohs persists JSON and a stable input-contract ID. The Java class is local to each registration. Declare that identity explicitly with `JobRef.of("calculate-tax", TaxInput.class, "tax-input-v1")`, or use the two-argument factory to take the job ID as the contract.

A compatible Java class rename does not change durable input identity. Reusing the same contract ID with an incompatible format is the application's error; equality of IDs does not prove arbitrary JSON compatibility. A retained job cannot change its input contract, including after retirement. Use a new job identity for an incompatible format. Generic root types still use `Class<T>`; wrap a generic collection in an application record. Explicit Map contracts remain supported. No-input handlers use `NoInput`.

Every submission, including every batch member, validates its typed reference and a JSON round-trip before acceptance. The transaction rechecks the current active definition and input contract. JDBC does not load Java classes or serialize inputs. The dispatcher decodes with the registered local contract before invoking user code; a malformed snapshot fails without invoking the handler.

Negative delays, nonpositive timeouts, negative retry/concurrency limits, invalid resource keys, invalid cron, an on-demand recurrence and duplicate executable declarations fail validation. Technical keys are exact ASCII strings of 1–255 characters. Actor is independent audit metadata; it does not determine whether a trigger or user originated the work.

`Enqueued` is an acceptance receipt, not a future or business result. A duplicate with an unexpired `(job key, idempotency key)` returns the original receipt, including its scheduled instant and actor. It creates no execution or event even if the new input differs. Deduplication does not compare payloads. Expiration is fixed when the receipt is accepted; history retention does not expire the receipt. A configured zero receipt lifetime means unlimited retention for new receipts.

Within a host transaction, the host commit decides durability. An exception around commit can mean an unknown result; a key can reconcile it through its receipt. Do not automatically repeat an unkeyed submission or batch. See [transactions](../04-engineering/transactions.md).

## Catalog and lifecycle

Annotations and `JobRegistration` declarations produce one immutable local catalog before any worker starts. Joining workers must have the installed executable/input-contract catalog. Changing the catalog requires stopping or expiring all old sessions, including paused or draining ones. Normal heartbeats do not take the catalog lock. See [executable catalog](../04-engineering/executable-catalog.md).

Startup registration preserves retired definitions. `mohs.remove(key)` cancels their queued work and prevents future acceptance and firing; acquired handlers can finish. Retire active named recurrences before their executable. To restore a retained definition deliberately, call `mohs.reactivate(key)` with the installed/local catalog. This preserves pause, advances the revision and computes a new trigger; cancelled old occurrences stay cancelled.

The first release has one direct typed API and one canonical installer per database. No staged scheduling chain, metadata-only definition facade, typeless registration, deprecated adapter or pre-release migration chain is shipped. Fresh databases use the [canonical installation](../06-data/migrations.md).
