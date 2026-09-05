/*
 * Copyright 2026 The Mohs Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mohs.store.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ClassUtils;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import io.github.robsonkades.uuidv7.UUIDv7;

import io.mohs.core.definition.DefinitionSource;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.CronSpec;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.core.schedule.Schedule;
import io.mohs.core.execution.Execution;
import io.mohs.engine.JobStore;
import io.mohs.engine.NextFireCalculator;
import io.mohs.engine.StoredJob;
import io.mohs.store.jdbc.delegate.JdbcDelegate;

/**
 * {@link JobStore} over {@code mohs_job_definitions} (a Data Mapper, PoEAA).
 * {@code updated_at}/{@code created_at} come from the injected {@link Clock} — never a direct read
 * (a convention across {@code io.mohs.engine} and {@code io.mohs.store.jdbc}).
 *
 * <p>{@link NamedParameterJdbcTemplate} rather than a raw {@code JdbcTemplate}: {@link #upsert}'s
 * INSERT alone has more than twenty columns — counting positional {@code ?} against an argument list
 * at that width is a real risk of a silent bug (a swapped position neither breaks compilation nor
 * always fails at runtime); a named parameter ({@code :column}) removes that class of error and
 * leaves the SQL self-describing.
 */
public final class JdbcJobStore implements JobStore {

    private static final Logger log = LoggerFactory.getLogger(JdbcJobStore.class);

    /**
     * No tick ceiling here, on purpose: the loop thread's two statements against this table are the
     * definition scans, and a scan's cost is rows transferred, not a lock — measured at 2.8 s for
     * 1M definitions and 2.3 s for 1M due triggers, which a 3 s ceiling would turn into a tick that
     * dies every cycle. The facade's listing and the boot reconcile share the same cursor helper.
     */
    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final Clock clock;
    private final JdbcDelegate delegate;
    private final NextFireCalculator nextFireCalculator = new NextFireCalculator();

    /**
     * Creates a {@code JdbcJobStore} with the supplied values.
     *
     * @param dataSource the configured database connection source
     * @param clock the time source used by the component
     * @param delegate the database-specific SQL and timestamp adapter
     */
    public JdbcJobStore(DataSource dataSource, Clock clock, JdbcDelegate delegate) {
        this.jdbcTemplate = JdbcSupport.namedTemplateWithStreamFetchSize(Objects.requireNonNull(dataSource, "dataSource"));
        this.transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        // Default propagation, on purpose and unlike the claim: this template wraps one act, remove()
        // — draining the queue and marking retired as an indivisible pair — and a remove called from
        // the host's code JOINS the host's transaction when there is one, so a remove inside a
        // @Transactional rolls back with it. The isolation below therefore holds only when the
        // template opens the transaction itself (a remove with no outer transaction), where the
        // guarded DELETE assumes "last write wins" and must not inherit the database's default
        // (MySQL's is REPEATABLE READ); inside the host's transaction it is the host's level, which
        // Spring ignores here rather than validating. The other writes of this store fall in two
        // shapes, neither wrapped here: pause, resume, reschedule, markOrphaned and arming the next
        // fire are one guarded UPDATE each, in autocommit or in whatever transaction the host has
        // bound; define (upsert) is a trigger snapshot followed by one guarded UPDATE or INSERT,
        // whose lost-update protection is NOT rewriting next_fire_at on an unchanged schedule —
        // never an isolation level. Under the host's REPEATABLE READ the re-decide after a lost
        // INSERT race sees the pre-INSERT snapshot and overwrites with a value computed from the
        // same instant; benign, and documented at upsert().
        this.transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.clock = Objects.requireNonNull(clock, "clock");
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public JobDefinition upsert(JobDefinition definition) {
        Objects.requireNonNull(definition, "definition");
        Instant now = clock.instant();
        ScheduleColumns scheduleColumns = ScheduleColumns.of(definition.schedule());
        NextFireDecision nextFire = upsertNextFire(definition, scheduleColumns, now);
        MapSqlParameterSource params = upsertParams(definition, scheduleColumns, nextFire,
                JdbcTimestamps.toUtcLocalDateTime(now));

        // Try the UPDATE first; 0 rows affected means a new key, so INSERT. That avoids the extra round
        // trip (and the time-of-check/time-of-use race) of a prior SELECT COUNT to decide which of the
        // two paths to take.
        //
        // orphaned/retired = FALSE even on an UPDATE: unlike paused (an operator decision the upsert
        // never touches), both are consequences of "the source is gone" (an annotation removed, or
        // Mohs.remove) — the upsert happening at all is proof that a real source (a scan, or
        // Mohs.define) wants this job again, so define() after remove() resurrects the definition with
        // its history intact.
        int updated = jdbcTemplate.update(
                delegate.upsertJobUpdate(nextFire instanceof NextFireDecision.Write), params);
        return updated == 0 ? insertOrRedecide(definition, params, now) : definition;
    }

    /**
     * The INSERT of a new key. The UPDATE-first shape does not close the race on its own: two nodes
     * registering the same job at boot may both see 0 rows and both attempt an INSERT — the loser
     * receives a {@link DuplicateKeyException} (job_key already exists, the other won) and turns into
     * an UPDATE, not an error propagated to the bootstrap.
     */
    private JobDefinition insertOrRedecide(JobDefinition definition, MapSqlParameterSource params, Instant now) {
        // A new row: Preserve is impossible here by construction (the snapshot saw the row exist, and
        // a row is never deleted — remove is a soft retire); the guard reconstructs the initial value
        // so that a violation of that invariant does not become a cryptic bind error in an INSERT that
        // creates the row from scratch anyway.
        if (!params.hasValue("nextFireAt")) {
            params.addValue("nextFireAt",
                    JdbcTimestamps.toUtcLocalDateTimeOrNull(initialNextFire(definition.schedule(), now)));
        }
        // The id enters only the INSERT: an existing row keeps the id it already had (a stable primary
        // key for the job_key's lifetime, never rewritten).
        // UUIDv7 (io.github.robsonkades:uuidv7) — the same generation as mohs_execution.execution_id.
        params.addValue("id", UUIDv7.randomUUIDString())
                .addValue("createdAt", JdbcTimestamps.toUtcLocalDateTime(now))
                .addValue("paused", definition.startPaused());
        try {
            jdbcTemplate.update(delegate.insertJob(), params);
        } catch (DuplicateKeyException _) {
            // The other node won the INSERT: re-decide against the row that now exists — a decision
            // taken against a snapshot dies with the snapshot (an identical schedule becomes
            // Preserve, and the re-UPDATE does not touch next_fire_at). It terminates in one level:
            // the row exists, and the second pass's UPDATE always finds it.
            return upsert(definition);
        }
        return definition;
    }

    /**
     * Every column both the UPDATE and the INSERT bind. {@code nextFireAt} is bound only under
     * {@link NextFireDecision.Write}: its ABSENCE is what keeps the column out of the UPDATE
     * ({@code delegate.upsertJobUpdate(false)}), which is how {@code Preserve} preserves.
     */
    private static MapSqlParameterSource upsertParams(JobDefinition definition, ScheduleColumns scheduleColumns,
            NextFireDecision nextFire, LocalDateTime updatedAt) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("jobKey", definition.key().value())
                .addValue("name", definition.name())
                .addValue("handlerType", definition.handlerType().getName())
                .addValue("scheduleType", scheduleType(definition.schedule()))
                .addValue("cronExpression", scheduleColumns.cronExpression())
                .addValue("cronZone", scheduleColumns.cronZone())
                .addValue("intervalDuration", scheduleColumns.intervalDuration())
                .addValue("intervalAfterFinish", scheduleColumns.intervalAfterFinish())
                .addValue("runner", definition.runner())
                .addValue("windowName", definition.window())
                .addValue("rateLimit", definition.rateLimit())
                .addValue("misfire", definition.misfire().name())
                .addValue("startPaused", definition.startPaused())
                .addValue("allowConcurrentExecutions", definition.allowConcurrentExecutions())
                .addValue("maxConcurrentExecutions", definition.maxConcurrentExecutions())
                .addValue("retries", definition.retries())
                .addValue("timeout", definition.timeout() == null ? null : definition.timeout().toString())
                .addValue("retryPolicy", definition.retryPolicy())
                .addValue("source", definition.source().name())
                .addValue("orphaned", false)
                .addValue("retired", false)
                .addValue("updatedAt", updatedAt);
        // A record pattern Write(Instant v) would not match a null component (a type pattern) — bind the whole record
        if (nextFire instanceof NextFireDecision.Write write) {
            params.addValue("nextFireAt", JdbcTimestamps.toUtcLocalDateTimeOrNull(write.value()));
        }
        return params;
    }

    /**
     * The trigger's initial state belongs to the upsert: a new or altered schedule recomputes it
     * ({@code cron.next(now)}; an interval becomes {@code now + interval} — a fixed-delay job's first
     * firing anchors on the definition, there being no "previous end"), while an unchanged schedule
     * preserves it — otherwise every redeploy of an {@code every PT30M} job would push the firing
     * forward forever. An unrealisable cron fails HERE (an {@code IllegalArgumentException} from
     * {@link NextFireCalculator}) — fail fast at the definition rather than failing every tick.
     *
     * <p>Preserving means {@link NextFireDecision.Preserve not writing the column}, never rewriting the
     * value that was read: between the snapshot and the UPDATE, the firing CAS may advance the trigger
     * and a completion may rearm the chain — rewriting the observed value would regress the series (a
     * lost update, DDIA ch. 7) and re-fire an already materialised batch. The writes that remain are
     * deliberate: an altered schedule overwrites (an explicit reconfiguration beats a concurrent
     * firing), and the {@code NULL} cure races only against the completion's rearm, with near-identical
     * values (a disarmed trigger is not a candidate for the firing CAS).
     *
     * <p>The {@code NULL} cure on an unchanged recurring schedule (a new column on an old database; a
     * fixed-delay chain whose completion never rearmed): for {@code afterFinish}, only when there is no
     * live scheduler occurrence — arming with the chain alive would create the overlap fixed-delay
     * promises not to have.
     */
    private NextFireDecision upsertNextFire(JobDefinition definition, ScheduleColumns incoming, Instant now) {
        Schedule schedule = definition.schedule();
        TriggerSnapshot existing = selectTriggerSnapshot(definition.key());
        if (existing == null || !existing.sameSchedule(scheduleType(schedule), incoming)) {
            return new NextFireDecision.Write(initialNextFire(schedule, now));
        }
        if (existing.nextFireAt() != null || schedule instanceof OnDemandSpec) {
            return new NextFireDecision.Preserve();
        }
        if (schedule instanceof IntervalSpec interval && interval.afterFinish()
                && hasLiveSchedulerOccurrence(definition.key())) {
            return new NextFireDecision.Preserve();
        }
        return new NextFireDecision.Write(initialNextFire(schedule, now));
    }

    /** {@link #upsertNextFire}'s verdict: {@code Write} enters the UPDATE/INSERT; {@code Preserve} leaves the column out of the statement (see the lost-update comment in {@link #upsert}). */
    private sealed interface NextFireDecision {
        record Write(@Nullable Instant value) implements NextFireDecision {
        }

        record Preserve() implements NextFireDecision {
        }
    }

    private @Nullable Instant initialNextFire(Schedule schedule, Instant now) {
        return nextFireCalculator.nextFireAfter(schedule, now).orElse(null);
    }

    private @Nullable TriggerSnapshot selectTriggerSnapshot(JobKey key) {
        return JdbcSupport.findOne(jdbcTemplate, delegate.findTriggerSnapshot(),
                new MapSqlParameterSource("jobKey", key.value()),
                rs -> new TriggerSnapshot(rs.getString("schedule_type"), rs.getString("cron_expression"),
                        rs.getString("cron_zone"), rs.getString("interval_duration"),
                        rs.getObject("interval_after_finish", Boolean.class),
                        JdbcTimestamps.fromUtcLocalDateTimeOrNull(rs.getObject("next_fire_at", LocalDateTime.class))))
                .orElse(null);
    }

    private boolean hasLiveSchedulerOccurrence(JobKey key) {
        // A cold path (the upsert), where the per-job sweep serves
        Integer live = jdbcTemplate.queryForObject(delegate.countLiveSchedulerOccurrences(),
                new MapSqlParameterSource("jobKey", key.value()).addValue("actor", Execution.SCHEDULER_ACTOR),
                Integer.class);
        return live != null && live > 0;
    }

    /** The persisted schedule plus {@code next_fire_at} of an existing row — what {@link #upsertNextFire} compares to decide preserve versus recompute. */
    private record TriggerSnapshot(String scheduleType, @Nullable String cronExpression, @Nullable String cronZone,
            @Nullable String intervalDuration, @Nullable Boolean intervalAfterFinish, @Nullable Instant nextFireAt) {

        boolean sameSchedule(String incomingType, ScheduleColumns incoming) {
            return scheduleType.equals(incomingType)
                    && Objects.equals(cronExpression, incoming.cronExpression())
                    && Objects.equals(cronZone, incoming.cronZone())
                    && Objects.equals(intervalDuration, incoming.intervalDuration())
                    && Objects.equals(intervalAfterFinish, incoming.intervalAfterFinish());
        }
    }

    @Override
    public Optional<StoredJob> find(JobKey key) {
        Objects.requireNonNull(key, "key");
        List<String> unresolvedHandlerJobKeys = new ArrayList<>();
        // job_key is UNIQUE — at most one row. retired stays out of every read (as a parameter, not a
        // literal: SQL Server's BIT does not accept FALSE): a retired job does not exist for the facade
        // or the claim, only the row remains for the foreign key.
        Optional<StoredJob> result = JdbcSupport.findOne(jdbcTemplate, delegate.findJobByKey(),
                new MapSqlParameterSource("jobKey", key.value()).addValue("retired", false),
                rs -> mapRowOrNull(rs, unresolvedHandlerJobKeys));
        markOrphanedForUnresolvedHandlers(unresolvedHandlerJobKeys);
        return result;
    }

    @Override
    public Stream<StoredJob> findAll() {
        return queryForJobStream(delegate.findAllJobs(), new MapSqlParameterSource("retired", false));
    }

    @Override
    public Stream<StoredJob> findAllAnnotationSourced() {
        return queryForJobStream(delegate.findAllAnnotationSourcedJobs(),
                new MapSqlParameterSource("source", DefinitionSource.ANNOTATION.name()).addValue("retired", false));
    }

    /**
     * The ceiling is in the SQL ({@code LIMIT}/{@code TOP}), not a {@code Stream.limit} over the
     * cursor. It used to be the latter, on the argument that a small table does not earn a row
     * ceiling written out in four delegates — and it did not, until the index on {@code next_fire_at}
     * landed and the cost moved to the wire: in autocommit the drivers materialise the whole result
     * before the first row, so a cluster returning from downtime with 1M triggers due paid an
     * external sort of 80 MB and 2.3 s per tick to fire 500 of them. With the ceiling in the SQL the
     * planner walks the index in order and stops at the 500th row: 20 buffers, 0.1 ms, measured on
     * PostgreSQL 16 at 1M due.
     */
    @Override
    public List<StoredJob> findDueRecurring(Instant now, int limit) {
        Objects.requireNonNull(now, "now");
        try (Stream<StoredJob> due = queryForJobStream(delegate.findDueRecurringJobs(),
                new MapSqlParameterSource("retired", false).addValue("paused", false).addValue("orphaned", false)
                        .addValue("now", JdbcTimestamps.toUtcLocalDateTime(now)).addValue("limit", limit))) {
            return due.toList();
        }
    }

    @Override
    public void armNextFire(JobKey key, Instant nextFireAt) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(nextFireAt, "nextFireAt");
        // The IS NULL guard — the port's Javadoc: the cure only arms a disarmed trigger
        jdbcTemplate.update(delegate.armNextFire(),
                new MapSqlParameterSource("jobKey", key.value())
                        .addValue("nextFireAt", JdbcTimestamps.toUtcLocalDateTime(nextFireAt)));
    }

    /**
     * The schedule and the recomputed trigger land in a single UPDATE — the same discipline as the
     * upsert of an altered schedule ("an explicit reconfiguration beats a concurrent firing"). An
     * unrealisable cron fails HERE, before any write (an {@code IllegalArgumentException} from
     * {@link NextFireCalculator}).
     */
    @Override
    public boolean reschedule(JobKey key, Schedule schedule) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(schedule, "schedule");
        ScheduleColumns columns = ScheduleColumns.of(schedule);
        Instant now = clock.instant();
        Instant nextFireAt = initialNextFire(schedule, now);
        int updated = jdbcTemplate.update(delegate.rescheduleJob(),
                new MapSqlParameterSource()
                        .addValue("jobKey", key.value())
                        .addValue("scheduleType", scheduleType(schedule))
                        .addValue("cronExpression", columns.cronExpression())
                        .addValue("cronZone", columns.cronZone())
                        .addValue("intervalDuration", columns.intervalDuration())
                        .addValue("intervalAfterFinish", columns.intervalAfterFinish())
                        .addValue("nextFireAt", JdbcTimestamps.toUtcLocalDateTimeOrNull(nextFireAt))
                        .addValue("updatedAt", JdbcTimestamps.toUtcLocalDateTime(now))
                        .addValue("retired", false));
        return updated == 1;
    }

    private Stream<StoredJob> queryForJobStream(String sql, MapSqlParameterSource params) {
        List<String> unresolvedHandlerJobKeys = new ArrayList<>();
        return jdbcTemplate.queryForStream(sql, params,
                        (rs, _) -> mapRowOrNull(rs, unresolvedHandlerJobKeys))
                .filter(Objects::nonNull)
                // Registered after queryForStream's internal cursor — it runs only once that cursor has
                // closed, so the UPDATE below never contends for the connection with a ResultSet that is
                // still open.
                .onClose(() -> markOrphanedForUnresolvedHandlers(unresolvedHandlerJobKeys));
    }

    /**
     * A self-healing routine: previously, a row with an unresolved {@code handler_type} simply vanished
     * from {@link #find}/{@link #findAll} — the same failure mode already solved for "annotation absent
     * from the code" (it becomes ORPHANED, never silently disappears), except that this more severe
     * class of failure (the class no longer exists at all) did not trigger the same mechanism.
     *
     * <p>Called after the read has finished — never during it, so as not to write with a cursor still
     * open on the same connection.
     */
    private void markOrphanedForUnresolvedHandlers(List<String> jobKeys) {
        jobKeys.forEach(jobKey -> markOrphaned(JobKey.of(jobKey)));
    }

    @Override
    public void markOrphaned(JobKey key) {
        jdbcTemplate.update(delegate.markJobOrphaned(),
                new MapSqlParameterSource("jobKey", key.value()).addValue("orphaned", true));
    }

    @Override
    public void pause(JobKey key) {
        jdbcTemplate.update(delegate.setJobPaused(),
                new MapSqlParameterSource("jobKey", key.value()).addValue("paused", true));
    }

    @Override
    public void resume(JobKey key) {
        jdbcTemplate.update(delegate.setJobPaused(),
                new MapSqlParameterSource("jobKey", key.value()).addValue("paused", false));
    }

    /**
     * A soft retire, never a {@code DELETE}: "preserve history" ({@code Mohs#remove}) requires the job's
     * row to stay alive anyway — history in {@code mohs_execution} keeps pointing at it.
     *
     * <p>Its own transaction: draining the QUEUE and marking {@code retired} are an indivisible pair
     * (draining without marking leaves the firer materialising new occurrences; marking without
     * draining would leave entries stuck in {@code mohs_ready} forever — the queue does not filter
     * retired, this drain does).
     *
     * <p>A lease in flight ends on its own: the terminal outcome writes normally, and a post-retire
     * retry dies in failBeforeDispatch — it is {@link #find}'s {@code retired} filter that kills it (the
     * Engine's snapshot heal queries fresh, and a retired job must stay invisible there; removing that
     * filter would resurrect queue zombies).
     */
    @Override
    public void remove(JobKey key) {
        Objects.requireNonNull(key, "key");
        transactionTemplate.executeWithoutResult(_ -> {
            List<String> drained = drainQueue(key);
            // ONE clock read for the whole logical act: two calls to clock.instant() in the same
            // transaction used to write two different instants
            LocalDateTime now = JdbcTimestamps.toUtcLocalDateTime(clock.instant());
            countCancelledMembers(cancelDrained(drained, now));
            markRetired(key, now);
        });
    }

    /** The queue entries this transaction took — the set the cancellation is allowed to touch. */
    private List<String> drainQueue(JobKey key) {
        List<String> queued = jdbcTemplate.queryForList(delegate.findQueuedExecutionIdsByJob(),
                new MapSqlParameterSource("jobKey", key.value()), String.class);
        List<String> drained = new ArrayList<>(queued.size());
        for (String executionId : queued) {
            // The DELETE decides the race against a concurrent claim: 0 rows means the claim took the
            // entry and its lease ends on its own — never CANCELLED over a claimed execution (a
            // subquery predicate evaluates against a snapshot and serialises nothing; the DELETE's
            // row lock serialises — DDIA ch. 7). A cold path: N round trips per retirement do not
            // matter
            if (jdbcTemplate.update(delegate.deleteReadyById(),
                    new MapSqlParameterSource("executionId", executionId)) == 1) {
                drained.add(executionId);
            }
        }
        return drained;
    }

    /**
     * Cancels the drained executions and returns how many members each batch lost — cancelling is
     * terminal, and without counting, the member disappears from the batch and {@code pending} never
     * reaches zero: the batch stays open forever, with no reconciliation sweep to cure it.
     *
     * <p>Grouped over the set the drain ACTUALLY took (not over a pre-drain snapshot): whatever a
     * concurrent claim tore out of the queue will run and count in its own completion — counting it here
     * would be the double counting the old era accepted as a window.
     *
     * <p>Chunking is mandatory: {@code drained} is EVERYTHING that was in this job's queue, and an
     * on-demand job with 3,000 queued executions (a burst, or a queue dammed by a pause) blew past SQL
     * Server's ~2100 parameters. The whole transaction rolled back and the job was NOT retired — a
     * permanent failure, not a transient one.
     *
     * <p>The count ACCUMULATES per batch across the chunks and is only applied afterwards: a per-chunk
     * increment would touch the same {@code mohs_batches} in an order dictated by the arrival order of
     * entries in the queue, and two concurrent removes over overlapping batches would cross — the
     * stable order the {@link SortedMap} returns is what keeps the pair of UPDATEs free of deadlock.
     */
    private SortedMap<String, Integer> cancelDrained(List<String> drained, LocalDateTime now) {
        SortedMap<String, Integer> cancelledPerBatch = new TreeMap<>();
        for (List<String> chunk : JdbcSupport.chunksOf(drained)) {
            jdbcTemplate.update(delegate.cancelDrainedExecutions(),
                    new MapSqlParameterSource("ids", chunk).addValue("now", now));
            for (Map<String, Object> member : drainedBatchMembers(chunk)) {
                cancelledPerBatch.merge((String) member.get("batch_id"),
                        ((Number) member.get("pending")).intValue(), Integer::sum);
            }
        }
        return cancelledPerBatch;
    }

    private List<Map<String, Object>> drainedBatchMembers(List<String> drained) {
        return jdbcTemplate.queryForList(delegate.drainedBatchMembers(), new MapSqlParameterSource("ids", drained));
    }

    /**
     * A stable order by {@code batch_id} (the caller's {@link SortedMap} order): two concurrent removes
     * touching the same batches do not cross.
     *
     * <p>{@code mohs_batches} SQL lives here rather than in {@code BatchStore} because the increment is
     * done in bulk ({@code + :n} per batch, not N calls), a shape the port does not have. If a third
     * case appears, it becomes {@code incrementFailedBy} on the port.
     */
    private void countCancelledMembers(SortedMap<String, Integer> cancelledPerBatch) {
        for (Map.Entry<String, Integer> batch : cancelledPerBatch.entrySet()) {
            jdbcTemplate.update(delegate.countCancelledBatchMembers(),
                    new MapSqlParameterSource("id", batch.getKey()).addValue("pending", batch.getValue()));
        }
    }

    private void markRetired(JobKey key, LocalDateTime now) {
        jdbcTemplate.update(delegate.retireJob(),
                new MapSqlParameterSource("jobKey", key.value())
                        .addValue("retired", true)
                        .addValue("now", now));
    }

    private static String scheduleType(Schedule schedule) {
        return switch (schedule) {
            case CronSpec _ -> "CRON";
            case IntervalSpec _ -> "INTERVAL";
            case OnDemandSpec _ -> "ON_DEMAND";
        };
    }

    /**
     * The upsert's four schedule columns extracted into a single exhaustive switch, rather than four
     * independent {@code instanceof} tests — a new {@link Schedule} variant breaks compilation here, as
     * it already did in {@link #scheduleType}, instead of silently becoming four {@code null} columns.
     */
    private record ScheduleColumns(@Nullable String cronExpression, @Nullable String cronZone,
                                    @Nullable String intervalDuration, @Nullable Boolean intervalAfterFinish) {

        static ScheduleColumns of(Schedule schedule) {
            return switch (schedule) {
                case CronSpec cron -> new ScheduleColumns(cron.expression(), cron.zone().getId(), null, null);
                case IntervalSpec interval -> new ScheduleColumns(null, null, interval.interval().toString(), interval.afterFinish());
                case OnDemandSpec _ -> new ScheduleColumns(null, null, null, null);
            };
        }
    }

    /**
     * {@code null} if {@code handler_type} no longer resolves (the handler was removed from the code) —
     * the row is skipped from the result, a WARN is logged, and the {@code job_key} is noted in
     * {@code unresolvedHandlerJobKeys} for {@link #markOrphanedForUnresolvedHandlers} to mark
     * afterwards.
     */
    private static @Nullable StoredJob mapRowOrNull(ResultSet rs, List<String> unresolvedHandlerJobKeys) throws SQLException {
        String jobKey = rs.getString("job_key");
        String handlerTypeName = rs.getString("handler_type");
        Class<?> handlerType;
        try {
            handlerType = ClassUtils.forName(handlerTypeName, ClassUtils.getDefaultClassLoader());
        } catch (ClassNotFoundException e) {
            log.warn("handler type '{}' for job '{}' not found on classpath, marking orphaned", handlerTypeName, jobKey);
            unresolvedHandlerJobKeys.add(jobKey);
            return null;
        }

        Schedule schedule = readSchedule(rs, jobKey);
        String timeoutValue = rs.getString("timeout");
        Duration timeout = timeoutValue == null ? null : Duration.parse(timeoutValue);
        JobDefinition definition = new JobDefinition(
                JobKey.of(jobKey), rs.getString("name"), handlerType, schedule,
                rs.getString("runner"), rs.getString("window_name"), rs.getString("rate_limit"),
                Misfire.valueOf(rs.getString("misfire")), rs.getBoolean("start_paused"),
                rs.getBoolean("allow_concurrent_executions"), rs.getInt("max_concurrent_executions"),
                rs.getInt("retries"), timeout, rs.getString("retry_policy"),
                DefinitionSource.valueOf(rs.getString("source")));
        return new StoredJob(definition, rs.getBoolean("orphaned"), rs.getBoolean("paused"),
                JdbcTimestamps.fromUtcLocalDateTimeOrNull(rs.getObject("next_fire_at", LocalDateTime.class)));
    }

    /** The inverse of {@link ScheduleColumns#of}: the four schedule columns back into the {@link Schedule} variant {@code schedule_type} names. */
    private static Schedule readSchedule(ResultSet rs, String jobKey) throws SQLException {
        String scheduleType = rs.getString("schedule_type");
        return switch (scheduleType) {
            case "CRON" -> new CronSpec(rs.getString("cron_expression"), ZoneId.of(rs.getString("cron_zone")));
            case "INTERVAL" -> new IntervalSpec(Duration.parse(rs.getString("interval_duration")), rs.getBoolean("interval_after_finish"));
            case "ON_DEMAND" -> new OnDemandSpec();
            default -> throw new IllegalStateException("unknown schedule_type '" + scheduleType + "' for job '" + jobKey + "'");
        };
    }
}
