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
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;


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
import io.mohs.engine.JobBinding;
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
    private final NamedParameterJdbcTemplate tickTemplate;
    private final TransactionTemplate transactionTemplate;
    private final TransactionTemplate bindingTransaction;
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
        this.tickTemplate = JdbcSupport.tickTemplate(dataSource);
        this.transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        this.bindingTransaction = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        this.bindingTransaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_NESTED);
        // Missing-key UPDATEs must not take MySQL REPEATABLE READ gap locks: two first
        // registrations would otherwise deadlock before the unique key can choose a winner.
        // An enclosing application transaction retains its own isolation and retry policy.
        this.bindingTransaction.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.bindingTransaction.setTimeout(3);
        // Retirement joins a host transaction; standalone writes use READ_COMMITTED.
        // Registration is serialized by catalog and job locks; firing compares the schedule revision.
        this.transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.transactionTemplate.setTimeout(30);
        this.clock = Objects.requireNonNull(clock, "clock");
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public StoredJob upsertBound(JobDefinition definition, JobBinding binding) {
        Objects.requireNonNull(definition, "definition");
        Objects.requireNonNull(binding, "binding");
        if (!definition.key().equals(binding.key())) throw new IllegalArgumentException("binding key differs from definition");
        // A concurrent first insert is retried outside the savepoint: PostgreSQL must have a
        // healthy transaction before we read the winner and validate its ownership.
        for (int attempt = 0; ; attempt++) {
            try {
                return Objects.requireNonNull(bindingTransaction.execute(_ -> {
                    writeBound(definition, binding);
                    return find(binding.key()).orElseThrow();
                }));
            } catch (DuplicateKeyException conflict) {
                if (attempt >= 2) throw conflict;
            }
        }
    }

    private void writeBound(JobDefinition definition, JobBinding binding) {
        new JdbcCatalog(jdbcTemplate, delegate).requireExistingExecutable(binding.executableKey());
        definition = definition.withExecutableKey(binding.executableKey());
        if (binding.input() != null) definition = definition.withInputContract(binding.input().inputContract());
        new JdbcExecutionTransitions(jdbcTemplate, delegate).lockJobs(List.of(binding.key(), binding.executableKey()));
        if (!binding.key().equals(binding.executableKey())) {
            BindingOwner executable = lockBindingOwner(binding.executableKey()).orElse(null);
            if (executable == null || executable.retired()) {
                throw new IllegalArgumentException("no active executable '" + binding.executableKey().value() + "'");
            }
            requireInputContract(executable, definition);
            String target = executable.executableKey();
            if (target != null && !target.equals(binding.executableKey().value())) {
                throw new IllegalArgumentException("a recurrence cannot reference another recurrence");
            }
        }
        BindingOwner owner = lockBindingOwner(binding.key()).orElse(null);
        if (owner != null) {
            if (owner.retired()) throw new IllegalStateException("job is retired; use reactivate before updating it");
            requireInputContract(owner, definition);
            String previous = owner.executableKey();
            if (previous == null && !binding.key().equals(binding.executableKey())
                    || previous != null && !previous.equals(binding.executableKey().value())) {
                throw new IllegalArgumentException("job '" + binding.key().value()
                        + "' belongs to another definition or executable; choose a different schedule ID");
            }
        }
        Instant now = clock.instant();
        ScheduleColumns schedule = ScheduleColumns.of(definition.schedule());
        NextFireDecision nextFire = upsertNextFire(definition, owner, now);
        var input = binding.input();
        MapSqlParameterSource params = upsertParams(definition, schedule, nextFire, JdbcTimestamps.toUtcLocalDateTime(now))
                .addValue("executableKey", binding.executableKey().value())
                .addValue("recurringPayload", input == null ? null : input.json())
                .addValue("inputContract", definition.inputContract());
        if (owner == null) {
            // A competing insert is retried outside the savepoint, before it can change ownership.
            params.addValue("createdAt", JdbcTimestamps.toUtcLocalDateTime(now))
                    .addValue("paused", definition.startPaused());
            jdbcTemplate.update(delegate.insertJob(), params);
        } else {
            jdbcTemplate.update(delegate.upsertBoundJobUpdate(nextFire instanceof NextFireDecision.Write), params);
        }
    }

    private static void requireInputContract(BindingOwner owner, JobDefinition incoming) {
        if (!owner.inputContract().equals(incoming.inputContract())) {
            throw new IllegalArgumentException("input contract cannot be changed for a retained executable; use a new job ID");
        }
    }

    private Optional<BindingOwner> lockBindingOwner(JobKey key) {
        return JdbcSupport.findOne(jdbcTemplate, delegate.lockBindingOwner(),
                new MapSqlParameterSource("jobKey", key.value()),
                rs -> new BindingOwner(DefinitionSource.valueOf(rs.getString("source")), rs.getString("executable_key"),
                        rs.getBoolean("retired"), rs.getString("input_contract"), readSchedule(rs, key.value())));
    }

    private record BindingOwner(DefinitionSource source, @Nullable String executableKey, boolean retired,
            String inputContract, Schedule schedule) {}

    @Override
    public Optional<JobBinding> findBinding(JobKey key) {
        return JdbcJobBindings.find(jdbcTemplate, delegate, key);
    }

    @Override
    public Optional<JobKey> findExecutableKey(JobKey key) {
        return JdbcSupport.findOne(jdbcTemplate, delegate.findExecutableKey(),
                new MapSqlParameterSource("jobKey", key.value()), rs -> JobKey.of(rs.getString("executable_key")));
    }

    @Override
    public boolean hasActiveDependents(JobKey key) {
        return Integer.valueOf(1).equals(jdbcTemplate.queryForObject(delegate.hasActiveJobDependents(),
                new MapSqlParameterSource("jobKey", key.value()).addValue("retired", false), Integer.class));
    }

    @Override
    public List<JobBinding> bindings() {
        return jdbcTemplate.query(delegate.findActiveJobBindings(), new MapSqlParameterSource("retired", false),
                (rs, _) -> JdbcJobBindings.map(rs));
    }

    @Override
    public JobDefinition upsert(JobDefinition definition) {
        return writeExecutable(definition, false);
    }

    void reconcileExecutable(JobDefinition incoming, io.mohs.engine.CatalogStore.Conflict conflict) {
        BindingOwner owner = lockBindingOwner(incoming.key()).orElse(null);
        if (owner != null) {
            requireInputContract(owner, incoming);
            if (!incoming.key().value().equals(owner.executableKey())) {
                throw new IllegalStateException("executable collides with a retained named recurrence: " + incoming.key().value());
            }
            if (owner.retired()) return;
            var stored = find(incoming.key()).orElseThrow().definition();
            if (!stored.equals(incoming)) {
                if (conflict == io.mohs.engine.CatalogStore.Conflict.FAIL) {
                    throw new IllegalStateException("job '" + incoming.key().value() + "' definition diverged from the stored one");
                }
                if (conflict == io.mohs.engine.CatalogStore.Conflict.PRESERVE) return;
            }
        }
        writeExecutable(incoming, true);
    }

    private JobDefinition writeExecutable(JobDefinition definition, boolean catalogRegistration) {
        Objects.requireNonNull(definition, "definition");
        for (int attempt = 0; ; attempt++) {
            try {
                return Objects.requireNonNull(bindingTransaction.execute(_ -> {
                    if (!catalogRegistration) new JdbcCatalog(jdbcTemplate, delegate).requireExistingExecutable(definition.executableKey());
                    BindingOwner owner = lockBindingOwner(definition.key()).orElse(null);
                    if (owner != null && owner.retired()) throw new IllegalStateException("job is retired; use reactivate before updating it");
                    if (owner != null && !owner.executableKey().equals(definition.executableKey().value())) {
                        throw new IllegalArgumentException("executable identity cannot be changed for a retained job");
                    }
                    if (owner != null) requireInputContract(owner, definition);
                    Instant now = clock.instant();
                    ScheduleColumns columns = ScheduleColumns.of(definition.schedule());
                    NextFireDecision next = upsertNextFire(definition, owner, now);
                    var params = upsertParams(definition, columns, next, JdbcTimestamps.toUtcLocalDateTime(now));
                    if (owner == null) {
                        params.addValue("createdAt", JdbcTimestamps.toUtcLocalDateTime(now))
                                .addValue("paused", definition.startPaused());
                        jdbcTemplate.update(delegate.insertJob(), params);
                    } else {
                        jdbcTemplate.update(delegate.upsertJobUpdate(next instanceof NextFireDecision.Write), params);
                    }
                    return definition;
                }));
            } catch (DuplicateKeyException conflict) {
                if (attempt >= 2) throw conflict;
            }
        }
    }

    private static MapSqlParameterSource upsertParams(JobDefinition definition, ScheduleColumns scheduleColumns,
            NextFireDecision nextFire, LocalDateTime updatedAt) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("jobKey", definition.key().value())
                .addValue("name", definition.name())
                .addValue("scheduleType", scheduleType(definition.schedule()))
                .addValue("cronExpression", scheduleColumns.cronExpression())
                .addValue("cronZone", scheduleColumns.cronZone())
                .addValue("intervalMicros", scheduleColumns.intervalMicros())
                .addValue("intervalAfterFinish", scheduleColumns.intervalAfterFinish())
                .addValue("runner", definition.runner())
                .addValue("windowName", definition.window())
                .addValue("rateLimit", definition.rateLimit())
                .addValue("misfire", definition.misfire().name())
                .addValue("startPaused", definition.startPaused())
                .addValue("maxConcurrentExecutions", definition.maxConcurrentExecutions())
                .addValue("retries", definition.retries())
                .addValue("timeout_micros", definition.timeout() == null ? null : io.mohs.core.TimePrecision.micros(definition.timeout()))
                .addValue("retryPolicy", definition.retryPolicy())
                .addValue("source", definition.source().name())
                .addValue("executableKey", definition.executableKey().value())
                .addValue("recurringPayload", null)
                .addValue("inputContract", definition.inputContract())

                .addValue("retired", false)
                .addValue("updatedAt", updatedAt);
        // A record pattern Write(Instant v) would not match a null component (a type pattern) — bind the whole record
        if (nextFire instanceof NextFireDecision.Write write) {
            params.addValue("nextFireAt", JdbcTimestamps.toUtcLocalDateTimeOrNull(write.value()));
        }
        return params;
    }

    /** The locking projection is current even when a MySQL host retains an older consistent snapshot. */
    private NextFireDecision upsertNextFire(JobDefinition definition, @Nullable BindingOwner existing, Instant now) {
        return existing == null || existing.retired() || !existing.schedule().equals(definition.schedule())
                ? new NextFireDecision.Write(initialNextFire(definition.schedule(), now))
                : new NextFireDecision.Preserve();
    }

    /** {@link #upsertNextFire}'s verdict: {@code Write} enters the UPDATE/INSERT; {@code Preserve} leaves the column out of the statement (see the lost-update comment in {@link #upsert}). */
    private sealed interface NextFireDecision {
        record Write(@Nullable Instant value) implements NextFireDecision {
        }

        record Preserve() implements NextFireDecision {
        }
    }

    private @Nullable Instant initialNextFire(Schedule schedule, Instant now) {
        return nextFireCalculator.nextFireAfter(schedule, now).map(io.mohs.core.TimePrecision::eligible).orElse(null);
    }

    @Override
    public Optional<StoredJob> find(JobKey key) {
        Objects.requireNonNull(key, "key");
        // job_key is UNIQUE — at most one row. retired stays out of every read (as a parameter, not a
        // literal: SQL Server's BIT does not accept FALSE): a retired job does not exist for the facade
        // or the claim, only the row remains for the foreign key.
        Optional<StoredJob> result = JdbcSupport.findOne(jdbcTemplate, delegate.findJobByKey(),
                new MapSqlParameterSource("jobKey", key.value()).addValue("retired", false),
                JdbcJobStore::mapRow);
        return result;
    }

    @Override
    public boolean reactivate(JobKey key, io.mohs.engine.ExecutableCatalog catalog) {
        Objects.requireNonNull(key, "key");
        return Boolean.TRUE.equals(bindingTransaction.execute(_ -> {
            new JdbcCatalog(jdbcTemplate, delegate).join(catalog);
            var binding = findBinding(key).orElse(null);
            if (binding == null) return false;
            new JdbcExecutionTransitions(jdbcTemplate, delegate).lockJobs(List.of(key, binding.executableKey()));
            var stored = lockBindingOwner(key).orElse(null);
            if (stored == null || !stored.retired()) return false;
            catalog.require(binding.executableKey(), stored.inputContract());
            if (!key.equals(binding.executableKey())
                    && lockBindingOwner(binding.executableKey()).filter(owner -> !owner.retired()).isEmpty()) {
                throw new IllegalStateException("reactivate the executable before its named recurrences");
            }
            Instant now = clock.instant();
            jdbcTemplate.update("""
                    UPDATE mohs_job_definitions SET retired = :active, schedule_revision = schedule_revision + 1,
                        next_fire_at = :nextFireAt, updated_at = :now WHERE job_key = :key
                    """, new MapSqlParameterSource("key", key.value()).addValue("active", false)
                    .addValue("nextFireAt", JdbcTimestamps.toUtcLocalDateTimeOrNull(initialNextFire(stored.schedule(), now)))
                    .addValue("now", JdbcTimestamps.toUtcLocalDateTime(now)));
            return true;
        }));
    }

    @Override
    public List<StoredJob> findByKeys(java.util.Collection<JobKey> keys) {
        var found = new ArrayList<StoredJob>();
        for (var chunk : JdbcSupport.chunksOf(keys.stream().map(JobKey::value).distinct().toList())) {
            found.addAll(tickTemplate.query(delegate.findJobsByKeys(), new MapSqlParameterSource("jobKeys", chunk)
                    .addValue("retired", false), (rs, _) -> mapRow(rs)));
        }
        return List.copyOf(found);
    }

    @Override
    public Optional<Instant> nextFireAfter(Instant now) {
        Objects.requireNonNull(now, "now");
        return Optional.ofNullable(tickTemplate.queryForObject("""
                SELECT MIN(next_fire_at) AS next_fire_at FROM mohs_job_definitions
                WHERE retired = :inactive AND paused = :inactive AND next_fire_at > :now
                """, new MapSqlParameterSource("inactive", false).addValue("now", delegate.splitTimestamp(now)),
                (rs, _) -> JdbcTimestamps.fromUtcLocalDateTimeOrNull(rs.getObject("next_fire_at", LocalDateTime.class))));
    }

    @Override
    public Stream<StoredJob> findAll() {
        return queryForJobStream(delegate.findAllJobs(), new MapSqlParameterSource("retired", false));
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
    public DueJobs findDueRecurring(Instant horizon, int limit) {
        Objects.requireNonNull(horizon, "horizon");
        if (limit < 1 || limit > 1000) throw new IllegalArgumentException("due page limit must be in [1, 1000]");
        Instant sampled = JdbcDatabaseTime.now(tickTemplate, delegate);
        Instant now = io.mohs.core.TimePrecision.observed(horizon.isBefore(sampled) ? horizon : sampled);
        var jobs = tickTemplate.query(delegate.findDueRecurringJobs(),
                new MapSqlParameterSource("retired", false).addValue("paused", false)
                        .addValue("now", delegate.splitTimestamp(now)).addValue("limit", (long) limit, java.sql.Types.BIGINT),
                (rs, _) -> mapRow(rs));
        return new DueJobs(now, jobs);
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
                        .addValue("intervalMicros", columns.intervalMicros())
                        .addValue("intervalAfterFinish", columns.intervalAfterFinish())
                        .addValue("nextFireAt", JdbcTimestamps.toUtcLocalDateTimeOrNull(nextFireAt))
                        .addValue("updatedAt", JdbcTimestamps.toUtcLocalDateTime(now))
                        .addValue("retired", false));
        return updated == 1;
    }

    private Stream<StoredJob> queryForJobStream(String sql, MapSqlParameterSource params) {
        return jdbcTemplate.queryForStream(sql, params, (rs, _) -> mapRow(rs));
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
     * Retains the definition identity and atomically cancels queued work. A held job lock excludes
     * all supported writers; the current locking scan determines the batch of cancellations.
     * Acquired work remains fenced and may finish, but retirement suppresses further retries.
     * The host must use READ_COMMITTED; standalone retirement has a 30-second deadline.
     */
    @Override
    public void remove(JobKey key) {
        Objects.requireNonNull(key, "key");
        transactionTemplate.executeWithoutResult(_ -> {
            // A retained REPEATABLE_READ snapshot can miss work accepted before this job lock.
            jdbcTemplate.getJdbcTemplate().execute((org.springframework.jdbc.core.ConnectionCallback<Void>) connection -> {
                if (connection.getTransactionIsolation() != java.sql.Connection.TRANSACTION_READ_COMMITTED) {
                    throw new org.springframework.transaction.IllegalTransactionStateException(
                            "job retirement requires READ_COMMITTED; use that isolation for the host transaction or retire outside it");
                }
                return null;
            });
            if (lockBindingOwner(key).isPresent()) {
                if (hasActiveDependents(key)) {
                    throw new IllegalArgumentException("executable '" + key.value()
                            + "' still has active recurring schedules; remove those schedules first");
                }
            }
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
        // Every live writer takes this job lock first; the locking read fixes the complete set.
        for (List<String> chunk : JdbcSupport.chunksOf(queued)) {
            int deleted = jdbcTemplate.update(delegate.readyDelete(), new MapSqlParameterSource("ids", chunk));
            if (deleted != chunk.size()) {
                throw new IllegalStateException("queued work changed while the retirement held its job and work locks");
            }
        }
        return queued;
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
    private SortedMap<String, Long> cancelDrained(List<String> drained, LocalDateTime now) {
        SortedMap<String, Long> cancelledPerBatch = new TreeMap<>();
        for (List<String> chunk : JdbcSupport.chunksOf(drained)) {
            jdbcTemplate.update(delegate.cancelDrainedExecutions(),
                    new MapSqlParameterSource("ids", chunk).addValue("now", now));
            for (Map<String, Object> member : drainedBatchMembers(chunk)) {
                cancelledPerBatch.merge((String) member.get("batch_id"),
                        ((Number) member.get("pending")).longValue(), Math::addExact);
            }
        }
        return cancelledPerBatch;
    }

    private List<Map<String, Object>> drainedBatchMembers(List<String> drained) {
        return jdbcTemplate.queryForList(delegate.drainedBatchMembers(), new MapSqlParameterSource("ids", drained));
    }

    private void countCancelledMembers(SortedMap<String, Long> cancelledPerBatch) {
        new JdbcExecutionTransitions(jdbcTemplate, delegate).countFailedMembers(cancelledPerBatch);
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
                                    @Nullable Long intervalMicros, @Nullable Boolean intervalAfterFinish) {

        static ScheduleColumns of(Schedule schedule) {
            return switch (schedule) {
                case CronSpec cron -> new ScheduleColumns(cron.expression(), cron.zone().getId(), null, null);
                case IntervalSpec interval -> new ScheduleColumns(null, null, io.mohs.core.TimePrecision.micros(interval.interval()), interval.afterFinish());
                case OnDemandSpec _ -> new ScheduleColumns(null, null, null, null);
            };
        }
    }

    /** Reads metadata only; executable classes and input decoding belong to the frozen local catalog. */
    private static StoredJob mapRow(ResultSet rs) throws SQLException {
        String jobKey = rs.getString("job_key");
        Schedule schedule = readSchedule(rs, jobKey);
        Long timeoutValue = rs.getObject("timeout_micros", Long.class);
        Duration timeout = timeoutValue == null ? null : io.mohs.core.TimePrecision.duration(timeoutValue);
        JobDefinition definition = new JobDefinition(
                JobKey.of(jobKey), rs.getString("name"), JobKey.of(rs.getString("executable_key")), schedule,
                rs.getString("runner"), rs.getString("window_name"), rs.getString("rate_limit"),
                Misfire.valueOf(rs.getString("misfire")), rs.getBoolean("start_paused"), rs.getInt("max_concurrent_executions"),
                rs.getInt("retries"), timeout, rs.getString("retry_policy"),
                DefinitionSource.valueOf(rs.getString("source")), rs.getString("input_contract"));
        return new StoredJob(definition, rs.getBoolean("paused"),
                JdbcTimestamps.fromUtcLocalDateTimeOrNull(rs.getObject("next_fire_at", LocalDateTime.class)), rs.getLong("schedule_revision"));
    }

    /** The inverse of {@link ScheduleColumns#of}: the four schedule columns back into the {@link Schedule} variant {@code schedule_type} names. */
    private static Schedule readSchedule(ResultSet rs, String jobKey) throws SQLException {
        String scheduleType = rs.getString("schedule_type");
        return switch (scheduleType) {
            case "CRON" -> new CronSpec(rs.getString("cron_expression"), ZoneId.of(rs.getString("cron_zone")));
            case "INTERVAL" -> new IntervalSpec(io.mohs.core.TimePrecision.duration(rs.getLong("interval_micros")), rs.getBoolean("interval_after_finish"));
            case "ON_DEMAND" -> new OnDemandSpec();
            default -> throw new IllegalStateException("unknown schedule_type '" + scheduleType + "' for job '" + jobKey + "'");
        };
    }
}
