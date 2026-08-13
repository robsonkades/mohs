package io.mohs.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import io.mohs.core.definition.DefinitionSource;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.CronSpec;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.core.schedule.Schedule;
import io.mohs.engine.JobStore;
import io.mohs.engine.StoredJob;

/**
 * {@link JobStore} sobre {@code job_definitions} (Data Mapper, PoEAA).
 * {@code updated_at}/{@code created_at} vêm do {@link Clock} injetado —
 * nunca leitura direta (regra ArchUnit de {@code io.mohs.engine}/
 * {@code io.mohs.jdbc}).
 */
public final class JdbcJobStore implements JobStore {

    private static final Logger log = LoggerFactory.getLogger(JdbcJobStore.class);

    private final JdbcTemplate jdbcTemplate;
    private final Clock clock;

    public JdbcJobStore(DataSource dataSource, Clock clock) {
        this.jdbcTemplate = new JdbcTemplate(Objects.requireNonNull(dataSource, "dataSource"));
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public JobDefinition upsert(JobDefinition definition) {
        Objects.requireNonNull(definition, "definition");
        String key = definition.key().value();
        Timestamp now = Timestamp.from(clock.instant());

        Integer existing = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM job_definitions WHERE job_key = ?", Integer.class, key);

        String scheduleType = scheduleType(definition.schedule());
        String cronExpression = definition.schedule() instanceof CronSpec cron ? cron.expression() : null;
        String cronZone = definition.schedule() instanceof CronSpec cron ? cron.zone().getId() : null;
        String intervalDuration = definition.schedule() instanceof IntervalSpec interval ? interval.interval().toString() : null;
        Boolean intervalAfterFinish = definition.schedule() instanceof IntervalSpec interval ? interval.afterFinish() : null;
        String timeout = definition.timeout() == null ? null : definition.timeout().toString();

        if (existing != null && existing > 0) {
            jdbcTemplate.update("""
                    UPDATE job_definitions SET
                        name = ?, handler_type = ?, schedule_type = ?, cron_expression = ?, cron_zone = ?,
                        interval_duration = ?, interval_after_finish = ?, runner = ?, queue_name = ?,
                        window_name = ?, misfire = ?, retries = ?, timeout = ?, retry_policy = ?,
                        source = ?, updated_at = ?
                    WHERE job_key = ?
                    """,
                    definition.name(), definition.handlerType().getName(), scheduleType, cronExpression, cronZone,
                    intervalDuration, intervalAfterFinish, definition.runner(), definition.queue(),
                    definition.window(), definition.misfire().name(), definition.retries(), timeout, definition.retryPolicy(),
                    definition.source().name(), now, key);
        } else {
            jdbcTemplate.update("""
                    INSERT INTO job_definitions (
                        job_key, name, handler_type, schedule_type, cron_expression, cron_zone,
                        interval_duration, interval_after_finish, runner, queue_name, window_name,
                        misfire, retries, timeout, retry_policy, source, orphaned, paused, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, FALSE, FALSE, ?, ?)
                    """,
                    key, definition.name(), definition.handlerType().getName(), scheduleType, cronExpression, cronZone,
                    intervalDuration, intervalAfterFinish, definition.runner(), definition.queue(), definition.window(),
                    definition.misfire().name(), definition.retries(), timeout, definition.retryPolicy(),
                    definition.source().name(), now, now);
        }
        return definition;
    }

    @Override
    public Optional<StoredJob> find(JobKey key) {
        Objects.requireNonNull(key, "key");
        List<@Nullable StoredJob> rows = jdbcTemplate.query(
                "SELECT * FROM job_definitions WHERE job_key = ?", JdbcJobStore::mapRowOrNull, key.value());
        return rows.stream().filter(Objects::nonNull).findFirst();
    }

    @Override
    public List<StoredJob> findAll() {
        return jdbcTemplate.query("SELECT * FROM job_definitions", JdbcJobStore::mapRowOrNull)
                .stream().filter(Objects::nonNull).toList();
    }

    @Override
    public void markOrphaned(JobKey key) {
        jdbcTemplate.update("UPDATE job_definitions SET orphaned = TRUE WHERE job_key = ?", key.value());
    }

    @Override
    public void pause(JobKey key) {
        jdbcTemplate.update("UPDATE job_definitions SET paused = TRUE WHERE job_key = ?", key.value());
    }

    @Override
    public void resume(JobKey key) {
        jdbcTemplate.update("UPDATE job_definitions SET paused = FALSE WHERE job_key = ?", key.value());
    }

    @Override
    public void remove(JobKey key) {
        jdbcTemplate.update("DELETE FROM job_definitions WHERE job_key = ?", key.value());
    }

    private static String scheduleType(Schedule schedule) {
        return switch (schedule) {
            case CronSpec cron -> "CRON";
            case IntervalSpec interval -> "INTERVAL";
            case OnDemandSpec onDemand -> "ON_DEMAND";
        };
    }

    /** {@code null} se {@code handler_type} não resolve mais (handler removido do código) — linha pulada, WARN logado. */
    private static @Nullable StoredJob mapRowOrNull(ResultSet rs, int rowNum) throws SQLException {
        String jobKey = rs.getString("job_key");
        String handlerTypeName = rs.getString("handler_type");
        Class<?> handlerType;
        try {
            handlerType = Class.forName(handlerTypeName);
        } catch (ClassNotFoundException e) {
            log.warn("handler type '{}' for job '{}' not found on classpath, skipping row", handlerTypeName, jobKey);
            return null;
        }

        Schedule schedule = switch (rs.getString("schedule_type")) {
            case "CRON" -> new CronSpec(rs.getString("cron_expression"), ZoneId.of(rs.getString("cron_zone")));
            case "INTERVAL" -> new IntervalSpec(Duration.parse(rs.getString("interval_duration")), rs.getBoolean("interval_after_finish"));
            default -> new OnDemandSpec();
        };

        String timeoutValue = rs.getString("timeout");
        Duration timeout = timeoutValue == null ? null : Duration.parse(timeoutValue);

        JobDefinition definition = new JobDefinition(
                JobKey.of(jobKey), rs.getString("name"), handlerType, schedule,
                rs.getString("runner"), rs.getString("queue_name"), rs.getString("window_name"),
                Misfire.valueOf(rs.getString("misfire")), rs.getInt("retries"), timeout, rs.getString("retry_policy"),
                DefinitionSource.valueOf(rs.getString("source")));

        return new StoredJob(definition, rs.getBoolean("orphaned"), rs.getBoolean("paused"));
    }
}
