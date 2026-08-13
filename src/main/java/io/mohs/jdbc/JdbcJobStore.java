package io.mohs.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

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
 * {@link JobStore} sobre {@code mohs_job_definitions} (Data Mapper, PoEAA).
 * {@code updated_at}/{@code created_at} vêm do {@link Clock} injetado —
 * nunca leitura direta (regra ArchUnit de {@code io.mohs.engine}/
 * {@code io.mohs.jdbc}).
 *
 * <p>{@link NamedParameterJdbcTemplate} em vez de {@code JdbcTemplate}
 * cru: {@link #upsert} sozinho tem 16 colunas — contar {@code ?}
 * posicional contra uma lista de argumentos nessa largura é risco real de
 * bug silencioso (troca de posição não quebra a compilação nem sempre
 * falha em runtime); parâmetro nomeado (`:coluna`) elimina essa classe de
 * erro e deixa o SQL autodescritivo.
 */
public final class JdbcJobStore implements JobStore {

    private static final Logger log = LoggerFactory.getLogger(JdbcJobStore.class);

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final Clock clock;

    public JdbcJobStore(DataSource dataSource, Clock clock) {
        this.jdbcTemplate = new NamedParameterJdbcTemplate(Objects.requireNonNull(dataSource, "dataSource"));
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public JobDefinition upsert(JobDefinition definition) {
        Objects.requireNonNull(definition, "definition");
        Timestamp now = Timestamp.from(clock.instant());

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("jobKey", definition.key().value())
                .addValue("name", definition.name())
                .addValue("handlerType", definition.handlerType().getName())
                .addValue("scheduleType", scheduleType(definition.schedule()))
                .addValue("cronExpression", definition.schedule() instanceof CronSpec cron ? cron.expression() : null)
                .addValue("cronZone", definition.schedule() instanceof CronSpec cron ? cron.zone().getId() : null)
                .addValue("intervalDuration", definition.schedule() instanceof IntervalSpec interval ? interval.interval().toString() : null)
                .addValue("intervalAfterFinish", definition.schedule() instanceof IntervalSpec interval ? interval.afterFinish() : null)
                .addValue("runner", definition.runner())
                .addValue("queueName", definition.queue())
                .addValue("windowName", definition.window())
                .addValue("misfire", definition.misfire().name())
                .addValue("retries", definition.retries())
                .addValue("timeout", definition.timeout() == null ? null : definition.timeout().toString())
                .addValue("retryPolicy", definition.retryPolicy())
                .addValue("source", definition.source().name())
                .addValue("updatedAt", now);

        // tenta UPDATE primeiro; 0 linhas afetadas = chave nova, faz INSERT.
        // Evita o round-trip extra (e a corrida TOCTOU) de um SELECT COUNT
        // prévio pra decidir qual dos dois caminhos tomar.
        int updated = jdbcTemplate.update("""
                UPDATE mohs_job_definitions SET
                    name = :name, handler_type = :handlerType, schedule_type = :scheduleType,
                    cron_expression = :cronExpression, cron_zone = :cronZone,
                    interval_duration = :intervalDuration, interval_after_finish = :intervalAfterFinish,
                    runner = :runner, queue_name = :queueName, window_name = :windowName,
                    misfire = :misfire, retries = :retries, timeout = :timeout, retry_policy = :retryPolicy,
                    source = :source, updated_at = :updatedAt
                WHERE job_key = :jobKey
                """, params);

        if (updated == 0) {
            jdbcTemplate.update("""
                    INSERT INTO mohs_job_definitions (
                        job_key, name, handler_type, schedule_type, cron_expression, cron_zone,
                        interval_duration, interval_after_finish, runner, queue_name, window_name,
                        misfire, retries, timeout, retry_policy, source, orphaned, paused, created_at, updated_at)
                    VALUES (
                        :jobKey, :name, :handlerType, :scheduleType, :cronExpression, :cronZone,
                        :intervalDuration, :intervalAfterFinish, :runner, :queueName, :windowName,
                        :misfire, :retries, :timeout, :retryPolicy, :source, FALSE, FALSE, :createdAt, :updatedAt)
                    """, params.addValue("createdAt", now));
        }
        return definition;
    }

    @Override
    public Optional<StoredJob> find(JobKey key) {
        Objects.requireNonNull(key, "key");
        // job_key é PK — no máximo uma linha; ResultSetExtractor lê essa
        // linha única direto, sem passar por List/stream/findFirst.
        StoredJob result = jdbcTemplate.query(
                "SELECT * FROM mohs_job_definitions WHERE job_key = :jobKey",
                new MapSqlParameterSource("jobKey", key.value()),
                rs -> rs.next() ? mapRowOrNull(rs, 1) : null);
        return Optional.ofNullable(result);
    }

    @Override
    public Stream<StoredJob> findAll() {
        return jdbcTemplate.queryForStream(
                        "SELECT * FROM mohs_job_definitions", new MapSqlParameterSource(), JdbcJobStore::mapRowOrNull)
                .filter(Objects::nonNull);
    }

    @Override
    public void markOrphaned(JobKey key) {
        jdbcTemplate.update("UPDATE mohs_job_definitions SET orphaned = TRUE WHERE job_key = :jobKey",
                new MapSqlParameterSource("jobKey", key.value()));
    }

    @Override
    public void pause(JobKey key) {
        jdbcTemplate.update("UPDATE mohs_job_definitions SET paused = TRUE WHERE job_key = :jobKey",
                new MapSqlParameterSource("jobKey", key.value()));
    }

    @Override
    public void resume(JobKey key) {
        jdbcTemplate.update("UPDATE mohs_job_definitions SET paused = FALSE WHERE job_key = :jobKey",
                new MapSqlParameterSource("jobKey", key.value()));
    }

    @Override
    public void remove(JobKey key) {
        jdbcTemplate.update("DELETE FROM mohs_job_definitions WHERE job_key = :jobKey",
                new MapSqlParameterSource("jobKey", key.value()));
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
