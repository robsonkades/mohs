package io.mohs.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import io.github.robsonkades.uuidv7.UUIDv7;

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
        this.jdbcTemplate = JdbcSupport.namedTemplateWithStreamFetchSize(Objects.requireNonNull(dataSource, "dataSource"));
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public JobDefinition upsert(JobDefinition definition) {
        Objects.requireNonNull(definition, "definition");
        Timestamp now = JdbcTimestamps.toUtcTimestamp(clock.instant());

        ScheduleColumns scheduleColumns = ScheduleColumns.of(definition.schedule());
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
                .addValue("misfire", definition.misfire().name())
                .addValue("allowConcurrentExecutions", definition.allowConcurrentExecutions())
                .addValue("maxConcurrentExecutions", definition.maxConcurrentExecutions())
                .addValue("retries", definition.retries())
                .addValue("timeout", definition.timeout() == null ? null : definition.timeout().toString())
                .addValue("retryPolicy", definition.retryPolicy())
                .addValue("source", definition.source().name())
                .addValue("updatedAt", now);
        // id é gerado aqui mas só entra no INSERT — se cair no UPDATE, o
        // valor gerado fica sem uso; a linha existente mantém o id que já
        // tinha (PK estável pro ciclo de vida do job_key, nunca reescrita).
        // UUIDv7 (io.github.robsonkades:uuidv7) — mesma geração de mohs_executions.id.
        String id = UUIDv7.randomUUIDString();

        // tenta UPDATE primeiro; 0 linhas afetadas = chave nova, faz INSERT.
        // Evita o round-trip extra (e a corrida TOCTOU) de um SELECT COUNT
        // prévio pra decidir qual dos dois caminhos tomar. Isso não fecha a
        // corrida sozinho: dois nós registrando o mesmo job no boot podem os
        // dois ver 0 linhas e os dois tentar INSERT — quem perde recebe
        // DuplicateKeyException (job_key já existe, o outro venceu) e vira
        // UPDATE, não erro propagado pro bootstrap (CONC-2).
        String updateSql = """
                UPDATE mohs_job_definitions SET
                    name = :name, handler_type = :handlerType, schedule_type = :scheduleType,
                    cron_expression = :cronExpression, cron_zone = :cronZone,
                    interval_duration = :intervalDuration, interval_after_finish = :intervalAfterFinish,
                    runner = :runner, window_name = :windowName,
                    misfire = :misfire, allow_concurrent_executions = :allowConcurrentExecutions,
                    max_concurrent_executions = :maxConcurrentExecutions,
                    retries = :retries, timeout = :timeout, retry_policy = :retryPolicy,
                    source = :source, updated_at = :updatedAt
                WHERE job_key = :jobKey
                """;
        int updated = jdbcTemplate.update(updateSql, params);

        if (updated == 0) {
            try {
                jdbcTemplate.update("""
                        INSERT INTO mohs_job_definitions (
                            id, job_key, name, handler_type, schedule_type, cron_expression, cron_zone,
                            interval_duration, interval_after_finish, runner, window_name,
                            misfire, allow_concurrent_executions, max_concurrent_executions, retries,
                            timeout, retry_policy, source,
                            orphaned, paused, running_execution_count, created_at, updated_at)
                        VALUES (
                            :id, :jobKey, :name, :handlerType, :scheduleType, :cronExpression, :cronZone,
                            :intervalDuration, :intervalAfterFinish, :runner, :windowName,
                            :misfire, :allowConcurrentExecutions, :maxConcurrentExecutions, :retries,
                            :timeout, :retryPolicy, :source,
                            :orphaned, :paused, :runningExecutionCount, :createdAt, :updatedAt)
                        """, params.addValue("id", id).addValue("createdAt", now)
                                .addValue("orphaned", false).addValue("paused", false).addValue("runningExecutionCount", 0));
            } catch (DuplicateKeyException _) {
                jdbcTemplate.update(updateSql, params);
            }
        }
        return definition;
    }

    @Override
    public Optional<StoredJob> find(JobKey key) {
        Objects.requireNonNull(key, "key");
        List<String> unresolvedHandlerJobKeys = new ArrayList<>();
        // job_key é UNIQUE — no máximo uma linha.
        Optional<StoredJob> result = JdbcSupport.findOne(jdbcTemplate,
                "SELECT * FROM mohs_job_definitions WHERE job_key = :jobKey",
                new MapSqlParameterSource("jobKey", key.value()),
                rs -> mapRowOrNull(rs, 1, unresolvedHandlerJobKeys));
        markOrphanedForUnresolvedHandlers(unresolvedHandlerJobKeys);
        return result;
    }

    @Override
    public Stream<StoredJob> findAll() {
        List<String> unresolvedHandlerJobKeys = new ArrayList<>();
        return jdbcTemplate.queryForStream("SELECT * FROM mohs_job_definitions", new MapSqlParameterSource(),
                        (rs, rowNum) -> mapRowOrNull(rs, rowNum, unresolvedHandlerJobKeys))
                .filter(Objects::nonNull)
                // registrado depois do cursor interno do queryForStream — roda só
                // depois que ele já fechou, então o UPDATE de baixo nunca disputa
                // conexão com um ResultSet ainda aberto (DUP-3).
                .onClose(() -> markOrphanedForUnresolvedHandlers(unresolvedHandlerJobKeys));
    }

    /**
     * Rotina de auto-cura (DUP-3): antes, uma linha com {@code handler_type}
     * não resolvido simplesmente sumia de {@link #find}/{@link #findAll} —
     * mesmo modo de falha que a ADR-0006 já resolveu pra "annotation ausente
     * do código" (vira ORPHANED, nunca some em silêncio), só que sem essa
     * classe de falha, mais severa (a classe nem existe mais), acionar o
     * mesmo mecanismo. Chamado depois que a leitura já terminou — nunca
     * durante, pra não escrever com um cursor ainda aberto na mesma conexão.
     */
    private void markOrphanedForUnresolvedHandlers(List<String> jobKeys) {
        jobKeys.forEach(jobKey -> markOrphaned(JobKey.of(jobKey)));
    }

    @Override
    public void markOrphaned(JobKey key) {
        jdbcTemplate.update("UPDATE mohs_job_definitions SET orphaned = :orphaned WHERE job_key = :jobKey",
                new MapSqlParameterSource("jobKey", key.value()).addValue("orphaned", true));
    }

    @Override
    public void pause(JobKey key) {
        jdbcTemplate.update("UPDATE mohs_job_definitions SET paused = :paused WHERE job_key = :jobKey",
                new MapSqlParameterSource("jobKey", key.value()).addValue("paused", true));
    }

    @Override
    public void resume(JobKey key) {
        jdbcTemplate.update("UPDATE mohs_job_definitions SET paused = :paused WHERE job_key = :jobKey",
                new MapSqlParameterSource("jobKey", key.value()).addValue("paused", false));
    }

    @Override
    public void remove(JobKey key) {
        jdbcTemplate.update("DELETE FROM mohs_job_definitions WHERE job_key = :jobKey",
                new MapSqlParameterSource("jobKey", key.value()));
    }

    @Override
    public boolean tryIncrementRunningExecutions(JobKey key) {
        Objects.requireNonNull(key, "key");
        int updated = jdbcTemplate.update(
                "UPDATE mohs_job_definitions SET running_execution_count = running_execution_count + 1 "
                      + "WHERE job_key = :jobKey AND running_execution_count < max_concurrent_executions",
                new MapSqlParameterSource("jobKey", key.value()));
        return updated == 1;
    }

    @Override
    public void decrementRunningExecutions(JobKey key) {
        Objects.requireNonNull(key, "key");
        jdbcTemplate.update(
                "UPDATE mohs_job_definitions SET running_execution_count = running_execution_count - 1 "
                      + "WHERE job_key = :jobKey AND running_execution_count > 0",
                new MapSqlParameterSource("jobKey", key.value()));
    }

    private static String scheduleType(Schedule schedule) {
        return switch (schedule) {
            case CronSpec _ -> "CRON";
            case IntervalSpec _ -> "INTERVAL";
            case OnDemandSpec _ -> "ON_DEMAND";
        };
    }

    /**
     * As quatro colunas de agenda de {@code upsert} extraídas num switch
     * exaustivo único (JAVA-7) em vez de quatro testes {@code instanceof}
     * independentes — uma variante nova de {@link Schedule} quebra a
     * compilação aqui, igual já acontecia em {@link #scheduleType}, em vez
     * de virar quatro colunas {@code null} em silêncio.
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
     * {@code null} se {@code handler_type} não resolve mais (handler
     * removido do código) — linha pulada do resultado, WARN logado, e o
     * {@code job_key} anotado em {@code unresolvedHandlerJobKeys} pra
     * {@link #markOrphanedForUnresolvedHandlers} marcar depois.
     */
    private static @Nullable StoredJob mapRowOrNull(ResultSet rs, int rowNum, List<String> unresolvedHandlerJobKeys) throws SQLException {
        String jobKey = rs.getString("job_key");
        String handlerTypeName = rs.getString("handler_type");
        Class<?> handlerType;
        try {
            handlerType = Class.forName(handlerTypeName);
        } catch (ClassNotFoundException e) {
            log.warn("handler type '{}' for job '{}' not found on classpath, marking orphaned", handlerTypeName, jobKey);
            unresolvedHandlerJobKeys.add(jobKey);
            return null;
        }

        String scheduleType = rs.getString("schedule_type");
        Schedule schedule = switch (scheduleType) {
            case "CRON" -> new CronSpec(rs.getString("cron_expression"), ZoneId.of(rs.getString("cron_zone")));
            case "INTERVAL" -> new IntervalSpec(Duration.parse(rs.getString("interval_duration")), rs.getBoolean("interval_after_finish"));
            case "ON_DEMAND" -> new OnDemandSpec();
            default -> throw new IllegalStateException("unknown schedule_type '" + scheduleType + "' for job '" + jobKey + "'");
        };

        String timeoutValue = rs.getString("timeout");
        Duration timeout = timeoutValue == null ? null : Duration.parse(timeoutValue);

        JobDefinition definition = new JobDefinition(
                JobKey.of(jobKey), rs.getString("name"), handlerType, schedule,
                rs.getString("runner"), rs.getString("window_name"),
                Misfire.valueOf(rs.getString("misfire")), rs.getBoolean("allow_concurrent_executions"),
                rs.getInt("max_concurrent_executions"),
                rs.getInt("retries"), timeout, rs.getString("retry_policy"),
                DefinitionSource.valueOf(rs.getString("source")));

        return new StoredJob(definition, rs.getBoolean("orphaned"), rs.getBoolean("paused"), rs.getInt("running_execution_count"));
    }
}
