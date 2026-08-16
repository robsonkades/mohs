package io.mohs.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
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

/**
 * {@link JobStore} sobre {@code mohs_job_definitions} (Data Mapper, PoEAA).
 * {@code updated_at}/{@code created_at} vêm do {@link Clock} injetado —
 * nunca leitura direta (regra ArchUnit de {@code io.mohs.engine}/
 * {@code io.mohs.jdbc}).
 *
 * <p>{@link NamedParameterJdbcTemplate} em vez de {@code JdbcTemplate}
 * cru: o INSERT de {@link #upsert} sozinho tem mais de vinte colunas —
 * contar {@code ?} posicional contra uma lista de argumentos nessa largura
 * é risco real de bug silencioso (troca de posição não quebra a compilação
 * nem sempre falha em runtime); parâmetro nomeado (`:coluna`) elimina essa
 * classe de erro e deixa o SQL autodescritivo.
 */
public final class JdbcJobStore implements JobStore {

    private static final Logger log = LoggerFactory.getLogger(JdbcJobStore.class);

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final Clock clock;
    private final NextFireCalculator nextFireCalculator = new NextFireCalculator();

    public JdbcJobStore(DataSource dataSource, Clock clock) {
        this.jdbcTemplate = JdbcSupport.namedTemplateWithStreamFetchSize(Objects.requireNonNull(dataSource, "dataSource"));
        this.transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        // mesmo raciocínio da DBTUNE-4 em JdbcClaimer: escrita guardada assume
        // "última escrita vence" (READ COMMITTED), não herda o default do banco.
        this.transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public JobDefinition upsert(JobDefinition definition) {
        Objects.requireNonNull(definition, "definition");
        Instant nowInstant = clock.instant();
        Timestamp now = JdbcTimestamps.toUtcTimestamp(nowInstant);

        ScheduleColumns scheduleColumns = ScheduleColumns.of(definition.schedule());
        NextFireDecision nextFire = upsertNextFire(definition, scheduleColumns, nowInstant);
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
                .addValue("startPaused", definition.startPaused())
                .addValue("allowConcurrentExecutions", definition.allowConcurrentExecutions())
                .addValue("maxConcurrentExecutions", definition.maxConcurrentExecutions())
                .addValue("retries", definition.retries())
                .addValue("timeout", definition.timeout() == null ? null : definition.timeout().toString())
                .addValue("retryPolicy", definition.retryPolicy())
                .addValue("source", definition.source().name())
                .addValue("orphaned", false)
                .addValue("retired", false)
                .addValue("updatedAt", now);
        // record pattern Write(Instant v) não casaria componente null (type pattern) — bind do record inteiro
        if (nextFire instanceof NextFireDecision.Write write) {
            params.addValue("nextFireAt", write.value() == null ? null : JdbcTimestamps.toUtcTimestamp(write.value()));
        }
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
        // orphaned/retired = FALSE mesmo em UPDATE: diferente de paused (decisão de
        // operador, upsert nunca toca), os dois são consequência de "a fonte sumiu"
        // (anotação removida / Mohs.remove) — o próprio upsert acontecer já é prova
        // de que uma fonte real (scan ou Mohs.define) quer este job de novo, então
        // define() depois de remove() ressuscita a definição com o histórico intacto.
        // Duas variantes por Preserve/Write (mesmo precedente do par COMPLETE_CAS/
        // COMPLETE_CAS_FENCED em JdbcExecutionStore): preservar next_fire_at é NÃO
        // escrever a coluna — reescrever o valor lido seria lost update (DDIA cap. 7)
        // contra o CAS do disparo e o rearme guardado da conclusão (review ADR-0035).
        String updateSql = """
                UPDATE mohs_job_definitions SET
                    name = :name, handler_type = :handlerType, schedule_type = :scheduleType,
                    cron_expression = :cronExpression, cron_zone = :cronZone,
                    interval_duration = :intervalDuration, interval_after_finish = :intervalAfterFinish,
                    runner = :runner, window_name = :windowName,
                    misfire = :misfire, start_paused = :startPaused,
                    allow_concurrent_executions = :allowConcurrentExecutions,
                    max_concurrent_executions = :maxConcurrentExecutions,
                    retries = :retries, timeout = :timeout, retry_policy = :retryPolicy,
                    source = :source, orphaned = :orphaned, retired = :retired,
                    %supdated_at = :updatedAt
                WHERE job_key = :jobKey
                """.formatted(nextFire instanceof NextFireDecision.Write ? "next_fire_at = :nextFireAt, " : "");
        int updated = jdbcTemplate.update(updateSql, params);

        if (updated == 0) {
            // linha nova: Preserve é impossível aqui por construção (o snapshot viu a
            // linha existir, e linha nunca é apagada — remove é soft-retire); o guard
            // recompõe o valor inicial pra falha desse invariante não virar erro de
            // bind críptico num INSERT que, de todo jeito, cria a linha do zero.
            if (!params.hasValue("nextFireAt")) {
                Instant initial = initialNextFire(definition.schedule(), nowInstant);
                params.addValue("nextFireAt", initial == null ? null : JdbcTimestamps.toUtcTimestamp(initial));
            }
            try {
                jdbcTemplate.update("""
                        INSERT INTO mohs_job_definitions (
                            id, job_key, name, handler_type, schedule_type, cron_expression, cron_zone,
                            interval_duration, interval_after_finish, runner, window_name,
                            misfire, start_paused, allow_concurrent_executions, max_concurrent_executions, retries,
                            timeout, retry_policy, source,
                            orphaned, retired, paused, running_execution_count, next_fire_at, created_at, updated_at)
                        VALUES (
                            :id, :jobKey, :name, :handlerType, :scheduleType, :cronExpression, :cronZone,
                            :intervalDuration, :intervalAfterFinish, :runner, :windowName,
                            :misfire, :startPaused, :allowConcurrentExecutions, :maxConcurrentExecutions, :retries,
                            :timeout, :retryPolicy, :source,
                            :orphaned, :retired, :paused, :runningExecutionCount, :nextFireAt, :createdAt, :updatedAt)
                        """, params.addValue("id", id).addValue("createdAt", now)
                                // ADR-0037: startPaused só vale no nascimento — o UPDATE nunca toca paused
                                .addValue("paused", definition.startPaused()).addValue("runningExecutionCount", 0));
            } catch (DuplicateKeyException _) {
                // o outro nó venceu o INSERT: re-decide contra a linha que agora existe —
                // uma decisão tomada contra um snapshot morre com o snapshot (agenda igual
                // vira Preserve, e o re-UPDATE não toca next_fire_at). Termina em um nível:
                // a linha existe e o UPDATE da segunda passada sempre a encontra.
                return upsert(definition);
            }
        }
        return definition;
    }

    /**
     * O estado inicial do trigger é do upsert (ADR-0035): agenda nova ou
     * alterada recalcula ({@code cron.next(now)}; intervalo →
     * {@code now + interval} — primeiro disparo de fixed-delay ancora na
     * definição, não há "fim anterior"); agenda inalterada preserva —
     * senão todo redeploy de um job {@code every PT30M} empurraria o
     * disparo para sempre. Cron irrealizável falha AQUI
     * ({@code IllegalArgumentException} do {@link NextFireCalculator}) —
     * fail-fast na definição em vez de falhar todo tick.
     *
     * <p>Preservar é {@link NextFireDecision.Preserve não escrever a
     * coluna}, nunca reescrever o valor lido: entre o snapshot e o UPDATE,
     * o CAS do disparo pode avançar o trigger e a conclusão pode rearmar a
     * corrente — reescrever o valor observado regrediria a série (lost
     * update, DDIA cap. 7) e re-dispararia lote já materializado. As
     * escritas que restam são deliberadas: agenda alterada sobrescreve
     * (reconfiguração explícita vence disparo concorrente) e a cura de
     * {@code NULL} corre apenas contra o rearme da conclusão, com valores
     * quase idênticos (trigger desarmado não é candidato do CAS de
     * disparo).
     *
     * <p>Cura de {@code NULL} em agenda recorrente inalterada (coluna nova
     * em base velha; corrente fixed-delay cuja conclusão nunca rearmou):
     * para {@code afterFinish}, só sem ocorrência viva do scheduler —
     * armar com a corrente viva criaria a sobreposição que fixed-delay
     * promete não ter.
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

    /** O veredito de {@link #upsertNextFire}: {@code Write} entra no UPDATE/INSERT; {@code Preserve} deixa a coluna fora do statement (ver o comentário sobre lost update em {@link #upsert}). */
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
        return JdbcSupport.findOne(jdbcTemplate, """
                SELECT schedule_type, cron_expression, cron_zone, interval_duration, interval_after_finish, next_fire_at
                FROM mohs_job_definitions WHERE job_key = :jobKey
                """,
                new MapSqlParameterSource("jobKey", key.value()),
                rs -> {
                    Timestamp nextFire = rs.getTimestamp("next_fire_at");
                    return new TriggerSnapshot(rs.getString("schedule_type"), rs.getString("cron_expression"),
                            rs.getString("cron_zone"), rs.getString("interval_duration"),
                            rs.getObject("interval_after_finish", Boolean.class),
                            nextFire == null ? null : JdbcTimestamps.fromUtcTimestamp(nextFire));
                })
                .orElse(null);
    }

    private boolean hasLiveSchedulerOccurrence(JobKey key) {
        Integer live = jdbcTemplate.queryForObject("""
                SELECT COUNT(*) FROM mohs_executions
                WHERE job_key = :jobKey AND actor = :actor AND state IN ('ENQUEUED', 'RETRY_SCHEDULED', 'RUNNING')
                """,
                new MapSqlParameterSource("jobKey", key.value()).addValue("actor", Execution.SCHEDULER_ACTOR),
                Integer.class);
        return live != null && live > 0;
    }

    /** A agenda persistida + {@code next_fire_at} de uma linha existente — o que {@link #upsertNextFire} compara pra decidir preservar × recalcular. */
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
        // job_key é UNIQUE — no máximo uma linha. retired fora de toda leitura
        // (parâmetro, não literal: BIT do SQL Server não aceita FALSE): job
        // aposentado não existe pra fachada/claim, só a linha fica pela FK.
        Optional<StoredJob> result = JdbcSupport.findOne(jdbcTemplate,
                "SELECT * FROM mohs_job_definitions WHERE job_key = :jobKey AND retired = :retired",
                new MapSqlParameterSource("jobKey", key.value()).addValue("retired", false),
                rs -> mapRowOrNull(rs, 1, unresolvedHandlerJobKeys));
        markOrphanedForUnresolvedHandlers(unresolvedHandlerJobKeys);
        return result;
    }

    @Override
    public Stream<StoredJob> findAll() {
        return queryForJobStream("SELECT * FROM mohs_job_definitions WHERE retired = :retired",
                new MapSqlParameterSource("retired", false));
    }

    @Override
    public Stream<StoredJob> findAllAnnotationSourced() {
        return queryForJobStream("SELECT * FROM mohs_job_definitions WHERE source = :source AND retired = :retired",
                new MapSqlParameterSource("source", DefinitionSource.ANNOTATION.name()).addValue("retired", false));
    }

    /**
     * Teto em Java sobre o cursor ({@code Stream.limit}), não
     * {@code LIMIT} SQL — dispensa acoplar este store ao {@code JdbcDialect}
     * por uma tabela pequena por natureza (definições, não execuções); o
     * cursor lê no máximo um fetch batch além do teto. Sem índice em
     * {@code next_fire_at} pelo mesmo motivo — DBTUNE com número próprio
     * se um harness mostrar que importa (ADR-0035).
     */
    @Override
    public List<StoredJob> findDueRecurring(Instant now, int limit) {
        Objects.requireNonNull(now, "now");
        try (Stream<StoredJob> due = queryForJobStream("""
                SELECT * FROM mohs_job_definitions
                WHERE retired = :retired AND paused = :paused AND orphaned = :orphaned
                  AND next_fire_at IS NOT NULL AND next_fire_at <= :now
                ORDER BY next_fire_at
                """,
                new MapSqlParameterSource("retired", false).addValue("paused", false).addValue("orphaned", false)
                        .addValue("now", JdbcTimestamps.toUtcTimestamp(now)))) {
            return due.limit(limit).toList();
        }
    }

    @Override
    public void armNextFire(JobKey key, Instant nextFireAt) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(nextFireAt, "nextFireAt");
        // guard IS NULL — Javadoc da porta: cura só arma trigger desarmado
        jdbcTemplate.update(
                "UPDATE mohs_job_definitions SET next_fire_at = :nextFireAt WHERE job_key = :jobKey AND next_fire_at IS NULL",
                new MapSqlParameterSource("jobKey", key.value())
                        .addValue("nextFireAt", JdbcTimestamps.toUtcTimestamp(nextFireAt)));
    }

    /**
     * ADR-0036: agenda e trigger recomputado aterrissam num único UPDATE —
     * mesma disciplina do upsert de agenda alterada ("reconfiguração
     * explícita vence disparo concorrente", ADR-0035). Cron irrealizável
     * falha AQUI, antes de qualquer escrita ({@code IllegalArgumentException}
     * do {@link NextFireCalculator}).
     */
    @Override
    public boolean reschedule(JobKey key, Schedule schedule) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(schedule, "schedule");
        ScheduleColumns columns = ScheduleColumns.of(schedule);
        Instant now = clock.instant();
        Instant nextFireAt = initialNextFire(schedule, now);
        int updated = jdbcTemplate.update("""
                UPDATE mohs_job_definitions SET
                    schedule_type = :scheduleType, cron_expression = :cronExpression, cron_zone = :cronZone,
                    interval_duration = :intervalDuration, interval_after_finish = :intervalAfterFinish,
                    next_fire_at = :nextFireAt, updated_at = :updatedAt
                WHERE job_key = :jobKey AND retired = :retired
                """,
                new MapSqlParameterSource()
                        .addValue("jobKey", key.value())
                        .addValue("scheduleType", scheduleType(schedule))
                        .addValue("cronExpression", columns.cronExpression())
                        .addValue("cronZone", columns.cronZone())
                        .addValue("intervalDuration", columns.intervalDuration())
                        .addValue("intervalAfterFinish", columns.intervalAfterFinish())
                        .addValue("nextFireAt", nextFireAt == null ? null : JdbcTimestamps.toUtcTimestamp(nextFireAt))
                        .addValue("updatedAt", JdbcTimestamps.toUtcTimestamp(now))
                        .addValue("retired", false));
        return updated == 1;
    }

    private Stream<StoredJob> queryForJobStream(String sql, MapSqlParameterSource params) {
        List<String> unresolvedHandlerJobKeys = new ArrayList<>();
        return jdbcTemplate.queryForStream(sql, params,
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

    /**
     * Soft-retire, nunca {@code DELETE}: um job aposentado normalmente tem
     * execuções, e a FK de {@code mohs_executions.job_key} derrubaria um
     * hard delete — e "preservar histórico" ({@code Mohs#remove}) exige a
     * linha viva de qualquer jeito. Transação própria: cancelar os
     * {@code ENQUEUED} e marcar {@code retired} é um par indivisível
     * (cancelar sem marcar deixa o job aceitando claim; marcar sem
     * cancelar deixa execuções presas em {@code ENQUEUED} pra sempre,
     * já que a claim query filtra {@code retired}).
     */
    @Override
    public void remove(JobKey key) {
        Objects.requireNonNull(key, "key");
        transactionTemplate.executeWithoutResult(_ -> {
            // os DOIS estados claimáveis (ADR-0033) — RETRY_SCHEDULED fora daqui
            // ficaria preso pra sempre: claim filtra retired, reaper só vê RUNNING
            jdbcTemplate.update("""
                    UPDATE mohs_executions SET state = 'CANCELLED'
                    WHERE job_key = :jobKey AND state IN ('ENQUEUED', 'RETRY_SCHEDULED')
                    """, new MapSqlParameterSource("jobKey", key.value()));
            jdbcTemplate.update("UPDATE mohs_job_definitions SET retired = :retired, updated_at = :now WHERE job_key = :jobKey",
                    new MapSqlParameterSource("jobKey", key.value())
                            .addValue("retired", true)
                            .addValue("now", JdbcTimestamps.toUtcTimestamp(clock.instant())));
        });
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
                Misfire.valueOf(rs.getString("misfire")), rs.getBoolean("start_paused"),
                rs.getBoolean("allow_concurrent_executions"), rs.getInt("max_concurrent_executions"),
                rs.getInt("retries"), timeout, rs.getString("retry_policy"),
                DefinitionSource.valueOf(rs.getString("source")));

        Timestamp nextFireAt = rs.getTimestamp("next_fire_at");
        return new StoredJob(definition, rs.getBoolean("orphaned"), rs.getBoolean("paused"), rs.getInt("running_execution_count"),
                nextFireAt == null ? null : JdbcTimestamps.fromUtcTimestamp(nextFireAt));
    }
}
