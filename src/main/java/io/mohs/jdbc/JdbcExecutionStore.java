package io.mohs.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import tools.jackson.databind.ObjectMapper;

import io.mohs.core.execution.Attempt;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.engine.ExecutionStore;
import io.mohs.engine.JobStore;

/**
 * {@link ExecutionStore} sobre {@code mohs_executions}/{@code mohs_attempts}
 * (Data Mapper, PoEAA). Payload serializado via {@link ObjectMapper}
 * (Jackson, dependência obrigatória do módulo) — nunca campo de
 * {@link Execution}, só grava/carrega na borda JDBC.
 *
 * <p>{@link #insert} é o "insert do terminal" da cláusula 4 da ADR-0003:
 * usa o mesmo {@code DataSource} do chamador via
 * {@link NamedParameterJdbcTemplate}, que participa da transação Spring
 * já ativa quando existe uma — nenhuma transação própria é aberta aqui.
 */
public final class JdbcExecutionStore implements ExecutionStore {

    /** Bem abaixo do teto de 2100 parâmetros do SQL Server pro `IN (:ids)` de {@link #findByIds} (DB-11). Package-private pro teste de fronteira. */
    static final int MAX_IDS_PER_QUERY = 1000;

    private static final String EXECUTION_COLUMNS = "id, job_key, state, scheduled_at, fired_at, actor";

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final Clock clock;
    private final ObjectMapper objectMapper;

    public JdbcExecutionStore(DataSource dataSource, Clock clock, ObjectMapper objectMapper) {
        this.jdbcTemplate = JdbcSupport.namedTemplateWithStreamFetchSize(Objects.requireNonNull(dataSource, "dataSource"));
        this.clock = Objects.requireNonNull(clock, "clock");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
    }

    @Override
    public Execution insert(Execution execution, Object payload) {
        Objects.requireNonNull(execution, "execution");
        Objects.requireNonNull(payload, "payload");
        if (!execution.attempts().isEmpty()) {
            throw new IllegalArgumentException("execution.attempts() must be empty on insert — nothing has run yet");
        }

        String payloadJson = objectMapper.writeValueAsString(payload);

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", execution.id().value())
                .addValue("jobKey", execution.jobKey().value())
                .addValue("state", execution.state().name())
                .addValue("scheduledAt", JdbcTimestamps.toUtcTimestamp(execution.scheduledAt()))
                .addValue("firedAt", execution.firedAt() == null ? null : JdbcTimestamps.toUtcTimestamp(execution.firedAt()))
                .addValue("actor", execution.actor())
                .addValue("payload", payloadJson)
                .addValue("payloadType", payload.getClass().getName())
                .addValue("createdAt", JdbcTimestamps.toUtcTimestamp(clock.instant()));

        jdbcTemplate.update("""
                INSERT INTO mohs_executions (
                    id, job_key, state, scheduled_at, fired_at, actor, payload, payload_type, created_at)
                VALUES (:id, :jobKey, :state, :scheduledAt, :firedAt, :actor, :payload, :payloadType, :createdAt)
                """, params);

        return execution;
    }

    @Override
    public void markFired(ExecutionId id, Instant firedAt) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(firedAt, "firedAt");
        jdbcTemplate.update("UPDATE mohs_executions SET fired_at = :firedAt WHERE id = :id",
                new MapSqlParameterSource()
                        .addValue("firedAt", JdbcTimestamps.toUtcTimestamp(firedAt))
                        .addValue("id", id.value()));
    }

    /**
     * CAS primeiro (ADR-0024): só grava o {@link Attempt} e libera a vaga de
     * concorrência (ADR-0025) se a transição de estado realmente ocorreu —
     * uma conclusão concorrente (ex.: dispatch normal terminou entre o
     * reaper selecionar o candidato e chamar isto) não deixa Attempt órfão
     * de uma execução que já tinha terminado de outro jeito.
     */
    @Override
    public boolean complete(ExecutionId id, JobKey jobKey, Attempt attempt, ExecutionState newState, JobStore jobStore) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(jobKey, "jobKey");
        Objects.requireNonNull(attempt, "attempt");
        Objects.requireNonNull(newState, "newState");
        Objects.requireNonNull(jobStore, "jobStore");

        int updated = jdbcTemplate.update(
                "UPDATE mohs_executions SET state = :newState WHERE id = :id AND state = 'RUNNING'",
                new MapSqlParameterSource()
                        .addValue("newState", newState.name())
                        .addValue("id", id.value()));
        if (updated == 0) {
            return false;
        }

        jdbcTemplate.update("""
                INSERT INTO mohs_attempts (execution_id, number, started_at, finished_at, outcome, error)
                VALUES (:executionId, :number, :startedAt, :finishedAt, :outcome, :error)
                """, attemptParams(id, attempt));
        jobStore.decrementRunningExecutions(jobKey);
        return true;
    }

    /**
     * DBTUNE-14: {@link #complete} em lote — um {@code UPDATE} guardado por
     * {@code newState} (agrupa, não assume lote uniforme), uma consulta pra
     * confirmar quais ids realmente transicionaram (o {@code UPDATE} não
     * diz quais linhas casaram, só quantas) e um {@code INSERT} em lote só
     * dos {@link Attempt} confirmados — {@code jobStore.decrementRunningExecutions}
     * continua uma chamada por execução (guardada, barata, sem API de lote
     * em {@link JobStore} — não vale o acoplamento novo pra isto).
     */
    @Override
    public Set<ExecutionId> completeAll(List<ExecutionStore.CompletionRequest> requests, JobStore jobStore) {
        Objects.requireNonNull(requests, "requests");
        Objects.requireNonNull(jobStore, "jobStore");
        if (requests.isEmpty()) {
            return Set.of();
        }

        Map<String, ExecutionStore.CompletionRequest> byId = requests.stream()
                .collect(Collectors.toMap(r -> r.id().value(), r -> r));
        Set<ExecutionId> completedIds = new LinkedHashSet<>();
        for (Map.Entry<ExecutionState, List<ExecutionStore.CompletionRequest>> group :
                requests.stream().collect(Collectors.groupingBy(ExecutionStore.CompletionRequest::newState)).entrySet()) {
            completedIds.addAll(transitionGroup(group.getKey(), group.getValue()));
        }
        if (completedIds.isEmpty()) {
            return completedIds;
        }

        MapSqlParameterSource[] attemptParams = completedIds.stream()
                .map(id -> attemptParams(id, byId.get(id.value()).attempt()))
                .toArray(MapSqlParameterSource[]::new);
        jdbcTemplate.batchUpdate("""
                INSERT INTO mohs_attempts (execution_id, number, started_at, finished_at, outcome, error)
                VALUES (:executionId, :number, :startedAt, :finishedAt, :outcome, :error)
                """, attemptParams);

        for (ExecutionId id : completedIds) {
            jobStore.decrementRunningExecutions(byId.get(id.value()).jobKey());
        }
        return completedIds;
    }

    /** CAS por {@code WHERE id IN (:ids) AND state = 'RUNNING'}, em lotes de {@link #MAX_IDS_PER_QUERY} (DB-11). */
    private Set<ExecutionId> transitionGroup(ExecutionState newState, List<ExecutionStore.CompletionRequest> group) {
        Set<ExecutionId> confirmed = new LinkedHashSet<>();
        List<String> ids = group.stream().map(r -> r.id().value()).toList();
        for (int start = 0; start < ids.size(); start += MAX_IDS_PER_QUERY) {
            List<String> chunk = ids.subList(start, Math.min(start + MAX_IDS_PER_QUERY, ids.size()));
            jdbcTemplate.update(
                    "UPDATE mohs_executions SET state = :newState WHERE id IN (:ids) AND state = 'RUNNING'",
                    new MapSqlParameterSource().addValue("newState", newState.name()).addValue("ids", chunk));
            // o UPDATE não diz quais ids casaram, só quantos — confirma via SELECT
            // (o mesmo state que acabamos de gravar só pode ter vindo desta transição:
            // os ids do lote eram todos RUNNING momentos atrás, nesta mesma transação).
            confirmed.addAll(jdbcTemplate.query(
                    "SELECT id FROM mohs_executions WHERE id IN (:ids) AND state = :newState",
                    new MapSqlParameterSource().addValue("ids", chunk).addValue("newState", newState.name()),
                    (rs, _) -> ExecutionId.of(rs.getString("id"))));
        }
        return confirmed;
    }

    private static MapSqlParameterSource attemptParams(ExecutionId id, Attempt attempt) {
        return new MapSqlParameterSource()
                .addValue("executionId", id.value())
                .addValue("number", attempt.number())
                .addValue("startedAt", JdbcTimestamps.toUtcTimestamp(attempt.startedAt()))
                .addValue("finishedAt", attempt.finishedAt() == null ? null : JdbcTimestamps.toUtcTimestamp(attempt.finishedAt()))
                .addValue("outcome", attempt.outcome().name())
                .addValue("error", attempt.error());
    }

    @Override
    public Optional<Execution> find(ExecutionId id) {
        Objects.requireNonNull(id, "id");
        Optional<ExecutionRow> row = JdbcSupport.findOne(jdbcTemplate,
                "SELECT " + EXECUTION_COLUMNS + " FROM mohs_executions WHERE id = :id",
                new MapSqlParameterSource("id", id.value()),
                JdbcExecutionStore::mapRow);
        return row.map(r -> hydrate(r, fetchAttempts(r.id())));
    }

    /**
     * DBTUNE-3: busca as linhas em lote e SÓ DEPOIS, com o cursor da query
     * externa já fechado, busca os attempts de todas elas numa segunda
     * consulta em lote (mesmo chunking de {@link #MAX_IDS_PER_QUERY}) — não
     * mais uma query de attempts por linha dentro do row mapper, que além
     * do N+1 mantinha uma segunda conexão do pool presa enquanto a primeira
     * ainda segurava o {@code ResultSet} externo (risco real de pool
     * deadlock sob concorrência, documentado no HikariCP).
     */
    @Override
    public List<Execution> findByIds(List<ExecutionId> ids) {
        Objects.requireNonNull(ids, "ids");
        if (ids.isEmpty()) {
            return List.of();
        }
        List<String> rawIds = ids.stream().map(ExecutionId::value).toList();
        List<ExecutionRow> rows = new ArrayList<>(rawIds.size());
        for (int start = 0; start < rawIds.size(); start += MAX_IDS_PER_QUERY) {
            List<String> chunk = rawIds.subList(start, Math.min(start + MAX_IDS_PER_QUERY, rawIds.size()));
            rows.addAll(jdbcTemplate.query(
                    "SELECT " + EXECUTION_COLUMNS + " FROM mohs_executions WHERE id IN (:ids)",
                    new MapSqlParameterSource("ids", chunk),
                    (rs, _) -> mapRow(rs)));
        }
        Map<String, List<Attempt>> attemptsByExecutionId = fetchAttemptsByExecutionIds(rows.stream().map(r -> r.id().value()).toList());
        return rows.stream()
                .map(row -> hydrate(row, attemptsByExecutionId.getOrDefault(row.id().value(), List.of())))
                .toList();
    }

    /**
     * Continua N+1 de propósito, ao contrário de {@link #findByIds}
     * (DBTUNE-3): streama sobre um cursor aberto, e batchar os attempts
     * aqui exigiria materializar a tabela inteira antes da primeira linha
     * (perde o cursor) ou uma janela de chunking manual — trabalho real
     * sem caller de produção ainda para medir contra. Nada chama isto em
     * produção nesta etapa (entra no wiring REST de M3) — mede com um
     * caller de verdade antes de trocar.
     */
    @Override
    public Stream<Execution> findByJobKey(JobKey jobKey) {
        Objects.requireNonNull(jobKey, "jobKey");
        return jdbcTemplate.queryForStream(
                "SELECT " + EXECUTION_COLUMNS + " FROM mohs_executions WHERE job_key = :jobKey",
                new MapSqlParameterSource("jobKey", jobKey.value()),
                (rs, _) -> hydrateEagerly(mapRow(rs)));
    }

    /** Ver Javadoc de {@link #findByJobKey} sobre o N+1 deliberado. */
    @Override
    public Stream<Execution> findAll() {
        return jdbcTemplate.queryForStream(
                "SELECT " + EXECUTION_COLUMNS + " FROM mohs_executions",
                new MapSqlParameterSource(),
                (rs, _) -> hydrateEagerly(mapRow(rs)));
    }

    private Execution hydrateEagerly(ExecutionRow row) {
        return hydrate(row, fetchAttempts(row.id()));
    }

    private static Execution hydrate(ExecutionRow row, List<Attempt> attempts) {
        return new Execution(row.id(), row.jobKey(), row.state(), row.scheduledAt(), row.firedAt(), attempts, row.actor());
    }

    /** DBTUNE-6: colunas explícitas em vez de {@code SELECT *} — {@code payload} sozinho é JSON de tamanho arbitrário que nenhum destes métodos lê, transferido e descartado à toa em todo poll do claim. */
    private static ExecutionRow mapRow(ResultSet rs) throws SQLException {
        Timestamp firedAt = rs.getTimestamp("fired_at");
        return new ExecutionRow(
                ExecutionId.of(rs.getString("id")),
                JobKey.of(rs.getString("job_key")),
                ExecutionState.valueOf(rs.getString("state")),
                JdbcTimestamps.fromUtcTimestamp(rs.getTimestamp("scheduled_at")),
                firedAt == null ? null : JdbcTimestamps.fromUtcTimestamp(firedAt),
                rs.getString("actor"));
    }

    private List<Attempt> fetchAttempts(ExecutionId executionId) {
        return jdbcTemplate.query(
                "SELECT number, started_at, finished_at, outcome, error FROM mohs_attempts WHERE execution_id = :executionId ORDER BY number",
                new MapSqlParameterSource("executionId", executionId.value()),
                JdbcExecutionStore::mapAttemptRow);
    }

    private Map<String, List<Attempt>> fetchAttemptsByExecutionIds(List<String> executionIds) {
        if (executionIds.isEmpty()) {
            return Map.of();
        }
        List<ExecutionIdAndAttempt> rows = new ArrayList<>();
        for (int start = 0; start < executionIds.size(); start += MAX_IDS_PER_QUERY) {
            List<String> chunk = executionIds.subList(start, Math.min(start + MAX_IDS_PER_QUERY, executionIds.size()));
            rows.addAll(jdbcTemplate.query(
                    "SELECT execution_id, number, started_at, finished_at, outcome, error FROM mohs_attempts WHERE execution_id IN (:executionIds) ORDER BY execution_id, number",
                    new MapSqlParameterSource("executionIds", chunk),
                    (rs, rowNum) -> new ExecutionIdAndAttempt(rs.getString("execution_id"), mapAttemptRow(rs, rowNum))));
        }
        return rows.stream().collect(Collectors.groupingBy(
                ExecutionIdAndAttempt::executionId,
                Collectors.mapping(ExecutionIdAndAttempt::attempt, Collectors.toList())));
    }

    private static Attempt mapAttemptRow(ResultSet rs, int rowNum) throws SQLException {
        Timestamp finishedAt = rs.getTimestamp("finished_at");
        return new Attempt(
                rs.getInt("number"),
                JdbcTimestamps.fromUtcTimestamp(rs.getTimestamp("started_at")),
                finishedAt == null ? null : JdbcTimestamps.fromUtcTimestamp(finishedAt),
                ExecutionState.valueOf(rs.getString("outcome")),
                rs.getString("error"));
    }

    /** Linha crua de {@code mohs_executions}, sem attempts — hidratada por {@link #hydrate}/{@link #hydrateEagerly}. */
    private record ExecutionRow(ExecutionId id, JobKey jobKey, ExecutionState state, Instant scheduledAt, @Nullable Instant firedAt, String actor) {
    }

    private record ExecutionIdAndAttempt(String executionId, Attempt attempt) {
    }
}
