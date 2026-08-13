package io.mohs.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import tools.jackson.databind.ObjectMapper;

import io.mohs.core.execution.Attempt;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.engine.ExecutionStore;

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

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final Clock clock;
    private final ObjectMapper objectMapper;

    public JdbcExecutionStore(DataSource dataSource, Clock clock, ObjectMapper objectMapper) {
        this.jdbcTemplate = new NamedParameterJdbcTemplate(Objects.requireNonNull(dataSource, "dataSource"));
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
                .addValue("scheduledAt", Timestamp.from(execution.scheduledAt()))
                .addValue("firedAt", execution.firedAt() == null ? null : Timestamp.from(execution.firedAt()))
                .addValue("actor", execution.actor())
                .addValue("payload", payloadJson)
                .addValue("payloadType", payload.getClass().getName())
                .addValue("createdAt", Timestamp.from(clock.instant()));

        jdbcTemplate.update("""
                INSERT INTO mohs_executions (
                    id, job_key, state, scheduled_at, fired_at, actor, payload, payload_type, created_at)
                VALUES (:id, :jobKey, :state, :scheduledAt, :firedAt, :actor, :payload, :payloadType, :createdAt)
                """, params);

        return execution;
    }

    @Override
    public Optional<Execution> find(ExecutionId id) {
        Objects.requireNonNull(id, "id");
        // id é PK — no máximo uma linha; ResultSetExtractor lê essa linha
        // única direto, sem passar por List/stream/findFirst.
        Execution result = jdbcTemplate.query(
                "SELECT * FROM mohs_executions WHERE id = :id",
                new MapSqlParameterSource("id", id.value()),
                rs -> rs.next() ? mapRow(rs) : null);
        return Optional.ofNullable(result);
    }

    @Override
    public Stream<Execution> findByJobKey(JobKey jobKey) {
        Objects.requireNonNull(jobKey, "jobKey");
        return jdbcTemplate.queryForStream(
                "SELECT * FROM mohs_executions WHERE job_key = :jobKey",
                new MapSqlParameterSource("jobKey", jobKey.value()),
                (rs, rowNum) -> mapRow(rs));
    }

    @Override
    public Stream<Execution> findAll() {
        return jdbcTemplate.queryForStream(
                "SELECT * FROM mohs_executions", new MapSqlParameterSource(), (rs, rowNum) -> mapRow(rs));
    }

    /**
     * Busca os attempts numa query à parte por execução — N+1 real em
     * {@link #findAll()}/{@link #findByJobKey}, deliberadamente não
     * otimizado ainda: nada chama esses dois em produção nesta etapa
     * (entra na etapa 7, wiring do REST); vale medir com um caller de
     * verdade antes de trocar por um JOIN com agregação em memória.
     */
    private Execution mapRow(ResultSet rs) throws SQLException {
        ExecutionId id = ExecutionId.of(rs.getString("id"));
        Timestamp firedAt = rs.getTimestamp("fired_at");
        return new Execution(
                id,
                JobKey.of(rs.getString("job_key")),
                ExecutionState.valueOf(rs.getString("state")),
                rs.getTimestamp("scheduled_at").toInstant(),
                firedAt == null ? null : firedAt.toInstant(),
                fetchAttempts(id),
                rs.getString("actor"));
    }

    private List<Attempt> fetchAttempts(ExecutionId executionId) {
        return jdbcTemplate.query(
                "SELECT * FROM mohs_attempts WHERE execution_id = :executionId ORDER BY number",
                new MapSqlParameterSource("executionId", executionId.value()),
                JdbcExecutionStore::mapAttemptRow);
    }

    private static Attempt mapAttemptRow(ResultSet rs, int rowNum) throws SQLException {
        Timestamp finishedAt = rs.getTimestamp("finished_at");
        return new Attempt(
                rs.getInt("number"),
                rs.getTimestamp("started_at").toInstant(),
                finishedAt == null ? null : finishedAt.toInstant(),
                ExecutionState.valueOf(rs.getString("outcome")),
                rs.getString("error"));
    }
}
