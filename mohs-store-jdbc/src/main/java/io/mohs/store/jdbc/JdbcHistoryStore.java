package io.mohs.store.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.sql.DataSource;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.execution.Attempt;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.engine.HistoryStore;
import io.mohs.store.jdbc.dialect.JdbcDialect;

/**
 * {@link HistoryStore} sobre {@code mohs_execution}/{@code mohs_attempt}/
 * {@code mohs_idempotency} (Phase 5, ADR-A). SEM {@code TransactionTemplate}
 * de propósito: {@link #record} participa da transação ativa do chamador
 * (ADR-0003 §4 — o enqueue junta-se à transação do host; §7.5-1: execução
 * + fila + idempotência são a mesma unidade de trabalho, composta por quem
 * chama, nunca aqui); as leituras são autocommit como as de qualquer
 * store. A escrita terminal NÃO mora aqui — é da transação de conclusão
 * ({@code JdbcLeaseStore.complete}, §7.5-3).
 */
public final class JdbcHistoryStore implements HistoryStore {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final JsonMapper objectMapper;
    private final JdbcDialect dialect;

    public JdbcHistoryStore(DataSource dataSource, JsonMapper objectMapper, JdbcDialect dialect) {
        Objects.requireNonNull(dataSource, "dataSource");
        this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.dialect = Objects.requireNonNull(dialect, "dialect");
    }

    private static final String RECORD = """
            INSERT INTO mohs_execution (
                execution_id, job_key, shard, priority, state, scheduled_at, created_at, actor,
                correlation_id, idempotency_key, payload, payload_type)
            VALUES (:executionId, :jobKey, :shard, :priority, 'PENDING', :scheduledAt, :createdAt, :actor,
                :correlationId, :idempotencyKey, :payload, :payloadType)
            """;

    private static final String IDEMPOTENCY_INSERT = """
            INSERT INTO mohs_idempotency (job_key, idempotency_key, execution_id, created_at)
            VALUES (:jobKey, :idempotencyKey, :executionId, :createdAt)
            """;

    @Override
    public void record(List<NewExecution> executions) {
        if (executions.isEmpty()) {
            return;
        }
        // a idempotência entra ANTES da história: o conflito de PK É o check
        // de dedup (Idempotent Receiver, EIP) e precisa abortar a unidade
        // inteira antes de qualquer linha de história nascer
        MapSqlParameterSource[] keys = executions.stream()
                .filter(e -> e.idempotencyKey() != null)
                .map(this::idempotencyParams)
                .toArray(MapSqlParameterSource[]::new);
        if (keys.length > 0) {
            jdbcTemplate.batchUpdate(IDEMPOTENCY_INSERT, keys);
        }
        jdbcTemplate.batchUpdate(RECORD, executions.stream()
                .map(this::recordParams)
                .toArray(MapSqlParameterSource[]::new));
    }

    private MapSqlParameterSource idempotencyParams(NewExecution execution) {
        return new MapSqlParameterSource()
                .addValue("jobKey", execution.jobKey().value())
                .addValue("idempotencyKey", execution.idempotencyKey())
                .addValue("executionId", execution.executionId().value())
                .addValue("createdAt", dialect.splitTimestamp(execution.createdAt()));
    }

    private MapSqlParameterSource recordParams(NewExecution execution) {
        return new MapSqlParameterSource()
                .addValue("executionId", execution.executionId().value())
                .addValue("jobKey", execution.jobKey().value())
                .addValue("shard", execution.shard())
                .addValue("priority", execution.priority())
                .addValue("scheduledAt", dialect.splitTimestamp(execution.scheduledAt()))
                .addValue("createdAt", dialect.splitTimestamp(execution.createdAt()))
                .addValue("actor", execution.actor())
                .addValue("correlationId", execution.correlationId())
                .addValue("idempotencyKey", execution.idempotencyKey())
                .addValue("payload", writePayload(execution.payload()))
                .addValue("payloadType", execution.payload().getClass().getName());
    }

    private String writePayload(Object payload) {
        return objectMapper.writeValueAsString(payload);
    }

    @Override
    public Optional<ExecutionId> findByIdempotencyKey(JobKey jobKey, String idempotencyKey) {
        Objects.requireNonNull(jobKey, "jobKey");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey");
        return JdbcSupport.findOne(jdbcTemplate, """
                SELECT execution_id FROM mohs_idempotency
                WHERE job_key = :jobKey AND idempotency_key = :idempotencyKey
                """,
                new MapSqlParameterSource().addValue("jobKey", jobKey.value()).addValue("idempotencyKey", idempotencyKey),
                rs -> ExecutionId.of(rs.getString("execution_id")));
    }

    /** Mesma disciplina do {@code findPayloads} da ADR-0047: veredito POR LINHA — linha ilegível não contamina as vizinhas; só infra propaga. */
    @Override
    public PayloadBatch findPayloads(List<ExecutionId> ids) {
        Objects.requireNonNull(ids, "ids");
        if (ids.isEmpty()) {
            return new PayloadBatch(Map.of(), Map.of());
        }
        Map<ExecutionId, PayloadRow> rows = new LinkedHashMap<>();
        Map<ExecutionId, RuntimeException> unreadable = new LinkedHashMap<>();
        for (List<String> chunk : JdbcSupport.chunksOf(ids.stream().map(ExecutionId::value).toList())) {
            jdbcTemplate.query("""
                    SELECT execution_id, created_at, payload, payload_type FROM mohs_execution
                    WHERE execution_id IN (:ids)
                    """, new MapSqlParameterSource("ids", chunk), rs -> {
                ExecutionId id = ExecutionId.of(rs.getString("execution_id"));
                try {
                    rows.put(id, new PayloadRow(id,
                            Objects.requireNonNull(dialect.readSplitTimestamp(rs, "created_at"), "created_at"),
                            readPayload(rs)));
                } catch (RuntimeException e) {
                    unreadable.put(id, e);
                }
            });
        }
        return new PayloadBatch(rows, unreadable);
    }

    private Object readPayload(ResultSet rs) throws SQLException {
        String payloadJson = rs.getString("payload");
        String payloadType = rs.getString("payload_type");
        Class<?> type;
        try {
            type = Class.forName(payloadType);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("payload type '" + payloadType + "' not found on classpath", e);
        }
        return objectMapper.readValue(payloadJson, type);
    }

    @Override
    public List<Attempt> findAttempts(ExecutionId executionId) {
        Objects.requireNonNull(executionId, "executionId");
        return jdbcTemplate.query("""
                SELECT number, started_at, finished_at, outcome, error FROM mohs_attempt
                WHERE execution_id = :executionId ORDER BY number
                """, new MapSqlParameterSource("executionId", executionId.value()), (rs, _) -> mapAttempt(rs));
    }

    @Override
    public int pruneIdempotencyBefore(Instant cutoff) {
        Objects.requireNonNull(cutoff, "cutoff");
        return jdbcTemplate.update("DELETE FROM mohs_idempotency WHERE created_at < :cutoff",
                new MapSqlParameterSource("cutoff", dialect.splitTimestamp(cutoff)));
    }

    private Attempt mapAttempt(ResultSet rs) throws SQLException {
        return new Attempt(
                rs.getInt("number"),
                Objects.requireNonNull(dialect.readSplitTimestamp(rs, "started_at"), "started_at"),
                dialect.readSplitTimestamp(rs, "finished_at"),
                ExecutionState.valueOf(rs.getString("outcome")),
                rs.getString("error"));
    }
}
