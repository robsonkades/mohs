package io.mohs.store.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.execution.Attempt;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.execution.Priority;
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

    private static final String HEAD_COLUMNS =
            "execution_id, job_key, scheduled_at, created_at, actor, priority, correlation_id";

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
            jdbcTemplate.query("SELECT " + HEAD_COLUMNS + ", payload, payload_type FROM mohs_execution WHERE execution_id IN (:ids)",
                    new MapSqlParameterSource("ids", chunk), rs -> {
                ExecutionId id = ExecutionId.of(rs.getString("execution_id"));
                try {
                    rows.put(id, new PayloadRow(mapHead(rs), readPayload(rs)));
                } catch (RuntimeException e) {
                    unreadable.put(id, e);
                }
            });
        }
        return new PayloadBatch(rows, unreadable);
    }

    @Override
    public List<ExecutionHead> findHeads(List<ExecutionId> ids) {
        Objects.requireNonNull(ids, "ids");
        if (ids.isEmpty()) {
            return List.of();
        }
        List<ExecutionHead> heads = new ArrayList<>(ids.size());
        for (List<String> chunk : JdbcSupport.chunksOf(ids.stream().map(ExecutionId::value).toList())) {
            heads.addAll(jdbcTemplate.query("SELECT " + HEAD_COLUMNS + " FROM mohs_execution WHERE execution_id IN (:ids)",
                    new MapSqlParameterSource("ids", chunk), (rs, _) -> mapHead(rs)));
        }
        return heads;
    }

    private ExecutionHead mapHead(ResultSet rs) throws SQLException {
        return new ExecutionHead(
                ExecutionId.of(rs.getString("execution_id")),
                JobKey.of(rs.getString("job_key")),
                Objects.requireNonNull(dialect.readSplitTimestamp(rs, "scheduled_at"), "scheduled_at"),
                Objects.requireNonNull(dialect.readSplitTimestamp(rs, "created_at"), "created_at"),
                rs.getString("actor"),
                rs.getInt("priority"),
                rs.getString("correlation_id"));
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

    // ─── read model (§6.2): a derivação de estado sobre história+fila+posse ──

    private static final String READ_MODEL_COLUMNS = """
            e.execution_id, e.job_key, e.state, e.scheduled_at, e.created_at, e.actor, e.priority,
            e.correlation_id, e.idempotency_key,
            l.node_id AS lease_node, l.claimed_at AS lease_claimed_at,
            r.execution_id AS ready_id, r.attempt AS ready_attempt, r.visible_at AS ready_visible_at
            """;

    private static final String READ_MODEL_FROM = """
            FROM mohs_execution e
            LEFT JOIN mohs_lease l ON l.execution_id = e.execution_id
            LEFT JOIN mohs_ready r ON r.execution_id = e.execution_id
            """;

    @Override
    public Optional<Execution> find(ExecutionId id, Instant now) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(now, "now");
        Optional<Execution> summary = JdbcSupport.findOne(jdbcTemplate,
                "SELECT " + READ_MODEL_COLUMNS + READ_MODEL_FROM + "WHERE e.execution_id = :executionId",
                new MapSqlParameterSource("executionId", id.value()),
                rs -> mapDerived(rs, now, List.of()));
        // detail view carrega attempts (contrato da era anterior preservado);
        // duas leituras, não um join N×M — attempts multiplicariam a linha
        return summary.map(execution -> withAttempts(execution, findAttempts(id)));
    }

    private static Execution withAttempts(Execution execution, List<Attempt> attempts) {
        return new Execution(execution.id(), execution.jobKey(), execution.state(), execution.scheduledAt(),
                execution.firedAt(), attempts, execution.actor(), execution.priority(), execution.idempotencyKey(),
                execution.batchId(), execution.owner());
    }

    @Override
    public List<Execution> findPage(@Nullable JobKey jobKey, @Nullable ExecutionState status, @Nullable Instant from,
            @Nullable Instant to, @Nullable ExecutionId cursor, int limit, Instant now) {
        Objects.requireNonNull(now, "now");
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("limit", limit)
                .addValue("now", dialect.splitTimestamp(now));
        List<String> conditions = new ArrayList<>();
        if (jobKey != null) {
            conditions.add("e.job_key = :jobKey");
            params.addValue("jobKey", jobKey.value());
        }
        if (from != null) {
            conditions.add("e.scheduled_at >= :from");
            params.addValue("from", dialect.splitTimestamp(from));
        }
        if (to != null) {
            conditions.add("e.scheduled_at <= :to");
            params.addValue("to", dialect.splitTimestamp(to));
        }
        if (cursor != null) {
            conditions.add("e.execution_id < :cursor");
            params.addValue("cursor", cursor.value());
        }
        if (status != null) {
            // filtro sobre o estado DERIVADO: terminal mora na coluna;
            // RUNNING mora na posse; ENQUEUED/RETRY_WAITING moram na fila,
            // separados pela regra de visibilidade (§4.3)
            switch (status) {
                case SUCCEEDED, FAILED, CANCELLED -> {
                    conditions.add("e.state = :status");
                    params.addValue("status", status.name());
                }
                case RUNNING -> conditions.add("l.node_id IS NOT NULL");
                case RETRY_WAITING -> conditions.add("r.attempt > 1 AND r.visible_at > :now");
                case ENQUEUED -> conditions.add("r.execution_id IS NOT NULL AND (r.attempt = 1 OR r.visible_at <= :now)");
            }
        }
        String where = conditions.isEmpty() ? "" : "WHERE " + String.join(" AND ", conditions) + "\n";
        String sql = "SELECT " + dialect.topClause() + READ_MODEL_COLUMNS + READ_MODEL_FROM + where
                + "ORDER BY e.execution_id DESC " + dialect.limitClause();
        // SUMÁRIO por contrato (era anterior preservada): attempts vazios na listagem
        return jdbcTemplate.query(sql, params, (rs, _) -> mapDerived(rs, now, List.of()));
    }

    /** A derivação do §4.3 — ver o Javadoc de {@link HistoryStore#find}. */
    private Execution mapDerived(ResultSet rs, Instant now, List<Attempt> attempts) throws SQLException {
        ExecutionId id = ExecutionId.of(rs.getString("execution_id"));
        String column = rs.getString("state");
        String leaseNode = rs.getString("lease_node");
        boolean queued = rs.getString("ready_id") != null;

        ExecutionState state;
        String owner = null;
        Instant firedAt = null;
        if (!"PENDING".equals(column)) {
            state = ExecutionState.valueOf(column);
        } else if (leaseNode != null) {
            state = ExecutionState.RUNNING;
            owner = leaseNode;
            firedAt = dialect.readSplitTimestamp(rs, "lease_claimed_at");
        } else if (queued && rs.getInt("ready_attempt") > 1
                && Objects.requireNonNull(dialect.readSplitTimestamp(rs, "ready_visible_at"), "ready_visible_at").isAfter(now)) {
            state = ExecutionState.RETRY_WAITING;
        } else {
            // na fila e visível — ou na janela de um flush de conclusão em
            // curso (PENDING órfão): ENQUEUED, a staleness aceita do §6.2
            state = ExecutionState.ENQUEUED;
        }
        return new Execution(id, JobKey.of(rs.getString("job_key")), state,
                Objects.requireNonNull(dialect.readSplitTimestamp(rs, "scheduled_at"), "scheduled_at"),
                firedAt, attempts, rs.getString("actor"), Priority.fromValue(rs.getInt("priority")),
                rs.getString("idempotency_key"), rs.getString("correlation_id"), owner);
    }

    @Override
    public Map<ExecutionState, Long> countActiveByState(Instant now) {
        Objects.requireNonNull(now, "now");
        Map<ExecutionState, Long> counts = new LinkedHashMap<>();
        // duas queries pequenas, custo = trabalho vivo por construção (§5.3):
        // a fila É o backlog e a posse É o em-execução — a história não entra
        jdbcTemplate.query("""
                SELECT COUNT(*) AS queued,
                       SUM(CASE WHEN attempt > 1 AND visible_at > :now THEN 1 ELSE 0 END) AS waiting
                FROM mohs_ready
                """, new MapSqlParameterSource("now", dialect.splitTimestamp(now)), rs -> {
            long waiting = rs.getLong("waiting");
            counts.put(ExecutionState.ENQUEUED, rs.getLong("queued") - waiting);
            counts.put(ExecutionState.RETRY_WAITING, waiting);
        });
        Long running = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_lease", new MapSqlParameterSource(), Long.class);
        counts.put(ExecutionState.RUNNING, running == null ? 0L : running);
        return counts;
    }

    @Override
    public Map<ExecutionState, Long> countTerminalOutcomesSince(Instant since) {
        Objects.requireNonNull(since, "since");
        Map<ExecutionState, Long> counts = new LinkedHashMap<>();
        jdbcTemplate.query("""
                SELECT outcome, COUNT(*) AS finished FROM mohs_attempt
                WHERE finished_at >= :since AND outcome IN ('SUCCEEDED', 'FAILED')
                GROUP BY outcome
                """, new MapSqlParameterSource("since", dialect.splitTimestamp(since)), rs -> {
            counts.put(ExecutionState.valueOf(rs.getString("outcome")), rs.getLong("finished"));
        });
        return counts;
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
