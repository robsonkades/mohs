package io.mohs.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
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
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import tools.jackson.databind.ObjectMapper;

import io.mohs.core.execution.Attempt;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.execution.Priority;
import io.mohs.core.job.JobKey;
import io.mohs.engine.ExecutionStore;
import io.mohs.engine.JobStore;
import io.mohs.jdbc.dialect.JdbcDialect;

/**
 * {@link ExecutionStore} sobre {@code mohs_executions}/{@code mohs_attempts}
 * (Data Mapper, PoEAA). Payload serializado via {@link ObjectMapper}
 * (Jackson, dependência obrigatória do módulo) — nunca campo de
 * {@link Execution}, só grava/carrega na borda JDBC.
 *
 * <p>{@link #insert} é o "insert do terminal" da cláusula 4 da ADR-0003:
 * usa o mesmo {@code DataSource} do chamador via
 * {@link NamedParameterJdbcTemplate}, que participa da transação Spring
 * já ativa quando existe uma — nenhuma transação própria é aberta por ele.
 *
 * <p>{@link #complete}/{@link #completeAll} são a exceção: a transição de
 * estado, o {@code INSERT} do {@link Attempt} e a devolução da vaga de
 * concorrência ({@code JobStore.decrementRunningExecutions}) são um
 * invariante que cruza duas tabelas — Unit of Work (PoEAA), atomicidade
 * de transação (DDIA cap. 7). Sem fronteira de atomicidade, um crash
 * entre o CAS e o decremento deixaria {@code running_execution_count}
 * incrementado pra sempre (a execução já não está {@code RUNNING}, o
 * reaper nunca a vê) — pra um job com {@code allowConcurrentExecutions =
 * false}, o mutex vaza e o job para de rodar permanentemente.
 * {@code PROPAGATION_REQUIRED}: participa da transação do reaper quando
 * ela existir (sincronização por {@code DataSource}, mesmo mecanismo
 * documentado em {@link JdbcClaimer}), abre a própria no caminho quente
 * do dispatch, que roda sem transação ativa.
 */
public final class JdbcExecutionStore implements ExecutionStore {

    /** Bem abaixo do teto de 2100 parâmetros do SQL Server pro `IN (:ids)` de {@link #findByIds} (DB-11). Package-private pro teste de fronteira. */
    static final int MAX_IDS_PER_QUERY = 1000;

    private static final String EXECUTION_COLUMNS = "id, job_key, state, scheduled_at, fired_at, actor, priority, idempotency_key";

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final Clock clock;
    private final ObjectMapper objectMapper;
    private final JdbcDialect dialect;

    public JdbcExecutionStore(DataSource dataSource, Clock clock, ObjectMapper objectMapper, JdbcDialect dialect) {
        this.jdbcTemplate = JdbcSupport.namedTemplateWithStreamFetchSize(Objects.requireNonNull(dataSource, "dataSource"));
        this.transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        // mesmo raciocínio da DBTUNE-4 em JdbcClaimer: CAS guardado assume
        // "última escrita vence" (READ COMMITTED), não herda o default do banco.
        this.transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.clock = Objects.requireNonNull(clock, "clock");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.dialect = Objects.requireNonNull(dialect, "dialect");
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
                .addValue("priority", execution.priority().value())
                .addValue("idempotencyKey", execution.idempotencyKey())
                .addValue("payload", payloadJson)
                .addValue("payloadType", payload.getClass().getName())
                .addValue("createdAt", JdbcTimestamps.toUtcTimestamp(clock.instant()));

        jdbcTemplate.update("""
                INSERT INTO mohs_executions (
                    id, job_key, state, scheduled_at, fired_at, actor, priority, idempotency_key, payload, payload_type, created_at)
                VALUES (:id, :jobKey, :state, :scheduledAt, :firedAt, :actor, :priority, :idempotencyKey, :payload, :payloadType, :createdAt)
                """, params);

        return execution;
    }

    @Override
    public Optional<Object> findPayload(ExecutionId id) {
        Objects.requireNonNull(id, "id");
        return JdbcSupport.findOne(jdbcTemplate,
                "SELECT payload, payload_type FROM mohs_executions WHERE id = :id",
                new MapSqlParameterSource("id", id.value()),
                this::mapPayloadRow);
    }

    private Object mapPayloadRow(ResultSet rs) throws SQLException {
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
    public boolean complete(ExecutionStore.CompletionRequest request, JobStore jobStore) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(jobStore, "jobStore");
        return Boolean.TRUE.equals(transactionTemplate.execute(_ ->
                completeWithinTransaction(request, jobStore)));
    }

    /**
     * O CAS de conclusão (ADR-0024/0033): {@code COALESCE} grava o
     * {@code retryAt} como novo {@code scheduled_at} na mesma transição
     * (invariante do record: só presente em {@code RETRY_SCHEDULED}).
     * A variante com fence acrescenta {@code lease_expires_at} ao predicado
     * — anti-ABA do reaper (ver Javadoc de
     * {@link ExecutionStore.CompletionRequest#expectedLeaseExpiresAt}).
     * Dois templates em vez de {@code (:p IS NULL OR ...)}: parâmetro em
     * predicado de nulidade tem suporte irregular entre os 4 dialetos.
     */
    private static final String COMPLETE_CAS = """
            UPDATE mohs_executions SET state = :newState, scheduled_at = COALESCE(:retryAt, scheduled_at)
            WHERE id = :id AND state = 'RUNNING'
            """;

    private static final String COMPLETE_CAS_FENCED = """
            UPDATE mohs_executions SET state = :newState, scheduled_at = COALESCE(:retryAt, scheduled_at)
            WHERE id = :id AND state = 'RUNNING' AND lease_expires_at = :expectedLease
            """;

    private boolean completeWithinTransaction(ExecutionStore.CompletionRequest request, JobStore jobStore) {
        String sql = request.expectedLeaseExpiresAt() == null ? COMPLETE_CAS : COMPLETE_CAS_FENCED;
        int updated = jdbcTemplate.update(sql, completionParams(request));
        if (updated == 0) {
            return false;
        }

        jdbcTemplate.update("""
                INSERT INTO mohs_attempts (execution_id, number, started_at, finished_at, outcome, error)
                VALUES (:executionId, :number, :startedAt, :finishedAt, :outcome, :error)
                """, attemptParams(request.id(), request.attempt()));
        jobStore.decrementRunningExecutions(request.jobKey());
        return true;
    }

    /**
     * DBTUNE-14: {@link #complete} em lote — {@code batchUpdate} com params
     * por linha (cada request carrega seu {@code retryAt}/fence), cujo
     * {@code int[]} é a confirmação exata do CAS linha a linha, e um
     * {@code INSERT} em lote só dos {@link Attempt} confirmados —
     * {@code jobStore.decrementRunningExecutions} continua uma chamada por
     * execução (guardada, barata, sem API de lote em {@link JobStore} — não
     * vale o acoplamento novo pra isto).
     */
    @Override
    public Set<ExecutionId> completeAll(List<ExecutionStore.CompletionRequest> requests, JobStore jobStore) {
        Objects.requireNonNull(requests, "requests");
        Objects.requireNonNull(jobStore, "jobStore");
        if (requests.isEmpty()) {
            return Set.of();
        }
        // requireNonNull só documenta o invariante pro @NullMarked (JAVA-8) —
        // o callback sempre devolve um Set.
        return Objects.requireNonNull(transactionTemplate.execute(_ -> completeAllWithinTransaction(requests, jobStore)));
    }

    private Set<ExecutionId> completeAllWithinTransaction(List<ExecutionStore.CompletionRequest> requests, JobStore jobStore) {
        Map<String, ExecutionStore.CompletionRequest> byId = requests.stream()
                .collect(Collectors.toMap(r -> r.id().value(), r -> r));
        Set<ExecutionId> completedIds = transitionAll(requests);
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

    /**
     * ADR-0012 — renovação em lote por node, {@code batchUpdate} por linha
     * com o {@code int[]} como confirmação exata (mesmo padrão de
     * {@link #completeAll}); {@code SUCCESS_NO_INFO} recai na confirmação
     * por {@code SELECT} da lease recém-gravada.
     */
    @Override
    public Set<ExecutionId> renewLeases(String nodeId, List<ExecutionId> ids, Instant leaseExpiresAt) {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(ids, "ids");
        Objects.requireNonNull(leaseExpiresAt, "leaseExpiresAt");
        if (ids.isEmpty()) {
            return Set.of();
        }
        Timestamp newLease = JdbcTimestamps.toUtcTimestamp(leaseExpiresAt);
        MapSqlParameterSource[] params = ids.stream()
                .map(id -> new MapSqlParameterSource()
                        .addValue("id", id.value())
                        .addValue("nodeId", nodeId)
                        .addValue("leaseExpiresAt", newLease))
                .toArray(MapSqlParameterSource[]::new);
        int[] updated = jdbcTemplate.batchUpdate("""
                UPDATE mohs_executions SET lease_expires_at = :leaseExpiresAt
                WHERE id = :id AND state = 'RUNNING' AND node_id = :nodeId
                """, params);

        Set<ExecutionId> renewed = new LinkedHashSet<>();
        for (int i = 0; i < updated.length; i++) {
            if (updated[i] == Statement.SUCCESS_NO_INFO) {
                return confirmRenewalsBySelect(nodeId, ids);
            }
            if (updated[i] > 0) {
                renewed.add(ids.get(i));
            }
        }
        return renewed;
    }

    /**
     * Fallback raro do {@code SUCCESS_NO_INFO} — confirma por POSSE
     * ({@code state + node_id}), nunca por igualdade de timestamp: precisão
     * temporal não faz round-trip garantido entre JVM e os 4 dialetos
     * (nanos do {@code Instant} vs micros da coluna), e uma igualdade
     * quebrada aqui dropava o node inteiro da renovação como falso "lost
     * lease". A posse implica o UPDATE aplicado: re-claim pelo próprio node
     * dentro da mesma chamada é impossível (o claim roda depois, na mesma
     * thread do tick) e re-claim por outro node troca o {@code node_id}.
     */
    private Set<ExecutionId> confirmRenewalsBySelect(String nodeId, List<ExecutionId> ids) {
        Set<ExecutionId> renewed = new LinkedHashSet<>();
        for (List<String> chunk : chunksOf(ids.stream().map(ExecutionId::value).toList())) {
            renewed.addAll(jdbcTemplate.query("""
                    SELECT id FROM mohs_executions
                    WHERE id IN (:ids) AND state = 'RUNNING' AND node_id = :nodeId
                    """,
                    new MapSqlParameterSource().addValue("ids", chunk).addValue("nodeId", nodeId),
                    (rs, _) -> ExecutionId.of(rs.getString("id"))));
        }
        return renewed;
    }

    /** Particiona por presença de fence — mesmo par de templates do caminho unitário; na prática o lote do reaper é uniformemente fenced. */
    private Set<ExecutionId> transitionAll(List<ExecutionStore.CompletionRequest> requests) {
        Map<Boolean, List<ExecutionStore.CompletionRequest>> byFence = requests.stream()
                .collect(Collectors.partitioningBy(r -> r.expectedLeaseExpiresAt() != null));
        Set<ExecutionId> confirmed = new LinkedHashSet<>();
        confirmed.addAll(transitionBatch(byFence.get(Boolean.FALSE), COMPLETE_CAS));
        confirmed.addAll(transitionBatch(byFence.get(Boolean.TRUE), COMPLETE_CAS_FENCED));
        return confirmed;
    }

    /** O {@code int[]} do batch é a confirmação exata do CAS; {@code SUCCESS_NO_INFO} (JDBC permite) recai na confirmação por {@code SELECT}. */
    private Set<ExecutionId> transitionBatch(List<ExecutionStore.CompletionRequest> batch, String sql) {
        if (batch.isEmpty()) {
            return Set.of();
        }
        MapSqlParameterSource[] params = batch.stream()
                .map(JdbcExecutionStore::completionParams)
                .toArray(MapSqlParameterSource[]::new);
        int[] updated = jdbcTemplate.batchUpdate(sql, params);
        Set<ExecutionId> confirmed = new LinkedHashSet<>();
        for (int i = 0; i < updated.length; i++) {
            if (updated[i] == Statement.SUCCESS_NO_INFO) {
                return confirmBySelect(batch);
            }
            if (updated[i] > 0) {
                confirmed.add(batch.get(i).id());
            }
        }
        return confirmed;
    }

    /**
     * Fallback raro do {@code SUCCESS_NO_INFO}: confirma por estado, em
     * chunks (DB-11). Sob READ COMMITTED pode confirmar como nossa uma
     * transição que outro ator commitou no meio — e o raio de dano real não
     * é só imprecisão: o id mal-atribuído entra no INSERT em lote de
     * attempts, colide com a PK do attempt que o outro ator gravou e
     * reverte a transação inteira do {@code completeAll} (auto-cicatriza no
     * tick seguinte). Aceito só porque este caminho exige driver que
     * devolva {@code SUCCESS_NO_INFO}; o caminho normal usa o {@code int[]}
     * exato.
     */
    private Set<ExecutionId> confirmBySelect(List<ExecutionStore.CompletionRequest> batch) {
        Set<ExecutionId> confirmed = new LinkedHashSet<>();
        batch.stream().collect(Collectors.groupingBy(ExecutionStore.CompletionRequest::newState))
                .forEach((newState, group) -> {
                    for (List<String> chunk : chunksOf(group.stream().map(r -> r.id().value()).toList())) {
                        confirmed.addAll(jdbcTemplate.query(
                                "SELECT id FROM mohs_executions WHERE id IN (:ids) AND state = :newState",
                                new MapSqlParameterSource().addValue("ids", chunk).addValue("newState", newState.name()),
                                (rs, _) -> ExecutionId.of(rs.getString("id"))));
                    }
                });
        return confirmed;
    }

    private static MapSqlParameterSource completionParams(ExecutionStore.CompletionRequest request) {
        return new MapSqlParameterSource()
                .addValue("newState", request.newState().name())
                .addValue("retryAt", request.retryAt() == null ? null : JdbcTimestamps.toUtcTimestamp(request.retryAt()), Types.TIMESTAMP)
                .addValue("expectedLease", request.expectedLeaseExpiresAt() == null ? null : JdbcTimestamps.toUtcTimestamp(request.expectedLeaseExpiresAt()), Types.TIMESTAMP)
                .addValue("id", request.id().value());
    }

    /** Fatias de no máximo {@link #MAX_IDS_PER_QUERY} ids pro {@code IN (:ids)} (DB-11) — views de {@code subList}, sem cópia. */
    private static List<List<String>> chunksOf(List<String> ids) {
        List<List<String>> chunks = new ArrayList<>();
        for (int start = 0; start < ids.size(); start += MAX_IDS_PER_QUERY) {
            chunks.add(ids.subList(start, Math.min(start + MAX_IDS_PER_QUERY, ids.size())));
        }
        return chunks;
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

    /** No máximo uma linha — unicidade garantida por {@code uq_mohs_executions_idem} no schema. */
    @Override
    public Optional<Execution> findByIdempotencyKey(JobKey jobKey, String idempotencyKey) {
        Objects.requireNonNull(jobKey, "jobKey");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey");
        Optional<ExecutionRow> row = JdbcSupport.findOne(jdbcTemplate,
                "SELECT " + EXECUTION_COLUMNS + " FROM mohs_executions WHERE job_key = :jobKey AND idempotency_key = :idempotencyKey",
                new MapSqlParameterSource()
                        .addValue("jobKey", jobKey.value())
                        .addValue("idempotencyKey", idempotencyKey),
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
        for (List<String> chunk : chunksOf(rawIds)) {
            rows.addAll(jdbcTemplate.query(
                    "SELECT " + EXECUTION_COLUMNS + " FROM mohs_executions WHERE id IN (:ids)",
                    new MapSqlParameterSource("ids", chunk),
                    (rs, _) -> mapRow(rs)));
        }
        return hydrateAll(rows);
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

    /**
     * Sem lock, sem cursor aberto (ao contrário de {@link #findAll}/
     * {@link #findByJobKey}) — resultado limitado por {@code limit},
     * então batch de attempts como {@link #findByIds} (DBTUNE-3), não N+1.
     */
    @Override
    public List<Execution> findPage(@Nullable JobKey jobKey, @Nullable ExecutionState status, @Nullable Instant from,
            @Nullable Instant to, @Nullable ExecutionId cursor, int limit) {
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("limit", limit);
        List<String> conditions = new ArrayList<>();
        if (jobKey != null) {
            conditions.add("job_key = :jobKey");
            params.addValue("jobKey", jobKey.value());
        }
        if (status != null) {
            conditions.add("state = :status");
            params.addValue("status", status.name());
        }
        if (from != null) {
            conditions.add("scheduled_at >= :from");
            params.addValue("from", JdbcTimestamps.toUtcTimestamp(from));
        }
        if (to != null) {
            conditions.add("scheduled_at <= :to");
            params.addValue("to", JdbcTimestamps.toUtcTimestamp(to));
        }
        if (cursor != null) {
            conditions.add("id < :cursor");
            params.addValue("cursor", cursor.value());
        }
        String where = conditions.isEmpty() ? "" : " WHERE " + String.join(" AND ", conditions);

        String sql = "SELECT " + dialect.topClause() + EXECUTION_COLUMNS + " FROM mohs_executions" + where
                + " ORDER BY id DESC " + dialect.limitClause();

        List<ExecutionRow> rows = jdbcTemplate.query(sql, params, (rs, _) -> mapRow(rs));
        return hydrateAll(rows);
    }

    /** DBTUNE-3: attempts de todas as linhas numa segunda consulta em lote — nunca uma query de attempts por linha (N+1). */
    private List<Execution> hydrateAll(List<ExecutionRow> rows) {
        Map<String, List<Attempt>> attemptsByExecutionId = fetchAttemptsByExecutionIds(rows.stream().map(r -> r.id().value()).toList());
        return rows.stream()
                .map(row -> hydrate(row, attemptsByExecutionId.getOrDefault(row.id().value(), List.of())))
                .toList();
    }

    private Execution hydrateEagerly(ExecutionRow row) {
        return hydrate(row, fetchAttempts(row.id()));
    }

    private static Execution hydrate(ExecutionRow row, List<Attempt> attempts) {
        return new Execution(row.id(), row.jobKey(), row.state(), row.scheduledAt(), row.firedAt(), attempts, row.actor(), row.priority(), row.idempotencyKey());
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
                rs.getString("actor"),
                Priority.fromValue(rs.getInt("priority")),
                rs.getString("idempotency_key"));
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
        for (List<String> chunk : chunksOf(executionIds)) {
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
    private record ExecutionRow(ExecutionId id, JobKey jobKey, ExecutionState state, Instant scheduledAt, @Nullable Instant firedAt, String actor,
                                 Priority priority, @Nullable String idempotencyKey) {
    }

    private record ExecutionIdAndAttempt(String executionId, Attempt attempt) {
    }
}
