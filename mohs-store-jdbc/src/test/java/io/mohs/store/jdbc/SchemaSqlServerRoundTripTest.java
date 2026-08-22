package io.mohs.store.jdbc;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.core.RateLimitSnapshot;
import io.mohs.core.resource.RateLimit;
import io.mohs.engine.BatchCounters;
import io.mohs.engine.HistoryStore;
import io.mohs.engine.StoredJob;
import io.mohs.store.jdbc.dialect.SqlServerJdbcDialect;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * ADR-0023: prova que o schema (todo ele, não só o que {@code JdbcWorkQueue} toca) e o DML de cada store fazem round-trip contra SQL
 * Server real, não só H2/Postgres — em particular {@code
 * mohs_execution.payload}/{@code mohs_attempt.error}
 * ({@code NVARCHAR(MAX)}, não {@code CLOB}/{@code TEXT} — deprecados em
 * SQL Server) e as colunas {@code DATETIME2}/{@code NVARCHAR}/
 * {@code BIT} (ver schema-sqlserver.sql).
 */
class SchemaSqlServerRoundTripTest {

    record Handler() {
    }

    record WelcomeEmail(String user, int age) {
    }

    private DataSource dataSource;
    private MutableClock clock;

    @BeforeEach
    void setUp() {
        dataSource = SqlServerTestSupport.freshSchema();
        clock = new MutableClock(Instant.parse("2026-08-13T00:00:00Z"), ZoneId.of("UTC"));
    }

    @Test
    void jobStoreRoundTripsAgainstSqlServer() {
        JdbcJobStore store = new JdbcJobStore(dataSource, clock);
        JobDefinition definition = JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().runner("io"));

        store.upsert(definition);
        Optional<StoredJob> stored = store.find(JobKey.of("welcome-email"));

        assertThat(stored).isPresent();
        assertThat(stored.get().definition()).isEqualTo(definition);
    }

    @Test
    void historyStoreRoundTripsThePayloadStoredAsNvarcharMax() {
        new JdbcJobStore(dataSource, clock).upsert(
                JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().runner("io")));
        JdbcHistoryStore store = new JdbcHistoryStore(dataSource, JsonMapper.builder().build(), new SqlServerJdbcDialect());
        Instant when = Instant.parse("2026-08-13T00:00:00Z");

        store.record(List.of(new HistoryStore.NewExecution(ExecutionId.of("019abc-1"), JobKey.of("welcome-email"),
                0, 20, when, when, "application", null, null, new WelcomeEmail("ana", 31))));
        HistoryStore.PayloadBatch batch = store.findPayloads(List.of(ExecutionId.of("019abc-1")));

        assertThat(batch.unreadable()).isEmpty();
        assertThat(batch.rows().get(ExecutionId.of("019abc-1")).payload()).isEqualTo(new WelcomeEmail("ana", 31));
    }

    /**
     * A semântica do dedup no dialeto real, não só o DDL: na mesa nova o
     * Idempotent Receiver é o conflito de PK de {@code mohs_idempotency} —
     * mesma chave colide; execução sem chave nunca disputa a tabela.
     */
    @Test
    void idempotencyPrimaryKeyRejectsDuplicatesAndAllowsNullKeys() {
        new JdbcJobStore(dataSource, clock).upsert(
                JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().runner("io")));
        JdbcHistoryStore store = new JdbcHistoryStore(dataSource, JsonMapper.builder().build(), new SqlServerJdbcDialect());

        store.record(List.of(newExecutionWithKey("idem-1", "req-1")));
        assertThatThrownBy(() -> store.record(List.of(newExecutionWithKey("idem-2", "req-1"))))
                .isInstanceOf(DuplicateKeyException.class);
        store.record(List.of(newExecutionWithKey("idem-3", null)));
        store.record(List.of(newExecutionWithKey("idem-4", null)));

        assertThat(store.findByIdempotencyKey(JobKey.of("welcome-email"), "req-1"))
                .contains(ExecutionId.of("idem-1"));
    }

    private static HistoryStore.NewExecution newExecutionWithKey(String id, @Nullable String idempotencyKey) {
        Instant when = Instant.parse("2026-08-13T00:00:00Z");
        return new HistoryStore.NewExecution(ExecutionId.of(id), JobKey.of("welcome-email"), 0, 20, when, when,
                "application", null, idempotencyKey, new WelcomeEmail("ana", 31));
    }

    @Test
    void batchStoreRoundTripsAgainstSqlServer() {
        JdbcBatchStore store = new JdbcBatchStore(dataSource, clock);

        store.insert("batch-1", 10);
        BatchCounters counters = store.find("batch-1").orElseThrow();

        assertThat(counters.total()).isEqualTo(10);
    }

    @Test
    void rateLimitStoreRoundTripsAgainstSqlServer() {
        JdbcRateLimitStore store = new JdbcRateLimitStore(dataSource, Clock.systemUTC());
        RateLimit rateLimit = new RateLimit("smtp", 100, Duration.ofMinutes(1));

        store.upsert(rateLimit);

        assertThat(store.find("smtp").map(RateLimitSnapshot::rateLimit)).contains(rateLimit);
    }

    /**
     * O balde da ADR-0042 vive em duas colunas, e {@code refilled_at} é a que
     * carrega a fração: o refill conta tokens pelo tempo decorrido desde ela e
     * grava de volta o instante avançado pelo que converteu — nunca "agora".
     * Se o dialeto engolir a fração sub-segundo desse instante, o balde acorda
     * com a idade errada: mais velho libera token a mais e estoura justamente o
     * limite que protege o recurso externo; mais novo segura job sem um erro
     * sequer no log. Até aqui o caminho de {@code charge} só rodava em H2.
     *
     * <p>A aritmética é escolhida para não absorver o erro. Com 100/min sai um
     * token a cada 600ms, a linha nasce em {@code .500} e a segunda cobrança vem
     * 1s depois: com a fração intacta o decorrido rende exatamente 1 token e
     * sobram 50. O que este teste pega é desvio de {@code refilled_at} a partir
     * de 200ms para o passado (dá 51) ou acima de 400ms para o futuro (dá 49) —
     * coluna de segundo cheio cai fora dos dois lados: trunca, 51; arredonda,
     * 49, medido degradando o MySQL para {@code DATETIME} sem precisão
     * declarada. Perda mais fina passa aqui, e é inofensiva no intervalo por
     * token de qualquer limite real.
     */
    @Test
    void chargingAcrossATokenBoundaryProvesRefilledAtKeepsItsFraction() {
        clock.advance(Duration.ofMillis(500));
        JdbcRateLimitStore store = new JdbcRateLimitStore(dataSource, clock);
        store.upsert(new RateLimit("smtp", 100, Duration.ofMinutes(1)));

        assertThat(store.charge("smtp", 50, clock.instant())).isTrue();
        clock.advance(Duration.ofSeconds(1));
        assertThat(store.charge("smtp", 1, clock.instant())).isTrue();

        assertThat(store.available("smtp", clock.instant())).isEqualTo(50);
    }
}
