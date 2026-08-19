package io.mohs.jdbc;

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
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.execution.Priority;
import io.mohs.core.job.JobKey;
import io.mohs.core.RateLimitSnapshot;
import io.mohs.core.resource.RateLimit;
import io.mohs.engine.BatchCounters;
import io.mohs.engine.StoredJob;
import io.mohs.jdbc.dialect.MySqlJdbcDialect;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * ADR-0023: prova que o schema (todo ele, não só o que {@link
 * JdbcClaimer} toca) e o DML de cada store fazem round-trip contra
 * MySQL real, não só H2/Postgres — em particular {@code
 * mohs_executions.payload}/{@code mohs_attempts.error} (`TEXT`) e as
 * colunas {@code DATETIME} (MySQL não usa {@code TIMESTAMP} — ver
 * schema-mysql.sql).
 */
class SchemaMySqlRoundTripTest {

    record Handler() {
    }

    record WelcomeEmail(String user, int age) {
    }

    private DataSource dataSource;
    private MutableClock clock;

    @BeforeEach
    void setUp() {
        dataSource = MySqlTestSupport.freshSchema();
        clock = new MutableClock(Instant.parse("2026-08-13T00:00:00Z"), ZoneId.of("UTC"));
    }

    @Test
    void jobStoreRoundTripsAgainstMySql() {
        JdbcJobStore store = new JdbcJobStore(dataSource, clock);
        JobDefinition definition = JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().runner("io"));

        store.upsert(definition);
        Optional<StoredJob> stored = store.find(JobKey.of("welcome-email"));

        assertThat(stored).isPresent();
        assertThat(stored.get().definition()).isEqualTo(definition);
    }

    @Test
    void executionStoreRoundTripsThePayloadStoredAsText() {
        new JdbcJobStore(dataSource, clock).upsert(
                JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().runner("io")));
        JdbcExecutionStore store = new JdbcExecutionStore(dataSource, clock, JsonMapper.builder().build(), new MySqlJdbcDialect());
        Execution execution = new Execution(
                ExecutionId.of("019abc-1"), JobKey.of("welcome-email"), ExecutionState.ENQUEUED,
                Instant.parse("2026-08-13T00:00:00Z"), null, List.of(), "application");

        store.insert(execution, new WelcomeEmail("ana", 31));
        Optional<Execution> found = store.find(ExecutionId.of("019abc-1"));

        assertThat(found).contains(execution);
    }

    /**
     * A semântica do índice único de idempotência no dialeto real, não só o
     * DDL: aqui é UNIQUE composto puro (InnoDB admite múltiplos NULLs em
     * índice único) — mesma chave colide, chaves NULL nunca colidem.
     */
    @Test
    void idempotencyUniqueIndexRejectsDuplicatesAndAllowsNullKeys() {
        new JdbcJobStore(dataSource, clock).upsert(
                JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().runner("io")));
        JdbcExecutionStore store = new JdbcExecutionStore(dataSource, clock, JsonMapper.builder().build(), new MySqlJdbcDialect());

        store.insert(executionWithKey("idem-1", "req-1"), new WelcomeEmail("ana", 31));
        assertThatThrownBy(() -> store.insert(executionWithKey("idem-2", "req-1"), new WelcomeEmail("ana", 31)))
                .isInstanceOf(DuplicateKeyException.class);
        store.insert(executionWithKey("idem-3", null), new WelcomeEmail("ana", 31));
        store.insert(executionWithKey("idem-4", null), new WelcomeEmail("ana", 31));

        assertThat(store.findByIdempotencyKey(JobKey.of("welcome-email"), "req-1"))
                .map(Execution::id).contains(ExecutionId.of("idem-1"));
    }

    private static Execution executionWithKey(String id, @Nullable String idempotencyKey) {
        return new Execution(ExecutionId.of(id), JobKey.of("welcome-email"), ExecutionState.ENQUEUED,
                Instant.parse("2026-08-13T00:00:00Z"), null, List.of(), "application", Priority.NORMAL, idempotencyKey);
    }

    /** DBTUNE-2: DATETIME puro (sem fração) arredondaria isto pro segundo — prova que DATETIME(6) preserva microssegundo. */
    @Test
    void executionStoreRoundTripsSubSecondPrecision() {
        new JdbcJobStore(dataSource, clock).upsert(
                JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().runner("io")));
        JdbcExecutionStore store = new JdbcExecutionStore(dataSource, clock, JsonMapper.builder().build(), new MySqlJdbcDialect());
        Instant scheduledAt = Instant.parse("2026-08-13T00:00:00.123456Z");
        Execution execution = new Execution(
                ExecutionId.of("019abc-2"), JobKey.of("welcome-email"), ExecutionState.ENQUEUED,
                scheduledAt, null, List.of(), "application");

        store.insert(execution, new WelcomeEmail("ana", 31));
        Optional<Execution> found = store.find(ExecutionId.of("019abc-2"));

        assertThat(found).map(Execution::scheduledAt).contains(scheduledAt);
    }

    @Test
    void batchStoreRoundTripsAgainstMySql() {
        JdbcBatchStore store = new JdbcBatchStore(dataSource, clock);

        store.insert("batch-1", 10);
        BatchCounters counters = store.find("batch-1").orElseThrow();

        assertThat(counters.total()).isEqualTo(10);
    }

    @Test
    void rateLimitStoreRoundTripsAgainstMySql() {
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
