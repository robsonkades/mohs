package io.mohs.jdbc;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.core.resource.RateLimit;
import io.mohs.engine.BatchCounters;
import io.mohs.engine.StoredJob;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;

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
        JdbcExecutionStore store = new JdbcExecutionStore(dataSource, clock, JsonMapper.builder().build());
        Execution execution = new Execution(
                ExecutionId.of("019abc-1"), JobKey.of("welcome-email"), ExecutionState.ENQUEUED,
                Instant.parse("2026-08-13T00:00:00Z"), null, List.of(), "application");

        store.insert(execution, new WelcomeEmail("ana", 31));
        Optional<Execution> found = store.find(ExecutionId.of("019abc-1"));

        assertThat(found).contains(execution);
    }

    @Test
    void batchStoreRoundTripsAgainstMySql() {
        JdbcBatchStore store = new JdbcBatchStore(dataSource, clock);

        store.create("batch-1", 10);
        BatchCounters counters = store.find("batch-1").orElseThrow();

        assertThat(counters.total()).isEqualTo(10);
    }

    @Test
    void rateLimitStoreRoundTripsAgainstMySql() {
        JdbcRateLimitStore store = new JdbcRateLimitStore(dataSource);
        RateLimit rateLimit = new RateLimit("smtp", 100, Duration.ofMinutes(1));

        store.upsert(rateLimit);

        assertThat(store.find("smtp")).contains(rateLimit);
    }
}
