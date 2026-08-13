package io.mohs.jdbc;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.PolicySpec;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.CronSpec;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.core.schedule.Schedule;
import io.mohs.engine.StoredJob;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcJobStoreTest {

    record Handler() {
    }

    private DataSource dataSource;
    private MutableClock clock;
    private JdbcJobStore store;

    @BeforeEach
    void setUp() {
        dataSource = freshH2DataSource();
        clock = new MutableClock(Instant.parse("2026-08-13T00:00:00Z"), ZoneId.of("UTC"));
        store = new JdbcJobStore(dataSource, clock);
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:job-store-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema.sql")).execute(h2);
        return h2;
    }

    private static JobDefinition definition(String id, Schedule schedule) {
        return JobDefinition.of(id, Handler.class, spec -> {
            PolicySpec policySpec = switch (schedule) {
                case CronSpec cron -> spec.cron(cron.expression(), cron.zone());
                case IntervalSpec interval -> interval.afterFinish()
                        ? spec.everyAfterFinish(interval.interval())
                        : spec.every(interval.interval());
                case OnDemandSpec onDemand -> spec.onDemand();
            };
            policySpec.runner("io").queue("emails").retries(3);
        });
    }

    @Test
    void upsertInsertsANewCronDefinition() {
        JobDefinition definition = definition("welcome-email", new CronSpec("0 0 2 * * *", ZoneId.of("America/Sao_Paulo")));

        store.upsert(definition);
        Optional<StoredJob> stored = store.find(JobKey.of("welcome-email"));

        assertThat(stored).isPresent();
        assertThat(stored.get().definition()).isEqualTo(definition);
        assertThat(stored.get().orphaned()).isFalse();
        assertThat(stored.get().paused()).isFalse();
    }

    @Test
    void roundTripsIntervalSchedule() {
        JobDefinition definition = definition("poll", new IntervalSpec(Duration.ofMinutes(5), true));

        store.upsert(definition);

        assertThat(store.find(JobKey.of("poll")).map(StoredJob::definition)).contains(definition);
    }

    @Test
    void roundTripsOnDemandSchedule() {
        JobDefinition definition = definition("import-file", new OnDemandSpec());

        store.upsert(definition);

        assertThat(store.find(JobKey.of("import-file")).map(StoredJob::definition)).contains(definition);
    }

    @Test
    void upsertOnExistingKeyAppliesDefinitionalChanges() {
        store.upsert(definition("welcome-email", new CronSpec("0 0 2 * * *", ZoneId.of("UTC"))));

        JobDefinition changed = JobDefinition.of("welcome-email", Handler.class,
                spec -> spec.cron("0 0 3 * * *", ZoneId.of("UTC")).retries(9));
        store.upsert(changed);

        StoredJob stored = store.find(JobKey.of("welcome-email")).orElseThrow();
        assertThat(stored.definition().retries()).isEqualTo(9);
        assertThat(stored.definition().schedule()).isEqualTo(new CronSpec("0 0 3 * * *", ZoneId.of("UTC")));
    }

    @Test
    void upsertPreservesOperationalStateAcrossReupsert() {
        JobKey key = JobKey.of("welcome-email");
        store.upsert(definition("welcome-email", new OnDemandSpec()));
        store.pause(key);

        // redeploy: o código reaplica a mesma definição
        store.upsert(definition("welcome-email", new OnDemandSpec()));

        assertThat(store.find(key)).map(StoredJob::paused).contains(true);
    }

    @Test
    void findReturnsEmptyForUnknownKey() {
        assertThat(store.find(JobKey.of("ghost"))).isEmpty();
    }

    @Test
    void findAllReturnsEveryStoredJob() {
        store.upsert(definition("a", new OnDemandSpec()));
        store.upsert(definition("b", new OnDemandSpec()));

        List<StoredJob> all = store.findAll();

        assertThat(all).extracting(job -> job.definition().key()).containsExactlyInAnyOrder(JobKey.of("a"), JobKey.of("b"));
    }

    @Test
    void markOrphanedSetsTheFlag() {
        JobKey key = JobKey.of("welcome-email");
        store.upsert(definition("welcome-email", new OnDemandSpec()));

        store.markOrphaned(key);

        assertThat(store.find(key)).map(StoredJob::orphaned).contains(true);
    }

    @Test
    void pauseAndResumeToggleTheFlag() {
        JobKey key = JobKey.of("welcome-email");
        store.upsert(definition("welcome-email", new OnDemandSpec()));

        store.pause(key);
        assertThat(store.find(key)).map(StoredJob::paused).contains(true);

        store.resume(key);
        assertThat(store.find(key)).map(StoredJob::paused).contains(false);
    }

    @Test
    void removeDeletesTheRow() {
        JobKey key = JobKey.of("welcome-email");
        store.upsert(definition("welcome-email", new OnDemandSpec()));

        store.remove(key);

        assertThat(store.find(key)).isEmpty();
    }

    @Test
    void findSkipsRowsWhoseHandlerTypeNoLongerResolves() {
        // simula um handler removido do código: insere a linha direto,
        // sem passar pela store (que exige um Class<?> de verdade).
        Timestamp now = Timestamp.from(clock.instant());
        JdbcTemplate rawJdbcTemplate = new JdbcTemplate(dataSource);
        rawJdbcTemplate.update("""
                INSERT INTO job_definitions (
                    job_key, handler_type, schedule_type, misfire, retries, source, orphaned, paused, created_at, updated_at)
                VALUES ('ghost-handler', 'com.example.LongGoneHandler', 'ON_DEMAND', 'IGNORE', 0, 'ANNOTATION', TRUE, FALSE, ?, ?)
                """, now, now);
        store.upsert(definition("still-here", new OnDemandSpec()));

        List<StoredJob> all = store.findAll();

        assertThat(all).extracting(job -> job.definition().key()).containsExactly(JobKey.of("still-here"));
    }
}
