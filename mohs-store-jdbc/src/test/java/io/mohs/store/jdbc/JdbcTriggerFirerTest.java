package io.mohs.store.jdbc;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.execution.Priority;
import io.mohs.core.job.JobKey;
import io.mohs.store.jdbc.dialect.H2JdbcDialect;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link JdbcTriggerFirer} — o CAS transacional da ADR-0035 sobre as mesas
 * da Phase 5: avanço de {@code next_fire_at}, história ({@code
 * mohs_execution}) e fila ({@code mohs_ready}) vencem ou perdem juntos.
 */
class JdbcTriggerFirerTest {

    private static final Instant NOW = Instant.parse("2026-08-15T12:00:00Z");

    record Handler() {
    }

    private DataSource dataSource;
    private MutableClock clock;
    private JdbcJobStore jobStore;
    private JdbcTriggerFirer firer;
    private JdbcTemplate rawJdbc;

    @BeforeEach
    void setUp() {
        dataSource = freshH2DataSource();
        clock = new MutableClock(NOW, ZoneId.of("UTC"));
        jobStore = new JdbcJobStore(dataSource, clock);
        JdbcHistoryStore historyStore = new JdbcHistoryStore(dataSource, JsonMapper.builder().build(), new H2JdbcDialect());
        JdbcWorkQueue workQueue = new JdbcWorkQueue(dataSource, new H2JdbcDialect(), new JdbcBatchStore(dataSource, clock));
        firer = new JdbcTriggerFirer(dataSource, historyStore, workQueue);
        rawJdbc = new JdbcTemplate(dataSource);
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:trigger-firer-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }

    /** upsert arma o trigger em {@code NOW + 1min} — o valor observado dos testes. */
    private Instant armEveryMinuteJob(String jobKey) {
        jobStore.upsert(JobDefinition.of(jobKey, Handler.class, spec -> spec.every(Duration.ofMinutes(1))));
        return NOW.plus(Duration.ofMinutes(1));
    }

    private static Execution occurrence(String id, String jobKey, Instant scheduledAt) {
        return new Execution(ExecutionId.of(id), JobKey.of(jobKey), ExecutionState.ENQUEUED, scheduledAt, null,
                List.of(), Execution.SCHEDULER_ACTOR, Priority.NORMAL, null);
    }

    private @Nullable Instant nextFireAtOf(String jobKey) {
        LocalDateTime stored = rawJdbc.queryForObject(
                "SELECT next_fire_at FROM mohs_job_definitions WHERE job_key = ?", LocalDateTime.class, jobKey);
        return stored == null ? null : JdbcTimestamps.fromUtcLocalDateTime(stored);
    }

    private @Nullable String historyStateOf(String id) {
        List<String> states = rawJdbc.queryForList(
                "SELECT state FROM mohs_execution WHERE execution_id = ?", String.class, id);
        return states.isEmpty() ? null : states.get(0);
    }

    @Test
    void fireAdvancesTheTriggerAndInsertsTheOccurrences() {
        Instant observed = armEveryMinuteJob("poll");
        Instant newNextFire = observed.plus(Duration.ofMinutes(1));

        boolean fired = firer.fire(JobKey.of("poll"), observed, newNextFire,
                List.of(occurrence("occ-1", "poll", observed)), new LinkedHashMap<String, Object>(), NOW);

        assertThat(fired).isTrue();
        assertThat(nextFireAtOf("poll")).isEqualTo(newNextFire);
        // história advisory + fila devida em scheduledAt — a unidade de enqueue do §7.5-1
        assertThat(historyStateOf("occ-1")).isEqualTo("PENDING");
        assertThat(rawJdbc.queryForObject(
                "SELECT actor FROM mohs_execution WHERE execution_id = 'occ-1'", String.class))
                .isEqualTo(Execution.SCHEDULER_ACTOR);
        assertThat(rawJdbc.queryForObject(
                "SELECT visible_at FROM mohs_ready WHERE execution_id = 'occ-1'", LocalDateTime.class))
                .isEqualTo(JdbcTimestamps.toUtcLocalDateTime(observed));
        // createdAt = now do disparo (chave de partição), não o scheduledAt
        assertThat(rawJdbc.queryForObject(
                "SELECT created_at FROM mohs_execution WHERE execution_id = 'occ-1'", LocalDateTime.class))
                .isEqualTo(JdbcTimestamps.toUtcLocalDateTime(NOW));
    }

    /** A corrida entre nós: quem observa um next_fire_at que já avançou perde e não insere nada. */
    @Test
    void fireLosesWhenTheObservedTriggerIsStale() {
        Instant observed = armEveryMinuteJob("poll");
        Instant stale = observed.minus(Duration.ofMinutes(1));

        boolean fired = firer.fire(JobKey.of("poll"), stale, observed,
                List.of(occurrence("occ-1", "poll", stale)), new LinkedHashMap<String, Object>(), NOW);

        assertThat(fired).isFalse();
        assertThat(nextFireAtOf("poll")).isEqualTo(observed);
        assertThat(historyStateOf("occ-1")).isNull();
    }

    /** fixed-delay: o plano desarma o trigger — a conclusão rearma (ADR-0035). */
    @Test
    void fireCanDisarmTheTrigger() {
        Instant observed = armEveryMinuteJob("poll");

        boolean fired = firer.fire(JobKey.of("poll"), observed, null,
                List.of(occurrence("occ-1", "poll", observed)), new LinkedHashMap<String, Object>(), NOW);

        assertThat(fired).isTrue();
        assertThat(nextFireAtOf("poll")).isNull();
    }

    /** Mohs.remove entre a varredura e o CAS: o predicado de retired impede materializar ocorrência zumbi que o cancel do remove já não alcança. */
    @Test
    void fireLosesWhenTheJobWasRetiredAfterTheSweep() {
        Instant observed = armEveryMinuteJob("poll");
        jobStore.remove(JobKey.of("poll"));

        boolean fired = firer.fire(JobKey.of("poll"), observed, observed.plus(Duration.ofMinutes(1)),
                List.of(occurrence("occ-1", "poll", observed)), new LinkedHashMap<String, Object>(), NOW);

        assertThat(fired).isFalse();
        assertThat(historyStateOf("occ-1")).isNull();
    }

    /** Avanço e inserção são atômicos: inserção que falha desfaz o avanço — a ocorrência não é perdida, o próximo tick tenta de novo. */
    @Test
    void aFailedInsertRollsBackTheAdvance() {
        Instant observed = armEveryMinuteJob("poll");
        // segunda ocorrência colide com uma execução pré-existente — PK viola no record
        rawJdbc.update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, payload, payload_type)
                VALUES ('occ-2', 'poll', 'PENDING', ?, ?, 'test', '{}', 'java.lang.Object')
                """, JdbcTimestamps.toUtcLocalDateTime(NOW), JdbcTimestamps.toUtcLocalDateTime(NOW));

        assertThatThrownBy(() -> firer.fire(JobKey.of("poll"), observed, observed.plus(Duration.ofMinutes(1)),
                List.of(occurrence("occ-1", "poll", observed), occurrence("occ-2", "poll", observed)),
                new LinkedHashMap<String, Object>(), NOW))
                .isInstanceOf(DataIntegrityViolationException.class);

        assertThat(nextFireAtOf("poll")).isEqualTo(observed);
        assertThat(historyStateOf("occ-1")).isNull();
    }
}
