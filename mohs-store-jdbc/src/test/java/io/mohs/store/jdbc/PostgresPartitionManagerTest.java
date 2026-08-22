package io.mohs.store.jdbc;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.IsoFields;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * O gestor que o S5.1 deixou como requisito (PLAN.md): as migrações criam
 * só esqueleto + DEFAULT; semanais são runtime. O modo de falha
 * default-populada foi MEDIDO no tuning do S5.1 (CREATE bloqueado pela
 * validação da default) — aqui vira contrato: nunca exceção, sempre WARN.
 */
class PostgresPartitionManagerTest {

    private static final Instant NOW = Instant.parse("2026-08-22T12:00:00Z");

    private JdbcTemplate rawJdbcTemplate;
    private PostgresPartitionManager manager;

    @BeforeEach
    void setUp() {
        DataSource dataSource = PostgresTestSupport.freshSchema();
        rawJdbcTemplate = new JdbcTemplate(dataSource);
        manager = new PostgresPartitionManager(dataSource);
        // estado conhecido: só as DEFAULT das migrações (partições semanais de rodadas anteriores caem)
        for (String partition : weeklyPartitions()) {
            rawJdbcTemplate.execute("DROP TABLE IF EXISTS " + partition);
        }
    }

    private List<String> weeklyPartitions() {
        return rawJdbcTemplate.queryForList("""
                SELECT c.relname FROM pg_inherits i
                JOIN pg_class c ON c.oid = i.inhrelid
                JOIN pg_class p ON p.oid = i.inhparent
                WHERE p.relname IN ('mohs_execution', 'mohs_attempt') AND c.relname NOT LIKE '%_default'
                """, String.class);
    }

    private static String weekTag(Instant instant, int plusWeeks) {
        LocalDate week = LocalDate.ofInstant(instant, ZoneOffset.UTC).with(java.time.DayOfWeek.MONDAY).plusWeeks(plusWeeks);
        return "%dw%02d".formatted(week.get(IsoFields.WEEK_BASED_YEAR), week.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR));
    }

    @Test
    void ensuresCurrentAndNextWeekForBothParentsIdempotently() {
        manager.ensureWeeklyPartitions(NOW);
        manager.ensureWeeklyPartitions(NOW); // IF NOT EXISTS: segunda passada é no-op

        assertThat(weeklyPartitions()).containsExactlyInAnyOrder(
                "mohs_execution_" + weekTag(NOW, 0), "mohs_execution_" + weekTag(NOW, 1),
                "mohs_attempt_" + weekTag(NOW, 0), "mohs_attempt_" + weekTag(NOW, 1));
    }

    @Test
    void rowsRouteToTheirWeekOnceThePartitionExists() {
        manager.ensureWeeklyPartitions(NOW);
        rawJdbcTemplate.update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, payload, payload_type)
                VALUES ('exec-routed', 'job-a', 'PENDING', ?, ?, 'test', '{}', 'java.lang.Object')
                """, JdbcTimestamps.toUtcOffsetDateTime(NOW), JdbcTimestamps.toUtcOffsetDateTime(NOW));

        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM mohs_execution_" + weekTag(NOW, 0), Integer.class)).isEqualTo(1);
        assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_execution_default", Integer.class)).isZero();
    }

    /** O contrato "nunca derruba o boot" vale pro método INTEIRO (review S5.2): até uma DEFAULT dropada por operador (fim errado da dança de recuperação) degrada pra WARN. */
    @Test
    void aDroppedDefaultPartitionAlsoDegradesToAWarning() {
        rawJdbcTemplate.execute("DROP TABLE mohs_attempt_default");
        try {
            assertThatCode(() -> manager.ensureWeeklyPartitions(NOW)).doesNotThrowAnyException();
            assertThat(weeklyPartitions()).contains("mohs_attempt_" + weekTag(NOW, 0));
        } finally {
            rawJdbcTemplate.execute("CREATE TABLE IF NOT EXISTS mohs_attempt_default PARTITION OF mohs_attempt DEFAULT");
        }
    }

    /** O contrato de operabilidade: DEFAULT populada bloqueia o CREATE daquela semana (comportamento do PG, medido no S5.1) — o gestor WARNa e segue; enqueue nunca cai. */
    @Test
    void aPopulatedDefaultPartitionDegradesToAWarningNeverAnException() {
        rawJdbcTemplate.update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, payload, payload_type)
                VALUES ('exec-in-default', 'job-a', 'PENDING', ?, ?, 'test', '{}', 'java.lang.Object')
                """, JdbcTimestamps.toUtcOffsetDateTime(NOW), JdbcTimestamps.toUtcOffsetDateTime(NOW));

        assertThatCode(() -> manager.ensureWeeklyPartitions(NOW)).doesNotThrowAnyException();

        // a linha continua servida pela default; a semana seguinte (range livre) foi criada normalmente
        assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_execution_default", Integer.class)).isEqualTo(1);
        assertThat(weeklyPartitions()).contains("mohs_execution_" + weekTag(NOW, 1));
    }
}
