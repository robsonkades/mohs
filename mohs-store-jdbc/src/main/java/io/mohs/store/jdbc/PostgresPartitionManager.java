package io.mohs.store.jdbc;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.IsoFields;
import java.util.List;
import java.util.Objects;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Gestor das partições semanais de {@code mohs_execution}/{@code mohs_attempt}
 * (Phase 5, ADR-A §7.3) — Postgres apenas: os equivalentes funcionais de
 * Tier 2/3 não particionam (PLAN.md, decisão 3). As migrações criam só o
 * esqueleto + DEFAULT (SQL procedural não sobrevive aos dois caminhos de
 * instalação); criar adiante é responsabilidade DESTE gestor, no boot —
 * ANTES de qualquer enqueue — e periodicamente de carona no housekeeping.
 *
 * <p>Modo de falha documentado (medido no tuning do S5.1): linha na
 * DEFAULT **bloqueia** o {@code CREATE PARTITION} da semana que cobre o
 * range dela — e a validação varre a DEFAULT inteira sob ACCESS EXCLUSIVE.
 * Com create-ahead de duas semanas a DEFAULT nunca pega linha em operação
 * normal; se pegar (gestor parado por mais de uma semana), o WARN daqui
 * aponta a dança de recuperação (DETACH default → CREATE semana → mover
 * linhas → ATTACH), que é rotina de operador até existir demanda real de
 * automatizá-la.
 *
 * <p>Nome de partição entra por {@code format} — DDL de partição é o caso
 * legítimo de SQL dinâmico: o nome deriva de {@code IsoFields}, nunca de
 * entrada externa.
 */
public final class PostgresPartitionManager {

    private static final Logger log = LoggerFactory.getLogger(PostgresPartitionManager.class);

    private static final List<String> PARENTS = List.of("mohs_execution", "mohs_attempt");

    private final JdbcTemplate jdbcTemplate;

    public PostgresPartitionManager(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(Objects.requireNonNull(dataSource, "dataSource"));
    }

    /**
     * Garante as partições da semana ISO corrente e da próxima —
     * idempotente ({@code IF NOT EXISTS}); a falha por DEFAULT populada
     * vira WARN com a rotina de recuperação, nunca exceção: um gestor que
     * derruba o boot por partição atrasada seria pior que a DEFAULT
     * absorvendo as linhas (o backstop existe exatamente pra isso).
     */
    public void ensureWeeklyPartitions(Instant now) {
        Objects.requireNonNull(now, "now");
        LocalDate week = LocalDate.ofInstant(now, ZoneOffset.UTC).with(DayOfWeek.MONDAY);
        for (int i = 0; i < 2; i++) {
            LocalDate from = week.plusWeeks(i);
            for (String parent : PARENTS) {
                createPartition(parent, from);
            }
        }
        warnAboutDefaultRows();
    }

    private void createPartition(String parent, LocalDate weekStart) {
        String name = "%s_%dw%02d".formatted(parent,
                weekStart.get(IsoFields.WEEK_BASED_YEAR), weekStart.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR));
        try {
            jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM ('%s 00:00:00+00') TO ('%s 00:00:00+00')"
                    .formatted(name, parent, weekStart, weekStart.plusWeeks(1)));
        } catch (DataAccessException e) {
            log.warn("could not create partition {} of {} — rows for its range are sitting in the DEFAULT partition; "
                    + "recovery: DETACH the default, CREATE the week, move the rows, ATTACH the default back "
                    + "(inserts keep landing in the default meanwhile — no outage)", name, parent, e);
        }
    }

    private void warnAboutDefaultRows() {
        for (String parent : PARENTS) {
            try {
                // EXISTS, não COUNT: a default deveria estar vazia; contar 1M de
                // linhas a cada housekeeping seria pagar caro pra confirmar o normal
                Boolean hasRows = jdbcTemplate.queryForObject(
                        "SELECT EXISTS (SELECT 1 FROM %s_default)".formatted(parent), Boolean.class);
                if (Boolean.TRUE.equals(hasRows)) {
                    log.warn("the DEFAULT partition of {} holds rows — the partition manager was down past a week boundary; "
                            + "they are served correctly but block that week's CREATE PARTITION until moved (see recovery above)",
                            parent);
                }
            } catch (DataAccessException e) {
                // o contrato "nunca derruba o boot" vale pro método INTEIRO:
                // default dropada/detached sem re-ATTACH é erro de operador a
                // reportar, não razão pra segurar o engine no chão
                log.warn("could not probe the DEFAULT partition of {} — it may have been dropped or detached "
                        + "without re-ATTACH; weekly partitions were still ensured", parent, e);
            }
        }
    }
}
