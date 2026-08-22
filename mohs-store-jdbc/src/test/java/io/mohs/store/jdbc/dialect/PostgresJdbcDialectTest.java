package io.mohs.store.jdbc.dialect;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Teste puro de string, sem banco — deliberadamente fora de
 * {@code io.mohs.store.jdbc.JdbcClaimerPostgresTest}, que paga Testcontainers
 * no setup.
 */
class PostgresJdbcDialectTest {

    /**
     * DBTUNE-16: o CAS em lote é o template ANSI com o predicado de id
     * trocado e {@code RETURNING} — as guardas do WHERE são o núcleo de
     * corretude da ADR-0018, e a ADR prevê que guardas novas entrem no CAS
     * (foi assim que {@code scheduled_at <= :now} chegou, via ADR-0033).
     * Guarda nova que entrar no ANSI e não entrar na gêmea do Postgres
     * quebra este build, não a produção — cópia manual já sofreu drift
     * duas vezes neste projeto (nota de metodologia da rodada 08-15 do
     * BASELINE.md).
     */
    @Test
    void batchCasIsTheAnsiCasWithSetPredicateAndReturning() {
        assertThat(PostgresJdbcDialect.BATCH_TRANSITION_TO_RUNNING)
                .isEqualTo(JdbcDialect.ANSI_TRANSITION_TO_RUNNING
                        .replace("WHERE id = :id AND", "WHERE id IN (:ids) AND")
                        + "RETURNING id\n");
    }
}
