package io.mohs.jdbc.dialect;

import java.time.Instant;
import java.util.List;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * As poucas divergências reais de dialeto que {@code io.mohs.jdbc}
 * precisa (ADR-0023) — modelo na forma do {@code LimitHandler}/
 * {@code LockingStrategy} do Hibernate (interfaces pequenas, uma
 * preocupação cada), sem trazer o Hibernate como dependência: essas
 * duas interfaces vivem dentro do {@code Dialect}, que só existe depois
 * de inicializar {@code SessionFactory}/{@code ServiceRegistry} —
 * usá-las isoladamente significaria adotar boa parte do framework mesmo
 * assim.
 *
 * <p>Cada implementação é dona do template SQL inteiro de {@code
 * io.mohs.jdbc.JdbcClaimer#claim}, não de fragmentos concatenáveis — o
 * {@code TOP} do SQL Server muda de <b>posição</b> na query (logo após
 * {@code SELECT}, não no fim como {@code LIMIT}), então uma composição
 * de fragmentos genéricos não fecha limpo. Mesmo padrão que o próprio
 * Quartz usa ({@code StdJDBCDelegate}/{@code MSSQLDelegate}: cada
 * Delegate tem o SQL completo de cada operação) e como Hibernate
 * realmente implementa {@code LimitHandler} por baixo (recebe o SQL e
 * devolve o SQL reescrito, não um fragmento).
 *
 * <p>Escolha explícita, nunca auto-detecção — mesmo padrão do Quartz
 * ({@code org.quartz.jobStore.driverDelegateClass}): detectar por
 * {@code Connection.getMetaData()} é frágil entre forks/versões de
 * driver.
 */
public interface JdbcDialect {

    /**
     * Template ANSI compartilhado por H2/Postgres/MySQL — os três nasceram
     * byte a byte idênticos e nunca divergiram; a constante elimina os 3
     * clones sem tirar de nenhum dialeto a liberdade de divergir depois
     * (basta parar de usá-la). SQL Server não a usa: {@code TOP} muda de
     * posição e o lock é hint de tabela (ver Javadoc da interface).
     * {@code e.priority} já é {@code Priority.value()} (menor reivindica
     * primeiro) — {@code NOT NULL DEFAULT 20} no schema, ordena direto,
     * sem {@code CASE}. {@code j.retired = FALSE}: job aposentado
     * ({@code Mohs.remove}) nunca volta a ser candidato.
     */
    String ANSI_SKIP_LOCKED_CANDIDATES = """
            SELECT e.id AS id, e.job_key AS job_key,
                   j.allow_concurrent_executions AS allow_concurrent_executions,
                   j.window_name AS window_name
            FROM mohs_executions e
            JOIN mohs_job_definitions j ON j.job_key = e.job_key
            WHERE e.state IN ('ENQUEUED', 'RETRY_SCHEDULED')
              AND e.scheduled_at <= :now
              AND j.retired = FALSE
              AND (j.allow_concurrent_executions = TRUE OR j.running_execution_count < j.max_concurrent_executions)
            ORDER BY e.priority ASC, e.scheduled_at ASC
            LIMIT :batchSize
            FOR UPDATE OF e SKIP LOCKED
            """;

    List<Candidate> selectCandidates(NamedParameterJdbcTemplate jdbcTemplate, Instant now, int batchSize);

    /**
     * Segunda divergência real de dialeto (a primeira é {@link #selectCandidates}):
     * {@code io.mohs.jdbc.JdbcExecutionStore#findPage} precisa do mesmo
     * teto de linhas em todo dialeto, sem lock nenhum — query simples
     * demais pra justificar 4 templates completos como {@code
     * selectCandidates}. {@code :limit} é o mesmo parâmetro nomeado nos
     * dois métodos; só a posição do texto na query muda. Default cobre
     * H2/Postgres/MySQL ({@code LIMIT} no fim); SQL Server sobrescreve os
     * dois juntos.
     */
    default String topClause() {
        return "";
    }

    default String limitClause() {
        return "LIMIT :limit";
    }
}
