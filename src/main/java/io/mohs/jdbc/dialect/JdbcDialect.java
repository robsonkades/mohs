package io.mohs.jdbc.dialect;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import io.mohs.jdbc.JdbcTimestamps;

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
     * CAS final pra {@code RUNNING} — a garantia real da ADR-0018 contra
     * double-claim, independente do lock de {@link #selectCandidates}. Os
     * dois estados claimáveis são o mesmo par do template de candidatos
     * (ADR-0033). {@code scheduled_at <= :now} reverificado aqui porque o
     * retry o tornou mutável: entre o SELECT e este CAS, outro nó pode
     * reivindicar o candidato, falhar rápido e reagendá-lo pro futuro —
     * sem a guarda, o backoff seria furado (ADR-0018: toda condição de
     * elegibilidade que muda entre SELECT e CAS pertence ao CAS).
     * {@code UPDATE} simples, sem sintaxe divergente — compartilhado pelos
     * 4 dialetos via {@link #transitionToRunning}.
     */
    String ANSI_TRANSITION_TO_RUNNING = """
            UPDATE mohs_executions
            SET state = 'RUNNING', lease_expires_at = :leaseExpiresAt, node_id = :nodeId
            WHERE id = :id AND state IN ('ENQUEUED', 'RETRY_SCHEDULED') AND scheduled_at <= :now
            """;

    /**
     * Terceira divergência real de dialeto (DBTUNE-16): executa o CAS de
     * {@link #ANSI_TRANSITION_TO_RUNNING} pra cada id e devolve os que
     * venceram — subconjunto de {@code ids}, <b>ordem não garantida</b>
     * ({@code io.mohs.jdbc.JdbcClaimer} reordena pela ordem dos
     * candidatos). {@code ids} nunca pode ser vazio: a expansão de
     * {@code IN (:ids)} do override do Postgres não aceita coleção vazia
     * — o chamador guarda, e é quem deve continuar guardando. Default:
     * um {@code UPDATE} por id, atômico por
     * construção em qualquer banco — o comportamento que todos os dialetos
     * tinham antes. Postgres sobrescreve com um único
     * {@code UPDATE ... RETURNING}: mesmas guardas por linha, N round
     * trips viram 1.
     */
    default List<String> transitionToRunning(NamedParameterJdbcTemplate jdbcTemplate, List<String> ids, String nodeId,
            Instant now, Instant leaseExpiresAt) {
        List<String> claimed = new ArrayList<>(ids.size());
        for (String id : ids) {
            int updated = jdbcTemplate.update(ANSI_TRANSITION_TO_RUNNING, new MapSqlParameterSource()
                    .addValue("leaseExpiresAt", JdbcTimestamps.toUtcTimestamp(leaseExpiresAt))
                    .addValue("nodeId", nodeId)
                    .addValue("now", JdbcTimestamps.toUtcTimestamp(now))
                    .addValue("id", id));
            if (updated == 1) {
                claimed.add(id);
            }
        }
        return claimed;
    }

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
