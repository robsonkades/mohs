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

    List<Candidate> selectCandidates(NamedParameterJdbcTemplate jdbcTemplate, Instant now, int batchSize);
}
