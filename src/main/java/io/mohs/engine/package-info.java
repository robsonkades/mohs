/**
 * Motor de execução interno (claim, dispatch, retry, misfire). Não faz
 * parte da API pública — ver {@code io.mohs.core} para os contratos
 * públicos.
 *
 * <p>{@link io.mohs.engine.JobStore} é a porta (Repository, PoEAA) que
 * {@code io.mohs.jdbc} implementa — este pacote não conhece JDBC, só o
 * contrato. {@link io.mohs.engine.StoredJob} combina
 * {@link io.mohs.core.definition.JobDefinition} com o estado operacional
 * que a ADR-0006 mantém separado do definicional.
 * {@link io.mohs.engine.SyncableClock} é a mesma ideia aplicada ao
 * {@code Clock}: a porta mora aqui, {@code DatabaseClock}
 * (io.mohs.jdbc) implementa — o motor nunca importa
 * {@code NamedParameterJdbcTemplate}/{@code DataSource}.
 * {@link io.mohs.engine.ExecutionStore} e {@link io.mohs.engine.BatchStore}
 * seguem o mesmo padrão pra {@link io.mohs.core.execution.Execution} e
 * contadores de lote ({@link io.mohs.engine.BatchCounters}).
 * {@link io.mohs.engine.QueueStore} (com {@link io.mohs.engine.StoredQueue},
 * mesma separação definicional×operacional de {@link io.mohs.engine.StoredJob})
 * e {@link io.mohs.engine.RateLimitStore} fecham as portas de persistência
 * da etapa 2 de M3.
 *
 * <p>{@link io.mohs.engine.Claimer} é a etapa 3a: aquisição sem contenção
 * (claim), diferente dos {@code *Store} acima porque cruza três tabelas
 * numa transação só — mutex por job e admissão de queue decididos dentro
 * dela via CAS guardado, não lock especializado (ADR-0016, ADR-0018).
 */
@NullMarked
package io.mohs.engine;

import org.jspecify.annotations.NullMarked;
