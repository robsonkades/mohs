/**
 * Motor de execução interno (claim, dispatch, retry, misfire). Não faz
 * parte da API pública — ver {@code io.mohs.core} para os contratos
 * públicos.
 *
 * <p>{@link io.mohs.engine.JobStore} é a porta (Repository, PoEAA) que
 * {@code io.mohs.store.jdbc} implementa — este pacote não conhece JDBC, só o
 * contrato. {@link io.mohs.engine.StoredJob} combina
 * {@link io.mohs.core.definition.JobDefinition} com o estado operacional
 * que a ADR-0006 mantém separado do definicional.
 * {@link io.mohs.engine.SyncableClock} é a mesma ideia aplicada ao
 * {@code Clock}: a porta mora aqui, {@code DatabaseClock}
 * (io.mohs.store.jdbc) implementa — o motor nunca importa
 * {@code NamedParameterJdbcTemplate}/{@code DataSource}.
 * {@link io.mohs.engine.ExecutionStore} e {@link io.mohs.engine.BatchStore}
 * seguem o mesmo padrão pra {@link io.mohs.core.execution.Execution} e
 * contadores de lote ({@link io.mohs.engine.BatchCounters}).
 * {@link io.mohs.engine.RateLimitStore} fecha as portas de persistência
 * da etapa 2 de M3.
 *
 * <p>{@link io.mohs.engine.Claimer} é a etapa 3a: aquisição sem contenção
 * (claim), diferente dos {@code *Store} acima porque cruza duas tabelas
 * numa transação só — mutex por job decidido dentro dela via CAS guardado,
 * não lock especializado (ADR-0016, ADR-0018, ADR-0020).
 *
 * <p>{@link io.mohs.engine.Dispatcher} é a etapa 3b que o Javadoc de
 * {@code Claimer} já deixava em aberto: invoca o handler de uma execução
 * já {@code RUNNING}, via {@link io.mohs.engine.HandlerRegistry} (registro
 * {@code JobKey → JobHandler} em memória — quem povoa escaneando
 * {@code @MohsJob} é {@code io.mohs.autoconfigure}, ainda não construído)
 * e publica {@link io.mohs.core.event.ExecutionEvent} através de
 * {@link io.mohs.engine.ExecutionEventPublisher} (package-private, único
 * consumidor hoje é {@code Dispatcher}).
 *
 * <p>{@link io.mohs.engine.Reaper} é a etapa de liveness (ADR-0012):
 * reclama {@code Execution RUNNING} cuja lease expirou — mesmo motivo de
 * {@code Claimer} pra não ser um {@code *Store} (cruza
 * {@code ExecutionStore} e {@code JobStore} numa única operação).
 * {@link io.mohs.engine.NodeStore} é o registro de heartbeat por node —
 * só informativo, nenhuma lógica de claim/reclaim consulta — e
 * {@link io.mohs.engine.StoredNode} é sua leitura.
 *
 * <p>{@link io.mohs.engine.Engine} é quem finalmente liga os quatro:
 * nenhum de {@code Claimer}/{@code Dispatcher}/{@code NodeStore}/
 * {@code Reaper} tinha chamador em produção antes dele. É o poll loop de
 * intervalo fixo (heartbeat + reclaim + claim + dispatch a cada tick) e
 * implementa {@link io.mohs.core.MohsLifecycle} diretamente — a mesma
 * máquina de estados da ADR-0007. {@link io.mohs.engine.MohsExecutors} é
 * a fábrica central de executor/scheduler que {@code Engine}/
 * {@code Dispatcher}/{@code ExecutionEventPublisher} recebem injetado:
 * nenhuma classe deste pacote cria {@code Executor}/
 * {@code ScheduledExecutorService} na própria mão.
 */
@NullMarked
package io.mohs.engine;

import org.jspecify.annotations.NullMarked;
