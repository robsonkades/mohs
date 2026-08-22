/**
 * Motor de execução interno (claim, dispatch, retry, misfire). Não faz
 * parte da API pública — ver {@code io.mohs.core} para os contratos
 * públicos.
 *
 * <p>As portas de persistência (Repository/Data Mapper, PoEAA) que
 * {@code io.mohs.store.jdbc} implementa — este pacote não conhece JDBC, só
 * os contratos: {@link io.mohs.engine.JobStore} (definições;
 * {@link io.mohs.engine.StoredJob} combina
 * {@link io.mohs.core.definition.JobDefinition} com o estado operacional
 * que a ADR-0006 mantém separado do definicional),
 * {@link io.mohs.engine.WorkQueue} (a fila {@code mohs_ready} — claim,
 * requeue, cancel de enfileirado, rearme manual),
 * {@link io.mohs.engine.LeaseStore} (a posse {@code mohs_lease} com o
 * fence {@code (node_id, epoch)} e a transação de conclusão do §7.5-3),
 * {@link io.mohs.engine.HistoryStore} (história
 * {@code mohs_execution}/{@code mohs_attempt} e o read model derivado),
 * {@link io.mohs.engine.StoreTransactions} (a unidade de enqueue do
 * §7.5-1), {@link io.mohs.engine.BatchStore} (contadores de lote,
 * {@link io.mohs.engine.BatchCounters}),
 * {@link io.mohs.engine.RateLimitStore} e
 * {@link io.mohs.engine.NodeStore} (heartbeat/lease do NÓ, ADR-0051 —
 * {@link io.mohs.engine.StoredNode} é sua leitura).
 * {@link io.mohs.engine.SyncableClock} é a mesma ideia aplicada ao
 * {@code Clock}: a porta mora aqui, {@code DatabaseClock}
 * (io.mohs.store.jdbc) implementa — o motor nunca importa
 * {@code NamedParameterJdbcTemplate}/{@code DataSource}.
 *
 * <p>{@link io.mohs.engine.Dispatcher} invoca o handler de uma execução
 * reivindicada, via {@link io.mohs.engine.HandlerRegistry} (registro
 * {@code JobKey → JobHandler} em memória, povoado por
 * {@code io.mohs.autoconfigure}), e publica
 * {@link io.mohs.core.event.ExecutionEvent} através de
 * {@link io.mohs.engine.ExecutionEventPublisher}; a conclusão pode passar
 * pelo group commit do {@link io.mohs.engine.CompletionBatcher}
 * (ADR-0047).
 *
 * <p>{@link io.mohs.engine.Engine} é quem liga tudo: o poll loop de
 * intervalo fixo (heartbeat do nó + reaper de leases de nós mortos +
 * reconciliação + triggers + claim com admissão §5.4 + dispatch a cada
 * tick) e implementa {@link io.mohs.core.MohsLifecycle} diretamente — a
 * mesma máquina de estados da ADR-0007.
 * {@link io.mohs.engine.MohsExecutors} é a fábrica central de
 * executor/scheduler que {@code Engine}/{@code Dispatcher}/
 * {@code ExecutionEventPublisher} recebem injetado: nenhuma classe deste
 * pacote cria {@code Executor}/{@code ScheduledExecutorService} na
 * própria mão.
 */
@NullMarked
package io.mohs.engine;

import org.jspecify.annotations.NullMarked;
