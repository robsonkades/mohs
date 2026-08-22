/**
 * Persistência JDBC interna para jobs, fila, posse, história, lotes
 * e rate limits — Data Mapper (PoEAA) sobre as portas de
 * {@code io.mohs.engine}: {@link io.mohs.store.jdbc.JdbcJobStore} implementa
 * {@link io.mohs.engine.JobStore};
 * {@link io.mohs.store.jdbc.JdbcWorkQueue} implementa
 * {@link io.mohs.engine.WorkQueue} — o claim move {@code mohs_ready} →
 * {@code mohs_lease} sob {@code SKIP LOCKED} (statement único no
 * Postgres, forma portátil nos demais);
 * {@link io.mohs.store.jdbc.JdbcLeaseStore} implementa
 * {@link io.mohs.engine.LeaseStore} — a conclusão do §7.5-3, cercada pelo
 * fence {@code (node_id, epoch)};
 * {@link io.mohs.store.jdbc.JdbcHistoryStore} implementa
 * {@link io.mohs.engine.HistoryStore} (payload serializado via Jackson,
 * nunca campo de {@code Execution}; estado derivado no read model);
 * {@link io.mohs.store.jdbc.JdbcStoreTransactions} implementa
 * {@link io.mohs.engine.StoreTransactions} (savepoint/NESTED — a unidade
 * de enqueue compõe com a transação do host);
 * {@link io.mohs.store.jdbc.JdbcBatchStore} implementa
 * {@link io.mohs.engine.BatchStore}, e
 * {@link io.mohs.store.jdbc.JdbcRateLimitStore} implementa
 * {@link io.mohs.engine.RateLimitStore}.
 * {@link io.mohs.store.jdbc.DatabaseClock}
 * também mora aqui — implementa {@code Clock} +
 * {@link io.mohs.engine.SyncableClock}, a implementação "database" da
 * ADR-0008; é a única classe do projeto onde ler o relógio de verdade é
 * o propósito, não uma violação (exceção nomeada em
 * {@code ArchitectureTest}). Não faz parte da API pública —
 * ver {@code io.mohs.core} para os contratos públicos.
 */
@NullMarked
package io.mohs.store.jdbc;

import org.jspecify.annotations.NullMarked;
