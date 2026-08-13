/**
 * Persistência JDBC interna para jobs, execuções, attempts, lotes e
 * filas — Data Mapper (PoEAA) sobre as portas de {@code io.mohs.engine}:
 * {@link io.mohs.jdbc.JdbcJobStore} implementa
 * {@link io.mohs.engine.JobStore}, {@link io.mohs.jdbc.JdbcExecutionStore}
 * implementa {@link io.mohs.engine.ExecutionStore} (payload serializado
 * via Jackson, nunca campo de {@code Execution}), e
 * {@link io.mohs.jdbc.JdbcBatchStore} implementa
 * {@link io.mohs.engine.BatchStore}. {@link io.mohs.jdbc.DatabaseClock}
 * também mora aqui — implementa {@code Clock} +
 * {@link io.mohs.engine.SyncableClock}, a implementação "database" da
 * ADR-0008; é a única classe do projeto onde ler o relógio de verdade é
 * o propósito, não uma violação (exceção nomeada em
 * {@code ArchitectureTest}). Não faz parte da API pública — ver
 * {@code io.mohs.core} para os contratos públicos.
 */
@NullMarked
package io.mohs.jdbc;

import org.jspecify.annotations.NullMarked;
