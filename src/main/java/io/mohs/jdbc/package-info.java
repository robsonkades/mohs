/**
 * Persistência JDBC interna para jobs, execuções e filas — Data Mapper
 * (PoEAA) sobre as portas de {@code io.mohs.engine}
 * ({@link io.mohs.jdbc.JdbcJobStore} implementa
 * {@link io.mohs.engine.JobStore}). Não faz parte da API pública — ver
 * {@code io.mohs.core} para os contratos públicos.
 */
@NullMarked
package io.mohs.jdbc;

import org.jspecify.annotations.NullMarked;
