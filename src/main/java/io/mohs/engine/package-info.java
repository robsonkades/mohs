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
 */
@NullMarked
package io.mohs.engine;

import org.jspecify.annotations.NullMarked;
