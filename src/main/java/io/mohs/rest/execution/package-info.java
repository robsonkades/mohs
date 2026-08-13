/**
 * Área de recurso "executions": {@link io.mohs.rest.execution.ExecutionsController}
 * e seus DTOs, {@link io.mohs.rest.execution.ExecutionResponse} e
 * {@link io.mohs.rest.execution.AttemptResponse}. Batches tem pacote
 * próprio, {@code io.mohs.rest.batch} — 1:1 com {@code BatchesController},
 * mesmo granularidade do resto de {@code io.mohs.rest}. Depende de
 * {@code io.mohs.core.execution} (tipo reaproveitado direto:
 * {@code ExecutionState}).
 */
@NullMarked
package io.mohs.rest.execution;

import org.jspecify.annotations.NullMarked;
