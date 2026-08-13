/**
 * Área de recurso "executions" (inclui batches):
 * {@link io.mohs.rest.execution.ExecutionsController} e
 * {@link io.mohs.rest.execution.BatchesController} — batches entra aqui,
 * não em pacote próprio, mesmo eco de {@code BatchCompleted} já morar em
 * {@code io.mohs.core.event} junto dos outros eventos de execução em vez
 * de um pacote "batch" à parte. DTOs:
 * {@link io.mohs.rest.execution.ExecutionResponse},
 * {@link io.mohs.rest.execution.AttemptResponse},
 * {@link io.mohs.rest.execution.BatchResponse}. Depende de
 * {@code io.mohs.core.execution} (tipos reaproveitados direto:
 * {@code ExecutionState}).
 */
@NullMarked
package io.mohs.rest.execution;

import org.jspecify.annotations.NullMarked;
