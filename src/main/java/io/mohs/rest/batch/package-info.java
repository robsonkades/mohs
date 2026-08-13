/**
 * Área de recurso "batches": {@link io.mohs.rest.batch.BatchesController}
 * ({@code GET /batches/{id}}) e seus DTOs —
 * {@link io.mohs.rest.batch.BatchResponse}, {@link io.mohs.rest.batch.BatchState}.
 * Pacote próprio, 1:1 com o controller, apesar de {@code BatchCompleted}
 * morar dentro de {@code io.mohs.core.event} junto dos outros eventos de
 * execução no lado do domínio — a granularidade aqui segue o controller,
 * não o agrupamento do core.
 */
@NullMarked
package io.mohs.rest.batch;

import org.jspecify.annotations.NullMarked;
