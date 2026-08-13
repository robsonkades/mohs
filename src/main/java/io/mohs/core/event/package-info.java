/**
 * Observação e extensão do ciclo de execução:
 * {@link io.mohs.core.event.ExecutionEvent} selado (8 variantes),
 * {@link io.mohs.core.event.ExecutionListener} (Observer),
 * {@link io.mohs.core.event.ExecutionInterceptor} (Chain of Responsibility) e o
 * açúcar {@link io.mohs.core.event.OnExecution @OnExecution}. Depende de
 * {@code io.mohs.core} (identidade) e {@code io.mohs.core.execution}
 * ({@code JobContext}, só em {@code ExecutionInterceptor}).
 */
@NullMarked
package io.mohs.core.event;

import org.jspecify.annotations.NullMarked;
