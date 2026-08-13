/**
 * Observação e extensão do ciclo de execução:
 * {@link io.mohs.event.ExecutionEvent} selado (8 variantes),
 * {@link io.mohs.event.ExecutionListener} (Observer),
 * {@link io.mohs.event.ExecutionInterceptor} (Chain of Responsibility) e o
 * açúcar {@link io.mohs.event.OnExecution @OnExecution}. Depende de
 * {@code io.mohs} (identidade) e {@code io.mohs.execution}
 * ({@code JobContext}, só em {@code ExecutionInterceptor}).
 */
package io.mohs.event;
