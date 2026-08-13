/**
 * Modelo de execução: {@link io.mohs.core.execution.Execution},
 * {@link io.mohs.core.execution.Attempt}, {@link io.mohs.core.execution.ExecutionState},
 * {@link io.mohs.core.execution.JobContext} (parâmetro opcional do handler) e
 * {@link io.mohs.core.execution.Priority}. Depende só de {@code io.mohs.core.job}
 * ({@code JobKey}); {@link io.mohs.core.execution.ExecutionId} mora aqui —
 * seu alcance não passa de {@code io.mohs.core.execution} e
 * {@code io.mohs.core.event}, que dependem daqui.
 */
@NullMarked
package io.mohs.core.execution;

import org.jspecify.annotations.NullMarked;
