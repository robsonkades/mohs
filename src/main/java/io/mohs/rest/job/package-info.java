/**
 * Área de recurso "jobs": {@link io.mohs.rest.job.JobsController}
 * (definições registradas, invocação, pause/resume, histórico) e seus DTOs
 * — {@link io.mohs.rest.job.JobResponse}, o {@link io.mohs.rest.job.ScheduleView}
 * selado (espelha {@link io.mohs.core.schedule.Schedule}) e
 * {@link io.mohs.rest.job.ScheduleJobRequest}. Depende de
 * {@code io.mohs.core.definition}/{@code .schedule} (tipos reaproveitados
 * direto: {@code Misfire}, {@code DefinitionSource}) e de
 * {@code io.mohs.rest.execution} ({@code ExecutionResponse}, só no
 * histórico de execuções por job).
 */
@NullMarked
package io.mohs.rest.job;

import org.jspecify.annotations.NullMarked;
