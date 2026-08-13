package io.mohs.core.schedule;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.JobSpec;

/**
 * Quando um job dispara: cron, fixed-rate, fixed-delay ou sob demanda.
 * Selado para que o motor faça switch exaustivo sobre as três variantes —
 * adicionar um quarto tipo de agenda é erro de compilação em todo ponto de
 * uso até ser tratado, não um {@code default} silencioso.
 *
 * <p>Este tipo carrega só o gatilho. Políticas do job (runner, queue,
 * window, misfire, retries, timeout) vivem em {@link JobDefinition}; ver
 * {@link JobSpec} para o builder staged que monta os dois.
 */
public sealed interface Schedule permits CronSpec, IntervalSpec, OnDemandSpec {
}
