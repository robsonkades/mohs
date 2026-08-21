/**
 * Área de recurso "runners": {@link io.mohs.rest.runner.RunnersController}
 * ({@code GET /runners}, só leitura — runner é config, não runtime
 * ajustável) e seu DTO, {@link io.mohs.rest.runner.RunnerResponse}. Depende
 * de {@code io.mohs.core.resource} (tipo reaproveitado direto: {@code RunnerMode}).
 */
@NullMarked
package io.mohs.rest.runner;

import org.jspecify.annotations.NullMarked;
