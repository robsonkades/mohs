/**
 * Área de recurso "capacidade" — visibilidade e ajuste runtime dos specs
 * de {@code io.mohs.core.resource}: {@link io.mohs.rest.resource.QueuesController},
 * {@link io.mohs.rest.resource.RateLimitsController} (cluster-wide,
 * suportam {@code PATCH}), {@link io.mohs.rest.resource.RunnersController}
 * (node-local, só leitura) e {@link io.mohs.rest.resource.NodesController}
 * (visão de cluster sobre o registro de heartbeat). Eco temático — não
 * estrutural — de {@code io.mohs.core.resource}: lá são specs de boot;
 * aqui é leitura/ajuste do estado runtime desses specs. Depende de
 * {@code io.mohs.core.resource} (tipos reaproveitados direto:
 * {@code RunnerMode}) e {@code io.mohs.core} ({@code EngineState}).
 */
@NullMarked
package io.mohs.rest.resource;

import org.jspecify.annotations.NullMarked;
