/**
 * Área de recurso "nodes": {@link io.mohs.rest.node.NodesController}
 * ({@code GET /nodes}, visão de cluster) e seu DTO,
 * {@link io.mohs.rest.node.NodeResponse}. Leitura sobre o registro de
 * heartbeat que a liveness (M3) constrói. Depende de {@code io.mohs.core}
 * (tipo reaproveitado direto: {@code EngineState}).
 */
@NullMarked
package io.mohs.rest.node;

import org.jspecify.annotations.NullMarked;
