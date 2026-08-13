/**
 * API REST operacional (M2 — contrato; ver
 * {@code docs/adr/0010-rest-api-v1.md}). Depende só da API pública em
 * {@code io.mohs.core} — nunca diretamente de {@code io.mohs.engine} ou
 * {@code io.mohs.jdbc} (regra ArchUnit).
 *
 * <p>Este pacote carrega o que é transversal aos subpacotes de recurso:
 * {@link io.mohs.rest.ActorResolver} (SPI), o envelope de paginação
 * {@link io.mohs.rest.CursorPage} e o recibo de invocação
 * {@link io.mohs.rest.AcceptedExecutionResponse}, além do endpoint
 * âncora {@code GET /overview}. Cada área de recurso tem seu próprio
 * subpacote: {@code io.mohs.rest.error} (tradução pra
 * {@code ProblemDetail}), {@code io.mohs.rest.job}, {@code
 * io.mohs.rest.execution} (inclui batches) e {@code io.mohs.rest.resource}
 * (queues, rate-limits, runners, nodes — eco temático de
 * {@code io.mohs.core.resource}).
 */
@NullMarked
package io.mohs.rest;

import org.jspecify.annotations.NullMarked;
