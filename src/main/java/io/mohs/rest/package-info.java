/**
 * API REST operacional (M2 — contrato; ver
 * {@code docs/adr/0010-rest-api-v1.md}). Depende só da API pública em
 * {@code io.mohs.core} — nunca diretamente de {@code io.mohs.engine} ou
 * {@code io.mohs.jdbc} (regra ArchUnit).
 *
 * <p>Este pacote carrega só o que é transversal — usado por mais de um
 * subpacote de recurso, então não pertence a nenhum em particular:
 * {@link io.mohs.rest.ActorResolver} (SPI) e sua implementação padrão
 * {@link io.mohs.rest.HeaderActorResolver}, o envelope de paginação
 * {@link io.mohs.rest.CursorPage}, o recibo de invocação
 * {@link io.mohs.rest.AcceptedExecutionResponse} e o envelope de ajuste
 * runtime {@link io.mohs.rest.RuntimePatchResponse}. Não tem controller
 * próprio — cada área de recurso da tabela de
 * {@code docs/REST-API-DESIGN.md} tem um subpacote 1:1 com seu controller:
 * {@code io.mohs.rest.error} (tradução de exceção pra
 * {@code ProblemDetail}, usada por todos, também sem controller),
 * {@code io.mohs.rest.overview}, {@code io.mohs.rest.job},
 * {@code io.mohs.rest.execution}, {@code io.mohs.rest.batch},
 * {@code io.mohs.rest.queue}, {@code io.mohs.rest.ratelimit},
 * {@code io.mohs.rest.runner}, {@code io.mohs.rest.node}.
 */
@NullMarked
package io.mohs.rest;

import org.jspecify.annotations.NullMarked;
