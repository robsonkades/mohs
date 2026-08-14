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
 * {@link io.mohs.rest.AcceptedExecutionResponse}, o envelope de ajuste
 * runtime {@link io.mohs.rest.RuntimePatchResponse} e o prefixo
 * compartilhado {@link io.mohs.rest.ApiPaths#V1}. Não tem controller
 * próprio — cada área de recurso da tabela de
 * {@code docs/REST-API-DESIGN.md} tem um subpacote 1:1 com seu controller:
 * {@code io.mohs.rest.error} (tradução de exceção pra
 * {@code ProblemDetail}, usada por todos, também sem controller),
 * {@code io.mohs.rest.overview}, {@code io.mohs.rest.job},
 * {@code io.mohs.rest.execution}, {@code io.mohs.rest.batch},
 * {@code io.mohs.rest.ratelimit}, {@code io.mohs.rest.runner},
 * {@code io.mohs.rest.node}.
 *
 * <p>Convenção de sufixo de DTO — {@code *Response} é o default, usado tanto
 * para o corpo direto de um endpoint quanto para um DTO aninhado dentro de
 * outro (ex. {@link io.mohs.rest.execution.AttemptResponse}, aninhado em
 * {@code ExecutionResponse.attempts()} — aninhamento não é o critério que
 * decide o sufixo). {@code *View} fica reservado para dois casos: a
 * wire-adaptação de um tipo {@code sealed} do domínio, espelhando suas
 * variantes 1:1 (ex. {@link io.mohs.rest.job.ScheduleView}, que espelha
 * {@link io.mohs.core.schedule.Schedule}), ou uma projeção computada sem
 * entidade correspondente em {@code io.mohs.core} (ex.
 * {@link io.mohs.rest.overview.ThroughputView} — não existe um tipo
 * {@code Throughput} no domínio). Fora desses dois casos, todo DTO novo usa
 * {@code *Response}.
 */
@NullMarked
package io.mohs.rest;

import org.jspecify.annotations.NullMarked;
