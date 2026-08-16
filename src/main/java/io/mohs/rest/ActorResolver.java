package io.mohs.rest;

import jakarta.servlet.http.HttpServletRequest;

/**
 * Resolve quem disparou uma ação mutável pela API REST — GoF Strategy: com
 * segurança plugada, o principal autenticado; sem ela (v1), atribuição
 * declarativa via o header {@code X-Mohs-Actor} (ver
 * {@link HeaderActorResolver}). "Quem disparou" é inegociável em toda
 * invocação (ver {@code docs/adr/0010-rest-api-v1.md}).
 *
 * <p>Registrar a implementação como bean é responsabilidade de
 * {@code io.mohs.autoconfigure} (M3) — este contrato só congela a SPI.
 */
public interface ActorResolver {

    String ANONYMOUS = "anonymous";

    /**
     * Nunca devolva {@link io.mohs.core.execution.Execution#SCHEDULER_ACTOR}
     * (nem variação de caixa): é nome reservado do motor (ADR-0035) —
     * {@code ScheduleCommand.as} o rejeita, e a rejeição de um valor vindo
     * desta SPI aparece como 500 genérico, não o 400 que a borda dá. Um
     * principal autenticado com esse nome (ex.: service account
     * "scheduler") precisa ser mapeado para outro identificador aqui.
     */
    String resolve(HttpServletRequest request);
}
