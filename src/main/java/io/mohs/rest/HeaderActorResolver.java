package io.mohs.rest;

import jakarta.servlet.http.HttpServletRequest;

import io.mohs.core.execution.Execution;
import io.mohs.rest.error.InvalidActorException;

/**
 * {@link ActorResolver} declarativo, não autenticado — lê o header
 * {@code X-Mohs-Actor}; ausente, cai para {@link ActorResolver#ANONYMOUS}.
 * Registrar como bean é decisão de {@code io.mohs.autoconfigure} (M3); esta
 * classe só congela a lógica pra quando esse fiação existir.
 */
public final class HeaderActorResolver implements ActorResolver {

    private static final String ACTOR_HEADER = "X-Mohs-Actor";

    /** Teto da coluna {@code actor} (VARCHAR(255) no schema) — validado aqui pra virar 400 que ensina, não erro de INSERT (500). */
    private static final int MAX_ACTOR_LENGTH = 255;

    @Override
    public String resolve(HttpServletRequest request) {
        String actor = request.getHeader(ACTOR_HEADER);
        if (actor == null || actor.isBlank()) {
            return ANONYMOUS;
        }
        if (actor.length() > MAX_ACTOR_LENGTH) {
            throw new InvalidActorException(
                    ACTOR_HEADER + " must be at most " + MAX_ACTOR_LENGTH + " characters, got " + actor.length());
        }
        // validado aqui pelo mesmo motivo do teto de tamanho: 400 que ensina, não o
        // IllegalArgumentException de ScheduleCommand.as virando 500 no handler genérico.
        // Case/espaço-insensível pela mesma razão do guard de as(): a cura do upsert
        // compara actor no banco, cuja collation default pode ser case-insensitive
        if (Execution.SCHEDULER_ACTOR.equalsIgnoreCase(actor.strip())) {
            throw new InvalidActorException(ACTOR_HEADER + " must not be '" + Execution.SCHEDULER_ACTOR
                    + "' — reserved for engine-fired occurrences (ADR-0035); identify the real caller or omit the header");
        }
        return actor;
    }
}
