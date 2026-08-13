package io.mohs.rest;

import jakarta.servlet.http.HttpServletRequest;

/**
 * {@link ActorResolver} declarativo, não autenticado — lê o header
 * {@code X-Mohs-Actor}; ausente, cai para {@link ActorResolver#ANONYMOUS}.
 * Registrar como bean é decisão de {@code io.mohs.autoconfigure} (M3); esta
 * classe só congela a lógica pra quando esse fiação existir.
 */
public final class HeaderActorResolver implements ActorResolver {

    private static final String ACTOR_HEADER = "X-Mohs-Actor";

    @Override
    public String resolve(HttpServletRequest request) {
        String actor = request.getHeader(ACTOR_HEADER);
        return actor == null || actor.isBlank() ? ANONYMOUS : actor;
    }
}
