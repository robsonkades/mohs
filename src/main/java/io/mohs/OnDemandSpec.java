package io.mohs;

/**
 * Sem gatilho automático — o job só dispara via {@link Mohs#schedule},
 * {@link Mohs#batch} ou o dashboard. Explícito em vez de "sem agenda
 * definida", para que um job sem cron/every seja uma escolha deliberada,
 * não um esquecimento (ver
 * {@code docs/adr/0002-definition-vs-invocation.md}).
 */
public record OnDemandSpec() implements Schedule {
}
