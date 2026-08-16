package io.mohs.rest;

import jakarta.servlet.http.HttpServletRequest;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.mohs.core.execution.Execution;
import io.mohs.rest.error.InvalidActorException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HeaderActorResolverTest {

    private final HeaderActorResolver resolver = new HeaderActorResolver();

    @Test
    void resolvesTheHeaderWhenPresent() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getHeader("X-Mohs-Actor")).thenReturn("ana.ops");

        assertThat(resolver.resolve(request)).isEqualTo("ana.ops");
    }

    @Test
    void fallsBackToAnonymousWhenHeaderIsAbsent() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getHeader("X-Mohs-Actor")).thenReturn(null);

        assertThat(resolver.resolve(request)).isEqualTo(ActorResolver.ANONYMOUS);
    }

    @Test
    void fallsBackToAnonymousWhenHeaderIsBlank() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getHeader("X-Mohs-Actor")).thenReturn("  ");

        assertThat(resolver.resolve(request)).isEqualTo(ActorResolver.ANONYMOUS);
    }

    /** actor é VARCHAR(255) no schema — header maior vira 400 que ensina na borda, nunca erro de INSERT respondido como 500. */
    @Test
    void rejectsAHeaderLongerThanTheActorColumn() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getHeader("X-Mohs-Actor")).thenReturn("a".repeat(256));

        assertThatThrownBy(() -> resolver.resolve(request))
                .isInstanceOf(InvalidActorException.class)
                .hasMessageContaining("255");
    }

    @Test
    void acceptsAHeaderExactlyAtTheColumnLimit() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        String actor = "a".repeat(255);
        Mockito.when(request.getHeader("X-Mohs-Actor")).thenReturn(actor);

        assertThat(resolver.resolve(request)).isEqualTo(actor);
    }

    /** ADR-0035: 'scheduler' é reservado do motor (load-bearing no rearme fixed-delay) — forjá-lo via header, em qualquer caixa, vira 400 que ensina. */
    @Test
    void rejectsTheReservedSchedulerActorInAnyCase() {
        for (String forged : new String[] {Execution.SCHEDULER_ACTOR, "Scheduler", "SCHEDULER"}) {
            HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
            Mockito.when(request.getHeader("X-Mohs-Actor")).thenReturn(forged);

            assertThatThrownBy(() -> resolver.resolve(request))
                    .isInstanceOf(InvalidActorException.class)
                    .hasMessageContaining("reserved for engine-fired occurrences");
        }
    }
}
