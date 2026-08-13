package io.mohs.rest;

import jakarta.servlet.http.HttpServletRequest;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

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
}
