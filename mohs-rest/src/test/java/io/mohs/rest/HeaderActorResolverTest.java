/*
 * Copyright 2026 The Mohs Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mohs.rest;

import jakarta.servlet.http.HttpServletRequest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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

    /** actor is VARCHAR(255) in the schema — a longer header becomes a 400 that teaches at the boundary, never an INSERT error answered as a 500. */
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

    /** 'scheduler' is the engine's reserved actor (load-bearing in the fixed-delay rearm) — forging it through the header, in any casing, becomes a 400 that teaches. */
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
    /**
     * The actor is the only caller-supplied string Mohs persists AND writes into the audit trail, so
     * it validates content. The rule denies the THREAT (C0/C1 controls and directional overrides)
     * rather than permitting a shape: the first version was an allowlist of {@code \u005Cp{Print}},
     * which in Java is pure US-ASCII and rejected "José" in NFD, the typographic em dash and
     * Arabic-Indic digits — a person's name turning into a 400.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "deploy-bot",
            "Jos\u00E9",                       // NFC
            "Jose\u0301",                      // NFD — what macOS and SSO produce
            "Anne\u2013Marie",                 // a typographic en dash
            "\u0418\u0432\u0430\u043D",  // Cyrillic
            "\u65E5\u672C",                  // CJK
            "\u0660\u0661",                  // Arabic-Indic digits
            "bot \uD83E\uDD16"               // emoji (par surrogate)
    })
    void acceptsAnyHumanIdentity(String actor) {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getHeader("X-Mohs-Actor")).thenReturn(actor);

        assertThat(resolver.resolve(request)).isEqualTo(actor);
    }

    /** Controls and directional overrides tamper with how the trail READS — that is what the rule exists to block. */
    @ParameterizedTest
    @ValueSource(strings = {
            "a\u001B[31mvermelho",   // an ANSI sequence in an operator's terminal
            "a\u202Eb",              // RIGHT-TO-LEFT OVERRIDE
            "a\u0007b",              // BEL
            "a\u2066b"               // FIRST STRONG ISOLATE
    })
    void rejectsControlAndBidiOverrideCharacters(String actor) {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getHeader("X-Mohs-Actor")).thenReturn(actor);

        assertThatThrownBy(() -> resolver.resolve(request))
                .isInstanceOf(InvalidActorException.class)
                .hasMessageContaining("control or bidirectional-override");
    }
}
