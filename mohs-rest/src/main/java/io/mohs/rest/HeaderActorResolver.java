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

import java.util.regex.Pattern;

import io.mohs.core.execution.Execution;
import io.mohs.rest.error.InvalidActorException;

/**
 * A declarative, unauthenticated {@link ActorResolver} — it reads the {@code X-Mohs-Actor} header
 * and, when absent, falls back to {@link ActorResolver#ANONYMOUS}. Registering it as a bean is
 * {@code io.mohs.autoconfigure}'s decision; this class only freezes the logic for when that wiring
 * exists.
 */
public final class HeaderActorResolver implements ActorResolver {

    private static final String ACTOR_HEADER = "X-Mohs-Actor";

    /** The {@code actor} column's ceiling (VARCHAR(255) in the schema) — validated here so it becomes a 400 that teaches rather than an INSERT failure (a 500). */
    private static final int MAX_ACTOR_LENGTH = 255;

    /**
     * The actor is the only fully caller-controlled string Mohs PERSISTS and writes into the audit
     * trail ("job '{}' rescheduled at runtime by '{}'"). Without this validation, ANSI sequences and
     * bidi characters crossed into the terminal of whoever reads that trail at 3 a.m. — tampering
     * with how the audit reads is precisely what strips the field of its value. (Tomcat already
     * rejects CR/LF in a header value, so forging a whole line was never viable; what got through
     * was everything else.)
     *
     * <p>It denies the THREAT rather than permitting a shape. The first version was an allowlist of
     * "printable", and {@code \p{Print}} in Java is pure US-ASCII: it rejected "José" in NFD (e plus
     * U+0301, which is what macOS and SSO produce), the typographic em dash, and Arabic-Indic
     * digits — a person's name turning into a 400. An allowlist of shape is always ASCII-centric in
     * practice.
     *
     * <p>The list has to be the COMPLETE family of the threat: U+061C (ARABIC LETTER MARK) is a bidi
     * control and fell outside the 200E/202x/206x ranges; U+2028/2029 forge a line in a JSON log
     * consumer, where the CR/LF argument does not hold; and the invisibles (U+200B, U+00AD, U+FEFF)
     * make two DISTINCT actors render identically in the trail. ZWJ/ZWNJ are deliberately left out —
     * they are legitimate in Persian and Indic scripts, and denying all of {@code \p{Cf}} would be
     * the allowlist bug all over again.
     */
    private static final Pattern UNSAFE_ACTOR =
            Pattern.compile(
                    "[\\p{Cc}\\p{Zl}\\p{Zp}"              // C0/C1 controls plus line and paragraph separators
                            + "\\u061C\\u200E\\u200F\\u202A-\\u202E\\u2066-\\u2069"  // ALL bidi controls (U+061C included)
                            + "\\u200B\\u00AD\\uFEFF"         // invisibles: two distinct actors rendering identically
                            + "\\u2060-\\u2064\\u206A-\\u206F\\uFFF9-\\uFFFB"  // U+2060 IS FEFF's successor; the rest are zero-width and have no use in a name
                            + "\\x{E0000}-\\x{E007F}"       // The Tags block: invisible ASCII — "admin" and "admin"+tags render identically
                            + "]");

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
        if (UNSAFE_ACTOR.matcher(actor).find()) {
            throw new InvalidActorException(ACTOR_HEADER + " must not contain control or bidirectional-override"
                    + " characters — the actor is persisted and written to the audit log");
        }
        // Validated here for the same reason as the length ceiling: a 400 that teaches, not
        // ScheduleCommand.as's IllegalArgumentException becoming a 500 in the generic handler.
        // Case- and whitespace-insensitive for the same reason as the guard in as(): the upsert's
        // cure compares the actor in the database, whose default collation may be case-insensitive
        if (Execution.SCHEDULER_ACTOR.equalsIgnoreCase(actor.strip())) {
            throw new InvalidActorException(ACTOR_HEADER + " must not be '" + Execution.SCHEDULER_ACTOR
                    + "' — reserved for engine-fired occurrences; identify the real caller or omit the header");
        }
        return actor;
    }
}
