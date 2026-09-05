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
package io.mohs.store.jdbc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A source scan guarding how keys are generated: every generated key is UUIDv7 — neither the JDK's
 * v4 nor the database's own sequence produces one. The invariant has a performance argument on each
 * half. A v4 UUID is random, so each insert lands at a random point of the B-tree and shatters
 * locality on the system's hottest table; v7 is time-ordered, keeping inserts at the index tail. A
 * database-sequential surrogate ({@code IDENTITY}, {@code SERIAL}, {@code AUTO_INCREMENT},
 * {@code SEQUENCE}) costs an allocation round trip and ties the key's existence to the server,
 * where v7 is generated client-side for free.
 *
 * <p>Half of this rule used to be ArchUnit and half was always prose: ArchUnit forbade
 * {@code UUID.randomUUID} but does not read SQL, so the schema half lived only in review — the same
 * gap {@link TerminalStateWriteScanTest} closes for terminal-state writes. When the ArchUnit suite
 * went away, both halves became convention. A source scan sees both. It lives here because every
 * schema and migration is in this module's {@code src/main} — the SQL half has no other home — and
 * the Java half deliberately covers only this module: most id generation sites live in
 * {@code mohs-engine}, which stays outside this scan's reach until an engine-side scan exists.
 *
 * <p>The Java half erases the sanctioned call form ({@code UUIDv7.randomUUID…}) from the line
 * before matching, so a line that merely MENTIONS the library — a comment promising migration next
 * to a real {@code UUID.randomUUID()} — is still flagged. Matching the bare method name closes the
 * static-import door, which also means the sanctioned generator must be called qualified; all
 * current code is.
 */
class KeyGenerationScanTest {

    /** {@code SERIALIZABLE} in a comment must not trip {@code SERIAL}, hence the word boundaries; case-insensitive because DDL has no canonical case. */
    private static final Pattern SEQUENTIAL_SURROGATE = Pattern.compile(
            "(?i)\\b(IDENTITY|BIGSERIAL|SMALLSERIAL|SERIAL[248]?|AUTO_INCREMENT|SEQUENCE)\\b");

    @Test
    void no_v4_uuid_is_generated_in_main_sources() throws IOException {
        List<String> violations = scan("src/main/java", ".java",
                line -> line.replace("UUIDv7.randomUUID", "").contains("randomUUID"));

        assertThat(violations)
                .as("v4 UUID generation in main sources — random ids shatter insert locality on the "
                        + "hottest table; generate ids with UUIDv7 instead")
                .isEmpty();
    }

    @Test
    void no_schema_declares_a_database_sequential_surrogate() throws IOException {
        List<String> violations = scan("src/main/resources", ".sql",
                line -> SEQUENTIAL_SURROGATE.matcher(line).find());

        assertThat(violations)
                .as("database-sequential surrogate in a schema or migration — a sequential key costs "
                        + "an allocation round trip and is banned on every dialect; use a client-side "
                        + "UUIDv7 column, as every existing table does")
                .isEmpty();
    }

    private static List<String> scan(String root, String extension, Predicate<String> violates) throws IOException {
        List<Path> files;
        try (Stream<Path> sources = Files.walk(Path.of(root))) {
            files = sources.filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(extension)).toList();
        }
        // A scan of zero files passes for the wrong reason — if the sources move, fail here, loudly.
        assertThat(files).as("no %s files under %s — the scan no longer sees what it guards", extension, root)
                .isNotEmpty();
        List<String> violations = new ArrayList<>();
        for (Path source : files) {
            List<String> lines = Files.readAllLines(source);
            for (int line = 0; line < lines.size(); line++) {
                if (violates.test(lines.get(line))) {
                    violations.add(source + ":" + (line + 1) + "  " + lines.get(line).strip());
                }
            }
        }
        return violations;
    }
}
