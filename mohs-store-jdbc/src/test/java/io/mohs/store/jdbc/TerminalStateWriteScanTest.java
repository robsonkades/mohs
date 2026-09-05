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
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A source scan guarding a batch invariant; it lives in this module, rather than in mohs-demo's
 * {@code ArchitectureTest}, because it scans the {@code src/main/java} of the module it runs in — and all
 * the SQL that writes a terminal state into the {@code mohs_execution} advisory is here, in
 * {@code io.mohs.store.jdbc.delegate}: one copy per delegate of {@code cancelExecution},
 * {@code terminalStateUpdate} and {@code cancelDrainedExecutions}. Since every statement moved into the
 * delegates the scan's reach GREW — twelve write sites instead of three — which is exactly what a rule
 * that protects a class rather than known cases is for.
 */
class TerminalStateWriteScanTest {

    /** The states that end an execution — writing one is the trigger for the batch count. The target is the {@code mohs_execution} advisory. */
    private static final Pattern TERMINAL_STATE_WRITE = Pattern.compile(
            "UPDATE\\s+mohs_execution\\s+SET\\s+state\\s*=\\s*(:state|'SUCCEEDED'|'FAILED'|'CANCELLED')");

    /** The marker every terminal write site must carry, with the reason beside it. */
    private static final String BATCH_COUNTED_MARKER = "batch-counted:";

    /** How far above a write the marker may sit: close enough to be read along with it, and no further. */
    private static final int MARKER_LOOKBEHIND = 6;

    /**
     * The rule the batch design needed and did not have: <b>whoever writes a terminal state into
     * {@code mohs_execution} counts towards the batch</b>. Four paths violated that in the implementation
     * — the reaper, cancellation, job retirement and manual retry — and the first three left the batch
     * open forever, with no error and no reconciliation sweep to cure it (the design dispensed with the
     * sweep precisely on the premise that every path counts).
     *
     * <p>The error that produced it was one of boundaries: the search was for "who calls
     * {@code complete()}" when what matters is "who writes a terminal state". This rule protects the whole
     * class, not the four known cases — it exists for the next path that gets written.
     *
     * <p>A SOURCE scan, not ArchUnit, and not out of laziness: the terminal state lives in a SQL literal,
     * and ArchUnit does not read SQL — the same gap {@code ArchitectureTest}'s
     * {@code ids_are_generated_as_uuidv7_never_v4} rule records by leaving half of that invariant in
     * prose. Here the half that matters is precisely the SQL one, so the scan is the only instrument that
     * can see the invariant.
     *
     * <p>A marker rather than an allowlist, on purpose: a list of approved paths ages in silence and tells
     * a reader nothing. The mandatory comment sits beside the write, names who counts, and anyone adding a
     * new site is forced to answer the question before the test passes.
     */
    @Test
    void every_terminal_state_write_declares_how_the_batch_is_counted() throws IOException {
        List<String> unmarked = new ArrayList<>();
        try (Stream<Path> sources = Files.walk(Path.of("src/main/java"))) {
            for (Path source : sources.filter(path -> path.toString().endsWith(".java")).toList()) {
                List<String> lines = Files.readAllLines(source);
                for (int line = 0; line < lines.size(); line++) {
                    if (TERMINAL_STATE_WRITE.matcher(lines.get(line)).find()
                            && !markedWithin(lines, line)) {
                        unmarked.add(source + ":" + (line + 1) + "  " + lines.get(line).strip());
                    }
                }
            }
        }

        assertThat(unmarked)
                .as("terminal-state writes to mohs_execution that do not declare the batch count — "
                        + "add '// " + BATCH_COUNTED_MARKER + " <who counts>' above, in the SAME transaction, "
                        + "or those members' batch never closes")
                .isEmpty();
    }

    /**
     * A short window, looking backwards: the marker has to sit next to the write to be read along with it.
     * Too far away and it becomes a stamp nobody checks.
     */
    private static boolean markedWithin(List<String> lines, int writeLine) {
        int firstLineToInspect = Math.max(0, writeLine - MARKER_LOOKBEHIND);
        return lines.subList(firstLineToInspect, writeLine + 1).stream()
                .anyMatch(line -> line.contains(BATCH_COUNTED_MARKER));
    }

}
