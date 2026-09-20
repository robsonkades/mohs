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
package io.mohs.autoconfigure;

import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import javax.tools.ToolProvider;
import io.mohs.core.SubmissionOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.mohs.core.definition.JobOptions;
import io.mohs.core.execution.Priority;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.RecurringOptions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProgrammaticApiTest {
    @TempDir Path directory;

    @Test
    void typedEntryPointsRejectIncompatibleInputsAtCompilation() throws Exception {
        String source = """
                import io.mohs.core.Mohs;
                import io.mohs.core.job.JobRef;
                import io.mohs.core.definition.JobRegistration;
                import io.mohs.core.schedule.CronSpec;
                import java.time.Instant;
                import java.time.Duration;
                import java.time.ZoneId;
                class Consumer {
                    record Input(String company) {}
                    static final JobRef<Input> TAX = JobRef.of("tax", Input.class);
                    final JobRegistration<Input> registration = JobRegistration.of(TAX, (input, context) -> {});
                    void submit(Mohs mohs) {
                        mohs.enqueue(TAX, %s);
                        mohs.scheduleAt(TAX, %s, Instant.EPOCH);
                        mohs.scheduleAfter(TAX, %s, Duration.ZERO);
                        mohs.scheduleRecurring("tax-a", TAX, %s, new CronSpec("0 0 9 * * MON", ZoneId.of("UTC")));
                    }
                }
                """;
        Path file = directory.resolve("Consumer.java");
        String correct = "new Input(\"A\")";
        Files.writeString(file, source.formatted(correct, correct, correct, correct));
        var compiler = ToolProvider.getSystemJavaCompiler();
        var diagnostics = new StringWriter();
        try (var files = compiler.getStandardFileManager(null, null, null)) {
            var options = java.util.List.of("-classpath", System.getProperty("java.class.path"), "-d", directory.toString(), "-proc:none");
            assertThat(compiler.getTask(diagnostics, files, null, options, null,
                    files.getJavaFileObjects(file)).call()).as(diagnostics.toString()).isTrue();
            Files.writeString(file, source.formatted("\"wrong\"", "\"wrong\"", "\"wrong\"", "\"wrong\""));
            var errors = new javax.tools.DiagnosticCollector<javax.tools.JavaFileObject>();
            assertThat(compiler.getTask(diagnostics, files, errors, options, null,
                    files.getJavaFileObjects(file)).call()).isFalse();
            assertThat(errors.getDiagnostics().stream()
                    .filter(d -> d.getKind() == javax.tools.Diagnostic.Kind.ERROR)).hasSize(4);
        }
    }

    @Test
    void optionalCopiesPreserveReusableDefaultsAndValidateUnsafeValues() {
        JobOptions base = JobOptions.defaults().withRunner("cpu").withRetries(3);
        JobOptions bounded = base.withTimeout(Duration.ofMinutes(2)).withMaxConcurrentExecutions(1);
        assertThat(base.timeout()).isNull();
        assertThat(base.maxConcurrentExecutions()).isZero();
        assertThat(bounded.runner()).isEqualTo("cpu");
        assertThat(bounded.retries()).isEqualTo(3);
        SubmissionOptions submission = SubmissionOptions.defaults().withActor("tax-service").withPriority(Priority.HIGH);
        assertThat(submission.withIdempotencyKey("A-2026").priority()).isEqualTo(Priority.HIGH);
        assertThat(submission.idempotencyKey()).isNull();
        assertThat(RecurringOptions.defaults().withJobOptions(bounded).withMisfire(Misfire.FIRE_NOW).jobOptions())
                .isEqualTo(bounded);
        assertThatThrownBy(() -> base.withTimeout(Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> base.withMaxConcurrentExecutions(-1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> base.withRunner(" ")).isInstanceOf(IllegalArgumentException.class);
        assertThat(submission.withActor(" Scheduler ").actor()).isEqualTo(" Scheduler ");
        assertThatThrownBy(() -> submission.withIdempotencyKey("x".repeat(256))).isInstanceOf(IllegalArgumentException.class);
    }
}
