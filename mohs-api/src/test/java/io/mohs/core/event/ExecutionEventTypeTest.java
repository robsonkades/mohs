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
package io.mohs.core.event;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link ExecutionEventType} duplicates {@link ExecutionEvent}'s {@code permits} clause as an enum,
 * and nothing in the compiler stops the two from diverging when a new variant is added on only one
 * side. This test is the mechanical parity that is otherwise missing.
 *
 * <p>It DERIVES the expected list from {@code getPermittedSubclasses()} rather than repeating it as
 * literals: the earlier version compared against hand-written strings, so adding a ninth variant to
 * the sealed hierarchy and forgetting the enum passed green — the test only failed if somebody
 * touched the enum, which is precisely the side that needs no protection. That inversion is what
 * let {@code RETRY_WAITING} coexist with the {@code RetryScheduled} record.
 */
class ExecutionEventTypeTest {

    @Test
    void mirrorsEveryExecutionEventPermittedSubtype() {
        List<String> permitted = Arrays.stream(ExecutionEvent.class.getPermittedSubclasses())
                .map(Class::getSimpleName)
                .map(ExecutionEventTypeTest::toConstantCase)
                .toList();

        assertThat(ExecutionEventType.values()).extracting(Enum::name)
                .as("every ExecutionEvent variant has a constant of the same name, and vice versa")
                .containsExactlyInAnyOrderElementsOf(permitted);
    }

    /** {@code AttemptFailed} → {@code ATTEMPT_FAILED}. */
    private static String toConstantCase(String simpleName) {
        return simpleName.replaceAll("(?<=[a-z])(?=[A-Z])", "_").toUpperCase(Locale.ROOT);
    }
}
