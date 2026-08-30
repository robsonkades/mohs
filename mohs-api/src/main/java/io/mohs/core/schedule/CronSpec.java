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
package io.mohs.core.schedule;

import java.time.ZoneId;
import java.util.Objects;

/**
 * A cron-based schedule: Quartz-style, seconds-first syntax, evaluated in the given {@link ZoneId}
 * — never in the JVM's default zone.
 */
public record CronSpec(String expression, ZoneId zone) implements Schedule {

    public CronSpec {
        Objects.requireNonNull(expression, "expression");
        Objects.requireNonNull(zone, "zone");
        if (expression.isBlank()) {
            throw new IllegalArgumentException("expression must not be blank");
        }
    }
}
