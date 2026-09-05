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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.mohs.core.definition.JobDefinition;
import io.mohs.store.jdbc.delegate.MySqlJdbcDelegate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The definition store's statements that MySQL binds differently, against a real MySQL (Tier 2) —
 * the complete semantics live in {@code JdbcJobStoreTest} (H2). What earns a container here is a
 * parameter the driver has to bind in a place the H2 parser is lenient about: the due-trigger
 * sweep's {@code LIMIT :limit}.
 */
@Tag("docker")
class JdbcJobStoreMySqlTest {

    private static final Instant NOW = Instant.parse("2026-08-22T12:00:00Z");

    record Handler() {
    }

    private JdbcJobStore jobStore;

    @BeforeEach
    void setUp() {
        jobStore = new JdbcJobStore(MySqlTestSupport.freshSchema(), Clock.fixed(NOW, ZoneOffset.UTC), new MySqlJdbcDelegate());
    }

    /** {@code findDueRecurringJobs}: the ceiling is a bound {@code LIMIT}, and the order is still {@code next_fire_at}. */
    @Test
    void findDueRecurringBoundsWithLimitAndFiresOldestFirst() {
        jobStore.upsert(JobDefinition.of("job-b-due-later", Handler.class, spec -> spec.every(Duration.ofMinutes(5))));
        jobStore.upsert(JobDefinition.of("job-a-due-first", Handler.class, spec -> spec.every(Duration.ofMinutes(1))));

        assertThat(jobStore.findDueRecurring(NOW.plus(Duration.ofMinutes(10)), 1))
                .extracting(job -> job.definition().key().value())
                .containsExactly("job-a-due-first");
        assertThat(jobStore.findDueRecurring(NOW.plus(Duration.ofMinutes(10)), 10)).hasSize(2);
    }
}
