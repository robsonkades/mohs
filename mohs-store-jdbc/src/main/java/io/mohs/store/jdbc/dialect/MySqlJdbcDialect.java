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
package io.mohs.store.jdbc.dialect;

/**
 * MySQL 8.0+ — {@code LIMIT} at the end, native {@code SKIP LOCKED} since 8.0; the claim in the default
 * portable form ({@code SELECT FOR UPDATE SKIP LOCKED}, then DELETE, then INSERT).
 */
public final class MySqlJdbcDialect implements JdbcDialect {

    @Override
    public String migrationLocation() {
        return "classpath:io/mohs/store/jdbc/migration/mysql";
    }
}
