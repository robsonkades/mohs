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
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A source scan guarding the SQL Server invariant: <b>every text column is {@code NVARCHAR}</b>. It lives
 * here rather than in {@code ArchitectureTest} because the invariant lives in DDL and ArchUnit does not
 * read SQL — the same reason as {@link TerminalStateWriteScanTest}.
 *
 * <p><b>Why a scan and not a round-trip test.</b> The round trip exists
 * ({@code SchemaSqlServerRoundTripTest}) and is the proof of behaviour, but it depends on a container: on
 * a machine without Docker it does not even run, and that is exactly how the regression slipped through.
 * This scan runs on every build, with no container, and catches the whole class rather than the known
 * cases.
 *
 * <p><b>The regression that produced it.</b> This had already been catalogued as a critical finding and
 * the fix adopted was explicit: {@code NVARCHAR} uniformly across the SQL Server DDL, rather than
 * deciding column by column what is "safe". The newer tables were born {@code VARCHAR} regardless, ten
 * lines below a header comment saying the opposite. Three consequences, measured in the 2026-08-29
 * review:
 * <ul>
 *   <li><b>wrong deduplication</b> — two {@code idempotency_key}s outside the code page collapse into the
 *       same string of {@code ?}, the primary-key conflict "resolves" a race that never existed and the
 *       second enqueue is silently lost, which is the opposite of an Idempotent Receiver;</li>
 *   <li><b>corruption</b> — CJK, Cyrillic, Greek, Hebrew, Arabic and emoji become {@code ?} in
 *       {@code payload} and {@code error}, without exception and without warning, under the default
 *       collation;</li>
 *   <li><b>lost seeks</b> — the driver sends parameters as {@code nvarchar}
 *       ({@code sendStringParametersAsUnicode} by default), and binding that to a {@code varchar} column
 *       converts THE COLUMN: the claim and the completion lose their seek. The recorded tuning decision
 *       depends on this premise.</li>
 * </ul>
 */
class SqlServerUnicodeScanTest {

    /**
     * {@code VARCHAR(} or {@code CHAR(} not preceded by {@code N} — the types that are not Unicode on SQL
     * Server. CASE_INSENSITIVE because the likeliest way for the regression to return is somebody writing
     * {@code varchar(255)} in a new migration; the lookbehind still blocks {@code nvarchar(}/{@code nchar(},
     * since the {@code n} is a letter.
     */
    private static final Pattern NON_UNICODE_TEXT_COLUMN =
            Pattern.compile("(?<![A-Za-z])(?:VAR)?CHAR\\s*\\(", Pattern.CASE_INSENSITIVE);

    @Test
    void everyTextColumnInTheSqlServerDdlIsNvarchar() throws IOException {
        List<Path> ddl = sqlServerDdl();
        assertThat(ddl)
                .as("the scan is only worth anything if it finds the files it is supposed to scan")
                .isNotEmpty();

        for (Path file : ddl) {
            String sql = Files.readString(file);
            Matcher matcher = NON_UNICODE_TEXT_COLUMN.matcher(sql);
            assertThat(matcher.find())
                    .as("%s declares a text column as VARCHAR — on SQL Server that is not Unicode:"
                            + " payload/error in other alphabets are silently truncated to '?',"
                            + " two distinct idempotency_key values collapse into one, and the driver's"
                            + " nvarchar parameter starts converting the column (goodbye seek). Use NVARCHAR.", file)
                    .isFalse();
        }
    }

    private static List<Path> sqlServerDdl() throws IOException {
        Path resources = Path.of("src", "main", "resources");
        try (Stream<Path> tree = Files.walk(resources)) {
            return tree.filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".sql"))
                    .filter(SqlServerUnicodeScanTest::isSqlServer)
                    .toList();
        }
    }

    private static boolean isSqlServer(Path path) {
        String normalized = path.toString().replace('\\', '/');
        return normalized.contains("sqlserver");
    }
}
