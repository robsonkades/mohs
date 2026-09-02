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
package io.mohs.store.jdbc.delegate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * The one database setting the SQL Server dialect refuses to run without:
 * {@code READ_COMMITTED_SNAPSHOT}.
 *
 * <p>Under SQL Server's default locking {@code READ COMMITTED}, every plain read takes shared locks
 * against the claim's exclusive ones. Measured on the split tables, the overview counts did not
 * merely contend — one uncommitted claim blocked them to a lock timeout ({@code Msg 1222}), and the
 * dashboard's stream runs those counts every two seconds. The lock-free alternative,
 * {@code WITH (NOLOCK)}, was measured too, and disqualified for the opposite reason: it read the
 * uncommitted {@code DELETE} of a claim that then rolled back — a wrong number a dashboard cannot
 * distinguish from a right one. Row versioning is the only answer that is both non-blocking and
 * correct, so the dialect states it as a requirement and {@link SqlServerJdbcDelegate} carries no
 * read hints at all.
 *
 * <p>Checked once, at boot, because the failure mode is silent otherwise: a deployment without RCSI
 * works in every test and degrades only under concurrent load, which is a 3 a.m. discovery. A boot
 * that stops names its cause. Azure SQL Database ships with the setting ON; on-premises servers
 * default to OFF, which is exactly why the check exists.
 */
public final class SqlServerRcsiRequirement {

    private SqlServerRcsiRequirement() {
    }

    /**
     * Refuses the boot unless {@code READ_COMMITTED_SNAPSHOT} is ON in the connection's database.
     *
     * @throws IllegalStateException when the setting is OFF — the message carries the exact
     *         {@code ALTER DATABASE} to run — or when the database could not be inspected at all,
     *         with the {@link SQLException} as the cause
     */
    public static void verify(DataSource dataSource) {
        String database;
        boolean enabled;
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT DB_NAME(), is_read_committed_snapshot_on FROM sys.databases WHERE database_id = DB_ID()");
                ResultSet resultSet = statement.executeQuery()) {
            if (!resultSet.next()) {
                throw new IllegalStateException(
                        "mohs.jdbc.dialect=sqlserver: could not inspect READ_COMMITTED_SNAPSHOT — sys.databases "
                                + "returned no row for the current database. The dialect requires the setting and "
                                + "will not start without confirming it.");
            }
            database = resultSet.getString(1);
            enabled = resultSet.getBoolean(2);
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "mohs.jdbc.dialect=sqlserver: could not inspect READ_COMMITTED_SNAPSHOT on the target "
                            + "database. The dialect requires the setting and will not start without confirming it "
                            + "— see the cause for what failed.",
                    e);
        }
        if (!enabled) {
            throw new IllegalStateException(
                    ("mohs.jdbc.dialect=sqlserver: READ_COMMITTED_SNAPSHOT is OFF on database [%s]. Without it, "
                            + "reads take shared locks against the claim's exclusive ones and the dashboard's "
                            + "counts block to lock timeouts under load; reading uncommitted instead reports "
                            + "wrong numbers. Enable it once, as an administrator: "
                            + "ALTER DATABASE [%s] SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE "
                            + "(Azure SQL Database ships with it ON.)").formatted(database, database));
        }
    }
}
