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

import java.sql.SQLException;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

import org.junit.jupiter.api.Test;

import io.mohs.store.jdbc.delegate.SqlServerRcsiRequirement;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The boot requirement behind the SQL Server dialect, against a real SQL Server.
 *
 * <p>The refusal arm runs against {@code master} — a system database on which
 * {@code READ_COMMITTED_SNAPSHOT} cannot be enabled, so it is permanently the misconfiguration the
 * check exists to catch. What the test pins about the message is that it is ACTIONABLE: it names the
 * database it inspected and the exact {@code ALTER DATABASE} the operator has to run.
 */
class SqlServerRcsiRequirementTest {

    @Test
    void refusesADatabaseWithoutReadCommittedSnapshot() {
        assertThatThrownBy(() -> SqlServerRcsiRequirement.verify(SqlServerTestSupport.dataSourceFor("master")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("READ_COMMITTED_SNAPSHOT is OFF")
                .hasMessageContaining("ALTER DATABASE [master] SET READ_COMMITTED_SNAPSHOT ON");
    }

    @Test
    void acceptsADatabaseWithReadCommittedSnapshotOn() {
        assertThatCode(() -> SqlServerRcsiRequirement.verify(SqlServerTestSupport.freshSchema()))
                .doesNotThrowAnyException();
    }

    /** The cause chain survives the wrapping — a boot that stops on an unreachable database must name the SQLException, not swallow it. */
    @Test
    void aFailedInspectionNamesItsCause() {
        SQLServerDataSource unreachable = new SQLServerDataSource();
        unreachable.setURL("jdbc:sqlserver://localhost:1;loginTimeout=1");

        assertThatThrownBy(() -> SqlServerRcsiRequirement.verify(unreachable))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("could not inspect")
                .cause().isInstanceOf(SQLException.class);
    }
}
