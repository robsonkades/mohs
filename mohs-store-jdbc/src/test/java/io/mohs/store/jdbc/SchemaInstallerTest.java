package io.mohs.store.jdbc;

import java.sql.DatabaseMetaData;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import javax.sql.DataSource;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import io.mohs.core.EngineState;
import static org.assertj.core.api.Assertions.assertThat;

/** One fresh-install definition per database; no upgrade path exists before the first release. */
class SchemaInstallerTest {
    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void reinstallPreservesStructureAndExistingSession(JdbcJobBindingTest.Backend backend) throws Exception {
        var nodes = new JdbcNodeStore(backend.dataSource(), backend.delegate());
        var session = nodes.register("01991337-0001-7000-8000-00000000008f", EngineState.RUNNING, Duration.ofMinutes(5), io.mohs.store.jdbc.TestSessions.catalog(backend.dataSource()));
        List<String> before = structure(backend.dataSource());
        new ResourceDatabasePopulator(new ClassPathResource("schema-" + backend.name() + ".sql"))
                .execute(backend.dataSource());
        assertThat(structure(backend.dataSource())).isEqualTo(before);
        assertThat(nodes.findAll()).containsExactly(session);
        assertThat(before).anyMatch(column -> column.startsWith("mohs_nodes.expires_at:") && column.endsWith(":false"));
        assertThat(before).anyMatch(column -> column.startsWith("mohs_work.claim_token:") && column.endsWith(":true"));
        assertThat(before).noneMatch(column -> column.contains(".epoch:"));
        for (String column : List.of("mohs_nodes.node_id", "mohs_work.execution_id",
                "mohs_work.node_id", "mohs_work.claim_token", "mohs_execution.execution_id", "mohs_execution.batch_id",
                "mohs_execution.last_attempt_node_id", "mohs_execution.last_claim_token", "mohs_batches.id", "mohs_idempotency.execution_id")) {
            assertThat(before).anyMatch(entry -> entry.startsWith(column + ":") && entry.split(":")[2].equals("36"));
        }
        assertThat(before).noneMatch(column -> column.startsWith("mohs_job_definitions.id:"));
        assertThat(before).contains("mohs_job_definitions:primaryKey:1:job_key");
    }

    private static List<String> structure(DataSource source) throws Exception {
        List<String> result = new ArrayList<>();
        try (var connection = source.getConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            try (var tables = metadata.getTables(connection.getCatalog(), null, "%", new String[]{"TABLE"})) {
                while (tables.next()) {
                    String table = tables.getString("TABLE_NAME");
                    if (!table.toLowerCase(Locale.ROOT).startsWith("mohs_")) continue;
                    String schema = tables.getString("TABLE_SCHEM");
                    try (var primaryKey = metadata.getPrimaryKeys(connection.getCatalog(), schema, table)) {
                        while (primaryKey.next()) {
                            result.add(table.toLowerCase(Locale.ROOT) + ":primaryKey:" + primaryKey.getInt("KEY_SEQ")
                                    + ":" + primaryKey.getString("COLUMN_NAME").toLowerCase(Locale.ROOT));
                        }
                    }
                    try (var columns = metadata.getColumns(connection.getCatalog(), schema, table, "%")) {
                        while (columns.next()) {
                            result.add(table.toLowerCase(Locale.ROOT) + "." + columns.getString("COLUMN_NAME").toLowerCase(Locale.ROOT)
                                    + ":" + columns.getInt("DATA_TYPE") + ":" + columns.getInt("COLUMN_SIZE")
                                    + ":" + columns.getInt("DECIMAL_DIGITS") + ":" + columns.getString("COLUMN_DEF")
                                    + ":" + (columns.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls));
                        }
                    }
                    try (var indexes = metadata.getIndexInfo(connection.getCatalog(), schema, table, false, false)) {
                        while (indexes.next()) {
                            if (indexes.getShort("TYPE") == DatabaseMetaData.tableIndexStatistic) continue;
                            result.add(table + ":index:" + indexes.getString("INDEX_NAME") + ":" + indexes.getInt("ORDINAL_POSITION")
                                    + ":" + indexes.getString("COLUMN_NAME") + ":" + indexes.getBoolean("NON_UNIQUE")
                                    + ":" + indexes.getString("ASC_OR_DESC"));
                        }
                    }
                }
            }
        }
        return result.stream().sorted().toList();
    }
}
