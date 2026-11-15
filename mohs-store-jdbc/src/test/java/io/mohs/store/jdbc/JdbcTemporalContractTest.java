package io.mohs.store.jdbc;

import io.mohs.engine.SubmissionStore;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import io.mohs.core.execution.ExecutionId;
import io.mohs.engine.HistoryStore;
import io.mohs.engine.WorkQueue;
import static org.assertj.core.api.Assertions.assertThat;

/** Receipt, history and eligibility have one precision; a rounding step must never execute early. */
class JdbcTemporalContractTest {
    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void receiptAndQueueRoundUpToTheSameMicrosecond(JdbcJobBindingTest.Backend backend) {
        var f = new JdbcOwnershipProtocolTest.Fixture(backend);
        f.register("01991337-0000-7000-8000-000000000006");
        var id = ExecutionId.of("01991337-0000-7000-8000-00000000005e");
        var requested = Instant.parse("2026-09-09T12:00:00.123456001Z");
        var canonical = Instant.parse("2026-09-09T12:00:00.123457Z");
        f.firing.accept(List.of(new SubmissionStore.NewExecution(id, f.job, 0, 20, requested, requested,
                "test", null, "precision-key", io.mohs.test.InputFixtures.encode(f.job, "input"), SubmissionStore.Origin.MANUAL, null)), null, () -> {});

        var receipt = f.history.findReceiptByIdempotencyKey(f.job, "precision-key").orElseThrow();
        assertThat(receipt.scheduledAt()).isEqualTo(canonical);
        assertThat(f.history.find(id, requested).orElseThrow().scheduledAt()).isEqualTo(canonical);
        assertThat(f.queue.claim(0, "01991337-0000-7000-8000-000000000006", 1, List.of(), canonical.minusNanos(1))).isEmpty();
        assertThat(f.queue.claim(0, "01991337-0000-7000-8000-000000000006", 1, List.of(), canonical)).hasSize(1);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void operationalColumnsUseOneUtcLocalTimestampType(JdbcJobBindingTest.Backend backend) throws Exception {
        try (var connection = backend.dataSource().getConnection(); var statement = connection.createStatement()) {
            for (String[] column : List.of(new String[]{"mohs_work", "visible_at"}, new String[]{"mohs_execution", "scheduled_at"},
                    new String[]{"mohs_idempotency", "scheduled_at"}, new String[]{"mohs_nodes", "expires_at"})) {
                String query = "SELECT " + column[1] + " FROM " + column[0];
                try (var rows = statement.executeQuery(query)) {
                    assertThat(rows.getMetaData().getColumnType(1)).as(query).isEqualTo(java.sql.Types.TIMESTAMP);
                }
                // Connector/J reports a zero ResultSet scale for DATETIME(6); inspect declared precision.
                try (var rows = statement.executeQuery("SELECT datetime_precision FROM information_schema.columns"
                        + " WHERE LOWER(table_name) = '" + column[0] + "' AND LOWER(column_name) = '" + column[1] + "'")) {
                    assertThat(rows.next()).as(query).isTrue();
                    assertThat(rows.getInt(1)).as(query).isEqualTo(6);
                }
            }
        }
    }
}
