package io.mohs.store.jdbc;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.dao.DataAccessException;
import io.mohs.core.execution.ExecutionState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The physical row cannot represent both queue membership and acquired ownership. */
class JdbcWorkShapeTest {
    private static final Instant NOW = Instant.parse("2026-09-09T12:00:00Z");
    private static final String NODE = "01991337-0000-7000-8000-000000000006";

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void invalidWorkShapesAreRejectedWithoutChangingTheCommittedState(JdbcJobBindingTest.Backend backend) {
        var f = new JdbcOwnershipProtocolTest.Fixture(backend);
        f.register(NODE);
        var id = f.enqueue("01991337-0004-7000-8000-000000000001");
        assertThatThrownBy(() -> f.jdbc.update("UPDATE mohs_work SET node_id = ? WHERE execution_id = ?", NODE, id.value()))
                .isInstanceOf(DataAccessException.class)
                .satisfies(failure -> assertThat(failure.getMessage().toLowerCase(java.util.Locale.ROOT)).contains("ck_mohs_work_state"));
        assertThatThrownBy(() -> f.jdbc.update("UPDATE mohs_work SET work_state = 'LEASED' WHERE execution_id = ?", id.value()))
                .isInstanceOf(DataAccessException.class)
                .satisfies(failure -> assertThat(failure.getMessage().toLowerCase(java.util.Locale.ROOT)).contains("ck_mohs_work_state"));
        assertThat(f.history.find(id, NOW).orElseThrow().state()).isEqualTo(ExecutionState.ENQUEUED);
        f.queue.claim(0, NODE, 1, List.of(), NOW);
        assertThatThrownBy(() -> f.jdbc.update("UPDATE mohs_work SET work_state = 'READY' WHERE execution_id = ?", id.value()))
                .isInstanceOf(DataAccessException.class)
                .satisfies(failure -> assertThat(failure.getMessage().toLowerCase(java.util.Locale.ROOT)).contains("ck_mohs_work_state"));
        assertThatThrownBy(() -> f.jdbc.update("UPDATE mohs_work SET claim_token = NULL WHERE execution_id = ?", id.value()))
                .isInstanceOf(DataAccessException.class)
                .satisfies(failure -> assertThat(failure.getMessage().toLowerCase(java.util.Locale.ROOT)).contains("ck_mohs_work_state"));
        assertThat(f.history.find(id, NOW).orElseThrow().state()).isEqualTo(ExecutionState.RUNNING);
    }
}
