package io.mohs.core;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.mohs.core.execution.ExecutionState;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** O contrato de {@code GET /overview} mora aqui: só trabalho vivo, sempre os três estados (zero incluso), em ordem de ciclo de vida. */
class OverviewSnapshotTest {

    private static final Duration WINDOW = Duration.ofSeconds(60);

    @Test
    void normalizesTheThreeLiveStatesWithZeroForAbsentKeys() {
        OverviewSnapshot snapshot = new OverviewSnapshot(Map.of(ExecutionState.RUNNING, 3L), WINDOW, 0L, 0L);

        assertThat(snapshot.executionCountsByState()).containsExactly(
                entry(ExecutionState.ENQUEUED, 0L),
                entry(ExecutionState.RUNNING, 3L),
                entry(ExecutionState.RETRY_SCHEDULED, 0L));
    }

    /** A defesa em código da decisão do doc v0.6: contagem all-time de terminal não entra — histórico sem teto numa âncora de polling. */
    @Test
    void rejectsTerminalStateKeys() {
        assertThatThrownBy(() -> new OverviewSnapshot(Map.of(ExecutionState.SUCCEEDED, 1L), WINDOW, 0L, 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SUCCEEDED");
    }

    @Test
    void rejectsNonPositiveWindow() {
        assertThatThrownBy(() -> new OverviewSnapshot(Map.of(), Duration.ZERO, 0L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OverviewSnapshot(Map.of(), Duration.ofSeconds(-1), 0L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void exposesAnImmutableMap() {
        OverviewSnapshot snapshot = new OverviewSnapshot(Map.of(), WINDOW, 0L, 0L);

        assertThatThrownBy(() -> snapshot.executionCountsByState().put(ExecutionState.ENQUEUED, 9L))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
