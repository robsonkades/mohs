package io.mohs.rest.overview;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.mohs.core.Mohs;
import io.mohs.core.OverviewSnapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** O contrato de custo do stream: zero leitura sem assinante; um conjunto de leituras por tick, compartilhado — nunca por cliente. */
class OverviewStreamBroadcasterTest {

    private static final Instant NOW = Instant.parse("2026-08-17T12:00:00Z");

    private final Mohs mohs = mock(Mohs.class);
    private final OverviewStreamBroadcaster broadcaster =
            new OverviewStreamBroadcaster(mohs, Clock.fixed(NOW, ZoneOffset.UTC));

    @AfterEach
    void closeBroadcaster() {
        broadcaster.close();
    }

    private void stubSnapshotReads() {
        when(mohs.overview(any())).thenReturn(
                new OverviewSnapshot(Map.of(), Duration.ofSeconds(60), 0L, 0L));
        when(mohs.jobs()).thenReturn(List.of());
        when(mohs.nodes()).thenReturn(List.of());
        when(mohs.executions(any())).thenReturn(List.of());
    }

    @Test
    void tickWithoutSubscribersNeverTouchesTheFacade() {
        broadcaster.tick();

        verifyNoInteractions(mohs);
    }

    @Test
    void subscribeSendsTheInitialSnapshotImmediately() {
        stubSnapshotReads();

        assertThat(broadcaster.subscribe()).isNotNull();

        verify(mohs).overview(OverviewController.DEFAULT_THROUGHPUT_WINDOW);
        verify(mohs).jobs();
        verify(mohs).nodes();
        verify(mohs).executions(any());
    }

    /**
     * Retrato inicial que falha (banco degradado) vira o 500 do subscribe
     * SEM deixar emitter órfão registrado — um emitter nunca inicializado
     * não dispara callback de remoção e bufferizaria todo tick futuro sem
     * teto (leak reportado no review deste ciclo).
     */
    @Test
    void aFailedInitialSnapshotDoesNotLeakTheEmitter() {
        when(mohs.overview(any())).thenThrow(new IllegalStateException("database unavailable"));

        assertThatThrownBy(broadcaster::subscribe).isInstanceOf(IllegalStateException.class);

        clearInvocations(mohs);
        broadcaster.tick();
        verifyNoInteractions(mohs);
    }
}
