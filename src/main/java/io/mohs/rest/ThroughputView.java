package io.mohs.rest;

import java.time.Duration;
import java.util.Objects;

/** Vazão da janela recente — parte de {@link OverviewResponse}. */
public record ThroughputView(Duration window, long succeeded, long failed) {

    public ThroughputView {
        Objects.requireNonNull(window, "window");
    }
}
