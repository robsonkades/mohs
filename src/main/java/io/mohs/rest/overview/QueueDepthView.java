package io.mohs.rest.overview;

import java.util.Objects;

/** Profundidade de uma {@link io.mohs.core.resource.JobQueue} — parte de {@link OverviewResponse}. */
public record QueueDepthView(String queueName, int depth) {

    public QueueDepthView {
        Objects.requireNonNull(queueName, "queueName");
    }
}
