package io.mohs.rest.resource;

import java.util.Objects;

/** Forma de wire de {@link io.mohs.core.resource.JobQueue}, com o contador runtime de {@code running}. */
public record QueueResponse(String name, int maxConcurrent, int running) {

    public QueueResponse {
        Objects.requireNonNull(name, "name");
    }
}
