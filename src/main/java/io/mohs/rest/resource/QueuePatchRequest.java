package io.mohs.rest.resource;

/** Corpo de {@code PATCH /queues/{name}} — mesmo invariante de {@link io.mohs.core.resource.JobQueue#maxConcurrent()}. */
public record QueuePatchRequest(int maxConcurrent) {

    public QueuePatchRequest {
        if (maxConcurrent < 1) {
            throw new IllegalArgumentException("maxConcurrent must be at least 1");
        }
    }
}
