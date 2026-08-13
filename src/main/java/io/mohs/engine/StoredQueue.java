package io.mohs.engine;

import java.util.Objects;

import io.mohs.core.resource.JobQueue;

/**
 * {@link JobQueue} mais {@code runningCount} — o contador ADR-0009 (ainda
 * Proposed) não é campo de {@link JobQueue} (spec de boot), é estado
 * operacional que a etapa 3 (claim) escreve; mesma separação que
 * {@link StoredJob} já faz entre definição e {@code orphaned}/{@code paused}.
 */
public record StoredQueue(JobQueue queue, int runningCount) {

    public StoredQueue {
        Objects.requireNonNull(queue, "queue");
        if (runningCount < 0) {
            throw new IllegalArgumentException("runningCount must not be negative");
        }
    }
}
