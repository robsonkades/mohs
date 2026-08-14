package io.mohs.test;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.job.JobKey;
import io.mohs.engine.JobStore;
import io.mohs.engine.StoredJob;

/**
 * {@link JobStore} em memória — a implementação "storage em memória" do
 * test kit (§5.14 do documento mestre, {@code @MohsTest}). Mesma
 * precisão definicional×operacional da ADR-0006 que {@code JdbcJobStore}
 * (io.mohs.jdbc), sem tocar banco nenhum — prova que {@link JobStore}
 * não vazou nada específico de JDBC.
 */
public final class InMemoryJobStore implements JobStore {

    private final Map<JobKey, StoredJob> jobs = new ConcurrentHashMap<>();

    @Override
    public JobDefinition upsert(JobDefinition definition) {
        Objects.requireNonNull(definition, "definition");
        jobs.compute(definition.key(), (_, existing) -> {
            boolean orphaned = existing != null && existing.orphaned();
            boolean paused = existing != null && existing.paused();
            int runningExecutionCount = existing != null ? existing.runningExecutionCount() : 0;
            return new StoredJob(definition, orphaned, paused, runningExecutionCount);
        });
        return definition;
    }

    @Override
    public Optional<StoredJob> find(JobKey key) {
        Objects.requireNonNull(key, "key");
        return Optional.ofNullable(jobs.get(key));
    }

    @Override
    public Stream<StoredJob> findAll() {
        return jobs.values().stream();
    }

    @Override
    public void markOrphaned(JobKey key) {
        jobs.computeIfPresent(key, (_, stored) -> new StoredJob(stored.definition(), true, stored.paused(), stored.runningExecutionCount()));
    }

    @Override
    public void pause(JobKey key) {
        jobs.computeIfPresent(key, (_, stored) -> new StoredJob(stored.definition(), stored.orphaned(), true, stored.runningExecutionCount()));
    }

    @Override
    public void resume(JobKey key) {
        jobs.computeIfPresent(key, (_, stored) -> new StoredJob(stored.definition(), stored.orphaned(), false, stored.runningExecutionCount()));
    }

    @Override
    public void remove(JobKey key) {
        jobs.remove(key);
    }

    @Override
    public boolean tryIncrementRunningExecutions(JobKey key) {
        Objects.requireNonNull(key, "key");
        boolean[] acquired = {false};
        jobs.computeIfPresent(key, (_, stored) -> {
            if (stored.runningExecutionCount() >= stored.definition().maxConcurrentExecutions()) {
                return stored;
            }
            acquired[0] = true;
            return new StoredJob(stored.definition(), stored.orphaned(), stored.paused(), stored.runningExecutionCount() + 1);
        });
        return acquired[0];
    }

    @Override
    public void decrementRunningExecutions(JobKey key) {
        Objects.requireNonNull(key, "key");
        jobs.computeIfPresent(key, (_, stored) -> stored.runningExecutionCount() <= 0
                ? stored
                : new StoredJob(stored.definition(), stored.orphaned(), stored.paused(), stored.runningExecutionCount() - 1));
    }
}
