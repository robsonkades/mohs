package io.mohs.store.jdbc;

import io.mohs.engine.SubmissionStore;

import java.util.List;
import javax.sql.DataSource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import io.mohs.engine.SubmissionStore.NewExecution;
import io.mohs.engine.WorkQueue.ReadyEntry;
import io.mohs.store.jdbc.delegate.JdbcDelegate;

/** Raw fixtures for claim/recovery tests, including deliberately incomplete or corrupt work. */
public final class TestWork {
    private TestWork() {}

    public static void batch(DataSource source, JdbcDelegate dialect, String id, String name, long total) {
        new NamedParameterJdbcTemplate(source).update(dialect.insertBatch(), new MapSqlParameterSource("id", id)
                .addValue("name", name).addValue("total", total)
                .addValue("createdAt", dialect.splitTimestamp(java.time.Instant.now())));
    }

    public static void ready(DataSource source, JdbcDelegate dialect, List<ReadyEntry> entries) {
        if (entries.isEmpty()) return;
        new NamedParameterJdbcTemplate(source).batchUpdate(dialect.readyInsert(), entries.stream()
                .map(e -> JdbcSupport.readyEntryParams(e, dialect)).toArray(MapSqlParameterSource[]::new));
    }

    /** Receipt acceptance must be exercised through JdbcSubmissionStore, never this fixture. */
    public static void history(DataSource source, JdbcDelegate dialect, List<NewExecution> executions) {
        if (executions.isEmpty()) return;
        if (executions.stream().anyMatch(e -> e.idempotencyKey() != null)) {
            throw new IllegalArgumentException("use complete acceptance to create idempotency receipts");
        }
        new NamedParameterJdbcTemplate(source).batchUpdate(dialect.recordExecution(), executions.stream().map(e ->
                new MapSqlParameterSource("executionId", e.executionId().value())
                        .addValue("jobKey", e.jobKey().value()).addValue("shard", e.shard()).addValue("priority", e.priority())
                        .addValue("scheduledAt", dialect.splitTimestamp(e.scheduledAt()))
                        .addValue("createdAt", dialect.splitTimestamp(e.createdAt())).addValue("actor", e.actor())
                        .addValue("batchId", e.batchId()).addValue("origin", e.origin().name())
                        .addValue("scheduleRevision", e.scheduleRevision()).addValue("idempotencyKey", null)
                        .addValue("payload", e.payload().json()).addValue("inputContract", e.payload().inputContract()))
                .toArray(MapSqlParameterSource[]::new));
    }
}
