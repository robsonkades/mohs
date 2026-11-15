/* Copyright 2026 The Mohs Authors. Licensed under the Apache License, Version 2.0. */
package io.mohs.store.jdbc;

import java.util.Objects;
import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import io.mohs.core.job.JobKey;
import io.mohs.engine.ExecutableCatalog;
import io.mohs.store.jdbc.delegate.JdbcDelegate;

/** The singleton is a durable catalog mutex, including when no node or executable exists yet. */
final class JdbcCatalog {
    private final NamedParameterJdbcTemplate jdbc;
    private final JdbcDelegate dialect;

    JdbcCatalog(NamedParameterJdbcTemplate jdbc, JdbcDelegate dialect) {
        this.jdbc = jdbc;
        this.dialect = dialect;
    }

    /** Consume the locking read itself: a MySQL host may already have an older RR snapshot. */
    @Nullable String lock() {
        return jdbc.queryForObject(dialect.lockCatalog(),
                new MapSqlParameterSource(), String.class);
    }

    void install(ExecutableCatalog catalog) {
        String current = lock();
        if (Objects.equals(current, catalog.fingerprint())) return;
        var sessions = new JdbcSessions(jdbc, dialect);
        var ids = jdbc.query("SELECT node_id FROM mohs_nodes ORDER BY node_id", (rs, _) -> rs.getString(1));
        for (String id : ids) {
            var node = sessions.lock(id);
            if (node.isPresent() && sessions.active(node.orElseThrow())) {
                throw new IllegalStateException("catalog change requires all previous worker sessions to stop or expire");
            }
        }
        jdbc.update("UPDATE mohs_catalog SET fingerprint = :fingerprint WHERE singleton = 1",
                new MapSqlParameterSource("fingerprint", catalog.fingerprint()));
    }

    void join(ExecutableCatalog catalog) {
        String current = lock();
        if (current == null) {
            // Standalone engine composition may seed definitions without the Spring scanner.
            install(catalog);
        } else if (!current.equals(catalog.fingerprint())) {
            throw new IllegalStateException("worker catalog differs from the installed catalog; reconcile before joining");
        }
        validateRequired(catalog);
    }

    void validateRequired(ExecutableCatalog catalog) {
        jdbc.query("""
                SELECT d.executable_key, d.input_contract FROM mohs_job_definitions d
                WHERE d.retired = :active OR EXISTS (
                    SELECT 1 FROM mohs_execution e WHERE e.job_key = d.job_key AND e.state = 'PENDING')
                """, new MapSqlParameterSource("active", false), rs -> {
            catalog.require(JobKey.of(rs.getString("executable_key")), rs.getString("input_contract"));
        });
    }

    void requireExistingExecutable(JobKey key) {
        if (lock() != null && jdbc.queryForObject(
                "SELECT COUNT(*) FROM mohs_job_definitions WHERE job_key = :key AND executable_key = :key",
                new MapSqlParameterSource("key", key.value()), Long.class) == 0L) {
            throw new IllegalStateException("new executable requires complete catalog reconciliation: " + key.value());
        }
    }
}
