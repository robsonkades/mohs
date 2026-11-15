package io.mohs.store.jdbc;

import java.time.Duration;
import java.util.HashMap;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import io.mohs.core.EngineState;
import io.mohs.core.job.JobKey;
import io.mohs.engine.ExecutableCatalog;
import io.mohs.engine.StoredNode;
import io.mohs.store.jdbc.delegate.JdbcDelegate;

/** Fixtures for work-operation tests; real catalog and session arbitration have dedicated protocol tests. */
public final class TestSessions {
    private TestSessions() {}

    public static ExecutableCatalog catalog(DataSource source) {
        var contracts = new HashMap<JobKey, String>();
        new JdbcTemplate(source).query("SELECT executable_key, input_contract FROM mohs_job_definitions", rs -> {
            contracts.put(JobKey.of(rs.getString(1)), rs.getString(2));
        });
        return new ExecutableCatalog(contracts);
    }

    public static StoredNode seed(DataSource source, JdbcDelegate dialect, String id, EngineState state, Duration ttl) {
        var jdbc = new NamedParameterJdbcTemplate(source);
        var now = JdbcDatabaseTime.now(jdbc, dialect);
        var existing = new JdbcSessions(jdbc, dialect).lock(id);
        if (existing.isPresent()) return existing.orElseThrow();
        new JdbcTemplate(source).update("""
                INSERT INTO mohs_nodes (node_id, state, last_heartbeat_at, expires_at, revoked)
                VALUES (?, ?, ?, ?, ?)
                """, id, state.name(), JdbcTimestamps.toUtcLocalDateTime(now), JdbcTimestamps.toUtcLocalDateTime(now.plus(ttl)), false);
        return new StoredNode(id, state, now, now.plus(ttl), false);
    }
}
