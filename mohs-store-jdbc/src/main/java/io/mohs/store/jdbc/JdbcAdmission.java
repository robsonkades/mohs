package io.mohs.store.jdbc;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import io.mohs.core.job.JobKey;
import io.mohs.store.jdbc.delegate.ClaimedReady;
import io.mohs.store.jdbc.delegate.JdbcDelegate;

/** Admission shares the claim transaction: session, sorted jobs, then selected work identities. */
final class JdbcAdmission {
    private final NamedParameterJdbcTemplate jdbc;
    private final JdbcDelegate dialect;

    JdbcAdmission(NamedParameterJdbcTemplate jdbc, JdbcDelegate dialect) {
        this.jdbc = jdbc;
        this.dialect = dialect;
    }

    List<String> select(int shard, int limit, Collection<String> candidateJobs, Instant now) {
        var candidates = jdbc.query(dialect.admissionCandidates(!candidateJobs.isEmpty()),
                new MapSqlParameterSource("shard", shard).addValue("limit", (long) limit, java.sql.Types.BIGINT)
                        .addValue("now", dialect.splitTimestamp(now)).addValue("inactive", false)
                        .addValue("candidateJobs", candidateJobs), ClaimedReady::fromReadyRow);
        if (candidates.isEmpty()) return List.of();
        var keys = candidates.stream().map(ClaimedReady::jobKey).distinct().toList();
        new JdbcExecutionTransitions(jdbc, dialect).lockJobs(keys.stream().map(JobKey::of).toList());
        Map<String, Long> capacity = new HashMap<>();
        for (var chunk : JdbcSupport.chunksOf(keys)) {
            var parameters = new MapSqlParameterSource("jobKeys", chunk);
            jdbc.query("""
                    SELECT job_key, max_concurrent_executions, paused, retired
                    FROM mohs_job_definitions WHERE job_key IN (:jobKeys)
                    """, parameters, rs -> {
                boolean active = !rs.getBoolean("retired");
                long cap = rs.getLong("max_concurrent_executions") == 0 ? Long.MAX_VALUE : rs.getLong("max_concurrent_executions");
                capacity.put(rs.getString("job_key"), active ? cap : 0L);
            });
            jdbc.query(dialect.countLeasesByJob(), parameters, rs -> {
                long occupied = rs.getLong("leases");
                capacity.computeIfPresent(rs.getString("job_key"), (_, cap) -> Math.max(0, cap - occupied));
            });
        }
        List<String> selected = new ArrayList<>();
        for (var candidate : candidates) {
            long remaining = capacity.getOrDefault(candidate.jobKey(), 0L);
            if (remaining > 0) {
                selected.add(candidate.executionId());
                capacity.put(candidate.jobKey(), remaining - 1);
            }
        }
        return selected;
    }
}
