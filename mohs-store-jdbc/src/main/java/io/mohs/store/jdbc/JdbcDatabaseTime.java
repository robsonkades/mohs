package io.mohs.store.jdbc;

import java.time.Instant;
import java.util.Objects;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import io.mohs.core.TimePrecision;
import io.mohs.store.jdbc.delegate.JdbcDelegate;

/** Samples actual database time on the caller's connection, after any required arbitration locks. */
final class JdbcDatabaseTime {
    private JdbcDatabaseTime() {}

    static Instant now(NamedParameterJdbcTemplate jdbc, JdbcDelegate dialect) {
        return Objects.requireNonNull(jdbc.query(dialect.nowQuery(), rs -> {
            if (!rs.next()) throw new IllegalStateException("database clock returned no row");
            return TimePrecision.observed(dialect.readNow(rs));
        }));
    }
}
