package io.mohs.store.jdbc.dialect;

/**
 * MySQL 8.0+ — {@code LIMIT} no fim, {@code SKIP LOCKED} nativo desde o
 * 8.0 (ADR-0023); claim da Phase 5 na forma portátil default
 * ({@code SELECT FOR UPDATE SKIP LOCKED → DELETE → INSERT}).
 */
public final class MySqlJdbcDialect implements JdbcDialect {

    @Override
    public String migrationLocation() {
        return "classpath:io/mohs/store/jdbc/migration/mysql";
    }
}
