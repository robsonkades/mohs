package io.mohs.store.jdbc.dialect;

/** H2 (embarcado/teste) — {@code LIMIT} no fim, {@code SKIP LOCKED} nativo (ADR-0023); claim da Phase 5 na forma portátil default. */
public final class H2JdbcDialect implements JdbcDialect {

    @Override
    public String migrationLocation() {
        return "classpath:io/mohs/store/jdbc/migration/h2";
    }
}
