/* Copyright 2026 The Mohs Authors. Licensed under the Apache License, Version 2.0. */
package io.mohs.store.jdbc;

import java.time.Clock;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import javax.sql.DataSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import io.mohs.core.definition.JobDefinition;
import io.mohs.engine.CatalogStore;
import io.mohs.engine.ExecutableCatalog;
import io.mohs.store.jdbc.delegate.JdbcDelegate;

/** Atomic startup publication. Lock order: catalog, sessions, then sorted executable definitions. */
public final class JdbcCatalogStore implements CatalogStore {
    private final JdbcCatalog catalog;
    private final JdbcJobStore jobs;
    private final TransactionTemplate transaction;

    public JdbcCatalogStore(DataSource dataSource, Clock clock, JdbcDelegate dialect) {
        this.catalog = new JdbcCatalog(new NamedParameterJdbcTemplate(dataSource), dialect);
        this.jobs = new JdbcJobStore(dataSource, clock, dialect);
        this.transaction = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        transaction.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        transaction.setTimeout(30);
    }

    @Override
    public void reconcile(ExecutableCatalog incoming, List<JobDefinition> definitions, Conflict conflict) {
        Objects.requireNonNull(conflict, "conflict");
        definitions = List.copyOf(definitions);
        incoming.requireDefinitions(definitions);
        var sorted = definitions.stream().sorted(java.util.Comparator.comparing(d -> d.key().value())).toList();
        transaction.executeWithoutResult(_ -> {
            catalog.install(incoming);
            for (var definition : sorted) jobs.reconcileExecutable(definition, conflict);
            catalog.validateRequired(incoming);
        });
    }
}
