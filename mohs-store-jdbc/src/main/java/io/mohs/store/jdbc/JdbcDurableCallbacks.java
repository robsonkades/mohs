/* Copyright 2026 The Mohs Authors. Licensed under the Apache License, Version 2.0. */
package io.mohs.store.jdbc;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/** One callback journal per submission store and physical host transaction, including its savepoints. */
final class JdbcDurableCallbacks implements TransactionSynchronization {
    private final JdbcSubmissionStore owner;
    private final List<Runnable> callbacks = new ArrayList<>();
    private final Map<Object, Integer> savepoints = new IdentityHashMap<>();

    private JdbcDurableCallbacks(JdbcSubmissionStore owner) {
        this.owner = owner;
    }

    static void register(JdbcSubmissionStore owner, Runnable callback) {
        for (var synchronization : TransactionSynchronizationManager.getSynchronizations()) {
            if (synchronization instanceof JdbcDurableCallbacks journal && journal.owner == owner) {
                journal.callbacks.add(callback);
                return;
            }
        }
        var journal = new JdbcDurableCallbacks(owner);
        journal.callbacks.add(callback);
        TransactionSynchronizationManager.registerSynchronization(journal);
    }

    @Override
    public void savepoint(Object savepoint) {
        savepoints.put(savepoint, callbacks.size());
    }

    @Override
    public void savepointRollback(Object savepoint) {
        // An unknown savepoint predates this journal, so all its acceptances belong to that scope.
        // A later savepoint retains the prefix accepted before it, including across nested rollbacks.
        int retained = savepoints.getOrDefault(savepoint, 0);
        callbacks.subList(retained, callbacks.size()).clear();
        savepoints.entrySet().removeIf(entry -> entry.getValue() > retained);
    }

    @Override
    public void afterCommit() {
        List.copyOf(callbacks).forEach(Runnable::run);
    }
}
