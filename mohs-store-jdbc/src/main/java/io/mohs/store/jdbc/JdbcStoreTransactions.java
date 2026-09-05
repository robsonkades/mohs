/*
 * Copyright 2026 The Mohs Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mohs.store.jdbc;

import java.util.Objects;

import javax.sql.DataSource;

import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import io.mohs.engine.StoreTransactions;

/**
 * {@link StoreTransactions} over Mohs's {@code DataSource} — {@code NESTED} propagation on purpose, the
 * exact opposite of claim and completion.
 *
 * <p>With no active transaction it behaves like {@code REQUIRED} (opening and committing its own);
 * INSIDE the host's transaction ("joins your transaction") it becomes a <b>savepoint</b> — and it is the
 * savepoint that makes the Idempotent Receiver composable: {@code mohs_idempotency}'s primary-key
 * conflict undoes ONLY the enqueue unit, leaving the connection healthy (on Postgres, a violation
 * without a savepoint aborts the whole transaction — {@code 25P02} — and the recovery path that reads
 * the winner would be unreachable) and the host's transaction committable (with {@code REQUIRED}, the
 * template's rollback-only would doom the host's commit AFTER we had returned a successful
 * {@code Enqueued}).
 *
 * <p>The "joins and falls together" semantics are preserved: the unit only becomes durable with the
 * host's commit. No explicit isolation — inside the host's transaction the isolation is theirs, and pure
 * inserts do not depend on the level.
 */
public final class JdbcStoreTransactions implements StoreTransactions {

    private final TransactionTemplate transactionTemplate;

    /**
     * Creates a {@code JdbcStoreTransactions} with the supplied values.
     *
     * @param dataSource the configured database connection source
     */
    public JdbcStoreTransactions(DataSource dataSource) {
        // DataSourceTransactionManager is created with nestedTransactionAllowed=true — NESTED here is a
        // pure JDBC savepoint, supported by all four databases
        this.transactionTemplate = new TransactionTemplate(
                new DataSourceTransactionManager(Objects.requireNonNull(dataSource, "dataSource")));
        this.transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_NESTED);
    }

    /**
     * {@code status.isNewTransaction()} is the one honest signal of "durable now" versus "durable
     * with the host": the thread-level "is a transaction active" would also say yes for a host
     * transaction on ANOTHER DataSource, inside which this template opens and commits a transaction
     * of its own — and an event tied to that host's commit would then trail the execution, or be
     * lost to a rollback that never touched it.
     */
    @Override
    public void inTransaction(Runnable work, Runnable onDurable) {
        Objects.requireNonNull(work, "work");
        Objects.requireNonNull(onDurable, "onDurable");
        boolean joinedHost = Boolean.TRUE.equals(transactionTemplate.execute(status -> {
            // Checked BEFORE the writes: a host transaction without synchronization (a manager set to
            // SYNCHRONIZATION_NEVER, a connection bound by hand) cannot carry the after-commit hook,
            // and finding that out after the enqueue would be a rollback with Spring's generic message
            if (!status.isNewTransaction() && !TransactionSynchronizationManager.isSynchronizationActive()) {
                throw new IllegalStateException("the enqueue joined a host transaction that has no transaction"
                        + " synchronization, so Mohs cannot publish Enqueued after its commit — keep synchronization"
                        + " on the host's PlatformTransactionManager (the default) or schedule outside the transaction");
            }
            work.run();
            if (status.isNewTransaction()) {
                return false;
            }
            // A savepoint inside the host's transaction: durable only with the host's commit
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    onDurable.run();
                }
            });
            return true;
        }));
        if (!joinedHost) {
            onDurable.run();
        }
    }
}
