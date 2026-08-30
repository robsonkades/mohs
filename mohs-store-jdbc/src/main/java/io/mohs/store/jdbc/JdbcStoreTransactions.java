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

    public JdbcStoreTransactions(DataSource dataSource) {
        // DataSourceTransactionManager is created with nestedTransactionAllowed=true — NESTED here is a
        // pure JDBC savepoint, supported by all four dialects
        this.transactionTemplate = new TransactionTemplate(
                new DataSourceTransactionManager(Objects.requireNonNull(dataSource, "dataSource")));
        this.transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_NESTED);
    }

    @Override
    public void inTransaction(Runnable work) {
        Objects.requireNonNull(work, "work");
        transactionTemplate.executeWithoutResult(_ -> work.run());
    }
}
