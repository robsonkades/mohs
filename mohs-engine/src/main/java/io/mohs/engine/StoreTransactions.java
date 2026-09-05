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
package io.mohs.engine;

/**
 * The transactional boundary the engine cannot open by itself (with no Spring-tx or JDBC in this
 * module): the enqueue unit — {@code HistoryStore.record} plus {@code WorkQueue.offer} — needs ONE
 * transaction, and the async contract requires it to join the host's transaction when there is one.
 *
 * <p>This port is the minimal Unit of Work (PoEAA) for that: {@code io.mohs.store.jdbc} implements
 * it with {@code NESTED} propagation — a savepoint inside the caller's active transaction, or a
 * transaction of its own when there is none. The savepoint is load-bearing, not incidental: the
 * enqueue unit must be able to FAIL and be recovered from inside a caller's transaction, which is
 * what the idempotency conflict relies on — the duplicate-key insert rolls back to the savepoint and
 * the winner is read afterwards, in the same outer transaction. Under plain {@code REQUIRED} that
 * read would hit PostgreSQL's "current transaction is aborted" and poison the caller's transaction.
 *
 * <p>It is not a general-purpose {@code TransactionTemplate} for hire: the only legitimate caller is
 * the facade composing the enqueue unit (one-off, batch; occurrences do not come through here — the
 * firer has its own transaction). An exception inside {@code work} aborts the whole unit and
 * propagates.
 */
public interface StoreTransactions {

    /**
     * Runs {@code work} atomically and {@code onDurable} once its writes are committed: right after,
     * when this call opened the transaction, or after the host's commit when it joined one — the
     * only party that knows which of the two happened is the implementation, which is why the
     * "after commit" hook lives here and not with the caller. A rollback runs nothing.
     *
     * @param work the writes to execute atomically
     * @param onDurable the callback invoked after the transaction commits
     */
    void inTransaction(Runnable work, Runnable onDurable);
}
