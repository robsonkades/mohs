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
 * it with {@code REQUIRED} propagation — joining the caller's active transaction, or opening and
 * committing one of its own.
 *
 * <p>It is not a general-purpose {@code TransactionTemplate} for hire: the only legitimate caller is
 * the facade composing the enqueue unit (one-off, batch; occurrences do not come through here — the
 * firer has its own transaction). An exception inside {@code work} aborts the whole unit and
 * propagates.
 */
public interface StoreTransactions {

    void inTransaction(Runnable work);
}
