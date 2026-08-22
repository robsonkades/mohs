/**
 * Test kit embarcado no jar principal, para quem consome a biblioteca
 * testar os próprios handlers de job. {@link io.mohs.test.MutableClock} é
 * a implementação "test" das três de {@code docs/adr/0008-configurable-time-source.md}
 * — o relógio por trás de {@code mohs.clock()} no test kit (§5.14 do
 * documento mestre). {@link io.mohs.test.InMemoryJobStore} é a
 * implementação "storage em memória" de {@link io.mohs.engine.JobStore}
 * — mesma precisão definicional×operacional (ADR-0006) que
 * {@code JdbcJobStore} (io.mohs.store.jdbc), sem depender de banco.
 */
@NullMarked
package io.mohs.test;

import org.jspecify.annotations.NullMarked;
