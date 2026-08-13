/**
 * Test kit embarcado no jar principal, para quem consome a biblioteca
 * testar os próprios handlers de job. {@link io.mohs.test.MutableClock} é
 * a implementação "test" das três de {@code docs/adr/0008-configurable-time-source.md}
 * — o relógio por trás de {@code mohs.clock()} no test kit (§5.14 do
 * documento mestre).
 */
@NullMarked
package io.mohs.test;

import org.jspecify.annotations.NullMarked;
