package io.mohs;

import java.time.Duration;
import java.time.Instant;

import org.springframework.lang.CheckReturnValue;

/**
 * Cadeia fluente sobre uma definição já existente. Pré-terminais ajustam só
 * a instância — {@code priority}, {@code as}, {@code idempotencyKey}; a
 * política (retry, runner, queue) pertence à {@link JobDefinition} e não é
 * sobrescrita aqui. Terminais ({@code now/at/after}) fecham a cadeia e
 * persistem a execução — {@link CheckReturnValue} torna uma cadeia
 * abandonada antes do terminal um warning de compilação, não silêncio em
 * runtime (o bug clássico do builder sem {@code .build()}).
 */
public interface ScheduleCommand {

    ScheduleCommand priority(Priority priority);

    ScheduleCommand as(String actor);

    ScheduleCommand idempotencyKey(String key);

    @CheckReturnValue
    Enqueued now();

    @CheckReturnValue
    Enqueued at(Instant when);

    @CheckReturnValue
    Enqueued after(Duration delay);
}
