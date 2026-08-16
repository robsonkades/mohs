package io.mohs.core;

import java.time.Duration;
import java.time.Instant;

import org.springframework.lang.CheckReturnValue;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.event.Enqueued;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.Priority;

/**
 * Cadeia fluente sobre uma definição já existente. Pré-terminais ajustam só
 * a instância — {@code priority}, {@code as}, {@code idempotencyKey}; a
 * política (retry, runner) pertence à {@link JobDefinition} e não é
 * sobrescrita aqui. Terminais ({@code now/at/after}) fecham a cadeia e
 * persistem a execução — {@link CheckReturnValue} torna uma cadeia
 * abandonada antes do terminal um warning de compilação, não silêncio em
 * runtime (o bug clássico do builder sem {@code .build()}).
 */
public interface ScheduleCommand {

    @CheckReturnValue
    ScheduleCommand priority(Priority priority);

    /**
     * A trilha de auditoria da execução ({@code Execution.actor}).
     *
     * @throws IllegalArgumentException se {@code actor} for em branco ou
     * {@link Execution#SCHEDULER_ACTOR} — nome reservado do motor
     * (ADR-0035): agendamento manual jamais pode se passar por ocorrência
     * do trigger.
     */
    @CheckReturnValue
    ScheduleCommand as(String actor);

    /**
     * Dedupe por {@code (job, key)} — Idempotent Receiver: um terminal com a
     * mesma chave de uma execução já gravada não duplica nada e devolve o
     * {@link Enqueued} original (mesmo recibo, mesma {@code ExecutionId}).
     * A chave deduplica enquanto a execução existir (ADR-0030: a janela é
     * a da retenção de execuções — ilimitada enquanto a política de
     * retenção não existir); reutilizar uma chave antiga devolve a
     * execução antiga.
     */
    @CheckReturnValue
    ScheduleCommand idempotencyKey(String key);

    @CheckReturnValue
    Enqueued now();

    @CheckReturnValue
    Enqueued at(Instant when);

    @CheckReturnValue
    Enqueued after(Duration delay);
}
