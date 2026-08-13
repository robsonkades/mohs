package io.mohs.event;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.mohs.JobKey;

/**
 * Açúcar por método para {@link ExecutionListener}, filtrado por job e tipo
 * de evento — estilo {@code @EventListener} do Spring.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface OnExecution {

    /** {@link JobKey#value()} do job a observar. */
    String job();

    /** Qual variante de {@link ExecutionEvent} dispara este método. */
    ExecutionEventType event();
}
