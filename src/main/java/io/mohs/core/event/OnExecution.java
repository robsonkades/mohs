package io.mohs.core.event;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.mohs.core.job.JobKey;

/**
 * Açúcar por método para {@link ExecutionListener}, filtrado por job e tipo
 * de evento — estilo {@code @EventListener} do Spring.
 *
 * <p><b>Ainda não processada nesta versão</b>: o motor não entrega eventos
 * filtrados a métodos anotados — o scanner falha o boot ao encontrar a
 * anotação, em vez de aceitá-la em silêncio. Registre um
 * {@link ExecutionListener} como bean até o processamento existir.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface OnExecution {

    /** {@link JobKey#value()} do job a observar. */
    String job();

    /** Qual variante de {@link ExecutionEvent} dispara este método. */
    ExecutionEventType event();
}
