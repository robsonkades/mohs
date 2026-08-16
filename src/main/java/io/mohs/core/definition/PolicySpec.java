package io.mohs.core.definition;

import java.time.Duration;

import io.mohs.core.schedule.Misfire;

/**
 * O estágio de {@link JobSpec} alcançado depois que um gatilho foi
 * escolhido. Carrega toda política do job que não é o gatilho em si —
 * runner, window, misfire, retries, timeout — cada uma opcional e
 * ajustável em qualquer ordem.
 */
public sealed interface PolicySpec permits JobSpecImpl {

    PolicySpec runner(String name);

    PolicySpec window(String name);

    PolicySpec misfire(Misfire policy);

    /**
     * Nasce pausado no PRIMEIRO registro da definição (ADR-0037): a agenda
     * fica declarada e desarmada até um {@code resume}; execução manual
     * sob demanda continua valendo mesmo pausado. Depois do nascimento,
     * {@code paused} é decisão de operador — redeploy nunca re-pausa.
     */
    PolicySpec startPaused();

    /**
     * Impede que mais de uma execução deste job fique {@code RUNNING} ao
     * mesmo tempo. Default: concorrência permitida — a maioria dos jobs é
     * invocada várias vezes com payloads independentes (ex.: um job de
     * e-mail, uma invocação por destinatário), e essas invocações não têm
     * por que serializar entre si só por compartilhar o mesmo {@code
     * job_key}. Use isto pro caso mais estreito: um job cron/interval cujo
     * próprio disparo seguinte pode ocorrer antes do anterior terminar
     * (ex.: sincronização que às vezes demora mais que o intervalo) — aí as
     * duas "execuções" são a mesma tarefa se sobrepondo, não trabalho
     * independente. Mesmo default do {@code @DisallowConcurrentExecution}
     * do Quartz — lá também é opt-in, não opt-out.
     */
    PolicySpec preventOverlap();

    /**
     * Como {@link #preventOverlap()}, mas com um teto explícito maior que 1
     * em vez de exclusão mútua total — ex.: um relatório cujo handler
     * compartilha um recurso externo com capacidade própria por {@code
     * job_key}, e pode rodar até N instâncias ao mesmo tempo, nunca N+1.
     */
    PolicySpec maxConcurrentExecutions(int max);

    PolicySpec retries(int max);

    PolicySpec timeout(Duration timeout);

    /** Nome do bean de uma política de retry customizada, para casos que {@link #retries(int)} não expressa. */
    PolicySpec retryPolicy(String beanName);
}
