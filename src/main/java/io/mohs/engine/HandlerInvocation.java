package io.mohs.engine;

import io.mohs.core.execution.JobContext;

/**
 * Handler já resolvido, pronto pra chamar — desacopla {@link Dispatcher}
 * de como um {@link io.mohs.core.job.JobKey} vira algo invocável. Escanear
 * {@code @MohsJob} e resolver o bean Spring dono do método é trabalho de
 * {@code io.mohs.autoconfigure} (M3, ainda não construído); esta porta só
 * assume que, quando chamada, essa resolução já aconteceu.
 */
@FunctionalInterface
public interface HandlerInvocation {

    void invoke(Object payload, JobContext ctx) throws Exception;
}
