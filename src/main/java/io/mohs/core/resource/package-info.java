/**
 * Recursos nomeados — specs, nunca {@code Executor}:
 * {@link io.mohs.core.resource.MohsRunner} (capacidade node-local),
 * {@link io.mohs.core.resource.JobQueue} (cap de concorrência cluster-wide),
 * {@link io.mohs.core.resource.RateLimit} (cap de vazão cluster-wide) e
 * {@link io.mohs.core.resource.ExecutionWindow} (janela de exclusão de disparo).
 * Sem dependência de nenhum outro subpacote público.
 */
@NullMarked
package io.mohs.core.resource;

import org.jspecify.annotations.NullMarked;
