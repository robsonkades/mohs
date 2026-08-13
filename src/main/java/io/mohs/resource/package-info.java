/**
 * Recursos nomeados — specs, nunca {@code Executor}:
 * {@link io.mohs.resource.MohsRunner} (capacidade node-local),
 * {@link io.mohs.resource.JobQueue} (cap cluster-wide) e
 * {@link io.mohs.resource.ExecutionWindow} (janela de exclusão de disparo).
 * Sem dependência de nenhum outro subpacote público.
 */
@NullMarked
package io.mohs.resource;

import org.jspecify.annotations.NullMarked;
