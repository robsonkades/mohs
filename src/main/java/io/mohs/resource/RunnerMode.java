package io.mohs.resource;

/**
 * Natureza do workload que um {@link MohsRunner} executa — decide a
 * disciplina de threads por baixo (virtual para {@code IO}, platform pool
 * limitado para {@code CPU}); nunca fixed/cached pool para virtual threads.
 */
public enum RunnerMode {
    IO,
    CPU
}
