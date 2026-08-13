package io.mohs.engine;

import java.util.Optional;
import java.util.stream.Stream;

import io.mohs.core.resource.RateLimit;

/**
 * Persistência de {@link RateLimit} — Repository (PoEAA), porta que
 * {@code io.mohs.jdbc} implementa. Sem estado operacional a preservar em
 * {@link #upsert} — diferente de {@link JobStore}/{@link QueueStore},
 * nenhuma ADR ainda define um mecanismo de enforcement (contador,
 * sliding window) pra {@code RateLimit}, então não há coluna
 * operacional a proteger aqui ainda.
 */
public interface RateLimitStore {

    RateLimit upsert(RateLimit rateLimit);

    Optional<RateLimit> find(String name);

    /** Stream sobre um cursor aberto — quem chama é dono do ciclo de vida (try-with-resources). */
    Stream<RateLimit> findAll();
}
