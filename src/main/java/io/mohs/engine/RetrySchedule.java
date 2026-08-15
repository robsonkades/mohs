package io.mohs.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Decisão de retry compartilhada por {@link Dispatcher} (falha de attempt)
 * e pelo reaper (reclaim de lease expirada) — ADR-0033: um lugar só decide
 * orçamento e backoff; dois caminhos de falha com cópias próprias da
 * política divergiriam na primeira mudança (Shotgun Surgery).
 *
 * <p>Backoff exponencial com full jitter (estilo AWS): delay uniforme em
 * {@code [0, min(1s × 2^(tentativa−1), 10min)]}. Full jitter em vez de
 * exponencial puro porque o caso das 3h da manhã é um recurso compartilhado
 * caindo e derrubando muitas execuções juntas — sem jitter, todas voltariam
 * em sincronia contra o recurso ainda se recuperando (thundering herd).
 * Constantes internas, sem propriedade de configuração — política
 * customizada por job ({@code JobDefinition.retryPolicy}) é SPI futura,
 * ainda não honrada.
 */
public final class RetrySchedule {

    private static final Duration BASE = Duration.ofSeconds(1);
    private static final Duration CAP = Duration.ofMinutes(10);
    /** 2^20 × 1s já passa do CAP — expoente maior só arriscaria overflow do shift. */
    private static final int MAX_EXPONENT = 20;

    private RetrySchedule() {
    }

    /**
     * Hora do próximo retry depois da tentativa {@code failedAttempt}
     * (1-based) falhar — vazia quando o orçamento acabou ({@code retries} =
     * tentativas adicionais além da primeira, contrato de
     * {@code JobDefinition.retries}: total de tentativas = retries + 1).
     */
    public static Optional<Instant> nextRetryAt(int failedAttempt, int retries, Instant now) {
        if (failedAttempt < 1) {
            throw new IllegalArgumentException("failedAttempt must be at least 1");
        }
        if (retries < 0) {
            throw new IllegalArgumentException("retries must not be negative");
        }
        Objects.requireNonNull(now, "now");
        if (failedAttempt > retries) {
            return Optional.empty();
        }
        long exponent = Math.min(failedAttempt - 1L, MAX_EXPONENT);
        long boundMillis = Math.min(BASE.toMillis() << exponent, CAP.toMillis());
        long delayMillis = ThreadLocalRandom.current().nextLong(boundMillis + 1);
        return Optional.of(now.plus(Duration.ofMillis(delayMillis)));
    }
}
