package io.mohs.engine;

import java.time.Clock;
import java.time.Duration;

/**
 * Um {@link Clock} que precisa ser reamostrado periodicamente contra uma
 * fonte externa de tempo (ex.: banco, ADR-0008 modo "database"). Quem
 * constrói o {@code Clock} ({@code io.mohs.autoconfigure}) é dono de
 * chamar {@link #sync()} no intervalo configurado (`mohs.time.sync-interval`)
 * — este contrato existe pra isso não precisar conhecer o tipo concreto
 * (ex.: {@code DatabaseClock}, em {@code io.mohs.store.jdbc}) nem nenhuma
 * dependência de JDBC.
 */
public interface SyncableClock {

    /** Uma amostra: mede o offset contra a fonte externa e aplica o clamp monotônico. */
    void sync();

    /** Offset atual (fonte externa − app), exposto pra quando a infra de métricas existir. */
    Duration currentOffset();
}
