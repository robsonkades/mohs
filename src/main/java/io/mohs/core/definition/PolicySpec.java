package io.mohs.core.definition;

import java.time.Duration;

import io.mohs.core.schedule.Misfire;

/**
 * O estágio de {@link JobSpec} alcançado depois que um gatilho foi
 * escolhido. Carrega toda política do job que não é o gatilho em si —
 * runner, queue, window, misfire, retries, timeout — cada uma opcional e
 * ajustável em qualquer ordem.
 */
public sealed interface PolicySpec permits JobSpecImpl {

    PolicySpec runner(String name);

    PolicySpec queue(String name);

    PolicySpec window(String name);

    PolicySpec misfire(Misfire policy);

    PolicySpec retries(int max);

    PolicySpec timeout(Duration timeout);
}
