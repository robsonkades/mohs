package io.mohs;

import java.time.Duration;

/**
 * O estágio de {@link JobSpec} alcançado depois que um gatilho foi
 * escolhido. Carrega toda política do job que não é o gatilho em si —
 * runner, queue, window, misfire, retries, timeout — cada uma opcional e
 * ajustável em qualquer ordem.
 */
public sealed interface Configured permits JobSpecImpl {

    Configured runner(String name);

    Configured queue(String name);

    Configured window(String name);

    Configured misfire(Misfire policy);

    Configured retries(int max);

    Configured timeout(Duration timeout);
}
