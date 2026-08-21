package io.mohs.rest.runner;

import java.util.Objects;

import io.mohs.core.RunnerSnapshot;
import io.mohs.core.resource.RunnerMode;

/**
 * Forma de wire de {@link io.mohs.core.resource.MohsRunner} — visão
 * node-local por natureza (ver {@code docs/REST-API-DESIGN.md}: "modo,
 * max, em execução"). {@code max} é derivado do modo:
 * {@code maxConcurrent} pra {@link RunnerMode#IO}, {@code maxSize} pra
 * {@link RunnerMode#CPU} — os demais campos de tuning exclusivos de CPU
 * ({@code coreSize}/{@code queueCapacity}/{@code keepAlive}) não são
 * expostos aqui, só o que o design doc pede.
 */
public record RunnerResponse(String name, RunnerMode mode, int max, int running) {

    public RunnerResponse {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(mode, "mode");
    }

    public static RunnerResponse from(RunnerSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        return new RunnerResponse(snapshot.name(), snapshot.mode(), snapshot.max(), snapshot.running());
    }
}
