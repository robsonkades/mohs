package io.mohs.definition;

import java.time.Duration;
import java.time.ZoneId;

/**
 * Builder staged para definições de job programáticas (ver
 * {@link JobDefinition#of}). Escolher um gatilho — {@link #cron},
 * {@link #every}, {@link #everyAfterFinish} ou {@link #onDemand} — é o
 * primeiro passo e retorna {@link PolicySpec}, que não expõe esses métodos
 * de novo: o compilador torna "cron e every" irrepresentável em vez de um
 * erro de validação no boot (ver {@code docs/API-DESIGN.md} §"Disciplina
 * de interfaces fluentes", ponto 3).
 *
 * <p>Selado para uma única implementação de propósito (Design Patterns,
 * Builder staged): mantém compatibilidade binária para novos métodos em
 * minor releases, já que nada fora deste pacote pode implementá-la.
 */
public sealed interface JobSpec permits JobSpecImpl {

    PolicySpec cron(String expression, ZoneId zone);

    PolicySpec every(Duration interval);

    PolicySpec everyAfterFinish(Duration interval);

    PolicySpec onDemand();
}
