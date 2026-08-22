package io.mohs.core.definition;

import io.mohs.core.Mohs;

/**
 * De onde veio um {@link JobDefinition}. Definições anotadas que
 * desaparecem do código num redeploy viram {@code ORPHANED} em vez de
 * apagadas silenciosamente (ver
 * {@code docs/adr/0006-registration-lifecycle-and-conflict-policy.md});
 * as programáticas são aposentadas explicitamente via {@link Mohs#remove}.
 */
public enum DefinitionSource {
    ANNOTATION,
    PROGRAMMATIC
}
