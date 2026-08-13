package io.mohs;

/**
 * Where a {@link JobDefinition} came from. Annotation-sourced definitions
 * that disappear from the code on redeploy become {@code ORPHANED} rather
 * than silently deleted (see
 * {@code docs/adr/0006-registration-lifecycle-and-conflict-policy.md});
 * programmatic ones are retired explicitly via {@link Mohs#remove}.
 */
public enum DefinitionSource {
    ANNOTATION,
    PROGRAMMATIC
}
