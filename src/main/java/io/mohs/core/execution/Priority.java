package io.mohs.core.execution;

/**
 * Prioridade de uma instância agendada, 5 níveis. Sem aging nesta versão —
 * risco documentado de starvation de {@link #BACKGROUND} sob carga
 * sustentada de níveis mais altos (ver §3 do documento mestre).
 */
public enum Priority {
    BACKGROUND,
    LOW,
    NORMAL,
    HIGH,
    CRITICAL
}
