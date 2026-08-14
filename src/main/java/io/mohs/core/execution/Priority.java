package io.mohs.core.execution;

/**
 * Prioridade de uma instância agendada, 5 níveis. {@link #value()} é o peso
 * usado pra ordenar candidatos no claim — menor valor reivindica primeiro
 * (Effective Java Item 34: dado associado vira campo de instância, nunca
 * {@code ordinal()}). Sem aging nesta versão — risco documentado de
 * starvation de {@link #BACKGROUND} sob carga sustentada de níveis mais
 * altos (ver §3 do documento mestre).
 */
public enum Priority {
    /** Claims before everything else. */
    CRITICAL(0),
    /** Claims before {@link #NORMAL}. */
    HIGH(10),
    /** The default. */
    NORMAL(20),
    /** Claims after {@link #NORMAL}. */
    LOW(30),
    /** Claims last. */
    BACKGROUND(40);

    private final int value;

    Priority(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }
}
