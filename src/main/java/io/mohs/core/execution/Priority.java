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

    /** {@code values()} clona o array a cada chamada — cacheado porque {@link #fromValue} roda por linha mapeada na borda JDBC do claim. */
    private static final Priority[] VALUES = values();

    private final int value;

    Priority(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }

    /** Inverso de {@link #value()} — usado na borda JDBC, onde só a coluna {@code priority} (int) é gravada. Zero alocação: roda por linha mapeada do claim. */
    public static Priority fromValue(int value) {
        for (Priority priority : VALUES) {
            if (priority.value == value) {
                return priority;
            }
        }
        throw new IllegalArgumentException("no Priority with value " + value);
    }
}
