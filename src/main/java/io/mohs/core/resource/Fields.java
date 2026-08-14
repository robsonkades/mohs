package io.mohs.core.resource;

import java.util.Objects;

/**
 * Validação "non-null então non-blank" compartilhada por
 * {@link MohsRunner}, {@link ExecutionWindow} e {@link RateLimit} — as
 * três únicas classes deste pacote com um campo {@code name} textual.
 * Não estende além de {@code io.mohs.core.resource}: os demais tipos do
 * módulo com a mesma checagem ({@code JobKey}, {@code ExecutionId} etc.)
 * moram em pacotes públicos distintos, onde compartilhar exigiria expor
 * este utilitário como API pública — custo maior que a duplicação de
 * duas linhas que ele evita.
 */
final class Fields {

    private Fields() {
    }

    static void requireNotBlank(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}
