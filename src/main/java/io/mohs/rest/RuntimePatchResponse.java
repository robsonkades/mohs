package io.mohs.rest;

import java.util.Objects;

/**
 * Envelope de toda resposta de {@code PATCH} — carrega o aviso de que a
 * mudança é de emergência e vale só até o próximo boot (ver
 * {@code docs/REST-API-DESIGN.md}, seção "PATCH runtime × configuração de
 * boot"), a menos que {@code mohs.registration.on-conflict: preserve}
 * esteja configurado.
 */
public record RuntimePatchResponse<T>(T resource, String notice) {

    public static final String BOOT_REVERSION_NOTICE =
            "Mudança de emergência: vale até o próximo boot; codifique em properties para torná-la permanente.";

    public RuntimePatchResponse {
        Objects.requireNonNull(resource, "resource");
        Objects.requireNonNull(notice, "notice");
    }

    public static <T> RuntimePatchResponse<T> of(T resource) {
        return new RuntimePatchResponse<>(resource, BOOT_REVERSION_NOTICE);
    }
}
