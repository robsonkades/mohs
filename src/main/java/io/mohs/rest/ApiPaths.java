package io.mohs.rest;

/**
 * Prefixo compartilhado por todo controller REST — hardcoded por agora
 * (REST-9); vira placeholder de property ({@code mohs.api.base-path})
 * quando {@code io.mohs.autoconfigure} existir.
 */
public final class ApiPaths {

    public static final String V1 = "/api/mohs/v1";

    private ApiPaths() {
    }
}
