package io.mohs.rest;

/**
 * Fonte única do default do prefixo REST (REST-9): fallback dos
 * placeholders {@code ${mohs.api.base-path:...}} em todo
 * {@code @RequestMapping} — anotação não lê property binding, o
 * placeholder é o único mecanismo lá — e default do binder em
 * {@code MohsProperties.Api#basePath}, que é de onde código lê o valor
 * resolvido (ex.: o header {@code Location} de {@code JobsController}).
 */
public final class ApiPaths {

    public static final String V1 = "/api/mohs/v1";

    private ApiPaths() {
    }
}
