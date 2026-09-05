/*
 * Copyright 2026 The Mohs Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mohs.rest;

/**
 * The single source of the REST prefix default: the fallback of the {@code ${mohs.api.base-path:...}}
 * placeholders in every {@code @RequestMapping} — an annotation cannot read a property binding, so
 * the placeholder is the only mechanism there — and the binder default in
 * {@code MohsProperties.Api#basePath}, which is where code reads the resolved value (the
 * {@code Location} header of {@code JobsController}, for instance).
 */
public final class ApiPaths {

    /**
     * The default route prefix for version one of the operational API.
     */
    public static final String V1 = "/api/mohs/v1";

    private ApiPaths() {
    }
}
