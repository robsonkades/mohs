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

import java.net.URI;

import jakarta.servlet.http.HttpServletRequest;

/**
 * An execution's {@code Location}, derived from the REQUEST — never assembled by concatenating
 * configuration.
 *
 * <p>The reason is concrete: {@code mohs.api.base-path} is the path INSIDE the application, and an
 * app with {@code server.servlet.context-path=/app} serves the execution at
 * {@code /app/api/mohs/v1/executions/{id}}. Concatenating the base path returned a header pointing
 * at a 404 — and on a 202 receipt that is the worst possible outcome: the client follows the
 * {@code Location}, does not find the execution it just scheduled, concludes the write was lost and
 * resends.
 *
 * <p>The rule is cross-cutting across controllers (jobs returns the receipt of
 * {@code POST /jobs/{key}/schedule}, executions returns those of {@code /cancel} and
 * {@code /retry}), and used to live duplicated in both with derivations that disagreed. Hence it
 * lives here, in the root package that the {@code package-info} reserves for cross-cutting concerns.
 */
public final class ExecutionLocations {

    private ExecutionLocations() {
    }

    /**
     * The {@code Location} of an execution's detail, from a request made to another resource (the
     * scheduling receipt). The application prefix comes from {@code getContextPath()}, which is what
     * the base path does not know about.
     *
     * @param request the incoming HTTP request
     * @param basePath the configured REST route prefix
     * @param executionId the identity of the execution
     * @return the execution detail URI under the configured route prefix
     */
    public static URI ofExecution(HttpServletRequest request, String basePath, String executionId) {
        return URI.create(request.getContextPath() + basePath + "/executions/" + executionId);
    }

    /**
     * The detail's {@code Location} from a request made to the execution ITSELF, dropping the action
     * suffix ({@code /cancel}, {@code /retry}). {@code getRequestURI()} already includes the context
     * path.
     *
     * @param request the incoming HTTP request
     * @param actionSuffix the action path segment appended to the execution URI
     * @return the execution action URI under the configured route prefix
     */
    public static URI ofAction(HttpServletRequest request, String actionSuffix) {
        // endsWith/substring rather than replaceFirst: the suffix went into a Pattern, so any future
        // caller passing something with '.', '+' or '(' would silently cut in the wrong place (or
        // throw PatternSyntaxException). Nothing here needs a regex, and it removes a Pattern
        // compilation per request
        String uri = request.getRequestURI();
        return URI.create(uri.endsWith(actionSuffix) ? uri.substring(0, uri.length() - actionSuffix.length()) : uri);
    }
}
