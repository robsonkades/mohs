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

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.rest.error.PayloadValidationException;

/**
 * The identifiers a request carries as plain strings, turned into the API's value objects at the
 * boundary. The value objects refuse a blank value (and, for {@code JobKey}, one wider than its
 * column) with an {@code IllegalArgumentException}, which the REST layer would answer as a 500 with a stack trace
 * in the log — a whitespace-only path segment, reachable by anyone, must not be an error alarm at
 * 3 a.m. Here the refusal becomes the 422 the validation error model already promises, naming the
 * field. The detail is the value object's own message, verbatim: it already names the type and the
 * limit, and the field is what tells the client where to look.
 */
public final class RequestIdentifiers {

    private RequestIdentifiers() {
    }

    /**
     * Validates and parses a job key from an HTTP request.
     *
     * @param raw the untrusted serialized identifier
     * @return the validated job identity
     */
    public static JobKey jobKey(String raw) {
        return PayloadValidationException.validating("jobKey", () -> JobKey.of(raw));
    }

    /**
     * Validates and parses an execution identifier from an HTTP request.
     *
     * @param raw the untrusted serialized identifier
     * @return the validated execution identity
     */
    public static ExecutionId executionId(String raw) {
        return PayloadValidationException.validating("id", () -> ExecutionId.of(raw));
    }
}
