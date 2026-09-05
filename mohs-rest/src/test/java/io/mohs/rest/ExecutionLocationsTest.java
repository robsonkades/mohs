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

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A 202 receipt's {@code Location} is the HTTP expression of synchronous durability: the client
 * FOLLOWS the header. A wrong header is the worst possible outcome in an asynchronous API — the
 * client does not find the execution it just scheduled, concludes the write was lost and resends.
 *
 * <p>The defect these tests pin down: {@code JobsController} used to build the URL by concatenating
 * {@code mohs.api.base-path}, which is the path INSIDE the application. In an app with
 * {@code server.servlet.context-path=/app} the header pointed at a 404, while the neighbouring
 * controller derived it correctly from the request — two derivations disagreeing in one module.
 */
class ExecutionLocationsTest {

    private static final String BASE_PATH = "/api/mohs/v1";
    private static final String EXECUTION_ID = "0198f2c1-4b7e-7000-8000-000000000001";

    @Test
    void executionLocationHasNoPrefixWhenTheAppIsMountedAtTheRoot() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", BASE_PATH + "/jobs/welcome/schedule");

        assertThat(ExecutionLocations.ofExecution(request, BASE_PATH, EXECUTION_ID))
                .hasToString(BASE_PATH + "/executions/" + EXECUTION_ID);
    }

    /** The regression guard: the context path is not in the base path, and without it the receipt points at a 404. */
    @Test
    void executionLocationCarriesTheContextPathOfTheHostApplication() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", BASE_PATH + "/jobs/welcome/schedule");
        request.setContextPath("/app");

        assertThat(ExecutionLocations.ofExecution(request, BASE_PATH, EXECUTION_ID))
                .hasToString("/app" + BASE_PATH + "/executions/" + EXECUTION_ID);
    }

    @Test
    void actionLocationStripsTheSuffixAndKeepsTheContextPath() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST",
                "/app" + BASE_PATH + "/executions/" + EXECUTION_ID + "/retry");
        request.setContextPath("/app");

        assertThat(ExecutionLocations.ofAction(request, "/retry"))
                .hasToString("/app" + BASE_PATH + "/executions/" + EXECUTION_ID);
    }

    /**
     * The suffix is treated as text, not as a regex. It used to go into a {@code Pattern}: a future
     * caller with a {@code '.'} or {@code '('} in the suffix would silently cut in the wrong place,
     * or throw {@code PatternSyntaxException}.
     */
    @Test
    void actionSuffixIsMatchedLiterallyNotAsARegex() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST",
                BASE_PATH + "/executions/" + EXECUTION_ID + "/a.b");

        assertThat(ExecutionLocations.ofAction(request, "/a.b"))
                .hasToString(BASE_PATH + "/executions/" + EXECUTION_ID);
    }

    /** An absent suffix returns the URI untouched, rather than cutting off something that is not the action. */
    @Test
    void aUriThatDoesNotEndWithTheSuffixIsLeftAlone() {
        MockHttpServletRequest request = new MockHttpServletRequest("GET",
                BASE_PATH + "/executions/" + EXECUTION_ID);

        assertThat(ExecutionLocations.ofAction(request, "/retry"))
                .hasToString(BASE_PATH + "/executions/" + EXECUTION_ID);
    }
}
