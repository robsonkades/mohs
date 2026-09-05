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
package io.mohs.rest.runner;

import java.util.List;
import java.util.Objects;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.mohs.core.Mohs;
import io.mohs.rest.ApiPaths;

/**
 * {@code GET /runners} — the node-local view: mode, max, running. Read-only; a runner is
 * configuration, not adjustable runtime.
 *
 * <p>No cursor, unlike the execution listings: the cardinality is what the application declared at
 * boot, not what it accumulated while running — the same criterion as {@code GET /jobs} and
 * {@code GET /nodes}.
 *
 * <p>Node-local means node-local: the response describes the process that served the request, not
 * the cluster. Behind a load balancer, two consecutive calls may legitimately answer with different
 * numbers — a thread pool is not shared state.
 */
@RestController
@RequestMapping("${mohs.api.base-path:" + ApiPaths.V1 + "}/runners")
public class RunnersController {

    private final Mohs mohs;

    /**
     * Creates a {@code RunnersController} with the supplied values.
     *
     * @param mohs the scheduling and operations facade
     */
    public RunnersController(Mohs mohs) {
        this.mohs = Objects.requireNonNull(mohs, "mohs");
    }

    /**
     * Lists this node's runner configuration and occupancy.
     *
     * @return the local runner configurations and occupancy
     */
    @GetMapping
    public List<RunnerResponse> list() {
        return mohs.runners().stream().map(RunnerResponse::from).toList();
    }
}
