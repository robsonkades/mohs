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
package io.mohs.rest.node;

import java.util.List;
import java.util.Objects;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.mohs.core.Mohs;
import io.mohs.rest.ApiPaths;

/**
 * {@code GET /nodes} — the cluster view: nodes with a recent heartbeat and their last-seen time,
 * most recent first (the order comes from {@code Mohs#nodes}).
 *
 * <p>Death is not a field of the response: a crash writes nothing — the reader derives alive versus
 * suspect from the age of {@code lastHeartbeatAt}, and {@code STOPPED} is the only self-reported
 * outcome (a clean shutdown); the purge keeps the list to recent nodes.
 */
@RestController
@RequestMapping("${mohs.api.base-path:" + ApiPaths.V1 + "}/nodes")
public class NodesController {

    private final Mohs mohs;

    public NodesController(Mohs mohs) {
        this.mohs = Objects.requireNonNull(mohs, "mohs");
    }

    @GetMapping
    public List<NodeResponse> list() {
        return mohs.nodes().stream().map(NodeResponse::from).toList();
    }
}
