/*
 * Copyright 2026 The Mohs Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package io.mohs.engine;

import java.time.Duration;
import java.util.Objects;
import org.jspecify.annotations.Nullable;
import io.github.robsonkades.uuidv7.UUIDv7;
import io.mohs.core.EngineState;

/** Owns this process's session; handlers retain the immutable identity of their own acquisition. */
final class NodeMembership {
    private final NodeStore nodes;
    private final Duration ttl;
    private final java.util.function.Supplier<ExecutableCatalog> catalog;
    private volatile String nodeId = UUIDv7.randomUUIDString();
    private @Nullable StoredNode confirmed;

    NodeMembership(NodeStore nodes, Duration ttl, java.util.function.Supplier<ExecutableCatalog> catalog) {
        this.nodes = Objects.requireNonNull(nodes, "nodes");
        this.ttl = Objects.requireNonNull(ttl, "ttl");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
    }

    String nodeId() {
        return nodeId;
    }

    java.time.Instant observedAt() {
        return Objects.requireNonNull(confirmed, "session heartbeat has not been confirmed").lastHeartbeatAt();
    }

    /** Serialized by the engine lifecycle/tick owner. A timeout never fabricates confirmation. */
    void heartbeat(EngineState state) {
        if (confirmed != null) {
            var renewed = nodes.renew(nodeId, state, ttl);
            if (renewed.isPresent()) {
                confirmed = renewed.orElseThrow();
                return;
            }
            nodeId = UUIDv7.randomUUIDString();
            confirmed = null;
        }
        try {
            confirmed = nodes.register(nodeId, state, ttl, catalog.get());
        } catch (NodeStore.SessionExpired _) {
            // The original register may have committed before its response was lost.
            nodeId = UUIDv7.randomUUIDString();
            confirmed = nodes.register(nodeId, state, ttl, catalog.get());
        }
    }

    void stop() {
        nodes.stop(nodeId);
        confirmed = null;
    }
}
