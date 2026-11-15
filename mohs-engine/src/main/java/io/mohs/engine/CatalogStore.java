/* Copyright 2026 The Mohs Authors. Licensed under the Apache License, Version 2.0. */
package io.mohs.engine;

import java.util.List;
import io.mohs.core.definition.JobDefinition;

/** Complete startup reconciliation, serialized with catalog changes and node joins. */
public interface CatalogStore {
    enum Conflict { OVERRIDE, PRESERVE, FAIL }

    /** Registers all local executable definitions atomically; startup never reactivates retired identities. */
    void reconcile(ExecutableCatalog catalog, List<JobDefinition> definitions, Conflict conflict);
}
