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
/*
 * The published API is what this file exports, and nothing else.
 *
 * mohs-api is 100% contract (records, sealed types, interfaces), so it exports everything — the
 * value of a module-info here is the other side: the INTERNAL modules stop exporting what is
 * `public` today only because the language offered no alternative.
 */
/**
 * Public scheduling contracts, job definitions, execution events and resource policies.
 */
module io.mohs.core {
    requires static org.jspecify;
    requires spring.core;

    exports io.mohs.core;
    exports io.mohs.core.definition;
    exports io.mohs.core.event;
    exports io.mohs.core.execution;
    exports io.mohs.core.job;
    exports io.mohs.core.resource;
    exports io.mohs.core.schedule;
}
