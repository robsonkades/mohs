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
package io.mohs.autoconfigure;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

/**
 * The one mapper every payload goes through: the store's write and read, and the REST layer's
 * conversion of a request body into the job's payload type.
 *
 * <p>A raw {@code JsonMapper}, deliberately not the context's {@code ObjectMapper}: the persisted
 * payload format belongs to Mohs, not to the host's web configuration. The context's mapper would
 * let the application's HTTP settings (a naming strategy, a module, a leniency toward unknown
 * properties) define a durable format shared between nodes, and break reading already-written
 * payloads the day it changed. The REST layer uses the SAME mapper for the same reason from the
 * other side: what it accepts and how it maps a body must be exactly what the store can read back
 * — a host mapper on snake_case would take {@code {"user_id": 1}} where the store persists and
 * expects {@code userId}. Not a bean: a {@code JsonMapper} bean would satisfy Spring Boot's
 * {@code @ConditionalOnMissingBean} and silently replace the host's own.
 */
final class PayloadMapper {

    static final JsonMapper SHARED = JsonMapper.builder().build();

    /**
     * The REST conversion's variant: the same format, but an unknown property is refused (a 422
     * naming the payload) instead of being dropped. A misspelt field in a request body would
     * otherwise become a silent {@code null} the job runs with. The store keeps the tolerant
     * mapper on purpose — a payload written by an older version of the job's class must still be
     * readable after a field is removed.
     */
    static final JsonMapper STRICT = SHARED.rebuild().enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();

    private PayloadMapper() {
    }
}
