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
 * The internal/public boundary stops depending on convention.
 *
 * Without a module-info, `public` in io.mohs.cron is public to ANY consumer of the published jar —
 * the reactor guards the boundary in this repository and nothing guards it outside. Here the whole
 * package is exported on purpose: mohs-cron IS a cron parsing library, and has no internals to hide.
 */
module io.mohs.cron {
    requires static org.jspecify;

    exports io.mohs.cron;
}
