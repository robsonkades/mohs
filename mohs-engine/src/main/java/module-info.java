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
 * Here the module-info does the work that motivated the change.
 *
 * 29 of this package's 33 types are `public` — not by an API decision, but because
 * mohs-store-jdbc implements the ports and the starter builds the Engine, and the language has no
 * "public to my other modules". Without a module-info, a consumer's IDE autocompleted Engine,
 * Dispatcher and CompletionBatcher as if they were API. With `exports ... to`, they become
 * internal again in fact.
 */
/**
 * Polling, dispatch, ownership and persistence ports for the scheduling engine.
 */
module io.mohs.engine {
    requires static org.jspecify;
    requires io.mohs.core;
    requires io.mohs.cron;
    requires micrometer.core;
    requires org.slf4j;
    requires spring.core;
    requires spring.context;
    requires spring.tx;
    requires spring.beans;
    requires io.github.robsonkades.uuidv7;

    exports io.mohs.engine to io.mohs.store.jdbc, io.mohs.autoconfigure, io.mohs.test;
}
