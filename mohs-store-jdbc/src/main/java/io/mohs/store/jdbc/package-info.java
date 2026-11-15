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
/**
 * Internal JDBC implementation of complete scheduling operations. Active work has one READY/LEASED
 * row, separate retained execution/latest result and independent receipts. Operations own explicit
 * transaction/lock protocols; shared statements have concrete dialect overrides. No handler runs
 * inside acquisition/completion transactions. Public contracts live in io.mohs.core.
 */
@NullMarked
package io.mohs.store.jdbc;

import org.jspecify.annotations.NullMarked;
