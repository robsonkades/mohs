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
 * Named resources — specs, never {@code Executor}s: {@link io.mohs.core.resource.MohsRunner}
 * (node-local capacity), {@link io.mohs.core.resource.RateLimit} (a cluster-wide throughput cap) and
 * {@link io.mohs.core.resource.ExecutionWindow} (a firing exclusion window). It depends on no other
 * public subpackage.
 */
@NullMarked
package io.mohs.core.resource;

import org.jspecify.annotations.NullMarked;
