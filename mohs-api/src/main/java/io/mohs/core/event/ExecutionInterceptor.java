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
package io.mohs.core.event;

import io.mohs.core.execution.JobContext;

/**
 * A Chain of Responsibility (GoF) wrapping handler execution on the attempt's own thread — the
 * place for MDC, tracing spans and context through {@code ScopedValue}.
 *
 * <p>Unlike {@link ExecutionListener}, an exception from an interceptor IS a failure of the attempt
 * and follows the normal retry flow: whatever sits on the critical path takes part in the outcome.
 */
@FunctionalInterface
public interface ExecutionInterceptor {

    void intercept(JobContext ctx, Chain chain) throws Exception;

    /** The continuation of the interceptor chain down to the handler. */
    @FunctionalInterface
    interface Chain {
        void proceed() throws Exception;
    }
}
