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
package io.mohs.rest.execution;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import jakarta.servlet.http.HttpServletRequest;

import org.jspecify.annotations.Nullable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.mohs.core.ExecutionQuery;
import io.mohs.core.Mohs;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.rest.AcceptedExecutionResponse;
import io.mohs.rest.ExecutionLocations;
import io.mohs.rest.ApiPaths;
import io.mohs.rest.CursorPage;
import io.mohs.rest.error.ExecutionNotFoundException;
import io.mohs.rest.error.ExecutionNotRetryableException;

/**
 * The "executions" resource area. A lookup
 * global (cursor), detalhe, cancelamento cooperativo e retry manual.
 */
@RestController
@RequestMapping("${mohs.api.base-path:" + ApiPaths.V1 + "}/executions")
public class ExecutionsController {

    private final Mohs mohs;

    public ExecutionsController(Mohs mohs) {
        this.mohs = Objects.requireNonNull(mohs, "mohs");
    }

    /** {@code size} — ver {@link CursorPage#DEFAULT_PAGE_SIZE}/{@link CursorPage#MAX_PAGE_SIZE}. The list is a summary ({@link ExecutionSummaryResponse}) — attempts live in the detail view. */
    @GetMapping
    public CursorPage<ExecutionSummaryResponse> search(
            @RequestParam(required = false) @Nullable ExecutionState status,
            @RequestParam(required = false) @Nullable String jobKey,
            @RequestParam(required = false) @Nullable Instant from,
            @RequestParam(required = false) @Nullable Instant to,
            @RequestParam(required = false) @Nullable String cursor,
            @RequestParam(required = false) @Nullable Integer size) {
        int pageSize = CursorPage.clampSize(size);
        JobKey key = jobKey == null ? null : JobKey.of(jobKey);
        List<Execution> fetched = mohs.executions(new ExecutionQuery(key, status, from, to, cursor, pageSize + 1));
        List<ExecutionSummaryResponse> responses = fetched.stream().map(ExecutionSummaryResponse::from).toList();
        return CursorPage.of(responses, pageSize, ExecutionSummaryResponse::executionId);
    }

    @GetMapping("/{id}")
    public ExecutionResponse get(@PathVariable String id) {
        return mohs.findExecution(ExecutionId.of(id)).map(ExecutionResponse::from)
                .orElseThrow(() -> new ExecutionNotFoundException(ExecutionId.of(id)));
    }

    /**
     * Cancellation is cooperative, not immediate — a 202 with the current state, not necessarily a
     * terminal one: a pending execution already comes back {@code CANCELLED}, while a
     * {@code RUNNING} one comes back as it is, with the request recorded.
     *
     * <p>It returns a {@code ResponseEntity} rather than a bare {@code ExecutionResponse} for the
     * same reason as {@link #retry}: that is where the {@code Location: /executions/{id}} header
     * required by the REST design is attached — {@code @ResponseStatus} would have no effect here.
     */
    @PostMapping("/{id}/cancel")
    public ResponseEntity<ExecutionResponse> cancel(@PathVariable String id, HttpServletRequest request) {
        ExecutionId executionId = ExecutionId.of(id);
        Execution execution = mohs.cancel(executionId)
                .orElseThrow(() -> new ExecutionNotFoundException(executionId));
        URI location = executionDetailLocation(request, "/cancel");
        return ResponseEntity.accepted().location(location).body(ExecutionResponse.from(execution));
    }

    /**
     * A manual retry: it rearms the SAME {@code FAILED} execution as {@code RETRY_WAITING} due now,
     * bypassing the retry budget; the new attempt competes for the claim like any other candidate.
     *
     * <p>Deliberately without an {@code Idempotency-Key}: a retry does not go through deduplication
     * because nothing new is inserted. The idempotence is the CAS itself, and repeating the POST
     * becomes a 409 naming the current state. The 202 comes through {@code ResponseEntity} —
     * {@code @ResponseStatus} has no effect on one.
     */
    @PostMapping("/{id}/retry")
    public ResponseEntity<AcceptedExecutionResponse> retry(@PathVariable String id, HttpServletRequest request) {
        ExecutionId executionId = ExecutionId.of(id);
        Execution rearmed = retryOrConflict(executionId)
                .orElseThrow(() -> new ExecutionNotFoundException(executionId));
        URI location = executionDetailLocation(request, "/retry");
        return ResponseEntity.accepted().location(location).body(AcceptedExecutionResponse.from(rearmed));
    }

    /**
     * {@code Location: /executions/{id}} is derived from the request's own URI (whose prefix already
     * honours {@code mohs.api.base-path}), simply dropping the action suffix ({@code /cancel},
     * {@code /retry}).
     */
    private static URI executionDetailLocation(HttpServletRequest request, String actionSuffix) {
        return ExecutionLocations.ofAction(request, actionSuffix);
    }

    /**
     * Every ISE in this scope is the retry's state guard (the contract documented on
     * {@code Mohs#retry}) — outside it, an unrelated ISE is never translated into a 409.
     */
    private Optional<Execution> retryOrConflict(ExecutionId executionId) {
        try {
            return mohs.retry(executionId);
        } catch (IllegalStateException e) {
            throw new ExecutionNotRetryableException(Objects.requireNonNullElse(e.getMessage(), e.toString()));
        }
    }
}
