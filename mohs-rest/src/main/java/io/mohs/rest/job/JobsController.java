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
package io.mohs.rest.job;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import jakarta.servlet.http.HttpServletRequest;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import tools.jackson.databind.DatabindException;
import tools.jackson.databind.ObjectMapper;

import io.mohs.core.Mohs;
import io.mohs.core.JobSnapshot;
import io.mohs.core.ExecutionQuery;
import io.mohs.core.event.Enqueued;
import io.mohs.core.execution.Execution;
import io.mohs.core.job.JobKey;
import io.mohs.rest.AcceptedExecutionResponse;
import io.mohs.rest.ExecutionLocations;
import io.mohs.rest.ActorResolver;
import io.mohs.rest.ApiPaths;
import io.mohs.rest.CursorPage;
import io.mohs.rest.RuntimePatchResponse;
import io.mohs.rest.error.JobNotFoundException;
import io.mohs.rest.error.PayloadValidationException;
import io.mohs.rest.execution.ExecutionSummaryResponse;

/**
 * The "jobs" resource area.
 *
 * <p>A job's definition is code, not API — there is no {@code POST}/{@code PUT}/{@code DELETE} of a
 * definition here, only reads and invocation over an existing one. {@code PATCH .../schedule} is an
 * emergency runtime adjustment to the schedule, not a definition.
 */
@RestController
@RequestMapping("${mohs.api.base-path:" + ApiPaths.V1 + "}/jobs")
public class JobsController {

    private static final Logger log = LoggerFactory.getLogger(JobsController.class);

    /** The maximum edit distance for suggesting a neighbouring {@code jobKey} in a 404. */
    private static final int NEARBY_THRESHOLD = 2;

    private final Mohs mohs;
    private final ActorResolver actorResolver;
    private final ObjectMapper objectMapper;
    private final String basePath;

    public JobsController(Mohs mohs, ActorResolver actorResolver, ObjectMapper objectMapper, String basePath) {
        this.mohs = Objects.requireNonNull(mohs, "mohs");
        this.actorResolver = Objects.requireNonNull(actorResolver, "actorResolver");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.basePath = Objects.requireNonNull(basePath, "basePath");
    }

    @GetMapping
    public List<JobResponse> list() {
        return mohs.jobs().stream().map(JobResponse::from).toList();
    }

    @GetMapping("/{jobKey}")
    public JobResponse get(@PathVariable String jobKey) {
        return JobResponse.from(requireJob(jobKey));
    }

    /** The 202 comes from {@link ResponseEntity#getStatusCode()} in the method body — {@code @ResponseStatus} has no effect on a {@code ResponseEntity}. */
    @PostMapping("/{jobKey}/schedule")
    public ResponseEntity<AcceptedExecutionResponse> schedule(@PathVariable String jobKey, @RequestBody ScheduleJobRequest body,
            @RequestHeader(value = "Idempotency-Key", required = false) @Nullable String idempotencyKey, HttpServletRequest request) {
        JobKey key = requireJob(jobKey).definition().key();
        Object payload = convertPayload(key, body.payload());

        var command = mohs.schedule(key.value(), payload).as(actorResolver.resolve(request));
        if (body.priority() != null) {
            command = command.priority(body.priority());
        }
        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            command = command.idempotencyKey(idempotencyKey);
        }
        // ScheduleCommand's three terminals — at and delay were already validated as exclusive in the record
        Enqueued enqueued = body.at() != null ? command.at(body.at())
                : body.delay() != null ? command.after(body.delay())
                : command.now();

        AcceptedExecutionResponse accepted = AcceptedExecutionResponse.from(enqueued);
        URI location = ExecutionLocations.ofExecution(request, basePath, accepted.executionId());
        return ResponseEntity.accepted().location(location).body(accepted);
    }

    @PostMapping("/{jobKey}/pause")
    public JobResponse pause(@PathVariable String jobKey) {
        mohs.pause(requireJob(jobKey).definition().key());
        return JobResponse.from(requireJob(jobKey));
    }

    @PostMapping("/{jobKey}/resume")
    public JobResponse resume(@PathVariable String jobKey) {
        mohs.resume(requireJob(jobKey).definition().key());
        return JobResponse.from(requireJob(jobKey));
    }

    /**
     * Changes the schedule at runtime, under the house's emergency PATCH contract: the response
     * warns that the change holds until the next boot ({@code on-conflict=override} restores the
     * code's version with a logged diff).
     *
     * <p>The body is a {@link ScheduleView} ({@code CRON}/{@code INTERVAL}/{@code ON_DEMAND}); an
     * unrealisable schedule (a cron that never fires) becomes a 422 that teaches, never a 500.
     */
    @PatchMapping("/{jobKey}/schedule")
    public RuntimePatchResponse<JobResponse> reschedule(@PathVariable String jobKey, @RequestBody ScheduleView body,
            HttpServletRequest request) {
        JobKey key = requireJob(jobKey).definition().key();
        // The actor is validated BEFORE the mutation: a 4xx is a contract of "nothing changed" —
        // resolving it afterwards left the schedule altered with a 400 in the client's hand and NO
        // audit trail (an actor is non-negotiable on a mutation)
        String actor = actorResolver.resolve(request);
        JobSnapshot snapshot = rescheduleOrReject(key, body)
                .orElseThrow(() -> new JobNotFoundException(key, nearbyJobKeys(key)));
        // Logs what THIS actor asked for (the body), not the post-write snapshot — an audit trail
        // records intent, and two concurrent PATCHes never swap authorship
        log.info("job '{}' rescheduled at runtime by '{}' to {} — an emergency change, reverting on the next boot under on-conflict=override",
                key.value(), actor, body);
        return RuntimePatchResponse.of(JobResponse.from(snapshot));
    }

    /**
     * Every IAE in this scope is schedule validation by construction ({@code jobKey} has already
     * passed {@code requireJob}): the compact constructors of the specs in {@code toSchedule()} (a
     * non-positive interval, a blank cron) and {@code NextFireCalculator} (an unrealisable cron).
     * The contract's 422 covers both sources; outside this scope, an unrelated IAE is never
     * translated.
     */
    private Optional<JobSnapshot> rescheduleOrReject(JobKey key, ScheduleView body) {
        try {
            return mohs.reschedule(key, body.toSchedule());
        } catch (IllegalArgumentException e) {
            throw new PayloadValidationException("schedule", Objects.requireNonNullElse(e.getMessage(), e.toString()));
        }
    }

    /** {@code size} — ver {@link CursorPage#DEFAULT_PAGE_SIZE}/{@link CursorPage#MAX_PAGE_SIZE}. The list is a summary ({@link ExecutionSummaryResponse}) — attempts live in the detail view. */
    @GetMapping("/{jobKey}/executions")
    public CursorPage<ExecutionSummaryResponse> executions(
            @PathVariable String jobKey, @RequestParam(required = false) @Nullable String cursor, @RequestParam(required = false) @Nullable Integer size) {
        JobKey key = requireJob(jobKey).definition().key();
        int pageSize = CursorPage.clampSize(size);
        List<Execution> fetched = mohs.executions(new ExecutionQuery(key, null, null, null, cursor, pageSize + 1));
        List<ExecutionSummaryResponse> responses = fetched.stream().map(ExecutionSummaryResponse::from).toList();
        return CursorPage.of(responses, pageSize, ExecutionSummaryResponse::executionId);
    }

    private JobSnapshot requireJob(String jobKeyValue) {
        JobKey key = JobKey.of(jobKeyValue);
        return mohs.findJob(key).orElseThrow(() -> new JobNotFoundException(key, nearbyJobKeys(key)));
    }

    private List<JobKey> nearbyJobKeys(JobKey key) {
        return mohs.jobs().stream()
                .map(snapshot -> snapshot.definition().key())
                .filter(candidate -> !candidate.equals(key))
                .filter(candidate -> levenshtein(candidate.value(), key.value()) <= NEARBY_THRESHOLD)
                .toList();
    }

    private Object convertPayload(JobKey key, Map<String, Object> rawPayload) {
        Optional<Class<?>> payloadType = mohs.payloadType(key);
        if (payloadType.isEmpty()) {
            if (!rawPayload.isEmpty()) {
                throw new PayloadValidationException("payload", "job '" + key.value() + "' does not accept a payload");
            }
            return rawPayload;
        }
        try {
            return objectMapper.convertValue(rawPayload, payloadType.get());
        } catch (DatabindException e) {
            // Jackson 3: a databind failure propagates DatabindException directly — it is no longer
            // wrapped in IllegalArgumentException as it was in Jackson 2.
            throw new PayloadValidationException("payload",
                    "payload incompatible with " + payloadType.get().getSimpleName() + ": " + e.getMessage());
        }
    }

    private static int levenshtein(String a, String b) {
        int[] previous = new int[b.length() + 1];
        for (int j = 0; j <= b.length(); j++) {
            previous[j] = j;
        }
        for (int i = 1; i <= a.length(); i++) {
            int[] current = new int[b.length() + 1];
            current[0] = i;
            for (int j = 1; j <= b.length(); j++) {
                int cost = a.charAt(i - 1) == b.charAt(j - 1) ? 0 : 1;
                current[j] = Math.min(Math.min(current[j - 1] + 1, previous[j] + 1), previous[j - 1] + cost);
            }
            previous = current;
        }
        return previous[b.length()];
    }
}
