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
import io.mohs.core.SubmissionOptions;
import io.mohs.core.job.JobRef;
import io.mohs.core.event.Enqueued;
import io.mohs.core.execution.Execution;
import io.mohs.core.job.JobKey;
import io.mohs.rest.AcceptedExecutionResponse;
import io.mohs.rest.ActorResolver;
import io.mohs.rest.ApiPaths;
import io.mohs.rest.CursorPage;
import io.mohs.rest.ExecutionLocations;
import io.mohs.rest.RequestIdentifiers;
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

    /**
     * Creates a {@code JobsController} with the supplied values.
     *
     * @param mohs the scheduling and operations facade
     * @param actorResolver the resolver that attributes HTTP operations to an actor
     * @param objectMapper the JSON mapper used for payload conversion
     * @param basePath the configured REST route prefix
     */
    public JobsController(Mohs mohs, ActorResolver actorResolver, ObjectMapper objectMapper, String basePath) {
        this.mohs = Objects.requireNonNull(mohs, "mohs");
        this.actorResolver = Objects.requireNonNull(actorResolver, "actorResolver");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.basePath = Objects.requireNonNull(basePath, "basePath");
    }

    /**
     * Lists the registered jobs.
     *
     * @return the registered job snapshots
     */
    @GetMapping
    public List<JobResponse> list() {
        return mohs.jobs().stream().map(JobResponse::from).toList();
    }

    /**
     * Returns a job definition and its operational state.
     *
     * @param jobKey the stable identity of the job
     * @return the requested job snapshot
     */
    @GetMapping("/{jobKey}")
    public JobResponse get(@PathVariable String jobKey) {
        return JobResponse.from(requireJob(jobKey));
    }

    /**
     * The 202 comes from {@link ResponseEntity#getStatusCode()} in the method body — {@code @ResponseStatus} has no effect on a {@code ResponseEntity}.
     *
     * @param jobKey the stable identity of the job
     * @param body the request body
     * @param idempotencyKey the optional key used to deduplicate scheduling requests
     * @param request the incoming HTTP request
     * @return the accepted enqueue receipt and execution location
     */
    @PostMapping("/{jobKey}/schedule")
    public ResponseEntity<AcceptedExecutionResponse> schedule(@PathVariable String jobKey, @RequestBody ScheduleJobRequest body,
            @RequestHeader(value = "Idempotency-Key", required = false) @Nullable String idempotencyKey, HttpServletRequest request) {
        JobKey key = requireJobKey(jobKey);
        JobRef<?> reference = mohs.jobRef(key).orElseThrow(() ->
                new PayloadValidationException("payload", "job executable is unavailable on this worker"));
        SubmissionOptions options = SubmissionOptions.defaults().withActor(actorResolver.resolve(request));
        if (body.priority() != null) options = options.withPriority(body.priority());
        if (idempotencyKey != null) {
            SubmissionOptions current = options;
            options = PayloadValidationException.validating("Idempotency-Key", () -> current.withIdempotencyKey(idempotencyKey));
        }
        AcceptedExecutionResponse accepted = AcceptedExecutionResponse.from(enqueue(reference, body, options));
        URI location = ExecutionLocations.ofExecution(request, basePath, accepted.executionId());
        return ResponseEntity.accepted().location(location).body(accepted);
    }

    /** Captures the local reference type once, then calls the public typed submission operation. */
    private <T> Enqueued enqueue(JobRef<T> reference, ScheduleJobRequest body, SubmissionOptions options) {
        T input = convertTo(reference.payloadType(), body.payload());
        if (body.at() != null) return mohs.scheduleAt(reference, input, body.at(), options);
        if (body.delay() != null) return mohs.scheduleAfter(reference, input, body.delay(), options);
        return mohs.enqueue(reference, input, options);
    }

    /**
     * Answers with the snapshot re-read AFTER the write: {@code paused} is state the definition does not carry, and the pre-write snapshot would say the opposite of what just happened.
     *
     * @param jobKey the stable identity of the job
     * @return the paused job snapshot
     * @param request the request supplying the audit actor
     */
    @PostMapping("/{jobKey}/pause")
    public JobResponse pause(@PathVariable String jobKey, HttpServletRequest request) {
        JobKey key = requireJobKey(jobKey);
        String actor = actorResolver.resolve(request);
        mohs.pause(key);
        log.info("job paused: job={}, actor={}", key.value(), actor);
        return JobResponse.from(requireJob(jobKey));
    }

    /**
     * See {@link #pause} — the same post-write re-read.
     *
     * @param jobKey the stable identity of the job
     * @return the resumed job snapshot
     * @param request the request supplying the audit actor
     */
    @PostMapping("/{jobKey}/resume")
    public JobResponse resume(@PathVariable String jobKey, HttpServletRequest request) {
        JobKey key = requireJobKey(jobKey);
        String actor = actorResolver.resolve(request);
        mohs.resume(key);
        log.info("job resumed: job={}, actor={}", key.value(), actor);
        return JobResponse.from(requireJob(jobKey));
    }

    /**
     * Changes the schedule at runtime, under the house's emergency PATCH contract: the response
     * warns that the change holds until the next boot ({@code on-conflict=override} restores the
     * code's version with a logged diff).
     *
     * <p>The body is a {@link ScheduleView} ({@code CRON}/{@code INTERVAL}/{@code ON_DEMAND}); an
     * unrealisable schedule (a cron that never fires) becomes a 422 that teaches, never a 500.
     *
     * @param jobKey the stable identity of the job
     * @param body the request body
     * @param request the incoming HTTP request
     * @return the updated job with its runtime-change notice
     */
    @PatchMapping("/{jobKey}/schedule")
    public RuntimePatchResponse<JobResponse> reschedule(@PathVariable String jobKey, @RequestBody ScheduleView body,
            HttpServletRequest request) {
        JobKey key = requireJobKey(jobKey);
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
        return PayloadValidationException.validating("schedule", () -> mohs.reschedule(key, body.toSchedule()));
    }

    /**
     * {@code size} — see {@link CursorPage#DEFAULT_PAGE_SIZE}/{@link CursorPage#MAX_PAGE_SIZE}. The list is a summary ({@link ExecutionSummaryResponse}) — attempts live in the detail view.
     *
     * @param jobKey the stable identity of the job
     * @param cursor the last execution identity of the previous page, or {@code null}
     * @param size the requested page size
     * @return the job's execution summaries and optional continuation cursor
     */
    @GetMapping("/{jobKey}/executions")
    public CursorPage<ExecutionSummaryResponse> executions(
            @PathVariable String jobKey, @RequestParam(required = false) @Nullable String cursor, @RequestParam(required = false) @Nullable Integer size) {
        JobKey key = requireJobKey(jobKey);
        int pageSize = CursorPage.clampSize(size);
        var query = PayloadValidationException.validating("cursor", () -> new ExecutionQuery(key, null, null, null, cursor, pageSize + 1));
        List<Execution> fetched = mohs.executions(query);
        List<ExecutionSummaryResponse> responses = fetched.stream().map(ExecutionSummaryResponse::from).toList();
        return CursorPage.of(responses, pageSize, ExecutionSummaryResponse::executionId);
    }

    /** Every route under {@code /{jobKey}} starts here: the registered job, or the 404 with neighbouring suggestions. */
    private JobSnapshot requireJob(String jobKeyValue) {
        JobKey key = RequestIdentifiers.jobKey(jobKeyValue);
        return mohs.findJob(key).orElseThrow(() -> new JobNotFoundException(key, nearbyJobKeys(key)));
    }

    private JobKey requireJobKey(String jobKeyValue) {
        return requireJob(jobKeyValue).definition().key();
    }

    private List<JobKey> nearbyJobKeys(JobKey key) {
        return mohs.jobs().stream()
                .map(snapshot -> snapshot.definition().key())
                .filter(candidate -> !candidate.equals(key))
                .filter(candidate -> levenshtein(candidate.value(), key.value()) <= NEARBY_THRESHOLD)
                .toList();
    }

    private <T> T convertTo(Class<T> payloadType, Map<String, Object> rawPayload) {
        try {
            return objectMapper.convertValue(rawPayload, payloadType);
        } catch (DatabindException e) {
            // Jackson 3: a databind failure propagates DatabindException directly — it is no longer
            // wrapped in IllegalArgumentException as it was in Jackson 2.
            throw new PayloadValidationException("payload",
                    "payload does not match the job's declared input schema");
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
