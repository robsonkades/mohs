package io.mohs.rest.job;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import jakarta.servlet.http.HttpServletRequest;

import org.jspecify.annotations.Nullable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
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
import io.mohs.rest.ActorResolver;
import io.mohs.rest.ApiPaths;
import io.mohs.rest.CursorPage;
import io.mohs.rest.error.JobNotFoundException;
import io.mohs.rest.error.PayloadValidationException;
import io.mohs.rest.execution.ExecutionResponse;

/**
 * Área de recurso "jobs" (ver {@code docs/REST-API-DESIGN.md}). Definição
 * de job é código, não API — não há {@code POST}/{@code PUT}/{@code DELETE}
 * aqui, só leitura e invocação sobre definição existente.
 */
@RestController
@RequestMapping("${mohs.api.base-path:" + ApiPaths.V1 + "}/jobs")
public class JobsController {

    /** Distância de edição máxima para sugerir um {@code jobKey} vizinho num 404 (§5.13 do documento mestre). */
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

    /** Status 202 vem de {@link ResponseEntity#getStatusCode()} no corpo do método — {@code @ResponseStatus} não tem efeito sobre {@code ResponseEntity} (REST-1). */
    @PostMapping("/{jobKey}/schedule")
    public ResponseEntity<AcceptedExecutionResponse> schedule(@PathVariable String jobKey, @RequestBody ScheduleJobRequest body,
            @RequestHeader(value = "Idempotency-Key", required = false) @Nullable String idempotencyKey, HttpServletRequest request) {
        JobKey key = requireJob(jobKey).definition().key();
        Object payload = convertPayload(key, body.payload());

        var command = mohs.schedule(key.value(), payload).as(actorResolver.resolve(request));
        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            command = command.idempotencyKey(idempotencyKey);
        }
        Enqueued enqueued = body.at() != null ? command.at(body.at()) : command.now();

        AcceptedExecutionResponse accepted = AcceptedExecutionResponse.from(enqueued);
        URI location = URI.create(basePath + "/executions/" + accepted.executionId());
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

    /** {@code size} — ver {@link CursorPage#DEFAULT_PAGE_SIZE}/{@link CursorPage#MAX_PAGE_SIZE}. */
    @GetMapping("/{jobKey}/executions")
    public CursorPage<ExecutionResponse> executions(
            @PathVariable String jobKey, @RequestParam(required = false) @Nullable String cursor, @RequestParam(required = false) @Nullable Integer size) {
        JobKey key = requireJob(jobKey).definition().key();
        int pageSize = CursorPage.clampSize(size);
        List<Execution> fetched = mohs.executions(new ExecutionQuery(key, null, null, null, cursor, pageSize + 1));
        List<ExecutionResponse> responses = fetched.stream().map(ExecutionResponse::from).toList();
        return CursorPage.of(responses, pageSize, ExecutionResponse::executionId);
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
            // Jackson 3: falha de databind propaga DatabindException direto —
            // não é mais embrulhada em IllegalArgumentException como no Jackson 2.
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
