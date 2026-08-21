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
import io.mohs.rest.ActorResolver;
import io.mohs.rest.ApiPaths;
import io.mohs.rest.CursorPage;
import io.mohs.rest.RuntimePatchResponse;
import io.mohs.rest.error.JobNotFoundException;
import io.mohs.rest.error.PayloadValidationException;
import io.mohs.rest.execution.ExecutionSummaryResponse;

/**
 * Área de recurso "jobs" (ver {@code docs/REST-API-DESIGN.md}). Definição
 * de job é código, não API — não há {@code POST}/{@code PUT}/{@code DELETE}
 * de definição aqui, só leitura e invocação sobre definição existente; o
 * {@code PATCH .../schedule} (ADR-0036) é ajuste runtime de emergência
 * sobre a agenda, não definição.
 */
@RestController
@RequestMapping("${mohs.api.base-path:" + ApiPaths.V1 + "}/jobs")
public class JobsController {

    private static final Logger log = LoggerFactory.getLogger(JobsController.class);

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
        if (body.priority() != null) {
            command = command.priority(body.priority());
        }
        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            command = command.idempotencyKey(idempotencyKey);
        }
        // os três terminais de ScheduleCommand — at/delay já validados como exclusivos no record
        Enqueued enqueued = body.at() != null ? command.at(body.at())
                : body.delay() != null ? command.after(body.delay())
                : command.now();

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

    /**
     * ADR-0036 — muda a agenda em runtime, sob o contrato de PATCH de
     * emergência da casa: a resposta avisa que a mudança vale até o próximo
     * boot ({@code on-conflict=override} restaura o código com diff
     * logado). Corpo é {@link ScheduleView} ({@code CRON}/{@code INTERVAL}/
     * {@code ON_DEMAND}); agenda irrealizável (cron que nunca dispara) vira
     * 422 que ensina, nunca 500.
     */
    @PatchMapping("/{jobKey}/schedule")
    public RuntimePatchResponse<JobResponse> reschedule(@PathVariable String jobKey, @RequestBody ScheduleView body,
            HttpServletRequest request) {
        JobKey key = requireJob(jobKey).definition().key();
        // actor validado ANTES da mutação (review ADR-0036): 4xx é contrato de "nada
        // mudou" — resolver depois deixava a agenda alterada com 400 na mão do cliente
        // e SEM a trilha de auditoria (actor é inegociável em mutação, ADR-0010)
        String actor = actorResolver.resolve(request);
        JobSnapshot snapshot = rescheduleOrReject(key, body)
                .orElseThrow(() -> new JobNotFoundException(key, nearbyJobKeys(key)));
        // loga o que ESTE actor pediu (o body), não o snapshot pós-escrita — trilha de
        // auditoria registra intenção; dois PATCHes concorrentes nunca trocam de autoria
        log.info("job '{}' rescheduled at runtime by '{}' to {} — emergency change (ADR-0036), reverts on next boot under on-conflict=override",
                key.value(), actor, body);
        return RuntimePatchResponse.of(JobResponse.from(snapshot));
    }

    /**
     * Toda IAE deste escopo é validação de agenda por construção
     * ({@code jobKey} já passou por {@code requireJob}): os compact
     * constructors dos specs em {@code toSchedule()} (interval não
     * positivo, cron em branco) e o {@code NextFireCalculator} (cron
     * irrealizável) — o 422 do contrato cobre as duas fontes; fora deste
     * escopo, IAE alheia nunca é traduzida.
     */
    private Optional<JobSnapshot> rescheduleOrReject(JobKey key, ScheduleView body) {
        try {
            return mohs.reschedule(key, body.toSchedule());
        } catch (IllegalArgumentException e) {
            throw new PayloadValidationException("schedule", Objects.requireNonNullElse(e.getMessage(), e.toString()));
        }
    }

    /** {@code size} — ver {@link CursorPage#DEFAULT_PAGE_SIZE}/{@link CursorPage#MAX_PAGE_SIZE}. Lista é sumário ({@link ExecutionSummaryResponse}) — attempts moram no detalhe. */
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
