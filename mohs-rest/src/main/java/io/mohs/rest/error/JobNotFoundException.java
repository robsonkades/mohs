package io.mohs.rest.error;

import java.io.Serial;
import java.util.List;
import java.util.Objects;

import io.mohs.core.job.JobKey;

/**
 * Job inexistente numa rota que espera um {@code jobKey} válido. Carrega
 * sugestões por distância de edição (ver
 * {@code docs/MOHS-DOCUMENTO-MESTRE.md} §5.13) pra
 * {@link io.mohs.rest.error.RestExceptionHandler} anexar no
 * {@code ProblemDetail} — "404 com sugestão de jobKeys próximos" (ver
 * {@code docs/REST-API-DESIGN.md}).
 */
public final class JobNotFoundException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final JobKey jobKey;
    private final List<JobKey> nearbyJobKeys;

    public JobNotFoundException(JobKey jobKey, List<JobKey> nearbyJobKeys) {
        super("Job not found: " + jobKey.value());
        this.jobKey = Objects.requireNonNull(jobKey, "jobKey");
        this.nearbyJobKeys = List.copyOf(nearbyJobKeys);
    }

    public JobKey jobKey() {
        return jobKey;
    }

    public List<JobKey> nearbyJobKeys() {
        return nearbyJobKeys;
    }
}
