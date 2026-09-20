package io.mohs.engine;

import java.util.Objects;
import io.mohs.core.job.JobRef;
import tools.jackson.databind.json.JsonMapper;

/**
 * Owns Mohs' JSON format for typed submission, dispatch and HTTP input conversion.
 * Snapshots carry a caller-declared contract, never a persisted Java class name.
 * Unknown fields are rejected; changing the input shape requires an explicit contract decision.
 * The host application's HTTP mapper does not configure this durable format.
 */
public final class InputCodec {
    public static final InputCodec JSON = new InputCodec();
    private final JsonMapper mapper = JsonMapper.builder()
            .enable(tools.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();

    private InputCodec() {}

    /** Immutable Jackson 3 mapper used by the HTTP adapter; it is not installed as a host bean. */
    public JsonMapper requestMapper() { return mapper; }

    public JobPayload encode(JobRef<?> reference, Object value) {
        Objects.requireNonNull(reference, "reference");
        Objects.requireNonNull(value, "value");
        Class<?> type = org.springframework.util.ClassUtils.resolvePrimitiveIfNecessary(reference.payloadType());
        if (!type.isInstance(value)) throw new IllegalArgumentException("input does not match " + reference.inputContract());
        var encoded = new JobPayload(mapper.writeValueAsString(value), reference.inputContract());
        decode(encoded, reference); // Refuse inputs that cannot reconstruct their declared contract.
        return encoded;
    }

    public <T> T decode(JobPayload encoded, JobRef<T> reference) {
        Objects.requireNonNull(encoded, "encoded");
        Objects.requireNonNull(reference, "reference");
        if (!encoded.inputContract().equals(reference.inputContract())) {
            throw new IllegalArgumentException("input contract '" + encoded.inputContract()
                    + "' does not match registered contract '" + reference.inputContract() + "'");
        }
        return Objects.requireNonNull(mapper.readValue(encoded.json(), reference.payloadType()), "decoded input");
    }
}
