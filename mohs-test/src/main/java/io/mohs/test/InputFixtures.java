package io.mohs.test;

import java.util.Map;
import io.mohs.core.job.JobKey;
import io.mohs.core.job.JobRef;
import io.mohs.engine.InputCodec;
import io.mohs.engine.JobPayload;

/** Explicit input snapshots for persistence and dispatch test fixtures. */
public final class InputFixtures {
    private InputFixtures() {}

    public static JobPayload encode(JobKey key, Object value) {
        Class<?> type = value instanceof Map<?, ?> ? Map.class : value.getClass();
        return InputCodec.JSON.encode(new JobRef<>(key, type, key.value()), value);
    }

    public static <T> T decode(JobPayload value, Class<T> type) {
        return InputCodec.JSON.decode(value, JobRef.of("fixture", type, value.inputContract()));
    }
}
