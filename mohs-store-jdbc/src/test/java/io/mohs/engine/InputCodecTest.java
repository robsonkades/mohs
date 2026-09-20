package io.mohs.engine;

import org.junit.jupiter.api.Test;
import io.mohs.core.NoInput;
import io.mohs.core.job.JobRef;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InputCodecTest {
    record BeforeRename(String account, int amount) {}
    record AfterRename(String account, int amount) {}
    private final InputCodec codec = InputCodec.JSON;

    @Test
    void aCompatibleClassRenameDoesNotChangeThePersistedContract() {
        var oldReference = JobRef.of("charge", BeforeRename.class, "charge-input-v1");
        var newReference = JobRef.of("charge", AfterRename.class, "charge-input-v1");
        var encoded = codec.encode(oldReference, new BeforeRename("A", 42));
        assertThat(encoded.inputContract()).isEqualTo("charge-input-v1");
        assertThat(codec.decode(encoded, newReference)).isEqualTo(new AfterRename("A", 42));
    }

    @Test
    void matchingJsonShapeDoesNotMakeDifferentContractsInterchangeable() {
        var reference = JobRef.of("charge", BeforeRename.class, "charge-input-v1");
        var encoded = codec.encode(reference, new BeforeRename("A", 42));
        assertThatThrownBy(() -> codec.decode(encoded, JobRef.of("charge", BeforeRename.class, "charge-input-v2")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("does not match registered contract");
    }

    @Test
    void noInputHasAnExplicitTypeAndRoundTrips() {
        var reference = JobRef.of("tick", NoInput.class);
        assertThat(codec.decode(codec.encode(reference, NoInput.INSTANCE), reference)).isEqualTo(NoInput.INSTANCE);
    }

    @Test
    void persistedAndHttpInputsRejectUnknownFieldsUnderTheSameContract() {
        var reference = JobRef.of("charge", BeforeRename.class, "charge-input-v1");
        var input = new JobPayload("{\"account\":\"A\",\"amount\":42,\"removed\":true}", reference.inputContract());
        assertThatThrownBy(() -> codec.decode(input, reference))
                .isInstanceOf(tools.jackson.databind.DatabindException.class);
        assertThatThrownBy(() -> codec.requestMapper().readValue(input.json(), reference.payloadType()))
                .isInstanceOf(tools.jackson.databind.DatabindException.class);
    }
}
