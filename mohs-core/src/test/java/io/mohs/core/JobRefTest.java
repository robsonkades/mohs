package io.mohs.core;

import io.mohs.core.job.JobKey;
import io.mohs.core.job.JobRef;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JobRefTest {

    record WelcomeEmail(String user) {
    }

    @Test
    void ofBindsKeyAndPayloadType() {
        JobRef<WelcomeEmail> ref = JobRef.of("welcome-email", WelcomeEmail.class);

        assertThat(ref.key()).isEqualTo(JobKey.of("welcome-email"));
        assertThat(ref.payloadType()).isEqualTo(WelcomeEmail.class);
    }

    @Test
    void rejectsNullPayloadType() {
        assertThatThrownBy(() -> new JobRef<>(JobKey.of("id"), null))
                .isInstanceOf(NullPointerException.class);
    }
}
