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
