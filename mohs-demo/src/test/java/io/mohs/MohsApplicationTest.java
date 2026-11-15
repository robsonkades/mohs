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
package io.mohs;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.web.client.RestClient;

import io.mohs.core.EngineState;
import io.mohs.core.Mohs;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.demo.examples.BatchExample;
import io.mohs.demo.examples.ProgrammaticDefinitionExample;
import io.mohs.demo.examples.RateLimitExample;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = MohsApplication.class,
        useMainMethod = SpringBootTest.UseMainMethod.ALWAYS,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.main.allow-circular-references=false",
                "spring.datasource.url=jdbc:h2:mem:demo-boot-test;DB_CLOSE_DELAY=-1",
                "spring.datasource.driver-class-name=org.h2.Driver",
                "spring.datasource.username=sa",
                "spring.datasource.password=",
                "mohs.api.enabled=true"
        })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class MohsApplicationTest {

    @Value("${local.server.port}")
    int port;

    @Test
    void demoStartsWithAllExamplesAndExecutesSubmissions(@Autowired Mohs mohs,
            @Autowired BatchExample batch, @Autowired RateLimitExample limits,
            @Autowired ProgrammaticDefinitionExample submissions) {
        assertThat(batch).isNotNull();
        assertThat(limits.currentBalances()).extracting(limit -> limit.rateLimit().name()).contains("example-smtp");
        assertThat(mohs.findJob(JobKey.of("example-send-newsletter"))).isPresent();
        var receipt = submissions.synchronizeNow("company-a");
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(mohs.lifecycle().state()).isEqualTo(EngineState.RUNNING);
            assertThat(mohs.findExecution(receipt.executionId()).orElseThrow().state())
                    .isEqualTo(ExecutionState.SUCCEEDED);
        });

        var client = RestClient.create("http://localhost:" + port);
        assertThat(client.get().uri("/actuator/health").retrieve().body(String.class))
                .contains("\"status\":\"UP\"");
        assertThat(client.get().uri("/api/mohs/v1/executions/" + receipt.executionId().value())
                .retrieve().body(String.class)).contains("\"state\":\"SUCCEEDED\"");
        assertThat(client.get().uri("/api/mohs/v1/overview").retrieve().body(String.class))
                .contains("\"throughput\"");
    }
}
