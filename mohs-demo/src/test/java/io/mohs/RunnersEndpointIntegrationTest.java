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

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.resource.RunnerMode;
import io.mohs.rest.runner.RunnerResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code GET /runners} actually served, with the whole context assembled: the
 * {@code RunnerRegistry} auto-configuration built from the properties, the {@code Mohs} facade and
 * the controller.
 *
 * <p>It lives here rather than in {@code mohs-rest} because that module's contract test uses a
 * mocked {@code Mohs} — it proves the wire format, not that the real registry reaches the endpoint.
 * The wiring between the three modules only exists once assembled, and it is exactly what would
 * break silently: a runner missing from the list brings nothing down, it just leaves the page
 * empty.
 *
 * <p>The {@code io} runner is the only one an application always has — {@code RunnerRegistry}
 * refuses to be created without it — so it is the assertion that holds for any configuration.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "mohs.jdbc.dialect=h2",
        "spring.sql.init.schema-locations=classpath:schema-h2.sql",
        "spring.sql.init.mode=always",
        "mohs.api.enabled=true"
})
class RunnersEndpointIntegrationTest {

    @Value("${local.server.port}")
    private int port;

    /**
     * Deserialises rather than matching a substring: {@code contains("\"max\":")} would pass with
     * {@code null} or with a negative number, and would break if Jackson changed its formatting —
     * proving the key exists, not that the value makes sense. The round trip also exercises the
     * public DTO, which is what an external consumer actually consumes.
     */
    @Test
    void runnersEndpointServesTheRegistryOfThisNode() throws IOException, InterruptedException {
        HttpResponse<String> response = get("/api/mohs/v1/runners");

        assertThat(response.statusCode()).isEqualTo(200);
        List<RunnerResponse> runners = JsonMapper.builder().build()
                .readValue(response.body(), new TypeReference<List<RunnerResponse>>() { });

        // 'io' is the only runner the RunnerRegistry insists on
        assertThat(runners).anySatisfy(runner -> {
            assertThat(runner.name()).isEqualTo("io");
            assertThat(runner.mode()).isEqualTo(RunnerMode.IO);
        });
        // No exact value: the demo has real jobs running, so occupancy varies
        assertThat(runners).allSatisfy(runner -> {
            assertThat(runner.max()).isPositive();
            assertThat(runner.running()).isNotNegative();
        });
    }

    private HttpResponse<String> get(String path) throws IOException, InterruptedException {
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).GET().build();
            return client.send(request, HttpResponse.BodyHandlers.ofString());
        }
    }
}
