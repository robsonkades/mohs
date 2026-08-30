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
package io.mohs.autoconfigure;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a SIGTERM costs with DASHBOARDS OPEN — the other half of the question
 * {@code ShutdownLatencyScenario} answers for the engine. There the clock measures
 * {@code engine.stop(grace)}; here it measures the web server's phase, which is sequential to the
 * engine's and therefore ADDS to it.
 *
 * <p>It starts the whole application (embedded Tomcat, {@code server.shutdown=graceful}, the
 * operational API on), opens N real SSE connections to {@code /overview/stream} and times
 * {@code context.close()}. The emitters are created with timeout {@code 0L} and Boot's
 * {@code GracefulShutdown} waits while {@code getInProgressAsyncCount() > 0}: without
 * {@link MohsOverviewStreamLifecycle} closing the streams one phase earlier, the whole phase only
 * ends when {@code spring.lifecycle.timeout-per-shutdown-phase} expires.
 *
 * <p>It lives in this module rather than in {@code mohs-benchmark} because this is the only one
 * where the web stack exists: the benchmark depends on {@code mohs-store-jdbc} and has neither
 * Tomcat nor the starter on its classpath. Run by name (surefire's default pattern does not match
 * {@code *Harness}): {@code ./mvnw -o test -pl mohs-spring-boot-starter -am
 * -Dtest=ShutdownWithOpenStreamsHarness -Dsurefire.failIfNoSpecifiedTests=false
 * -Dskip.frontend=true}.
 */
class ShutdownWithOpenStreamsHarness {

    /** One dashboard is enough to hold the phase; 3 show that the cost does not scale with the number of subscribers. */
    private static final int OPEN_DASHBOARDS = 3;

    /**
     * Deliberately shortened from Boot's 30s default: it is the CEILING the phase spends when
     * nobody closes the streams, and the harness does not need to pay half a minute to show that it
     * hit the ceiling.
     */
    private static final Duration PHASE_TIMEOUT = Duration.ofSeconds(10);

    @SpringBootApplication
    static class App {

        @Bean
        DataSource dataSource() {
            JdbcDataSource h2 = new JdbcDataSource();
            h2.setURL("jdbc:h2:mem:mohs-shutdown-harness-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
            h2.setUser("sa");
            h2.setPassword("");
            new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
            return h2;
        }
    }

    @Test
    void openDashboardsDoNotHoldTheShutdown() throws Exception {
        ConfigurableApplicationContext context = new SpringApplicationBuilderCompat().run();
        int port = ((WebServerApplicationContext) context).getWebServer().getPort();

        // The JDK's HttpClient: the response arrives as an InputStream and the connection stays
        // OPEN while nobody reads it to the end — which is exactly what the container counts as an
        // active async request
        try (HttpClient client = HttpClient.newHttpClient()) {
            List<HttpResponse<InputStream>> streams = new ArrayList<>();
            for (int i = 0; i < OPEN_DASHBOARDS; i++) {
                streams.add(client.send(
                        HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/mohs/v1/overview/stream"))
                                .header("Accept", "text/event-stream")
                                .build(),
                        HttpResponse.BodyHandlers.ofInputStream()));
            }
            assertThat(streams).allSatisfy(response -> assertThat(response.statusCode()).isEqualTo(200));

            long signalAt = System.nanoTime();
            context.close();
            Duration shutdownTook = Duration.ofNanos(System.nanoTime() - signalAt);

            System.out.printf("""

                    === Shutdown with open dashboards ===
                    open SSE streams        : %d
                    phase timeout configured: %s   <- the ceiling when nobody closes the streams
                    context.close() took    : %.2fs
                    """, OPEN_DASHBOARDS, PHASE_TIMEOUT, shutdownTook.toNanos() / 1e9);

            assertThat(shutdownTook)
                    .as("an open dashboard must not hold the shutdown — %s means the graceful phase waited for a "
                            + "stream that never ends on its own", shutdownTook)
                    .isLessThan(PHASE_TIMEOUT.dividedBy(2));
        }
    }

    /** Only to keep the {@link SpringApplication} assembly out of the test body — what the test measures is the clock, not the configuration. */
    private static final class SpringApplicationBuilderCompat {

        ConfigurableApplicationContext run() {
            SpringApplication application = new SpringApplication(App.class);
            application.setWebApplicationType(WebApplicationType.SERVLET);
            application.setDefaultProperties(java.util.Map.of(
                    "server.port", "0",
                    "server.shutdown", "graceful",
                    "spring.lifecycle.timeout-per-shutdown-phase", PHASE_TIMEOUT.toString(),
                    "mohs.api.enabled", "true",
                    "mohs.jdbc.dialect", "h2",
                    // With no jobs the engine drains instantly: the measured number is the web
                    // server's phase, isolated from the cost of the drain
                    "mohs.lifecycle.start-mode", "auto"));
            return application.run();
        }
    }
}
