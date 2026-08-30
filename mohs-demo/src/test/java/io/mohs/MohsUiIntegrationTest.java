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
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The dashboard's happy path, actually served.
 *
 * <p>It lives here rather than in the starter because this is the only module that declares
 * {@code mohs-ui}: without the jar, {@code MohsUiAutoConfiguration}'s {@code @ConditionalOnResource}
 * never activates, and over there only its ABSENCE can be proven.
 *
 * <p>It covers what manual verification covered and CI did not: that the bundle Vite builds reaches
 * the classpath under the name the resource handler looks for. Moving {@code /mohs-ui-webapp}, or
 * the path it is mounted at, stops being something that only breaks in production.
 *
 * <p>The JDK's {@code HttpClient} rather than {@code TestRestTemplate}: the latter needs
 * {@code spring-boot-restclient} on the test classpath, and one more dependency does not pay for
 * three body-less GETs.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "mohs.jdbc.dialect=h2",
        "spring.sql.init.schema-locations=classpath:schema-h2.sql",
        "spring.sql.init.mode=always"
})
class MohsUiIntegrationTest {

    /** The marker every Vite {@code index.html} carries — the root React mounts into. */
    private static final String INDEX_MARKER = "<div id=\"root\"></div>";

    /** The hash changes on every build, so the asset is discovered from the index itself. */
    private static final Pattern ASSET = Pattern.compile("/mohs-ui/assets/[A-Za-z0-9._-]+\\.(?:js|css)");

    @Value("${local.server.port}")
    private int port;

    private final HttpClient httpClient = HttpClient.newHttpClient();

    private HttpResponse<String> get(String path) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private void assertIndexServedAt(String path) throws IOException, InterruptedException {
        HttpResponse<String> response = get(path);

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains(INDEX_MARKER);
    }

    /**
     * The bare mount, which depends on the {@code forward:} — exactly the path that used to fail
     * with a 500 before the fix in {@code MohsJobScanner}.
     */
    @Test
    void dashboardIndexIsServedAtTheBareMount() throws Exception {
        assertIndexServedAt("/mohs-ui");
    }

    /** A client-side route is not an asset, so the resolver's fallback has to return the index. */
    @Test
    void clientSideRouteFallsBackToTheIndex() throws Exception {
        assertIndexServedAt("/mohs-ui/jobs");
    }

    /**
     * The assets the index itself references — which is what proves the whole bundle reached the
     * classpath, not just {@code index.html}.
     */
    @Test
    void hashedAssetsReferencedByTheIndexAreServed() throws Exception {
        Matcher matcher = ASSET.matcher(get("/mohs-ui").body());
        List<String> assets = matcher.results().map(MatchResult::group).distinct().toList();

        assertThat(assets).as("assets referenced by index.html").isNotEmpty();
        for (String asset : assets) {
            assertThat(get(asset).statusCode()).as(asset).isEqualTo(200);
        }
    }
}
