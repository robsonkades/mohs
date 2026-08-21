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
 * O caminho feliz do dashboard, servido de verdade. Mora aqui e não no starter porque este é o
 * único módulo que declara {@code mohs-ui} — sem o jar, o {@code @ConditionalOnResource} de
 * {@code MohsUiAutoConfiguration} não liga, e lá só dá para provar a ausência.
 *
 * <p>Cobre o que a verificação manual cobria e o CI não: que o bundle construído pelo Vite chega
 * ao classpath com o nome que o resource handler procura. Mover {@code /mohs-ui-webapp} de lugar,
 * ou o mount de path, deixa de ser algo que só quebra em produção.
 *
 * <p>{@code HttpClient} do JDK em vez de {@code TestRestTemplate}: aquele exige
 * {@code spring-boot-restclient} no classpath de teste, e uma dependência a mais não se paga para
 * três GETs sem corpo.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "mohs.jdbc.dialect=h2",
        "spring.sql.init.schema-locations=classpath:schema-h2.sql",
        "spring.sql.init.mode=always"
})
class MohsUiIntegrationTest {

    /** O marcador que o {@code index.html} do Vite sempre carrega — é a raiz onde o React monta. */
    private static final String INDEX_MARKER = "<div id=\"root\"></div>";

    /** O hash muda a cada build, então o asset é descoberto a partir do próprio index. */
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
     * O mount pelado, que depende do {@code forward:} — exatamente o caminho que quebrava com 500
     * antes da correção em {@code MohsJobScanner} (ver errata da ADR-0045).
     */
    @Test
    void dashboardIndexIsServedAtTheBareMount() throws Exception {
        assertIndexServedAt("/mohs-ui");
    }

    /** Rota de cliente: não é asset, então o fallback do resolver tem que devolver o index. */
    @Test
    void clientSideRouteFallsBackToTheIndex() throws Exception {
        assertIndexServedAt("/mohs-ui/jobs");
    }

    /**
     * Os assets que o próprio index referencia — é o que prova que o bundle inteiro chegou ao
     * classpath, não só o {@code index.html}.
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
