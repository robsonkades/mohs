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
 * {@code GET /runners} servido de verdade, com o contexto inteiro montado: o
 * {@code RunnerRegistry} que a auto-config construiu a partir das properties,
 * a fachada {@code Mohs} e o controller.
 *
 * <p>Mora aqui e não em {@code mohs-rest} porque o teste de contrato daquele
 * módulo usa um {@code Mohs} mockado — ele prova o formato do wire, não que
 * o registry real chega ao endpoint. Toda a fiação entre os três módulos só
 * existe montada, e é exatamente ela que quebraria em silêncio: um runner que
 * some da lista não derruba nada, só deixa a página vazia.
 *
 * <p>O runner {@code io} é o único que a aplicação sempre tem —
 * {@code RunnerRegistry} recusa nascer sem ele —, então é a asserção que vale
 * para qualquer configuração.
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
     * Desserializa em vez de casar substring: {@code contains("\"max\":")}
     * passaria com {@code null} ou com um número negativo, e quebraria se o
     * Jackson mudasse de formatação — provaria que a chave existe, não que o
     * valor faz sentido. O round-trip também exercita o DTO público, que é o
     * que um consumidor externo realmente consome.
     */
    @Test
    void runnersEndpointServesTheRegistryOfThisNode() throws IOException, InterruptedException {
        HttpResponse<String> response = get("/api/mohs/v1/runners");

        assertThat(response.statusCode()).isEqualTo(200);
        List<RunnerResponse> runners = JsonMapper.builder().build()
                .readValue(response.body(), new TypeReference<List<RunnerResponse>>() { });

        // 'io' é o único runner que o RunnerRegistry exige que exista
        assertThat(runners).anySatisfy(runner -> {
            assertThat(runner.name()).isEqualTo("io");
            assertThat(runner.mode()).isEqualTo(RunnerMode.IO);
        });
        // sem cravar valor: o demo tem job de verdade rodando, e a ocupação varia
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
