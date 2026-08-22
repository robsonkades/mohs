package io.mohs;

import java.util.Map;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.AutoConfigurationExcludeFilter;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.TypeExcludeFilter;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

/**
 * {@code @ComponentScan} exclui {@code io.mohs.rest} — sem isso, o
 * component-scan default (raiz {@code io.mohs}) acharia
 * {@code JobsController}/{@code ExecutionsController} sozinho, colidindo
 * com o {@code @Bean} explícito de {@code MohsRestAutoConfiguration}
 * sempre que {@code mohs.api.enabled=true} (dois bean definitions pro
 * mesmo tipo). Excluir o pacote também faz este app de dev exercitar
 * exatamente o caminho de auto-configuration que um consumidor real usa
 * — o {@code main()} só mora dentro do pacote da própria lib por
 * conveniência de desenvolvimento.
 *
 * <p>{@code excludeFilters} repete os dois filtros default de {@code
 * @SpringBootApplication} ({@link TypeExcludeFilter}, {@link
 * AutoConfigurationExcludeFilter}) porque declarar {@code @ComponentScan}
 * diretamente na classe SUBSTITUI o meta-anotado, não soma a ele — sem
 * isso, {@code MohsAutoConfiguration}/{@code MohsRestAutoConfiguration}
 * (ambos sob {@code io.mohs}) voltam a ser achadas pelo scan comum, além
 * do caminho de auto-configuration — confirmado batendo de frente com
 * {@code @WebMvcTest} de qualquer outro controller (bean duplicado /
 * {@code mohsClock} exigindo {@code DataSource} numa fatia que não devia
 * carregar o motor nenhum).
 */
@SpringBootApplication
@ComponentScan(basePackages = "io.mohs", excludeFilters = {
        @ComponentScan.Filter(type = FilterType.CUSTOM, classes = TypeExcludeFilter.class),
        @ComponentScan.Filter(type = FilterType.CUSTOM, classes = AutoConfigurationExcludeFilter.class),
        @ComponentScan.Filter(type = FilterType.REGEX, pattern = "io\\.mohs\\.rest\\..*")
})
public class MohsApplication {

    /**
     * Config de dev local vive aqui, nunca em {@code src/main/resources/
     * application.yaml}: um {@code application.yaml} no classpath root do
     * jar da biblioteca compete com o do aplicativo hospedeiro (só um é
     * carregado, decidido pela ordem do classpath) — config de aplicação é
     * sempre do app, nunca da dependência. {@code defaultProperties} perde
     * pra qualquer fonte externa (arquivo do dev, argumento, env var): só
     * preenche o vazio, e só quando é este {@code main()} que sobe.
     */
    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(MohsApplication.class);
        // schema por conta do Flyway próprio do Mohs (ADR-0048) — o
        // spring.sql.init que morava aqui saiu junto com a Phase 2
        app.setDefaultProperties(Map.of(
                "spring.application.name", "mohs",
                "mohs.jdbc.dialect", "h2"));
        app.run(args);
    }

}
