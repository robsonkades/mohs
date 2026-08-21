package io.mohs.autoconfigure;

import java.io.IOException;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnResource;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.resource.PathResourceResolver;
import org.springframework.web.servlet.resource.ResourceHttpRequestHandler;

/**
 * Serve o dashboard do {@code mohs-ui}, quando ele está no classpath.
 *
 * <p>A condição é o próprio bundle ({@code @ConditionalOnResource} sobre o {@code index.html}),
 * não uma classe marcadora: {@code mohs-ui} é um jar só de recurso, e o starter não depende dele
 * — quem quer o dashboard declara {@code io.mohs:mohs-ui} e ele aparece. Sem o jar, nada aqui
 * liga, e o aplicativo não paga nem a avaliação de um bean.
 *
 * <p>Roda no servidor do próprio aplicativo hospedeiro, deliberadamente como único modo: um
 * servidor nosso ficaria inteiramente fora da cadeia de filtros do Spring Security dele, e um
 * aplicativo que se protegeu com cuidado ainda assim exporia pause/cancel/retry numa porta
 * lateral. No servidor do host, a configuração de segurança do host se aplica — proteja lá o
 * prefixo de {@code mohs.api.base-path} e o {@code /mohs-ui}.
 */
@AutoConfiguration
@ConditionalOnClass(DispatcherServlet.class)
@ConditionalOnProperty(prefix = "mohs", name = "enabled", matchIfMissing = true)
@ConditionalOnResource(resources = "classpath:/mohs-ui-webapp/index.html")
public class MohsUiAutoConfiguration {

    /** Onde o dashboard é montado. Casa com o {@code base} do Vite e com o basepath do router. */
    static final String UI_PATH = "/mohs-ui";

    private static final String WEBAPP_LOCATION = "classpath:/mohs-ui-webapp/";

    private static final String INDEX = "/mohs-ui-webapp/index.html";

    private static final String FORWARD_TO_INDEX_VIEW = "forward:" + UI_PATH + "/index.html";

    /**
     * Localização própria no classpath, e não um dos diretórios estáticos default do Boot (que
     * mapeiam para {@code /}): assim o dashboard nunca colide com o que o hospedeiro já serve na
     * raiz — um actuator com {@code base-path: /}, por exemplo.
     *
     * <p>O caminho pelado do mount ({@code /mohs-ui} e {@code /mohs-ui/}) precisa do forward
     * explícito abaixo, à parte do fallback de {@link SpaFallbackResourceResolver}:
     * {@link ResourceHttpRequestHandler} devolve 404 para caminho de recurso vazio antes de
     * qualquer {@link PathResourceResolver} rodar, então o fallback do resolver nunca chega a se
     * aplicar a ele.
     */
    @Bean
    WebMvcConfigurer mohsUiStaticAppConfigurer() {
        return new WebMvcConfigurer() {
            @Override
            public void addViewControllers(ViewControllerRegistry registry) {
                registry.addViewController(UI_PATH).setViewName(FORWARD_TO_INDEX_VIEW);
                registry.addViewController(UI_PATH + "/").setViewName(FORWARD_TO_INDEX_VIEW);
            }

            /**
             * {@code resourceChain(false)}: sem o {@code CachingResourceResolver}. Ele é um
             * {@code ConcurrentMapCache} sem TTL nem teto, com o path da requisição como chave —
             * e o fallback abaixo faz TODO caminho sob {@code /mohs-ui/**} resolver com sucesso,
             * inclusive os inexistentes. Sem 404, some a válvula que normalmente impede o cache
             * de crescer: um crawler batendo em paths aleatórios criaria uma entrada permanente
             * por path, pela vida do processo. E não se perde nada em troca — os recursos são
             * imutáveis dentro do jar, e resolvê-los pelo classloader já é barato.
             */
            @Override
            public void addResourceHandlers(ResourceHandlerRegistry registry) {
                registry.addResourceHandler(UI_PATH + "/**")
                        .addResourceLocations(WEBAPP_LOCATION)
                        .resourceChain(false)
                        .addResolver(new SpaFallbackResourceResolver());
            }
        };
    }

    /**
     * Sub-caminho que não é um asset real cai no {@code index.html}, para que um refresh em rota
     * de cliente ({@code /mohs-ui/jobs}) resolva em vez de dar 404 — o router montado naquele
     * mesmo basepath então renderiza.
     */
    private static final class SpaFallbackResourceResolver extends PathResourceResolver {

        /**
         * {@code checkResource} vem do supertipo e é o que garante que o recurso resolvido ainda
         * está DENTRO da location — a defesa contra {@code ../} e escape por symlink. Sobrescrever
         * {@code getResource} sem reintroduzi-la deixaria só o {@code isInvalidPath} do
         * {@code ResourceHttpRequestHandler} de pé; sobrescrever não é reimplementar do zero.
         *
         * <p>Recurso que não passa cai no {@code index.html} junto com o que simplesmente não
         * existe: para quem chama, "não é asset desta aplicação" é a mesma resposta.
         */
        @Override
        protected Resource getResource(String resourcePath, Resource location) throws IOException {
            Resource requested = location.createRelative(resourcePath);
            boolean servable = requested.exists() && requested.isReadable() && checkResource(requested, location);
            return servable ? requested : new ClassPathResource(INDEX);
        }
    }

}
