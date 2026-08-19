package io.mohs.autoconfigure;

import java.time.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.web.servlet.DispatcherServlet;

import tools.jackson.databind.ObjectMapper;

import io.mohs.core.Mohs;
import io.mohs.rest.ActorResolver;
import io.mohs.rest.HeaderActorResolver;
import io.mohs.rest.error.RestExceptionHandler;
import io.mohs.rest.execution.ExecutionsController;
import io.mohs.rest.job.JobsController;
import io.mohs.rest.node.NodesController;
import io.mohs.rest.overview.OverviewController;
import io.mohs.rest.overview.OverviewStreamBroadcaster;
import io.mohs.rest.ratelimit.RateLimitsController;

/**
 * Liga o contrato REST v1 ({@code io.mohs.rest}) à {@link Mohs} pública —
 * fechada por padrão (ADR-0010, princípio 5): {@code mohs.api.enabled=false}
 * não registra nenhum bean deste pacote. {@link ConditionalOnClass} em
 * {@link DispatcherServlet} evita carregar esta configuração quando o
 * consumidor não trouxe {@code spring-boot-starter-webmvc} (dependência
 * {@code optional} do módulo, mesmo padrão do actuator).
 *
 * <p>Também condicionada ao gate mestre {@code mohs.enabled}: kill switch
 * vence em silêncio — o Javadoc do gate promete "desligar remove todos os
 * beans do Mohs", e falha de boot aqui transformaria o botão de emergência
 * em crash quando {@code mohs.api.enabled=true} já estiver no ambiente.
 * Sem essa condição, a combinação caía num {@code NoSuchBeanDefinitionException}
 * genérico ao criar {@link JobsController} (não há bean {@link Mohs}).
 * A proteção cobre só o gate por propriedade: host que exclui
 * {@link MohsAutoConfiguration} na mão ({@code spring.autoconfigure.exclude})
 * com a API ligada mantém o erro de boot — exclusão manual da auto-config
 * da própria biblioteca é cenário não suportado, de propósito (a
 * alternativa, {@code @ConditionalOnBean(Mohs.class)}, esconderia também
 * misconfiguração genuína que deveria estourar).
 *
 * <p>Só {@code jobs}/{@code executions}/{@code nodes}/{@code overview}/
 * {@code rate-limits} têm {@code @Bean} aqui — os demais controllers
 * (batches, runners) continuam contrato M2 sem implementação por trás; registrá-los
 * antes do tempo só exporia rotas que respondem 501
 * ({@code RestExceptionHandler} traduz o
 * {@code UnsupportedOperationException} dos stubs), sem ganho nenhum.
 *
 * <p>{@link ActorResolver} é {@link ConditionalOnMissingBean}: a 1.x
 * troca {@link HeaderActorResolver} (atribuição declarativa, não
 * autenticada) por uma implementação de segurança real sem mudar
 * contrato nenhum (ADR-0010, princípio 5).
 */
@AutoConfiguration(after = MohsAutoConfiguration.class)
@ConditionalOnProperty(prefix = "mohs", name = "enabled", matchIfMissing = true)
@ConditionalOnProperty(prefix = "mohs.api", name = "enabled", havingValue = "true")
@ConditionalOnClass(DispatcherServlet.class)
public class MohsRestAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(MohsRestAutoConfiguration.class);

    public MohsRestAutoConfiguration() {
        log.warn("mohs.api.enabled=true: API operacional sem autenticação; não exponha publicamente");
    }

    @Bean
    @ConditionalOnMissingBean(ActorResolver.class)
    public ActorResolver mohsActorResolver() {
        return new HeaderActorResolver();
    }

    @Bean
    public RestExceptionHandler mohsRestExceptionHandler() {
        return new RestExceptionHandler();
    }

    @Bean
    public JobsController mohsJobsController(Mohs mohs, ActorResolver mohsActorResolver, ObjectMapper objectMapper, MohsProperties properties) {
        return new JobsController(mohs, mohsActorResolver, objectMapper, properties.api().basePath());
    }

    @Bean
    public ExecutionsController mohsExecutionsController(Mohs mohs) {
        return new ExecutionsController(mohs);
    }

    @Bean
    public NodesController mohsNodesController(Mohs mohs) {
        return new NodesController(mohs);
    }

    @Bean
    public RateLimitsController mohsRateLimitsController(Mohs mohs, ActorResolver mohsActorResolver) {
        return new RateLimitsController(mohs, mohsActorResolver);
    }

    /** {@code AutoCloseable}: o container chama {@code close()} no shutdown — timer parado, streams SSE completados (fim de stream limpo, não conexão morta). */
    @Bean
    public OverviewStreamBroadcaster mohsOverviewStreamBroadcaster(Mohs mohs, @Qualifier("mohsClock") Clock mohsClock) {
        return OverviewStreamBroadcaster.start(mohs, mohsClock);
    }

    @Bean
    public OverviewController mohsOverviewController(Mohs mohs, OverviewStreamBroadcaster mohsOverviewStreamBroadcaster) {
        return new OverviewController(mohs, mohsOverviewStreamBroadcaster);
    }
}
