package io.mohs.autoconfigure;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

/**
 * Liga o contrato REST v1 ({@code io.mohs.rest}) à {@link Mohs} pública —
 * fechada por padrão (ADR-0010, princípio 5): {@code mohs.api.enabled=false}
 * não registra nenhum bean deste pacote. {@link ConditionalOnClass} em
 * {@link DispatcherServlet} evita carregar esta configuração quando o
 * consumidor não trouxe {@code spring-boot-starter-webmvc} (dependência
 * {@code optional} do módulo, mesmo padrão do actuator).
 *
 * <p>Só {@code jobs}/{@code executions} têm {@code @Bean} aqui — os
 * demais controllers (overview, batches, rate-limits, runners, nodes)
 * continuam contrato M2 sem implementação por trás; registrá-los antes
 * do tempo só exporia rotas que respondem 501 ({@code RestExceptionHandler}
 * traduz o {@code UnsupportedOperationException} dos stubs), sem ganho
 * nenhum.
 *
 * <p>{@link ActorResolver} é {@link ConditionalOnMissingBean}: a 1.x
 * troca {@link HeaderActorResolver} (atribuição declarativa, não
 * autenticada) por uma implementação de segurança real sem mudar
 * contrato nenhum (ADR-0010, princípio 5).
 */
@AutoConfiguration(after = MohsAutoConfiguration.class)
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
}
