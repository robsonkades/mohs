package io.mohs.autoconfigure;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * O gate desta auto-config é o próprio bundle no classpath, não uma classe: {@code mohs-ui} é um
 * jar só de recurso. Este módulo NÃO depende de {@code mohs-ui}, então em teste o
 * {@code classpath:/mohs-ui-webapp/index.html} nunca existe — e é exatamente o cenário do
 * consumidor que não quer dashboard, que é o que precisa ser provado aqui: nada liga, e o boot
 * não quebra por isso.
 */
class MohsUiAutoConfigurationTest {

    private final WebApplicationContextRunner runner = new WebApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(MohsUiAutoConfiguration.class));

    @Test
    void dashboardIsAbsentWhenTheBundleIsNotOnTheClasspath() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean("mohsUiStaticAppConfigurer");
        });
    }

    /**
     * Mesmo com o bundle presente, um aplicativo sem stack web não pode quebrar: a auto-config
     * referencia {@link WebMvcConfigurer} e {@code PathResourceResolver} no corpo, e é o
     * {@code @ConditionalOnClass(DispatcherServlet.class)} que impede o Spring de sequer carregar
     * a classe. Sem esse gate seria {@code NoClassDefFoundError} no boot, não um bean a menos.
     */
    @Test
    void dashboardStaysSilentWithoutDispatcherServletInsteadOfFailingTheBoot() {
        runner.withClassLoader(new FilteredClassLoader(DispatcherServlet.class)).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(WebMvcConfigurer.class);
        });
    }

    /** Kill switch mestre: {@code mohs.enabled=false} remove todos os beans do Mohs, o dashboard junto. */
    @Test
    void masterGateOffSuppressesTheDashboard() {
        runner.withPropertyValues("mohs.enabled=false").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean("mohsUiStaticAppConfigurer");
        });
    }
}
