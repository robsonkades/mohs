package io.mohs.autoconfigure;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.GenericBeanDefinition;

import io.mohs.core.definition.DefinitionSource;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.MohsJob;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.engine.HandlerRegistry;
import io.mohs.engine.StoredJob;
import io.mohs.test.InMemoryJobStore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MohsJobScannerTest {

    static class WelcomeEmailJob {
        @MohsJob(id = "welcome-email")
        void send() {
        }
    }

    static class DuplicateIdJobA {
        @MohsJob(id = "duplicate")
        void a() {
        }
    }

    static class DuplicateIdJobB {
        @MohsJob(id = "duplicate")
        void b() {
        }
    }

    private DefaultListableBeanFactory beanFactory;
    private InMemoryJobStore jobStore;
    private HandlerRegistry handlerRegistry;

    @BeforeEach
    void setUp() {
        beanFactory = new DefaultListableBeanFactory();
        jobStore = new InMemoryJobStore();
        handlerRegistry = new HandlerRegistry();
    }

    private MohsJobScanner newScanner(MohsProperties.Registration.OnConflict onConflict) {
        // construção direta do record, com os mesmos defaults do binding — o scanner só lê registration()
        MohsProperties properties = new MohsProperties(
                true,
                new MohsProperties.Jdbc(null),
                new MohsProperties.Engine(Duration.ofSeconds(5), 50, Duration.ofSeconds(30), 64, 16),
                new MohsProperties.Lifecycle(MohsProperties.Lifecycle.StartMode.AUTO,
                        new MohsProperties.Lifecycle.Shutdown(Duration.ofSeconds(30))),
                new MohsProperties.Time(MohsProperties.Time.Mode.APPLICATION, Duration.ofSeconds(1), Duration.ofSeconds(30)),
                new MohsProperties.Registration(onConflict),
                new MohsProperties.Api(false, "/api/mohs/v1"),
                Map.of());
        MohsJobScanner scanner = new MohsJobScanner(providerOf(handlerRegistry), providerOf(jobStore), providerOf(properties));
        scanner.setBeanFactory(beanFactory);
        return scanner;
    }

    private static <T> ObjectProvider<T> providerOf(T instance) {
        return new ObjectProvider<>() {
            @Override
            public T getObject() {
                return instance;
            }
        };
    }

    private void registerSingleton(String beanName, Object bean) {
        GenericBeanDefinition definition = new GenericBeanDefinition();
        definition.setBeanClass(bean.getClass());
        definition.setScope("singleton");
        beanFactory.registerBeanDefinition(beanName, definition);
    }

    private void registerPrototype(String beanName, Object bean) {
        GenericBeanDefinition definition = new GenericBeanDefinition();
        definition.setBeanClass(bean.getClass());
        definition.setScope("prototype");
        beanFactory.registerBeanDefinition(beanName, definition);
    }

    /** ANNOTATION-sourced, mesma key de WelcomeEmailJob, gatilho diferente — simula drift definicional pros testes de on-conflict/órfã. */
    private void seedDivergedStoredDefinition() {
        JobDefinition existing = new JobDefinition(JobKey.of("welcome-email"), null, WelcomeEmailJob.class,
                new IntervalSpec(Duration.ofMinutes(5), false),
                null, null, Misfire.IGNORE, true, 0, 0, null, null, DefinitionSource.ANNOTATION);
        jobStore.upsert(existing);
    }

    @Test
    void scansAndRegistersASimpleJob() {
        MohsJobScanner scanner = newScanner(MohsProperties.Registration.OnConflict.OVERRIDE);
        WelcomeEmailJob bean = new WelcomeEmailJob();
        registerSingleton("welcomeEmailJob", bean);

        scanner.postProcessAfterInitialization(bean, "welcomeEmailJob");
        scanner.afterSingletonsInstantiated();

        StoredJob stored = jobStore.find(JobKey.of("welcome-email")).orElseThrow();
        assertThat(stored.definition().source()).isEqualTo(DefinitionSource.ANNOTATION);
        assertThat(stored.definition().schedule()).isEqualTo(new OnDemandSpec());
        assertThat(handlerRegistry.find(JobKey.of("welcome-email"))).isPresent();
    }

    @Test
    void prototypeBeanIsSkipped() {
        MohsJobScanner scanner = newScanner(MohsProperties.Registration.OnConflict.OVERRIDE);
        WelcomeEmailJob bean = new WelcomeEmailJob();
        registerPrototype("welcomeEmailJob", bean);

        scanner.postProcessAfterInitialization(bean, "welcomeEmailJob");
        scanner.afterSingletonsInstantiated();

        assertThat(jobStore.find(JobKey.of("welcome-email"))).isEmpty();
        assertThat(handlerRegistry.find(JobKey.of("welcome-email"))).isEmpty();
    }

    @Test
    void duplicateAnnotationIdFailsDuringScan() {
        MohsJobScanner scanner = newScanner(MohsProperties.Registration.OnConflict.OVERRIDE);
        DuplicateIdJobA a = new DuplicateIdJobA();
        DuplicateIdJobB b = new DuplicateIdJobB();
        registerSingleton("a", a);
        registerSingleton("b", b);
        scanner.postProcessAfterInitialization(a, "a");

        assertThatThrownBy(() -> scanner.postProcessAfterInitialization(b, "b"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("duplicate @MohsJob id 'duplicate'");
    }

    @Test
    void annotationCollidingWithProgrammaticDefinitionFailsBoot() {
        jobStore.upsert(JobDefinition.of("welcome-email", Object.class, spec -> spec.onDemand()));
        MohsJobScanner scanner = newScanner(MohsProperties.Registration.OnConflict.OVERRIDE);
        WelcomeEmailJob bean = new WelcomeEmailJob();
        registerSingleton("welcomeEmailJob", bean);
        scanner.postProcessAfterInitialization(bean, "welcomeEmailJob");

        assertThatThrownBy(scanner::afterSingletonsInstantiated)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("PROGRAMMATIC");
    }

    @Test
    void onConflictOverrideAppliesTheCodeVersion() {
        seedDivergedStoredDefinition();
        MohsJobScanner scanner = newScanner(MohsProperties.Registration.OnConflict.OVERRIDE);
        WelcomeEmailJob bean = new WelcomeEmailJob();
        registerSingleton("welcomeEmailJob", bean);
        scanner.postProcessAfterInitialization(bean, "welcomeEmailJob");

        scanner.afterSingletonsInstantiated();

        StoredJob stored = jobStore.find(JobKey.of("welcome-email")).orElseThrow();
        assertThat(stored.definition().schedule()).isEqualTo(new OnDemandSpec());
    }

    @Test
    void onConflictPreserveKeepsTheStoredVersionButStillRegistersHandler() {
        seedDivergedStoredDefinition();
        MohsJobScanner scanner = newScanner(MohsProperties.Registration.OnConflict.PRESERVE);
        WelcomeEmailJob bean = new WelcomeEmailJob();
        registerSingleton("welcomeEmailJob", bean);
        scanner.postProcessAfterInitialization(bean, "welcomeEmailJob");

        scanner.afterSingletonsInstantiated();

        StoredJob stored = jobStore.find(JobKey.of("welcome-email")).orElseThrow();
        assertThat(stored.definition().schedule()).isEqualTo(new IntervalSpec(Duration.ofMinutes(5), false));
        assertThat(handlerRegistry.find(JobKey.of("welcome-email"))).isPresent();
    }

    @Test
    void onConflictFailAbortsBoot() {
        seedDivergedStoredDefinition();
        MohsJobScanner scanner = newScanner(MohsProperties.Registration.OnConflict.FAIL);
        WelcomeEmailJob bean = new WelcomeEmailJob();
        registerSingleton("welcomeEmailJob", bean);
        scanner.postProcessAfterInitialization(bean, "welcomeEmailJob");

        assertThatThrownBy(scanner::afterSingletonsInstantiated).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void definitionMissingFromScanIsMarkedOrphaned() {
        seedDivergedStoredDefinition(); // ANNOTATION, mas nada escaneado nesta rodada

        newScanner(MohsProperties.Registration.OnConflict.OVERRIDE).afterSingletonsInstantiated();

        assertThat(jobStore.find(JobKey.of("welcome-email"))).map(StoredJob::orphaned).contains(true);
    }

    @Test
    void reappearingAnnotationClearsOrphaned() {
        seedDivergedStoredDefinition();
        jobStore.markOrphaned(JobKey.of("welcome-email"));
        MohsJobScanner scanner = newScanner(MohsProperties.Registration.OnConflict.OVERRIDE);
        WelcomeEmailJob bean = new WelcomeEmailJob();
        registerSingleton("welcomeEmailJob", bean);
        scanner.postProcessAfterInitialization(bean, "welcomeEmailJob");

        scanner.afterSingletonsInstantiated();

        assertThat(jobStore.find(JobKey.of("welcome-email"))).map(StoredJob::orphaned).contains(false);
    }
}
