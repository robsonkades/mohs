package io.mohs.autoconfigure;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.annotation.AliasFor;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.GenericBeanDefinition;

import io.mohs.core.definition.DefinitionSource;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.MohsJob;
import io.mohs.core.definition.OnDemandJob;
import io.mohs.core.definition.RecurringJob;
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

    static class StereotypedJobs {
        @RecurringJob(id = "auto-sync", every = "PT30S", rateLimit = "sync-api")
        void autoSync() {
        }

        @OnDemandJob(value = "import-file", rateLimit = "smtp")
        void importFile(Map<String, Object> payload) {
        }
    }

    static class TriggerlessRecurringJob {
        @RecurringJob(id = "no-trigger")
        void run() {
        }
    }

    static class DoubleAnnotatedJob {
        @MohsJob(id = "double")
        @OnDemandJob("double")
        void run() {
        }
    }

    static class BlankIdStereotypeJob {
        @OnDemandJob
        void run() {
        }
    }

    static class TypedPayloadRecurringJob {
        @RecurringJob(id = "typed", every = "PT30S")
        void run(String payload) {
        }
    }

    static class MapPayloadRecurringJob {
        @RecurringJob(id = "map-ok", every = "PT30S")
        void run(Map<String, Object> payload) {
        }
    }

    /** Estereótipo composto pelo CONSUMIDOR sobre o nosso — o "de brinde" da ADR-0038: alias transita NightlySync.value → RecurringJob.id → MohsJob.id. */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    @RecurringJob(every = "PT1M")
    @interface NightlySync {
        @AliasFor(annotation = RecurringJob.class, attribute = "id")
        String value() default "";
    }

    static class ComposedStereotypeJob {
        @NightlySync("nightly")
        void run() {
        }
    }

    static class ComposedPlusDirectJob {
        @NightlySync("nightly")
        @OnDemandJob("import-file")
        void run() {
        }
    }

    static class TypedPayloadGeneralFormJob {
        @MohsJob(id = "typed-general", every = "PT30S")
        void run(String payload) {
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
                new MohsProperties.Engine(Duration.ofSeconds(5), 50, 1, Duration.ofSeconds(30), null, Duration.ofSeconds(60), 64, 16),
                new MohsProperties.Lifecycle(MohsProperties.Lifecycle.StartMode.AUTO,
                        new MohsProperties.Lifecycle.Shutdown(Duration.ofSeconds(30))),
                new MohsProperties.Time(MohsProperties.Time.Mode.APPLICATION, Duration.ofSeconds(1), Duration.ofSeconds(30)),
                new MohsProperties.Registration(onConflict),
                new MohsProperties.Api(false, "/api/mohs/v1"),
                Map.of(), Map.of());
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
                .hasMessageContaining("duplicate job id 'duplicate'");
    }

    /** ADR-0038: os estereótipos carregam @MohsJob como meta-anotação — o scanner enxerga através deles, com @AliasFor (value = id) honrado. */
    @Test
    void stereotypesAreScannedThroughTheMergedMetaAnnotation() {
        MohsJobScanner scanner = newScanner(MohsProperties.Registration.OnConflict.OVERRIDE);
        StereotypedJobs bean = new StereotypedJobs();
        registerSingleton("stereotypedJobs", bean);

        scanner.postProcessAfterInitialization(bean, "stereotypedJobs");
        scanner.afterSingletonsInstantiated();

        StoredJob recurring = jobStore.find(JobKey.of("auto-sync")).orElseThrow();
        assertThat(recurring.definition().schedule()).isEqualTo(new IntervalSpec(Duration.ofSeconds(30), false));
        assertThat(recurring.definition().source()).isEqualTo(DefinitionSource.ANNOTATION);
        StoredJob onDemand = jobStore.find(JobKey.of("import-file")).orElseThrow(); // id veio do value() conciso
        assertThat(onDemand.definition().schedule()).isEqualTo(new OnDemandSpec());
        // ADR-0042: o limite de vazão atravessa os DOIS estereótipos — sem o
        // alias, quem declara pelo estereótipo (a forma recomendada da
        // ADR-0038) não teria como pedir rate limit nenhum.
        assertThat(recurring.definition().rateLimit()).isEqualTo("sync-api");
        assertThat(onDemand.definition().rateLimit()).isEqualTo("smtp");
        assertThat(handlerRegistry.find(JobKey.of("auto-sync"))).isPresent();
        assertThat(handlerRegistry.find(JobKey.of("import-file"))).isPresent();
    }

    /** @RecurringJob sem gatilho não vira on-demand em silêncio — a meta-anotação não expressa "pelo menos um", o scanner expressa. */
    @Test
    void recurringStereotypeWithoutTriggerFailsBoot() {
        MohsJobScanner scanner = newScanner(MohsProperties.Registration.OnConflict.OVERRIDE);
        TriggerlessRecurringJob bean = new TriggerlessRecurringJob();
        registerSingleton("bean", bean);

        assertThatThrownBy(() -> scanner.postProcessAfterInitialization(bean, "bean"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("declares no trigger");
    }

    @Test
    void multipleJobAnnotationFormsOnOneMethodFailBoot() {
        MohsJobScanner scanner = newScanner(MohsProperties.Registration.OnConflict.OVERRIDE);
        DoubleAnnotatedJob bean = new DoubleAnnotatedJob();
        registerSingleton("bean", bean);

        assertThatThrownBy(() -> scanner.postProcessAfterInitialization(bean, "bean"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("more than one job annotation");
    }

    /** id dos estereótipos é obrigatório em boot (o alias value/id exige default no atributo — a obrigatoriedade de compilação ficou na forma geral). */
    @Test
    void blankStereotypeIdFailsBoot() {
        MohsJobScanner scanner = newScanner(MohsProperties.Registration.OnConflict.OVERRIDE);
        BlankIdStereotypeJob bean = new BlankIdStereotypeJob();
        registerSingleton("bean", bean);

        assertThatThrownBy(() -> scanner.postProcessAfterInitialization(bean, "bean"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("blank id");
    }

    /** §5.13 (ADR-0038): agenda recorrente + handler exigindo payload tipado falharia TODA ocorrência em runtime — falha o boot com erro que ensina. */
    @Test
    void recurringJobWhoseHandlerDemandsTypedPayloadFailsBoot() {
        MohsJobScanner scanner = newScanner(MohsProperties.Registration.OnConflict.OVERRIDE);
        TypedPayloadRecurringJob bean = new TypedPayloadRecurringJob();
        registerSingleton("bean", bean);

        assertThatThrownBy(() -> scanner.postProcessAfterInitialization(bean, "bean"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("automatic occurrences carry no payload");
    }

    /**
     * Review ADR-0038 (verificação empírica): composto + direto no mesmo
     * método resolveria pela ORDEM DE DECLARAÇÃO no fonte, não pela forma
     * direta — colisão de identidade nunca resolve por ordem; falha sempre.
     */
    @Test
    void composedStereotypePlusDirectFormFailsBoot() {
        MohsJobScanner scanner = newScanner(MohsProperties.Registration.OnConflict.OVERRIDE);
        ComposedPlusDirectJob bean = new ComposedPlusDirectJob();
        registerSingleton("bean", bean);

        assertThatThrownBy(() -> scanner.postProcessAfterInitialization(bean, "bean"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("more than one job annotation");
    }

    // Deliberadamente SEM fixture pra value≠id no par de aliases: bytecode com mirror
    // inválido sob io.mohs.** envenena o component scan de TODO teste de contexto
    // (@WebMvcTest lê os metadados de cada classe do classpath de teste e a validação
    // do Spring explode na leitura). O comportamento é contrato do Spring, verificado
    // empiricamente no review da ADR-0038: AnnotationConfigurationException nomeando
    // anotação, atributos e método — registrado na ADR, não re-testado aqui.

    /** A promessa da ADR-0038: o §5.13 decide pela DEFINIÇÃO, então cobre a forma geral com gatilho igualmente. */
    @Test
    void recurringGeneralFormWithTypedPayloadAlsoFailsBoot() {
        MohsJobScanner scanner = newScanner(MohsProperties.Registration.OnConflict.OVERRIDE);
        TypedPayloadGeneralFormJob bean = new TypedPayloadGeneralFormJob();
        registerSingleton("bean", bean);

        assertThatThrownBy(() -> scanner.postProcessAfterInitialization(bean, "bean"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("automatic occurrences carry no payload");
    }

    @Test
    void consumerComposedStereotypeIsResolvedThroughTheMetaChain() {
        MohsJobScanner scanner = newScanner(MohsProperties.Registration.OnConflict.OVERRIDE);
        ComposedStereotypeJob bean = new ComposedStereotypeJob();
        registerSingleton("bean", bean);

        scanner.postProcessAfterInitialization(bean, "bean");
        scanner.afterSingletonsInstantiated();

        StoredJob stored = jobStore.find(JobKey.of("nightly")).orElseThrow();
        assertThat(stored.definition().schedule()).isEqualTo(new IntervalSpec(Duration.ofMinutes(1), false));
    }

    /** Map passa de propósito: disparo automático entrega mapa vazio; invocação manual avulsa do mesmo job pode entregar dados. */
    @Test
    void recurringHandlerAcceptingAMapIsAllowed() {
        MohsJobScanner scanner = newScanner(MohsProperties.Registration.OnConflict.OVERRIDE);
        MapPayloadRecurringJob bean = new MapPayloadRecurringJob();
        registerSingleton("bean", bean);

        scanner.postProcessAfterInitialization(bean, "bean");
        scanner.afterSingletonsInstantiated();

        assertThat(jobStore.find(JobKey.of("map-ok"))).isPresent();
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
