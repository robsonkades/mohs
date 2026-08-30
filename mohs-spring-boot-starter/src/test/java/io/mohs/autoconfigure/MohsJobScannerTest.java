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

import static org.assertj.core.api.Assertions.assertThatCode;
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

    /** A stereotype composed by the CONSUMER on top of ours: the alias travels NightlySync.value to RecurringJob.id to MohsJob.id. */
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
    private OnExecutionRegistry onExecutionRegistry;

    @BeforeEach
    void setUp() {
        beanFactory = new DefaultListableBeanFactory();
        jobStore = new InMemoryJobStore();
        handlerRegistry = new HandlerRegistry();
        onExecutionRegistry = new OnExecutionRegistry();
    }

    private MohsJobScanner newScanner(MohsProperties.Registration.OnConflict onConflict) {
        // The record built directly, with the same defaults as the binding — the scanner only reads registration()
        MohsProperties properties = new MohsProperties(
                true,
                new MohsProperties.Jdbc(null, true),
                new MohsProperties.Engine(Duration.ofSeconds(5), Duration.ofSeconds(5), 50, 1, Duration.ofSeconds(30), Duration.ofSeconds(15), null, Duration.ofSeconds(60), Duration.ofDays(7), 64, 16, false),
                new MohsProperties.Lifecycle(MohsProperties.Lifecycle.StartMode.AUTO,
                        new MohsProperties.Lifecycle.Shutdown(Duration.ofSeconds(30))),
                new MohsProperties.Time(MohsProperties.Time.Mode.APPLICATION, Duration.ofSeconds(1), Duration.ofSeconds(30)),
                new MohsProperties.Registration(onConflict),
                new MohsProperties.Api(false, "/api/mohs/v1"),
                Map.of(), Map.of());
        MohsJobScanner scanner = new MohsJobScanner(providerOf(handlerRegistry), providerOf(jobStore),
                providerOf(properties), providerOf(onExecutionRegistry));
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

    /** ANNOTATION-sourced, the same key as WelcomeEmailJob with a different trigger — it simulates definitional drift for the on-conflict and orphan tests. */
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

    /** The stereotypes carry @MohsJob as a meta-annotation, so the scanner sees through them, with @AliasFor (value = id) honoured. */
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
        // The rate limit travels through BOTH stereotypes — without the alias, anyone declaring a
        // job through a stereotype (the recommended form) would have no way to ask for a rate
        // limit at all.
        assertThat(recurring.definition().rateLimit()).isEqualTo("sync-api");
        assertThat(onDemand.definition().rateLimit()).isEqualTo("smtp");
        assertThat(handlerRegistry.find(JobKey.of("auto-sync"))).isPresent();
        assertThat(handlerRegistry.find(JobKey.of("import-file"))).isPresent();
    }

    /** @RecurringJob without a trigger does not silently become on-demand — the meta-annotation cannot express "at least one", so the scanner does. */
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

    /** The stereotypes' id is mandatory at boot (the value/id alias requires a default on the attribute — compile-time mandatoriness stayed with the general form). */
    @Test
    void blankStereotypeIdFailsBoot() {
        MohsJobScanner scanner = newScanner(MohsProperties.Registration.OnConflict.OVERRIDE);
        BlankIdStereotypeJob bean = new BlankIdStereotypeJob();
        registerSingleton("bean", bean);

        assertThatThrownBy(() -> scanner.postProcessAfterInitialization(bean, "bean"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("blank id");
    }

    /** A recurring schedule plus a handler demanding a typed payload would fail EVERY occurrence at runtime — the boot fails with an error that teaches. */
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
     * Verified empirically: a composed and a direct annotation on the same method would resolve by
     * DECLARATION ORDER in the source rather than by the direct form. An identity collision never
     * resolves by order; it always fails.
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

    // Deliberately NO fixture for value != id in the alias pair: bytecode with an invalid mirror
    // under io.mohs.** poisons the component scan of EVERY context test (@WebMvcTest reads the
    // metadata of each class on the test classpath, and Spring's validation blows up while reading).
    // The behaviour is Spring's contract, verified empirically: an AnnotationConfigurationException
    // naming the annotation, the attributes and the method — recorded rather than re-tested here.

    /** The rule decides from the DEFINITION, so it covers the general form with a trigger just the same. */
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

    /** Map passes on purpose: an automatic firing delivers an empty map, while a one-off manual invocation of the same job may carry data. */
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
        seedDivergedStoredDefinition(); // ANNOTATION, but nothing scanned in this round

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

    /**
     * Spring initialises objects that are NOT declared beans, under the name that identifies them
     * in the context where they were created — {@code UrlBasedViewResolver.applyLifecycleMethods}
     * calls {@code initializeBean(view, viewName)}, and for an
     * {@code addViewController(...).setViewName("forward:/something")} that name is
     * {@code "forward:"}. A name with no bean definition makes {@code isSingleton} throw
     * {@link org.springframework.beans.factory.NoSuchBeanDefinitionException} and fails the request
     * with a 500.
     *
     * <p>This is not hypothetical: the mohs-ui dashboard serves its bare mount through a forward,
     * and broke exactly this way. Any host application with a {@code forward:}/{@code redirect:}
     * view broke the same, merely by having Mohs on the classpath.
     */
    @Test
    void aBeanNameWithoutADefinitionIsProcessedInsteadOfBlowingUp() {
        MohsJobScanner scanner = newScanner(MohsProperties.Registration.OnConflict.FAIL);
        Object notABean = new Object();

        assertThatCode(() -> scanner.postProcessAfterInitialization(notABean, "forward:"))
                .doesNotThrowAnyException();
    }
}
