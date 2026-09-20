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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.HashSet;
import java.util.Set;

import org.jspecify.annotations.Nullable;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.MergedAnnotations;
import org.springframework.util.ReflectionUtils;
import org.springframework.scheduling.annotation.Async;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.MohsJob;
import io.mohs.core.definition.RecurringJob;
import io.mohs.core.event.OnExecution;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.engine.HandlerRegistry;
import io.mohs.engine.JobHandler;
import io.mohs.engine.JobStore;
import io.mohs.engine.JobBinding;
import io.mohs.engine.JobDefinitions;
import io.mohs.core.definition.JobRegistration;
import io.mohs.core.schedule.Misfire;

/**
 * Scans singleton beans for {@link MohsJob} methods, translating them into
 * {@link JobDefinition}/{@link JobHandler} pairs, and for {@link OnExecution} methods, handing
 * them to the {@link OnExecutionRegistry}.
 *
 * <p>It uses the same two-phase pattern as Spring's {@code ScheduledAnnotationBeanPostProcessor}
 * for {@code @Scheduled}: accumulate while each bean initialises ({@link BeanPostProcessor}), and
 * only commit once every singleton exists ({@link SmartInitializingSingleton}). Without that, bean
 * creation order would arbitrarily decide which {@code @MohsJob} "wins" an id conflict.
 *
 * <p>It runs before the {@code Engine} starts — no claim may happen before every annotated
 * definition is registered — because {@code afterSingletonsInstantiated} happens during
 * {@code finishBeanFactoryInitialization}, always ahead of {@code finishRefresh}, where
 * {@code SmartLifecycle.start()} fires ({@link MohsEngineLifecycle}).
 *
 * <p>Dependencies arrive through {@link ObjectProvider} rather than direct injection: a
 * {@link BeanPostProcessor} with an ordinary constructor dependency forces Spring to create that
 * dependency too early, before all the other {@code BeanPostProcessor}s are registered — Spring
 * itself warns about this at runtime ("not eligible for getting processed by all
 * BeanPostProcessors"). {@code ObjectProvider} defers resolution to
 * {@link #afterSingletonsInstantiated}, where the problem no longer exists.
 */
final class MohsJobScanner implements BeanPostProcessor, BeanFactoryAware, SmartInitializingSingleton {

    private final ObjectProvider<HandlerRegistry> handlerRegistry;
    private final ObjectProvider<JobStore> jobStore;
    private final ObjectProvider<io.mohs.engine.CatalogStore> catalogStore;
    private final ObjectProvider<MohsProperties> properties;
    private final ObjectProvider<OnExecutionRegistry> onExecutionRegistry;

    /**
     * Guarded by {@code synchronized} for the same reason
     * {@code ScheduledAnnotationBeanPostProcessor} guards its own collections: with Spring
     * Framework 6.2+ background bootstrap ({@code bootstrapExecutor}),
     * {@link #postProcessAfterInitialization} can run on concurrent threads in the host
     * application, and an embedded library does not control that.
     */
    private final Map<JobKey, ScannedJob> scanned = new LinkedHashMap<>();
    private final Map<JobKey, NamedRegistration> registrations = new LinkedHashMap<>();

    /** {@code @OnExecution} methods, accumulated by the same two-phase rule and under the same lock. */
    private final List<ScannedObserver> observers = new ArrayList<>();
    private final Set<String> processedBeans = new HashSet<>();
    private boolean published;

    private @Nullable ConfigurableListableBeanFactory beanFactory;

    MohsJobScanner(ObjectProvider<HandlerRegistry> handlerRegistry, ObjectProvider<JobStore> jobStore,
            ObjectProvider<io.mohs.engine.CatalogStore> catalogStore,
            ObjectProvider<MohsProperties> properties, ObjectProvider<OnExecutionRegistry> onExecutionRegistry) {
        this.handlerRegistry = Objects.requireNonNull(handlerRegistry, "handlerRegistry");
        this.jobStore = Objects.requireNonNull(jobStore, "jobStore");
        this.catalogStore = Objects.requireNonNull(catalogStore, "catalogStore");
        this.properties = Objects.requireNonNull(properties, "properties");
        this.onExecutionRegistry = Objects.requireNonNull(onExecutionRegistry, "onExecutionRegistry");
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        if (beanFactory instanceof ConfigurableListableBeanFactory clbf) {
            this.beanFactory = clbf;
        }
    }

    /**
     * Skips non-singleton beans — the same guard as
     * {@code ScheduledAnnotationBeanPostProcessor}: without it, a {@code prototype} bean would
     * reprocess the same method on every creation.
     *
     * <p>{@code containsBean} comes before {@code isSingleton} because not every object passing
     * through here is a context bean: Spring initialises a {@code View} by the VIEW NAME
     * ({@code UrlBasedViewResolver.applyLifecycleMethods} then {@code initializeBean(view,
     * viewName)}), so a {@code setViewName("forward:/x")} arrives here as {@code "forward:"}.
     * Without the guard, {@code isSingleton} throws {@code NoSuchBeanDefinitionException} and fails
     * the request — meaning any host with a {@code forward:}/{@code redirect:} view broke merely by
     * having Mohs on the classpath. An unknown name is scanned normally: it is not a reprocessed
     * singleton, it is an object that went through post-processing and simply has no
     * {@code @MohsJob}.
     */
    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if (beanFactory != null && beanFactory.containsBean(beanName) && !beanFactory.isSingleton(beanName)) {
            return bean;
        }
        synchronized (scanned) {
            if (!published) {
                processedBeans.add(bean instanceof org.springframework.beans.factory.FactoryBean<?>
                        ? BeanFactory.FACTORY_BEAN_PREFIX + beanName : beanName);
            }
        }
        if (bean instanceof JobRegistration<?> registration) {
            synchronized (scanned) {
                requireUnpublished(beanName);
                JobKey key = registration.job().key();
                NamedRegistration previous = registrations.get(key);
                if (previous != null || scanned.containsKey(key)) {
                    String other = previous != null ? previous.beanName() : scanned.get(key).declaringMethod();
                    throw new IllegalStateException("duplicate job id '" + key.value() + "' — "
                            + beanName + " and " + other + " both declare it");
                }
                registrations.put(key, new NamedRegistration(registration, beanName));
            }
            return bean;
        }
        Class<?> targetClass = AopUtils.getTargetClass(bean);
        ReflectionUtils.doWithMethods(targetClass, method -> {
            scanObserver(bean, method);
            scanJob(bean, targetClass, method);
        });
        return bean;
    }

    private void scanObserver(Object bean, Method targetMethod) {
        OnExecution observer = targetMethod.getAnnotation(OnExecution.class);
        if (observer == null) {
            return;
        }
        String declaring = describe(targetMethod);
        // The signature and the filter are checked HERE rather than at delivery: both failures
        // are silent at runtime — a method that never fires looks exactly like one whose event
        // never happened
        OnExecutionRegistry.validate(observer, targetMethod, declaring);
        synchronized (scanned) {
            requireUnpublished(declaring);
            observers.add(new ScannedObserver(observer, bean,
                    AopUtils.selectInvocableMethod(targetMethod, bean.getClass()), declaring));
        }
    }

    private void scanJob(Object bean, Class<?> targetClass, Method targetMethod) {
        // Merged, not a raw getAnnotation: the stereotypes (@RecurringJob/@OnDemandJob, and consumer
        // compositions over them) carry @MohsJob as a meta-annotation with @AliasFor, which only
        // merged-annotation resolution honours.
        MohsJob annotation = AnnotatedElementUtils.findMergedAnnotation(targetMethod, MohsJob.class);
        if (annotation == null) {
            return;
        }
        String declaringMethod = describe(targetMethod);
        if (AnnotatedElementUtils.hasAnnotation(targetMethod, Async.class)
                || AnnotatedElementUtils.hasAnnotation(targetClass, Async.class)) {
            throw new IllegalStateException("@MohsJob method " + declaringMethod
                    + " must complete synchronously; @Async on the method or class is not supported");
        }
        requireSingleJobForm(targetMethod, declaringMethod);
        if (annotation.id().isBlank()) {
            // Only reachable through the stereotypes (the general form's id is mandatory at compile time)
            throw new IllegalStateException("job annotation on " + declaringMethod
                    + " has a blank id — set value/id (e.g. @OnDemandJob(\"my-job\"))");
        }
        // getTargetClass, not bean.getClass(): a CGLIB proxy must not become the persisted
        // handlerType (a synthetic name does not resolve stably across restarts).
        Method invocable = AopUtils.selectInvocableMethod(targetMethod, bean.getClass());
        JobKey key = JobKey.of(annotation.id());

        synchronized (scanned) {
            requireUnpublished(declaringMethod);
            if (registrations.containsKey(key)) {
                throw new IllegalStateException("duplicate job id '" + key.value() + "' — "
                        + declaringMethod + " and " + registrations.get(key).beanName() + " both declare it");
            }
            ScannedJob already = scanned.get(key);
            if (already != null) {
                throw new IllegalStateException("duplicate job id '" + annotation.id() + "' — "
                        + already.declaringMethod() + " and " + declaringMethod + " both declare it");
            }

            JobDefinition definition = MohsJobs.toDefinition(key, annotation);
            if (definition.schedule() instanceof OnDemandSpec
                    && AnnotatedElementUtils.hasAnnotation(targetMethod, RecurringJob.class)) {
                throw new IllegalStateException("@RecurringJob(id=\"" + annotation.id() + "\") on " + declaringMethod
                        + " declares no trigger — set cron/every/everyAfterFinish, or use @OnDemandJob for a job without a schedule");
            }
            MohsJobs.AdaptedHandler handler = MohsJobs.adaptHandler(bean, invocable);
            MohsJobs.requireRecurringHandlerAcceptsAutomaticPayload(definition, handler, declaringMethod);
            scanned.put(key, new ScannedJob(definition, handler, declaringMethod));
        }
    }

    /**
     * One method is exactly one job — a clash of forms belongs to the same family as a duplicate
     * id: it always fails, unconditionally.
     *
     * <p>It counts appearances of {@code @MohsJob} across the merged-annotation graph (each direct
     * form — general, stereotype, or a consumer's composition — contributes exactly one), because
     * counting only the three direct forms let "composed + direct" through, to be resolved silently
     * by DECLARATION ORDER in the source (verified Spring behaviour — the direct form is not the
     * one that wins), which is exactly the arbitrariness this scanner exists to prevent.
     */
    private static void requireSingleJobForm(Method method, String declaringMethod) {
        long declaredForms = MergedAnnotations.from(method).stream(MohsJob.class).count();
        if (declaredForms > 1) {
            throw new IllegalStateException(declaringMethod
                    + " declares more than one job annotation (@MohsJob/@RecurringJob/@OnDemandJob, "
                    + "directly or through a composed stereotype) — a method is exactly one job");
        }
    }

    private static String describe(Method method) {
        return method.getDeclaringClass().getName() + "#" + method.getName();
    }

    @Override
    public void afterSingletonsInstantiated() {
        initializeLazyDeclarations();
        JobStore store = jobStore.getObject();
        HandlerRegistry registry = handlerRegistry.getObject();
        MohsProperties.Registration.OnConflict onConflict = properties.getObject().registration().onConflict();

        OnExecutionRegistry observerRegistry = onExecutionRegistry.getObject();

        synchronized (scanned) {
            List<JobDefinition> definitions = new ArrayList<>();
            for (ScannedJob job : scanned.values()) {
                registry.validatePolicy(job.definition());
                definitions.add(job.definition());
                registry.register(job.definition().key(), job.handler().handler(),
                        job.handler().payloadType() == null ? io.mohs.core.NoInput.class : job.handler().payloadType());
            }
            for (NamedRegistration named : registrations.values()) {
                var registration = named.registration();
                var definition = JobDefinitions.definition(registration.job().key(), registration.options(),
                        new OnDemandSpec(), Misfire.IGNORE, false).withInputContract(registration.job().inputContract());
                registry.validatePolicy(definition);
                definitions.add(definition);
                registerAction(registry, registration);
            }
            catalogStore.getObject().reconcile(registry.freeze(), definitions,
                    io.mohs.engine.CatalogStore.Conflict.valueOf(onConflict.name()));
            for (ScannedObserver observer : observers) {
                observerRegistry.register(observer.annotation(), observer.bean(), observer.method(), observer.declaringMethod());
            }
            validateBindings(store, registry);
            published = true;
            processedBeans.clear();
        }
    }

    /** Materialize only singleton job/observer declarations, leaving unrelated lazy beans lazy. */
    private void initializeLazyDeclarations() {
        if (beanFactory == null) {
            return;
        }
        for (String name : beanFactory.getBeanDefinitionNames()) {
            synchronized (scanned) {
                if (processedBeans.contains(name)) {
                    continue;
                }
            }
            if (!beanFactory.getBeanDefinition(name).isSingleton()) {
                continue;
            }
            // Do not instantiate unrelated FactoryBeans just to discover their product type.
            Class<?> type = beanFactory.getType(name, false);
            if (type == null) {
                continue;
            }
            boolean declaration = JobRegistration.class.isAssignableFrom(type);
            if (!declaration) {
                for (Method method : ReflectionUtils.getAllDeclaredMethods(type)) {
                    if (AnnotatedElementUtils.hasAnnotation(method, MohsJob.class)
                            || method.isAnnotationPresent(OnExecution.class)) {
                        declaration = true;
                        break;
                    }
                }
            }
            if (declaration && beanFactory.isSingleton(name)) {
                beanFactory.getBean(name);
            }
        }
    }

    private void requireUnpublished(String declaration) {
        if (published) {
            throw new IllegalStateException("Mohs declaration " + declaration
                    + " was initialized after the executable catalog was frozen; expose its concrete bean type"
                    + " and declare it before application startup");
        }
    }

    @SuppressWarnings("unchecked") // The boxed class represents the same generic input contract.
    private static <T> void registerAction(HandlerRegistry registry, JobRegistration<T> registration) {
        Class<?> type = org.springframework.util.ClassUtils.resolvePrimitiveIfNecessary(registration.job().payloadType());
        registry.register(registration.job().key(),
                (input, context) -> registration.action().execute((T) type.cast(input), context), type, registration.job().inputContract());
    }

    private void validateBindings(JobStore store, HandlerRegistry registry) {
        // Validate all durable policies with one read instead of one query per recurrence.
        try (var jobs = store.findAll()) {
            jobs.forEach(job -> {
                registry.validatePolicy(job.definition());
                JobKey executable = job.definition().executableKey();
                if (registry.find(executable).isEmpty()) {
                    throw new IllegalStateException("job '" + job.definition().key().value()
                            + "' requires executable '" + executable.value() + "' on every worker");
                }
                try {
                    registry.requireContract(executable, job.definition().inputContract());
                } catch (IllegalArgumentException mismatch) {
                    throw new IllegalStateException("local catalog disagrees with the persisted input contract", mismatch);
                }
            });
        }
        for (JobBinding binding : store.bindings()) {
            if (registry.find(binding.executableKey()).isEmpty()) {
                throw new IllegalStateException("job '" + binding.key().value() + "' requires executable '"
                        + binding.executableKey().value() + "'; declare its JobRegistration bean or @MohsJob handler on every worker");
            }
            if (binding.input() != null) {
                Class<?> expected = registry.payloadType(binding.executableKey()).orElseThrow(() ->
                        new IllegalStateException("executable '" + binding.executableKey().value() + "' has no input contract"));
                Object input = registry.decode(binding.executableKey(), binding.input());
                if (!org.springframework.util.ClassUtils.resolvePrimitiveIfNecessary(expected).isInstance(input)) {
                    throw new IllegalStateException("recurrence '" + binding.key().value() + "' expects "
                            + expected.getName() + "; stored input has type " + input.getClass().getName());
                }
            }
        }
    }

    private record NamedRegistration(JobRegistration<?> registration, String beanName) {}

    private record ScannedJob(JobDefinition definition, MohsJobs.AdaptedHandler handler, String declaringMethod) {
    }

    private record ScannedObserver(OnExecution annotation, Object bean, Method method, String declaringMethod) {
    }
}
