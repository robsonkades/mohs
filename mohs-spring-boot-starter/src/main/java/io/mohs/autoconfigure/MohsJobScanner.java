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
import java.util.Optional;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

import io.mohs.core.definition.DefinitionSource;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.MohsJob;
import io.mohs.core.definition.RecurringJob;
import io.mohs.core.event.OnExecution;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.engine.HandlerRegistry;
import io.mohs.engine.JobStore;
import io.mohs.engine.StoredJob;

/**
 * Scans singleton beans for {@link MohsJob} methods and translates them into
 * {@link JobDefinition}/{@link JobHandler} pairs.
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

    private static final Logger log = LoggerFactory.getLogger(MohsJobScanner.class);

    private final ObjectProvider<HandlerRegistry> handlerRegistry;
    private final ObjectProvider<JobStore> jobStore;
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

    /** {@code @OnExecution} methods, accumulated by the same two-phase rule and under the same lock. */
    private final List<ScannedObserver> observers = new ArrayList<>();

    private @Nullable ConfigurableListableBeanFactory beanFactory;

    MohsJobScanner(ObjectProvider<HandlerRegistry> handlerRegistry, ObjectProvider<JobStore> jobStore,
            ObjectProvider<MohsProperties> properties, ObjectProvider<OnExecutionRegistry> onExecutionRegistry) {
        this.handlerRegistry = Objects.requireNonNull(handlerRegistry, "handlerRegistry");
        this.jobStore = Objects.requireNonNull(jobStore, "jobStore");
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
        Class<?> targetClass = AopUtils.getTargetClass(bean);
        ReflectionUtils.doWithMethods(targetClass, method -> scanMethod(bean, targetClass, method));
        return bean;
    }

    private void scanMethod(Object bean, Class<?> targetClass, Method targetMethod) {
        OnExecution observer = targetMethod.getAnnotation(OnExecution.class);
        if (observer != null) {
            String declaring = describe(targetMethod);
            // The signature and the filter are checked HERE rather than at delivery: both failures
            // are silent at runtime — a method that never fires looks exactly like one whose event
            // never happened
            OnExecutionRegistry.validate(observer, targetMethod, declaring);
            synchronized (scanned) {
                observers.add(new ScannedObserver(observer, bean,
                        AopUtils.selectInvocableMethod(targetMethod, bean.getClass()), declaring));
            }
        }
        // Merged, not a raw getAnnotation: the stereotypes (@RecurringJob/@OnDemandJob, and consumer
        // compositions over them) carry @MohsJob as a meta-annotation with @AliasFor, which only
        // merged-annotation resolution honours.
        MohsJob annotation = AnnotatedElementUtils.findMergedAnnotation(targetMethod, MohsJob.class);
        if (annotation == null) {
            return;
        }
        String declaringMethod = describe(targetMethod);
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
            ScannedJob already = scanned.get(key);
            if (already != null) {
                throw new IllegalStateException("duplicate job id '" + annotation.id() + "' — "
                        + already.declaringMethod() + " and " + declaringMethod + " both declare it");
            }

            JobDefinition definition = MohsJobs.toDefinition(key, annotation, targetClass);
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
        JobStore store = jobStore.getObject();
        HandlerRegistry registry = handlerRegistry.getObject();
        MohsProperties.Registration.OnConflict onConflict = properties.getObject().registration().onConflict();

        OnExecutionRegistry observerRegistry = onExecutionRegistry.getObject();

        synchronized (scanned) {
            for (ScannedJob job : scanned.values()) {
                reconcile(store, onConflict, job);
                registry.register(job.definition().key(), job.handler().handler(), job.handler().payloadType());
            }
            for (ScannedObserver observer : observers) {
                observerRegistry.register(observer.annotation(), observer.bean(), observer.method(), observer.declaringMethod());
            }
            reconcileOrphans(store);
        }
    }

    /**
     * {@code annotation x programmatic} always fails, unconditionally — that is not what
     * {@code on-conflict} governs, being an identity collision rather than definitional drift.
     * {@code annotation x annotation} already failed earlier, in {@link #scanMethod}. Real drift
     * within the same {@code ANNOTATION} lineage follows
     * {@link MohsProperties.Registration.OnConflict}.
     */
    private void reconcile(JobStore jobStore, MohsProperties.Registration.OnConflict onConflict, ScannedJob job) {
        JobDefinition incoming = job.definition();
        Optional<StoredJob> existing = jobStore.find(incoming.key());
        if (existing.isEmpty()) {
            jobStore.upsert(incoming);
            return;
        }

        JobDefinition stored = existing.get().definition();
        if (stored.source() == DefinitionSource.PROGRAMMATIC) {
            throw new IllegalStateException("@MohsJob id '" + incoming.key().value() + "' (" + job.declaringMethod()
                    + ") collides with a PROGRAMMATIC definition already registered for the same id");
        }
        if (stored.equals(incoming)) {
            jobStore.upsert(incoming);
            return;
        }

        switch (onConflict) {
            case OVERRIDE -> {
                log.info("job '{}' definition changed, code wins (mohs.registration.on-conflict=override): {}",
                        incoming.key().value(), MohsJobs.diff(stored, incoming));
                jobStore.upsert(incoming);
            }
            case PRESERVE -> log.warn(
                    "job '{}' definition changed but store wins (mohs.registration.on-conflict=preserve), code version ignored: {}",
                    incoming.key().value(), MohsJobs.diff(stored, incoming));
            case FAIL -> throw new IllegalStateException("job '" + incoming.key().value()
                    + "' definition diverged from the stored one (mohs.registration.on-conflict=fail): "
                    + MohsJobs.diff(stored, incoming));
        }
    }

    /**
     * An ANNOTATION job present in the store but absent from this scan becomes ORPHANED.
     *
     * <p>{@code markOrphaned} runs after the try-with-resources closes the
     * {@link JobStore#findAllAnnotationSourced()} cursor — never inside the {@code forEach}, for
     * the same reason as {@code JdbcJobStore#markOrphanedForUnresolvedHandlers}: never write with a
     * read cursor still open on the same connection.
     */
    private void reconcileOrphans(JobStore jobStore) {
        List<JobKey> toOrphan = new ArrayList<>();
        try (var annotationSourced = jobStore.findAllAnnotationSourced()) {
            annotationSourced.filter(stored -> !stored.orphaned())
                    .filter(stored -> !scanned.containsKey(stored.definition().key()))
                    .forEach(stored -> toOrphan.add(stored.definition().key()));
        }
        for (JobKey key : toOrphan) {
            log.warn("job '{}' annotation no longer found in code — marking ORPHANED", key.value());
            jobStore.markOrphaned(key);
        }
    }

    private record ScannedJob(JobDefinition definition, MohsJobs.AdaptedHandler handler, String declaringMethod) {
    }

    private record ScannedObserver(OnExecution annotation, Object bean, Method method, String declaringMethod) {
    }
}
