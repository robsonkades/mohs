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
import org.springframework.util.ReflectionUtils;

import io.mohs.core.definition.DefinitionSource;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.MohsJob;
import io.mohs.core.event.OnExecution;
import io.mohs.core.job.JobKey;
import io.mohs.engine.HandlerRegistry;
import io.mohs.engine.JobStore;
import io.mohs.engine.StoredJob;

/**
 * Escaneia métodos {@link MohsJob} em beans singleton e os traduz em
 * {@link JobDefinition}/{@link JobHandler} — mesmo padrão de duas fases de
 * {@code ScheduledAnnotationBeanPostProcessor} (Spring, para
 * {@code @Scheduled}): acumula durante a inicialização de cada bean
 * ({@link BeanPostProcessor}), só efetiva depois que todos os singletons
 * existem ({@link SmartInitializingSingleton}) — sem isso, a ordem de
 * criação de bean decidiria arbitrariamente qual {@code @MohsJob} "ganha"
 * um conflito de id.
 *
 * <p>Roda antes do {@code Engine} iniciar (ADR-0006: "nenhum claim antes
 * de todas as definições anotadas estarem registradas") porque
 * {@code afterSingletonsInstantiated} acontece durante
 * {@code finishBeanFactoryInitialization}, sempre antes de
 * {@code finishRefresh} — onde {@code SmartLifecycle.start()} dispara
 * ({@link MohsEngineLifecycle}, fase {@link Integer#MAX_VALUE}).
 *
 * <p>Dependências via {@link ObjectProvider}, não injeção direta: um
 * {@link BeanPostProcessor} com dependência de construtor comum força o
 * Spring a criar essa dependência cedo demais, antes de todos os outros
 * {@code BeanPostProcessor}s estarem registrados — o próprio Spring avisa
 * disso em runtime ("not eligible for getting processed by all
 * BeanPostProcessors"). {@code ObjectProvider} adia a resolução pra
 * {@link #afterSingletonsInstantiated}, onde já não há mais problema.
 */
final class MohsJobScanner implements BeanPostProcessor, BeanFactoryAware, SmartInitializingSingleton {

    private static final Logger log = LoggerFactory.getLogger(MohsJobScanner.class);

    private final ObjectProvider<HandlerRegistry> handlerRegistry;
    private final ObjectProvider<JobStore> jobStore;
    private final ObjectProvider<MohsProperties> properties;

    /**
     * Guardado por {@code synchronized} — mesmo motivo de
     * {@code ScheduledAnnotationBeanPostProcessor} guardar suas coleções:
     * com o bootstrap em background do Spring Framework 6.2+
     * ({@code bootstrapExecutor}), {@link #postProcessAfterInitialization}
     * pode rodar em threads concorrentes na aplicação hospedeira —
     * biblioteca embarcada não controla isso.
     */
    private final Map<JobKey, ScannedJob> scanned = new LinkedHashMap<>();

    private @Nullable ConfigurableListableBeanFactory beanFactory;

    MohsJobScanner(ObjectProvider<HandlerRegistry> handlerRegistry, ObjectProvider<JobStore> jobStore, ObjectProvider<MohsProperties> properties) {
        this.handlerRegistry = Objects.requireNonNull(handlerRegistry, "handlerRegistry");
        this.jobStore = Objects.requireNonNull(jobStore, "jobStore");
        this.properties = Objects.requireNonNull(properties, "properties");
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        if (beanFactory instanceof ConfigurableListableBeanFactory clbf) {
            this.beanFactory = clbf;
        }
    }

    /**
     * Pula bean não-singleton — mesma guarda do
     * {@code ScheduledAnnotationBeanPostProcessor}: sem ela, um bean
     * {@code prototype} reprocessaria o mesmo método a cada criação.
     */
    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if (beanFactory != null && !beanFactory.isSingleton(beanName)) {
            return bean;
        }
        Class<?> targetClass = AopUtils.getTargetClass(bean);
        ReflectionUtils.doWithMethods(targetClass, method -> scanMethod(bean, targetClass, method));
        return bean;
    }

    private void scanMethod(Object bean, Class<?> targetClass, Method targetMethod) {
        // fail-fast até o processamento existir — anotação aceita em silêncio
        // seria falha silenciosa (o método nunca receberia evento nenhum);
        // mesma filosofia de "conflito de identidade falha sempre" do reconcile.
        if (targetMethod.isAnnotationPresent(OnExecution.class)) {
            throw new IllegalStateException("@OnExecution on " + describe(targetMethod)
                    + " is not supported yet — the engine does not deliver filtered events to annotated"
                    + " methods in this milestone; register an ExecutionListener bean instead");
        }
        MohsJob annotation = targetMethod.getAnnotation(MohsJob.class);
        if (annotation == null) {
            return;
        }
        // getTargetClass, não bean.getClass(): o proxy CGLIB não deveria virar handlerType
        // persistido (nome sintético, não resolve de forma estável entre restarts).
        Method invocable = AopUtils.selectInvocableMethod(targetMethod, bean.getClass());
        JobKey key = JobKey.of(annotation.id());
        String declaringMethod = describe(targetMethod);

        synchronized (scanned) {
            ScannedJob already = scanned.get(key);
            if (already != null) {
                throw new IllegalStateException("duplicate @MohsJob id '" + annotation.id() + "' — "
                        + already.declaringMethod() + " and " + declaringMethod + " both declare it");
            }

            JobDefinition definition = MohsJobs.toDefinition(key, annotation, targetClass);
            MohsJobs.AdaptedHandler handler = MohsJobs.adaptHandler(bean, invocable);
            scanned.put(key, new ScannedJob(definition, handler, declaringMethod));
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

        synchronized (scanned) {
            for (ScannedJob job : scanned.values()) {
                reconcile(store, onConflict, job);
                registry.register(job.definition().key(), job.handler().handler(), job.handler().payloadType());
            }
            reconcileOrphans(store);
        }
    }

    /**
     * {@code annotation × programmatic} falha sempre, incondicional —
     * não é isso que {@code on-conflict} governa (é uma colisão de
     * identidade, não drift definicional). {@code annotation × annotation}
     * já falhou antes, em {@link #scanMethod}. Drift real na mesma
     * linhagem {@code ANNOTATION} segue {@link MohsProperties.Registration.OnConflict}.
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
     * ANNOTATION presente no store, ausente do scan de agora — vira ORPHANED
     * (ADR-0006). {@code markOrphaned} roda depois que o
     * try-with-resources fecha o cursor de {@link JobStore#findAllAnnotationSourced()}
     * — nunca dentro do {@code forEach}, mesmo motivo de
     * {@code JdbcJobStore#markOrphanedForUnresolvedHandlers} (DUP-3): não
     * escrever com um cursor de leitura ainda aberto na mesma conexão.
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
}
