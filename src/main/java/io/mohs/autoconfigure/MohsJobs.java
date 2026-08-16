package io.mohs.autoconfigure;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.RecordComponent;
import java.time.Duration;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.StringJoiner;

import io.mohs.core.definition.DefinitionSource;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.MohsJob;
import io.mohs.core.execution.JobContext;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.CronSpec;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.core.schedule.Schedule;
import io.mohs.engine.JobHandler;

import org.jspecify.annotations.Nullable;

/**
 * Vocabulário puro do scanner de {@link MohsJob} — sem estado, sem Spring,
 * testável isolado. {@link MohsJobScanner} é a única chamadora;
 * package-private pelo mesmo motivo de {@code JobSpecImpl} (Effective
 * Java Item 15: minimize acessibilidade).
 */
final class MohsJobs {

    private MohsJobs() {
    }

    /**
     * Casamento de parâmetro do método anotado: até um {@link JobContext}
     * e um payload (o resto), em qualquer ordem — a única regra que
     * {@link MohsJob} documenta ("até um payload e um JobContext,
     * opcionais, em qualquer ordem") e que não tinha algoritmo nenhum
     * especificado até aqui. {@link InvocationTargetException} é
     * desembrulhada: {@code Dispatcher} grava a mensagem da exceção
     * original em {@code Attempt.error()}, não o nome de
     * "InvocationTargetException". A {@link IllegalArgumentException} crua
     * do reflection (payload de tipo errado — o método nem roda) é
     * embrulhada nomeando método e tipos: "argument type mismatch" sozinho
     * não diz o que corrigir.
     */
    static AdaptedHandler adaptHandler(Object bean, Method method) {
        Objects.requireNonNull(bean, "bean");
        Objects.requireNonNull(method, "method");
        method.setAccessible(true);

        Parameter[] parameters = method.getParameters();
        if (parameters.length > 2) {
            throw new IllegalStateException(unsupportedSignature(method,
                    "supports at most 2 parameters (a payload and/or a JobContext), found " + parameters.length));
        }

        int payloadIndex = -1;
        int contextIndex = -1;
        for (int i = 0; i < parameters.length; i++) {
            if (JobContext.class.isAssignableFrom(parameters[i].getType())) {
                if (contextIndex != -1) {
                    throw new IllegalStateException(unsupportedSignature(method, "declares more than one JobContext parameter"));
                }
                contextIndex = i;
            } else {
                if (payloadIndex != -1) {
                    throw new IllegalStateException(unsupportedSignature(method, "declares more than one non-JobContext (payload) parameter"));
                }
                payloadIndex = i;
            }
        }

        int finalPayloadIndex = payloadIndex;
        int finalContextIndex = contextIndex;
        int argCount = parameters.length;
        Class<?> payloadType = payloadIndex == -1 ? null : parameters[payloadIndex].getType();
        JobHandler handler = (payload, ctx) -> {
            Object[] args = new Object[argCount];
            if (finalPayloadIndex != -1) {
                args[finalPayloadIndex] = payload;
            }
            if (finalContextIndex != -1) {
                args[finalContextIndex] = ctx;
            }
            try {
                method.invoke(bean, args);
            } catch (IllegalArgumentException e) {
                // IAE direta do reflection (sem InvocationTargetException): o método
                // nem chegou a rodar — payload de tipo que o parâmetro não aceita.
                throw new IllegalStateException(unsupportedSignature(method,
                        "cannot receive payload of type " + (payload == null ? "null" : payload.getClass().getName())
                                + (payloadType == null ? "" : " — parameter expects " + payloadType.getName())
                                + " (scheduled with the wrong type, or persisted before a deploy that changed the parameter — ADR-0011)"), e);
            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                if (cause instanceof Exception checkedCause) {
                    throw checkedCause;
                }
                if (cause instanceof Error error) {
                    throw error;
                }
                throw e;
            }
        };
        return new AdaptedHandler(handler, payloadType);
    }

    private static String unsupportedSignature(Method method, String reason) {
        return "@MohsJob method " + method.getDeclaringClass().getName() + "#" + method.getName() + " " + reason;
    }

    /**
     * {@link #adaptHandler} devolve os dois juntos porque nascem do mesmo
     * reflection sobre o método anotado — {@code payloadType} é
     * {@code null} quando o método não declara parâmetro de payload
     * nenhum (ex.: só {@link JobContext}, ou nenhum parâmetro). Usado por
     * {@code io.mohs.rest} (via {@code HandlerRegistry#payloadType}) pra
     * converter o corpo JSON do {@code POST .../schedule} pro tipo real
     * antes de agendar, em vez de persistir um {@code Map} cru.
     */
    record AdaptedHandler(JobHandler handler, @Nullable Class<?> payloadType) {
    }

    /**
     * Traduz a anotação numa {@link JobDefinition} {@code ANNOTATION} —
     * constrói o record direto pelo construtor canônico, não pelo builder
     * staged ({@link JobDefinition#of}): {@code ANNOTATION} só deveria ser
     * produzível por quem realmente escaneou uma anotação, e o builder
     * público hardcoda {@code PROGRAMMATIC}/{@code name = null} de
     * propósito (ver {@code JobSpecImpl#toDefinition}). Toda validação de
     * combinação de campo (ex. allow/maxConcurrentExecutions) já vem de
     * graça do compact constructor do record.
     */
    static JobDefinition toDefinition(JobKey key, MohsJob annotation, Class<?> handlerType) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(annotation, "annotation");
        Objects.requireNonNull(handlerType, "handlerType");

        Schedule declaredTrigger = triggerOrNull("@MohsJob(id=\"" + annotation.id() + "\")",
                annotation.cron(), annotation.zone(), annotation.every(), annotation.everyAfterFinish());
        Schedule schedule = Objects.requireNonNullElse(declaredTrigger, new OnDemandSpec());
        int maxConcurrentExecutions = annotation.allowConcurrentExecutions() ? 0 : annotation.maxConcurrentExecutions();

        return new JobDefinition(key, blankToNull(annotation.name()), handlerType, schedule,
                blankToNull(annotation.runner()), blankToNull(annotation.window()), annotation.misfire(),
                annotation.startPaused(), annotation.allowConcurrentExecutions(), maxConcurrentExecutions,
                annotation.retries(), parseDurationOrNull(annotation.timeout()), blankToNull(annotation.retryPolicy()),
                DefinitionSource.ANNOTATION);
    }

    /**
     * §5.13 (ADR-0038): job recorrente não tem fonte de payload — o disparo
     * automático entrega mapa vazio. Handler cujo parâmetro não pode
     * recebê-lo falharia TODA ocorrência em runtime; falha o boot em vez
     * disso. {@code Map}/{@code Object} passam de propósito: invocação
     * manual avulsa do mesmo job pode entregar dados (padrão "parâmetro
     * opcional"). Cobre qualquer anotação (a decisão é pela
     * {@link JobDefinition}); registro manual via {@code HandlerRegistry}
     * (test kit) não passa por aqui — a validação pertence ao scan de boot.
     */
    static void requireRecurringHandlerAcceptsAutomaticPayload(JobDefinition definition, AdaptedHandler handler, String declaringMethod) {
        if (definition.schedule() instanceof OnDemandSpec) {
            return;
        }
        Class<?> payloadType = handler.payloadType();
        if (payloadType == null || payloadType.isAssignableFrom(LinkedHashMap.class)) {
            return;
        }
        throw new IllegalStateException("job '" + definition.key().value() + "' (" + declaringMethod
                + ") has a recurring schedule but its handler declares a payload parameter of type "
                + payloadType.getName() + " — automatic occurrences carry no payload (an empty map). "
                + "Drop the parameter, accept java.util.Map, or make the job on-demand (@OnDemandJob)");
    }

    /**
     * O gatilho declarado, ou {@code null} quando nenhum foi — quem chama
     * decide o significado da ausência ({@code @MohsJob} → on-demand;
     * {@code @RecurringJob} → erro de boot). {@code label} nomeia a
     * anotação e o id nas mensagens — erro de boot que não diz ONDE
     * corrigir não ensina nada.
     */
    private static @Nullable Schedule triggerOrNull(String label, String cron, String zone, String every, String everyAfterFinish) {
        boolean hasCron = !cron.isBlank();
        boolean hasEvery = !every.isBlank();
        boolean hasEveryAfterFinish = !everyAfterFinish.isBlank();
        int triggerCount = (hasCron ? 1 : 0) + (hasEvery ? 1 : 0) + (hasEveryAfterFinish ? 1 : 0);
        if (triggerCount > 1) {
            throw new IllegalStateException(label + " sets more than one trigger — "
                    + "cron/every/everyAfterFinish are mutually exclusive");
        }
        if (hasCron) {
            if (zone.isBlank()) {
                throw new IllegalStateException(label + " sets cron() without zone() — zone is required for cron");
            }
            return new CronSpec(cron, ZoneId.of(zone));
        }
        if (hasEvery) {
            return new IntervalSpec(Duration.parse(every), false);
        }
        if (hasEveryAfterFinish) {
            return new IntervalSpec(Duration.parse(everyAfterFinish), true);
        }
        return null;
    }

    private static @Nullable String blankToNull(String value) {
        return value.isBlank() ? null : value;
    }

    private static @Nullable Duration parseDurationOrNull(String value) {
        return value.isBlank() ? null : Duration.parse(value);
    }

    /** Só os componentes do record que mudaram — formato "campo: antigo -> novo" (ADR-0006). */
    static String diff(JobDefinition existing, JobDefinition incoming) {
        Objects.requireNonNull(existing, "existing");
        Objects.requireNonNull(incoming, "incoming");
        StringJoiner joiner = new StringJoiner(", ");
        for (RecordComponent component : JobDefinition.class.getRecordComponents()) {
            try {
                Object oldValue = component.getAccessor().invoke(existing);
                Object newValue = component.getAccessor().invoke(incoming);
                if (!Objects.equals(oldValue, newValue)) {
                    joiner.add(component.getName() + ": '" + oldValue + "' -> '" + newValue + "'");
                }
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException("failed to diff JobDefinition." + component.getName(), e);
            }
        }
        return joiner.length() == 0 ? "(no definitional change)" : joiner.toString();
    }
}
