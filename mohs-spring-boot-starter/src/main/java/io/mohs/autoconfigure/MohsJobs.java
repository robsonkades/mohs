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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.RecordComponent;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Predicate;
import java.util.stream.Stream;

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
 * Pure vocabulary for the {@link MohsJob} scanner — no state, no Spring, testable in isolation.
 * {@link MohsJobScanner} is its only caller; package-private for the same reason as
 * {@code JobSpecImpl} (Effective Java Item 15: minimise accessibility).
 */
final class MohsJobs {

    private MohsJobs() {
    }

    /**
     * Builds the invoker for the annotated method over the signature already resolved by
     * {@link ParameterBinding#of}.
     *
     * <p>{@link InvocationTargetException} is unwrapped, because {@code Dispatcher} records the
     * ORIGINAL exception's message in {@code Attempt.error()}, not the string
     * "InvocationTargetException". The raw {@link IllegalArgumentException} from reflection (a
     * payload of the wrong type — the method never even runs) is wrapped naming the method and the
     * types: "argument type mismatch" on its own does not say what to fix.
     */
    static AdaptedHandler adaptHandler(Object bean, Method method) {
        Objects.requireNonNull(bean, "bean");
        Objects.requireNonNull(method, "method");
        method.setAccessible(true);

        ParameterBinding binding = ParameterBinding.of(method);
        JobHandler handler = (payload, ctx) -> {
            Object[] args = binding.arguments(payload, ctx);
            try {
                method.invoke(bean, args);
            } catch (IllegalArgumentException e) {
                // A direct IAE from reflection (no InvocationTargetException): the method never ran
                // at all — a payload of a type the parameter does not accept.
                throw new IllegalStateException(unsupportedSignature(method,
                        "cannot receive payload of type " + (payload == null ? "null" : payload.getClass().getName())
                                + (binding.payloadType() == null ? "" : " — parameter expects " + binding.payloadType().getName())
                                + " (scheduled with the wrong type, or persisted before a deploy that changed the parameter)"), e);
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
        return new AdaptedHandler(handler, binding.payloadType());
    }

    /**
     * How the arguments of a {@link MohsJob} method are assembled: the payload's position and the
     * {@link JobContext}'s ({@code -1} when absent), how many arguments the invoke takes, and the
     * payload's declared type.
     *
     * <p>It exists to separate RESOLVING the signature — the part with error cases — from building
     * the invoker.
     */
    record ParameterBinding(int payloadIndex, int contextIndex, int argCount, @Nullable Class<?> payloadType) {

        /**
         * Matches the annotated method's parameters: at most one {@link JobContext} and one payload
         * (whatever is left), in any order — the only rule {@link MohsJob} documents, and the one
         * that had no algorithm specified anywhere until now. A signature error fails here, so the
         * invoker is born already valid.
         */
        static ParameterBinding of(Method method) {
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

            return new ParameterBinding(payloadIndex, contextIndex, parameters.length,
                    payloadIndex == -1 ? null : parameters[payloadIndex].getType());
        }

        /**
         * The {@code invoke} arguments, each in the position the method declared it. Every declared
         * parameter is either the payload or the {@link JobContext} — the array never leaves with
         * an empty slot.
         */
        Object[] arguments(Object payload, JobContext ctx) {
            Object[] args = new Object[argCount];
            if (payloadIndex != -1) {
                args[payloadIndex] = payload;
            }
            if (contextIndex != -1) {
                args[contextIndex] = ctx;
            }
            return args;
        }
    }

    private static String unsupportedSignature(Method method, String reason) {
        return "@MohsJob method " + method.getDeclaringClass().getName() + "#" + method.getName() + " " + reason;
    }

    /**
     * {@link #adaptHandler} returns both together because they come from the same reflection over
     * the annotated method. {@code payloadType} is {@code null} when the method declares no payload
     * parameter at all (only a {@link JobContext}, say, or no parameters).
     *
     * <p>Used by {@code io.mohs.rest} (through {@code HandlerRegistry#payloadType}) to convert the
     * JSON body of {@code POST .../schedule} into the real type before scheduling, rather than
     * persisting a raw {@code Map}.
     */
    record AdaptedHandler(JobHandler handler, @Nullable Class<?> payloadType) {
    }

    /**
     * Translates the annotation into an {@code ANNOTATION} {@link JobDefinition}, building the
     * record through its canonical constructor rather than the staged builder
     * ({@link JobDefinition#of}): {@code ANNOTATION} should only be producible by something that
     * actually scanned an annotation, and the public builder hardcodes
     * {@code PROGRAMMATIC}/{@code name = null} on purpose (see {@code JobSpecImpl#toDefinition}).
     * Every field-combination validation comes free from the record's compact constructor.
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
                blankToNull(annotation.runner()), blankToNull(annotation.window()),
                blankToNull(annotation.rateLimit()), annotation.misfire(),
                annotation.startPaused(), annotation.allowConcurrentExecutions(), maxConcurrentExecutions,
                annotation.retries(), parseDurationOrNull(annotation.timeout()), blankToNull(annotation.retryPolicy()),
                DefinitionSource.ANNOTATION);
    }

    /**
     * A recurring job has no payload source — an automatic firing delivers an empty map. A handler
     * whose parameter cannot receive that would fail EVERY occurrence at runtime, so the boot fails
     * instead.
     *
     * <p>{@code Map}/{@code Object} pass deliberately: a one-off manual invocation of the same job
     * may carry data (the "optional parameter" pattern). This covers any annotation, since the
     * decision is made from the {@link JobDefinition}; manual registration through
     * {@code HandlerRegistry} (the test kit) does not come through here, because the validation
     * belongs to the boot scan.
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
     * The declared trigger, or {@code null} when there was none — the caller decides what absence
     * means ({@code @MohsJob} means on-demand; {@code @RecurringJob} means a boot error).
     * {@code label} names the annotation and the id in messages: a boot error that does not say
     * WHERE to fix it teaches nothing.
     */
    private static @Nullable Schedule triggerOrNull(String label, String cron, String zone, String every, String everyAfterFinish) {
        long declaredTriggers = Stream.of(cron, every, everyAfterFinish).filter(Predicate.not(String::isBlank)).count();
        if (declaredTriggers > 1) {
            throw new IllegalStateException(label + " sets more than one trigger — "
                    + "cron/every/everyAfterFinish are mutually exclusive");
        }
        if (!cron.isBlank()) {
            if (zone.isBlank()) {
                throw new IllegalStateException(label + " sets cron() without zone() — zone is required for cron");
            }
            return new CronSpec(cron, parseZone(label, zone));
        }
        if (!every.isBlank()) {
            return new IntervalSpec(parseIsoDuration(label, "every", every), false);
        }
        if (!everyAfterFinish.isBlank()) {
            return new IntervalSpec(parseIsoDuration(label, "everyAfterFinish", everyAfterFinish), true);
        }
        return null;
    }

    /**
     * {@code label} on BOTH branches that people actually get wrong. {@code every = "5s"} (the
     * human form instead of ISO-8601) used to bring the boot down with
     * {@code DateTimeParseException: Text '5s' could not be parsed at index 0} — no job id, no
     * class, no method, and no hint about the format it wanted.
     */
    private static Duration parseIsoDuration(String label, String attribute, String value) {
        try {
            return Duration.parse(value);
        } catch (DateTimeParseException e) {
            throw new IllegalStateException(label + " has " + attribute + "=\"" + value
                    + "\" — expected an ISO-8601 duration such as PT5S, PT30M or PT1H30M", e);
        }
    }

    /** Same reason as {@link #parseIsoDuration}: without the {@code label}, a mistyped zone brings the boot down without saying WHICH job to fix. */
    private static ZoneId parseZone(String label, String zone) {
        try {
            return ZoneId.of(zone);
        } catch (DateTimeException e) {
            throw new IllegalStateException(label + " has zone=\"" + zone
                    + "\" — expected an IANA zone id such as America/Sao_Paulo or UTC", e);
        }
    }

    private static @Nullable String blankToNull(String value) {
        return value.isBlank() ? null : value;
    }

    private static @Nullable Duration parseDurationOrNull(String value) {
        return value.isBlank() ? null : Duration.parse(value);
    }

    /** Only the record components that changed, formatted as "field: old -> new". */
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
