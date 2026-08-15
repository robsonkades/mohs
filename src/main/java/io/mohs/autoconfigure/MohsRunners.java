package io.mohs.autoconfigure;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jspecify.annotations.Nullable;

import io.mohs.core.resource.MohsRunner;
import io.mohs.core.resource.RunnerMode;
import io.mohs.engine.RunnerRegistry;

/**
 * Montagem dos runners nomeados — vocabulário puro do wiring, sem estado,
 * sem Spring, testável isolado (mesmo padrão de {@link MohsJobs}).
 * {@link MohsAutoConfiguration} é a única chamadora; package-private
 * (Effective Java Item 15: minimize acessibilidade).
 */
final class MohsRunners {

    private static final String BUILT_IN = "built-in";

    private MohsRunners() {
    }

    /** Runner junto da fonte que o declarou — um mapa só carrega os dois, em vez de dois mapas paralelos com a mesma chave. */
    private record SourcedRunner(MohsRunner runner, String source) {
    }

    /**
     * {@code io}/{@code cpu} built-in sempre presentes (defaults do
     * documento mestre — {@code io} reaproveita
     * {@code mohs.engine.dispatch-concurrency}, mesmo papel que tinha
     * quando ainda era o único executor de dispatch fixo). Built-in pode
     * ser sobrescrito por propriedade ou {@code @Bean}; nome duplicado
     * entre {@code mohs.runners.*} e {@code @Bean MohsRunner} é erro de
     * boot — mesma filosofia de "conflito de identidade falha sempre" já
     * usada pro {@code annotation × programmatic} do {@link MohsJobScanner}.
     */
    static List<MohsRunner> assemble(MohsProperties properties, List<MohsRunner> beanRunners) {
        Map<String, SourcedRunner> byName = new LinkedHashMap<>();
        byName.put(RunnerRegistry.DEFAULT_RUNNER, new SourcedRunner(
                MohsRunner.io(RunnerRegistry.DEFAULT_RUNNER).maxConcurrent(properties.engine().dispatchConcurrency()).build(), BUILT_IN));
        byName.put("cpu", new SourcedRunner(MohsRunner.cpu("cpu").build(), BUILT_IN));

        properties.runners().forEach((name, spec) ->
                declare(byName, name, toMohsRunner(name, spec), "mohs.runners." + name));
        for (MohsRunner beanRunner : beanRunners) {
            declare(byName, beanRunner.name(), beanRunner, "@Bean MohsRunner " + beanRunner.name());
        }
        return byName.values().stream().map(SourcedRunner::runner).toList();
    }

    private static void declare(Map<String, SourcedRunner> byName, String name, MohsRunner runner, String source) {
        SourcedRunner existing = byName.get(name);
        if (existing != null && !existing.source().equals(BUILT_IN)) {
            throw new IllegalStateException("runner '" + name + "' declared more than once: " + existing.source() + " and " + source);
        }
        byName.put(name, new SourcedRunner(runner, source));
    }

    /**
     * Campo do modo errado é erro de boot, nunca descarte silencioso —
     * mesma postura do compact constructor de {@link MohsRunner}, que lança
     * pra campo do modo errado (e mesma filosofia de "conflito de identidade
     * falha sempre" de {@link #declare}): {@code core-size=2}
     * com {@code mode} esquecido no default {@code io} viraria um runner de
     * 64 virtual threads pra trabalho CPU-bound, sem aviso nenhum. A
     * validação do próprio builder ganha o contexto que só a propriedade tem
     * ("maxSize must be >= coreSize" sozinho não diz qual runner nem qual
     * propriedade — e {@code core-size} default depende dos núcleos da
     * máquina, então o boot falharia só em produção).
     */
    private static MohsRunner toMohsRunner(String name, MohsProperties.Runner spec) {
        String prefix = "mohs.runners." + name;
        try {
            return switch (spec.mode()) {
                case IO -> {
                    requireUnset(prefix, spec.mode(), "core-size", spec.coreSize());
                    requireUnset(prefix, spec.mode(), "max-size", spec.maxSize());
                    requireUnset(prefix, spec.mode(), "queue-capacity", spec.queueCapacity());
                    requireUnset(prefix, spec.mode(), "keep-alive", spec.keepAlive());
                    MohsRunner.IoBuilder builder = MohsRunner.io(name);
                    if (spec.max() != null) {
                        builder.maxConcurrent(spec.max());
                    }
                    yield builder.build();
                }
                case CPU -> {
                    requireUnset(prefix, spec.mode(), "max", spec.max());
                    MohsRunner.CpuBuilder builder = MohsRunner.cpu(name);
                    if (spec.coreSize() != null) {
                        builder.coreSize(spec.coreSize());
                    }
                    if (spec.maxSize() != null) {
                        builder.maxSize(spec.maxSize());
                    }
                    if (spec.queueCapacity() != null) {
                        builder.queueCapacity(spec.queueCapacity());
                    }
                    if (spec.keepAlive() != null) {
                        builder.keepAlive(spec.keepAlive());
                    }
                    yield builder.build();
                }
            };
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("invalid runner declared at " + prefix + ".*: " + e.getMessage(), e);
        }
    }

    private static void requireUnset(String prefix, RunnerMode mode, String property, @Nullable Object value) {
        if (value != null) {
            throw new IllegalStateException("invalid runner declared at " + prefix + ".*: " + property
                    + " does not apply to mode=" + mode + " — change " + prefix + ".mode or remove " + prefix + "." + property);
        }
    }
}
