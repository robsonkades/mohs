package io.mohs.engine;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

import org.jspecify.annotations.Nullable;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import io.mohs.core.resource.MohsRunner;

/**
 * Converte {@link MohsRunner} (spec puro, sem {@code Executor} nenhum —
 * ver Javadoc da classe) em executores vivos via {@link MohsExecutors}, e
 * é dona do ciclo de vida deles: constrói aqui, então fecha aqui
 * ({@link #close()}) — mesma disciplina de posse já documentada em
 * {@link MohsExecutors} ("quem constrói é dono do ciclo de vida").
 *
 * <p>Recebe a lista já resolvida — defaults, overrides de propriedade e
 * conflito entre fonte de config são decisão de quem monta a lista
 * (hoje, {@code io.mohs.autoconfigure}), não desta classe.
 */
public final class RunnerRegistry implements AutoCloseable {

    /** {@code JobDefinition.runner() == null} resolve pra este nome — o único runner que esta classe exige que exista. */
    public static final String DEFAULT_RUNNER = "io";

    private final Map<String, LiveRunner> executors;

    /**
     * Executor vivo pareado com a ação de desligamento que nasce junto
     * dele em {@link #build} — {@link #close()} só executa
     * {@code shutdown.run()}, sem re-derivar o tipo concreto por
     * {@code instanceof}: um modo novo de runner muda um único lugar.
     */
    private record LiveRunner(AsyncTaskExecutor executor, Runnable shutdown) {
    }

    public RunnerRegistry(List<MohsRunner> runners) {
        Objects.requireNonNull(runners, "runners");
        // valida os specs inteiros antes de construir qualquer executor: lançar
        // no meio da construção deixaria pools já inicializados órfãos, sem
        // ninguém pra chamar close()/destroy() neles
        Map<String, MohsRunner> specs = new LinkedHashMap<>();
        for (MohsRunner runner : runners) {
            if (specs.putIfAbsent(runner.name(), runner) != null) {
                throw new IllegalArgumentException("duplicate runner name '" + runner.name() + "'");
            }
        }
        if (!specs.containsKey(DEFAULT_RUNNER)) {
            throw new IllegalArgumentException(
                    "RunnerRegistry requires a '" + DEFAULT_RUNNER + "' runner (the default) — none provided: " + specs.keySet());
        }
        Map<String, LiveRunner> built = new LinkedHashMap<>();
        specs.forEach((name, spec) -> built.put(name, build(spec)));
        this.executors = Map.copyOf(built);
    }

    /**
     * Protocolo de desligamento certo por tipo concreto —
     * {@link SimpleAsyncTaskExecutor#close()} (IO) vs.
     * {@link ThreadPoolTaskExecutor#destroy()} (CPU), a mesma assimetria
     * já documentada em {@link MohsExecutors} — decidido aqui, no único
     * lugar que conhece o tipo construído.
     */
    private static LiveRunner build(MohsRunner runner) {
        String namePrefix = "mohs-runner-" + runner.name();
        return switch (runner.mode()) {
            case IO -> {
                SimpleAsyncTaskExecutor io = MohsExecutors.ioBoundExecutor(namePrefix, runner.maxConcurrent());
                yield new LiveRunner(io, io::close);
            }
            case CPU -> {
                ThreadPoolTaskExecutor cpu = MohsExecutors.cpuBoundExecutor(namePrefix, runner.coreSize(), runner.maxSize(), runner.queueCapacity(), runner.keepAlive());
                yield new LiveRunner(cpu, cpu::destroy);
            }
        };
    }

    /**
     * {@code null} resolve pro runner {@link #DEFAULT_RUNNER} — mesmo
     * contrato já documentado em {@code JobDefinition.runner()}: "null usa
     * o runner default". Nome não encontrado lança — quem chama (hoje,
     * {@link Engine#submitDispatch}) decide o que fazer (falhar só a
     * execução, não o node inteiro).
     */
    public AsyncTaskExecutor resolve(@Nullable String runnerName) {
        String name = runnerName == null ? DEFAULT_RUNNER : runnerName;
        LiveRunner runner = executors.get(name);
        if (runner == null) {
            throw new NoSuchElementException(noSuchRunnerMessage(name));
        }
        return runner.executor();
    }

    /**
     * Divergência só de caixa ganha diagnóstico próprio: o binder do Spring
     * canonicaliza chave de mapa não-bracketed pra minúsculas, então
     * {@code mohs.runners.myUpload.*} registra o runner como {@code myupload}
     * enquanto {@code JobDefinition.runner()} é case-sensitive — sem a dica,
     * "no runner named 'myUpload'" não ensina o que corrigir.
     */
    private String noSuchRunnerMessage(String name) {
        for (String registered : executors.keySet()) {
            if (registered.equalsIgnoreCase(name)) {
                return "no runner named '" + name + "' registered, but '" + registered + "' is — runner names are case-sensitive, and Spring's"
                        + " relaxed binding lowercases unbracketed map keys (declare it as mohs.runners.[" + name + "] to keep the exact case,"
                        + " or use the lowercase name in JobDefinition.runner())";
            }
        }
        return "no runner named '" + name + "' registered — available: " + executors.keySet();
    }

    @Override
    public void close() {
        for (LiveRunner runner : executors.values()) {
            runner.shutdown().run();
        }
    }
}
