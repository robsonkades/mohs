package io.mohs.engine;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mohs.core.resource.ExecutionWindow;

/**
 * Lookup de {@link ExecutionWindow} por nome — bem mais simples que
 * {@link RunnerRegistry}: {@code ExecutionWindow} não possui recurso
 * nenhum pra construir/fechar (sem {@code Executor} por trás, ao
 * contrário de {@link io.mohs.core.resource.MohsRunner}), então esta
 * classe não tem ciclo de vida próprio. Sem caminho via propriedade —
 * a própria Javadoc de {@link ExecutionWindow} já documenta que
 * predicados só existem em código, então só {@code @Bean ExecutionWindow}
 * alimenta esta lista.
 */
public final class ExecutionWindowRegistry {

    private static final Logger log = LoggerFactory.getLogger(ExecutionWindowRegistry.class);

    private final Map<String, ExecutionWindow> windows;

    public ExecutionWindowRegistry(List<ExecutionWindow> windows) {
        Objects.requireNonNull(windows, "windows");
        Map<String, ExecutionWindow> byName = new LinkedHashMap<>();
        for (ExecutionWindow window : windows) {
            if (byName.containsKey(window.name())) {
                throw new IllegalArgumentException("duplicate execution window name '" + window.name() + "'");
            }
            byName.put(window.name(), window);
        }
        this.windows = Map.copyOf(byName);
    }

    /**
     * {@code null} nunca exclui — job sem window nenhuma referenciada.
     * Nome desconhecido também exclui: fail-safe, um erro de digitação
     * na config não pode deixar um job furar a janela de exclusão
     * pretendida (permitir em silêncio seria pior que bloquear até a
     * config ser corrigida) — WARN a cada ocorrência.
     */
    public boolean excludes(@Nullable String windowName, Instant now) {
        if (windowName == null) {
            return false;
        }
        ExecutionWindow window = windows.get(windowName);
        if (window == null) {
            log.warn("job references unknown execution window '{}' — treating as excluded (fail-safe) until fixed", windowName);
            return true;
        }
        return window.excludes(now);
    }
}
