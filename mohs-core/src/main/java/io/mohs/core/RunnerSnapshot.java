package io.mohs.core;

import java.util.Objects;

import io.mohs.core.resource.RunnerMode;

/**
 * Um runner deste node: a configuração declarada mais quanto trabalho ele
 * carrega agora.
 *
 * <p><strong>Node-local por natureza.</strong> Runner é pool de threads, e
 * pool de threads não existe no cluster — existe num processo. Duas
 * instâncias do mesmo runner em nodes diferentes têm ocupação independente,
 * e não há soma que faça sentido: quem lê isto está olhando o node que
 * respondeu, não o sistema. É a diferença entre esta lista e
 * {@link Mohs#nodes() nodes}/{@link Mohs#jobs() jobs}, que são estado
 * compartilhado no banco.
 *
 * <p>{@code max} é o teto declarado, derivado do modo:
 * {@code maxConcurrent} para {@link RunnerMode#IO}, {@code maxSize} para
 * {@link RunnerMode#CPU}.
 *
 * <p>{@code running} conta o que foi <em>aceito e ainda não concluiu</em>.
 * No modo {@code IO} isso é praticamente o que executa: não há fila entre
 * aceitar e executar — o teto é um semáforo, e acima dele o executor
 * REJEITA ({@code TaskRejectedException}) em vez de enfileirar ou fazer
 * quem submete esperar. No modo {@code CPU} a fila fica entre aceitar e
 * executar, e o número inclui o que espera nela — medir só o que ocupa
 * thread esconderia justamente o acúmulo que o operador precisa ver, e é
 * por isso que {@code running} pode passar de {@code max} nesse modo.
 *
 * <p>Um componente {@code queued} separado foi considerado e recusado: dois
 * números para o mesmo fato ({@code running} já é a soma) obrigariam todo
 * consumidor a saber que só um deles vale por modo, e acrescentar componente
 * a record público quebra o construtor canônico e os padrões de
 * desconstrução — recuar depois custa mais que explicar agora. Quem precisa
 * do acúmulo calcula {@code running - max} no modo {@code CPU}, onde ele é o
 * único significado possível da diferença.
 */
public record RunnerSnapshot(String name, RunnerMode mode, int max, int running) {

    public RunnerSnapshot {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(mode, "mode");
        if (running < 0) {
            throw new IllegalArgumentException("running must not be negative: " + running);
        }
    }
}
