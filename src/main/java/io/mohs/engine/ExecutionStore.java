package io.mohs.engine;

import java.util.Optional;
import java.util.stream.Stream;

import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;

/**
 * Persistência de {@link Execution} — Repository (PoEAA), porta que
 * {@code io.mohs.jdbc} implementa (Data Mapper). {@link #insert} é o
 * "insert do terminal" da cláusula 4 da ADR-0003 (transacional por
 * participação): entra na transação já ativa do chamador quando existe
 * uma (mesmo {@code DataSource}), ou auto-commit sem ela — nenhuma
 * transação própria é aberta aqui.
 *
 * <p>Transição de estado (claim, conclusão, retry) não é responsabilidade
 * desta etapa — entra junto do claim/dispatch de verdade. Payload não é
 * campo de {@link Execution} (não é parte do contrato M1); carregá-lo de
 * volta pra dispatch é decisão de quem consome, não desta porta.
 */
public interface ExecutionStore {

    /** Grava a execução e o payload serializado; {@code execution.attempts()} deve estar vazio (ainda não disparou). */
    Execution insert(Execution execution, Object payload);

    Optional<Execution> find(ExecutionId id);

    /**
     * Stream sobre um cursor aberto — não materializa em memória de uma
     * vez. Quem chama é dono do ciclo de vida (try-with-resources).
     */
    Stream<Execution> findByJobKey(JobKey jobKey);

    /** Ver {@link #findByJobKey} sobre ciclo de vida do stream. */
    Stream<Execution> findAll();
}
