package io.mohs.engine;

import java.util.List;
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
     * Busca várias execuções por id numa única consulta — evita N+1 quando
     * o chamador já tem a lista de ids em mãos (ex.: hidratar o resultado
     * de um claim em lote). Limitado pelo tamanho de {@code ids}, por isso
     * {@code List}, não {@code Stream} (ver {@link #findAll} pra leitura
     * não limitada). Ordem do retorno não é garantida — quem chama reordena
     * se precisar.
     */
    List<Execution> findByIds(List<ExecutionId> ids);

    /**
     * Stream sobre um cursor aberto — não materializa em memória de uma
     * vez. Quem chama é dono do ciclo de vida (try-with-resources).
     * DBTUNE-7: no Postgres, isso só é verdade se a chamada rodar dentro de
     * uma transação (autocommit desligado) — fora dela, o driver
     * materializa o resultado inteiro antes de devolver o primeiro item,
     * apesar do {@code fetchSize} configurado do lado do template.
     */
    Stream<Execution> findByJobKey(JobKey jobKey);

    /** Ver {@link #findByJobKey} sobre ciclo de vida do stream. */
    Stream<Execution> findAll();
}
