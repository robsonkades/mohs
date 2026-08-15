package io.mohs.engine;

import java.util.Optional;
import java.util.stream.Stream;

import io.mohs.core.definition.DefinitionSource;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.job.JobKey;

/**
 * Persistência de {@link JobDefinition} — Repository (PoEAA), porta que
 * {@code io.mohs.jdbc} implementa (Data Mapper). {@link #upsert} segue a
 * precisão da ADR-0006: só grava estado definicional, nunca
 * {@code orphaned}/{@code paused} (operacional, exclusivo de
 * {@link #markOrphaned}/{@link #pause}/{@link #resume}).
 */
public interface JobStore {

    JobDefinition upsert(JobDefinition definition);

    Optional<StoredJob> find(JobKey key);

    /**
     * Stream sobre um cursor aberto — não materializa a tabela inteira em
     * memória de uma vez. Quem chama é dono do ciclo de vida
     * (try-with-resources); fechar o stream libera a conexão por trás.
     * DBTUNE-7: no Postgres, isso só é verdade se a chamada rodar dentro de
     * uma transação (autocommit desligado) — fora dela, o driver
     * materializa o resultado inteiro antes de devolver o primeiro item,
     * apesar do {@code fetchSize} configurado do lado do template.
     */
    Stream<StoredJob> findAll();

    /**
     * Mesmo contrato de cursor de {@link #findAll()}, filtrado a
     * {@link DefinitionSource#ANNOTATION} na fonte (não em memória depois) —
     * {@code io.mohs.autoconfigure.MohsJobScanner} reconcilia órfãs só
     * contra este subconjunto; {@code PROGRAMMATIC} nunca fica
     * {@code ORPHANED} (ver {@link #markOrphaned}), então baixá-las junto
     * seria banda de leitura sem uso.
     */
    Stream<StoredJob> findAllAnnotationSourced();

    /** {@code ANNOTATION} presente no store, ausente do código (ADR-0006) — não dispara, não apaga histórico. */
    void markOrphaned(JobKey key);

    void pause(JobKey key);

    void resume(JobKey key);

    /** Aposentadoria explícita ({@code Mohs#remove}) — só pra definições {@code PROGRAMMATIC}. */
    void remove(JobKey key);

    /**
     * Reserva uma vaga de execução concorrente se {@code
     * runningExecutionCount < maxConcurrentExecutions} — incremento atômico
     * guardado, sem {@code SELECT} prévio (ADR-0018/0020). Chamado pelo
     * claim, candidato a candidato, dentro da mesma transação.
     *
     * @return {@code true} se reservou a vaga; {@code false} se o job já
     *         está no teto (candidato fica de fora deste batch de claim).
     */
    boolean tryIncrementRunningExecutions(JobKey key);

    /**
     * Devolve uma vaga reservada por engano — usado dentro da própria
     * transação de claim quando um candidato reserva a vaga mas não chega a
     * ser efetivamente reivindicado. Guardado contra contagem negativa. Não
     * é o decremento de conclusão de execução (etapa de dispatch, ainda não
     * implementada).
     */
    void decrementRunningExecutions(JobKey key);
}
