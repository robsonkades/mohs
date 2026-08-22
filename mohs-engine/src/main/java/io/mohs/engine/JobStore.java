package io.mohs.engine;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import io.mohs.core.definition.DefinitionSource;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.Schedule;

/**
 * Persistência de {@link JobDefinition} — Repository (PoEAA), porta que
 * {@code io.mohs.store.jdbc} implementa (Data Mapper). {@link #upsert} segue a
 * precisão da ADR-0006: grava estado definicional; {@code orphaned}/
 * {@code paused} (operacionais) têm duas exceções deliberadas — o upsert
 * limpa {@code orphaned} (a própria fonte reaparecer é prova de vida,
 * DUP-3) e, SÓ no primeiro registro, inicializa
 * {@code paused = startPaused} (ADR-0037); depois do nascimento,
 * {@code paused} é exclusivo de {@link #pause}/{@link #resume}.
 */
public interface JobStore {

    /**
     * O upsert é o dono do estado inicial do trigger (ADR-0035): agenda
     * nova ou alterada arma {@code nextFireAt} recalculado do relógio;
     * agenda inalterada preserva o valor armazenado — preservar significa
     * <b>não escrever</b> a coluna, nunca reescrever o valor lido (lost
     * update contra o CAS do disparo e o rearme da conclusão); on-demand
     * fica desarmado ({@code null}). Agenda recorrente inalterada com
     * trigger desarmado é curada (rearmada) — para fixed-delay, só sem
     * ocorrência viva do scheduler.
     */
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

    /**
     * Jobs recorrentes devidos (ADR-0035): {@code next_fire_at <= now},
     * excluídos {@code paused}/{@code orphaned}/{@code retired} — pause
     * bloqueia exatamente o trigger; on-demand continua valendo mesmo
     * pausado. Mais antigo primeiro, no máximo {@code limit} (o excedente
     * continua devido e drena nos próximos ticks).
     */
    List<StoredJob> findDueRecurring(Instant now, int limit);

    /**
     * Arma {@code next_fire_at} só se está desarmado ({@code NULL}) — as
     * curas da corrente fixed-delay (ADR-0035): ocorrência cancelada ainda
     * {@code ENQUEUED} não passa pelo caminho de conclusão que rearma, e
     * este guard garante que a cura nunca clobra uma série que um upsert
     * de mudança de agenda já rearmou.
     */
    void armNextFire(JobKey key, Instant nextFireAt);

    /**
     * Reescreve a agenda e rearma o trigger recomputado do relógio na
     * MESMA escrita (ADR-0036) — a única outra escrita legítima de agenda
     * além do {@link #upsert}; escrita incondicional de {@code next_fire_at}
     * de propósito: "reconfiguração explícita vence disparo concorrente"
     * (ADR-0035). Guardada por {@code retired} — job aposentado é
     * invisível pra toda a API.
     *
     * @return {@code true} se a linha existia (e não aposentada)
     */
    boolean reschedule(JobKey key, Schedule schedule);

    /** {@code ANNOTATION} presente no store, ausente do código (ADR-0006) — não dispara, não apaga histórico. */
    void markOrphaned(JobKey key);

    void pause(JobKey key);

    void resume(JobKey key);

    /**
     * Aposentadoria explícita ({@code Mohs#remove}) — só pra definições
     * {@code PROGRAMMATIC} (quem valida o {@code source} é o chamador,
     * {@code MohsImpl}). Soft-retire, nunca delete: drena a fila
     * ({@code mohs_ready}) cancelando o que estava enfileirado, marca a
     * definição como {@code retired} — ela some de
     * {@link #find}/{@link #findAll}, mas a linha (e todo o histórico de
     * execuções) permanece. Um {@link #upsert} do mesmo {@code job_key}
     * ressuscita a definição. A vaga de concorrência não é assunto desta
     * porta desde a ADR-D: o cap deriva de {@code mohs_lease} — deletar a
     * lease É liberar a vaga.
     */
    void remove(JobKey key);
}
