package io.mohs.core;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import io.mohs.core.job.JobKey;
import io.mohs.core.job.JobRef;
import org.springframework.lang.CheckReturnValue;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.schedule.Schedule;

/**
 * Fachada pública do Mohs — um verbo por operação, sempre sobre definição
 * existente. Corpo ainda não ligado ao motor: M1 é só o contrato; a
 * implementação real vive em {@code io.mohs.engine}, fiada por
 * {@code io.mohs.autoconfigure} (M3).
 *
 * <p>Os métodos de leitura ({@link #findJob}/{@link #jobs}/
 * {@link #findExecution}/{@link #executions}/{@link #payloadType}) existem
 * pra {@code io.mohs.rest} (M3): a fronteira arquitetural
 * ({@code ArchitectureTest#rest_only_sees_public_api}) proíbe a REST de
 * enxergar {@code io.mohs.engine} diretamente — esta fachada é o único
 * caminho de leitura que ela pode usar.
 */
public interface Mohs {

    @CheckReturnValue
    <T> ScheduleCommand schedule(JobRef<T> ref, T payload);

    /** Overload por string; tipo do payload é checado em runtime contra a definição (erro claro, não CCE). */
    @CheckReturnValue
    ScheduleCommand schedule(String jobId, Object payload);

    /**
     * Agenda um lote nomeado de jobs. <b>Ainda não suportado nesta versão</b>
     * — lança {@link UnsupportedOperationException} até os contadores de
     * conclusão de lote serem ligados ao motor
     * (ver {@code docs/MOHS-DOCUMENTO-MESTRE.md}).
     */
    @CheckReturnValue
    Batch batch(String name, Consumer<BatchBuilder> configurer);

    void define(JobDefinition definition);

    /**
     * Aposentadoria: cancela disparos futuros (execuções {@code ENQUEUED}
     * viram {@code CANCELLED}), preserva histórico. Só pra definições
     * programáticas — pra um job {@code @MohsJob}, remova a anotação (o
     * scanner o marca {@code ORPHANED} no próximo boot); chamar isto nele
     * lança {@link IllegalArgumentException}. Job desconhecido é no-op.
     */
    void remove(JobKey jobKey);

    /**
     * Cancela uma execução (ADR-0034). Pendente ({@code ENQUEUED}/
     * {@code RETRY_SCHEDULED}) vira {@code CANCELLED} na hora;
     * {@code RUNNING} recebe o pedido cooperativo — o node dono observa em
     * até um poll-interval e o handler decide quando parar (via
     * {@link io.mohs.core.execution.JobContext#cancellationRequested()});
     * estado terminal não muda. Nunca é imediato nem garantido: uma
     * conclusão pode vencer a corrida, e nesse caso ela vale.
     *
     * @return a execução no estado corrente logo após o pedido — não
     *         necessariamente terminal; vazio se o id não existe
     */
    Optional<Execution> cancel(ExecutionId executionId);

    /**
     * Retry manual de uma execução {@code FAILED} (M3 sobre a ADR-0033):
     * rearma a MESMA linha como {@code RETRY_SCHEDULED} devida agora — a
     * nova tentativa viaja pelo caminho normal do claim, disputando como
     * qualquer candidato. Bypassa o orçamento de {@code retries} de
     * propósito: a política protege o sistema de loops automáticos; aqui a
     * decisão é do operador. Não passa pela dedupe de Idempotency-Key
     * (nada novo é inserido — ADR-0030/0033); a idempotência natural é o
     * próprio CAS: repetir a chamada encontra a execução já rearmada e
     * falha com a exceção de estado.
     *
     * @return a execução já rearmada ({@code RETRY_SCHEDULED}); vazio se o
     *         id não existe
     * @throws IllegalStateException se a execução existe mas não está
     *         {@code FAILED} (cancelada foi decisão explícita; os demais
     *         estados têm dono — o motor) ou pertence a um job aposentado
     *         (a linha rearmada nunca seria reivindicada — ADR-0033)
     */
    Optional<Execution> retry(ExecutionId executionId);

    Optional<JobSnapshot> findJob(JobKey jobKey);

    /** Todos os jobs registrados — cardinalidade limitada (definição, não execução), sem paginação. */
    List<JobSnapshot> jobs();

    /**
     * Os nodes do cluster com heartbeat registrado (ADR-0012/0041), mais
     * recente primeiro — cardinalidade limitada (tamanho do cluster + o
     * resíduo que o purge da ADR-0041 ainda não recolheu), sem
     * paginação, como {@link #jobs()}. Morte não é campo: deriva da idade
     * de {@link NodeSnapshot#lastHeartbeatAt()} na leitura; {@code STOPPED}
     * é o único desfecho auto-reportado (shutdown limpo).
     */
    List<NodeSnapshot> nodes();

    /**
     * Os limites de vazão declarados e o saldo corrente do balde de cada um
     * (ADR-0042), por nome — cardinalidade limitada, sem paginação, como
     * {@link #jobs()}. Leitura pura: consultar o saldo não consome token.
     */
    List<RateLimitSnapshot> rateLimits();

    /**
     * Ajusta {@code max}/{@code window} de um limite JÁ declarado, em
     * runtime e cluster-wide — mudança de emergência sob o mesmo contrato
     * de PATCH do {@link #reschedule}: o boot reaplica o valor do código no
     * próximo start sob o default {@code on-conflict: override}. O balde
     * sobrevive ao ajuste (saldo clampado ao novo teto): baixar o limite
     * corta a vazão futura, não devolve o que já foi consumido.
     *
     * @return o limite ajustado, ou vazio se {@code name} não existir — declarar limite novo é ato de boot, não de emergência
     */
    Optional<RateLimitSnapshot> adjustRateLimit(String name, int max, Duration window);

    /** Suspende disparos automáticos; schedule manual continua permitido (espelha o motor). Sem efeito se {@code jobKey} não existir. */
    void pause(JobKey jobKey);

    void resume(JobKey jobKey);

    /**
     * Muda a agenda armazenada do job em runtime (ADR-0036) — mudança de
     * emergência sob o contrato de PATCH runtime: num job {@code ANNOTATION}
     * vale até o próximo boot sob {@code on-conflict=override} (o scanner
     * restaura a versão do código com diff logado); {@code preserve} a
     * mantém; num job {@code PROGRAMMATIC} dura até a aplicação redefinir.
     * O trigger é recomputado do relógio na mesma escrita —
     * {@code ON_DEMAND} desarma a recorrência.
     *
     * @return o snapshot já com a agenda nova; vazio se o job não existe
     *         (ou está aposentado)
     * @throws IllegalArgumentException se a agenda for irrealizável
     *         (ex.: cron sintaticamente válido que nunca dispara)
     */
    Optional<JobSnapshot> reschedule(JobKey jobKey, Schedule schedule);

    /**
     * Tipo real do parâmetro de payload do handler de {@code jobKey}, ou
     * vazio se o job não existir ou o método anotado não declarar payload
     * (só {@code JobContext}, ou nenhum parâmetro). Usado pela REST pra
     * converter o corpo JSON de {@code POST .../schedule} antes de
     * agendar, em vez de persistir um {@code Map} cru que o handler não
     * consegue consumir. Só o scanner de {@code @MohsJob} conhece o tipo:
     * um handler registrado manualmente (caminho interno/de teste do
     * {@code HandlerRegistry}) sem declarar {@code payloadType} é tratado
     * pela REST como job que não aceita payload.
     */
    Optional<Class<?>> payloadType(JobKey jobKey);

    Optional<Execution> findExecution(ExecutionId executionId);

    /**
     * A visão agregada do dashboard ({@code GET /overview}): contagens do
     * trabalho vivo e a vazão terminal da última {@code throughputWindow}
     * — ver {@link OverviewSnapshot} pro contrato (e pro porquê de não
     * haver contagem all-time de estados terminais). Quem escolhe a
     * janela é o chamador: ela é parte da resposta, não política do motor.
     *
     * <p>As contagens saem de leituras independentes, não de um corte
     * transacional — execuções que transitam durante a consulta podem
     * divergir entre os números (read skew, DDIA cap. 7): aceitável para
     * polling, e o preço de um corte serializável aqui seria custo sem
     * benefício.
     */
    OverviewSnapshot overview(Duration throughputWindow);

    /**
     * Até {@code query.limit()} execuções, ordenadas por id (UUIDv7)
     * decrescente — ver {@link ExecutionQuery}. SUMÁRIO: {@code attempts()}
     * volta vazio em listagem (leitura de dashboard — uma query, sem a
     * coluna {@code error} de tamanho arbitrário); o detalhe com attempts
     * é {@link #findExecution}.
     */
    List<Execution> executions(ExecutionQuery query);

    MohsLifecycle lifecycle();
}
