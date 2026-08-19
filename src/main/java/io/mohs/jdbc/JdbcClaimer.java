package io.mohs.jdbc;

import java.io.Serial;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.engine.Claimer;
import io.mohs.engine.ExecutionStore;
import io.mohs.engine.ExecutionWindowRegistry;
import io.mohs.engine.JobStore;
import io.mohs.engine.RateLimitStore;
import io.mohs.jdbc.dialect.Candidate;
import io.mohs.jdbc.dialect.JdbcDialect;
import io.mohs.jdbc.dialect.SqlServerJdbcDialect;

/**
 * {@link Claimer} sobre {@code mohs_executions}/{@code mohs_job_definitions}
 * (ADR-0016, ADR-0018). Cruza duas tabelas numa única transação própria —
 * {@link TransactionTemplate} sobre um {@link DataSourceTransactionManager}
 * dedicado, já que claim é disparado pelo loop de poll do motor, não por um
 * chamador que já tem transação ativa (diferente do "insert do terminal" de
 * {@link JdbcExecutionStore}, que participa por conveniência — ADR-0003
 * cláusula 4). {@code jobStore} e {@code executionStore} precisam apontar
 * pro mesmo {@code DataSource} passado aqui — é assim que eles enxergam a
 * mesma transação (participação por {@code DataSource}, não por instância
 * de template).
 *
 * <p>A ADR-0018 substitui a ADR-0017: o lock otimista da consulta de
 * candidatos (ver {@link JdbcDialect}) continua existindo, mas só como
 * otimização (reduz quantos candidatos perdedores fazem trabalho à toa)
 * — a garantia de corretude real vem inteiramente de uma cadeia de
 * {@code UPDATE} guardados ({@link JobStore#tryIncrementRunningExecutions},
 * {@link JdbcDialect#transitionToRunning}), cada um atômico por construção
 * porque é uma escrita simples, não uma leitura com lock especializado.
 * Motivo: {@code SELECT ... FOR UPDATE SKIP LOCKED} do H2 2.4.240 tem
 * uma corrida real sob contenção genuína — confirmado empiricamente
 * (duas conexões JDBC cruas, sem nenhum código Spring, disputando a
 * MESMA linha via barrier: ~33% das vezes as duas obtêm o lock). Travar
 * só {@code e} (não mais {@code j}) também resolve o custo que jobs com
 * {@code allowConcurrentExecutions = true} pagavam à toa na ADR-0017. A
 * ADR-0020 generaliza o mutex de "um dono" pra um contador com teto
 * ({@link JobStore#tryIncrementRunningExecutions}/{@link
 * JobStore#decrementRunningExecutions}); a ADR-0021 remove a admissão de
 * queue que cruzava uma terceira tabela aqui. A ADR-0023 extrai a
 * consulta de candidatos (o único ponto sensível a dialeto: {@code
 * LIMIT}/{@code TOP}, {@code SKIP LOCKED}/hint de tabela) pra {@link
 * JdbcDialect} — a garantia de corretude acima vale igual em qualquer
 * dialeto, já que nunca dependeu do lock.
 */
public final class JdbcClaimer implements Claimer {

    private static final Logger log = LoggerFactory.getLogger(JdbcClaimer.class);

    /**
     * SQL Server (sem {@code SKIP LOCKED} de verdade — {@link
     * SqlServerJdbcDialect} usa hints pessimistas) escolhe uma
     * transação "vítima" e a mata sob deadlock genuíno entre dois nós
     * disputando o mesmo mutex de job — comportamento normal e
     * documentado do motor, não um bug daqui (Postgres/MySQL/H2 não
     * precisam disto na prática, mas o retry é inofensivo pra eles —
     * nunca dispara). Reexecuta o claim inteiro do zero (SELECT de novo
     * — a transação abortada não deixa candidatos válidos pra reusar).
     */
    private static final int MAX_DEADLOCK_RETRIES = 3;

    /**
     * Duas transações que deadlockam e retentam no mesmo instante têm alta
     * chance de deadlockar de novo — dezenas de ms de espera é a
     * recomendação da própria Microsoft pro deadlock victim de SQL Server
     * (DBTUNE-13). {@code Thread.sleep} aqui é espera deliberada de
     * operação, não sincronização de teste — desmonta o carrier.
     */
    private static final Duration DEADLOCK_RETRY_BASE_DELAY = Duration.ofMillis(20);
    private static final long DEADLOCK_RETRY_JITTER_MILLIS = 30;

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final JdbcDialect dialect;
    private final Clock clock;
    private final ExecutionStore executionStore;
    private final JobStore jobStore;
    private final Duration leaseTtl;
    private final ExecutionWindowRegistry windowRegistry;
    private final RateLimitStore rateLimitStore;

    public JdbcClaimer(DataSource dataSource, JdbcDialect dialect, Clock clock, ExecutionStore executionStore, JobStore jobStore,
            Duration leaseTtl, ExecutionWindowRegistry windowRegistry, RateLimitStore rateLimitStore) {
        Objects.requireNonNull(dataSource, "dataSource");
        this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        this.transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        // DBTUNE-4: sem isto, o isolamento herdava o default do banco —
        // REPEATABLE READ no MySQL/InnoDB, diferente dos outros 3 dialetos,
        // sem nenhuma linha de código declarando a divergência. A ADR-0018
        // raciocina o CAS guardado em termos de "última escrita vence", que
        // é semântica READ COMMITTED.
        this.transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.dialect = Objects.requireNonNull(dialect, "dialect");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.executionStore = Objects.requireNonNull(executionStore, "executionStore");
        this.jobStore = Objects.requireNonNull(jobStore, "jobStore");
        this.leaseTtl = Objects.requireNonNull(leaseTtl, "leaseTtl");
        this.windowRegistry = Objects.requireNonNull(windowRegistry, "windowRegistry");
        this.rateLimitStore = Objects.requireNonNull(rateLimitStore, "rateLimitStore");
    }

    @Override
    public List<Execution> claim(String nodeId, int batchSize) {
        Objects.requireNonNull(nodeId, "nodeId");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }

        List<String> claimedIds = claimIdsWithDeadlockRetry(nodeId, batchSize);
        if (claimedIds.isEmpty()) {
            return List.of();
        }

        // uma consulta em lote, não uma por id (N+1) — a ordem de retorno de
        // findByIds não é garantida, então reordena pelos ids já ordenados
        // por prioridade/scheduled_at que claimWithinTransaction produziu.
        Map<String, Execution> byId = executionStore.findByIds(claimedIds.stream().map(ExecutionId::of).toList())
                .stream()
                .collect(Collectors.toMap(e -> e.id().value(), Function.identity()));
        return claimedIds.stream()
                .map(id -> {
                    Execution execution = byId.get(id);
                    if (execution == null) {
                        throw new IllegalStateException("claimed execution " + id + " not found after claim — should be unreachable");
                    }
                    return execution;
                })
                .toList();
    }

    private List<String> claimIdsWithDeadlockRetry(String nodeId, int batchSize) {
        // Os DOIS modos de contenção, não só o deadlock: o timeout do lock do
        // balde (ADR-0042) chega como QueryTimeoutException, que é IRMÃ de
        // PessimisticLockingFailureException na hierarquia do Spring, não
        // filha — medido em H2 (SQLState 50200) e Postgres (57014). Sem este
        // segundo ramo, a contenção do balde abortava a transação de claim
        // inteira, jobs SEM limite inclusive, e virava ERROR por tick.
        // Engolir o timeout dentro do charge não seria alternativa: no
        // Postgres o statement cancelado aborta a transação (25P02), então o
        // retry precisa ser de transação nova, que é exatamente este laço.
        TransientDataAccessException lastFailure = null;
        for (int attempt = 0; attempt < MAX_DEADLOCK_RETRIES; attempt++) {
            try {
                // TransactionCallback aqui nunca devolve null (claimWithinTransaction
                // sempre retorna uma lista) — requireNonNull só documenta esse invariante
                // pro @NullMarked, já que o contrato de execute() em si é @Nullable (JAVA-8).
                return Objects.requireNonNull(transactionTemplate.execute(status -> claimWithinTransaction(nodeId, batchSize)));
            } catch (RateLimitChargeFailedException chargeLost) {
                // Ramo PRÓPRIO, não de carona: RateLimitChargeFailedException é
                // IRMÃ de PessimisticLockingFailureException/QueryTimeoutException
                // na hierarquia do Spring (as três descendem de
                // TransientDataAccessException), então o multi-catch acima nunca
                // a pegaria. E, esgotadas as tentativas, ela NÃO propaga: rodada
                // desfeita por contenção de balde é o limitador funcionando
                // (ADR-0042 §7), não falha de tick — propagar viraria
                // "engine tick failed" com pilha em metade das rodadas do regime
                // throttlado, que é o falso alarme que aquela decisão proíbe.
                // Uma retentativa IMEDIATA, sem o backoff de deadlock: as duas
                // contenções têm perfis opostos. Deadlock é raro e espera o par
                // soltar (dezenas de ms ajudam); balde esgotado é o caso NORMAL
                // sob throttling — 27-39% das rodadas, medido — e o que falta
                // não é lock, é saldo, que só volta com o refill. Dormir na
                // thread do tick não muda o veredito e atrasa heartbeat e lease.
                // A retentativa única existe porque ela reexecuta a FASE 1
                // contra o saldo novo, admitindo menos candidatos; além disso, o
                // próprio CAS já retentou 3x dentro do store.
                if (attempt >= 1) {
                    log.debug("round rolled back by rate limit contention ({}) — candidates stay ENQUEUED", chargeLost.getMessage());
                    return List.of();
                }
            } catch (PessimisticLockingFailureException | QueryTimeoutException e) {
                lastFailure = e;
                if (attempt < MAX_DEADLOCK_RETRIES - 1) {
                    backoffBeforeDeadlockRetry();
                }
            }
        }
        throw lastFailure;
    }

    private static void backoffBeforeDeadlockRetry() {
        try {
            Thread.sleep(DEADLOCK_RETRY_BASE_DELAY.plusMillis(ThreadLocalRandom.current().nextLong(DEADLOCK_RETRY_JITTER_MILLIS)));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Janela de exclusão é a primeira guarda e vazão (ADR-0042) é a
     * segunda, ambas antes de reservar qualquer vaga de concorrência:
     * candidato barrado nunca chega a disputar o mutex do job nem o CAS
     * pra {@code RUNNING}. Depois delas o lote se parte em dois
     * (DBTUNE-16): candidato sem mutex de job só precisa do
     * CAS — acumulado e resolvido numa única chamada de {@link
     * JdbcDialect#transitionToRunning} (1 statement no Postgres, N no
     * default); candidato com mutex mantém a cadeia guardada da
     * ADR-0018/0020 ({@link #tryClaimWithJobSlot}), que precisa do
     * veredito individual na hora pra desfazer a reserva do slot em caso
     * de derrota. O resultado é remontado na ordem dos candidatos
     * (prioridade, {@code scheduled_at}) — a mesma que o linha-a-linha
     * produzia.
     */
    private List<String> claimWithinTransaction(String nodeId, int batchSize) {
        Instant now = clock.instant();
        Instant leaseExpiresAt = now.plus(leaseTtl);
        List<Candidate> candidates = selectCandidates(now, batchSize);

        List<Candidate> admitted = admitByRate(withinWindow(candidates, now), now);

        Set<String> claimedIds = new HashSet<>();
        List<String> concurrentAllowedIds = new ArrayList<>(admitted.size());
        for (Candidate candidate : admitted) {
            if (candidate.allowConcurrentExecutions()) {
                concurrentAllowedIds.add(candidate.id());
            } else if (tryClaimWithJobSlot(candidate, nodeId, now, leaseExpiresAt)) {
                claimedIds.add(candidate.id());
            }
        }
        if (!concurrentAllowedIds.isEmpty()) {
            claimedIds.addAll(dialect.transitionToRunning(jdbcTemplate, concurrentAllowedIds, nodeId, now, leaseExpiresAt));
        }
        List<Candidate> claimed = admitted.stream().filter(candidate -> claimedIds.contains(candidate.id())).toList();
        chargeRateLimits(claimed, now);
        return claimed.stream().map(Candidate::id).toList();
    }

    /**
     * FASE 2 da vazão, no fim de tudo de propósito: o lock de linha do balde
     * nasce aqui e morre no commit, então a janela de serialização é a cauda
     * da transação em vez da transação inteira (medido: 2,3× a 4 nós, 3,5× a
     * 8). E cobra o que foi REIVINDICADO, não o que foi admitido — candidato
     * que perdeu o mutex do job não queima token, defeito que o desenho de
     * fase única tinha por construção.
     *
     * <p>Fracasso aqui derruba a rodada inteira, e tem que ser assim: as
     * execuções já estão {@code RUNNING} nesta transação, e entregá-las sem
     * token seria sobre-entrega — a única violação que este limite existe
     * pra impedir. O rollback devolve tudo ao estado anterior, o retry
     * refaz, e o custo é uma rodada de {@code poll-interval}.
     */
    private void chargeRateLimits(List<Candidate> claimed, Instant now) {
        countPerRateLimit(claimed).forEach((limitName, permits) -> {
            if (!rateLimitStore.charge(limitName, permits, now)) {
                throw new RateLimitChargeFailedException(limitName, permits);
            }
        });
    }

    /**
     * Quantos candidatos cada limite deve pagar — {@link TreeMap}, não
     * {@link HashMap}: as duas fases visitam os limites na MESMA ordem de
     * nome em todo nó (lock ordering, JCIP §10.1.1); sem isso, dois nós com
     * lotes que cruzam dois limites em ordens opostas deadlockam na
     * cobrança. Candidato sem limite não entra na conta.
     */
    private static Map<String, Integer> countPerRateLimit(List<Candidate> candidates) {
        Map<String, Integer> perLimit = new TreeMap<>();
        for (Candidate candidate : candidates) {
            String limitName = candidate.rateLimitName();
            if (limitName != null) {
                perLimit.merge(limitName, 1, Integer::sum);
            }
        }
        return perLimit;
    }

    /**
     * Sinaliza rollback da rodada quando o balde não cobre o que já foi
     * reivindicado. {@code TransientDataAccessException} porque é contenção,
     * não defeito — mas isso NÃO a faz cair no multi-catch de
     * {@code PessimisticLockingFailureException}/{@code QueryTimeoutException}:
     * as três são IRMÃS sob {@code TransientDataAccessException}, e irmã não
     * é filha. {@link #claimIdsWithDeadlockRetry} tem um ramo próprio para
     * ela, que retenta e, esgotado, devolve rodada VAZIA em vez de propagar
     * (ADR-0042 §7: limite segurando não é erro de tick).
     */
    static final class RateLimitChargeFailedException extends TransientDataAccessException {

        @Serial
        private static final long serialVersionUID = 1L;

        RateLimitChargeFailedException(String limitName, int permits) {
            super("rate limit '" + limitName + "' could not be charged " + permits
                    + " permit(s) after the claim — another node drained it; rolling the round back");
        }
    }

    private List<Candidate> withinWindow(List<Candidate> candidates, Instant now) {
        return candidates.stream().filter(candidate -> !windowRegistry.excludes(candidate.windowName(), now)).toList();
    }

    /**
     * FASE 1 da vazão (ADR-0042): UMA leitura de saldo por limite distinto
     * do lote, nunca uma por candidato — é a amortização em lote que torna
     * viável um contador cluster-wide (a ADR-0009 reprovou o desenho que
     * escrevia por execução). Leitura pura: nenhum lock nasce aqui, o que é
     * exatamente o ponto da revisão de duas fases; quem cobra é
     * {@link #chargeRateLimits}, no fim.
     *
     * <p>Os limites são lidos na ordem de nome de {@link #countPerRateLimit}
     * — a mesma que a fase 2 herda (lock ordering, JCIP §10.1.1).
     *
     * <p>O saldo lido pode estar velho quando a fase 2 chegar (outro nó
     * cobrou no meio). Isso nunca vira sobre-entrega: o CAS de
     * {@code charge} recusa, a rodada é desfeita e nada é entregue sem
     * token.
     */
    private List<Candidate> admitByRate(List<Candidate> candidates, Instant now) {
        Map<String, Integer> wantedPerLimit = countPerRateLimit(candidates);
        if (wantedPerLimit.isEmpty()) {
            return candidates;
        }

        Map<String, Integer> remainingGrant = grantPerLimit(wantedPerLimit, now);
        List<Candidate> admitted = new ArrayList<>(candidates.size());
        for (Candidate candidate : candidates) {
            String limitName = candidate.rateLimitName();
            if (limitName == null || remainingGrant.merge(limitName, -1, Integer::sum) >= 0) {
                admitted.add(candidate);
            }
        }
        return admitted;
    }

    /** Saldo concedido a cada limite nesta rodada: nunca mais que o pedido, nunca mais que o balde tem. */
    private Map<String, Integer> grantPerLimit(Map<String, Integer> wantedPerLimit, Instant now) {
        Map<String, Integer> grants = new HashMap<>();
        wantedPerLimit.forEach((limitName, wanted) -> {
            int granted = Math.min(rateLimitStore.available(limitName, now), wanted);
            grants.put(limitName, granted);
            // represamento por vazão não vira estado nem falha (ADR-0042 §7) —
            // sem esta linha, a única evidência de "o limite está segurando"
            // seria um monte de ENQUEUED parado e nenhuma pista da causa.
            if (granted < wanted) {
                log.debug("rate limit '{}' held back {} of {} candidate(s) this round — they stay ENQUEUED (ADR-0042)",
                        limitName, wanted - granted, wanted);
            }
        });
        return grants;
    }

    /**
     * Reivindica um candidato com mutex de job através da cadeia completa
     * de guardas atômicas (ADR-0018/0020), desfazendo a reserva do slot se
     * o CAS perder — nunca deixa mutex de job preso por um candidato que
     * no fim não foi reivindicado.
     */
    private boolean tryClaimWithJobSlot(Candidate candidate, String nodeId, Instant now, Instant leaseExpiresAt) {
        JobKey jobKey = JobKey.of(candidate.jobKey());
        if (!jobStore.tryIncrementRunningExecutions(jobKey)) {
            return false;
        }
        boolean claimed = !dialect.transitionToRunning(jdbcTemplate, List.of(candidate.id()), nodeId, now, leaseExpiresAt).isEmpty();
        if (!claimed) {
            jobStore.decrementRunningExecutions(jobKey);
        }
        return claimed;
    }

    private List<Candidate> selectCandidates(Instant now, int batchSize) {
        return dialect.selectCandidates(jdbcTemplate, now, batchSize);
    }
}
