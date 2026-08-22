package io.mohs.test;

import java.time.Clock;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import io.mohs.core.definition.DefinitionSource;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.Schedule;
import io.mohs.engine.JobStore;
import io.mohs.engine.NextFireCalculator;
import io.mohs.engine.StoredJob;

/**
 * {@link JobStore} em memória — a implementação "storage em memória" do
 * test kit (§5.14 do documento mestre, {@code @MohsTest}). Mesma
 * precisão definicional×operacional da ADR-0006 que {@code JdbcJobStore}
 * (io.mohs.store.jdbc), sem tocar banco nenhum — prova que {@link JobStore}
 * não vazou nada específico de JDBC.
 *
 * <p>O construtor com {@link Clock} existe pro estado do trigger
 * (ADR-0035): o upsert inicializa {@code nextFireAt} com o mesmo relógio
 * determinístico do teste ({@link MutableClock}); o sem argumento usa o
 * relógio do sistema — conveniência pra teste que não exercita agenda.
 *
 * <p>Divergências deliberadas do adapter JDBC, documentadas: (1) a cura
 * de trigger desarmado não é implementada — sem {@code TriggerFirer} nem
 * {@code ExecutionStore} em memória, trigger recorrente desarmado é
 * estado inalcançável neste store, e a cura seria código morto; ela
 * entra junto do engine in-memory do kit (§5.14). (2) {@link #remove} é
 * hard-delete (o JDBC faz soft-retire preservando a linha) — logo, aqui
 * "ressurreição" pós-remove é um nascimento e {@code startPaused}
 * re-aplica, enquanto no JDBC a linha é a memória e {@code paused} volta
 * como o operador o deixou (ADR-0037).
 */
public final class InMemoryJobStore implements JobStore {

    private final Map<JobKey, StoredJob> jobs = new ConcurrentHashMap<>();
    private final Clock clock;
    private final NextFireCalculator nextFireCalculator = new NextFireCalculator();

    public InMemoryJobStore() {
        this(Clock.systemUTC());
    }

    public InMemoryJobStore(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * {@code orphaned} sempre limpa aqui, ao contrário de {@code paused}
     * (preservado): diferente de pause (decisão de operador), orphaned é
     * dedução do sistema ("a anotação sumiu") — o próprio upsert acontecer
     * já é prova de que uma fonte real quer este job de novo.
     * {@code nextFireAt} segue a regra da ADR-0035: agenda inalterada
     * preserva; nova ou alterada recalcula do relógio.
     */
    @Override
    public JobDefinition upsert(JobDefinition definition) {
        Objects.requireNonNull(definition, "definition");
        jobs.compute(definition.key(), (_, existing) -> {
            // ADR-0037: startPaused só vale no nascimento — reupsert preserva a decisão do operador
            boolean paused = existing != null ? existing.paused() : definition.startPaused();
            int runningExecutionCount = existing != null ? existing.runningExecutionCount() : 0;
            Instant nextFireAt = existing != null && existing.definition().schedule().equals(definition.schedule())
                    ? existing.nextFireAt()
                    : nextFireCalculator.nextFireAfter(definition.schedule(), clock.instant()).orElse(null);
            return new StoredJob(definition, false, paused, runningExecutionCount, nextFireAt);
        });
        return definition;
    }

    @Override
    public Optional<StoredJob> find(JobKey key) {
        Objects.requireNonNull(key, "key");
        return Optional.ofNullable(jobs.get(key));
    }

    @Override
    public Stream<StoredJob> findAll() {
        return jobs.values().stream();
    }

    @Override
    public Stream<StoredJob> findAllAnnotationSourced() {
        return jobs.values().stream().filter(stored -> stored.definition().source() == DefinitionSource.ANNOTATION);
    }

    @Override
    public List<StoredJob> findDueRecurring(Instant now, int limit) {
        Objects.requireNonNull(now, "now");
        return jobs.values().stream()
                .filter(stored -> !stored.paused() && !stored.orphaned())
                .filter(stored -> stored.nextFireAt() != null && !stored.nextFireAt().isAfter(now))
                .sorted(Comparator.comparing(StoredJob::nextFireAt))
                .limit(limit)
                .toList();
    }

    @Override
    public void armNextFire(JobKey key, Instant nextFireAt) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(nextFireAt, "nextFireAt");
        jobs.computeIfPresent(key, (_, stored) -> stored.nextFireAt() != null
                ? stored
                : new StoredJob(stored.definition(), stored.orphaned(), stored.paused(), stored.runningExecutionCount(), nextFireAt));
    }

    @Override
    public boolean reschedule(JobKey key, Schedule schedule) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(schedule, "schedule");
        // o remapeamento nunca devolve null, então computeIfPresent não-null ⟺ o job existia
        return jobs.computeIfPresent(key, (_, stored) -> {
            JobDefinition current = stored.definition();
            JobDefinition redefined = new JobDefinition(current.key(), current.name(), current.handlerType(), schedule,
                    current.runner(), current.window(), current.rateLimit(), current.misfire(), current.startPaused(),
                    current.allowConcurrentExecutions(), current.maxConcurrentExecutions(), current.retries(),
                    current.timeout(), current.retryPolicy(), current.source());
            Instant nextFireAt = nextFireCalculator.nextFireAfter(schedule, clock.instant()).orElse(null);
            return new StoredJob(redefined, stored.orphaned(), stored.paused(), stored.runningExecutionCount(), nextFireAt);
        }) != null;
    }

    @Override
    public void markOrphaned(JobKey key) {
        jobs.computeIfPresent(key, (_, stored) ->
                new StoredJob(stored.definition(), true, stored.paused(), stored.runningExecutionCount(), stored.nextFireAt()));
    }

    @Override
    public void pause(JobKey key) {
        jobs.computeIfPresent(key, (_, stored) ->
                new StoredJob(stored.definition(), stored.orphaned(), true, stored.runningExecutionCount(), stored.nextFireAt()));
    }

    @Override
    public void resume(JobKey key) {
        jobs.computeIfPresent(key, (_, stored) ->
                new StoredJob(stored.definition(), stored.orphaned(), false, stored.runningExecutionCount(), stored.nextFireAt()));
    }

    @Override
    public void remove(JobKey key) {
        jobs.remove(key);
    }

    @Override
    public boolean tryIncrementRunningExecutions(JobKey key) {
        Objects.requireNonNull(key, "key");
        boolean[] acquired = {false};
        jobs.computeIfPresent(key, (_, stored) -> {
            if (stored.runningExecutionCount() >= stored.definition().maxConcurrentExecutions()) {
                return stored;
            }
            acquired[0] = true;
            return new StoredJob(stored.definition(), stored.orphaned(), stored.paused(), stored.runningExecutionCount() + 1, stored.nextFireAt());
        });
        return acquired[0];
    }

    @Override
    public void decrementRunningExecutions(JobKey key) {
        Objects.requireNonNull(key, "key");
        jobs.computeIfPresent(key, (_, stored) -> stored.runningExecutionCount() <= 0
                ? stored
                : new StoredJob(stored.definition(), stored.orphaned(), stored.paused(), stored.runningExecutionCount() - 1, stored.nextFireAt()));
    }

    /** Mesmo piso em zero do bloco JDBC (ADR-0047). */
    @Override
    public void decrementRunningExecutions(JobKey key, int permits) {
        Objects.requireNonNull(key, "key");
        if (permits < 0) {
            throw new IllegalArgumentException("permits must be >= 0, got " + permits);
        }
        jobs.computeIfPresent(key, (_, stored) -> new StoredJob(stored.definition(), stored.orphaned(), stored.paused(),
                Math.max(stored.runningExecutionCount() - permits, 0), stored.nextFireAt()));
    }
}
