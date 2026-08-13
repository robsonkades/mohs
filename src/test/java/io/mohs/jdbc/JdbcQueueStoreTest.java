package io.mohs.jdbc;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import io.mohs.core.resource.JobQueue;
import io.mohs.engine.StoredQueue;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcQueueStoreTest {

    private JdbcQueueStore store;

    @BeforeEach
    void setUp() {
        store = new JdbcQueueStore(freshH2DataSource());
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:queue-store-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema.sql")).execute(h2);
        return h2;
    }

    @Test
    void upsertInsertsANewQueueWithZeroRunningCount() {
        store.upsert(JobQueue.named("emails").maxConcurrent(10).build());

        StoredQueue stored = store.find("emails").orElseThrow();

        assertThat(stored.queue()).isEqualTo(JobQueue.named("emails").maxConcurrent(10).build());
        assertThat(stored.runningCount()).isZero();
    }

    @Test
    void upsertOnExistingNameAppliesDefinitionalChanges() {
        store.upsert(JobQueue.named("emails").maxConcurrent(10).build());

        store.upsert(JobQueue.named("emails").maxConcurrent(25).build());

        assertThat(store.find("emails")).map(StoredQueue::queue)
                .contains(JobQueue.named("emails").maxConcurrent(25).build());
    }

    @Test
    void findReturnsEmptyForUnknownName() {
        assertThat(store.find("ghost")).isEmpty();
    }

    @Test
    void findAllReturnsEveryStoredQueue() {
        store.upsert(JobQueue.named("emails").maxConcurrent(10).build());
        store.upsert(JobQueue.named("sms").maxConcurrent(5).build());

        try (var all = store.findAll()) {
            assertThat(all.map(sq -> sq.queue().name())).containsExactlyInAnyOrder("emails", "sms");
        }
    }

    @Test
    void tryIncrementRunningReservesASlotWhenBelowLimit() {
        store.upsert(JobQueue.named("emails").maxConcurrent(2).build());

        assertThat(store.tryIncrementRunning("emails")).isTrue();

        assertThat(store.find("emails")).map(StoredQueue::runningCount).contains(1);
    }

    @Test
    void tryIncrementRunningFailsWhenAtLimit() {
        store.upsert(JobQueue.named("emails").maxConcurrent(1).build());
        assertThat(store.tryIncrementRunning("emails")).isTrue();

        assertThat(store.tryIncrementRunning("emails")).isFalse();

        assertThat(store.find("emails")).map(StoredQueue::runningCount).contains(1);
    }

    @Test
    void tryIncrementRunningIsAtomicUnderConcurrentContention() throws InterruptedException {
        store.upsert(JobQueue.named("emails").maxConcurrent(10).build());
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        AtomicInteger accepted = new AtomicInteger();

        IntStream.range(0, 100).forEach(i -> executor.submit(() -> {
            if (store.tryIncrementRunning("emails")) {
                accepted.incrementAndGet();
            }
        }));
        executor.shutdown();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        assertThat(accepted.get()).isEqualTo(10);
        assertThat(store.find("emails")).map(StoredQueue::runningCount).contains(10);
    }

    @Test
    void decrementRunningReleasesAReservedSlot() {
        store.upsert(JobQueue.named("emails").maxConcurrent(2).build());
        store.tryIncrementRunning("emails");

        store.decrementRunning("emails");

        assertThat(store.find("emails")).map(StoredQueue::runningCount).contains(0);
    }

    /**
     * CONC-2: dois nós registrando a mesma queue nova ao mesmo tempo podem
     * os dois ver 0 linhas no UPDATE e os dois tentar INSERT — antes da
     * correção, um dos dois propagava DuplicateKeyException pro bootstrap.
     */
    @Test
    void upsertHandlesConcurrentFirstTimeInsertWithoutThrowing() throws Exception {
        JobQueue queue = JobQueue.named("emails").maxConcurrent(5).build();
        CyclicBarrier barrier = new CyclicBarrier(2);
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        Callable<JobQueue> upsert = () -> {
            barrier.await();
            return store.upsert(queue);
        };

        Future<JobQueue> futureA = executor.submit(upsert);
        Future<JobQueue> futureB = executor.submit(upsert);
        futureA.get(10, TimeUnit.SECONDS);
        futureB.get(10, TimeUnit.SECONDS);
        executor.shutdown();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        assertThat(store.find("emails")).map(sq -> sq.queue().maxConcurrent()).contains(5);
    }

    @Test
    void decrementRunningNeverGoesBelowZero() {
        store.upsert(JobQueue.named("emails").maxConcurrent(2).build());

        store.decrementRunning("emails");

        assertThat(store.find("emails")).map(StoredQueue::runningCount).contains(0);
    }
}
