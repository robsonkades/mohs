package io.mohs.jdbc;

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.event.ExecutionEvent;
import io.mohs.core.event.ExecutionInterceptor;
import io.mohs.core.event.ExecutionListener;
import io.mohs.core.event.Failed;
import io.mohs.core.event.Started;
import io.mohs.core.event.Succeeded;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.execution.JobContext;
import io.mohs.core.job.JobKey;
import io.mohs.engine.Dispatcher;
import io.mohs.engine.HandlerRegistry;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;

class DispatcherTest {

    record Handler() {
    }

    private static final Instant NOW = Instant.parse("2026-08-14T12:00:00Z");

    private DataSource dataSource;
    private MutableClock clock;
    private JdbcTemplate rawJdbcTemplate;
    private JdbcJobStore jobStore;
    private JdbcExecutionStore executionStore;
    private HandlerRegistry handlerRegistry;
    private RecordingListener listener;

    @BeforeEach
    void setUp() {
        dataSource = freshH2DataSource();
        clock = new MutableClock(NOW, ZoneId.of("UTC"));
        rawJdbcTemplate = new JdbcTemplate(dataSource);
        jobStore = new JdbcJobStore(dataSource, clock);
        executionStore = new JdbcExecutionStore(dataSource, clock, JsonMapper.builder().build());
        handlerRegistry = new HandlerRegistry();
        listener = new RecordingListener();
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:dispatcher-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }

    private Dispatcher newDispatcher(List<ExecutionInterceptor> interceptors) {
        return new Dispatcher(executionStore, jobStore, handlerRegistry, clock, interceptors, List.of(listener));
    }

    private Execution seedRunningExecution(String id, String jobKey) {
        jobStore.upsert(JobDefinition.of(jobKey, Handler.class, spec -> spec.onDemand()));
        rawJdbcTemplate.update("""
                INSERT INTO mohs_executions (
                    id, job_key, state, scheduled_at, actor, node_id, lease_expires_at, payload, payload_type, created_at)
                VALUES (?, ?, 'RUNNING', ?, 'test', 'node-a', ?, '{}', 'java.lang.Object', ?)
                """, id, jobKey, JdbcTimestamps.toUtcTimestamp(NOW), JdbcTimestamps.toUtcTimestamp(NOW.plusSeconds(30)), JdbcTimestamps.toUtcTimestamp(NOW));
        return executionStore.find(ExecutionId.of(id)).orElseThrow();
    }

    private ExecutionState stateOf(String id) {
        return ExecutionState.valueOf(rawJdbcTemplate.queryForObject(
                "SELECT state FROM mohs_executions WHERE id = ?", String.class, id));
    }

    @Test
    void dispatchInvokesTheHandlerAndRecordsSuccess() throws Exception {
        Execution execution = seedRunningExecution("exec-1", "welcome-email");
        List<Object> received = new CopyOnWriteArrayList<>();
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> received.add(payload));

        newDispatcher(List.of()).dispatch(execution, "hello");

        assertThat(received).containsExactly("hello");
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
        Execution found = executionStore.find(ExecutionId.of("exec-1")).orElseThrow();
        assertThat(found.attempts()).hasSize(1);
        assertThat(found.attempts().get(0).outcome()).isEqualTo(ExecutionState.SUCCEEDED);
        assertThat(found.firedAt()).isEqualTo(NOW);
        assertThat(listener.awaitEvent(Succeeded.class)).isNotNull();
    }

    @Test
    void dispatchRecordsTerminalFailureWhenHandlerThrows() throws Exception {
        Execution execution = seedRunningExecution("exec-1", "welcome-email");
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            throw new IllegalStateException("boom");
        });

        newDispatcher(List.of()).dispatch(execution, "hello");

        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
        Execution found = executionStore.find(ExecutionId.of("exec-1")).orElseThrow();
        assertThat(found.attempts().get(0).outcome()).isEqualTo(ExecutionState.FAILED);
        assertThat(found.attempts().get(0).error()).isEqualTo("boom");
        Failed event = listener.awaitEvent(Failed.class);
        assertThat(event.attemptsExhausted()).isTrue();
        assertThat(event.error()).hasMessage("boom");
    }

    @Test
    void dispatchFailsTerminallyWhenNoHandlerIsRegistered() {
        Execution execution = seedRunningExecution("exec-1", "welcome-email");

        newDispatcher(List.of()).dispatch(execution, "hello");

        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
        Execution found = executionStore.find(ExecutionId.of("exec-1")).orElseThrow();
        assertThat(found.attempts().get(0).error()).contains("no handler registered").contains("welcome-email");
    }

    /** ExecutionInterceptor's own Javadoc: sua exceção É falha de attempt, mesmo tratamento que exceção do handler. */
    @Test
    void interceptorExceptionCountsAsAttemptFailure() {
        Execution execution = seedRunningExecution("exec-1", "welcome-email");
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
        });
        ExecutionInterceptor failingInterceptor = (ctx, chain) -> {
            throw new RuntimeException("interceptor blew up");
        };

        newDispatcher(List.of(failingInterceptor)).dispatch(execution, "hello");

        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
        Execution found = executionStore.find(ExecutionId.of("exec-1")).orElseThrow();
        assertThat(found.attempts().get(0).error()).isEqualTo("interceptor blew up");
    }

    @Test
    void interceptorsRunInOrderAroundTheHandler() throws Exception {
        Execution execution = seedRunningExecution("exec-1", "welcome-email");
        List<String> order = new ArrayList<>();
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> order.add("handler"));
        ExecutionInterceptor first = (ctx, chain) -> {
            order.add("first-before");
            chain.proceed();
            order.add("first-after");
        };
        ExecutionInterceptor second = (ctx, chain) -> {
            order.add("second-before");
            chain.proceed();
            order.add("second-after");
        };

        newDispatcher(List.of(first, second)).dispatch(execution, "hello");

        assertThat(order).containsExactly("first-before", "second-before", "handler", "second-after", "first-after");
    }

    @Test
    void listenerExceptionNeverAffectsTheOutcome() throws Exception {
        Execution execution = seedRunningExecution("exec-1", "welcome-email");
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
        });
        ExecutionListener throwingListener = event -> {
            throw new RuntimeException("listener blew up");
        };
        Dispatcher dispatcher = new Dispatcher(executionStore, jobStore, handlerRegistry, clock, List.of(),
                List.of(throwingListener, listener));

        dispatcher.dispatch(execution, "hello");

        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
        assertThat(listener.awaitEvent(Succeeded.class)).isNotNull();
    }

    private static final class RecordingListener implements ExecutionListener {
        private final List<ExecutionEvent> events = new CopyOnWriteArrayList<>();
        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void on(ExecutionEvent event) {
            events.add(event);
            if (!(event instanceof Started)) {
                latch.countDown();
            }
        }

        <T extends ExecutionEvent> T awaitEvent(Class<T> type) throws InterruptedException {
            assertThat(latch.await(5, TimeUnit.SECONDS)).as("terminal event within timeout").isTrue();
            return events.stream().filter(type::isInstance).map(type::cast).findFirst()
                    .orElseThrow(() -> new AssertionError("no event of type " + type + " among " + events));
        }
    }
}
