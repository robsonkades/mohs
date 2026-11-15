package io.mohs.store.jdbc;

import io.mohs.engine.SubmissionStore;

import java.time.Clock;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import io.mohs.core.definition.JobOptions;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.core.job.JobRef;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.engine.HistoryStore;
import io.mohs.engine.InputCodec;
import io.mohs.engine.JobDefinitions;
import io.mohs.engine.JobPayload;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcInputContractTest {
    private static final java.time.Instant NOW = java.time.Instant.parse("2026-09-09T12:00:00Z");
    record BeforeRename(String account, int amount) {}
    record AfterRename(String account, int amount) {}

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void acceptedInputSurvivesACompatibleJavaClassRename(JdbcJobBindingTest.Backend backend) {
        var f = new JdbcOwnershipProtocolTest.Fixture(backend);
        var key = JobKey.of("charge");
        var contract = "charge-input-v1";
        var definition = JobDefinitions.definition(key, JobOptions.defaults(), new OnDemandSpec(), Misfire.IGNORE, false)
                .withInputContract(contract);
        f.jobs.upsert(definition);
        var before = JobRef.of(key.value(), BeforeRename.class, contract);
        var after = JobRef.of(key.value(), AfterRename.class, contract);
        var encoded = InputCodec.JSON.encode(before, new BeforeRename("A", 42));
        var id = ExecutionId.of("01991337-0000-7000-8000-000000000054");
        submissions(backend, f).accept(List.of(execution(id, key, encoded)), null, () -> {});

        var durable = f.history.findPayloads(List.of(id)).rows().get(id).payload();
        assertThat(InputCodec.JSON.decode(durable, after)).isEqualTo(new AfterRename("A", 42));
        assertThat(f.jdbc.queryForObject("SELECT input_contract FROM mohs_execution WHERE execution_id = ?",
                String.class, id.value())).isEqualTo(contract);
        assertThat(f.jobs.find(key).orElseThrow().definition().inputContract()).isEqualTo(contract);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void aRetainedIdentityCannotChangeItsInputContract(JdbcJobBindingTest.Backend backend) {
        var f = new JdbcOwnershipProtocolTest.Fixture(backend);
        var before = f.jobs.find(f.job).orElseThrow();
        assertThatThrownBy(() -> f.jobs.upsert(before.definition().withInputContract("incompatible-v2")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("use a new job ID");
        assertThat(f.jobs.find(f.job)).contains(before);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void acceptanceRejectsADifferentContractBeforeWritingAnyWork(JdbcJobBindingTest.Backend backend) {
        var f = new JdbcOwnershipProtocolTest.Fixture(backend);
        var events = new AtomicInteger();
        var invalid = execution(ExecutionId.of("01991337-0000-7000-8000-000000000055"), f.job, new JobPayload("{}", "other-contract"));
        assertThatThrownBy(() -> submissions(backend, f).accept(List.of(invalid), null, events::incrementAndGet))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("input contract differs");
        assertThat(events).hasValue(0);
        for (String table : List.of("mohs_execution", "mohs_work", "mohs_idempotency")) {
            assertThat(f.jdbc.queryForObject("SELECT COUNT(*) FROM " + table, Long.class)).as(table).isZero();
        }
    }

    private static SubmissionStore.NewExecution execution(ExecutionId id, JobKey key, JobPayload input) {
        return new SubmissionStore.NewExecution(id, key, 0, 20, NOW, NOW, "test", null, "request",
                input, SubmissionStore.Origin.MANUAL, null);
    }

    private static JdbcSubmissionStore submissions(JdbcJobBindingTest.Backend backend, JdbcOwnershipProtocolTest.Fixture f) {
        return new JdbcSubmissionStore(backend.dataSource(), backend.delegate(), java.time.Duration.ofDays(7));
    }
}
