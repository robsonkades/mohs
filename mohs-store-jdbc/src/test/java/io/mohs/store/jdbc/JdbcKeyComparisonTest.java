package io.mohs.store.jdbc;

import io.mohs.engine.SubmissionStore;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tools.jackson.databind.json.JsonMapper;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.engine.HistoryStore;
import static org.assertj.core.api.Assertions.assertThat;

class JdbcKeyComparisonTest {
    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void jobAndIdempotencyKeysCompareExactlyLikeJava(JdbcJobBindingTest.Backend backend) {
        var store = new JdbcHistoryStore(backend.dataSource(), backend.delegate());
        var now = Instant.now();
        for (String job : List.of("Alpha", "alpha")) {
            for (String key : List.of("Key", "key")) {
                var executionId = ExecutionId.of(io.mohs.test.TestIds.value(job + "-" + key));
                TestSubmissions.accept(backend.dataSource(), backend.delegate(), List.of(new SubmissionStore.NewExecution(executionId, JobKey.of(job), 0, 20, now, now,
                        "usuário São Paulo", null, key, io.mohs.test.InputFixtures.encode(JobKey.of(job), "input"), SubmissionStore.Origin.MANUAL, null)));
                assertThat(store.findReceiptByIdempotencyKey(JobKey.of(job), key).orElseThrow().executionId()).isEqualTo(executionId);
            }
        }
        assertThat(store.findReceiptByIdempotencyKey(JobKey.of("ALPHA"), "Key")).isEmpty();
        assertThat(store.findReceiptByIdempotencyKey(JobKey.of("Alpha"), "KEY")).isEmpty();
    }
}
