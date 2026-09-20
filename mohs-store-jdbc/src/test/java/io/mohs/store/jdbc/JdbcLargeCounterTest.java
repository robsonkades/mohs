package io.mohs.store.jdbc;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import static org.assertj.core.api.Assertions.assertThat;

class JdbcLargeCounterTest {
    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void countersRemainExactBeyondSignedInt(JdbcJobBindingTest.Backend backend) {
        var store = new JdbcBatchStore(backend.dataSource(), backend.delegate());
        long count = (long) Integer.MAX_VALUE + 10;
        TestWork.batch(backend.dataSource(), backend.delegate(), "01991337-0000-7000-8000-000000000057", "Large batch", count);
        new org.springframework.jdbc.core.JdbcTemplate(backend.dataSource()).update(
                "UPDATE mohs_batches SET succeeded = ? WHERE id = ?", count - 1, "01991337-0000-7000-8000-000000000057");
        var transaction = new org.springframework.transaction.support.TransactionTemplate(
                new org.springframework.jdbc.datasource.DataSourceTransactionManager(backend.dataSource()));
        var completed = transaction.execute(_ -> new JdbcExecutionTransitions(
                new org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate(backend.dataSource()), backend.delegate())
                .completeBatchMembers("01991337-0000-7000-8000-000000000057", 1, 0));
        assertThat(completed.total()).isEqualTo(count);
        assertThat(completed.succeeded()).isEqualTo(count);
        assertThat(completed.pending()).isZero();
        assertThat(store.find("01991337-0000-7000-8000-000000000057")).contains(completed);
    }
}
