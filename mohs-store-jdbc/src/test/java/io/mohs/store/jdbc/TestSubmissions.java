package io.mohs.store.jdbc;

import io.mohs.engine.SubmissionStore;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import javax.sql.DataSource;
import io.mohs.core.definition.DefinitionSource;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.engine.SubmissionStore.NewExecution;
import io.mohs.store.jdbc.delegate.JdbcDelegate;

/** Declares fixture jobs before exercising real atomic submission in read/receipt tests. */
final class TestSubmissions {
    private TestSubmissions() {}

    static void declare(DataSource source, JdbcDelegate dialect, List<NewExecution> executions) {
        var jobs = new JdbcJobStore(source, Clock.systemUTC(), dialect);
        for (var e : executions) {
            if (jobs.find(e.jobKey()).isEmpty()) {
                jobs.upsert(new JobDefinition(e.jobKey(), null, e.jobKey(), new OnDemandSpec(), null, null, null,
                        Misfire.IGNORE, false, 0, 0, null, null, DefinitionSource.PROGRAMMATIC, e.payload().inputContract()));
            }
        }
    }

    static void accept(DataSource source, JdbcDelegate dialect, List<NewExecution> executions) {
        declare(source, dialect, executions);
        new JdbcSubmissionStore(source, dialect, Duration.ofDays(7)).accept(executions, null, () -> {});
    }
}
