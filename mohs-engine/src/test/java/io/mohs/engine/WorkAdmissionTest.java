package io.mohs.engine;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import io.mohs.core.definition.JobOptions;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.OnDemandSpec;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class WorkAdmissionTest {
    private static final Instant NOW = Instant.parse("2026-09-10T00:00:00Z");

    @Test
    void sharedBucketIsChargedOnceAfterWindowsAndPreservesClaimOrder() {
        var windows = mock(ExecutionWindowRegistry.class);
        var limits = mock(RateLimitStore.class);
        when(windows.excludes("closed", NOW)).thenReturn(true);
        when(limits.available("shared")).thenReturn(2);
        when(limits.charge("shared", 2)).thenReturn(true);
        var claimed = List.of(work(1, "a"), work(2, "unlimited"), work(3, "closed"), work(4, "b"), work(5, "c"));
        var definitions = Map.of(JobKey.of("a"), job("a", "shared", null),
                JobKey.of("b"), job("b", "shared", null), JobKey.of("c"), job("c", "shared", null),
                JobKey.of("unlimited"), job("unlimited", null, null), JobKey.of("closed"), job("closed", "shared", "closed"));
        var decision = WorkAdmission.afterClaim(claimed, definitions, windows, limits, NOW);
        assertThat(decision.admitted()).containsExactly(claimed.get(0), claimed.get(1), claimed.get(3));
        assertThat(decision.rejected()).containsExactly(new WorkAdmission.Rejected(claimed.get(2), "window-closed"),
                new WorkAdmission.Rejected(claimed.get(4), "rate-limit"));
        verify(limits).available("shared");
        verify(limits).charge("shared", 2);
        verifyNoMoreInteractions(limits);
    }

    @Test
    void lostChargeRaceRejectsEveryMemberOfTheResourceGroup() {
        var windows = mock(ExecutionWindowRegistry.class);
        var limits = mock(RateLimitStore.class);
        when(limits.available("shared")).thenReturn(5);
        when(limits.charge("shared", 2)).thenReturn(false);
        var claimed = List.of(work(1, "a"), work(2, "b"), work(3, "unlimited"));
        var definitions = Map.of(JobKey.of("a"), job("a", "shared", null),
                JobKey.of("b"), job("b", "shared", null), JobKey.of("unlimited"), job("unlimited", null, null));
        var decision = WorkAdmission.afterClaim(claimed, definitions, windows, limits, NOW);
        assertThat(decision.admitted()).containsExactly(claimed.get(2));
        assertThat(decision.rejected()).extracting(WorkAdmission.Rejected::work).containsExactly(claimed.get(0), claimed.get(1));
    }

    private static WorkQueue.ClaimedWork work(int index, String key) {
        return new WorkQueue.ClaimedWork(ExecutionId.of("01991337-0000-7000-8000-%012d".formatted(index)),
                JobKey.of(key), 1, 20, "01991337-0001-7000-8000-000000000001");
    }

    private static StoredJob job(String key, String rate, String window) {
        return new StoredJob(JobDefinitions.definition(JobKey.of(key),
                new JobOptions(null, null, window, rate, 0, null, null, 0),
                new OnDemandSpec(), Misfire.IGNORE, false), false, null, 1);
    }
}
