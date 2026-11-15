package io.mohs.engine;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;

/** Local window policy and shared resource charging; the work store owns concurrency capacity. */
final class WorkAdmission {
    private WorkAdmission() {}

    static Set<JobKey> excluded(Map<JobKey, StoredJob> definitions, ExecutionWindowRegistry windows,
            RateLimitStore limits, Instant now) {
        Set<JobKey> excluded = new HashSet<>();
        Map<String, Boolean> rateAvailable = new HashMap<>();
        Map<String, Boolean> windowClosed = new HashMap<>();
        for (var stored : definitions.values()) {
            var definition = stored.definition();
            String window = definition.window();
            String rate = definition.rateLimit();
            if (window != null && windowClosed.computeIfAbsent(window, name -> windows.excludes(name, now))
                    || rate != null && !rateAvailable.computeIfAbsent(rate, name -> limits.available(name) > 0)) {
                excluded.add(definition.key());
            }
        }
        return excluded;
    }

    record Rejected(WorkQueue.ClaimedWork work, String reason) {}
    record Decision(List<WorkQueue.ClaimedWork> admitted, List<Rejected> rejected) {}

    /**
     * Checks windows before consuming tokens, then charges each shared bucket once for this batch.
     * A failed charge rejects its entire resource group. Permits are never refunded after admission;
     * a process failure can consume capacity without invoking the handler.
     */
    static Decision afterClaim(List<WorkQueue.ClaimedWork> claimed, Map<JobKey, StoredJob> definitions,
            ExecutionWindowRegistry windows, RateLimitStore limits, Instant now) {
        Map<ExecutionId, String> rejected = new HashMap<>();
        Map<String, List<WorkQueue.ClaimedWork>> byRate = new LinkedHashMap<>();
        Map<String, Boolean> windowClosed = new HashMap<>();
        for (var work : claimed) {
            var stored = definitions.get(work.jobKey());
            // The dispatcher diagnoses a missing retained definition; it is not a resource rejection.
            if (stored == null) continue;
            var definition = stored.definition();
            String window = definition.window();
            if (window != null && windowClosed.computeIfAbsent(window, name -> windows.excludes(name, now))) {
                rejected.put(work.executionId(), "window-closed");
            } else if (definition.rateLimit() != null) {
                byRate.computeIfAbsent(definition.rateLimit(), _ -> new ArrayList<>()).add(work);
            }
        }
        for (var entry : byRate.entrySet()) {
            var group = entry.getValue();
            int granted = Math.min(group.size(), limits.available(entry.getKey()));
            if (granted > 0 && !limits.charge(entry.getKey(), granted)) granted = 0;
            for (int i = granted; i < group.size(); i++) rejected.put(group.get(i).executionId(), "rate-limit");
        }
        return new Decision(claimed.stream().filter(work -> !rejected.containsKey(work.executionId())).toList(),
                claimed.stream().filter(work -> rejected.containsKey(work.executionId()))
                        .map(work -> new Rejected(work, rejected.get(work.executionId()))).toList());
    }
}
