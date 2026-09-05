/*
 * Copyright 2026 The Mohs Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mohs.demo.examples;

import java.time.Duration;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

import org.springframework.stereotype.Component;

import io.mohs.core.EngineState;
import io.mohs.core.ExecutionQuery;
import io.mohs.core.JobSnapshot;
import io.mohs.core.Mohs;
import io.mohs.core.MohsLifecycle;
import io.mohs.core.NodeSnapshot;
import io.mohs.core.OverviewSnapshot;
import io.mohs.core.RunnerSnapshot;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.CronSpec;

/**
 * <b>Scenario 16 — running the thing in production.</b>
 *
 * <p>Everything here is also an endpoint of the REST API ({@code mohs.api.enabled=true}) and a
 * button on the dashboard. The Java facade is the same surface: whatever an operator can do from a
 * screen, an application can do from code — which is what makes an admin page, a runbook script or
 * an incident automation possible without reaching into the engine.
 *
 * <h2>Pause is not stop, and drain is not cancel</h2>
 *
 * <p>Three different verbs that people reach for during an incident, and they do different things:
 *
 * <table border="1">
 * <caption>Scope of each control</caption>
 * <tr><th>Control</th><th>Scope</th><th>Effect</th></tr>
 * <tr><td>{@link Mohs#pause}</td><td>one job, the whole cluster</td><td>automatic firings stop; manual scheduling still works</td></tr>
 * <tr><td>{@link MohsLifecycle#pause()}</td><td>this node's engine</td><td>this node stops claiming; its peers carry on</td></tr>
 * <tr><td>{@link MohsLifecycle#drain(Duration)}</td><td>this node's engine</td><td>stop claiming, wait for in-flight work — a drain is not a cancel</td></tr>
 * </table>
 *
 * <h2>Cancel and retry are honest about the race</h2>
 *
 * <p>{@link Mohs#cancel} on a pending execution is immediate. On a RUNNING one it is a cooperative
 * REQUEST: the owning node observes it within at most one poll interval and the handler decides when
 * to stop (through {@link io.mohs.core.execution.JobContext#cancellationRequested()}). A completion
 * may win that race, and if it does, it stands — the returned execution is the state right after the
 * request, not necessarily a terminal one.
 *
 * <p>{@link Mohs#retry} rearms a FAILED execution as due now, bypassing the retry budget on purpose:
 * the budget protects the system from automatic loops, while this is an operator's deliberate
 * decision. It refuses a batch member — the batch already counted that failure, and counting it
 * twice would close the batch early.
 */
@Component
public class OperationsExample {

    private final Mohs mohs;

    /**
     * Creates a {@code OperationsExample} with the supplied values.
     *
     * @param mohs the scheduling and operations facade
     */
    public OperationsExample(Mohs mohs) {
        this.mohs = mohs;
    }

    /**
     * Stop a misbehaving job cluster-wide without a deploy. It suspends automatic firings only —
     * manual scheduling still works, mirroring the engine, so a fix can be tested on one execution
     * while the schedule stays disarmed.
     *
     * @param jobId the stable identity of the job
     */
    public void pause(String jobId) {
        mohs.pause(JobKey.of(jobId));
    }

    /**
     * Resumes automatic firings for the example job.
     *
     * @param jobId the stable identity of the job
     */
    public void resume(String jobId) {
        mohs.resume(JobKey.of(jobId));
    }

    /**
     * Change a schedule at runtime — the emergency knob when a nightly job is running into the
     * morning peak. The next firing is recomputed from the clock in the same write.
     *
     * <p>It is a PATCH, not a redefinition: on an annotated job it holds until the next boot, when
     * the scanner restores the code's version and logs the diff (under the default
     * {@code on-conflict=override}); {@code preserve} keeps the patched version instead. Fix the
     * annotation too, or the change disappears at the next deploy.
     *
     * @param jobId the stable identity of the job
     * @param zone the time zone used to evaluate the schedule
     * @return the rescheduled job snapshot, or empty when the job does not exist
     */
    public Optional<JobSnapshot> moveToMidnight(String jobId, ZoneId zone) {
        return mohs.reschedule(JobKey.of(jobId), new CronSpec("0 0 0 * * *", zone));
    }

    /**
     * Cooperative, never guaranteed — see this class's Javadoc on the race.
     *
     * @param executionId the identity of the execution
     * @return the execution after the cancellation request, or empty when absent
     */
    public Optional<Execution> cancel(String executionId) {
        return mohs.cancel(ExecutionId.of(executionId));
    }

    /**
     * An operator's decision to try a FAILED execution once more, outside the budget.
     *
     * @param executionId the identity of the execution
     * @return the execution after the retry request, or empty when absent
     */
    public Optional<Execution> retry(String executionId) {
        return mohs.retry(ExecutionId.of(executionId));
    }

    /**
     * Every registered job. Bounded cardinality — a definition, not an execution — so there is no
     * pagination, and {@link JobSnapshot} carries the operational state ({@code paused},
     * {@code nextFireAt}) that the definition itself does not.
     *
     * @return the registered job snapshots
     */
    public List<JobSnapshot> allJobs() {
        return mohs.jobs();
    }

    /**
     * Executions are unbounded, so this one paginates: descending id (UUIDv7, chronological), and
     * {@code cursor} is the last id of the previous page.
     *
     * <p>A listing is a SUMMARY — {@code attempts()} comes back empty, because a dashboard read must
     * not drag an arbitrarily large {@code error} column per row. {@link Mohs#findExecution} is the
     * detail view, attempts included.
     *
     * @param jobId the stable identity of the job
     * @param limit the maximum number of results in one batch
     * @return up to the requested limit of failed executions
     */
    public List<Execution> recentFailures(String jobId, int limit) {
        return mohs.executions(new ExecutionQuery(JobKey.of(jobId), ExecutionState.FAILED, null, null, null, limit));
    }

    /**
     * Looks up an execution and its recorded attempts.
     *
     * @param executionId the identity of the execution
     * @return the execution with its attempts, or empty when absent
     */
    public Optional<Execution> detail(String executionId) {
        return mohs.findExecution(ExecutionId.of(executionId));
    }

    /**
     * The dashboard's aggregate: live work by state plus terminal throughput over a window the
     * CALLER chooses. The counts come from independent reads rather than one transactional cut, so
     * executions in transit may disagree between the numbers — acceptable for polling, and a
     * serialisable cut here would be cost without benefit.
     *
     * @return the current execution counts and throughput
     */
    public OverviewSnapshot overview() {
        return mohs.overview(Duration.ofMinutes(5));
    }

    /**
     * The cluster as it sees itself. Death is not a field: it is derived from the age of
     * {@link NodeSnapshot#lastHeartbeatAt()} at read time — {@code STOPPED} is the only
     * self-reported outcome, because a clean shutdown is the only ending a node gets to announce.
     *
     * @return the cluster node heartbeat snapshots
     */
    public List<NodeSnapshot> cluster() {
        return mohs.nodes();
    }

    /**
     * This node's runners and their current occupancy. Unlike {@link #cluster()}, it touches no
     * database and does not see the cluster — a thread pool belongs to a process.
     *
     * @return the local runner configurations and occupancy
     */
    public List<RunnerSnapshot> localRunners() {
        return mohs.runners();
    }

    /**
     * The engine's lifecycle on THIS node, which is a different axis from pausing a job.
     *
     * <p>With {@code mohs.lifecycle.start-mode=manual} the engine stays stopped until
     * {@link MohsLifecycle#start()} — useful when the application must warm caches or await a leader
     * election before it is willing to run anything.
     *
     * <p>{@link MohsLifecycle#drain(Duration)} is what a rolling update does: stop claiming, let
     * in-flight work finish inside the grace, and only then escalate. Spring's shutdown already
     * calls the equivalent with {@code mohs.lifecycle.shutdown.grace-period}, so this is for the
     * cases the container's lifecycle does not cover.
     *
     * @param grace the maximum time allowed for in-flight work to finish
     * @return the state after draining the local engine
     */
    public EngineState drainThisNode(Duration grace) {
        MohsLifecycle lifecycle = mohs.lifecycle();
        lifecycle.drain(grace);
        return lifecycle.state();
    }
}
