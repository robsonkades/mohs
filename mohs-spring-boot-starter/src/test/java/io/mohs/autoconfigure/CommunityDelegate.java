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
package io.mohs.autoconfigure;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;

import org.jspecify.annotations.Nullable;

import io.mohs.store.jdbc.delegate.H2JdbcDelegate;
import io.mohs.store.jdbc.delegate.JdbcDelegate;

/**
 * A delegate this repository does not ship — the fixture behind
 * {@code MohsAutoConfigurationTest}'s {@code @ConditionalOnMissingBean} and database-time tests.
 *
 * <p>Its shape IS the contract's cost, and that is why it is a file of its own rather than a nested
 * class: since every statement moved into the delegates, a third-party implementation answers for all
 * of its own SQL. Here that is done by delegating to {@link H2JdbcDelegate} — the realistic starting
 * point for a database close to one already supported. Its schema is the operator's to install; Mohs
 * neither creates nor migrates one.
 *
 * <p>{@code nowQuery()} and {@code readNow()} are both delegated, and delegating them TOGETHER is the
 * point: the pair is one decision. This fixture stands in for an author who reached for the nearest
 * supported database and took its clock along with its SQL — which is correct here, and which the
 * compiler forced them to state rather than inherit.
 */
final class CommunityDelegate implements JdbcDelegate {

    private final JdbcDelegate delegate = new H2JdbcDelegate();

    @Override
    public String nowQuery() {
        return delegate.nowQuery();
    }

    @Override
    public Instant readNow(ResultSet rs) throws SQLException {
        return delegate.readNow(rs);
    }

    @Override
    public Object splitTimestamp(Instant instant) {
        return delegate.splitTimestamp(instant);
    }

    @Override
    public @Nullable Instant readSplitTimestamp(ResultSet rs, String column) throws SQLException {
        return delegate.readSplitTimestamp(rs, column);
    }

    @Override
    public String findExecutionPage(String whereClause) {
        return delegate.findExecutionPage(whereClause);
    }

    @Override
    public String upsertJobUpdate(boolean writeNextFire) {
        return delegate.upsertJobUpdate(writeNextFire);
    }

    @Override
    public String readyCandidates() {
        return delegate.readyCandidates();
    }

    @Override
    public String readyCandidatesFiltered() {
        return delegate.readyCandidatesFiltered();
    }

    @Override
    public String readyDelete() {
        return delegate.readyDelete();
    }

    @Override
    public String leaseInsert() {
        return delegate.leaseInsert();
    }

    @Override
    public String readyInsert() {
        return delegate.readyInsert();
    }

    @Override
    public String fencedLeaseDelete() {
        return delegate.fencedLeaseDelete();
    }

    @Override
    public String deleteReadyById() {
        return delegate.deleteReadyById();
    }

    @Override
    public String cancelExecution() {
        return delegate.cancelExecution();
    }

    @Override
    public String findBatchIdByExecution() {
        return delegate.findBatchIdByExecution();
    }

    @Override
    public String rearmExecutionByCas() {
        return delegate.rearmExecutionByCas();
    }

    @Override
    public String rearmReadyFromHistory() {
        return delegate.rearmReadyFromHistory();
    }

    @Override
    public String visibleWorkExists() {
        return delegate.visibleWorkExists();
    }

    @Override
    public String visibleWorkCount() {
        return delegate.visibleWorkCount();
    }

    @Override
    public String recordExecution() {
        return delegate.recordExecution();
    }

    @Override
    public String insertIdempotency() {
        return delegate.insertIdempotency();
    }

    @Override
    public String findExecutionIdByIdempotencyKey() {
        return delegate.findExecutionIdByIdempotencyKey();
    }

    @Override
    public String findPayloads() {
        return delegate.findPayloads();
    }

    @Override
    public String findHeads() {
        return delegate.findHeads();
    }

    @Override
    public String findAttempts() {
        return delegate.findAttempts();
    }

    @Override
    public String pruneIdempotencyBefore() {
        return delegate.pruneIdempotencyBefore();
    }

    @Override
    public String pruneTerminalExecutionsBefore() {
        return delegate.pruneTerminalExecutionsBefore();
    }

    @Override
    public String pruneOrphanedAttemptsBefore() {
        return delegate.pruneOrphanedAttemptsBefore();
    }

    @Override
    public String pruneEmptyBatchesBefore() {
        return delegate.pruneEmptyBatchesBefore();
    }

    @Override
    public String findExecutionById() {
        return delegate.findExecutionById();
    }

    @Override
    public String countActiveInQueue() {
        return delegate.countActiveInQueue();
    }

    @Override
    public String countRunning() {
        return delegate.countRunning();
    }

    @Override
    public String countTerminalOutcomesSince() {
        return delegate.countTerminalOutcomesSince();
    }

    @Override
    public String findLeasesByNodes() {
        return delegate.findLeasesByNodes();
    }

    @Override
    public String findOrphanedLeases() {
        return delegate.findOrphanedLeases();
    }

    @Override
    public String findOrphanedLeasesExceptAlive() {
        return delegate.findOrphanedLeasesExceptAlive();
    }

    @Override
    public String countLeasesByJob() {
        return delegate.countLeasesByJob();
    }

    @Override
    public String requestLeaseCancellation() {
        return delegate.requestLeaseCancellation();
    }

    @Override
    public String findCancelRequestedLeases() {
        return delegate.findCancelRequestedLeases();
    }

    @Override
    public String insertAttempt() {
        return delegate.insertAttempt();
    }

    @Override
    public String terminalStateUpdate() {
        return delegate.terminalStateUpdate();
    }

    @Override
    public String insertJob() {
        return delegate.insertJob();
    }

    @Override
    public String rescheduleJob() {
        return delegate.rescheduleJob();
    }

    @Override
    public String countLiveSchedulerOccurrences() {
        return delegate.countLiveSchedulerOccurrences();
    }

    @Override
    public String cancelDrainedExecutions() {
        return delegate.cancelDrainedExecutions();
    }

    @Override
    public String markJobOrphaned() {
        return delegate.markJobOrphaned();
    }

    @Override
    public String setJobPaused() {
        return delegate.setJobPaused();
    }

    @Override
    public String retireJob() {
        return delegate.retireJob();
    }

    @Override
    public String drainedBatchMembers() {
        return delegate.drainedBatchMembers();
    }

    @Override
    public String countCancelledBatchMembers() {
        return delegate.countCancelledBatchMembers();
    }

    @Override
    public String findTriggerSnapshot() {
        return delegate.findTriggerSnapshot();
    }

    @Override
    public String findJobByKey() {
        return delegate.findJobByKey();
    }

    @Override
    public String findAllJobs() {
        return delegate.findAllJobs();
    }

    @Override
    public String findAllAnnotationSourcedJobs() {
        return delegate.findAllAnnotationSourcedJobs();
    }

    @Override
    public String findDueRecurringJobs() {
        return delegate.findDueRecurringJobs();
    }

    @Override
    public String armNextFire() {
        return delegate.armNextFire();
    }

    @Override
    public String findQueuedExecutionIdsByJob() {
        return delegate.findQueuedExecutionIdsByJob();
    }

    @Override
    public String insertNode() {
        return delegate.insertNode();
    }

    @Override
    public String findAllNodes() {
        return delegate.findAllNodes();
    }

    @Override
    public String deleteHeartbeatsBefore() {
        return delegate.deleteHeartbeatsBefore();
    }

    @Override
    public String heartbeatUpdate() {
        return delegate.heartbeatUpdate();
    }

    @Override
    public String findRateLimitByName() {
        return delegate.findRateLimitByName();
    }

    @Override
    public String findAllRateLimits() {
        return delegate.findAllRateLimits();
    }

    @Override
    public String readRateLimitBucket() {
        return delegate.readRateLimitBucket();
    }

    @Override
    public String updateRateLimitSpec() {
        return delegate.updateRateLimitSpec();
    }

    @Override
    public String insertFullRateLimitBucket() {
        return delegate.insertFullRateLimitBucket();
    }

    @Override
    public String chargeRateLimitByCas() {
        return delegate.chargeRateLimitByCas();
    }

    @Override
    public String insertBatch() {
        return delegate.insertBatch();
    }

    @Override
    public String findBatch() {
        return delegate.findBatch();
    }

    @Override
    public String incrementBatchSucceeded() {
        return delegate.incrementBatchSucceeded();
    }

    @Override
    public String incrementBatchFailed() {
        return delegate.incrementBatchFailed();
    }

    @Override
    public String advanceTriggerByCas() {
        return delegate.advanceTriggerByCas();
    }
}
