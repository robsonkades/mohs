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

import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import io.mohs.core.Batch;
import io.mohs.core.BatchSnapshot;
import io.mohs.core.Mohs;
import io.mohs.core.definition.OnDemandJob;
import io.mohs.core.job.JobRef;

/**
 * <b>Scenario 8 — many jobs scheduled as one unit, with a completion you can observe.</b>
 *
 * <p>"Import this file" is not one job when the file has 10,000 rows: it is 10,000 independent
 * executions that fail, retry and succeed on their own, plus one question the operator keeps
 * asking — <i>is it done?</i> A batch answers that question without anybody counting rows.
 *
 * <h2>What the batch guarantees</h2>
 *
 * <ul>
 * <li><b>All or nothing at creation.</b> Every member is validated before any write, and the batch
 *     row plus its members enter in a single transaction. An exception from {@link Mohs#batch}
 *     guarantees nothing was persisted, so the call can simply be repeated.</li>
 * <li><b>The total is fixed at creation</b>, and each member is born already carrying the
 *     {@code batchId} — which is what makes its completion count towards the batch.</li>
 * <li><b>An empty batch is refused.</b> With no members it would never complete, and a
 *     forever-open batch is worse than an error.</li>
 * <li><b>The name is persisted.</b> It appears in {@link BatchSnapshot}, in the completion event
 *     and in {@code GET /batches/{id}} — so an operator at 3 a.m. sees "nightly-import" and not
 *     only a UUID.</li>
 * </ul>
 *
 * <p>Members are ordinary executions: independent, retried independently, and dispatched wherever
 * there is capacity. A batch is a counter over a set, not a transaction over their work — one
 * member failing does not roll back the others.
 *
 * <h2>Reading the outcome</h2>
 *
 * <p>{@link Batch#onCompletion} is a convenience notification, not a guarantee: it is delivered
 * best-effort, in the same spirit as {@link io.mohs.core.event.ExecutionListener}. If something MUST
 * happen when the batch closes, enqueue a job for it. {@link Mohs#findBatch} is the pull side, and
 * it is cheap at any size: the counter is maintained rather than aggregated over the members.
 */
@Component
public class BatchExample {

    /**
     * The typed reference to the row-import handler.
     */
    public static final JobRef<ImportRow> IMPORT_ROW = JobRef.of("example-import-row", ImportRow.class);

    private static final Logger log = LoggerFactory.getLogger(BatchExample.class);

    private final Mohs mohs;

    /**
     * Creates a {@code BatchExample} with the supplied values.
     *
     * @param mohs the scheduling and operations facade
     */
    public BatchExample(Mohs mohs) {
        this.mohs = mohs;
    }

    /**
     * One row submitted to the import batch.
     *
     * @param lineNumber the one-based input row number
     * @param csv the CSV row contents
     */
    public record ImportRow(int lineNumber, String csv) {
    }

    /**
     * One member per row, one receipt for the whole thing. The {@code batchId} is durable by the
     * time {@link Mohs#batch} returns — hand it back to whoever uploaded the file, and they can
     * poll {@link #progressOf} with it.
     *
     * @param lines the input rows to import
     * @return the identity of the scheduled import batch
     */
    public String importFile(List<String> lines) {
        Batch batch = mohs.batch("nightly-import", members -> {
            for (int line = 0; line < lines.size(); line++) {
                members.add(IMPORT_ROW, new ImportRow(line + 1, lines.get(line)));
            }
        });

        // Each call registers an independent callback; it never replaces the previous one.
        batch.onCompletion(completed -> log.info("batch {} ({}) finished: {} succeeded, {} failed of {}",
                completed.batchId(), completed.name(), completed.succeeded(), completed.failed(), completed.total()));

        return batch.batchId();
    }

    /**
     * The pull side of the same question. {@link BatchSnapshot#pending()} and
     * {@link BatchSnapshot#completed()} are derived from the counters, so a progress bar costs one
     * cheap read no matter how many members there are.
     *
     * @param batchId the identity of the batch
     * @return the batch snapshot, or empty when the batch does not exist
     */
    public Optional<BatchSnapshot> progressOf(String batchId) {
        return mohs.findBatch(batchId);
    }

    /**
     * A member is an ordinary job — nothing about this handler knows it is part of a batch.
     *
     * <p>One consequence worth knowing: a FAILED batch member cannot be manually retried through
     * {@link Mohs#retry}, because the batch already counted that failure and counting it again
     * would close the batch early. Redoing the work means scheduling the job standalone.
     */
    @OnDemandJob(id = "example-import-row", name = "Import one row", retries = 3)
    void importRow(ImportRow row) {
        log.info("importing line {}", row.lineNumber());
    }
}
