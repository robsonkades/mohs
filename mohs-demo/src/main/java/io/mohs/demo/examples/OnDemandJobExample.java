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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import io.mohs.core.Mohs;
import io.mohs.core.definition.OnDemandJob;
import io.mohs.core.event.Enqueued;
import io.mohs.core.job.JobRef;

/**
 * <b>Scenario 3 — a job with no schedule, invoked by the application.</b> This is the background
 * work of a request: the user clicked "send", and the HTTP response must not wait for SMTP.
 *
 * <p>{@code @OnDemandJob} exposes no trigger attributes at all — there is no firing to miss, so no
 * {@code misfire}, and pausing does not affect manual invocation, so no {@code startPaused}. What
 * it does have is the payload, supplied at invocation time.
 *
 * <h2>Reference the job typed, not by string</h2>
 *
 * <p>{@link #WELCOME_EMAIL} is a {@link JobRef}: it binds the job's id to its payload type at
 * compile time, so passing the wrong payload does not compile. The string overload
 * ({@code mohs.schedule("example-welcome-email", payload)}) exists for callers that only have a
 * name — REST, the dashboard, a dynamic dispatch — and checks the type at runtime, producing a
 * clear error rather than a {@code ClassCastException} inside the handler.
 *
 * <p>Publish the {@code JobRef} as a constant next to the job it names. It is the job's public
 * signature: one place to read what this job is called and what it accepts.
 *
 * <h2>The payload crosses a process boundary</h2>
 *
 * <p>It is serialised to JSON, stored, and deserialised — possibly on another node, possibly after
 * a deploy. So it is data, not an object graph: a record of plain fields and ids, never an entity
 * or anything holding a connection. Treat it as a wire contract, because that is what it is: a
 * payload written before a deploy is read after it.
 *
 * <p>The enqueue is durable before {@link #sendWelcome} returns — the {@link Enqueued} receipt
 * carries an {@code ExecutionId} that already exists in the database. Scheduling inside your own
 * transaction makes the two commit together (the Transactional Outbox pattern), which is why a
 * reaction that must not be lost is an enqueued job and never a listener.
 */
@Component
public class OnDemandJobExample {

    /** The job's public signature: its id and the payload it accepts, checked by the compiler. */
    public static final JobRef<WelcomeEmail> WELCOME_EMAIL =
            JobRef.of("example-welcome-email", WelcomeEmail.class);

    private static final Logger log = LoggerFactory.getLogger(OnDemandJobExample.class);

    private final Mohs mohs;

    public OnDemandJobExample(Mohs mohs) {
        this.mohs = mohs;
    }

    /** A record of plain data — it will be JSON on the way in and on the way out. */
    public record WelcomeEmail(String address, String locale) {
    }

    /**
     * What a caller — a controller, a service, another job — actually writes. {@code now()} means
     * "as soon as a claim can take it", not "on this thread": the call returns as soon as the row
     * is durable.
     */
    public Enqueued sendWelcome(String address, String locale) {
        return mohs.schedule(WELCOME_EMAIL, new WelcomeEmail(address, locale)).now();
    }

    /**
     * The handler. It runs on some node of the cluster, on the {@code io} runner, up to twice
     * ({@code retries = 1} beyond the first attempt) — see {@link RetryAndTimeoutExample} for why
     * that default is not zero.
     */
    @OnDemandJob(id = "example-welcome-email", name = "Welcome e-mail", retries = 1, timeout = "PT30S")
    void sendWelcomeEmail(WelcomeEmail email) {
        log.info("sending the welcome e-mail to {} in {}", email.address(), email.locale());
    }
}
