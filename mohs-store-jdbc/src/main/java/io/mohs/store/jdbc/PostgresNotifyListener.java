package io.mohs.store.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;

import org.jspecify.annotations.Nullable;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tier 2 do wake-up (§5.5, ADR-G): {@code LISTEN mohs_ready} numa conexão
 * DEDICADA, fora do pool — uma conexão do Hikari presa num LISTEN eterno
 * bloquearia o ciclo de vida do pool ({@code maxLifetime}, leak detection)
 * e roubaria capacidade do hot path. A thread é platform e nomeada
 * ({@code mohs-notify-listener}) pela mesma razão do loop do engine
 * (§12.1): aparece em qualquer thread dump.
 *
 * <p>Best-effort por contrato: queda da conexão degrada pro poll adaptativo
 * sem erro fatal — WARN, reconexão com backoff (1s→30s), e o backstop de
 * correção é o tier 3 (perder notificação é inofensivo por design; por
 * isso NENHUM caminho daqui pode derrubar o engine). O filtro de shard
 * ({@code ownsShard}) evita acordar o loop por trabalho de outro nó;
 * payload que não parse como shard acorda mesmo assim — sinal desconhecido
 * no canal é razão pra olhar a fila, nunca pra ignorar.
 */
public final class PostgresNotifyListener implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PostgresNotifyListener.class);

    private static final String CHANNEL = "mohs_ready";
    private static final int POLL_TIMEOUT_MILLIS = 500;
    private static final long RECONNECT_BACKOFF_START_MILLIS = 1_000;
    private static final long RECONNECT_BACKOFF_MAX_MILLIS = 30_000;

    private final String url;
    private final @Nullable String username;
    private final @Nullable String password;
    private final IntPredicate ownsShard;
    private final Runnable wake;

    /** {@code volatile}: escrito por {@link #close} (thread de shutdown), lido pelo loop — JCIP 3.1. */
    private volatile boolean closed;
    /** O despertador do backoff de reconexão — {@link #close} derruba o latch pra thread não dormir o backoff inteiro. */
    private final CountDownLatch closedLatch = new CountDownLatch(1);
    private volatile @Nullable Thread thread;
    private volatile @Nullable Connection connection;

    public PostgresNotifyListener(String url, @Nullable String username, @Nullable String password,
            IntPredicate ownsShard, Runnable wake) {
        this.url = Objects.requireNonNull(url, "url");
        this.username = username;
        this.password = password;
        this.ownsShard = Objects.requireNonNull(ownsShard, "ownsShard");
        this.wake = Objects.requireNonNull(wake, "wake");
    }

    public void start() {
        Thread listener = Thread.ofPlatform().name("mohs-notify-listener").daemon(true).unstarted(this::runLoop);
        thread = listener;
        listener.start();
    }

    private void runLoop() {
        long backoffMillis = RECONNECT_BACKOFF_START_MILLIS;
        while (!closed) {
            try (Connection listenConnection = DriverManager.getConnection(url, connectionProperties())) {
                connection = listenConnection;
                try (Statement statement = listenConnection.createStatement()) {
                    statement.execute("LISTEN " + CHANNEL);
                }
                log.info("LISTEN {} connected — tier-2 wake-up active (poll remains the correctness backstop)", CHANNEL);
                backoffMillis = RECONNECT_BACKOFF_START_MILLIS;
                pollNotificationsUntilClosed(listenConnection.unwrap(PGConnection.class));
            } catch (SQLException | RuntimeException e) {
                // RuntimeException incluída (JCIP 7.3): thread de serviço de
                // vida longa não deixa um unchecked — do wake, do unwrap, de
                // driver caprichoso — decidir seu destino via uncaught handler
                // (stderr, invisível pro log estruturado); a política é uma só:
                // WARN + backoff + reconectar (review S6.3)
                if (closed) {
                    return;
                }
                log.warn("LISTEN {} loop failed — dispatch degrades to the adaptive poll until reconnect "
                        + "(retrying in {} ms): {}", CHANNEL, backoffMillis, e.toString());
                sleepBeforeReconnect(backoffMillis);
                backoffMillis = Math.min(backoffMillis * 2, RECONNECT_BACKOFF_MAX_MILLIS);
            } finally {
                connection = null;
            }
        }
    }

    /**
     * {@code tcpKeepAlive}: sem ele, conexão morta sem FIN (NAT/firewall
     * idle, failover) deixa o {@code getNotifications} devolvendo null pra
     * sempre — timeout de socket não é erro, e o tier 2 ficaria morto com
     * o log dizendo "active" (review S6.3; DDIA cap. 8 — sem probe, "nada
     * chegou" e "conexão morta" são indistinguíveis). O keepalive do SO
     * detecta em minutos; probe ativo de segundos fica com gatilho
     * registrado no PLAN.md.
     */
    private Properties connectionProperties() {
        Properties properties = new Properties();
        if (username != null) {
            properties.setProperty("user", username);
        }
        if (password != null) {
            properties.setProperty("password", password);
        }
        properties.setProperty("tcpKeepAlive", "true");
        return properties;
    }

    private void pollNotificationsUntilClosed(PGConnection pgConnection) throws SQLException {
        while (!closed) {
            // timeout curto de propósito: é o que dá ao close() uma
            // janela de saída limpa sem depender de interrupt
            PGNotification[] notifications = pgConnection.getNotifications(POLL_TIMEOUT_MILLIS);
            if (notifications != null && shouldWake(notifications)) {
                wake.run();
            }
        }
    }

    /** N notificações do mesmo lote viram no MÁXIMO um wake — o lap que segue varre todos os shards próprios de uma vez. */
    private boolean shouldWake(PGNotification[] notifications) {
        for (PGNotification notification : notifications) {
            String payload = notification.getParameter();
            try {
                if (ownsShard.test(Integer.parseInt(payload))) {
                    return true;
                }
            } catch (NumberFormatException e) {
                log.debug("LISTEN {}: unparseable payload '{}' — waking anyway (unknown signal on the channel is a "
                        + "reason to look at the queue, not to ignore it)", CHANNEL, payload);
                return true;
            }
        }
        return false;
    }

    private void sleepBeforeReconnect(long backoffMillis) {
        try {
            // await no latch, não sleep: um close() durante o backoff (até 30s)
            // acorda na hora — o while externo vê closed e sai
            closedLatch.await(backoffMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // dona da thread (JCIP 7.1.3), mesmo racional do loop do engine:
            // o protocolo de parada é a flag closed, não interrupt
        }
    }

    @Override
    public void close() {
        closed = true;
        closedLatch.countDown();
        Connection current = connection;
        if (current != null) {
            try {
                // fecha por baixo do getNotifications pra saída imediata; o
                // loop vê closed e não loga a SQLException como queda
                current.close();
            } catch (SQLException e) {
                log.debug("closing the LISTEN connection during shutdown failed — irrelevant, the JVM is leaving", e);
            }
        }
        Thread listener = thread;
        if (listener != null) {
            try {
                listener.join(POLL_TIMEOUT_MILLIS * 2L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
