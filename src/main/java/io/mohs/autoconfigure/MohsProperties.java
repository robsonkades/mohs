package io.mohs.autoconfigure;

import java.time.Duration;

import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

/**
 * Propriedades {@code mohs.*} — só o que {@link MohsAutoConfiguration} de
 * fato consome nesta rodada (bean wiring do motor M3). Escaneamento de
 * {@code @MohsJob}, validações de boot, REST e enforcement de runner/rate
 * limit ainda não existem — as propriedades correspondentes
 * ({@code mohs.registration.on-conflict}, {@code mohs.runners.*},
 * {@code mohs.rate-limits.*}, {@code mohs.api.enabled}) entram junto delas,
 * não antes.
 */
@ConfigurationProperties("mohs")
public class MohsProperties {

    /** Gate mestre — desligar remove todos os beans do Mohs do contexto. */
    private boolean enabled = true;

    @NestedConfigurationProperty
    private final Jdbc jdbc = new Jdbc();

    @NestedConfigurationProperty
    private final Engine engine = new Engine();

    @NestedConfigurationProperty
    private final Lifecycle lifecycle = new Lifecycle();

    @NestedConfigurationProperty
    private final Time time = new Time();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Jdbc getJdbc() {
        return jdbc;
    }

    public Engine getEngine() {
        return engine;
    }

    public Lifecycle getLifecycle() {
        return lifecycle;
    }

    public Time getTime() {
        return time;
    }

    public static class Jdbc {

        /** ADR-0023: escolha explícita, nunca auto-detecção via {@code DataSource}. Sem default — obrigatório. */
        private @Nullable Dialect dialect;

        public @Nullable Dialect getDialect() {
            return dialect;
        }

        public void setDialect(Dialect dialect) {
            this.dialect = dialect;
        }

        public enum Dialect {
            H2, POSTGRESQL, MYSQL, SQLSERVER
        }
    }

    public static class Engine {

        private Duration pollInterval = Duration.ofSeconds(5);
        private int batchSize = 50;
        /** ADR-0012: alimenta {@code lease_expires_at} no claim; o reaper reclama execuções cuja lease já expirou. */
        private Duration leaseTtl = Duration.ofSeconds(30);
        /** Teto real de concorrência do executor de dispatch (nunca por tamanho de pool — CLAUDE.md). */
        private int dispatchConcurrency = 64;
        /** Teto real de concorrência do executor de publicação de eventos. */
        private int eventConcurrency = 16;

        public Duration getPollInterval() {
            return pollInterval;
        }

        public void setPollInterval(Duration pollInterval) {
            this.pollInterval = pollInterval;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public Duration getLeaseTtl() {
            return leaseTtl;
        }

        public void setLeaseTtl(Duration leaseTtl) {
            this.leaseTtl = leaseTtl;
        }

        public int getDispatchConcurrency() {
            return dispatchConcurrency;
        }

        public void setDispatchConcurrency(int dispatchConcurrency) {
            this.dispatchConcurrency = dispatchConcurrency;
        }

        public int getEventConcurrency() {
            return eventConcurrency;
        }

        public void setEventConcurrency(int eventConcurrency) {
            this.eventConcurrency = eventConcurrency;
        }
    }

    public static class Lifecycle {

        /** ADR-0007: {@code auto} chama {@link io.mohs.core.MohsLifecycle#start()} sozinho no boot; {@code manual} espera o consumidor chamar. */
        private StartMode startMode = StartMode.AUTO;

        @NestedConfigurationProperty
        private final Shutdown shutdown = new Shutdown();

        public StartMode getStartMode() {
            return startMode;
        }

        public void setStartMode(StartMode startMode) {
            this.startMode = startMode;
        }

        public Shutdown getShutdown() {
            return shutdown;
        }

        public enum StartMode {
            AUTO, MANUAL
        }

        public static class Shutdown {

            private Duration gracePeriod = Duration.ofSeconds(30);

            public Duration getGracePeriod() {
                return gracePeriod;
            }

            public void setGracePeriod(Duration gracePeriod) {
                this.gracePeriod = gracePeriod;
            }
        }
    }

    public static class Time {

        /** ADR-0008: {@code application} usa o relógio do sistema; {@code database} usa {@link io.mohs.jdbc.DatabaseClock} (banco é a autoridade de tempo do cluster). */
        private Mode mode = Mode.APPLICATION;
        /** Só lido quando {@link #mode} é {@code DATABASE} — limiar de WARN de {@link io.mohs.jdbc.DatabaseClock#sync()}. */
        private Duration skewWarnThreshold = Duration.ofSeconds(1);
        /** Só lido quando {@link #mode} é {@code DATABASE} — a cada quanto tempo reamostrar (ver Javadoc de {@link io.mohs.engine.SyncableClock}, que já nomeia esta propriedade). */
        private Duration syncInterval = Duration.ofSeconds(30);

        public Mode getMode() {
            return mode;
        }

        public void setMode(Mode mode) {
            this.mode = mode;
        }

        public Duration getSkewWarnThreshold() {
            return skewWarnThreshold;
        }

        public void setSkewWarnThreshold(Duration skewWarnThreshold) {
            this.skewWarnThreshold = skewWarnThreshold;
        }

        public Duration getSyncInterval() {
            return syncInterval;
        }

        public void setSyncInterval(Duration syncInterval) {
            this.syncInterval = syncInterval;
        }

        public enum Mode {
            APPLICATION, DATABASE
        }
    }
}
