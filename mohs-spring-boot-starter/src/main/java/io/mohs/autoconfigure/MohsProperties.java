package io.mohs.autoconfigure;

import java.time.Duration;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import io.mohs.core.resource.RunnerMode;
import io.mohs.rest.ApiPaths;

/**
 * Propriedades {@code mohs.*} — só o que {@link MohsAutoConfiguration}/
 * {@link MohsJobScanner}/{@link MohsRestAutoConfiguration} de fato
 * consomem até aqui (bean wiring do motor M3 + escaneamento de
 * {@code @MohsJob} + runners nomeados + REST v1 de jobs/executions).
 * Validações de boot e enforcement de rate limit ainda não existem — as
 * propriedades correspondentes ({@code mohs.rate-limits.*}) entram
 * junto delas, não antes.
 *
 * <p>Records com constructor binding: propriedade é snapshot imutável do
 * boot, não estado mutável — Javadoc de componente fica nas tags
 * {@code @param} (é o que o configuration-processor lê pra gerar o
 * metadata de record).
 *
 * @param enabled gate mestre — desligar remove todos os beans do Mohs do contexto
 * @param runners runners nomeados adicionais aos built-in — ver {@link Runner}
 * @param rateLimits limites de vazão cluster-wide por nome ({@code mohs.rate-limits.smtp.max=100}) — ver {@link RateLimitSpec}
 */
@ConfigurationProperties("mohs")
public record MohsProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue Jdbc jdbc,
        @DefaultValue Engine engine,
        @DefaultValue Lifecycle lifecycle,
        @DefaultValue Time time,
        @DefaultValue Registration registration,
        @DefaultValue Api api,
        @DefaultValue Map<String, Runner> runners,
        @DefaultValue Map<String, RateLimitSpec> rateLimits) {

    /**
     * @param dialect ADR-0023: escolha explícita, nunca auto-detecção via {@code DataSource}. Sem default — obrigatório.
     * @param migrate ADR-0048: o Mohs roda as próprias migrações Flyway no boot ({@code mohs_schema_history}); {@code false} para quem gerencia o schema por fora (DBA) — as migrações continuam no jar como fonte
     */
    public record Jdbc(@Nullable Dialect dialect, @DefaultValue("true") boolean migrate) {

        public enum Dialect {
            H2, POSTGRESQL, MYSQL, SQLSERVER
        }
    }

    /**
     * @param pollInterval intervalo entre ticks do poll loop do engine
     * @param batchSize máximo de execuções reclamadas por claim
     * @param claimRounds ADR-0040: quantos claims um mesmo tick encadeia enquanto o lote voltar cheio e houver folga de dispatch — relaxa o acoplamento da vazão com o {@code poll-interval} sob backlog (medição na ADR); 1 (default) = formato clássico de um claim por tick
     * @param leaseTtl ADR-0012: alimenta {@code lease_expires_at} no claim; desde a ADR-0051 é também o corte de staleness para linha de node legado sem {@code expires_at}
     * @param nodeLeaseTtl ADR-0051: lease do NÓ — o heartbeat de cada tick promete "vivo até agora+TTL" em {@code mohs_nodes.expires_at}; o reaper só reclama execuções de node cuja promessa venceu
     * @param watchdogTimeout Watchdog Bound (ADR-0012, revisto na ADR-0051): teto de runtime — atingido, o node LIBERA a posse (falha cercada, retry normal); {@code null} (default) = sem teto; quando presente, deve ser maior que {@code node-lease-ttl} (validado na montagem do engine)
     * @param misfireThreshold ADR-0035: separa disparo atrasado de perdido — ocorrência devida dentro do threshold dispara atrasada em qualquer política; mais velha responde ao {@code Misfire} do job
     * @param dispatchConcurrency teto real de concorrência do executor de dispatch (nunca por tamanho de pool — CLAUDE.md); também limita o claim (ADR-0039)
     * @param eventConcurrency teto real de concorrência do executor de publicação de eventos
     * @param completionFlushOnEveryResult ADR-0047: desliga o group commit da conclusão e volta ao commit síncrono por resultado — troca a janela de durabilidade (~5ms) pela latência por execução de antes; o único knob que a decisão adiciona
     */
    public record Engine(
            @DefaultValue("5s") Duration pollInterval,
            @DefaultValue("50") int batchSize,
            @DefaultValue("1") int claimRounds,
            @DefaultValue("30s") Duration leaseTtl,
            @DefaultValue("15s") Duration nodeLeaseTtl,
            @Nullable Duration watchdogTimeout,
            @DefaultValue("60s") Duration misfireThreshold,
            @DefaultValue("64") int dispatchConcurrency,
            @DefaultValue("16") int eventConcurrency,
            @DefaultValue("false") boolean completionFlushOnEveryResult) {
    }

    /**
     * @param startMode ADR-0007: {@code auto} chama {@link io.mohs.core.MohsLifecycle#start()} sozinho no boot; {@code manual} espera o consumidor chamar
     */
    public record Lifecycle(
            @DefaultValue("auto") StartMode startMode,
            @DefaultValue Shutdown shutdown) {

        public enum StartMode {
            AUTO, MANUAL
        }

        /**
         * @param gracePeriod quanto tempo o shutdown espera execuções em voo antes de interromper
         */
        public record Shutdown(@DefaultValue("30s") Duration gracePeriod) {
        }
    }

    /**
     * @param mode ADR-0008: {@code application} usa o relógio do sistema; {@code database} usa {@link io.mohs.store.jdbc.DatabaseClock} (banco é a autoridade de tempo do cluster)
     * @param skewWarnThreshold só lido quando {@code mode} é {@code database} — limiar de WARN de {@link io.mohs.store.jdbc.DatabaseClock#sync()}
     * @param syncInterval só lido quando {@code mode} é {@code database} — a cada quanto tempo reamostrar (ver Javadoc de {@link io.mohs.engine.SyncableClock}, que já nomeia esta propriedade)
     */
    public record Time(
            @DefaultValue("application") Mode mode,
            @DefaultValue("1s") Duration skewWarnThreshold,
            @DefaultValue("30s") Duration syncInterval) {

        public enum Mode {
            APPLICATION, DATABASE
        }
    }

    /**
     * @param onConflict ADR-0006: como {@link MohsJobScanner} resolve divergência definicional entre o código e o que já está no store
     */
    public record Registration(@DefaultValue("override") OnConflict onConflict) {

        public enum OnConflict {
            /** Código vence; toda mudança logada com diff (default). */
            OVERRIDE,
            /** Store vence; versão do código ignorada com WARN. */
            PRESERVE,
            /** Divergência derruba o boot exibindo o diff. */
            FAIL
        }
    }

    /**
     * ADR-0010: fechada por padrão ({@code enabled=false}) — ligar é ato
     * consciente, sinalizado por WARN no boot em
     * {@link MohsRestAutoConfiguration}.
     *
     * @param enabled liga a API REST operacional
     * @param basePath prefixo de toda rota de {@code io.mohs.rest}; default é a mesma constante {@link ApiPaths#V1} usada como fallback dos placeholders {@code ${mohs.api.base-path:...}} nos {@code @RequestMapping} (anotação não lê o binding — lá o placeholder é o único mecanismo; leitura em código usa este componente)
     */
    public record Api(
            @DefaultValue("false") boolean enabled,
            @DefaultValue(ApiPaths.V1) String basePath) {
    }

    /**
     * Runner nomeado adicional aos built-in ({@code io}/{@code cpu},
     * montados por {@link MohsAutoConfiguration} com os defaults do
     * documento mestre) — um valor de {@link MohsProperties#runners()}
     * (o próprio {@code Map}, sem embrulho: {@code mohs.runners.<nome>.mode}
     * mais os campos do modo declarado, mesma forma de
     * {@code docs/API-DESIGN.md} "Runners — especificação, nunca
     * Executor"). Campo do modo errado é erro de boot na conversão pra
     * {@code MohsRunner} ({@link MohsAutoConfiguration}) — mesma postura do
     * próprio {@code MohsRunner}, que lança pra campo do modo errado.
     *
     * <p>O binder do Spring canonicaliza chave de mapa não-bracketed pra
     * minúsculas: {@code mohs.runners.myUpload.*} registra o runner como
     * {@code myupload}, e {@code JobDefinition.runner()} é case-sensitive.
     * Prefira nomes minúsculos; pra preservar a caixa exata, use a forma
     * bracketed ({@code mohs.runners.[myUpload].max=8}).
     *
     * @param mode {@code io} (default) ou {@code cpu} — decide quais dos demais campos se aplicam
     * @param max {@link RunnerMode#IO} — default 64 se omitido (mesmo default de {@code MohsRunner.IoBuilder})
     * @param coreSize {@link RunnerMode#CPU} — default núcleos disponíveis se omitido
     * @param maxSize {@link RunnerMode#CPU} — teto de threads do pool
     * @param queueCapacity {@link RunnerMode#CPU} — capacidade da fila do pool
     * @param keepAlive {@link RunnerMode#CPU} — keep-alive das threads acima do core
     */
    public record Runner(
            @DefaultValue("io") RunnerMode mode,
            @Nullable Integer max,
            @Nullable Integer coreSize,
            @Nullable Integer maxSize,
            @Nullable Integer queueCapacity,
            @Nullable Duration keepAlive) {
    }

    /**
     * Um valor de {@code mohs.rate-limits.<nome>} — ADR-0042. O nome é a
     * chave do mapa (como em {@link #runners()}), então não se repete aqui.
     * Ambos obrigatórios: um limite pela metade não tem valor default
     * defensável — {@code max} sem {@code window} não é vazão.
     *
     * @param max disparos permitidos por janela, cluster-wide
     * @param window janela sobre a qual {@code max} vale ({@code 1m}, {@code PT30S})
     */
    public record RateLimitSpec(@Nullable Integer max, @Nullable Duration window) {
    }
}
