# Mohs — Documento Mestre do Projeto
Consolidação de todas as decisões · 12/08/2026
Fontes vivas: CLAUDE.md · API-DESIGN.md (v0.13) · REST-API-DESIGN.md (v0.3)

---

## 1. Visão e identidade

**Mohs** é um componente de agendamento de jobs em Java 25 + Spring Boot com a
ambição de ser referência de mercado em performance e confiabilidade. O nome
vem da escala de dureza de Mohs — **na qual o quartzo é só um 7**: a
provocação ao incumbente está no batismo.

Diligência do nome (verificada): espaço de software limpo (apenas contas
pessoais e repositórios acadêmicos); `mohs` **livre no npm, PyPI e
crates.io**; orgs GitHub livres: `mohs-io` (recomendada), `mohs-dev`,
`getmohs`, `usemohs`, `mohs-scheduler`; nenhum site em `mohs.io`/`mohs.dev`
(WHOIS não verificado — registrar imediatamente); vizinho fonético no mundo
dev: Mosh (mobile shell), sem colisão de grafia ou nicho; "Mohs10
Technologies" (consultoria indiana) — nome distinto, sem conflito; busca de
marca no INPI (classe de software) pendente com advogado.

Identidade técnica: org `mohs-io` · groupId `io.mohs` · artefato único
`io.mohs:mohs` · pacotes `io.mohs.*` · domínios `mohs.io`/`mohs.dev`.

Checklist fora do código: registrar os domínios; criar a org; reservar
`mohs` no npm e PyPI (livres hoje, baratos, evitam squatting); INPI.

---

## 2. Persona e processo de trabalho

O agente (Claude Code) atua como **líder técnico do Mohs**. Comportamentos,
não adjetivos: tem opinião e argumenta; após a decisão, registra discordância
em uma linha e executa (*disagree & commit*); clean code/SOLID/testes são
pré-requisito, não mérito; toda decisão relevante nasce com trade-offs e vira
mini-ADR (contexto → alternativas → decisão → consequências) em `docs/adr/`;
pensa primeiro em modos de falha (morte entre claim e execução; trigger
duplicado entre nós; relógio andando para trás); mede antes de opinar sobre
performance (BASELINE.md > intuição); compara com o estado da arte (Quartz,
JobRunr, db-scheduler, Temporal) sempre que pisa em território que eles já
resolveram; projeta para as 3h da manhã (operabilidade é requisito de
feature).

"Pronto" neste projeto exige: semântica de execução explícita (at-least-once
por padrão, idempotência documentada, misfire nomeado); concorrência
distribuída séria (`FOR UPDATE SKIP LOCKED`, lease com heartbeat, tolerância
a clock skew); mechanical sympathy (alocação mínima em hot paths, batching
onde o custo fixo domina); backpressure e limites com comportamento definido
em toda borda; API pública com DX de produto (defaults seguros, erros que
ensinam, deprecation com caminho de migração).

Preferências Java 25: records para value objects; sealed interfaces +
pattern matching para estados; `ScopedValue` > `ThreadLocal`; imutabilidade
por padrão; nada de abstração especulativa (três usos reais antes de
generalizar).

Concorrência (regra nº 1): workload I/O-bound → virtual threads
(`newVirtualThreadPerTaskExecutor`, limite via `Semaphore`, nunca
fixed/cached pool de virtual threads); CPU-bound → platform pool limitado;
proibido `synchronized` segurando bloqueio (usar `ReentrantLock`;
`Object.wait` → `Condition.await`); fan-out com `StructuredTaskScope`;
threads sempre nomeadas (`mohs-job-N`); HikariCP dimensionado para virtual
threads (`maximumPoolSize` alto, `connectionTimeout` baixo); validação de
pinning com `-Djdk.tracePinnedThreads`.

Disciplinas de código: testes verdes após cada etapa; trecho sem teste →
teste primeiro; testes de concorrência determinísticos (latches/Awaitility,
nunca `Thread.sleep`); um assunto por commit com o porquê; **tempo nunca é
lido direto** (`Instant.now()`/`currentTimeMillis()` proibidos no motor —
tudo via `Clock` injetado; duração via `System.nanoTime`) — regras de
arquitetura verificadas por **ArchUnit**.

### Processo de construção em 4 fases

Mohs é projeto novo — não há motor legado pra auditar antes de escrever a
primeira linha. As 4 fases rodam sobre o que este próprio design produz
(M1-M4, §9), não sobre um código pré-existente.

**Fase 0 — Baseline:** roda depois que M3 (§9) entrega uma implementação
para medir. Suíte completa; benchmark reproduzível (throughput jobs/s com
I/O simulado; latência trigger→início p50/p99; 10k+ jobs concorrentes; JMH
micro + harness macro); execução com `-Djdk.tracePinnedThreads=full`; tudo
em `BASELINE.md` — a referência que toda etapa de performance (M4) terá que
bater.

**Fase 1 — Auditoria, 5 lentes:** roda duas vezes — sobre os contratos de
M1 (revisão de design, ainda sem motor) e sobre a implementação de M3
(antes do refino de M4). Lentes: concorrência (classificação io/cpu,
pinning, pools, ScopedValue, StructuredTaskScope, connection pool, threads
sem nome); **modos de falha** (nó morre entre claim e execução? trigger
duplicado? relógio para trás? semântica real de execução?); **estado da
arte** (aquisição, misfire, retry, persistência, clustering: como
Quartz/JobRunr/db-scheduler/Temporal resolvem cada um; onde o Mohs fica
atrás/no páreo/à frente); design (classes-Deus, duplicação, acoplamento,
nomes); modernização Java 25. Fecha com o veredito do líder: as 3 apostas
que mais aproximam o Mohs de "melhor do mercado".

**Fase 2 — Plano:** sequência em milestones — ver §9. M1 (contratos do
core, `io.mohs`, sem lógica) antes de M3 (implementação do motor atrás do
contrato já congelado), antes de M4 (refino: performance → legibilidade →
Java 25). Decisão de arquitetura → mini-ADR aprovado antes.

**Fase 3 — Execução:** uma etapa por vez; teste antes se faltar; suíte
completa; etapas de performance comparam com BASELINE.md (regrediu →
reverte e explica); commit isolado; fechamento em 3 linhas (o que mudou, o
trade-off aceito, o que o líder faria diferente).

---

## 3. Escopo funcional do motor (a construir)

Projeto novo — nada abaixo existe em código ainda; é a especificação que M3
(§9) implementa, não um inventário.

**Scheduling:** one-time; cron sintaxe Quartz (seconds-first, `ZoneId`
explícito, expressões cacheadas); fixed-rate (ancorado no fire *agendado*);
fixed-delay (ancorado no *fim* da execução); exclusões de calendário
(weekday/data/faixa/predicado, até 10.000 rejeições antes de falhar alto);
misfire Ignore (default) / FireNow / FireAllMissed (replay com cap 1.440 por
ciclo, drenado, nunca descartado); upsert idempotente por JobKey; execução
manual sob demanda (mesmo pausado); SPI de extensão para
triggers/retry/misfire.

**Execução e falha:** modelo Execution/Attempt (retry incrementa `attempt`,
id permanece); retry fixo e exponencial com jitter; cancelamento cooperativo
(cache de 1s); timeout por interrupt com escalada WARN→ERROR; retry manual
de ops (bypassa política exaurida); batch flat com contadores agregados e
callback de conclusão; contrato **at-least-once** documentado (idempotência
como obrigação do handler); liveness — lease com heartbeat por node e
reaper de execuções órfãs, com Watchdog Bound cluster-wide opcional (§5,
seção Watchdog Bound) contra Attempt que ignora interrupt.

**Quatro eixos independentes de controle** (o diferencial da taxonomia):
exclusão mútua por job no próprio SQL de claim (`allowConcurrentExecutions`
default false); Runner node-local por natureza de workload (virtual cap 64 /
cpu = cores); Queue cluster-wide (enforcement a definir por benchmark —
seção 5.8); Rate Limiter cluster-wide de janela fixa (eixo de vazão,
distinto de concorrência); Priority em 5 níveis.

**Riscos e lacunas já identificados no design (a resolver em M3/M4):**
graceful shutdown (resolvido em design, seção 5.4); rate limiter de janela
fixa permite burst de ~2× na virada (evolução: token bucket/sliding
window); priority sem aging (risco de starvation de BACKGROUND);
observabilidade (Micrometer/OTel); test kit; latência de trigger com piso
no intervalo de poll (aposta: wakeup event-driven — LISTEN/NOTIFY com
fallback de polling adaptativo). Versionamento de payload: **decidido** —
compatibilidade é obrigação do handler, não do motor (ver Pendências).

**As 3 apostas do líder:** 1) liveness completo (lease + heartbeat + reaper
+ Watchdog Bound + graceful drain) — confiabilidade é o produto; 2) a
camada de DX deste documento sobre o motor programático; 3) latência
sub-segundo via wakeup event-driven, provada no BASELINE.md.

---

## 4. Empacotamento — módulo único, full Spring Boot

Um artefato: `io.mohs:mohs`. O motor usa a infraestrutura Spring livremente
— em particular transações via `TransactionSynchronizationManager`, o que
torna a cláusula transacional (5.6) infraestrutura, não artesanato. Aposta
estratégica registrada: Quarkus/Micronaut/standalone ficam fora; reabrir
será caro.

**Discordância do líder, registrada e superada (decisão do PO, 12/08/2026):**
full Spring Boot fecha a porta pra quem não usa Spring (fatia real:
db-scheduler, JobRunr e Quartz servem esse público). Decisão mantida — sigo.

O que substitui a disciplina do multi-módulo:

1. **Fronteira por pacote, guardada por ArchUnit:** API pública toda sob
   `io.mohs.core` — **[DECIDIDO EM ADR-0015]**, revisando a ADR-0013 (que
   deixava fachada/identidade soltas em `io.mohs` raiz): `io.mohs.core`
   (fachada + identidade: `Mohs`, `JobKey`, `JobRef`) · `io.mohs.core.schedule`
   (agenda) · `io.mohs.core.definition` (`JobDefinition`, `@MohsJob`) ·
   `io.mohs.core.execution` (`Execution`, `JobContext`) · `io.mohs.core.event`
   (eventos, listeners, interceptors) · `io.mohs.core.resource` (runners,
   queues, windows). `io.mohs` (raiz) fica só com o bootstrap Spring Boot
   deste módulo (`MohsApplication`), não API. `io.mohs.cron` é utilitário à
   parte (não é vocabulário de job, não migrou pra `core`) · `io.mohs.engine`
   e `io.mohs.jdbc` (internos, `@Internal`) · `io.mohs.autoconfigure` ·
   `io.mohs.rest` · `io.mohs.test`. Regras no build: interno não vaza para a
   API pública; `rest` só enxerga a API pública; `test` não vaza para
   produção.
2. **Web opcional:** `spring-web` como `<optional>`; REST/dashboard ativam
   via `@ConditionalOnClass` + `mohs.api.enabled` (padrão actuator). Teste
   de contrato: app sem web no classpath sobe.
3. **Test kit no jar** (`io.mohs.test`), `spring-test` opcional.

---

## 5. Design da API Java (v0.13)

### 5.0 A superfície em uma página

```java
@Component
public class EmailJobs {

    public record WelcomeEmail(String user, String name, int age) {}

    // Definição recorrente
    @MohsJob(id = "welcome-email", name = "E-mail de boas-vindas",
             cron = "0 0 2 * * *", zone = "America/Sao_Paulo",
             runner = "smtp", queue = "emails", window = "business-days",
             misfire = Misfire.FIRE_NOW, retries = 8, timeout = "PT5M")
    public void send(WelcomeEmail payload, JobContext ctx) { ... }

    // Definição sob demanda (sem cron/every)
    @MohsJob(id = "import-file", runner = "io")
    public void importFile(ImportFile payload) { ... }
}

public final class Jobs {
    public static final JobRef<WelcomeEmail> WELCOME =
        JobRef.of("welcome-email", WelcomeEmail.class);
}

// Invocação — um verbo, terminais definem o quando (cadeia @CheckReturnValue)
Enqueued e = mohs.schedule(Jobs.WELCOME, new WelcomeEmail("u1","Ana",31)).now();
mohs.schedule(Jobs.WELCOME, payload).at(instant);
mohs.schedule(Jobs.WELCOME, payload).after(Duration.ofHours(2));
mohs.schedule(Jobs.WELCOME, payload).priority(Priority.HIGH)
    .as("checkout-service").now();

// Lote flat + continuação
mohs.batch("import-2026-08", b ->
        files.forEach(f -> b.add(Jobs.IMPORT, new ImportFile(f))))
    .onCompletion(s -> mohs.schedule(Jobs.REPORT, new ImportReport(s.batchId())).now());

// Definição dinâmica (multi-tenant) — o mecanismo por baixo da annotation
mohs.define(JobDefinition.of("tenant-" + t.id() + "-sync", TenantSyncHandler.class,
    spec -> spec.cron(t.syncCron(), t.zone()).runner("io").queue("tenant-sync")));
mohs.remove(jobKey);  // aposentadoria: cancela fires futuros, preserva histórico
```

### 5.1 Princípios

1. **Definição × invocação** — define uma vez (handler + políticas), invoca
   de N formas (cron, `schedule`, `batch`, dashboard); invocação nunca
   redefine política.
2. **Tipado > stringly** — payload record/POJO; `JobRef<T>` valida em
   compilação; tipos do JDK (`Duration`, `Instant`, `ZoneId`).
3. **Fail-fast no boot com erros que ensinam.**
4. **Configuração referenciada, controle retido** — recursos nomeados por
   bean/properties, mas o Mohs materializa o runtime (nunca `Executor`
   arbitrário — a lição do `@Async`).
5. **Zero colisão com o JDK** em nomes públicos.
6. **Contrato honesto** — at-least-once explícito; idempotência é obrigação
   declarada do handler.

### 5.2 Vocabulário

| Conceito | Nome | Nota |
|---|---|---|
| Definição | `JobDefinition` / `@MohsJob` | annotation = forma canônica; `define` = mecanismo |
| Identidade | `JobKey` (`id`) | estável, chave de persistência; `name` = rótulo mutável |
| Referência tipada | `JobRef<T>` | amarra id + tipo do payload em compilação |
| Agenda | `Schedule` | `cron` · `every` (rate) · `everyAfterFinish` (delay) · `onDemand` |
| Instância | `Execution` / `Attempt` | retry incrementa attempt, id permanece |
| Capacidade node-local | `MohsRunner` (`mode: io\|cpu`) | built-ins `io`/`cpu`; customs = bulkheads |
| Cap cluster-wide | `JobQueue` | **[DECIDIDO]** rename de `Queue` (colisão JDK) |
| Janelas de exclusão | `ExecutionWindow` | **[DECIDIDO]** rename de `Calendar` (colisão JDK) |
| Misfire / Retry / RateLimit / Priority | — | espelham o motor |

### 5.3 Definição

`@MohsJob`: `id` obrigatório/único (= JobKey, upsert a cada boot); `cron` ×
`every` mutuamente exclusivos (ambos ausentes = sob demanda); parâmetros por
convenção (até um payload + um `JobContext`, opcionais, qualquer ordem); sem
interface `Job`, sem `implements`; casos comuns em atributos (`retries`,
`timeout`), política custom referencia bean.

`define`: o starter traduz cada annotation em exatamente um `define` no boot
— num app típico o usuário nunca o escreve. Uso direto: **agenda que vem de
dados** (multi-tenant — registrar em loop, upsert em runtime, tenant novo
sem deploy). Limite deliberado: id/agenda/políticas variam em runtime; o
**handler é sempre código compilado**. Mesmo id via annotation e `define` =
erro fatal de boot apontando os dois lugares.

### 5.4 Ciclos de vida

**Registro (boot):** scan `@MohsJob` → validações fatais → engine inicia
(SmartLifecycle, fase tardia) — **nenhum claim antes de todas as definições
anotadas registradas** → app ready (`define`/`remove` dinâmicos liberados).

Upsert preciso: **estado definicional** (agenda, políticas, runner, queue,
window, name, handler) pertence ao código → upsert aplica; **estado
operacional** (paused/resumed, histórico, contadores) pertence ao runtime →
upsert **preserva** (job pausado às 3h continua pausado após o deploy das 9h).

```yaml
mohs:
  registration:
    on-conflict: override   # override (default) | preserve | fail
```
`override`: código vence, mudanças logadas com diff (auditoria de drift);
`preserve`: store vence com WARN (PATCHes de runtime sobrevivem a deploys);
`fail`: divergência derruba o boot exibindo o diff.

Órfãs de definição: `source = ANNOTATION | PROGRAMMATIC`; ANNOTATION
presente no store e ausente do código → **ORPHANED** (não dispara, destaca
no dashboard, WARN) — nem fogo no vazio, nem delete de histórico;
PROGRAMMATIC fora da varredura, aposentadoria via `mohs.remove(jobKey)`.

**Engine (node-local — não confundir com pause de job, cluster-wide):**

```
CREATED → RUNNING ⇄ PAUSED → DRAINING → STOPPED
```
`mohs.lifecycle()`: `state() · start() · pause() · resume() ·
drain(grace) · stop(grace)`.

```yaml
mohs:
  enabled: true                # false = auto-config desligada
  lifecycle:
    start-mode: auto           # auto | manual → lifecycle().start()
    shutdown: { grace-period: 30s }
```

Shutdown gracioso (fase antecipada — drena **antes** do Spring fechar o
DataSource): SIGTERM → `DRAINING` (claim para, readiness cai) → in-flight
até o grace (**drain ≠ cancel**) → estouro: interrupt, attempt falha com
`NodeShutdown` e segue o retry — at-least-once honesto até no desligamento
→ runners desligam → só então pools fecham. `manual` para warm-up, leader
election externa, canário, test kit. Transições como `ApplicationEvent`
(`MohsLifecycleEvent`). Health indicator + readiness refletindo DRAINING;
`grace-period` documentado com `terminationGracePeriodSeconds`.

### 5.5 Invocação

Um verbo, sempre sobre definição existente: `schedule(ref, payload)` com
terminais `now() / at(Instant) / after(Duration)`; pré-terminais da
instância: `priority`, `as(actor)`, `idempotencyKey`. Política (retry,
runner, queue) pertence à definição — invocação não sobrescreve. Overload
por string com checagem de tipo em runtime (erro claro); `JobRef` sem
definição → falha imediata com sugestões; `JobRef`s em beans checados no
boot. Batch: flat, `b.add(ref, payload)`, `onCompletion` (continuação).

### 5.6 Contrato assíncrono [as 5 cláusulas]

1. **Execução sempre assíncrona** — o handler jamais roda na thread do
   chamador, nem para jobs imediatos.
2. **Durabilidade sempre síncrona** — o retorno do terminal = job
   persistido e sob custódia do Mohs; o at-least-once começa aqui; não
   existe fire-and-forget em memória.
3. **Retorno é recibo (`Enqueued`), nunca `Future` do resultado** —
   desfecho se observa por eventos, `mohs.execution(id)`, dashboard;
   `awaitExecution` só no test kit.
4. **Transacional por participação** — dentro de transação ativa (mesmo
   DataSource), o insert entra na transação do chamador: commit publica,
   rollback apaga — *transactional outbox* nativo, sem broker.
5. **Admissão nunca espera capacidade** — queue/rate/runner limitam a
   execução (no claim), não o aceite; p99 do terminal ≈ custo do insert
   (metrificado no BASELINE.md).

### 5.7 Runners

Referência por nome estilo `@Async("...")`, mas o bean é **spec**
(`MohsRunner`), nunca `Executor`: o Mohs cria e é dono das threads
(cancelamento, timeout, métricas por runner, disciplina io→virtual /
cpu→platform). Customs = bulkheads por integração.

```yaml
mohs:
  runners:
    io:   { mode: io,  max: 64 }
    cpu:  { mode: cpu, core-size: 4, max-size: 8, queue-capacity: 0, keep-alive: 60s }
    smtp: { mode: io,  max: 8 }
```
```java
@Bean MohsRunner s3() { return MohsRunner.io("s3").maxConcurrent(32).build(); }
@Bean MohsRunner batch() {
    return MohsRunner.cpu("batch").coreSize(4).maxSize(8)
        .queueCapacity(0).keepAlive(Duration.ofSeconds(60)).build();
}
```
Properties de `cpu` espelham `spring.task.execution.pool.*`
(`core-size`/`max-size`/`queue-capacity`/`keep-alive`) — mas com defaults
diferentes dos do Spring, deliberadamente (ver ADR-0014): pool fixo
(`max-size` = núcleos por padrão) e fila sem capacidade (`queue-capacity: 0`,
direct handoff), não ilimitados.

### 5.8 Queues, windows e o enforcement em revisão

Bean define a estrutura, property ajusta o número
(`mohs.queues.emails.max-concurrent: 10`); windows precisam de bean
(predicados só existem em código: `excludeWeekends().excludeDates(...)
.excludeDaily(...).exclude(pred)`). Semântica cluster-wide: definição no
boot é upsert; rolling deploy converge para o último nó (sob `override`).

**Papéis que nunca se fundem:** runner protege *este nó*; queue protege um
*recurso compartilhado* (SMTP, API parceira) — escalar nós não pode escalar
a pressão; runner não substitui queue.

**Enforcement [EM REVISÃO → ADR com gate de benchmark]:** o contador atual
(`UPDATE ... WHERE running_count < max`) tem três modos de falha — hot row,
bloat de tuplas no Postgres, drift do contador em crash (vaga vaza para
sempre, sem reconciliação). Proposta: **contagem derivada** no claim
(`(SELECT count(*) WHERE queue=? AND status='RUNNING') < max`, índice
parcial; conjunto RUNNING ≤ max → contagem O(max); claims batelados no
poll). Sem estado mantido: sem hot row, sem bloat, sem drift. Semântica
resultante: **soft cap** (overshoot transitório ≤ nós−1) — adequada a
proteção de recurso e documentada. Dependência: execuções de nó morto
seguram vaga até o **reaper** devolvê-las ao retry (auto-cura; o contador
vazava permanentemente). Gate: Fase 0/1 mede o contador sob carga-alvo; a
API é agnóstica ao enforcement.

### 5.9 JobContext

`jobKey() · executionId() · attempt() (1-based) · scheduledAt() · firedAt()
· cancellationRequested() (cache 1s) · progress(done, total)`.

### 5.10 Listeners × Interceptors

Duas SPIs (a fusão do Quartz é o anti-exemplo):

**`ExecutionListener`** — observar, nunca interferir. Eventos `sealed`:
`Enqueued, Started, AttemptFailed, RetryScheduled, Succeeded, Failed,
Cancelled, BatchCompleted` (pattern matching; release novo = compilador
avisa). Exceção capturada/logada/metrificada — jamais afeta o job; dispatch
assíncrono em executor próprio de virtual threads, ordenado por execution.
Açúcar: `@OnExecution(job=..., event=FAILED)` em método. Bridge
`ApplicationEvent` opt-in (`mohs.events.spring-bridge=true`).

**`ExecutionInterceptor`** — envolver (`intercept(ctx, chain)`), na thread
do attempt, cadeia `@Order`; lugar de MDC, spans, `ScopedValue`; exceção É
falha do attempt (retry normal).

Garantias: eventos in-process são **best-effort**; reação garantida =
handler enfileira continuação **dentro da transação** (cláusula 4) —
listener observa, job encadeado reage. Dogfooding: Micrometer e OTel do
próprio Mohs implementados sobre estas SPIs. Entrega durável/cluster-wide
(SSE/webhooks) nasce da futura tabela de eventos.

### 5.11 Disciplina fluente

1. Fluente na configuração, plano no caminho quente (`JobContext`/handler).
2. Cadeia pura até o terminal; registrar é papel do bean/`define`; terminais
   `@CheckReturnValue` (cadeia abandonada = warning).
3. **Staged builder só onde há invariante estrutural:** no Schedule,
   `cron()` retorna estágio sem `every()` e vice-versa — exclusão mútua vira
   erro de compilação; `onDemand()` explícito (ausência silenciosa não
   compila). Estágios `sealed` (métodos novos em minor sem quebrar ninguém).
   Annotation valida o mesmo no boot (strings não têm compilador).

### 5.12 Tempo

Costura única: todo "agora" via `java.time.Clock` injetado (leitura direta
proibida, ArchUnit). Três implementações: **application** (default, JVM
UTC); **database** (banco = autoridade do cluster; amostragem de offset
estilo NTP a cada `sync-interval` com compensação de ida-e-volta; leituras
locais O(1), **nunca** round-trip por leitura; clamp monotônico; banco fora
→ mantém último offset, nunca bloqueia); **test** (`MutableClock`).

```yaml
mohs:
  time: { source: application, sync-interval: 30s, skew-warn-threshold: 500ms }
```

Em qualquer modo, offset app×banco vira métrica (`mohs.time.offset`) com
WARN acima do limiar — clock skew deixa de ser silencioso até no default.
Onde a decisão é SQL (`WHERE next_fire_at <= now()`), a autoridade já é o
banco. Dois tempos: wall clock = "quando"; duração = `nanoTime` (nunca
subtração de wall clock).

### 5.13 Validações de boot (fatais, ensinam)

1. cron válido com próxima ocorrência; `zone` válida; `every` > 0;
2. `runner`/`queue`/`window`/`rateLimit`/`retryPolicy` existem (sugestão por
   distância de edição: "não encontrei o runner 'smpt'; quis dizer 'smtp'?");
3. `id` duplicado (annotation × annotation, annotation × programático);
4. payload serializável (round-trip no boot);
5. assinatura suportada; `JobRef`s de beans resolvem e tipos batem.

### 5.14 Test kit

`@MohsTest` (scheduler embarcado, storage em memória, start manual);
`mohs.clock().setTo(...) / advance(...)`; `awaitExecution(ref, timeout)`;
`triggerNow(ref, payload)`.

### 5.15 Actor e regressão ergonômica assumida

Actor: app registra `actor="application"` por padrão; REST/dashboard usam
`ActorResolver`; `.as(actor)` opcional para casos especiais — trilha de
"quem disparou" é inegociável em toda invocação. Regressão ergonômica
assumida: one-off exige definição prévia (`@MohsJob` sob demanda) antes de
`schedule().at()` — atrito deliberado em troca de payload tipado, políticas
na definição e visibilidade no dashboard.

**Fora de escopo v1:** DAG/workflows (batch + `onCompletion` cobre
encadeamento simples), API reativa, dialetos cron alternativos, ports para
outros frameworks (fechado pelo empacotamento).

---

## 6. API REST (v0.3)

Plano **operacional** — definição é código; não existem endpoints de
criar/alterar definição, nem endpoint síncrono "executa e espera".

Princípios: **202 Accepted** com recibo + `Location` (o contrato assíncrono
em HTTP); actor via **`ActorResolver`** (principal com segurança; header
`X-Mohs-Actor` declarativo sem ela); erros **RFC 7807**; **paginação por
cursor**; **fechada por padrão** (`mohs.api.enabled=false` é lei; habilitar
sem auth → WARN destacado; guia: rede interna/gateway/mTLS; segurança futura
pluga só o resolver — zero mudança de contrato).

| Endpoint | Efeito |
|---|---|
| `GET /overview` | âncora de polling do dashboard (contagens, filas, throughput) |
| `GET /jobs` · `/jobs/{key}` | definições, estado, próximo fire |
| `POST /jobs/{key}/schedule` | invoca — body `{payload, at?}`; `Idempotency-Key` (~24h) → 202 |
| `POST /jobs/{key}/pause` · `/resume` | cluster-wide; schedule manual segue permitido |
| `GET /executions` (+filtros) · `/{id}` | busca global · detalhe (attempts, erro, actor) |
| `POST /executions/{id}/cancel` · `/retry` | cooperativo · retry manual de ops |
| `GET/PATCH /queues` · `/rate-limits` | estado + ajuste runtime cluster-wide |
| `GET /runners` | visão node-local |
| `GET /nodes` | visão de cluster — nodes com heartbeat recente |
| `GET /batches/{id}` | contadores do lote |

PATCH runtime × boot: PATCH é o botão de emergência das 3h, durável até o
próximo boot (comportamento do `on-conflict: override`; sob `preserve`,
sobrevive) — resposta avisa "codifique em properties".

Decisões v0.3: **sem SSE** (polling-first; push futuro sobre tabela de
eventos durável); **sem autenticação embutida** (discordância do líder
registrada; mitigada pelos guardrails); **dashboard consome esta mesma API**
(dogfooding estrutural; toda feature de tela nasce scriptável).

Roadmap: SSE/webhooks; `POST /nodes/{id}/drain` (drain remoto; `GET /nodes`
já promovido pra v1 — reusa o registro de heartbeat que a liveness de M3 já
precisa construir); autorização fina.

---

## 7. Pendências e revisões

**Aguardando decisão do PO:** nenhuma pendência aberta hoje.

**Resolvidas (12/08/2026):**
1. **Renames `JobQueue` (← Queue) e `ExecutionWindow` (← Calendar)** —
   aprovados. Colisão de import com o JDK custava centavos agora e uma
   fortuna depois do 1.0.
2. **Serialização e versionamento de payload** — decidido: compatibilidade
   entre deploys é obrigação do handler/aplicação; o motor garante só o
   round-trip de boot (validação 4), não migração de Executions persistidas.
3. **Reaper de execuções órfãs** (lease/heartbeat) — especificado como
   capacidade obrigatória de M3 (liveness: lease com heartbeat + reaper +
   Watchdog Bound). Sustenta quatro capacidades — recuperação at-least-once
   real, `GET /nodes`, auto-cura do soft cap da queue, honestidade do
   contrato de execução — por isso entra em M3 e não fica pra depois.

**Em revisão com gate:** enforcement da queue (contador vs contagem
derivada) — decidido por benchmark na Fase 0/1 (seção 5.8).

**Em aberto, não bloqueante:** nome e valor de `node-heartbeat-interval`
(ver API-DESIGN.md) — intervalo do heartbeat de node ainda sem property
definida.

---

## 8. Plano de ADRs

| ADR | Tema | Status |
|---|---|---|
| 0001 | Empacotamento: módulo único full Spring Boot | decidido |
| 0002 | Arquitetura definição × invocação | decidido |
| 0003 | Contrato assíncrono e transacional | decidido |
| 0004 | Vocabulário e renames (Runner, JobQueue, ExecutionWindow) | decidido |
| 0005 | Listeners × Interceptors | decidido |
| 0006 | Ciclo de registro e `on-conflict` | decidido |
| 0007 | Lifecycle do engine | decidido |
| 0008 | Fonte de tempo configurável | decidido |
| 0009 | Enforcement de queue | em revisão (gate de benchmark) |
| 0010 | API REST v1 | decidido |
| 0011 | Serialização e versionamento de payload | decidido |
| 0012 | Liveness: heartbeat, lease e reaper (Watchdog Bound) | decidido |
| 0013 | Subpacotes da API pública (revisa ponto 1 da ADR-0001) | decidido |
| 0014 | Properties de pool estilo Spring para o runner CPU | decidido |
| 0015 | Consolidar a API pública sob `io.mohs.core` (revisa ADR-0013) | decidido |
| 0016 | Claim e transição para `RUNNING` são atômicos | decidido |
| 0017 | Mutex por job e admissão de queue no claim | superseded pela ADR-0018 |
| 0018 | Mutex por job via CAS guardado, não dependente de lock especializado | decidido |

**Etapas geradas pelo design** (entram no PLAN.md, sequenciadas em
milestones em §9): esqueleto de módulo + ArchUnit; contratos do core
(`io.mohs`); varredura de `Instant.now()` + costura de Clock; verbo
unificado `schedule` + terminais; teste de integração do rollback
transacional; SPIs de listener/interceptor + Micrometer/OTel; lifecycle do
engine + shutdown gracioso; `DatabaseSyncedClock`; validações de boot; test
kit; REST v1 + `/overview`; gate de benchmark do enforcement de queue;
Watchdog Bound (`watchdogTimeout`); endpoint `GET /nodes`.

---

## 9. Plano de Desenvolvimento

Ordem confirmada: core primeiro — interfaces e contratos que viram a API
pública — motor depois, plugado por trás do contrato já congelado.
Implementação só começa em M3; M1/M2 são puro desenho compilável.

### M0 — Bootstrap

Repositório novo; coordenadas Maven `io.mohs:mohs`; esqueleto de pacotes
(`io.mohs`, `io.mohs.engine`, `io.mohs.jdbc`, `io.mohs.autoconfigure`,
`io.mohs.rest`, `io.mohs.test`) com ArchUnit já testando a fronteira antes
de M1 escrever o primeiro tipo público (§4). Sem isso, M1 não tem onde
morar.

### M1 — Contratos do core (`io.mohs`), sem implementação

Todo o vocabulário fechado em §5 vira interface/record/sealed compilável,
zero lógica de motor por trás ainda:

- `JobKey`, `JobRef<T>`, `JobDefinition` / `@MohsJob` (annotation +
  validação de atributos; `define` ainda sem efeito real)
- `Schedule` sealed (`CronSpec`/`IntervalSpec`/`OnDemandSpec`) — exclusão
  mútua cron×every garantida pelo compilador desde o primeiro commit (5.11)
- `Execution` / `Attempt`, `ExecutionState`, `JobContext` (interface plana)
- `ExecutionListener` + eventos `sealed` (`Enqueued`…`BatchCompleted`);
  `ExecutionInterceptor`
- `MohsRunner`, `JobQueue`, `ExecutionWindow` — specs, nunca `Executor`
- Fachada `Mohs` (`schedule().now/at/after`, `batch`, `define`, `remove`,
  `lifecycle()`) com `@CheckReturnValue` nos terminais; corpo ainda não
  ligado ao motor
- ArchUnit testando a fronteira `io.mohs` × `io.mohs.engine`/`io.mohs.jdbc`
  **antes** de existir implementação pra violar — não depois

### M2 — Contratos REST, sem implementação

Fonte: REST-API-DESIGN.md (superfície completa da tabela).

- DTOs de request/response de cada endpoint
- Controllers com assinatura e `202`/`404`/`422` mapeados; corpo stub
- `ProblemDetail` (RFC 7807) como formato único de erro
- `ActorResolver` como SPI (interface); resolução real fica pra M3

Status: implementado — ver `io.mohs.rest` (5 pacotes: raiz + `error`/
`job`/`execution`/`resource`). Complemento pontual ao M1 nesse meio-tempo:
`Attempt.error` e `io.mohs.core.resource.RateLimit`, que o design REST
precisava e o M1 não tinha congelado ainda.

### M3 — Implementação do motor

Construção do zero, atrás dos contratos M1/M2 já congelados:

- Aquisição sem contenção (claim), lease com heartbeat por node + reaper de
  execuções órfãs, Watchdog Bound (`watchdogTimeout`) — liveness completa
  (§3, aposta #1)
- Dispatch, persistência, enforcement de queue (contador; contagem
  derivada se o gate de benchmark confirmar — §5.8), fonte de tempo via
  `Clock` (§5.12)
- Ligar fachada `Mohs` (M1) ao motor real
- Controllers M2 ligados aos stores reais (`ExecutionStore`, `JobStore`,
  `QueueStore`, store de heartbeat de node)
- `GET /nodes` — leitura sobre o registro de heartbeat construído acima
- Auto-configuração Spring Boot (`io.mohs.autoconfigure`),
  `mohs.api.enabled`, `mohs.registration.on-conflict`, validações de boot
  (§5.13)
- Idempotency-Key persistida — mesma durabilidade da Execution

### M4 — Refino (só depois de M1–M3 fechados)

Prioridade confirmada (§2, Fase 2): 1º performance/concorrência (bate
contra BASELINE.md) · 2º legibilidade/design · 3º Java 25 (records, sealed,
`ScopedValue`). Cada etapa isolada; decisão de arquitetura → mini-ADR antes.

**Dependente de M1-M4 mas fora da sequência linear:** gate de benchmark do
enforcement de queue (§5.8, mede o contador atual sob carga antes de
trocar); test kit; dashboard (consome a API de M2/M3, dogfooding).
