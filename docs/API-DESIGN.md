# Mohs — Design da API Java · draft v0.13

Fonte de contrato do **M1** do Plano de Desenvolvimento (MOHS-DOCUMENTO-MESTRE.md §9):
todo tipo abaixo vira interface/record/sealed compilável antes de qualquer implementação de motor.

Status: em discussão. [DECIDIDO] tem justificativa; [ABERTO] aguarda aprovação.
v0.2: separação definição × invocação (proposta do PO).
v0.3: conceito "lane" renomeado para **Runner** [DECIDIDO].
v0.4: disciplina de interfaces fluentes + staged builder no Schedule [DECIDIDO].
v0.5: contrato assíncrono das invocações — durabilidade síncrona, execução assíncrona [DECIDIDO].
v0.6: de-para da superfície atual + política de depreciação [DECIDIDO]; API REST em REST-API-DESIGN.md.
v0.7: modelo de observação e extensão — Listener × Interceptor [DECIDIDO].
v0.8: invocação unificada em um verbo — `schedule(...)` com terminais `now/at/after` [DECIDIDO].
v0.9: ciclo de registro no boot, upsert definicional × operacional, `on-conflict`, órfãs e `remove()` [DECIDIDO].
v0.10: fonte de tempo configurável — application (default) | database — sobre a costura única de Clock [DECIDIDO].
v0.11: empacotamento — módulo Maven único, full Spring Boot; fronteiras por pacote com ArchUnit [DECIDIDO].
v0.12: controle fino de lifecycle do engine — estados, drain, start manual, integração k8s [DECIDIDO].
v0.13: enforcement da queue em revisão — proposta contador → contagem derivada, com gate de benchmark [EM REVISÃO].
v0.14: liveness (lease/heartbeat/reaper) especificada como capacidade obrigatória do motor; Watchdog Bound documentado como conceito público, cluster-wide [DECIDIDO]; versionamento de payload decidido — compatibilidade é obrigação do handler [DECIDIDO].
v0.15: renames `JobQueue`/`ExecutionWindow` aprovados pelo PO [DECIDIDO] — nenhum item aberto restante neste documento.

## Princípios de design

1. **Definição × invocação.** Um Job é DEFINIDO uma única vez (handler +
   políticas); é INVOCADO de N formas (cron automático, `schedule`,
   `batch`, dashboard). A invocação nunca redefine políticas.
2. **Tipado > stringly.** Payload é record/POJO; invocação usa `JobRef<T>`
   para checagem em compilação; tipos do JDK (`Duration`, `Instant`, `ZoneId`).
3. **Fail-fast no boot, com erros que ensinam.** Referência a runner/queue/window
   inexistente, cron inválido, id duplicado, payload não-serializável: nada
   sobrevive ao startup, toda mensagem diz como corrigir.
4. **Configuração referenciada, controle retido.** Runners/queues/windows são
   recursos nomeados definidos pelo usuário (bean ou properties), mas o Mohs
   SEMPRE materializa o runtime — nunca aceita `Executor` arbitrário
   (a lição do `@Async`: quem entrega a thread perde cancelamento, timeout,
   métricas e a disciplina io/cpu).
5. **Zero colisão com o JDK** em nomes públicos.
6. **Contrato honesto:** at-least-once explícito; idempotência é obrigação
   declarada do handler.

## Vocabulário

| Conceito | Nome público | Nota |
|---|---|---|
| Definição de trabalho | `JobDefinition` / `@MohsJob` | annotation é a forma canônica em Spring; definição programática no core |
| Identidade estável | `JobKey` (`id` na annotation) | chave de persistência; `name` é rótulo mutável |
| Referência tipada p/ invocação | `JobRef<T>` | amarra id ao tipo do payload em compilação |
| Regra de disparo | `Schedule` | cron, `every` (fixed-rate), `everyAfterFinish` (fixed-delay), one-time |
| Instância disparada | `Execution` / `Attempt` | retry incrementa `attempt`, id permanece |
| Capacidade de execução node-local nomeada | `MohsRunner` (`mode: io\|cpu`) | built-ins `io` e `cpu`; customs viram bulkheads |
| Cap de concorrência cluster-wide | `JobQueue` | [DECIDIDO] renomeia `Queue` (colisão JDK) |
| Janelas de exclusão | `ExecutionWindow` | [DECIDIDO] renomeia `Calendar` (colisão JDK) |
| Misfire / Retry / RateLimit / Priority | idem v0.1 | espelham o motor |

## Definição — `@MohsJob` (camada canônica em Spring)

```java
@Component
public class EmailJobs {

    public record WelcomeEmail(String user, String name, int age) {}

    // Recorrente
    @MohsJob(id = "welcome-email", name = "E-mail de boas-vindas",
             cron = "0 0 2 * * *", zone = "America/Sao_Paulo",
             runner = "smtp", queue = "emails", window = "business-days",
             misfire = Misfire.FIRE_NOW, retries = 8, timeout = "PT5M")
    public void send(WelcomeEmail payload, JobContext ctx) { ... }

    // Sob demanda: sem cron/every — só dispara via schedule/batch/dashboard
    @MohsJob(id = "import-file", runner = "io")
    public void importFile(ImportFile payload) { ... }
}
```

Regras:
- `id` obrigatório e único (= `JobKey`); registro é upsert idempotente a cada boot.
- `cron` e `every` mutuamente exclusivos; ausentes = job sob demanda.
- Parâmetros por convenção: até um payload e um `JobContext`, opcionais,
  qualquer ordem. Sem interface, sem `implements`.
- Casos comuns em atributos simples (`retries`, `timeout`); política custom
  referencia bean (`retryPolicy = "minhaPolicy"`).

### `define` — o mecanismo por baixo da annotation

`@MohsJob` é açúcar: no boot, o starter traduz cada annotation em exatamente
uma chamada de `define`. Num app Spring típico você nunca o escreve — ele
roda por você. Chamadas diretas existem para dois cenários:

**Agenda que vem de dados, não de código** (multi-tenant e afins) — annotation
é estática; quando cada tenant tem seu cron numa tabela, registra-se em loop:

```java
@EventListener(ApplicationReadyEvent.class)
void registerTenantJobs() {
    tenants.findAll().forEach(t ->
        mohs.define(JobDefinition.of(
            "tenant-" + t.id() + "-sync",
            TenantSyncHandler.class,              // handler continua sendo código
            spec -> spec.cron(t.syncCron(), t.zone())
                        .runner("io").queue("tenant-sync"))));
}
```

`define` herda o upsert idempotente do antigo `recurring`: cria-ou-atualiza,
seguro em todo boot e em runtime (tenant novo → job novo, sem deploy).
Limite deliberado: id, agenda e políticas variam em runtime; o **handler é
sempre código compilado** — `define` cria instâncias dinâmicas de
comportamento existente, nunca comportamento dinâmico.

**Nota (v0.11):** com o empacotamento módulo-único full Spring Boot, o caso
"ambientes sem Spring" deixou de existir — a razão de ser do `define` é o
cenário dinâmico acima.

Convivência: mesmo `id` via annotation e via `define` = erro fatal de boot
apontando os dois lugares (validação 3).

## Ciclo de registro e política de conflito [DECIDIDO]

Ordem de boot (starter):
1. scan de `@MohsJob` → um `define` por annotation;
2. validações fatais (seção Validações de boot);
3. engine inicia (SmartLifecycle, fase tardia) — **nenhum claim acontece
   antes de todas as definições anotadas estarem registradas**;
4. app ready; `define`/`remove` dinâmicos permitidos daqui em diante.

O upsert é preciso sobre o que toca:
- **Estado definicional** (agenda, políticas, runner, queue, window, name,
  binding do handler) pertence ao código → upsert aplica.
- **Estado operacional** (paused/resumed, histórico, contadores, last fire)
  pertence ao runtime → upsert **preserva**. Job pausado pelas ops às 3h
  continua pausado após o deploy das 9h.

Conflito definicional (jobs e recursos — queues/rate-limits):

```yaml
mohs:
  registration:
    on-conflict: override   # override (default) | preserve | fail
```

- `override`: código vence; toda mudança logada com diff
  ("cron '0 0 2…' → '0 0 3…'") — auditoria de drift de definição.
- `preserve`: o store vence; versão do código ignorada com WARN. Efeito
  documentado: PATCHes de runtime sobrevivem a deploys.
- `fail`: divergência derruba o boot exibindo o diff — para ambientes que
  exigem migração explícita de agenda.

Órfãs e aposentadoria:
- Toda definição carrega `source` (`ANNOTATION` | `PROGRAMMATIC`).
- No boot, definição `ANNOTATION` presente no store e ausente do código vira
  **ORPHANED**: não dispara, destaca no dashboard, WARN no log — nem fogo no
  vazio, nem delete silencioso de histórico.
- `PROGRAMMATIC` fica fora da varredura; aposentadoria explícita via
  `mohs.remove(jobKey)`: cancela fires futuros, preserva histórico.

## Ciclo de vida do engine [DECIDIDO]

Node-local por natureza — não confundir os planos: pause de **job** é
cluster-wide e por job (REST/dashboard); lifecycle do **engine** governa
este nó, todos os jobs.

```
CREATED → RUNNING ⇄ PAUSED → DRAINING → STOPPED
```

```java
MohsLifecycle lc = mohs.lifecycle();
lc.state();                       // enum acima
lc.start();  lc.pause();  lc.resume();
lc.drain(Duration.ofSeconds(30)); // não claima mais; espera in-flight
lc.stop(Duration.ofSeconds(30));  // drain + desliga runners
```

```yaml
mohs:
  enabled: true                # false = auto-config inteira desligada
  lifecycle:
    start-mode: auto           # auto (default) | manual → mohs.lifecycle().start()
    shutdown:
      grace-period: 30s
```

Shutdown gracioso (SmartLifecycle, fase antecipada — o engine drena ANTES do
Spring fechar o DataSource):
1. SIGTERM → estado `DRAINING`; claim loop para; readiness cai;
2. in-flight tem até `grace-period` para concluir — **drain ≠ cancel**:
   nenhum sinal de cancelamento é enviado;
3. estouro do grace: interrupt pela maquinaria de timeout; attempt falha com
   causa `NodeShutdown` e segue o retry normal — at-least-once honesto até
   no desligamento;
4. runners desligam; só então o Spring fecha pools e contexto.

`start-mode: manual`: registro e validações acontecem no boot normalmente;
o engine aguarda `start()`. Casos: warm-up, leader election externa, canário
observador, test kit (que já opera assim).

Transições publicadas como `ApplicationEvent` do Spring (`MohsLifecycleEvent`)
— lifecycle é node-local e in-process, o escopo natural dos eventos Spring;
o bus de `ExecutionListener` permanece exclusivo do domínio de execução.

Operabilidade: health indicator `mohs` (estado + conectividade do store);
readiness reflete `DRAINING`; documentar `grace-period` lado a lado com
`terminationGracePeriodSeconds` do Kubernetes. `GET /nodes` já é v1 (ver
REST-API-DESIGN.md) — reusa o registro de heartbeat por node que a
liveness (seção seguinte) já precisa construir. Drain remoto
(`POST /nodes/{id}/drain`) fica no roadmap: mesmo registro, falta só o
endpoint de comando.

## Watchdog Bound — teto contra Attempt zumbi [DECIDIDO]

O motor renova a lease de toda Execution RUNNING a cada ciclo de poll — sem
isso, qualquer Attempt mais longo que a lease pareceria abandonado ao
reclaimer mesmo saudável. O Watchdog Bound é o teto opcional dessa proteção:
um Handler que engole o interrupt roda pra sempre — a JVM não mata — e
renovar a lease pra sempre faria o cluster inteiro esperar junto. Passado o
bound, o node para de renovar; a lease expira, o reclaimer trata o Attempt
como falho e a Retry Policy decide o resto. O zumbi pode ainda estar
rodando quando o retry começa — consistente com o contrato at-least-once; a
escrita terminal dele perde a CAS de versão em vez de corromper a do retry.

**Cluster-wide, não por Job [DECIDIDO — per-job avaliado e rejeitado].** A
renovação de lease é desenhada em lote — uma query cobrindo toda Execution
RUNNING do node de uma vez, o que mantém a tabela mais quente do sistema
barata de tocar a cada poll. Per-job quebraria esse lote: exigiria
aritmética de data dialect-specific na SQL de renovação (Postgres `INTERVAL`
≠ SQL Server `DATEADD`) ou uma coluna nova pré-calculada na Execution, em
todo backend de storage. O lever certo pra job lento já está especificado e
é mais barato: `timeout` do `@MohsJob`, avaliado em memória, sem tocar o
motor. Watchdog só entra depois que o `timeout` falhou (Handler nem assim
parou) — é rede de segurança de último caso, não afinação por job; um valor
cluster-wide maior que o timeout mais folgado do app + margem cobre bem.

```yaml
mohs:
  engine:
    lease-ttl: 30s
    watchdog-timeout: 10m   # null = sem teto (default); deve ser > lease-ttl
```

**Nome `lease-ttl`, não `liveness` [DECIDIDO].** "Liveness" é o termo
guarda-chuva do documento mestre pra heartbeat + lease + reaper juntos —
nomear o parâmetro assim perderia precisão. Mais importante: heartbeat de
node (só informativo — nenhuma lógica de claim/reclaim consulta) e lease de
Execution (funcional — é o que o reclaimer usa) são dois relógios distintos
por design. `lease-ttl` nomeia só o segundo. Intervalo do heartbeat de node
ainda não tem property — fica em aberto (`node-heartbeat-interval`,
provável nome, não decidido).

Estado da arte: nem Quartz nem JobRunr documentam um teto equivalente —
diferencial real do Mohs, não commodity.

## Invocação — sempre sobre uma definição existente

```java
// Referências tipadas (compilador valida o payload no ponto de chamada)
public final class Jobs {
    public static final JobRef<WelcomeEmail> WELCOME =
        JobRef.of("welcome-email", WelcomeEmail.class);
    public static final JobRef<ImportFile> IMPORT =
        JobRef.of("import-file", ImportFile.class);
}

// Um verbo; o terminal define o quando. Cadeia é @CheckReturnValue.
Enqueued e = mohs.schedule(Jobs.WELCOME, new WelcomeEmail("u1", "Ana", 31)).now();
// e.executionId(), e.scheduledAt() — recibo; nunca um future do resultado

mohs.schedule(Jobs.WELCOME, payload).at(instant);
mohs.schedule(Jobs.WELCOME, payload).after(Duration.ofHours(2));
mohs.schedule(Jobs.WELCOME, payload)
    .priority(Priority.HIGH).as("checkout-service").now();   // opções pré-terminal

mohs.batch("import-2026-08", b ->                            // lote flat
        files.forEach(f -> b.add(Jobs.IMPORT, new ImportFile(f))))
    .onCompletion(s -> mohs.schedule(Jobs.REPORT, new ImportReport(s.batchId())).now());
```

- Overload por string existe (`mohs.schedule("welcome-email", payload)`) com
  checagem do tipo em runtime contra a definição — erro claro, não CCE.
- Overrides por invocação: apenas o que é da instância (terminal `now/at/
  after`, `priority`, `as`, `idempotencyKey`). Política (retry, runner, queue) pertence à definição — invocação
  não redefine. [DECIDIDO — mantém uma fonte de verdade por job.]
- Invocar `JobRef` sem definição correspondente: falha imediata com sugestão
  de ids próximos. `JobRef`s referenciados em beans são checados no boot.

### Contrato assíncrono das invocações [DECIDIDO]

1. **Execução sempre assíncrona.** `schedule`/`batch` jamais
   executam o handler na thread do chamador — nem como atalho para jobs
   imediatos. Execução acontece num Runner, possivelmente em outro nó.
2. **Durabilidade sempre síncrona.** O retorno da chamada significa: o job
   está persistido e sob custódia do Mohs; o at-least-once começa aqui.
   Não existe modo fire-and-forget em memória — perder job em crash
   silenciosamente é a violação que este contrato proíbe por design.
3. **Retorno do terminal é recibo (`Enqueued`), nunca `Future` do resultado.** Um future
   de conclusão ofereceria exatamente o bloqueio que a regra 1 proíbe e
   acoplaria o chamador a uma execução remota. Observação de desfecho:
   eventos/listeners, `mohs.execution(id)` e dashboard. `awaitExecution`
   existe apenas no test kit.
4. **Transacional por participação.** Chamado dentro de transação ativa
   (mesmo DataSource), o insert do terminal entra na transação do chamador:
   commit publica, rollback apaga — transactional outbox nativo, sem broker.
   Sem transação ativa: auto-commit, mesma durabilidade.
5. **Admissão nunca espera capacidade.** Queue, rate limit e Runner limitam
   a execução (no claim), não o aceite. O terminal não bloqueia por fila
   cheia; p99 do terminal ≈ custo do insert (metrificado no BASELINE.md).

## Recursos nomeados: runners, queues, windows

### Runners — especificação, nunca Executor

```yaml
mohs:
  runners:
    io:   { mode: io,  max: 64 }   # built-in: virtual threads + semáforo
    cpu:  { mode: cpu, core-size: 4, max-size: 8, queue-capacity: 0, keep-alive: 60s }  # built-in: platform pool
    smtp: { mode: io,  max: 8 }    # bulkhead custom por integração
```

```java
@Bean MohsRunner s3Runner() { return MohsRunner.io("s3").maxConcurrent(32).build(); }
@Bean MohsRunner batchRunner() {
    return MohsRunner.cpu("batch").coreSize(4).maxSize(8)
        .queueCapacity(0).keepAlive(Duration.ofSeconds(60)).build();
}
```

[DECIDIDO — referência por nome estilo `@Async("...")`, mas o bean é um spec
(`MohsRunner`), não um `Executor`: o Mohs cria e é dono das threads — requisito
para cancelamento cooperativo, timeout por interrupt, métricas por runner e a
regra io→virtual/cpu→platform.]

[DECIDIDO EM ADR-0014 — as quatro properties de `cpu` espelham
`spring.task.execution.pool.*`, mas com defaults deliberadamente diferentes
dos do Spring: `max-size` default = núcleos disponíveis (pool fixo, não
elástico) e `queue-capacity` default 0 (direct handoff, não ilimitada) —
o Spring não sabe se o trabalho é CPU ou I/O-bound; `MohsRunner.CPU` sabe,
e "backpressure em toda borda... nunca espera infinita" já é regra do
projeto.]

### Queues e windows — bean define a estrutura, property ajusta o número

```java
@Bean
JobQueue emailsQueue() { return JobQueue.named("emails").maxConcurrent(5).build(); }

@Bean
ExecutionWindow businessDays() {
    return ExecutionWindow.named("business-days")
        .excludeWeekends()
        .excludeDates(feriadosNacionais2026())
        .excludeDaily(LocalTime.MIDNIGHT, LocalTime.of(6, 0))
        .exclude(custom -> minhaRegra(custom))   // predicado: só existe em código
        .build();
}
```

```yaml
mohs:
  queues:
    emails: { max-concurrent: 10 }   # property sobrescreve o número do bean
  rate-limits:
    smtp: { max: 100, window: 1m }
```

Semântica cluster-wide [DECIDIDO — precisa estar no manual]: queue/rate-limit
são estado compartilhado; a definição no boot é um upsert — em rolling deploy
com valores divergentes, o último nó a subir vence e o cluster converge
(sob `on-conflict: override`, o default; ver Ciclo de registro).

### Enforcement da queue [EM REVISÃO → ADR com gate de benchmark]

Papéis distintos, para nunca fundir: **runner** protege este nó (threads,
memória — local, sem banco); **queue** protege um recurso compartilhado
(SMTP, API parceira — escalar nós não pode escalar a pressão). Runner não
substitui queue; são eixos ortogonais.

O enforcement atual (`UPDATE ... WHERE running_count < max`) tem três modos
de falha: hot row (toda partida/conclusão serializa na mesma linha), bloat
(milhares de versões de tupla/s no Postgres) e drift do contador (nó morre
entre incremento e decremento → vaga vaza para sempre, sem reconciliação).

Proposta — **contagem derivada, sem contador**: a cláusula de claim verifica
`(SELECT count(*) ... WHERE queue=? AND status='RUNNING') < max`, servida por
índice parcial em `(queue) WHERE status='RUNNING'`. O conjunto RUNNING é
limitado pelo próprio max → contagem O(max) via index-only scan; claims são
batelados no poll (nós × frequência, não por execução). Sem estado mantido:
sem hot row, sem bloat, sem drift, sem job de reconciliação.

Semântica resultante, documentada: **soft cap** — overshoot transitório
≤ nós−1 sob claims simultâneos (adequado a proteção de recurso). Dependência
explícita: execuções RUNNING de nó morto seguram vaga até o reaper de órfãs
devolvê-las ao retry — o enforcement derivado se auto-cura via reaper
(o contador vazava permanentemente).

Gate: Fase 0/1 mede o contador atual sob carga-alvo (10k concorrentes,
queue quente); confirmada a contenção, troca-se o enforcement — a superfície
da API é agnóstica e não muda.

## JobContext

```java
public interface JobContext {
    JobKey jobKey();
    ExecutionId executionId();
    int attempt();                      // 1-based
    Instant scheduledAt();
    Instant firedAt();
    boolean cancellationRequested();    // cooperativo (cache 1s do motor)
    void progress(int done, int total); // opcional, dashboard
}
```

## Observação e extensão — Listeners e Interceptors [DECIDIDO]

Duas SPIs, porque observar e interceptar têm contratos opostos (a fusão dos
dois no `JobListener` do Quartz é o anti-exemplo):

### `ExecutionListener` — observar, nunca interferir

```java
public sealed interface ExecutionEvent
    permits Enqueued, Started, AttemptFailed, RetryScheduled,
            Succeeded, Failed, Cancelled, BatchCompleted { ... }

@Component
class OpsNotifier implements ExecutionListener {
    @Override public void on(ExecutionEvent e) {
        switch (e) {
            case Failed f when f.attemptsExhausted() ->
                slack.alert("%s esgotou retries: %s".formatted(f.jobKey(), f.error()));
            default -> { }
        }
    }
}

// Açúcar por método, filtrado (estilo @EventListener):
@OnExecution(job = "welcome-email", event = FAILED)
void alertOnFailure(Failed e) { ... }
```

Regras: exceção de listener é capturada, logada e metrificada — jamais afeta
o job; dispatch assíncrono em executor próprio de virtual threads
(`mohs-events`), com ordem preservada por execution; listener lento nunca
ocupa slot de Runner. Bridge para `ApplicationEvent` do Spring é opt-in
(`mohs.events.spring-bridge=true`).

### `ExecutionInterceptor` — envolver a execução

```java
@Component @Order(10)
class MdcInterceptor implements ExecutionInterceptor {
    @Override public void intercept(JobContext ctx, Chain chain) throws Exception {
        MDC.put("mohs.execution", ctx.executionId().toString());
        try { chain.proceed(); } finally { MDC.remove("mohs.execution"); }
    }
}
```

Roda na thread do attempt, em cadeia ordenada (`@Order`); é o lugar de MDC,
spans de tracing e contexto via `ScopedValue`. Exceção de interceptor É
falha do attempt e segue o fluxo normal de retry — quem está no caminho
crítico participa do desfecho.

### Garantias e dogfooding

- Eventos in-process são **best-effort**: crash entre persistir o desfecho e
  despachar pode perder o evento. Reação garantida não usa listener: o
  handler enfileira o job de continuação dentro da própria transação
  (cláusula 4 do contrato assíncrono) — listener observa, job encadeado
  reage.
- As integrações Micrometer e OpenTelemetry do Mohs são implementadas sobre
  estes dois SPIs. Se a observabilidade oficial precisar de hook que a SPI
  não oferece, a SPI está errada — corrige-se a SPI, não se abre porta
  interna.
- Entrega durável/cluster-wide (SSE, webhooks) nasce de uma tabela de
  eventos futura — item aberto no REST-API-DESIGN.md.

## Disciplina de interfaces fluentes

A API é fluente por padrão nos pontos de construção — com três regras que
separam DSL premium de armadilha:

1. **Fluente na configuração, plano no caminho quente.** Definição
   (`JobSpec`), invocação (`schedule(...).at(...)`) e recursos
   (`MohsRunner`, `JobQueue`, `ExecutionWindow`, `Retry`) são fluentes.
   `JobContext` e o handler são interfaces planas: DSL em hot path só polui
   stack trace e debug.
2. **Cadeia pura até o terminal.** Nenhum passo intermediário tem efeito
   colateral; registrar é papel do bean ou do `define(...)`, nunca do
   builder. Terminais anotados com `@CheckReturnValue` — cadeia abandonada
   (o bug clássico do builder sem `.build()`) vira warning de compilação.
3. **Staged builder onde há invariante estrutural — e só aí.** A exclusão
   mútua `cron` × `every` é garantida pelo compilador no programático:

```java
public sealed interface JobSpec permits ... {
    CronSpec cron(String expr, ZoneId zone);      // estágio sem every()
    IntervalSpec every(Duration interval);        // estágio sem cron()
    IntervalSpec everyAfterFinish(Duration d);
    OnDemandSpec onDemand();                      // explícito > ausência
}
// spec.cron("0 0 2 * * *", SP).every(...)  ← não compila
```

   Estágios são `sealed`: fechados para implementação externa, o que nos
   permite adicionar métodos em minor releases sem quebrar ninguém
   (compatibilidade binária — o calcanhar de aquiles das DSLs abertas).
   Runner/window/retry ficam com fluent simples + validação de boot: staged
   em tudo multiplica interfaces sem invariante que o justifique.

   Nota: a camada de annotation continua validando `cron` × `every` no boot
   (strings não têm compilador); o staged elimina a classe de erro apenas no
   programático — as duas camadas convergem na mesma regra.

## Tempo — fonte configurável [DECIDIDO]

Todo "agora" do motor passa por um único relógio injetado (`java.time.Clock`);
`Instant.now()`/`System.currentTimeMillis()` diretos são proibidos no código
do Mohs — regra verificada por teste de arquitetura. Três implementações da
mesma costura:

```yaml
mohs:
  time:
    source: application        # application (default) | database
    sync-interval: 30s         # amostragem do offset (modo database)
    skew-warn-threshold: 500ms # alerta de divergência app × banco
```

- **application** (default): relógio da JVM em UTC. Zero custo; pressupõe
  NTP saudável no cluster.
- **database**: o banco é a autoridade de tempo do cluster. Implementação
  por amostragem de offset (estilo NTP): a cada `sync-interval`, mede
  `SELECT now()` com compensação de ida-e-volta e aplica `app + offset` —
  leituras de tempo são locais e O(1), **nunca** uma round-trip por leitura.
  Clamp monotônico (reamostragem não anda para trás entre leituras); banco
  indisponível na amostragem → mantém último offset e avisa; leitura de
  tempo jamais bloqueia em I/O.
- **test** (`mohs-test`): `MutableClock` — a mesma costura que habilita
  `clock().advance(...)`.

Em qualquer modo, o offset app × banco é amostrado e exposto como métrica
(`mohs.time.offset`), com WARN acima de `skew-warn-threshold`: clock skew
deixa de ser silencioso mesmo para quem fica no default.

Onde a decisão já é SQL (claim `WHERE next_fire_at <= now()`), a autoridade
já é o banco por construção; o modo `database` alinha o lado da aplicação
(next fire, misfire) à mesma autoridade.

Disciplina de dois tempos: wall clock (o relógio acima) responde "quando";
durações (timeout de execução, benchmark) usam tempo monotônico
(`System.nanoTime`) — duração nunca é subtração de wall clock, ou o próprio
ajuste de offset viraria timeout fantasma.

## Validações de boot (fatais, com mensagem que ensina)

1. cron válido com próxima ocorrência computável; `zone` válida; `every` > 0;
2. `runner`/`queue`/`window`/`rateLimit`/`retryPolicy` referenciados existem —
   sugestão por distância de edição ("não encontrei a runner 'smpt'; você quis
   dizer 'smtp'?");
3. `id` duplicado (annotation × annotation, annotation × programático);
4. payload serializável: round-trip no boot;
5. assinatura de método suportada; `JobRef`s de beans resolvem para definição
   existente e tipo de payload compatível.

## Test kit (`mohs-test`)

```java
@MohsTest
class EmailJobsTest {
    @Autowired MohsTester mohs;

    @Test void firesAtTwoAm() {
        mohs.clock().setTo("2026-08-12T01:59:59-03:00");
        mohs.clock().advance(Duration.ofSeconds(2));
        assertThat(mohs.awaitExecution(Jobs.WELCOME, Duration.ofSeconds(5))
                       .status()).isEqualTo(SUCCEEDED);
    }

    @Test void runsOnDemand() {
        mohs.triggerNow(Jobs.WELCOME, new WelcomeEmail("u1", "Ana", 31));
    }
}
```

[DECIDIDO — exige `Clock` injetável no motor: entra no design de M3, §9 do
documento mestre.]

## Actor e regressão ergonômica assumida [DECIDIDO]

- **Actor:** aplicação registra `actor="application"` por padrão;
  REST/dashboard registram o actor resolvido pelo `ActorResolver`
  (principal com segurança; header declarativo sem ela); `.as(actor)`
  opcional para casos especiais. Trilha de "quem disparou" é inegociável.
- **Regressão ergonômica assumida:** one-off exige definição prévia
  (`@MohsJob` sob demanda) antes de `schedule().at()` — atrito deliberado
  em troca de payload tipado, políticas na definição e visibilidade no
  dashboard.

## Empacotamento — módulo único, full Spring Boot [DECIDIDO]

Um artefato: `io.mohs:mohs`. O motor usa a infraestrutura Spring livremente
(em particular, transações via `TransactionSynchronizationManager` — a
cláusula 4 do contrato assíncrono sai da infraestrutura, não de artesanato).
Aposta estratégica registrada: Quarkus/Micronaut/standalone ficam fora, e
reabrir essa porta no futuro será caro.

O que substitui a disciplina que o multi-módulo dava de graça:

1. **Fronteira por pacote, guardada por ArchUnit:**
   - `io.mohs` — fachada + identidade compartilhada (`Mohs`, `JobKey`, `JobRef`)
   - `io.mohs.schedule` · `io.mohs.definition` · `io.mohs.execution` ·
     `io.mohs.event` · `io.mohs.resource` — resto da API pública, dividida
     por concern **[DECIDIDO EM ADR-0013]** (revisa a versão anterior desta
     decisão, que descrevia `io.mohs` como pacote único)
   - `io.mohs.engine` · `io.mohs.jdbc` — internos (`@Internal`)
   - `io.mohs.autoconfigure` — auto-config, properties, validações de boot
   - `io.mohs.rest` — API REST/dashboard
   - `io.mohs.test` — test kit
   Regras de arquitetura no build: interno não vaza para a API pública
   (nenhum dos seis pacotes públicos); `rest` só enxerga a API pública;
   `test` não vaza para produção.
2. **Web opcional:** dependências de `spring-web` marcadas `<optional>`;
   REST/dashboard ativam via `@ConditionalOnClass` + `mohs.api.enabled`
   (padrão actuator). Teste de contrato: app sem web no classpath sobe.
3. **Test kit no jar**, pacote `io.mohs.test`, `spring-test` opcional —
   nenhum segundo artefato para o usuário gerenciar.

## Decisões em aberto → ADRs

1. Entrega durável/cluster-wide de eventos (tabela de eventos → SSE/webhooks)
   — o modelo in-process está decidido na seção de Listeners.
2. Fora de escopo da v1: DAG/workflows (batch flat + `onCompletion` cobre
   encadeamento simples), API reativa, dialetos cron alternativos, ports
   para outros frameworks (porta fechada pela decisão de empacotamento).

**Decidido (12/08/2026):**
- Serialização e versionamento de payload — compatibilidade entre deploys é
  obrigação do handler/aplicação, não do motor. Mohs garante round-trip de
  serialização no boot (validação 4); não garante, nem tenta migrar,
  Executions já persistidas contra um handler cujo payload mudou de forma.
  Quebra de contrato do lado da aplicação não tem rede de segurança do motor.
- Renames `JobQueue` (← `Queue`) e `ExecutionWindow` (← `Calendar`) —
  aprovados pelo PO. Nenhum item pendente restante neste documento.
