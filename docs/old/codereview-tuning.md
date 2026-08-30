# Code Review — Mohs: Java moderno, tuning de banco e tuning de JVM

**Data:** 2026-08-14
**Escopo:** codebase completa pós-remediação do review anterior (`src/main`, `src/test`,
os 4 schemas por dialeto, `../../pom.xml`, ADRs 0022-0025, `performance`), com três lentes
que `codereview.md` (2026-08-13) não cobriu nessa profundidade: (1) uso do estado da
arte do JDK 25, (2) tuning de banco de dados — queries, índices, isolamento, drivers —
como especialista, (3) tuning de JVM para a carga virtual-thread-heavy que o motor terá.
**Não repete** achados do review anterior (DB-1..12, PERF-1..8 etc.) nem o achado de índice
de claim já medido em `performance/BASELINE.md` (`(state, priority, scheduled_at)`,
+63% a +369% de throughput) — constrói a partir deles.
**Estágio:** M0-M2 entregues, M3 parcial (claim implementado e medido; dispatch, conclusão,
lease renewal, reaper e `io.mohs.autoconfigure` ainda não existem). Cada achado abaixo
declara explicitamente se é **problema real com evidência no código de hoje** ou
**recomendação para quando chegar em produção** — num projeto neste estágio, confundir os
dois seria desonesto nos dois sentidos.

---

## Sumário executivo

A remediação do review anterior foi real e verificada por leitura: o TOCTOU dos upserts tem
retry-como-UPDATE, o clamp do `DatabaseClock` virou `accumulateAndGet` atômico, o N+1 do
claim virou `findByIds` em lote com chunking abaixo do teto de 2100 parâmetros do SQL
Server, os 4 schemas por dialeto estão corretos onde o review anterior apontou erro
(NVARCHAR/DATETIME2/BIT no SQL Server, DATETIME/utf8mb4 no MySQL), e a suíte roda contra
Postgres/MySQL/SQL Server reais. O que segue são as camadas que só aparecem depois disso.

Os três achados mais importantes, um por eixo:

- **JVM-1 — `-Djdk.tracePinnedThreads` foi removida do JDK no 24.** A flag que o
  `../../CLAUDE.md` recomenda e que o documento mestre nomeia como ferramenta de validação da
  Fase 0 do baseline é um no-op silencioso no JDK 25 que o projeto usa (Temurin 25.0.4,
  per BASELINE.md). Pior: a premissa por trás dela — "`synchronized` em caminho bloqueante
  causa pinning do carrier" — deixou de ser verdade no JDK 24 (JEP 491). A doutrina de
  concorrência do projeto, incluindo o comentário da regra ArchUnit, cita um modo de falha
  que não existe mais no runtime alvo (JAVA-1).
- **DBTUNE-1 — todas as colunas temporais dos 4 schemas são "timestamp sem fuso", gravadas
  via `java.sql.Timestamp` — o valor persistido depende do fuso default da JVM de cada
  nó.** Num cluster onde dois nós rodem com `user.timezone` diferente (deploy heterogêneo,
  container sem TZ fixado), `scheduled_at`/`lease_expires_at` de nós distintos ficam
  incomparáveis entre si, com horas de skew — exatamente a classe de falha distribuída que
  o projeto declara como prioridade nº 1. Não é explorável hoje (teste = 1 JVM), é barato
  de fechar agora e caro depois do GA.
- **DBTUNE-3 — a hidratação do claim ainda paga 1 query de attempts por execução
  reivindicada, com uma segunda conexão do pool aberta enquanto o ResultSet externo ainda
  está sendo lido.** É o resíduo não fechado do PERF-1 (o fix em lote cobriu
  `mohs_executions`, não `mohs_attempts`) mais um risco novo: aquisição aninhada de
  conexão é a receita clássica de pool deadlock sob saturação. Provavelmente parte dos
  ~28ms/chamada que o BASELINE deixou como pergunta aberta.

No eixo Java, o veredito honesto é que a base **já está** no estado da arte onde importa —
records com invariantes, sealed + switch exaustivo, JSpecify consistente e verificado por
ArchUnit, virtual threads nomeadas nos testes — e o que resta são: uma doutrina de pinning
desatualizada (JAVA-1), um `default` de switch que engole valor persistido corrompido
(JAVA-4), a decisão explícita de **não** adotar `StructuredTaskScope` em `main` enquanto
for preview (JAVA-2, com o porquê registrado), e modernizações menores (`_`, Gatherers).

---

## Metodologia

Leitura direta e integral de `src/main/java/io/mohs/**` (jdbc + dialect, engine, core,
rest, test kit; `io.mohs.cron` tratado como vendorizado — auditado no review anterior
linha a linha contra o upstream, não re-auditado), dos 4 schemas, do `../../pom.xml`,
`application.yaml`, `ArchitectureTest`, dos testes de concorrência e harnesses, e dos
planos de execução salvos em `docs/performance/explain-*.txt`. Busca dirigida por padrões
(`synchronized`/`ThreadLocal`/`CompletableFuture`/idiomas pré-17/`get(0)`) em toda a
árvore. `git log` conferido para não re-reportar o que os 25 commits recentes já
corrigiram. Nenhum benchmark novo foi rodado nesta revisão — onde um achado prevê ganho de
performance, isso está dito como hipótese a medir com o harness existente
(`ClaimQueryLoadHarness`), nunca como número; a regra do projeto ("sem número, não é
otimização") vale para este documento também.

---

## 1. Java moderno (JDK 25)

### O que já está no estado da arte — verificado, sem ação

Registrado no estilo do DB-12 do review anterior, para que ninguém "descubra" isso de novo:

- **Sealed + pattern matching:** `ExecutionEvent` (8 variantes, listeners com switch
  exaustivo por contrato), `Schedule` (`CronSpec`/`IntervalSpec`/`OnDemandSpec`) com
  switches exaustivos sem `default` em `NextFireCalculator.nextFireAfter` e
  `JdbcJobStore.scheduleType` — variante nova quebra compilação, exatamente o uso que o
  JEP 441 quer.
- **Records com invariantes de verdade:** `Execution`, `Attempt`, `JobDefinition`,
  `StoredJob`, `BatchCounters`, `CursorPage` — compact constructors validando, `List.copyOf`
  defensivo, os achados API-2..API-11 do review anterior todos fechados por leitura.
- **JSpecify:** `@NullMarked` em todo `package-info` de produção, agora **verificado por
  ArchUnit** (`all_production_packages_declare_null_marked`), `@Nullable` só onde há null
  real. Nenhum achado novo de nulidade — exceto a nota menor JAVA-8.
- **Virtual threads:** nenhum uso em `main` ainda (motor não existe), e todos os fan-outs
  de teste já usam `Executors.newVirtualThreadPerTaskExecutor()` com latches/barriers, sem
  `Thread.sleep` de sincronização.
- **Blocos `synchronized`:** a lacuna admitida no comentário de
  `ArchitectureTest.java:79-86` (a regra só pega o modificador de método, não blocos) foi
  varrida manualmente nesta revisão — **zero blocos `synchronized` em `src/main`**. A
  lacuna da regra existe, o problema não.
- **Sequenced Collections (JDK 21):** nenhum `get(0)`/`get(size()-1)`/loop reverso manual
  em toda a base — não há o que migrar.
- **Idiomas pré-17:** nenhum switch statement antigo, nenhuma anonymous class onde caberia
  lambda, fora de `io.mohs.cron` (vendorizado do Spring de propósito — manter o shape do
  upstream barateia re-sincronização; modernizá-lo seria um erro, não uma melhoria).

### JAVA-1 — Doutrina de pinning desatualizada para o runtime alvo (JEP 491, JDK 24) — MÉDIO (doutrina), problema real hoje

- **Onde:** `../../CLAUDE.md` §Concorrência ("Proibido `synchronized` em caminho que bloqueia
  (I/O, sleep, lock): causa pinning do carrier"); `docs/MOHS-DOCUMENTO-MESTRE.md:58-65`
  (mesma regra + "validação de pinning com `-Djdk.tracePinnedThreads`");
  `src/test/java/io/mohs/ArchitectureTest.java:79-98` (comentário da regra
  `no_synchronized_methods_in_concurrency_critical_code` justifica pela "pinning do
  carrier em virtual threads").
- **Problema:** desde o JDK 24 (JEP 491, *Synchronize Virtual Threads without Pinning*),
  `synchronized` e `Object.wait()` **não pinam mais** o carrier — a virtual thread
  desmonta normalmente dentro de monitor. No JDK 25 que o projeto usa, os casos restantes
  de pinning são frames nativos (JNI) e inicializadores de classe. A regra do projeto foi
  escrita para o mundo JDK 21-23 e nunca revisitada.
- **Impacto concreto:** (a) engenheiros de M3 tomarão decisões de design (ex.: descartar
  uma biblioteca porque ela usa `synchronized` internamente, ou envolver HikariCP em
  workarounds) para evitar um problema que o runtime alvo não tem; (b) o argumento de
  autoridade do comentário ArchUnit está factualmente errado, o que corrói a confiança nas
  regras que estão certas.
- **O que NÃO fazer:** não é motivo para apagar a regra. `ReentrantLock` continua
  preferível em caminho de I/O pelos motivos de JCIP cap. 13 que sempre valeram —
  `tryLock` com timeout, aquisição interruptível, fairness opcional, `Condition` múltiplas
  — e o `../../CLAUDE.md` já cita esses ganhos. A correção é trocar a **justificativa** (de
  "pinning" para "capacidades de lock explícito"), não o comportamento.
- **Correção:** atualizar os três textos citados; na regra ArchUnit, reescrever o Javadoc.
  Ver JVM-1 para a metade "flag de diagnóstico" deste mesmo problema.

### JAVA-2 — `StructuredTaskScope` (JEP 505): não adotar em `main` enquanto preview — decisão a registrar, não lacuna — MÉDIO

- **Onde os candidatos estão:** os fan-outs concorrentes de teste —
  `src/test/java/io/mohs/jdbc/JdbcClaimerTest.java:274-288` e `:305-319`
  (`ExecutorService` + `Future.get(timeout)` + `CyclicBarrier`), o mesmo padrão nos
  quatro `JdbcClaimer*Test` por dialeto e nos dois harnesses de carga. Em `main`, nenhum
  fan-out existe ainda (o dispatcher é M3).
- **Análise:** o `../../CLAUDE.md` pede `StructuredTaskScope` para fan-out — mas JEP 505 é
  **preview** no JDK 25 (quinta iteração da API, redesenhada para
  `StructuredTaskScope.open()`/`Joiner`). Class files compilados com `--enable-preview`
  exigem `--enable-preview` no runtime **da mesma feature release** — para uma biblioteca
  embarcada, isso forçaria toda aplicação hospedeira a ligar preview e travar no JDK 25
  exato. Inaceitável para o artefato `io.mohs:mohs`; portanto a regra do CLAUDE.md é
  literalmente insatisfazível em `main` hoje, e ninguém escreveu isso em lugar nenhum.
- **Sobre os testes:** adotar preview só na suíte é possível (compiler/surefire `argLine`),
  mas o ganho sobre o padrão atual (latch + futures com timeout, correto e determinístico)
  não paga a fricção de build — e a API ainda pode mudar de novo antes de finalizar.
- **Correção sugerida:** registrar a decisão numa linha do `../../CLAUDE.md` ("fan-out com
  `StructuredTaskScope` **quando finalizar**; até lá, `ExecutorService` + futures com
  timeout é o padrão aceito") e — mais importante — desenhar o poll/dispatch loop de M3 já
  no formato estruturado (um escopo lógico por ciclo de poll, cancelamento cooperativo
  descendo pela árvore de subtarefas, `JobContext.cancellationRequested()` como o sinal),
  de modo que a migração seja mecânica quando a API for final.

### JAVA-3 — `ScopedValue` (JEP 506, final no 25): plano já existe, hora certa é M3 — INFO, sem ação agora

- **Onde:** `src/main/java/io/mohs/core/event/ExecutionInterceptor.java:8` já promete
  propagação implícita de contexto "via `ScopedValue`"; a regra ArchUnit
  `no_thread_local_in_concurrency_critical_code` (`ArchitectureTest.java:100-104`) já
  bloqueia a alternativa errada.
- **Análise:** procurei contexto que hoje viaja parâmetro-a-parâmetro e se beneficiaria:
  o candidato real é o trio actor/executionId/attempt dentro do futuro dispatcher (para
  listeners, interceptors e MDC de logging). No código de hoje, `actor` atravessa no
  máximo um nível (REST handler → domínio) e `JobContext` é parâmetro explícito do handler
  **por design correto** — `ScopedValue` é para o contexto ambiente dos interceptors, não
  para substituir o parâmetro do handler. Nada a fazer antes do dispatcher existir;
  o único cuidado a registrar para M3: fazer o *binding* (`ScopedValue.where(...).run(...)`)
  no topo da virtual thread da tentativa, para que a herança automática em subtarefas
  estruturadas (JEP 505+506 se integram) venha de graça.

### JAVA-4 — `default -> new OnDemandSpec()` engole `schedule_type` corrompido — MÉDIO, problema real

- **Onde:** `src/main/java/io/mohs/jdbc/JdbcJobStore.java:244-248` (`mapRowOrNull`):

  ```java
  Schedule schedule = switch (rs.getString("schedule_type")) {
      case "CRON" -> ...;
      case "INTERVAL" -> ...;
      default -> new OnDemandSpec();
  };
  ```
- **Problema:** o `default` trata "qualquer coisa que não seja CRON/INTERVAL" como
  `ON_DEMAND` — incluindo um valor corrompido, truncado ou escrito por uma versão futura.
  Inconsistente com as duas linhas abaixo no mesmo método: `Misfire.valueOf(...)` e
  `DefinitionSource.valueOf(...)` lançam `IllegalArgumentException` em lixo, como devem.
  Um job cron cuja linha degrade vira silenciosamente um job que **nunca mais dispara
  sozinho** — o modo de falha mais difícil de diagnosticar às 3h da manhã, exatamente o
  que a coluna comenta que não deveria acontecer (a ADR-0006/DUP-3 criou o mecanismo
  ORPHANED para "nunca sumir em silêncio"; isto é o mesmo espírito violado).
- **Correção:** `case "ON_DEMAND" -> new OnDemandSpec(); default -> throw new
  IllegalStateException("unknown schedule_type '" + ... + "' for job '" + jobKey + "'")` —
  ou, se preferir degradar em vez de falhar a leitura inteira, rotear pro mesmo mecanismo
  ORPHANED que `handler_type` não resolvido já usa.

### JAVA-5 — Variáveis não usadas onde o JDK 22+ tem `_` — BAIXO, modernização

- **Onde (verificado ocorrência a ocorrência):**
  - `rowNum` nunca usado nos row mappers lambda: `JdbcExecutionStore.java:105,116,121`,
    `JdbcJobStore.java:154`, `JdbcRateLimitStore.java:59`; e nos `mapCandidate` por
    referência de método dos 4 dialetos (`PostgresJdbcDialect.java:36`,
    `MySqlJdbcDialect.java:36`, `H2JdbcDialect.java:36`, `SqlServerJdbcDialect.java:41`)
    — nesses, o parâmetro do método privado pode virar `_` também (JDK 22 permite em
    parâmetro de lambda; em método, mantém o nome, só nos lambdas vale a troca).
  - Bindings de pattern nunca usados: `JdbcJobStore.java:219-222` (`case CronSpec cron ->
    "CRON"` etc. — os três) e `NextFireCalculator.java:45` (`case OnDemandSpec onDemand`)
    → `case CronSpec _`, `case OnDemandSpec _`.
  - Exceções capturadas e não usadas: `catch (DuplicateKeyException e)` em
    `JdbcJobStore.java:130` e `JdbcRateLimitStore.java:41` → `catch (DuplicateKeyException _)`
    — aqui o `_` **adiciona** informação: declara ao leitor que ignorar a exceção é o
    protocolo (perdeu a corrida do INSERT, o UPDATE seguinte resolve), não um catch vazio
    esquecido.
  - Chaves não usadas nos lambdas de `InMemoryJobStore.java:28,50,55,60,72,85`
    (`(key, existing)`/`(k, stored)` → `(_, stored)`).
- **Impacto:** só legibilidade — `_` comunica "não usado por contrato" e cala inspeções de
  IDE. Nenhuma urgência; bom lote para um commit cosmético único.

### JAVA-6 — `Gatherers.windowFixed` para o chunking de `findByIds` — BAIXO, opcional

- **Onde:** `src/main/java/io/mohs/jdbc/JdbcExecutionStore.java:100-106` — loop `for` com
  `subList(start, min(...))` particionando ids em lotes de `MAX_IDS_PER_QUERY`.
- **Análise:** é exatamente a forma que `Gatherers.windowFixed(MAX_IDS_PER_QUERY)`
  (JEP 485, final no JDK 24) expressa em uma linha:
  `rawIds.stream().gather(Gatherers.windowFixed(MAX_IDS_PER_QUERY)).forEach(chunk -> ...)`.
  Trade-off honesto: o loop imperativo atual é claro e não aloca à toa; o gatherer ganha
  em declarar a intenção ("janelas fixas") no nome. Vale na próxima vez que o método for
  tocado, não como commit dedicado. Fora este ponto, **não há outros candidatos fortes a
  Gatherers na base** — os pipelines existentes são simples demais para se beneficiarem.

### JAVA-7 — Quatro `instanceof`-ternários consecutivos sobre um tipo sealed — BAIXO

- **Onde:** `src/main/java/io/mohs/jdbc/JdbcJobStore.java:72-75` — quatro testes
  `definition.schedule() instanceof CronSpec cron ? ... : null` em sequência para extrair
  `cronExpression`/`cronZone`/`intervalDuration`/`intervalAfterFinish`.
- **Problema:** quatro dispatches sobre o mesmo valor sealed onde um único switch com
  record pattern (`case CronSpec(String expression, ZoneId zone) -> ...`) produziria as
  quatro colunas de uma vez — e ganharia a checagem de exaustividade que os ternários não
  têm (um `WeeklySpec` futuro passa despercebido aqui, virando 4 `null`s silenciosos,
  enquanto `scheduleType()` logo abaixo quebraria a compilação). É o mesmo padrão que o
  próprio arquivo já usa certo em `scheduleType` (`:218-224`).
- **Correção:** extrair um `record ScheduleColumns(...)` local ou preencher os quatro
  `addValue` dentro de um switch exaustivo único. Cosmético hoje, estrutural no dia em que
  `Schedule` ganhar a quarta variante.

### JAVA-8 — `transactionTemplate.execute()` é `@Nullable`; retorno repassado como não-nulo — BAIXO

- **Onde:** `src/main/java/io/mohs/jdbc/JdbcClaimer.java:132` — `return
  transactionTemplate.execute(status -> claimWithinTransaction(nodeId, batchSize));` num
  método que declara `List<String>` não-nulo sob `@NullMarked`.
- **Análise:** `TransactionCallback` devolvendo lista nunca-nula torna isso seguro em
  runtime, mas o contrato Spring de `execute` é `@Nullable T` — qualquer análise JSpecify
  estrita marca a linha. `Objects.requireNonNull(transactionTemplate.execute(...))`
  documenta o invariante de graça. Única inconsistência de nulidade nova encontrada.

### JAVA-9 — `MutableClock.advance()` é read-modify-write não atômico sobre `volatile` — BAIXO

- **Onde:** `src/main/java/io/mohs/test/MutableClock.java:34-37`
  (`this.now = this.now.plus(duration)`).
- **Problema:** JCIP §2.2 — compound action sobre `volatile`: dois `advance()` concorrentes
  podem perder um incremento. `MutableClock` é API pública do jar (test kit) e testes de
  concorrência são o público-alvo dele. Baixíssima probabilidade de morder (quem avança o
  relógio costuma ser o orquestrador do teste, single-threaded), mas um relógio de teste
  que perde ticks sob concorrência produz exatamente o flake não-determinístico que o
  test kit existe para eliminar.
- **Correção:** `AtomicReference<Instant>` com `updateAndGet(n -> n.plus(duration))` —
  mesma correção, mesmo motivo, do CONC-3 já aplicado ao `DatabaseClock`.

---

## 2. Banco de dados — queries, índices, isolamento, drivers

### Verificado sem achado — para não redescobrir

- **`mohs_batches` e `mohs_rate_limits`:** todo acesso é por PK (`find`/`increment*` por
  `id`/`name`) — nenhum índice faltando, nenhum sobrando.
- **`mohs_job_definitions`:** o join do claim e todos os `WHERE job_key` usam o índice
  UNIQUE de `job_key`; o plano MySQL pós-índice confirma `Single-row index lookup ...
  using job_key` (`explain-mysql.txt:5`). (O `Seq Scan` de `j` no plano Postgres,
  `explain-postgresql.txt:12`, é escolha de custo sobre tabela de 2 linhas do harness —
  com cardinalidade real o planner troca pro índice; não é achado.)
- **`sendStringParametersAsUnicode` (mssql-jdbc):** o default `true` envia parâmetros como
  NVARCHAR e o schema SQL Server é NVARCHAR em tudo — **a combinação atual está correta**:
  seek nos índices, sem conversão implícita. Registrado porque a "dica de tuning" mais
  repetida da internet para esse driver é setar `false` — aqui isso **quebraria** os seeks
  (conversão implícita da coluna NVARCHAR). Ver DBTUNE-10 para deixar isso escrito onde o
  autoconfigure vai nascer.
- **PERF-3 (HikariCP) segue aberto**, como esperado: `application.yaml` continua com 3
  linhas e `io.mohs.autoconfigure` continua só com `package-info.java`. Não re-registrado;
  DBTUNE-3 adiciona um dado novo ao dimensionamento (aquisição aninhada de conexão).
- **Nota de passagem (extensão do DB-9, não um achado novo):** desde a divisão em
  `schema-{platform}.sql`, nada define `spring.sql.init.platform` — hoje **nenhum** schema
  é aplicado automaticamente, nem no H2 embarcado; só os `*TestSupport` aplicam. A decisão
  de deploy do DB-9 ficou ainda mais urgente para M3, mas é a mesma decisão já registrada.

### DBTUNE-1 — Colunas temporais sem fuso + bind via `java.sql.Timestamp` = valor gravado depende do fuso default da JVM do nó — ALTO (latente; correção barata agora, cara depois do GA)

- **Onde (schema):** todas as colunas temporais dos 4 dialetos —
  `schema-postgresql.sql:28-29,37,46-47,52,56` e `schema-h2.sql` (`TIMESTAMP` =
  *without time zone*), `schema-mysql.sql:36-37,45,54-55,60,64,74-75` (`DATETIME`),
  `schema-sqlserver.sql` (`DATETIME2`). **Onde (bind):** os 10 call sites de
  `Timestamp.from(...)` — `JdbcExecutionStore.java:67-72`, `JdbcClaimer.java:183`,
  `JdbcJobStore.java:65`, `JdbcBatchStore.java:38` e os 4 dialetos
  (`PostgresJdbcDialect.java:18`, `MySqlJdbcDialect.java:18`, `H2JdbcDialect.java:18`,
  `SqlServerJdbcDialect.java:25`).
- **Problema:** `PreparedStatement.setTimestamp(Timestamp)` sem `Calendar` renderiza o
  instante **no fuso default da JVM** ao gravar num tipo sem fuso, e o driver reverte com
  o mesmo fuso na leitura. Dentro de uma JVM, o round-trip fecha; entre JVMs com
  `user.timezone` diferente, não: o nó A (UTC) grava `scheduled_at = 12:00`, o nó B
  (America/Sao_Paulo) lê e compara como se fosse 12:00 **local** — 3h de erro em
  `scheduled_at <= :now`, no lease e na ordenação do claim. Cluster heterogêneo de fuso
  não é cenário exótico: basta um container sem `TZ` fixado ao lado de um host legado.
  O `DatabaseClock` mitiga o *clock skew* físico, mas não isso — o erro aqui é de
  **codificação do valor**, não do relógio (e a própria amostragem
  `SELECT CURRENT_TIMESTAMP` → `getTimestamp()` participa da mesma dependência de fuso da
  sessão/JVM nos dialetos sem fuso).
- **Por que é a hora certa:** não há consumidor externo do jar (o mesmo raciocínio que as
  ADRs 0022/0023 usaram para mudar shape sem migração). Depois do GA isso vira migração de
  dado com janela.
- **Correção sugerida (duas camadas, uma decisão):**
  1. **Contrato:** decidir e documentar no schema que toda coluna temporal guarda UTC.
  2. **Bind:** trocar `Timestamp.from(instant)` por
     `OffsetDateTime.ofInstant(instant, ZoneOffset.UTC)` via `setObject` (JDBC 4.2 — os 4
     drivers suportam) **ou** manter `Timestamp` com `Calendar` UTC explícito. No
     Postgres, considerar `TIMESTAMPTZ` (mesmos 8 bytes, semântica de instante nativa —
     elimina a classe de erro por tipo, não por disciplina); nos demais dialetos o tipo
     atual fica, só o bind muda.
  3. Enquanto isso não for feito, o requisito "todos os nós com o mesmo fuso (ideal: UTC)"
     precisa estar escrito em algum lugar que um operador leia — hoje não está em nenhum.

### DBTUNE-2 — MySQL: `DATETIME` sem precisão fracionária = granularidade de 1 segundo, divergindo dos outros 3 dialetos — MÉDIO-ALTO, problema real

- **Onde:** `schema-mysql.sql:36-37,45,54-55,60,64,74-75` — todas as 8 colunas temporais
  são `DATETIME` puro, que no MySQL significa `DATETIME(0)`.
- **Problema:** Postgres/H2 guardam microssegundos e SQL Server `DATETIME2` guarda 100ns —
  MySQL, do jeito que está, **arredonda para o segundo inteiro** (e MySQL arredonda, não
  trunca: um `scheduled_at` `...500ms` pode ser gravado até 500ms **no futuro**). Efeitos
  concretos: (a) `ORDER BY scheduled_at` do claim vira loteria dentro do mesmo segundo —
  sob rajada, centenas de execuções empatam e a ordem de disparo deixa de ser a de
  agendamento; (b) `lease_expires_at` ganha ±0,5s de erro sistemático que o futuro reaper
  herdará; (c) qualquer teste de round-trip com instantes sub-segundo passa nos outros 3
  dialetos e falha (ou pior: passa por coincidência de arredondamento) no MySQL.
- **Correção:** `DATETIME(6)` nas 8 colunas — paridade de microssegundo com Postgres, 3
  bytes a mais por valor, sem qualquer outra mudança (Connector/J já envia frações por
  default). Um `SchemaMySqlRoundTripTest` com instante de precisão de microssegundo fecha
  a regressão.

### DBTUNE-3 — Resíduo do PERF-1: hidratação do claim ainda faz 1 query de attempts POR execução, com conexão aninhada — ALTO, problema real no hot path

- **Onde:** `src/main/java/io/mohs/jdbc/JdbcClaimer.java:114` chama
  `executionStore.findByIds(...)`; `JdbcExecutionStore.mapRow` (`:131-142`) chama
  `fetchAttempts(id)` (`:144-149`) **por linha**, de dentro do row mapper — ou seja,
  enquanto o ResultSet da query externa ainda está aberto.
- **Problema (duas camadas):**
  1. **Round-trips:** o fix do PERF-1 (commit `e971406`) tornou a busca de
     `mohs_executions` em lote, mas cada linha mapeada ainda dispara
     `SELECT * FROM mohs_attempts WHERE execution_id = :id` — para um claim de
     `batchSize = 20`, são 20 queries extras **que retornam sempre 0 linhas** (uma
     execução recém-reivindicada saiu de `ENQUEUED`; `insert` exige `attempts` vazio).
     Somado ao já registrado no BASELINE (até 20 UPDATEs sequenciais por chamada), o claim
     de 20 faz hoje ~42 round-trips. O BASELINE deixou como pergunta aberta de onde vêm os
     ~28ms/chamada remanescentes com o `SELECT` custando 0,13ms — esta é a metade da
     resposta que ainda não estava anotada (a outra metade, os UPDATEs, já está na
     recomendação 2 de lá).
  2. **Aquisição aninhada de conexão:** a query interna roda fora de transação, então
     `DataSourceUtils` pega uma **segunda** conexão do pool enquanto a primeira segue
     presa ao ResultSet externo. N pollers concorrentes precisando de 2 conexões cada é a
     receita clássica de pool deadlock sob saturação (HikariCP documenta a fórmula
     `pool ≥ Tn × (Cm − 1) + 1`); o `connectionTimeout` baixo que o CLAUDE.md pede
     transformaria o deadlock em rajada de timeouts — melhor, mas ainda é falha. Esse
     requisito de "2 conexões por claim" precisa entrar no dimensionamento do PERF-3
     quando o autoconfigure nascer — ou, melhor, desaparecer:
- **Correção:** uma query de attempts em lote **depois** que o cursor externo fechou —
  `SELECT * FROM mohs_attempts WHERE execution_id IN (:ids) ORDER BY execution_id, number`,
  agrupada em memória (mesmo chunking de `MAX_IDS_PER_QUERY`) — resolve as duas camadas de
  uma vez e serve igualmente o futuro `findByJobKey`/`findAll` do wiring REST (o
  comentário em `JdbcExecutionStore.java:124-130` já previa "medir com um caller de
  verdade"; o caller de verdade existe desde o claim e é o hot path). *Não* otimizar
  assumindo "attempts de claim são sempre vazios": com retry (M3), execuções
  `RETRY_SCHEDULED → ENQUEUED` voltarão ao claim **com** attempts — o batch é a forma que
  continua correta nesse futuro.

### DBTUNE-4 — Isolamento da transação de claim não é fixado; MySQL roda em REPEATABLE READ por default — MÉDIO-ALTO

- **Onde:** `src/main/java/io/mohs/jdbc/JdbcClaimer.java:91` — `new TransactionTemplate(
  new DataSourceTransactionManager(dataSource))` sem `setIsolationLevel`; herda o default
  do banco: READ COMMITTED em Postgres/SQL Server/H2, **REPEATABLE READ no InnoDB**.
- **Problema:** o único bloco transacional multi-statement do sistema roda com semântica
  diferente por dialeto sem que nenhuma linha de código declare isso. Em RR, locking reads
  (`FOR UPDATE`) tomam next-key/gap locks sobre a faixa varrida do índice de claim —
  bloqueiam INSERTs de execuções novas na vizinhança e criam uma classe de deadlock que
  READ COMMITTED (só record locks nas linhas casadas) não tem. Honestidade com o baseline:
  os contadores `Innodb_row_lock_*` medidos deram delta zero na escala testada, então
  **não** afirmo que isso explica o throughput menor do MySQL — o que afirmo é divergência
  semântica real e risco que cresce com densidade de inserts concorrentes ao claim.
- **Correção:** `transactionTemplate.setIsolationLevel(TransactionDefinition.
  ISOLATION_READ_COMMITTED)` no construtor do `JdbcClaimer` — uma linha, e os 4 dialetos
  passam a rodar o claim sob o mesmo contrato de isolamento em que ele foi raciocinado
  (ADR-0018 pensa em termos de "última escrita guardada vence", que é semântica RC).
  Aproveitar e registrar em Javadoc a nota do review anterior sobre RCSI/Azure SQL
  (`READPAST` + `UPDLOCK` seguem funcionando sob RCSI — os hints forçam locking mesmo com
  row versioning; vale o teste real quando Azure entrar no escopo).

### DBTUNE-5 — Índice de claim indexa a tabela inteira para servir uma query que só quer `ENQUEUED`: índice parcial/filtrado em Postgres e SQL Server — ALTO como recomendação de produção (medir antes)

- **Onde:** `schema-postgresql.sql:58` e `schema-sqlserver.sql:69-70` —
  `idx_mohs_executions_claim (state, priority, scheduled_at)` cobre **todas** as linhas,
  mas `mohs_executions` é append-only por desenho (linhas terminais nunca saem — premissa
  registrada desde o review anterior) e a claim query só toca a fatia `state = 'ENQUEUED'`,
  que tende a ~0% da tabela em regime.
- **Problema/oportunidade:** cada execução entra no índice no INSERT, é **re-inserida** a
  cada transição de estado (o UPDATE muda `state`, coluna-chave do índice) e fica lá para
  sempre como entrada morta-para-o-claim. Em Postgres e SQL Server existe a ferramenta
  exata para isso:
  - Postgres: `CREATE INDEX ... ON mohs_executions (priority, scheduled_at) WHERE state =
    'ENQUEUED'` — o predicado sai do índice para a cláusula WHERE do DDL; o índice contém
    só o backlog vivo, a ordenação `(priority, scheduled_at)` continua casando o `ORDER BY`
    sem sort, e escritas de transição para estado terminal **removem** a entrada em vez de
    acumular.
  - SQL Server: filtered index equivalente (`WHERE state = 'ENQUEUED'`) — utilizável aqui
    porque o predicado da query é literal no SQL do dialeto, não variável (limitação
    clássica de filtered index não se aplica).
  - MySQL/H2: não têm índice parcial — mantêm o composto atual; é exatamente o tipo de
    divergência que a estrutura de um-schema-por-dialeto (ADR-0023) existe para absorver.
- **Honestidade:** na escala do harness (3000 linhas) não haverá diferença mensurável — o
  ganho é assintótico (índice O(backlog) em vez de O(história)); o custo de write
  amplification evitado aparece com milhões de linhas terminais. Gate: rodar o
  `ClaimQueryLoadHarness` com história acumulada (não só backlog limpo) antes/depois, e
  registrar nova seção no BASELINE — a regra do projeto pede o número, e o cenário atual
  do harness não exercita a diferença.

### DBTUNE-6 — `SELECT *` na hidratação transfere `payload` inteiro e o descarta — MÉDIO, problema real no hot path

- **Onde:** `src/main/java/io/mohs/jdbc/JdbcExecutionStore.java:87,103,114,121` — os
  quatro finders fazem `SELECT * FROM mohs_executions`, mas `mapRow` (`:131-142`) lê só
  7 colunas — nunca `payload`, `payload_type`, `idempotency_key`, `priority`, `node_id`,
  `lease_expires_at`, `batch_id`, `created_at`.
- **Problema:** `payload` é `TEXT`/`NVARCHAR(MAX)` de tamanho arbitrário (JSON do job). No
  caminho do claim, cada poll transfere até `batchSize` payloads completos pela rede — com
  de-TOAST no Postgres para valores grandes — para jogá-los fora no mapper. É custo por
  linha reivindicada, a categoria de custo que a ADR-0009 manda evitar. Também acopla os
  finders a qualquer coluna futura (um `ALTER TABLE ADD` engorda todas as leituras sem
  ninguém decidir isso).
- **Correção:** lista explícita das 7 colunas mapeadas nos quatro SELECTs. Quando o
  dispatch (M3) precisar do payload, ele será uma leitura deliberada — o Javadoc de
  `ExecutionStore` (`engine/ExecutionStore.java:20-22`) já diz que payload não é campo de
  `Execution` e que carregá-lo é decisão de quem consome; o `SELECT *` atual contradiz o
  contrato que o próprio arquivo declara.

### DBTUNE-7 — O contrato "Stream sobre cursor aberto" não é verdade nos drivers reais sem fetch size — MÉDIO (vira real no wiring REST de M3)

- **Onde:** a promessa: `src/main/java/io/mohs/engine/ExecutionStore.java:41-48` e
  `engine/JobStore.java` ("Stream sobre um cursor aberto — não materializa em memória de
  uma vez"). As implementações: `queryForStream` em `JdbcExecutionStore.java:113-122`,
  `JdbcJobStore.java:153`, `JdbcRateLimitStore.java:59`, sem `fetchSize` configurado em
  nenhum template.
- **Problema:** `queryForStream` itera preguiçosamente **sobre o ResultSet**, mas quem
  decide se o ResultSet é um cursor de verdade é o driver:
  - **pgjdbc:** sem `fetchSize > 0` **e** `autoCommit = false`, o driver materializa o
    resultado inteiro em memória no `executeQuery` — o Stream vira uma ilusão; um
    `findAll()` na tabela mais quente do sistema é um OOM esperando o wiring REST chamar.
  - **Connector/J:** idem — streaming só com `fetchSize = Integer.MIN_VALUE` ou
    `useCursorFetch=true` + fetch size positivo.
  - **mssql-jdbc:** `responseBuffering=adaptive` é default — este se comporta.
- **Correção:** definir `fetchSize` nos `JdbcTemplate`/`NamedParameterJdbcTemplate` dos
  stores (ex.: 100-500) e documentar que os finders-stream exigem execução dentro de
  transação (autocommit off) no Postgres — ou rebaixar honestamente o Javadoc do contrato
  enquanto isso não for verdade, para o wiring de M3 não construir em cima de uma garantia
  que não existe. Combina com a paginação por cursor do REST (`CursorPage`), que tende a
  tornar `findAll`-stream desnecessário no caminho HTTP — decisão de M3, registrada aqui.

### DBTUNE-8 — `idempotency_key`: índice comum não sustenta o contrato de Idempotent Receiver, e cada dialeto exige uma grafia — MÉDIO (decidir antes do wiring M3)

- **Onde:** `schema-*.sql`, índice `idx_mohs_executions_idempotency_key` (ex.:
  `schema-postgresql.sql:60`), coluna NULLable e ainda sem escrita em código (nem
  `JdbcExecutionStore.insert` a preenche — o wiring do `Idempotency-Key` REST é M3).
- **Problema:** dedupe à prova de corrida exige unicidade **no banco** (Idempotent
  Receiver, EIP — check-then-insert em aplicação reintroduz TOCTOU, a lição do CONC-2).
  O índice atual é não-único, então hoje ele só acelera um lookup que ninguém faz, e cada
  linha inserida (quase todas com chave NULL) o engorda. E a unicidade correta diverge por
  dialeto de um jeito que vale registrar **antes** de alguém escrever o genérico errado:
  - Postgres: `CREATE UNIQUE INDEX ... ON mohs_executions (job_key, idempotency_key)
    WHERE idempotency_key IS NOT NULL` — parcial: NULLs fora do índice, índice mínimo.
  - SQL Server: filtered unique index com o mesmo `WHERE` é **obrigatório**, não
    otimização — unique não-filtrado trata NULL como valor e rejeita a segunda linha NULL.
  - MySQL: unique comum basta (NULLs duplicados são permitidos por spec no InnoDB), sem
    suporte a índice parcial.
  - Escopo `(job_key, idempotency_key)` em vez de global é a decisão de produto implícita
    no contrato REST (chave idempotente por job) — se for global, ajustar; o ponto é
    decidir explicitamente agora.
- **Correção:** trocar o índice comum pela variante única por dialeto quando o wiring
  entrar (mesmo commit), e mapear a `DuplicateKeyException` resultante para a resposta
  idempotente (retornar a execução existente), não para erro.

### DBTUNE-9 — Postgres: o ciclo de vida das execuções derrota HOT updates — fillfactor e autovacuum precisam entrar no schema antes de produção — recomendação de produção

- **Onde:** `schema-postgresql.sql:42-61` — `mohs_executions` com 4 índices;
  toda execução sofre ≥ 2 UPDATEs no ciclo (`ENQUEUED → RUNNING → terminal`), e todo
  UPDATE toca `state`, que é coluna indexada.
- **Problema:** update que toca coluna indexada nunca é HOT no Postgres — cada transição
  grava uma nova versão da linha **e** novas entradas em **todos os quatro** índices
  (inclusive `job_key`/`batch_id`/`idempotency_key`, cujas colunas não mudaram), deixando
  as versões antigas como dead tuples para o autovacuum. Na tabela mais quente do sistema,
  com defaults de autovacuum pensados para OLTP genérico, isso é bloat de índice e de heap
  crescendo com o throughput — o modo de degradação lenta que aparece na semana 3 de
  produção, não no teste.
- **Correção (documentar no próprio schema-postgresql.sql, aplicar quando houver ambiente
  de produção para medir):** `ALTER TABLE mohs_executions SET (fillfactor = 85)` (dar
  espaço na página para versões novas), autovacuum por tabela mais agressivo
  (`autovacuum_vacuum_scale_factor` na casa de 0.01-0.02 para esta tabela), e — a decisão
  de produto que nenhuma ADR fez ainda — **retenção**: linhas terminais nunca saem, por
  desenho; sem uma política de purge/arquivamento nomeada (job interno do próprio Mohs é o
  candidato natural), todo o resto do tuning só adia o problema. Vale mini-ADR própria.
  DBTUNE-5 reduz o lado índice-de-claim disso; este item é o resto da tabela.

### DBTUNE-10 — Índice do reaper: decidir junto com o schema por dialeto, antes de nascer com scan — ALTO, aplicado e medido

- **Onde:** `lease_expires_at` existe nos 4 schemas e nenhuma query o lê ainda (CONC-5 do
  review anterior); as ADRs 0024/0025 acabaram de fixar que o reaper reusa a conclusão do
  `ExecutionStore` — a query dele será `WHERE state = 'RUNNING' AND lease_expires_at < :now`.
- **Problema:** nenhum índice atual serve isso — `idx_mohs_executions_claim` lidera por
  `state` mas ordena por `priority, scheduled_at`; o reaper varreria todas as `RUNNING` (ok)
  **depois de** achar `RUNNING` no meio de um índice dominado por linhas terminais (não ok,
  mesmo argumento do DBTUNE-5).
- **Correção aplicada:** no mesmo movimento do DBTUNE-5 — Postgres/SQL Server:
  `(lease_expires_at) WHERE state = 'RUNNING'` (parcial/filtrado, minúsculo por natureza:
  só o que está rodando agora); MySQL/H2: composto `(state, lease_expires_at)`.
  `idx_mohs_executions_reaper` nos 4 schemas, junto com a implementação do reaper (ADR-0012),
  não depois de um incidente de recovery lento.
- **Medido** (`LivenessLoadHarness`, H2 + Postgres, 20k linhas de histórico): Postgres —
  `SELECT` de candidatos foi de `Seq Scan` (actual time 1.111ms) para `Bitmap Heap Scan`
  (0.026ms, ~42x). Isso por si só quase não mexeu no throughput fim-a-fim do reclaim —
  revelou que o gargalo real era outro (ver DBTUNE-14).

### DBTUNE-11 — Tuning de driver por dialeto: o que o autoconfigure (M3) deve fixar — recomendação consolidada

- **Onde:** nenhum properties de driver existe em lugar nenhum (`application.yaml` de 3
  linhas; harness usa Hikari default além de pool size/timeout). Consolidado aqui para o
  M3 não redescobrir item a item:
  - **Connector/J (MySQL):** `cachePrepStmts=true`, `prepStmtCacheSize=250`,
    `prepStmtCacheSqlLimit=2048`, `useServerPrepStmts=true` — o claim executa o mesmo
    SELECT/UPDATE milhares de vezes por minuto; o default (client-side prepare, sem cache)
    re-parseia texto a cada chamada. `rewriteBatchedStatements=true` quando a recomendação
    2 do BASELINE (batch dos UPDATEs de transição) for implementada — sem ela o batch JDBC
    do Connector/J continua enviando statements um a um.
  - **pgjdbc (Postgres):** `prepareThreshold` default (5) já converte o claim para
    server-side prepared statement depois do warm-up por conexão — adequado; **não**
    zerar. `reWriteBatchedInserts=true` para o futuro batch de inserts de attempts.
    Atenção se algum dia houver pooler externo em transaction mode (PgBouncer): prepared
    statements nomeados quebram — nota de operação, não de código.
  - **mssql-jdbc (SQL Server):** manter `sendStringParametersAsUnicode=true` (ver
    "Verificado sem achado"); `statementPoolingCacheSize` > 0 com
    `disableStatementPooling=false` para reuso de prepared handles.
  - **Fetch size:** ver DBTUNE-7 — é configuração de template Spring, não de URL, mas
    pertence ao mesmo pacote de decisões.
- **Honestidade:** nenhum desses foi medido neste projeto; são defaults de engenharia com
  mecanismo conhecido, e o harness do BASELINE é o lugar de validar os que tocam o claim
  (prepared cache do MySQL é o de maior probabilidade de aparecer no número).

### DBTUNE-12 — `VARCHAR(255)` para UUIDs de 36 chars fixos: tipos nativos por dialeto reduzem PK e todos os índices secundários — MÉDIO, decidir antes do GA

- **Onde:** `id` de `mohs_executions`/`mohs_job_definitions`/`mohs_batches`,
  `execution_id` de `mohs_attempts`, `batch_id` — todos `VARCHAR(255)`/`NVARCHAR(255)`
  guardando UUIDv7 canônico de 36 chars (`schema-h2.sql:12-15,50-51` e equivalentes).
- **Problema:** o conteúdo é fixo e opaco; o tipo declara texto livre de 255. Custo real:
  a PK é carregada por **toda** entrada de índice secundário (InnoDB clustered; SQL Server
  clustered por default) — 36 bytes (72 no NVARCHAR do SQL Server) por entrada × 4 índices
  na tabela mais quente, contra 16 bytes de um tipo binário. Comparação e ordenação viram
  memcmp curto em vez de comparação de string com collation (utf8mb4 no MySQL).
  Por dialeto:
  - Postgres: `uuid` nativo — 16 bytes, ordenação por bytes preserva a ordem temporal do
    UUIDv7 (a localidade de insert que o comentário do schema promete continua valendo).
  - MySQL: `BINARY(16)` (+ `UUID_TO_BIN`-less: converter no Java) — idem.
  - SQL Server: **não** usar `UNIQUEIDENTIFIER` — a ordenação dele compara os bytes finais
    primeiro e **destruiria** a localidade do UUIDv7; `BINARY(16)` cru ou manter
    `CHAR(36)` são as opções que preservam a ordem.
  - `VARCHAR(255)` para `job_key`/`actor`/`node_id` (chaves de negócio legíveis) está
    correto como está — o achado é só sobre os UUIDs.
- **Trade-off honesto:** perde-se a legibilidade do id em queries manuais (mitigável com
  `::text`/funções de conversão) e o `ExecutionId` público continua `String` — a conversão
  fica na borda JDBC. Como DBTUNE-1: custo zero de migração hoje, migração de dado com
  janela depois do GA. Nota menor no mesmo espírito, sem recomendação forte:
  `state VARCHAR(20)` como coluna líder do índice de claim custa 8-16 bytes/entrada vs. 1
  de um `TINYINT` — aqui a legibilidade operacional ("`WHERE state = 'RUNNING'` no psql às
  3h") vale o custo; registrado apenas para constar que foi considerado.

### DBTUNE-13 — Retry de deadlock do claim sem backoff — BAIXO

- **Onde:** `src/main/java/io/mohs/jdbc/JdbcClaimer.java:128-138` — três tentativas
  imediatas, sem pausa, contra o deadlock genuíno do SQL Server (ADR-0023).
- **Problema:** dois nós que deadlockam e retentam no mesmo instante têm probabilidade
  alta de deadlockar de novo — 3 tentativas coladas podem se esgotar numa única janela de
  contenção que 10-50ms de espera resolveria. Guidance da própria Microsoft para deadlock
  victim é retry com delay curto.
- **Correção:** backoff mínimo com jitter entre tentativas (`Thread.sleep` de dezenas de
  ms é aceitável aqui — o claim roda na thread do poll loop, que será virtual; sleep
  desmonta o carrier, e isto é espera deliberada, não sincronização de teste — a regra
  "nada de sleep" do CLAUDE.md é sobre testes). Alternativa sem sleep: devolver lista
  vazia e deixar o próximo tick do poll tentar — mais simples ainda; decidir quando o poll
  loop existir.

### DBTUNE-14 — Reclaim do reaper fazia round-trip por candidato — ALTO, aplicado e medido

- **Onde:** `src/main/java/io/mohs/jdbc/JdbcReaper.java` (implementado nesta rodada,
  ADR-0012/0025), consumindo `ExecutionStore.complete` uma vez por candidato.
- **Problema:** `find()` (2 queries) + `complete()` (até 3 queries) por execução órfã —
  para 100 candidatos, ~500 round-trips. Medido com `LivenessLoadHarness` (H2 + Postgres,
  20k linhas de histórico): o índice `idx_mohs_executions_reaper` (DBTUNE-10) deixou a
  query de seleção ~42x mais rápida no Postgres (`Seq Scan` 1.111ms → `Bitmap Heap Scan`
  0.026ms), mas o throughput fim-a-fim quase não mudeu (709.5 → 778.0 rows/s) — o gargalo
  tinha migrado pro loop, não estava mais na query.
- **Correção aplicada:** `ExecutionStore.completeAll` (lê via `findByIds` já existente,
  `UPDATE`/`SELECT` de confirmação em lote por grupo de `newState`, `INSERT` de `Attempt`
  em `batchUpdate`) — `jobStore.decrementRunningExecutions` continua uma chamada por
  execução (guardada, barata, sem API de lote em `JobStore`). Medido depois: Postgres
  709.5→2131.8 rows/s sem índice (+200%), 778.0→2222.1 rows/s com índice (+186%).

### DBTUNE-15 — `IN (:ids)` vs. `JOIN` contra `VALUES` derivada — investigado, não aplicado

- **Onde:** os 4 pontos que usam `IN (:...)` em `src/main/java/io/mohs/jdbc/JdbcExecutionStore.java`
  (`findByIds`, `fetchAttemptsByExecutionIds`, e os dois `UPDATE`/`SELECT` de
  `transitionGroup` dentro de `completeAll`) — busca confirmada, nenhum outro store usa `IN`.
- **Investigado:** `findByIds` (único hot path dos 4 — `JdbcClaimer.claim` chama a cada
  vez), harness dedicado (`src/test/java/io/mohs/jdbc/InVsJoinTuningHarness.java`), 4
  dialetos, lote de 20 ids (tamanho real do claim), 20k linhas de histórico, `EXPLAIN`
  antes/depois (`docs/performance/explain-invsjoin-*-{in,join}.txt`).
- **Resultado:** ganho real nos 3 bancos de produção (Postgres -12%/-9% p50/p99, MySQL
  -11%/-14%, SQL Server -20%/-33%), mas sub-milissegundo em termos absolutos (0.35ms →
  0.30ms no Postgres) — e regressão no H2 (+2.3x/5x). Plano de execução mostra o mesmo
  tipo de acesso por índice de PK nos dois casos (bitmap/nested loop/index range scan),
  não um plano qualitativamente melhor.
- **Decisão: não aplicar.** O ganho não se compara ao gargalo já conhecido e ainda aberto
  do `claim()` (loop de `UPDATE` sequencial em `tryClaimCandidate`, até 21 round-trips por
  chamada — ver `performance/BASELINE.md`), custaria SQL por dialeto novo (`ROW()` no
  MySQL) e regride o H2, que a suíte de testes usa pesado. Não estendido aos outros 3
  pontos — nenhum é hot path, ganho esperado da mesma ordem ou menor.

---

## 3. Tuning de JVM

Contexto que enquadra este eixo inteiro: **Mohs é biblioteca embarcada** — quem define
flags de JVM é a aplicação hospedeira. O produto do Mohs neste eixo não é um `jvm.config`,
é (a) não recomendar coisa errada, (b) entregar ao operador do host a página de guidance
que hoje não existe. Estado atual verificado: nenhum arquivo de configuração de JVM no
repositório (`../../.mvn` só tem o wrapper, sem `jvm.config`; sem Dockerfile; sem `argLine` no
surefire; `../../pom.xml` define só `<java.version>25</java.version>`).

### JVM-1 — `-Djdk.tracePinnedThreads` não existe mais no JDK que o projeto usa — ALTO (documentação operacional), problema real hoje

- **Onde:** `../../CLAUDE.md` §Comandos ("Flags úteis: `-Djdk.tracePinnedThreads=short`");
  `docs/MOHS-DOCUMENTO-MESTRE.md:65` ("validação de pinning com
  `-Djdk.tracePinnedThreads`") e `:83` (Fase 0 do baseline: "execução com
  `-Djdk.tracePinnedThreads=full`").
- **Problema:** a propriedade foi **removida** no JDK 24, junto com o JEP 491 (ela tinha,
  inclusive, um bug conhecido de hang). No Temurin 25.0.4 que o BASELINE registra, passar
  a flag é aceito e ignorado em silêncio — a Fase 0 do processo de construção, quando
  rodar, executaria seu passo de "validação de pinning" sem validar nada, e ninguém
  saberia.
- **Correção:** substituir nos dois documentos pelo mecanismo atual: o evento JFR
  `jdk.VirtualThreadPinned` (emitido pela VM para os casos remanescentes de pinning, com
  threshold configurável) — operacionalmente:
  `-XX:StartFlightRecording=filename=rec.jfr` + `jfr print --events
  jdk.VirtualThreadPinned rec.jfr`; e para inspeção ad-hoc, o thread dump JSON
  (`jcmd <pid> Thread.dump_to_file -format=json <file>`) que lista virtual threads e seus
  carriers. Par com JAVA-1 (mesma raiz: doutrina escrita para JDK 21-23).

### JVM-2 — Guidance de GC/heap/scheduler para carga virtual-thread-heavy não existe em nenhum documento — recomendação (bloqueante para "pronto para produção", não para M3)

- **Onde:** nenhum documento do repositório orienta o operador do host sobre JVM — o
  CLAUDE.md cobre o *código* concorrente, não o runtime. Quando M3 ligar o motor
  (dispatcher em virtual threads, milhares de jobs concorrentes é o cenário da Fase 0), o
  que precisa estar escrito, com a honestidade de que **nada disso foi medido aqui ainda**:
  - **GC:** G1 (default) é a escolha certa para começar — heaps pequenos/médios,
    throughput-friendly, pausas boas o suficiente para um scheduler (latência de claim é
    dominada por rede/banco, ~dezenas de ms; pausa de G1 bem dimensionado é ordem de
    magnitude menor). **ZGC generacional** (default do ZGC desde o JDK 23; o modo
    não-geracional já foi removido no 24) entra como candidato se o host tiver heap grande
    (>8-16GB) e p99 de dispatch virar requisito — trade-off documentado: ~sub-ms de pausa
    contra mais CPU e barreiras de leitura. A decisão certa é do host; o documento do Mohs
    deve dizer qual métrica observar (pausa vs. custo do claim medido no BASELINE) para
    decidir.
  - **Heap e virtual threads:** as stacks de virtual threads vivem **no heap** (stack
    chunks) — 10k jobs I/O-bound concorrentes são 10k stacks residentes no heap, coisa que
    o dimensionamento clássico ("heap = working set de objetos") não inclui. Regra
    prática a documentar: carga concorrente alta ⇒ margem de heap para stacks + os
    payloads em trânsito.
  - **Scheduler de virtual threads:** `jdk.virtualThreadScheduler.parallelism` (default =
    nº de cores) e `maxPoolSize` — relevantes se o host colocar o Mohs junto de outra
    carga CPU-bound pesada; default correto para o caso típico, documentar que existe.
  - **Observabilidade:** os eventos JFR que o runbook de operação deve citar —
    `jdk.VirtualThreadPinned` (JVM-1), `jdk.VirtualThreadSubmitFailed` (esgotamento do
    scheduler — o análogo de "pool cheio" num mundo sem pool).
- **Formato sugerido:** uma seção "Rodando em produção" no futuro doc de operação (ou
  `docs/OPERATIONS.md` quando M3 fechar), não flags no `../../pom.xml` — biblioteca não impõe
  runtime ao host.

### JVM-3 — Compact Object Headers (JEP 519, product no JDK 25): candidato barato para o gate de benchmark de M4 — BAIXO, hipótese a medir

- **Análise:** `-XX:+UseCompactObjectHeaders` virou feature de produto no 25 (sem
  `UnlockExperimentalVMOptions`), reduzindo o header de objeto de 12 para 8 bytes. O perfil
  de alocação do Mohs em regime é exatamente o que mais se beneficia: rajadas de objetos
  pequenos e efêmeros — `Candidate`, `Execution`, `Attempt`, `Timestamp`, boxes de bind
  JDBC — por ciclo de poll, mais as stacks de virtual threads no heap (JVM-2). Ganho
  típico reportado pelo JEP é da ordem de percentuais de heap/vazão de alocação — **não
  meço nem prometo número aqui**: é uma linha de variação para o harness do BASELINE
  quando a Fase 0 rodar (mesma máquina, mesma carga, flag on/off), e uma nota no guidance
  do JVM-2 se o número confirmar. Custo de adotar: zero código; risco: baixo e do host.

### JVM-4 — CDS/AOT (Leyden, JEPs 483/514/515): irrelevante como alavanca do Mohs; uma frase de posicionamento basta — INFO

- **Análise:** o AOT cache do Leyden (JDK 24-25: class loading & linking + ergonomia de
  linha de comando + profiles de método) acelera **startup de aplicação** — e quem tem
  startup é o host, não a biblioteca. Mohs não tem interesse próprio óbvio: um scheduler
  embarcado não é serverless/scale-to-zero; o custo de boot do Mohs em si (registro de
  definitions + upserts) é dominado por I/O de banco, que AOT não toca. O que vale uma
  frase no doc de operação: Mohs não faz nada AOT-hostil (sem geração dinâmica de
  bytecode; o único `Class.forName` é sobre classes do host, resolvíveis no training run
  do fluxo Spring Boot `spring-boot:process-aot`/CDS) — host apps que usem AOT cache podem
  incluir o Mohs no training run sem ressalva. Nenhuma ação de código.

---

## 4. Tabela consolidada

| ID | Severidade | Tipo | Arquivo(s) principal(is) | Resumo |
|---|---|---|---|---|
| JAVA-1 | Médio | Real (doutrina) | CLAUDE.md, MOHS-DOCUMENTO-MESTRE.md:58-65, ArchitectureTest.java:79-98 | Justificativa de pinning para banir `synchronized` não vale mais no JDK 25 (JEP 491) |
| JAVA-2 | Médio | Decisão a registrar | CLAUDE.md, JdbcClaimerTest.java:274-319 | `StructuredTaskScope` é preview — inviável em `main` de biblioteca; registrar e desenhar M3 no formato |
| JAVA-3 | Info | Futuro (M3) | ExecutionInterceptor.java:8 | `ScopedValue` já planejado no lugar certo; binding por tentativa quando o dispatcher existir |
| JAVA-4 | Médio | Real | JdbcJobStore.java:244-248 | `default -> OnDemandSpec` engole `schedule_type` corrompido — job cron vira on-demand em silêncio |
| JAVA-5 | Baixo | Modernização | jdbc/*, dialect/*, InMemoryJobStore | ~20 pontos para `_` (JDK 22): rowNum, bindings de pattern, exceções de protocolo |
| JAVA-6 | Baixo | Opcional | JdbcExecutionStore.java:100-106 | Chunking manual ↔ `Gatherers.windowFixed` (JDK 24) |
| JAVA-7 | Baixo | Real (menor) | JdbcJobStore.java:72-75 | 4 instanceof-ternários sobre sealed sem exaustividade; switch único com record pattern |
| JAVA-8 | Baixo | Real (menor) | JdbcClaimer.java:132 | Retorno `@Nullable` de `execute()` repassado como não-nulo sob `@NullMarked` |
| JAVA-9 | Baixo | Real (menor) | MutableClock.java:34-37 | `advance()` é RMW não atômico sobre volatile — mesmo bug/correção do CONC-3 |
| DBTUNE-1 | Alto | Real (latente) | 4 schemas + 10 binds `Timestamp.from` | Timestamp sem fuso + bind default = valor depende do fuso da JVM de cada nó |
| DBTUNE-2 | Médio-Alto | Real | schema-mysql.sql | `DATETIME` sem fração = granularidade de 1s, arredonda pro futuro; `DATETIME(6)` |
| DBTUNE-3 | Alto | Real (hot path) | JdbcClaimer.java:114, JdbcExecutionStore.java:131-149 | Resíduo do PERF-1: 1 query de attempts por execução + 2ª conexão aninhada (risco de pool deadlock) |
| DBTUNE-4 | Médio-Alto | Real | JdbcClaimer.java:91 | Isolamento não fixado — MySQL roda claim em REPEATABLE READ (gap locks) |
| DBTUNE-5 | Alto | Produção (medir) | schema-postgresql.sql:58, schema-sqlserver.sql:69 | Índice de claim parcial/filtrado (`WHERE state='ENQUEUED'`) em PG/SQL Server |
| DBTUNE-6 | Médio | Real (hot path) | JdbcExecutionStore.java:87,103,114,121 | `SELECT *` transfere payload inteiro e descarta; listar as 7 colunas mapeadas |
| DBTUNE-7 | Médio | Real em M3 | engine/ExecutionStore.java:41-48 + stores | Contrato de Stream promete cursor; pgjdbc/Connector/J materializam tudo sem fetchSize |
| DBTUNE-8 | Médio | Decidir p/ M3 | schema-*.sql (idx idempotency_key) | Dedupe exige UNIQUE parcial/filtrado, grafia diverge por dialeto (SQL Server: obrigatório) |
| DBTUNE-9 | Médio | Produção | schema-postgresql.sql | Updates nunca-HOT em mohs_executions: fillfactor + autovacuum + decisão de retenção (ADR) |
| DBTUNE-10 | Alto | Aplicado, medido | schema-*.sql | Índice do reaper `(lease_expires_at) WHERE state='RUNNING'` — SELECT ~42x mais rápido no Postgres (medido) |
| DBTUNE-11 | Baixo | Produção (M3) | futuro autoconfigure | Props de driver por dialeto (prepStmt cache MySQL, rewriteBatched, unicode mssql, fetch size) |
| DBTUNE-12 | Médio | Decidir pré-GA | 4 schemas (colunas de UUID) | `uuid` nativo PG / `BINARY(16)` MySQL; nunca `UNIQUEIDENTIFIER` (quebra ordem v7) |
| DBTUNE-13 | Baixo | Real (menor) | JdbcClaimer.java:128-138 | Retry de deadlock sem backoff/jitter |
| DBTUNE-14 | Alto | Aplicado, medido | JdbcExecutionStore.java (completeAll), JdbcReaper.java | Reclaim em lote (não candidato a candidato) — Postgres +200%/+186% (sem/com índice) |
| DBTUNE-15 | — | Investigado, não aplicado | JdbcExecutionStore.java (IN), InVsJoinTuningHarness.java | IN vs JOIN: ganho real nos 3 bancos reais mas sub-ms, não vale a complexidade — decisão registrada, não código |
| JVM-1 | Alto | Real (docs) | CLAUDE.md, MOHS-DOCUMENTO-MESTRE.md:65,83 | `-Djdk.tracePinnedThreads` removida no JDK 24 — no-op silencioso; usar JFR `jdk.VirtualThreadPinned` |
| JVM-2 | Médio | Produção | (documento inexistente) | Guidance de GC (G1 vs ZGC gen.), heap p/ VT stacks, scheduler knobs, eventos JFR — escrever com M3 |
| JVM-3 | Baixo | Hipótese (medir) | — | Compact Object Headers (JEP 519) como variação do harness na Fase 0 |
| JVM-4 | Info | Posicionamento | — | AOT/Leyden é alavanca do host, não do Mohs; uma frase de compatibilidade no doc de operação |

---

## 5. Ordem de ataque sugerida

1. **Agora, custo de minutos:** JVM-1 + JAVA-1 (corrigir os textos — evita decisão errada
   em M3 e uma Fase 0 que "valida" com flag morta); JAVA-4 (um `default` → `throw`);
   DBTUNE-4 (uma linha de isolamento); JAVA-8/DBTUNE-13 (linhas).
2. **Antes de qualquer consumidor externo do jar (baratos hoje, migração depois):**
   DBTUNE-1 (contrato UTC + bind), DBTUNE-2 (`DATETIME(6)`), DBTUNE-12 (tipos de UUID) —
   os três são a mesma janela de oportunidade que as ADRs 0022/0023 já usaram para mudar
   shape sem migração.
3. **Hot path do claim, com o harness existente como juiz:** DBTUNE-3 + DBTUNE-6 (mesma
   região de código; medir antes/depois no `ClaimQueryLoadHarness` e registrar nova seção
   no BASELINE — a latência residual de ~28ms/chamada é a pergunta aberta que eles
   provavelmente respondem, junto com a recomendação 2 já registrada lá).
4. **Junto do wiring de M3:** DBTUNE-7 (fetchSize/contrato), DBTUNE-8 (unique de
   idempotência), DBTUNE-10 (índice do reaper), DBTUNE-11 + PERF-3 (autoconfigure),
   JAVA-2/JAVA-3 (forma do dispatcher).
5. **Preparação de produção (medir, documentar, ADR):** DBTUNE-5, DBTUNE-9 (inclui a ADR
   de retenção), JVM-2, JVM-3.
6. **Cosmético, quando conveniente:** JAVA-5, JAVA-6, JAVA-7, JAVA-9.
