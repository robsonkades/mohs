# Code Review — Mohs: Nomenclatura, Organização de Pacotes e Responsabilidades

**Data:** 2026-08-14
**Escopo:** organização de pacotes, nomes de classes/interfaces/enums, nomes de métodos, nomes de
atributos e parâmetros, e distribuição de responsabilidades entre classes — `src/main` e `src/test`
completos, incluindo o estado atual do working tree (mudanças não commitadas em
`io.mohs.engine.ExecutionStore`/`io.mohs.jdbc.JdbcExecutionStore`, e os arquivos novos
`io.mohs.engine.HandlerInvocation`/`HandlerRegistry`).
**Não repete** achados de `docs/codereview.md` (bugs, portabilidade de banco, concorrência, contrato
REST, testes, dependências) nem de `docs/codereview-tuning.md` (modernização Java/JDK 25, tuning de
banco, tuning de JVM) — onde este documento toca um tema adjacente a um achado já registrado lá
(ex.: API-13, API-18, JAVA-5, JAVA-7), isso é citado explicitamente em vez de re-derivado.
**Estágio do projeto:** M0-M2 entregues, M3 parcial — o próprio estágio importa para como ler este
documento: parte do valor de um review de nomenclatura é justamente aparecer *antes* de mais código
depender dos nomes atuais, não depois.

---

## Sumário executivo

A qualidade de nomenclatura e organização desta base é alta — confirma o que `docs/codereview.md` já
media por outro ângulo ("qualidade média do código é alta"). Praticamente todo pacote de produção tem
`package-info.java` documentando intenção, direção de dependência e o padrão de projeto (GoF/PoEAA)
aplicado; a família `*Store`/`Jdbc*Store` é 1:1 sem exceção; nomes de teste são frases descritivas
consistentes em ~57 classes. Os achados abaixo são o tipo de coisa que só aparece numa leitura
completa arquivo a arquivo, não sintomas de descuido — vários são precisão, não erro.

Dito isso, dois achados são objetivos, sem espaço para discordância, e baratos de corrigir agora:

- **Um arquivo de teste inteiro está no pacote errado**: `DispatcherTest` testa
  `io.mohs.engine.Dispatcher` mas vive em `src/test/java/io/mohs/jdbc/` — quebra, sem exceção em
  nenhum outro lugar da suíte, o espelhamento 1:1 entre pacote de produção e pacote de teste
  (Seção 1, ORG-2).
- **O grafo de dependências entre subpacotes de `io.mohs.core` e `io.mohs.rest` está inteiramente
  documentado em prosa (`package-info.java`) mas não tem nenhuma regra ArchUnit guardando-o** —
  ao contrário de toda outra invariante deste projeto, que vira regra executável. Já existe uma
  rachadura real (não hipotética) que prova o risco: `ExecutionState` importa `ExecutionEvent` no
  sentido contrário ao documentado (Seção 1, ORG-1).

Um terceiro achado vale destaque por timing: `HandlerInvocation` (arquivo novo, ainda não commitado)
nomeia "o handler já resolvido, pronto pra chamar" com um substantivo que, no vocabulário que o
próprio projeto já estabeleceu (`Attempt`, `Execution`), soa mais como "um registro de uma chamada"
do que "a coisa chamável". É exatamente o raciocínio que a ADR-0004 já usou para `JobQueue`/
`ExecutionWindow`: renomear agora custa um `git mv`; renomear depois de `io.mohs.autoconfigure`
escanear `@MohsJob` contra isso é um problema maior (Seção 2, CLASS-1).

O restante são inconsistências pontuais de vocabulário (dois tipos chamados `Candidate` com formas
diferentes; quatro verbos — `upsert`/`upsert`/`insert`/`create` — para duas semânticas de escrita
nos `*Store`; dois sufixos de DTO — `Response`/`View` — sem regra escrita) e duas observações de
responsabilidade que valem registro consciente (mini-ADR), não necessariamente mudança de código.

---

## Metodologia

Leitura direta e integral de todo `src/main/java/io/mohs/**` (142 arquivos: `core` com seus 5
subpacotes, `rest` com seus 7 subpacotes, `engine`, `jdbc` + `dialect`, `test`, raiz;
`io.mohs.cron` tratado como vendorizado — fora de escopo para rename, mesma exceção que
`docs/codereview-tuning.md` já aplica, confirmada de novo aqui pelo cabeçalho de atribuição em cada
arquivo) e de todos os 23 `package-info.java`. Todas as 26 ADRs e os quatro documentos de design
(`API-DESIGN.md`, `REST-API-DESIGN.md`, `MOHS-DOCUMENTO-MESTRE.md`, `ArchitectureTest.java`) lidos
para estabelecer o vocabulário e as regras de organização *pretendidas* antes de avaliar o código
real contra elas — a maioria dos achados abaixo é justamente uma comparação entre o que o
`package-info`/a ADR promete e o que o import/arquivo real faz. Suíte de testes varrida por nome de
classe e método (grep dirigido, ~150 assinaturas) para avaliar convenção de nomenclatura de teste
sem precisar ler as ~57 classes inteiras. Nenhum arquivo de produção foi alterado por esta revisão.

---

## 1. Organização de pacotes e fronteiras arquiteturais

### ORG-1 — Grafo de dependência entre subpacotes de `core`/`rest` é só prosa, sem regra ArchUnit — ALTO

- **Onde:** todo `package-info.java` de `io.mohs.core.*` declara explicitamente quem depende de quem
  (ex.: `io.mohs.core.job`'s: "definition/execution/event... todos dependem daqui, não entre si";
  `io.mohs.core.resource`'s: "sem dependência de nenhum outro subpacote público"); o mesmo vale para
  `io.mohs.rest.job` → `io.mohs.rest.execution` (documentado, unidirecional). `src/test/java/io/mohs/ArchitectureTest.java`
  tem 7 regras (`internal_packages_do_not_leak_into_public_api`, `rest_only_sees_public_api`,
  `test_kit_does_not_leak_into_production`, `engine_never_reads_wall_clock_directly`,
  `no_synchronized_methods_in_concurrency_critical_code`, `no_thread_local_in_concurrency_critical_code`,
  `all_production_packages_declare_null_marked`) — nenhuma cobre a direção de dependência
  *dentro* de `io.mohs.core` ou *dentro* de `io.mohs.rest`.
- **Por que isso não é só teórico** — a rachadura já existe: `src/main/java/io/mohs/core/execution/ExecutionState.java:3`
  importa `io.mohs.core.event.ExecutionEvent`:
  ```java
  package io.mohs.core.execution;

  import io.mohs.core.event.ExecutionEvent;   // ← sentido contrário ao documentado
  ```
  O sentido documentado (por `io.mohs.core.event`'s próprio `package-info.java`) é `event` depende de
  `execution` (todo record de evento carrega `ExecutionId`), nunca o contrário. Hoje o import só é
  usado num `{@link ExecutionEvent}` de Javadoc — inofensivo, porque Javadoc não deixa rastro em
  bytecode e portanto uma regra ArchUnit baseada em `dependOnClassesThat()` não acusaria nada mesmo
  se existisse. Mas isso é exatamente o ponto: o import-fonte já está lá, um PR futuro que adicione um
  parâmetro ou campo do tipo `ExecutionEvent` a qualquer classe de `io.mohs.core.execution` fecha um
  ciclo real entre os dois pacotes sem que nada avise — nem compilador, nem ArchUnit, nem review
  manual (a menos que quem revisa tenha memorizado os 5 `package-info.java` de cor).
- **Impacto:** este projeto trata "regra documentada em prosa" como estado intermediário, não final —
  é literalmente o padrão usado em toda outra invariante (nulidade, `synchronized`, `ThreadLocal`,
  leitura direta de relógio). O grafo de `core`/`rest` é a única exceção, apesar de ser tão fácil de
  formalizar quanto as demais.
- **Correção:** uma regra por subpacote-base, espelhando o que os `package-info.java` já dizem — ex.:
  ```java
  @ArchTest
  static final ArchRule core_execution_does_not_depend_on_core_event =
      noClasses().that().resideInAPackage("io.mohs.core.execution..")
          .should().dependOnClassesThat().resideInAnyPackage("io.mohs.core.event..");
  ```
  repetida para `resource` (não depende de nenhum irmão), `definition` (só `job`+`schedule`), `event`
  (só `job`+`execution`), e para `io.mohs.rest.execution` não depender de `io.mohs.rest.job`. Ou,
  mais elegante e menos sujeito a esquecimento por subpacote novo: `ArchRuleDefinition.slices()`/
  `layeredArchitecture()` do próprio ArchUnit, que expressa o grafo inteiro numa declaração só. De
  quebra, isso também fecha a lacuna oposta: hoje nada impede um subpacote público novo de nascer sem
  qualquer regra de dependência, só porque a regra de fronteira pública (ADR-0013) é por exclusão dos
  5 pacotes internos, não por inclusão dos públicos.

### ORG-2 — `DispatcherTest` vive no pacote de teste errado — MÉDIO (mecânico, zero risco de corrigir)

- **Onde:** `src/test/java/io/mohs/jdbc/DispatcherTest.java:1` declara `package io.mohs.jdbc;`, mas a
  classe sob teste é `io.mohs.engine.Dispatcher` (`src/main/java/io/mohs/engine/Dispatcher.java`).
- **Confirmado:** é o único `DispatcherTest.java` do repositório (`Glob` não encontra nenhum em
  `io.mohs.engine`) — não é um caso de teste duplicado em dois lugares, é simplesmente o único teste
  de `Dispatcher` morando no diretório errado. Todo outro par produção/teste do projeto — mais de 55
  classes — espelha o pacote 1:1 sem exceção (`io.mohs.engine.NextFireCalculator` ↔
  `io.mohs.engine.NextFireCalculatorTest`, `io.mohs.engine.BatchCounters` ↔
  `io.mohs.engine.BatchCountersTest`, e assim por diante).
- **Causa provável:** `DispatcherTest` bootstrapa um H2 real via `schema-h2.sql` e usa
  `JdbcJobStore`/`JdbcExecutionStore`/`JdbcTimestamps` como implementações concretas dos dois `*Store`
  que `Dispatcher` orquestra — provavelmente foi escrito adaptando o setup de um teste JDBC vizinho
  (o padrão `freshH2DataSource()`/`ResourceDatabasePopulator` é idêntico ao dos `Jdbc*Test`) e nunca
  movido para o pacote da classe que de fato testa.
- **Por que a correção é mecânica e de risco zero:** `JdbcJobStore`, `JdbcExecutionStore` e
  `JdbcTimestamps` são todos `public` — nada no arquivo depende de acesso package-private a
  `io.mohs.jdbc`. Mover o arquivo para `src/test/java/io/mohs/engine/DispatcherTest.java`, trocar a
  linha 1 para `package io.mohs.engine;` e adicionar três imports explícitos (`io.mohs.jdbc.JdbcJobStore`,
  `io.mohs.jdbc.JdbcExecutionStore`, `io.mohs.jdbc.JdbcTimestamps`) compila e passa exatamente igual.
- **Correção:** `git mv src/test/java/io/mohs/jdbc/DispatcherTest.java src/test/java/io/mohs/engine/DispatcherTest.java`,
  ajustar `package`/imports conforme acima.

---

## 2. Nomes de classes e vocabulário

### CLASS-1 — `HandlerInvocation` nomeia "o handler pronto pra chamar" com um substantivo de "evento de uma chamada" — MÉDIO (arquivo não commitado — momento mais barato possível para agir)

- **Onde:** `src/main/java/io/mohs/engine/HandlerInvocation.java` (17 linhas, `?? ` no `git status` —
  ainda não commitado nesta sessão).
- **Problema:** o próprio Javadoc da interface descreve o conceito com precisão — "Handler já
  resolvido, pronto pra chamar" — mas o nome escolhido, `HandlerInvocation`, usa o sufixo `-tion` que
  este mesmo projeto já emprega consistentemente para "um registro de uma ocorrência específica":
  `Attempt` é uma tentativa que aconteceu, `Execution` é um disparo que aconteceu, `RetryScheduled`/
  `AttemptFailed` são eventos que aconteceram. `HandlerInvocation`, por esse padrão já estabelecido,
  lê como "o registro de uma chamada ao handler que ocorreu" — não é isso: é uma
  `@FunctionalInterface` de uma única capacidade (`invoke(Object payload, JobContext ctx)`), o mesmo
  papel que `Runnable`/`Callable`/`ExecutionListener`/`ExecutionInterceptor` ocupam no resto da base,
  todos nomeados pela *capacidade*, não pelo *evento*.
- **Impacto:** hoje é só uma interface de 4 linhas com dois consumidores (`HandlerRegistry`,
  `Dispatcher`) e um teste — o menor blast radius possível. Se `io.mohs.autoconfigure` (M3) nascer
  escaneando `@MohsJob` e populando `HandlerRegistry` referenciando este tipo em mais lugares (SPI de
  extensão, documentação pública, etc.), corrigir o nome fica progressivamente mais caro — o mesmo
  raciocínio que a ADR-0004 já registrou explicitamente para `JobQueue`/`ExecutionWindow`
  ("colisão... custava centavos agora e uma fortuna depois do 1.0"; aqui não é colisão de import, mas
  o cálculo de custo-de-adiar é o mesmo).
- **Correção sugerida:** `JobHandler` — encaixa na família de prefixo `Job*` já estabelecida
  (`JobKey`, `JobRef`, `JobDefinition`, `JobContext`, `JobStore`, `JobSpec`) e distingue-se
  claramente de `JobDefinition.handlerType()` (a `Class<?>` bruta) por ser "o par bean+método já
  resolvido e invocável". Alternativa mais simples, `Handler` — também aceitável (sem colisão real no
  classpath deste projeto; `java.util.logging.Handler` não é usado aqui), mas `JobHandler` é mais
  descritivo e mais seguro a longo prazo contra import ambíguo em IDEs, já que o projeto usa Spring
  extensivamente e a família `Handler*` do Spring MVC (`HandlerAdapter`, `HandlerMethod`,
  `HandlerInterceptor`) é vizinha de classpath.

### CLASS-2 — Dois tipos distintos chamados `Candidate` no mesmo pacote-pai — BAIXO-MÉDIO

- **Onde:** `io.mohs.jdbc.dialect.Candidate` (`src/main/java/io/mohs/jdbc/dialect/Candidate.java:4`,
  `public record Candidate(String id, String jobKey, boolean allowConcurrentExecutions)`, usado por
  `JdbcClaimer` via os 4 `JdbcDialect`) e um **segundo, não relacionado**,
  `io.mohs.jdbc.JdbcReaper.Candidate` (`src/main/java/io/mohs/jdbc/JdbcReaper.java:134`,
  `private record Candidate(String id, String jobKey)`, usado só dentro de `JdbcReaper`).
- **Problema:** não é colisão de compilação (escopos diferentes: um é `public` em
  `io.mohs.jdbc.dialect`, o outro é `private` aninhado em `io.mohs.jdbc.JdbcReaper`) — mas os dois
  representam conceitos genuinamente diferentes ("linha `ENQUEUED` candidata a claim" vs. "linha
  `RUNNING` com lease expirada candidata a reclaim") sob o mesmo nome, no mesmo pacote-pai
  (`io.mohs.jdbc`), com forma quase idêntica (dois campos em comum, um a mais no primeiro). Um
  leitor que já conhece `dialect.Candidate` — o que qualquer um que tenha lido `JdbcClaimer` já fez —
  tem todo motivo para presumir que `JdbcReaper` reusa o mesmo tipo; não reusa, e o próprio Javadoc de
  `Reaper` (a porta) já explica por quê ("não precisa da abstração de dialeto que o claim precisa") —
  só que essa explicação está a um pulo de distância do local onde a confusão aconteceria.
- **Correção:** renomear o tipo privado de `JdbcReaper` para algo que reflita o vocabulário que o
  próprio `Reaper`/`JdbcReaper` já usa em prosa — `ExpiredCandidate` ou `ReclaimCandidate` (ambos
  ecoam `Reaper#reclaimExpired`). Mudança de uma linha, dois pontos de uso, sem risco.

### CLASS-3 — `Schedule` (dado de agenda) e `ScheduleCommand` (cadeia de invocação) compartilham a raiz "Schedule" para conceitos não relacionados — BAIXO

- **Onde:** `io.mohs.core.schedule.Schedule` (`CronSpec`/`IntervalSpec`/`OnDemandSpec` — quando um job
  dispara automaticamente) vs. `io.mohs.core.ScheduleCommand` (a cadeia fluente devolvida por
  `Mohs.schedule(ref, payload)` — `priority`/`as`/`idempotencyKey`/`now`/`at`/`after`).
- **Problema:** `ScheduleCommand` não contém, não produz e não referencia nenhum `Schedule` — é o
  objeto-comando de uma única invocação manual, estruturalmente mais próximo de "InvocationCommand"
  ou "EnqueueCommand" do que de agenda automática. A proximidade de nome é mitigada hoje pelo Javadoc
  de ambos os tipos (cada um bem documentado isoladamente), mas nenhum dos dois faz a referência
  cruzada explícita "não confundir com o outro" — o mesmo cuidado que
  `io.mohs.core.MohsLifecycle`/`io.mohs.core.EngineState` já tomam ao se distinguir de "pause de job"
  ("não confundir com pause de job, que é cluster-wide e por job").
- **Correção:** não é rename — o nome `ScheduleCommand` é o verbo natural de `Mohs.schedule(...)`
  (`mohs.schedule(...)` retorna o comando daquela chamada a `schedule`, padrão idiomático comum). Só
  vale uma linha de Javadoc cruzado em `ScheduleCommand` do tipo "não é o mesmo conceito de
  `io.mohs.core.schedule.Schedule` (a agenda automática do job) — este é o comando de uma única
  invocação manual", no mesmo espírito preventivo que `MohsLifecycle` já usa.

### CLASS-4 — `ExecutionState` e `ExecutionEventType`: nomes próximos, conjuntos de valores diferentes — BAIXO (complementa API-13 de `docs/codereview.md`)

- **Onde:** `io.mohs.core.execution.ExecutionState` (6 valores: `ENQUEUED`/`RUNNING`/
  `RETRY_SCHEDULED`/`SUCCEEDED`/`FAILED`/`CANCELLED`) vs. `io.mohs.core.event.ExecutionEventType`
  (8 valores: os mesmos menos `RUNNING`, mais `STARTED`/`ATTEMPT_FAILED`/`BATCH_COMPLETED`).
- **Nota:** `docs/codereview.md` (API-13) já cobre o problema estrutural — o enum espelha
  `ExecutionEvent` manualmente, sem link de compilação. Este achado é menor e complementar: mesmo
  ignorando o problema de link, os dois *nomes* (`ExecutionState`/`ExecutionEventType`) são próximos o
  bastante, e os *conjuntos de valores* são próximos o bastante sem serem iguais, para convidar
  confusão verbal em review/discussão ("é `RUNNING` ou é `STARTED`?" — resposta: depende de qual dos
  dois enums, já que um dos dois não tem `RUNNING` e o outro não tem `STARTED`). Nenhuma ação de
  código adicional além da já sugerida em API-13; registrado aqui só para reforçar que a correção
  de API-13 (ligar os dois em tempo de compilação, ex. `ExecutionEventType` derivado de
  `ExecutionEvent` via método em vez de enum solto) também resolveria este ponto de leitura.

---

## 3. Nomes de métodos

### METHOD-1 — Quatro verbos de escrita (`upsert`/`upsert`/`insert`/`create`) para duas semânticas nos `*Store` — MÉDIO

- **Onde:** os cinco métodos de escrita-de-novo-registro nas portas-irmãs de `io.mohs.engine`:
  `JobStore.upsert(JobDefinition)`, `RateLimitStore.upsert(RateLimit)`,
  `ExecutionStore.insert(Execution, Object)`, `BatchStore.create(String, int)`.
- **O que já está certo:** a distinção `upsert` vs. não-`upsert` é semanticamente real, não
  arbitrária — `Job`/`RateLimit` são recursos redeclarados a cada boot (ADR-0006: upsert idempotente,
  "código vence" por padrão) enquanto `Execution`/`Batch` são escritos exatamente uma vez, nunca
  re-registrados. Não é um achado trocar `upsert` por outra coisa em `JobStore`/`RateLimitStore`.
- **Problema:** dentro do grupo "escreve uma vez, nunca upsert", `ExecutionStore` usa `insert` e
  `BatchStore` usa `create` para a mesma semântica exata — nenhuma diferença de comportamento entre
  os dois justifica o verbo diferente. Um leitor que memorizou "upsert = idempotente, redeclarável"
  não tem como inferir de `create`/`insert` sozinhos qual dos dois seguiria esse padrão sem abrir as
  duas interfaces.
- **Correção:** padronizar em um verbo só para o grupo write-once. Sugestão: `insert` — já é o termo
  usado ao longo de todo `io.mohs.jdbc`/ADR-0003 para descrever esta operação exata ("insert do
  terminal da cláusula 4"), então `BatchStore.create` → `BatchStore.insert` alinha o vocabulário sem
  precisar tocar `ExecutionStore`. (`create` também seria uma escolha válida se a preferência for
  vocabulário de domínio em vez de SQL — o importante é escolher um dos dois, não os dois.)

---

## 4. DTOs REST — convenção de sufixo não documentada

### DTO-1 — `*Response` e `*View` coexistem em `io.mohs.rest` sem regra escrita distinguindo quando usar qual — MÉDIO

- **Onde:** a maioria dos DTOs usa `*Response` — inclusive `io.mohs.rest.execution.AttemptResponse`,
  que é **aninhado** dentro de `ExecutionResponse.attempts()`, nunca devolvido sozinho por nenhum
  endpoint. Dois grupos usam `*View` em vez disso: `io.mohs.rest.job.ScheduleView`/`CronView`/
  `IntervalView`/`OnDemandView` (selado, espelha `Schedule`) e `io.mohs.rest.overview.ThroughputView`
  (aninhado em `OverviewResponse.throughput()`).
- **Problema:** como `AttemptResponse` já prova que "aninhado" não é o critério (ele é aninhado e
  ainda assim `*Response`), a regra real que separa os dois grupos nunca é dita em lugar nenhum —
  nem no `package-info.java` de `io.mohs.rest` (que já documenta várias outras convenções
  cross-cutting: `ApiPaths.V1`, `CursorPage`, `ActorResolver`) nem em nenhum Javadoc individual. A
  hipótese mais provável, reconstruída lendo os quatro casos — "`*View` é reservado para
  wire-adaptação de um tipo `sealed` do domínio (`Schedule`) ou uma projeção computada sem entidade
  correspondente no core (`Throughput` não existe em `io.mohs.core`)" — é plausível, mas é uma
  hipótese minha, não algo que um novo contribuidor consiga inferir sem grepar os quatro exemplos.
- **Impacto:** M3 ainda vai adicionar DTOs novos (toda `*Controller` hoje é stub) — sem a regra
  escrita, a próxima decisão (`*Response` ou `*View`?) vira uma moeda ao ar em vez de uma consulta a
  uma linha de documentação.
- **Correção:** escrever a regra explicitamente — lugar natural é o `package-info.java` de
  `io.mohs.rest` (raiz), que já é onde outras convenções cross-cutting do pacote vivem — ou, mais
  simples, colapsar para um sufixo só (`*Response` em tudo, já que é o majoritário) e tratar `View`
  como sinônimo não-intencional a eliminar.

---

## 5. Responsabilidades de classes

### RESP-1 — `ExecutionStore.complete`/`completeAll` cruzam para `JobStore` de dentro de uma porta nomeada como "uma entidade só" — MÉDIO (trade-off a decidir conscientemente, não bug)

- **Onde:** `io.mohs.engine.ExecutionStore.complete(ExecutionId, JobKey, Attempt, ExecutionState, JobStore)`
  e `.completeAll(List<CompletionRequest>, JobStore)` (`src/main/java/io/mohs/engine/ExecutionStore.java:67,85`) —
  ambos recebem `JobStore` como parâmetro e, na implementação (`JdbcExecutionStore.java:130,170-172`),
  chamam `jobStore.decrementRunningExecutions(...)` depois de gravar o `Attempt`.
- **Tensão com a própria regra do projeto:** o Javadoc de `Claimer` e de `Reaper` justifica,
  explicitamente e nos mesmos termos um do outro, por que essas duas portas **não** são um `*Store`:
  "não é Repository de uma entidade só... não cabe numa porta de entidade só" (`Claimer.java:10-13`,
  quase palavra-por-palavra repetido em `Reaper.java:10-14`). Essa é uma regra de organização real e
  bem aplicada — exceto que `ExecutionStore.complete`/`completeAll` fazem exatamente a mesma coisa que
  essa regra diz que desqualificaria um tipo de ser `*Store`: cruzam para uma segunda porta
  (`JobStore`) na mesma operação atômica, só que aqui o cruzamento ficou *dentro* do `*Store` em vez
  de virar uma porta própria.
- **Leitura a favor do desenho atual:** ao contrário de `Claimer`/`Reaper` (que cruzam duas
  *tabelas*, `mohs_executions`+`mohs_job_definitions`, dentro de uma transação que eles mesmos abrem),
  `ExecutionStore.complete` cruza duas *portas* mas a chamada a `JobStore` é fornecida pelo chamador,
  não decidida aqui — o método não abre transação própria, só participa de uma que já existe. Extrair
  isso para uma terceira porta (`ExecutionCompleter`? nunca reusada em nenhum outro contexto) seria
  abstração sem um segundo caso de uso real — o tipo de indireção que o `CLAUDE.md` deste projeto
  explicitamente pede para evitar ("interface com uma única implementação é indireção, não
  abstração").
- **Recomendação:** não é uma correção óbvia — é uma inconsistência real entre uma regra articulada
  (Claimer/Reaper) e uma exceção não articulada (ExecutionStore) que vale nomear em uma frase na
  própria porta ("cruza para `JobStore` deliberadamente, ao contrário da regra de `Claimer`/`Reaper`,
  porque X") ou revisitar se `complete`/`completeAll` deveriam de fato virar uma porta separada
  quando o dispatch (M3) estiver todo ligado e o padrão de uso real for conhecido. Qualquer uma das
  duas decisões é aceitável — o que falta é a decisão estar registrada, não o comportamento mudar.

### RESP-2 — Papel duplo de `Enqueued`/`BatchCompleted` (recibo síncrono *e* evento assíncrono) atinge o próprio critério do projeto para virar ADR, mas não tem uma — BAIXO-MÉDIO

- **Onde:** `io.mohs.core.event.Enqueued` (Javadoc: "é tanto o recibo retornado pelos terminais de
  `ScheduleCommand`... quanto a variante de `ExecutionEvent` correspondente") e
  `io.mohs.core.event.BatchCompleted` (mesmo padrão, para `Batch#onCompletion`).
- **Por que isso é uma decisão de arquitetura, não um detalhe local:** os dois consumidores destes
  tipos têm pressões de evolução diferentes — o recibo síncrono serve quem acabou de chamar
  `schedule(...).now()` e quer confirmação; o evento assíncrono serve um `ExecutionListener` que
  nunca viu a chamada original. Hoje os campos que cada lado precisa coincidem por acaso (4 campos
  para `Enqueued`, 4 para `BatchCompleted`) — o dia em que um dos dois lados precisar de um campo que
  só faz sentido para ele (ex.: o listener querendo `idempotencyKey`, ou o recibo síncrono querendo
  algo específico de REST), o tipo único vira uma escolha entre "campo irrelevante para metade dos
  consumidores" ou "quebrar o papel duplo depois que código já depende dele". Isso é precisamente o
  tipo de trade-off que este `CLAUDE.md` pede para virar mini-ADR ("toda decisão relevante nasce com
  trade-offs explícitos... vira mini-ADR") — e as 26 ADRs existentes já cobrem decisões de peso
  comparável ou menor (ex. ADR-0004, só sobre dois renames).
- **Correção:** não é mudar o código — é escrever a ADR, mesmo que a decisão registrada seja "manter
  o papel duplo, aceitando o acoplamento, porque X" (o Javadoc já dá 80% do conteúdo). O valor está em
  ter o trade-off visível da próxima vez que alguém propuser adicionar um campo a qualquer um dos
  dois tipos.

---

## 6. Duplicação pequena de estrutura

### DRY-1 — `mapCandidate` duplicado byte-a-byte nos 4 `JdbcDialect` — BAIXO

- **Onde:** `H2JdbcDialect.java:37-39`, `MySqlJdbcDialect.java:37-39`, `PostgresJdbcDialect.java:37-39`,
  `SqlServerJdbcDialect.java:42-44` — os quatro têm o mesmo corpo:
  ```java
  private static Candidate mapCandidate(ResultSet rs, int rowNum) throws SQLException {
      return new Candidate(rs.getString("id"), rs.getString("job_key"), rs.getBoolean("allow_concurrent_executions"));
  }
  ```
- **Por que isto é diferente da duplicação de SQL, que está correta:** o `package-info.java` de
  `io.mohs.jdbc.dialect` já justifica, bem, por que o *texto SQL* de cada dialeto fica duplicado
  mesmo quando idêntico hoje (H2/MySQL/Postgres) — acoplar bancos independentes a uma coincidência de
  sintaxe atual seria o erro real. Essa razão não se aplica a `mapCandidate`: os três aliases de
  coluna que ele lê (`id`, `job_key`, `allow_concurrent_executions`) já são, por desenho, um contrato
  fixo e compartilhado entre os quatro `SELECT`s — é assim que os quatro dialetos conseguem devolver
  o mesmo `Candidate` em primeiro lugar. Duplicar o *mapeamento* não protege nada que a duplicação do
  SQL já não proteja.
- **Correção:** um único local — ex. `static Candidate fromResultSet(ResultSet rs)` como método
  estático no próprio record `Candidate` (`io.mohs.jdbc.dialect.Candidate`), chamado pelos quatro
  dialetos. (Aproveitar o comentário já existente em `docs/codereview-tuning.md`, JAVA-5, sobre o
  parâmetro `rowNum` nunca usado — vale trocar por `_` no mesmo commit, já que o método muda de
  lugar de qualquer forma.)

---

## 7. Atributos e parâmetros — verificado, sem achado

Nomes de campo/atributo foram lidos em todos os `record`s de `io.mohs.core`/`io.mohs.engine`/
`io.mohs.jdbc`/`io.mohs.rest` (os componentes de record *são* os atributos neste projeto — não há
builder mutável fora de `JobSpecImpl`/`MohsRunner.IoBuilder`/`CpuBuilder`/`ExecutionWindow.Builder`,
todos já com nomes de campo idênticos aos dos records finais que produzem). Nenhuma inconsistência
de nome de atributo encontrada — a única sobreposição de vocabulário neste eixo já está coberta em
CLASS-2 (`Candidate`/`Candidate`), que é sobre o nome do *tipo*, não de um campo dentro dele.

Nomes de parâmetro seguem convenção estável em toda a base (`key`/`id` para identidade, `now`/
`reference` para instante — os dois nomes coexistem deliberadamente e corretamente:
`NextFireCalculator.nextFireAfter(Schedule, Instant reference)` usa `reference` porque o valor nem
sempre é "agora" — fixed-delay ancora no fim da execução anterior — enquanto todo outro `now` do
projeto é, de fato, sempre o instante atual). A única observação adjacente a parâmetros está descrita
em RESP-1 (`JobStore` recebido como parâmetro de método em vez de colaborador de construtor) — é uma
questão de responsabilidade/desenho, não de nome: o parâmetro `jobStore` está corretamente nomeado
para o que é.

---

## 8. Tabela consolidada

| ID | Achado | Severidade | Arquivo(s) principal(is) |
|---|---|---|---|
| ORG-1 | Grafo de dependência intra-`core`/`rest` sem regra ArchUnit | ALTO | `ArchitectureTest.java`; evidência em `ExecutionState.java:3` |
| ORG-2 | `DispatcherTest` no pacote errado (`jdbc` em vez de `engine`) | MÉDIO | `src/test/java/io/mohs/jdbc/DispatcherTest.java` |
| CLASS-1 | `HandlerInvocation` deveria se chamar `JobHandler`/`Handler` | MÉDIO | `engine/HandlerInvocation.java` (não commitado) |
| CLASS-2 | Dois tipos `Candidate` sem relação, mesmo pacote-pai | BAIXO-MÉDIO | `jdbc/dialect/Candidate.java`, `jdbc/JdbcReaper.java:134` |
| CLASS-3 | `Schedule` vs. `ScheduleCommand` — raiz de nome compartilhada, conceitos não relacionados | BAIXO | `core/schedule/Schedule.java`, `core/ScheduleCommand.java` |
| CLASS-4 | `ExecutionState`/`ExecutionEventType` — nomes próximos, valores diferentes (complementa API-13) | BAIXO | `core/execution/ExecutionState.java`, `core/event/ExecutionEventType.java` |
| METHOD-1 | `create`/`insert` inconsistentes para a mesma semântica write-once | MÉDIO | `engine/BatchStore.java`, `engine/ExecutionStore.java` |
| DTO-1 | Sufixo `Response` vs. `View` sem regra escrita | MÉDIO | `io.mohs.rest.**` (DTOs) |
| RESP-1 | `ExecutionStore` cruza para `JobStore`, ao contrário da própria regra Claimer/Reaper | MÉDIO | `engine/ExecutionStore.java:67,85` |
| RESP-2 | Papel duplo `Enqueued`/`BatchCompleted` sem ADR | BAIXO-MÉDIO | `core/event/Enqueued.java`, `core/event/BatchCompleted.java` |
| DRY-1 | `mapCandidate` duplicado 4x sem necessidade | BAIXO | `jdbc/dialect/*JdbcDialect.java` |

---

## 9. Ordem de ataque sugerida

1. **`git mv` do `DispatcherTest`** (ORG-2) — cinco minutos, zero risco, fecha a única violação de
   uma convenção que hoje é 100% consistente em todo o resto da suíte.
2. **Renomear `HandlerInvocation` → `JobHandler`** (CLASS-1) — enquanto o arquivo ainda não foi
   commitado nesta sessão é o momento de menor custo possível; adiar significa competir com mais
   call sites a cada milestone.
3. **Adicionar as regras ArchUnit de dependência intra-`core`/intra-`rest`** (ORG-1) — mesmo padrão
   já usado para as outras 7 regras; transforma 5 `package-info.java` de promessa em garantia.
4. **Escrever a regra `Response`/`View`** (DTO-1) antes que M3 adicione o próximo DTO sob a moeda ao
   ar — ou colapsar para um sufixo só.
5. **Padronizar `create`/`insert`** (METHOD-1) e **renomear o `Candidate` privado de `JdbcReaper`**
   (CLASS-2) — cosmético, cabem no mesmo commit pequeno.
6. **Deduplicar `mapCandidate`** (DRY-1) — junto do item 5, já que os quatro arquivos já estão em
   contexto.
7. **Registrar as duas mini-ADRs** (RESP-1: decisão consciente sobre o cruzamento `ExecutionStore`→
   `JobStore`; RESP-2: papel duplo de `Enqueued`/`BatchCompleted`) — nenhuma exige mudança de código,
   só o registro que o resto do projeto já pratica.

---

## 10. Pontos fortes observados

- **`package-info.java` como unidade de documentação arquitetural, não boilerplate**: os 23 arquivos
  lidos nesta revisão documentam intenção, direção de dependência *e* o padrão de projeto aplicado
  (Repository/Data Mapper de PoEAA, Observer/Chain of Responsibility/Builder de GoF) — nível de
  disciplina incomum mesmo comparado a projetos maduros.
- **Família `*Store`/`Jdbc*Store`**: `JobStore`/`ExecutionStore`/`BatchStore`/`RateLimitStore`/
  `NodeStore` ↔ `JdbcJobStore`/`JdbcExecutionStore`/`JdbcBatchStore`/`JdbcRateLimitStore`/
  `JdbcNodeStore` — mapeamento 1:1 perfeito, zero exceção encontrada.
- **`find`/`findAll`/`findBy*` com cardinalidade de retorno sempre justificada**: `Optional` para
  chave única, `Stream` para tabela sem teto (com a ressalva DBTUNE-7 já registrada em
  `docs/codereview-tuning.md`), `List` onde o tamanho é estruturalmente limitado (`NodeStore`, com o
  motivo escrito no próprio Javadoc).
- **`tryX` reservado para operação que pode falhar** (`tryIncrementRunningExecutions`, retorna
  `boolean`) vs. `incrementX` para operação que sempre sucede (`BatchStore.incrementSucceeded`) —
  convenção Java padrão aplicada com disciplina, nunca invertida.
- **Duplicação de SQL entre dialetos idênticos hoje (H2/MySQL/Postgres) é deliberada e bem
  justificada por nome** (citando Quartz `*Delegate`, Hibernate `LimitHandler`) — um caso raro de um
  projeto documentar corretamente *por que* não teve preguiça de abstrair algo que parecia repetido.
- **Nomenclatura de teste uniformemente descritiva**: as ~57 classes de teste varridas nesta revisão
  não têm uma única ocorrência de `testX`/nome não-descritivo — todas seguem frase-em-camelCase
  (`claimIsMutuallyExclusiveAcrossConcurrentNodes`, `reclaimExpiredReleasesTheJobConcurrencySlot`).
