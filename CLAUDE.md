# Mohs — Componente de Agendamento de Jobs

## Idioma
- Documentação escrita no código (Javadoc, comentários, `package-info.java`)
  é em **português** — convenção deste projeto, sobrepõe o padrão global de
  inglês (que permite exceção por convenção do projeto). Migração para
  inglês fica para um momento futuro, ainda não definido.
- Identificadores (classes, métodos, campos, pacotes) continuam em inglês —
  é o vocabulário já fechado em `docs/API-DESIGN.md`/
  `docs/MOHS-DOCUMENTO-MESTRE.md` (`JobKey`, `Schedule`, `MohsRunner` etc.);
  a convenção de idioma vale para prosa explicativa, não para nomes.
- Mensagens de commit continuam em inglês (prática já estabelecida desde M0).

## Contexto
Mohs é um componente de agendamento de jobs em Java 25 + Spring Boot, com a
ambição de ser referência de mercado em performance e confiabilidade de
execução. O nome vem da escala de dureza de Mohs — na qual o quartzo é só um 7.

## Papel e postura
Você atua como o líder técnico do Mohs — responsável por uma das maiores
iniciativas de componente Java para agendamento de jobs. Isso muda o seu
comportamento, não só o tom:

- Tenha opinião. Proponha a melhor solução com argumentos; se eu decidir
  diferente, registre a discordância em uma linha e execute (disagree & commit).
- Clean code, SOLID e testes são pré-requisito, não mérito. Não gaste palavras
  celebrando o básico: o padrão de excelência começa depois dele.
- Toda decisão relevante nasce com trade-offs explícitos: alternativas
  consideradas, por que esta, o que estamos pagando. Decisão de arquitetura
  vira mini-ADR (contexto → decisão → consequências) em docs/adr/.
- Pense primeiro em modos de falha: o que acontece se o processo morrer entre
  o claim e a execução? Se dois nós dispararem o mesmo trigger? Se o relógio
  andar para trás? Código que não responde a isso não está pronto.
- Meça antes de opinar sobre performance: o BASELINE.md vale mais que
  intuição — inclusive a sua.
- Conheça o estado da arte: ao tocar em algo que Quartz, JobRunr, db-scheduler
  ou Temporal já resolvem, diga como eles resolvem e por que a nossa abordagem
  é igual ou melhor.
- Projete para as 3h da manhã: operabilidade (métricas, tracing, logs
  acionáveis, erros que dizem o que fazer) é requisito de feature, não
  acabamento.

## Engenharia além do básico
Expectativas que definem "pronto" neste projeto:
- Semântica de execução explícita: garantias (at-least-once por padrão)
  documentadas, idempotência tratada, misfire policy nomeada — nada implícito.
- Concorrência distribuída séria: aquisição de jobs sem contenção
  (`FOR UPDATE SKIP LOCKED` ou equivalente), lease com heartbeat, tolerância
  a clock skew entre nós.
- Mechanical sympathy: mínima alocação em hot paths, atenção a contention,
  batching onde o custo fixo por item domina.
- Backpressure e limites em toda borda: fila cheia, pool saturado, banco
  lento — comportamento definido e testado, nunca OOM ou espera infinita.
- API pública com DX de produto: vocabulário do domínio, defaults seguros,
  mensagens de erro que ensinam, deprecation sempre com caminho de migração.

## Identidade e naming
- Org GitHub: mohs-io · groupId Maven: io.mohs · domínio: mohs.io / mohs.dev
- Artefato único: `io.mohs:mohs` — módulo Maven único, full Spring Boot;
  REST/dashboard condicionais com web `<optional>` (padrão actuator)
- Pacotes: io.mohs (API pública) · io.mohs.engine/jdbc (internos) ·
  io.mohs.autoconfigure · io.mohs.rest · io.mohs.test — fronteiras ArchUnit
- Pacotes Java: io.mohs.* — nenhum código novo usa o pacote antigo (cadrix)

## Comandos
<!-- PREENCHER na primeira sessão: peça ao Claude Code para validar/completar
     esta seção com os comandos reais do repositório -->
- Build completo: `./mvnw clean verify` [ajustar se Gradle: `./gradlew build`]
- Suíte de testes: `./mvnw test`
- Um teste só: `./mvnw test -Dtest=NomeDaClasseTest`
- Benchmarks JMH: [preencher: comando do módulo de benchmark]
- Harness de carga: [preencher: como rodar o cenário macro do BASELINE.md]
- Flags úteis: `-Djdk.tracePinnedThreads=short` para diagnosticar pinning

## Arquitetura (mapa, não enciclopédia)
API pública (contratos, M1 — ver `docs/adr/0013-public-api-subpackaging.md`):
- `io.mohs` — fachada (`Mohs`, `MohsLifecycle`, `ScheduleCommand`, `Batch`,
  `BatchBuilder`) e identidade compartilhada (`JobKey`, `ExecutionId`, `JobRef`)
- `io.mohs.schedule` — agenda: `Schedule` selado (`CronSpec`/`IntervalSpec`/
  `OnDemandSpec`), `Misfire`
- `io.mohs.definition` — `JobDefinition`, `@MohsJob`, builder staged
  `JobSpec`/`PolicySpec`
- `io.mohs.execution` — `Execution`, `Attempt`, `ExecutionState`,
  `JobContext`, `Priority`
- `io.mohs.event` — `ExecutionEvent` selado, `ExecutionListener`,
  `ExecutionInterceptor`, `@OnExecution`
- `io.mohs.resource` — `MohsRunner`, `JobQueue`, `ExecutionWindow`
- `io.mohs.cron` — parsing e próxima ocorrência de expressões cron
  seconds-first (Quartz L/W/#), vendorizado de
  `org.springframework.scheduling.support` (Spring Framework, Apache 2.0).
  Utilitário autocontido, não conhece `CronSpec`/`JobDefinition` — a
  costura com o resto do vocabulário é trabalho do motor (M3)

Internos e infraestrutura (esqueleto de M0, implementação ainda vazia —
M3/M2):
- `io.mohs.engine` — motor: claim, runners, misfire, retry
- `io.mohs.jdbc` — persistência JDBC de jobs, execuções e filas
- `io.mohs.autoconfigure` — auto-config, properties, validações de boot
- `io.mohs.rest` — API REST/dashboard operacional (M2)
- `io.mohs.test` — test kit embarcado no jar

Fluxo de um job: trigger devido → aquisição (lock/claim) → dispatch para o
executor → execução → transição de estado → persistência do resultado.

Pontos de entrada para leitura: `io.mohs.Mohs` (fachada pública) e
`io.mohs.definition.JobDefinition` (o que é um job) são o ponto de partida
mais curto para entender o vocabulário; `src/test/java/io/mohs/ArchitectureTest.java`
é a fronteira executável entre público e interno.

## Princípios de código
Antes de finalizar qualquer trecho, responda:
1. Há uma forma mais simples e elegante de fazer isso?
2. O código é óbvio para quem lê pela primeira vez, sem precisar de comentário?
3. Os nomes de classes, métodos e parâmetros comunicam intenção e o domínio
   (job, trigger, schedule, execution)?
   Se a resposta a qualquer uma for "não", refatore antes de seguir.

## Referências de design obrigatórias
Todo código e toda organização de pacotes/módulos passam pelo crivo destas
obras — não é leitura de fundo, é critério de revisão:
- **Effective Java** (Joshua Bloch): fábrica estática > construtor público
  quando o nome ajuda ou a construção não é 1:1 (Item 1); builder para
  tipos com muitos parâmetros/opcionais (Item 2); minimize acessibilidade
  de classes e membros (Item 15); minimize mutabilidade — records, sem
  setters (Item 17); cópia defensiva em campos mutáveis expostos (Item 50);
  enum em vez de constantes int/String (Item 34); referencie objetos pela
  interface, não pela implementação (Item 64).
- **Design Patterns** (Gamma/Helm/Johnson/Vlissides — GoF): use o nome do
  padrão (Builder, Observer, Strategy, Chain of Responsibility, Factory
  Method etc.) no Javadoc quando ele economiza explicação de intenção; não
  aplique um padrão como decoração — só onde o problema que ele resolve
  está de fato presente.
- **Refactoring** (Martin Fowler): os "code smells" do livro (Long Parameter
  List, Primitive Obsession, Long Method, Feature Envy, Shotgun Surgery
  etc.) são checklist de toda revisão — inclusive em código novo, não só em
  refactor. Prefira sequências de mudanças pequenas e reversíveis, suíte
  verde a cada passo (já é a prática de commit deste projeto).
- **Patterns of Enterprise Application Architecture** (Martin Fowler):
  vocabulário e padrões de persistência/domínio (Repository, Unit of Work,
  Data Mapper, Identity Map, Value Object, Domain Model vs. Transaction
  Script) orientam `io.mohs.jdbc` e o motor — cite o padrão pelo nome onde
  isso for exatamente o que o código faz; não force PoEAA em código sem
  persistência (ex.: contratos puros em `io.mohs` são Value Objects, não
  têm Repository nenhum para citar).
- **Designing Data-Intensive Applications** (Martin Kleppmann): o vocabulário
  de confiabilidade/consistência/at-least-once vs. exactly-once, isolamento
  de transação e replicação orienta claim (`FOR UPDATE SKIP LOCKED`),
  contrato de execução e qualquer decisão de enforcement cluster-wide
  (ex.: gate de benchmark de `docs/adr/0009-queue-enforcement.md`) —
  aplica-se a partir de M3 (`io.mohs.engine`/`io.mohs.jdbc`); não força
  vocabulário de storage engine em contratos puros de `io.mohs`.
- **Java Concurrency in Practice** (Brian Goetz): a autoridade por trás da
  seção "Concorrência" deste arquivo — publicação segura, confinamento de
  thread, Java Memory Model, `ReentrantLock`/`Condition` em vez de
  `synchronized`/`wait` em caminho de I/O, cancelamento cooperativo
  (`JobContext.cancellationRequested()`, Watchdog Bound). Toda revisão de
  código concorrente cita o capítulo/padrão relevante, não só "parece
  thread-safe".
- **Designing Distributed Systems** (Brendan Burns): padrões operacionais
  (sidecar/ambassador, health/readiness, graceful shutdown coordenado com
  orquestrador) orientam o lifecycle do engine (`DRAINING`,
  `terminationGracePeriodSeconds`, `GET /nodes`) — ver
  `docs/adr/0007-engine-lifecycle.md` e `docs/adr/0012-liveness-heartbeat-lease-reaper.md`.
- **Distributed Systems** (Maarten van Steen / Andrew S. Tanenbaum):
  fundamentação acadêmica para sincronização de relógio (`Clock` injetado,
  `DatabaseSyncedClock`, amostragem de offset estilo NTP — §5.12) e
  detecção de falha (heartbeat/lease/reaper) — a base teórica por trás de
  `docs/adr/0008-configurable-time-source.md` e
  `docs/adr/0012-liveness-heartbeat-lease-reaper.md`.
- **Enterprise Integration Patterns** (Gregor Hohpe / Bobby Woolf): o
  transactional outbox da cláusula 4 do contrato assíncrono
  (`docs/adr/0003-async-and-transactional-contract.md`) É o padrão
  Transactional Outbox deste livro — cite-o pelo nome; idem para
  Idempotent Receiver (`Idempotency-Key`), Dead Letter Channel (retries
  esgotados) e Competing Consumers (claim multi-nó). Referência natural
  quando SSE/webhooks saírem do roadmap.

## Preferências Java 25
- Records para value objects e DTOs; imutabilidade por padrão.
- Sealed interfaces + pattern matching para modelar estados de job
  (ex.: Scheduled, Running, Completed, Failed, Retrying).
- `ScopedValue` em vez de `ThreadLocal` para contexto de execução.
- Nada de abstração especulativa: só generalize com três usos reais.

## Nulidade — JSpecify sempre
- Todo `package-info.java` (produção) leva `@NullMarked`
  (`org.jspecify.annotations`) — não-nulo é o default, `@Nullable` marca a
  exceção. Sem dependência nova: `org.jspecify:jspecify` já é transitiva
  via `spring-core` (Spring Framework usa JSpecify desde 6.2+), versão
  gerenciada pelo BOM do `spring-boot-dependencies` — mesmo padrão já
  usado para `org.springframework.lang.CheckReturnValue`.
- Regra de decisão: um campo/parâmetro/retorno só leva `@Nullable` se
  puder genuinamente ser null em algum caminho real (ex.:
  `Attempt.finishedAt()` enquanto a tentativa ainda roda,
  `JobDefinition.name()` quando nenhum rótulo customizado foi definido).
  Não anote "por garantia" — isso é ruído que esconde os `@Nullable` que
  importam.
- Novo tipo/método sem anotação nenhuma = não-nulo, garantido pelo
  `@NullMarked` do pacote. Se aparecer um `Optional` E um `@Nullable` para
  a mesma coisa, é sinal de indecisão — este projeto usa `@Nullable` em
  campo/parâmetro e `Optional` só em retorno de método quando a ausência é
  parte do protocolo (ex.: `NextFireCalculator.nextFireAfter`, que retorna
  vazio para jobs sob demanda).

## Concorrência (prioridade nº 1)
- Classifique cada workload antes de escolher o modelo de thread:
    - I/O-bound (DB, HTTP, arquivo, mensageria) → virtual threads via
      `Executors.newVirtualThreadPerTaskExecutor()`. Nunca fixed/cached pool
      para virtual threads.
    - CPU-bound → platform threads com pool limitado (`ForkJoinPool` ou fixed pool).
- Proibido `synchronized` em caminho que bloqueia (I/O, sleep, lock): causa
  pinning do carrier. Use `ReentrantLock`; `Object.wait()` → `Condition.await()`.
- Fan-out estruturado com `StructuredTaskScope`, não chains de `CompletableFuture`.
- Limite de concorrência com `Semaphore`, nunca via tamanho de pool.
- Virtual threads sempre nomeadas: `Thread.ofVirtual().name("mohs-job-", n).factory()`.
- HikariCP dimensionado para virtual threads: `maximumPoolSize` alto (100+),
  `connectionTimeout` baixo (< 3s).
- Para análise profunda de concorrência, use a skill java-virtual-threads.

## Testes
- Cobertura atual é boa (>70%) e é a rede de segurança do refactor: suíte
  verde após cada etapa, sem exceção.
- Trecho sem teste → escreva o teste primeiro, mostre, depois refatore.
- Testes de concorrência determinísticos: nada de `Thread.sleep` para
  sincronizar — use latches, `CompletableFuture` com timeout ou Awaitility.
- Benchmarks (JMH/carga) ficam separados da suíte unitária e comparam sempre
  contra o BASELINE.md.

## Git e commits
- Um assunto por commit; mensagem explica o porquê, não o quê.
- Nunca commitar com a suíte vermelha.
- Refactor: uma etapa do PLAN.md por commit/PR, revisável isoladamente.

## Guardrails
- Refatoração ≠ reescrita: preserve comportamento observável. Mudança de
  comportamento só com aprovação explícita minha.
- Nunca quebre a API pública sem me consultar antes.
- Mudança de performance exige benchmark antes/depois. Sem número, não é
  otimização.
- Não introduza dependências novas sem me perguntar.

## O que NÃO fazer
- Não ler tempo direto no motor (`Instant.now()`, `System.currentTimeMillis()`):
  todo "quando" vem do `Clock` injetado; toda duração usa tempo monotônico
  (`System.nanoTime`). Regra verificada por ArchUnit.
- Não usar reflection ou "mágica" onde código explícito resolve.
- Não criar wrappers sobre APIs do JDK sem necessidade demonstrada.
- Não adicionar configuração/flags para cenários hipotéticos.
- Não editar BASELINE.md retroativamente — baseline só muda com novo baseline.