# PLAN — Validação de release: o que falta para chamar de estável

Estado: **Phase 6 encerrada** (o PLAN dela vive no histórico do git, até
c5fbcb4). Esta etapa não é uma fase do redesign — é a bancada de release:
descobrir o que quebra antes que um usuário descubra.

Base: `ARCHITECTURE_REDESIGN_PLAN.md` §20.2 (a suíte S1–S10 que "deve
existir"), ADR-0057, ADR-0058.

---

## O que a sessão de 2026-08-23 estabeleceu

Quatro commits: `a42c796` (janela do `stop`), `99667c4` (o teste que a
prende), `5708c72` (orçamento de retry por default), `dc45a0c` (fim do
particionamento).

**Bancada nova:** dez cenários em `../../mohs-benchmark/src/test/java/io/mohs/store/jdbc`,
que sobem N engines reais numa JVM contra Postgres do Testcontainers, com a
fiação de `MohsAutoConfiguration` (group commit ligado, mesma razão de
executor de eventos, mesma ordem de shutdown). Rodam por nome — o padrão
default do surefire não pega `*Scenario`:

```
./mvnw -pl mohs-benchmark test -Dtest=NomeDoScenario
```

| cenário | veredito |
|---|---|
| `RateLimitCeilingScenario` (S7) | ✅ zero over-delivery no envelope do bucket; job sem limite não paga pelo vizinho |
| `BatchCompletionScenario` (S10) | ✅ contadores certos na tabela; ⚠️ o canal de eventos descarta ~0,3% |
| `RollingUpdateScenario` (S9) | ✅ rollout que completa não perde nada; gap conhecido afirmado no valor atual |
| `ColdStartScenario` | ✅ limpo |
| `ConcurrentMigrationScenario` | ✅ 6 réplicas, 4 versões, uma vez cada |
| `RecurringTriggerScenario` | ✅ zero dupla materialização; pause segura |
| `NodeChurnScenario` (3 braços) | ✅ nada perdido; a aresta de `retries=0` é guarda, não demonstração |
| `ShutdownLatencyScenario` | ✅ 0,22s com 256 execuções em voo |
| `OverviewLatencyScenario` (S5) | ✅ 14,7 ms contra alvo de 100 |

**Rodada de 2026-08-30 (Docker no ar, Testcontainers reais).** A suíte inteira
passou pela primeira vez desde a normalização das PKs: **758 testes, 0 falhas,
0 erros** — inclusive o guardião estrutural do Postgres, que é quem prova que o
caminho Flyway e o `schema-postgresql.sql` convergem depois da mudança de chave.

| cenário | veredito |
|---|---|
| `ScheduleDensityScenario` (S4) — 2.000 definições | ✅ lag 0,125s (alvo < 2s) |
| `ScheduleDensityScenario` (S4) — 20.000 definições | ✅ lag 0,259s |
| `BurstAbsorptionScenario` (S3) — 40k | ✅ fila zerada, 40.000 sucessos, zero perda |
| `BurstAbsorptionScenario` (S3) — 100k (o critério) | ❌ teto do ambiente, ver abaixo |
| `NodeChurnScenario` — SQL Server | ✅ 3 braços, nada perdido |
| `NodeChurnScenario` — MySQL | ✅ 3 braços, nada perdido |
| `BatchCompletionScenario` — SQL Server | ⚠️ contadores duráveis certos; canal de eventos descartou 118/20.000 (0,59%) |
| `BatchCompletionScenario` — MySQL | ⚠️ idem, 54/20.000 (0,27%) |

**Sobre o ❌ do S3:** o critério (100k em 10s, recuperado em 60s) exige ~1.700
execuções/s sustentadas. Esta bancada entregou **~1.150/s** — três engines mais
o harness na MESMA JVM contra um Postgres de container, que não é a plataforma
do BASELINE (app demo contra Postgres local tunado, ponto de operação vários
múltiplos acima). O drenar foi linear, sem stall e com **zero perda**; foi só
mais lento que o orçamento. Rodar o tamanho do critério exige a plataforma do
BASELINE; `-Dmohs.scenario.burst=` existe para obter uma curva de absorção
significativa em qualquer outra.

**Sobre o ⚠️ do batch:** as asserções dos contadores duráveis e da contagem
terminal-vs-por-tentativa passaram nos dois dialetos. O que quebrou foi a
PRÉ-CONDIÇÃO sobre o canal de eventos — a pendência já registrada mais abaixo,
que os Tier 2 confirmam ser real e um pouco pior fora do Postgres.

**Sobre o S4 e a escala do critério:** o seed mede **18ms por upsert** (uma
viagem de rede cada, no DataSource sem pool que a bancada usa de propósito).
São 6 minutos para as 20k do default e ~50 HORAS para 10M — o seeder em lote
não é conveniência, é pré-requisito, e enquanto ele não existir o critério da
§20.2 segue NÃO ATENDIDO.

**Chaos (E6) re-rodado no binário atual** — era lacuna, porque o `stop()`
mudou: S6 (kill −9), S8 (banco pausado 30s) e SUSPEND (nó congelado),
30k execuções cada, **zero perda nos três**; re-execução exatamente igual
ao que estava em voo no kill; zero dupla-conclusão no SUSPEND.

---

## Passos (um por commit; suíte verde e pipeline de DoD ao fim de cada um)

- [ ] **P0 — Split de `io.mohs.engine` em subpacotes. BLOQUEADO por decisão.**
      É o único módulo plano do reator: 33 classes num pacote, contra 7
      subpacotes de `io.mohs.core` e 9 de `io.mohs.rest`; `Engine.java` tem
      1.773 linhas, 78 campos, 101 métodos e 12 colaboradores. O agrupamento
      já está latente (portas · política pura · runtime · registries ·
      fachada), e subpacotes não violam a regra 1:1 da ADR-0044 — ela mapeia
      pacote-RAIZ para módulo.
      **O bloqueio** (ADR-0061, verificado ao tentar): cinco classes de teste
      do motor vivem em outros módulos no pacote `io.mohs.engine`
      (`EngineTest`, `DispatcherTest`, `CompletionBatcherTest`,
      `ScheduleCommandImplTest` em `mohs-store-jdbc`; `MohsImplTest` em
      `mohs-test`), porque precisam de banco e o engine não vê JDBC. Mover
      `Engine` para `io.mohs.engine.runtime` tira delas o acesso
      package-private a `cappedByNextFire`/`earliestArmedFire`/`EngineSettings`,
      e alargar visibilidade para compensar deixaria o código PIOR.
      **Decidir antes de mexer:** (a) módulo `mohs-engine-it` não publicado
      que dependa dos dois e hospede as cinco classes, ou (b) seams públicos e
      testes de caixa-preta, ao custo de apagar testes de função pura que hoje
      pegam defeito real. É escolha de arquitetura, não trabalho mecânico —
      o mesmo bloqueio impede `module-info` nesses dois módulos.

- [x] **P1 — Normalizar as PKs da história. FEITO em 2026-08-29** (decisão: normalizar
      junto da V5). `mohs_execution` passa a `(execution_id)` e `mohs_attempt` a
      `(execution_id, number)` — a mesma forma dos outros três dialetos. Saíram
      `idx_mohs_execution_id` e `idx_mohs_attempt_exec` (viraram as chaves), os dois
      `TERMINAL_UPDATE` colapsaram num só e o `executionCreatedAt` sumiu de
      `LeaseStore.CompletionResult` e de `Dispatcher.Grant`. A V5 checa duplicata de id
      ANTES de copiar, para que a garantia nova falhe nomeando a linha em vez de dizer
      "duplicate key". **Falta verificar contra Postgres real** — a suíte Testcontainers
      não rodou nesta sessão (sem Docker no ambiente); os testes novos existem
      (`JdbcLeaseStorePostgresTest#theSchemaRefusesASecondRowWithTheSameExecutionId` e o
      par de `mohs_attempt`), o guardião estrutural e o scanner da ADR-0043 idem.
      O texto original da pendência:

- [ ] ~~**P1 — Normalizar as PKs da história. TEM PRAZO.**~~
      `mohs_execution (created_at, execution_id)` e `mohs_attempt
      (finished_at, execution_id, number)` têm a coluna de tempo à frente
      porque o Postgres exigia a chave de partição na PK. Sem partição, a
      medição diz que **a PK não serve query nenhuma**: o planner escolhe
      `idx_mohs_execution_id` e rebaixa `created_at` a `Filter`, e
      `TERMINAL_UPDATE`/`TERMINAL_UPDATE_UNPRUNED` são o mesmo plano
      (4 buffers, 0,047 ms). É um índice de duas colunas mantido em toda
      escrita só pela unicidade.
      **O prazo:** a parte cara — recriar a tabela — a `V5` já está
      pagando. Fazer junto custa ~zero; fazer depois custa uma `V6` que
      copia tudo de novo, com a mesma janela de indisponibilidade.
      Decida explicitamente **antes** de a V5 rodar numa base real,
      mesmo que a decisão seja "não".
      Escopo se for sim: PK natural, `idx_mohs_execution_id` vira a PK e
      é dropado, colapsar as duas constantes de UPDATE, e some o
      `executionCreatedAt` que viaja do claim até a conclusão. **Muda
      garantia de unicidade** (hoje o schema não impede dois
      `mohs_execution` com mesmo id e `created_at` diferente) — exige
      teste da garantia nova e passagem pelo scanner da ADR-0043.

- [ ] **P2 — Soak de 12–24h.** *(bancada escrita em 2026-08-29:
      `mohs-benchmark/scripts/soak.ps1` — carga contínua modesta, amostra a cada
      minuto vazão, profundidade de fila, heap, threads, conexões e drift de
      relógio, e fecha comparando a primeira metade da corrida com a segunda.
      Falta RODAR.)* O drain mais longo desta bancada foi ~1
      min. Nada aqui roda por horas, e é o único jeito de aparecer o que
      só uptime longo mostra (vazamento lento, drift de relógio,
      crescimento de conexões, degradação de plano com a história
      crescendo). Um nó, carga modesta e contínua, com
      `write-amplification.ps1` amostrando. Critério: vazão e latência
      estáveis do início ao fim, heap sem tendência, zero execução
      perdida.

- [ ] **P3 — Os cenários em Tier 2 (SQL Server e MySQL).** *(destravado em
      2026-08-29: `ScenarioCluster` recebe um `JdbcDialect` e `ScenarioBackend`
      escolhe o banco por `-Dmohs.scenario.backend=postgres|sqlserver|mysql`;
      `NodeChurnScenario` e `BatchCompletionScenario` — os dois que dependem de
      semântica de lock — já rodam por ele, e RODARAM em 2026-08-30 (tabela acima):
      `NodeChurnScenario` verde no SQL Server, `BatchCompletionScenario` com os
      contadores duráveis certos nos dois dialetos, e `NodeChurnScenario` verde também
      no MySQL. Falta converter os outros oito.)*
      Os dez
      `*Scenario` rodam **só em Postgres** — claim concorrente, reclaim de
      posse e fechamento de lote sob contenção real estão medidos apenas
      no dialeto de referência. A lacuna está registrada com gatilho na
      ADR-0050; o trabalho é generalizar o `ScenarioCluster`, hoje amarrado
      a `PostgresJdbcDialect` + `PostgresTestSupport`. Comece pelo
      `NodeChurnScenario` e pelo `BatchCompletionScenario` — são os que
      dependem de semântica de lock.

- [ ] **P4 — S3 e S4 escritos em 2026-08-29 e RODADOS em 2026-08-30** (tabela acima).
      O S4 passa nas duas densidades medidas e o S3 absorve 40k sem perda; o que
      continua em aberto é a ESCALA de cada um — 100k do S3 exige a plataforma do
      BASELINE, e as 10M do S4 exigem o seeder em lote. Descrição original:
      **S3** — `BurstAbsorptionScenario`: 100k enfileirados em 10 ondas de 1s (uma
      inserção única de 100k não é burst, é carga em lote), critério na IDADE do mais
      velho da fila, não no tamanho dela. **S4** — `ScheduleDensityScenario`: mede o lag
      de materialização, o passo que nenhum outro cenário isola (todos semeiam
      `mohs_ready` direto). **A escala do S4 não é a da §20.2:** as definições são
      semeadas pelo `JobStore` real, uma por upsert, porque a alternativa era um INSERT
      em lote na bancada repetindo as colunas de `mohs_job_definitions` por dialeto — a
      ~0,5ms cada, 10M é mais de uma hora só de seed. O default é 20k e o número real é
      opt-in (`-Dmohs.scenario.definitions=10000000`). **Enquanto essa corrida não
      acontecer, o critério da §20.2 está NÃO ATENDIDO, não atendido.** A peça que falta
      é um seeder em lote, e ele pertence ao store, não à bancada.

- [ ] **P5 — Nota de release.** Consolidar o que o usuário precisa saber
      antes de subir, e que hoje está espalhado por ADRs:
      - `retries` default mudou de 0 para 1 (ADR-0057) — inclusive o caso
        agudo: sob falso positivo de detecção de morte, duas invocações
        CONCORRENTES da mesma execução, furando `preventOverlap`;
      - a cadeia Flyway V1→V4 é destrutiva para dados da era single-table
        (ADR-0048);
      - o checksum da `V3` mudou: base que já a aplicou precisa de
        `UPDATE mohs_schema_history SET checksum = …` manual (ADR-0058);
      - a `V5` é janela de manutenção em base grande, e **drene o cluster
        antes** — com nós no ar, o pool esgota, o heartbeat não consegue
        conexão e os pares reapam um nó vivo (ADR-0058).

---

## Pendências registradas (com gatilho) — não são passos

- **O canal de eventos descarta sob rajada.** ~0,3% dos eventos terminais
  em 20k, mesmo dimensionado na razão 4:1 que o `PERFORMANCE.md`
  recomenda. É best-effort por contrato; o que importa é a consequência:
  **listener não é canal confiável sob carga** — contadores duráveis
  (`mohs_batches`) são a fonte de verdade. **Gatilho:** primeira
  reclamação de evento perdido, ou requisito de entrega garantida.
- **A espera pós-loop do `stop()` não renova a lease do nó.** Com os
  defaults (`grace` 30s, `node-lease-ttl` 15s), mais da metade da espera
  está descoberta e um par pode reclamar o que ainda roda. Ou renova em
  fatias, ou corta a espera no resíduo — muda a ADR-0051. **Gatilho:**
  primeira perda observada em shutdown com grace longo.
- **Durabilidade do group commit antes do heartbeat final** (~5ms): com o
  `CompletionBatcher`, a future completa no *submit*, não no commit.
  Mesma classe do defeito do `stop()`, ordem de grandeza menor. ADR-0047.
- **Rolling update heterogêneo queima o orçamento de retry** dos shards
  do nó cego (decisão 2 do PLAN da Phase 6 — handler-aware claiming ficou
  de fora). Com `retries=1` isso deixou de ser perda e virou retentativa.
  **Gatilho:** starvation observada, ou reclamação de retry queimado.
- **Latência de dispatch recorrente:** p50 ~300ms, p95 ~450ms num cluster
  de 3 nós (medido no `RecurringTriggerScenario`). É o ❌ do gate do S6.4,
  atribuído à ADR-0054 — sem o tier NOTIFY, um enqueue só é imediato se
  quem recebeu for o dono do shard. **A atribuição estava incompleta:** a
  ADR-0059 mostrou que parte do atraso era LOCAL — o backoff dormindo por
  cima do `next_fire_at` que o próprio nó conhecia — e não tem nada a ver
  com descoberta cross-nó. **Gatilho:** remedir o `RecurringTriggerScenario`
  com o cap por gatilho para saber quanto sobra de fato para a ADR-0054;
  até lá o p50 registrado aqui é histórico, não o número atual.
- **`mohs_ready.visible_at` continua sem horizonte** (ADR-0059, o que ela
  não cobriu): um retry marcado para `now + 2s` é descoberto pelos mesmos
  pontos de backoff que o gatilho recorrente era. O horizonte das
  definições saiu de graça (já em memória); o da fila exige um
  `min(visible_at)` por tick. **Gatilho:** atraso de retry observado, ou a
  medição acima mostrando que a fila responde pelo que sobrou.
- **A densidade da agenda ainda não foi MEDIDA em ticks/s.** Os dois 🟡 do
  review de 2026-08-29 foram corrigidos — `poll-interval` voltou a ser o
  piso do sono (ADR-0059, decisão 5) e o filtro pareado ganhou teste. Com
  o piso de volta, a aritmética limita o dano: no default de 25ms, um
  gatilho custa no máximo um tick a mais por 25ms de janela, e gatilhos que
  caem na mesma janela voltam a sair no mesmo lote. O que continua sem
  número é o regime denso: N ∈ {10, 100, 1000} jobs recorrentes, ticks/s e
  statements/s por nó, contra os 4,0 consultas/s de BASELINE "Phase 6 —
  S6.5". **Gatilho:** antes de anunciar custo ocioso com jobs recorrentes
  declarados, ou primeira instalação acima de ~100 jobs recorrentes.
- **`hasLiveSchedulerOccurrence` é O(história do job)** — 2.538 buffers
  por chamada, e a retenção por `DELETE` (que a ADR-0058 torna o caminho)
  **agrava**, deixando tuplas mortas no mesmo heap scan. **Gatilho:**
  medição sob carga real de upsert.
- **`fillfactor` em `mohs_execution`** está no default 100, e o perfil é
  1 INSERT + 1 UPDATE por linha: medido **0% de HOT updates**, ou seja
  todo UPDATE terminal escreve nos três índices. Com 80 seriam 22% e −19%
  de WAL. **Gatilho:** o número de produção do `write-amplification.ps1`
  — teste sintético não basta (a regra da casa).
- **Guarda de nós vivos na `V5`** (RAISE se houver `mohs_nodes` com lease
  válida): troca um outage invisível por um boot failure acionável, ao
  custo de exigir drenagem até em instalação pequena. **Gatilho:**
  primeira conversão real com nós no ar.
- **`repair()` antes de `migrate()` no `MohsFlyway`:** resolveria a quebra
  de checksum da V3 e as futuras; o custo é aceitar em silêncio qualquer
  edição de migração aplicada. **Gatilho:** segunda migração que precise
  ser corrigida depois de aplicada.
- **`pg_constraint` no guardião estrutural + container em PG 17/18:** o
  laço de `RENAME CONSTRAINT` da V5 só se manifesta em PG ≥17, e a suíte
  roda em 16 — o fix existe e nunca é exercitado. Pergunta correlata: o
  Tier 1 declara PG 14+, a bancada mede em 18, os testes rodam em 16.
- **O `EXPLAIN` do `OverviewLatencyScenario` sai por conexão diferente da
  medida**, com SQL escrito à mão em vez do SQL do store — a evidência
  não confirma qual plano o caminho medido usou. **Gatilho:** antes da
  próxima conclusão tirada desse cenário.
- **Cluster com mais de 64 nós tem nós que nunca reivindicam**
  (`Shards.ownedBy` devolve lista vazia para índice ≥ `SHARD_COUNT`).
  **Gatilho:** primeiro cluster planejado acima de 64 nós.
- **Nota datada no `ARCHITECTURE_REDESIGN_PLAN.md`:** a §7.2 ainda ensina
  o schema particionado e a Phase 8 ainda declara "retention by partition"
  como dependência da Phase 5. A ADR vence o plano por regra, mas o plano
  é o caminho natural de quem retoma a Phase 8.

---

## O que NÃO conta como pendência

- `docs/performance/BASELINE.md` continua dizendo "particionada" onde
  descreve o que foi medido. É o registro do que foi medido, na forma em
  que foi medido — invariante do CLAUDE.md. Linha nova no fim, nunca
  edição no corpo.
- O corpo histórico das ADRs 0050/0052: só o cabeçalho de revisão foi
  acrescentado, e é assim que deve ficar.
