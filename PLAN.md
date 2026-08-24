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

**Bancada nova:** dez cenários em `mohs-benchmark/src/test/java/io/mohs/store/jdbc/`,
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

**Chaos (E6) re-rodado no binário atual** — era lacuna, porque o `stop()`
mudou: S6 (kill −9), S8 (banco pausado 30s) e SUSPEND (nó congelado),
30k execuções cada, **zero perda nos três**; re-execução exatamente igual
ao que estava em voo no kill; zero dupla-conclusão no SUSPEND.

---

## Passos (um por commit; suíte verde e pipeline de DoD ao fim de cada um)

- [ ] **P1 — Normalizar as PKs da história. TEM PRAZO.**
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

- [ ] **P2 — Soak de 12–24h.** O drain mais longo desta bancada foi ~1
      min. Nada aqui roda por horas, e é o único jeito de aparecer o que
      só uptime longo mostra (vazamento lento, drift de relógio,
      crescimento de conexões, degradação de plano com a história
      crescendo). Um nó, carga modesta e contínua, com
      `write-amplification.ps1` amostrando. Critério: vazão e latência
      estáveis do início ao fim, heap sem tendência, zero execução
      perdida.

- [ ] **P3 — Os cenários em Tier 2 (SQL Server e MySQL).** Os dez
      `*Scenario` rodam **só em Postgres** — claim concorrente, reclaim de
      posse e fechamento de lote sob contenção real estão medidos apenas
      no dialeto de referência. A lacuna está registrada com gatilho na
      ADR-0050; o trabalho é generalizar o `ScenarioCluster`, hoje amarrado
      a `PostgresJdbcDialect` + `PostgresTestSupport`. Comece pelo
      `NodeChurnScenario` e pelo `BatchCompletionScenario` — são os que
      dependem de semântica de lock.

- [ ] **P4 — Fechar a suíte §20.2: faltam S3 e S4.**
      **S3** (burst de 0 → 100k em 10s; critério: idade do mais velho volta
      abaixo de 5s em 60s) e **S4** (10M triggers agendados, 1% devido por
      minuto; critério: lag de materialização < 2s). Nenhum dos dois
      jamais rodou.

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
  consequência direta da ADR-0054 — sem o tier NOTIFY, um enqueue só é
  imediato se quem recebeu for o dono do shard.
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
