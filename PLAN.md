# PLAN — Phase 6 do redesign: sharding + adaptive poll (ADR-F/G; tier NOTIFY retirado — ADR-0054)

Estado: **Phase 6 encerrada (S6.1–S6.5)** · Base:
`ARCHITECTURE_REDESIGN_PLAN.md` §5.4–5.6, §8.3–8.5, §11, §21 (Phase 6),
ADR-F, ADR-G · Pré-requisitos verdes: Phase 5 completa (97d781a→a6d9956),
E3 decidido (claim single-shard round-robin 2,21×/2,91×; 64 shards
escalam 345k→487k — BASELINE §E2/E3).

A Phase 5 encerrada vive no histórico do git (PLAN.md até a6d9956).

## Decisões de execução (desvios e escolhas, sujeitos a veto)

1. **Shard derivado do id, sem coluna nova em `mohs_lease`.** O plano §8.3
   define `shard = hash(execution_id) % 64`; como o shard é FUNÇÃO do id,
   requeue/retry/reaper o RE-DERIVAM (função compartilhada única) em vez
   de transportá-lo pela lease — resolve a pendência "shard=0 hardcoded"
   sem migração. `SHARD_COUNT = 64` fixo, não configurável (§8.3). Hash:
   o do próprio UUIDv7 string (`hashCode` normalizado) — determinístico
   entre JVMs é obrigatório, então NÃO é `String.hashCode` e sim um hash
   estável próprio (FNV-1a ou similar) fixado por teste de contrato.
2. **Handler-aware claiming e starvation floor (§5.6/§11.1) NÃO entram.**
   O cross-shard fallback é requisito de correção DE handler-aware
   claiming — que esta fase não introduz. Sem o filtro por handler, o
   dono do shard reivindica e falha como hoje (retry queimado no
   rolling-update heterogêneo — comportamento atual, nenhum strand novo).
   Ficam registrados como pendência com gatilho: primeiro rolling update
   real com handler removido, ou primeira reclamação de starvation por
   prioridade.
3. **`mohs_nodes` extras (`capacity`/`in_flight`/`version`/`shards`
   persistidos) NÃO entram.** A atribuição de shards é DERIVADA (ids
   vivos ordenados, §8.3) — não precisa persistir; os campos de dashboard
   são feature de `GET /nodes`, fase própria. `DRAINING`/`STOPPED` já
   existem e passam a excluir o nó da atribuição.
4. **Config do poll:** `mohs.engine.poll-interval` passa a ser o PISO do
   adaptativo (semântica preservada: é o intervalo sob carga) e nasce
   `mohs.engine.max-poll-interval` (default 2s) como teto do backoff
   (dobra a cada rodada vazia, reseta em trabalho). `max <= poll`
   desliga o adaptativo (comportamento atual). **Default do
   `poll-interval` muda de 5s → 25ms** (ADR-G). O trade HONESTO: idle
   sobe de 0,2 para 0,5 queries/s por nó (o backoff estaciona no teto de
   2s, não nos 5s de hoje) — desprezível em absoluto — em troca de um
   piso de latência de dispatch 200× melhor sob carga; quem se importar
   com o idle pina `poll-interval`/`max-poll-interval`. Mudança de
   default visível, por isso está aqui.
5. **O loop de tick vira platform thread própria** (§12.1: "claim loop —
   one platform thread per node"), substituindo o ThreadPoolTaskScheduler
   de intervalo fixo: espera em `Condition.await(nextDelay)` (adaptativo)
   e acordável por sinal (hand-off local). Tempo de espera por
   relógio MONOTÔNICO (invariante CLAUDE.md); o `Clock` injetado segue
   dono de todo "quando".
6. **NOTIFY Tier-1 — decisão original, RETIRADA na execução
   (ADR-0054).** A forma decidida aqui foi implementada e medida no
   S6.3: funcionava, mas o `pg_notify` serializa o commit do enqueue.
   Ver o passo S6.3 abaixo e a pendência com gatilho de retorno.
7. **Gate S2 na bancada local, com honestidade declarada:** 6×S1 literal
   (≥72k/s agregado) não cabe num único host. Medimos o que a bancada
   permite — agregado de 2–4 processos-nó vs 1 nó (escala relativa),
   idle query rate com N nós — e declaramos o resto como shape validado
   pelo E3 (micro, 64 shards, 8 claimers). Ritual de bancada antes das
   rodadas de registro (pendência da Phase 5): reboot do Docker + rounds
   de warmup descartados.

## Passos (um por commit; suíte verde e pipeline de DoD ao fim de cada um)

- [x] **S6.1 — Shard na escrita + claim em lap.** Função de shard
      compartilhada (`io.mohs.engine`, testada por contrato de
      estabilidade); enqueue/firer/retry/requeue/rearm gravam
      `hash(id)%64` (hoje 0); ownership derivada no tick (ids vivos
      ordenados; `DRAINING`/`STOPPED` fora); claim vira LAP round-robin —
      um shard próprio por statement, admission UMA vez por lap (§5.7),
      volta vazia encerra; `Admission`/requeue por lap. Backlog existente
      com shard=0 continua claimável (0 pertence a alguém em qualquer n).
      Testes: ownership (1 nó = todos; n nós = partição; membership
      change = sobreposição benigna), lap, distribuição do hash.
- [x] **S6.2 — Loop adaptativo + hand-off local.** Platform thread única
      no lugar do scheduler; backoff poll→max (dobra em lap vazio, reset
      em trabalho); `max-poll-interval` + validação de boot; hand-off
      pós-commit do enqueue local (tier 1 §5.5: `visible_at <= now`
      agendado NESTA JVM acorda o loop sem esperar poll). Heartbeat segue
      1×/tick — com backoff, tick esparso em idle: heartbeat precisa de
      cadência PRÓPRIA ≤ node-lease-ttl/3 (senão nó idle é declarado
      morto!) — este é o risco nº 1 da fase, teste dedicado.
      Executado: o cap de ttl/3 vale nas DUAS pontas (um piso maior que
      ele é engolido com WARN no start — liveness vence configuração);
      interrupt na thread do loop é engolido de propósito (JCIP 7.1.3 —
      a dona define a política; re-armar viraria busy-spin); o custo da
      manutenção por tick no piso de 25ms é medido no S6.4 (gate da
      fase), não reivindicado aqui.
- [x] **S6.3 — NOTIFY Tier-1: implementado, medido e RETIRADO
      (ADR-0054).** O tier funcionava (E2E cross-conexão < 1s com poll de
      2s), mas `pg_notify` na transação serializa commits notificantes
      (lock global do notify queue atravessa o flush — mata group commit)
      e derrubou o ingest REST de ~680 pra ~345 req/s (latência 7,7ms →
      1,3s) — a regressão foi PERCEBIDA em uso antes de medida. A
      mitigação P1 (conflação global por emissor, janela 50ms) devolveu o
      baseline, mas a retirada venceu no saldo: o valor residual do
      NOTIFY (dispatch cross-nó < max-poll num cluster multi-nó OCIOSO) é
      cenário que ninguém tem, e o custo permanente era superfície
      operacional PG-only (conexão dedicada, half-open, lock
      instância-wide compartilhado com outras aplicações, janela ×
      tamanho de cluster). Ficam tier 1 (hand-off local) + tier 3 (poll
      adaptativo). Números completos: BASELINE "Phase 6 — S6.1–S6.3 A/B";
      código com a lição da P1 vive no git (2fe9f08/e41ce96).
- [x] **S6.4 — Validação e registro: dois gates de quatro NÃO passaram, e
      isso está registrado** (BASELINE "Phase 6 — S6.4", ADR-0055/0056,
      §21). Bancada nova: `mohs-benchmark/scripts/cluster-scale.ps1`
      (`-Mode Idle|Latency|Drain`, sobe e derruba N nós sozinho).
      - **Escala relativa (ADR-F): passa, sublinear.** 1/2/4 nós =
        6,6k / 9,0k / 15,0k exec/s (1,00× / 1,37× / 2,29×), duas passadas
        palindrômicas concordando em ~5%. O teto medido não é o claim:
        CPU do host ≤ 44%, waits em `LWLock:WALWrite` + `IO:WalSync`. O
        6× do S2 continua lastreado só no E3 — nenhum nó desta bancada
        sai do mesmo host.
      - **E6: passa inteiro** no binário shardado (S6 re-execuções = as
        em voo no kill; SUSPEND 0 dupla-conclusão; S8 0 re-execução,
        1ª conclusão 259ms após unpause). O shard re-derivado sobrevive
        ao requeue — o risco da decisão 1.
      - **Ocioso: NÃO passa.** 96 consultas/s com 1 nó, 109/s com 4,
        contra gate de < 10/s. 96% é o lap: 64 statements por tick de
        CLUSTER, invariante em N. **Resolvido no S6.5, abaixo.**
      - **p50 dispatch < 5ms: NÃO passa** — 25ms com 1 nó, 461ms (p95
        1,65s) com 4 nós ociosos. Era o número do tier NOTIFY, retirado
        pela ADR-0054; o hand-off local só acorda o dono do shard (1/N).
- [x] **S6.5 — O gate ocioso: uma sonda no lugar do lap.** Enquanto a
      rodada de claim anterior voltou vazia, o tick pergunta UMA vez
      (`EXISTS` sobre os shards próprios) em vez de dar o lap de 64
      statements; achou trabalho, o lap roda no MESMO tick — economia,
      nunca latência. Flag confinada à thread do tick (JCIP 3.3); a
      sonda é `WorkQueue.hasVisibleWork`, leitura fora da transação de
      claim (1 round trip, não 3) e com o hint sem lock do dialeto no
      SQL Server, onde um SELECT simples tomaria shared locks na tabela
      mais quente — e quem bloquearia é a thread que carrega o heartbeat. **Ocioso 96 → 4,0 consultas/s por nó** (24×); o termo do
      claim virou 0,5/s por nó = 5/s em 10 nós, exatamente a conta da
      ADR-G. Latência de dispatch e vazão de drain inalteradas em A/B
      alternado com n=30 e células adjacentes (BASELINE "Phase 6 —
      S6.5"). O gate só arma com uma volta COMPLETA e vazia: folga de
      dispatch esgotada e orçamento de tempo estourado também devolvem
      zero, e armar neles pagaria uma sonda por tick no caminho quente.
      E6 re-rodado verde. A sonda tem teste nos
      QUATRO dialetos: o binding de `IN (:shards)` com dezenas de
      parâmetros é do driver, não do dialeto, e cada um paga o seu. A
      isolação explícita por transação de claim (o `SHOW` de 1/3 dos
      round trips) NÃO entrou junto — ver a pendência revisada abaixo.

## Pendências registradas (com gatilho) — herdadas e novas

- **Wakeup cross-nó (NOTIFY retirado — ADR-0054).** **Gatilho de
  retorno:** requisito real de latência de dispatch cross-nó menor que
  `max-poll-interval` num cluster OCIOSO (usuário/SLA nomeado, não
  hipótese). Ponto de partida: a forma P1-conflacionada em
  2fe9f08/e41ce96, com as pendências dela (half-open probe, janela ×
  tamanho do cluster) reabertas junto.
- **As contagens do `/overview` perderam o hint sem lock na Phase 5.**
  `JdbcHistoryStore#countActiveByState`/`countTerminalOutcomesSince`
  foram reescritas sobre `mohs_ready`/`mohs_lease`/`mohs_attempt` e não
  usam mais `JdbcDialect#lockFreeReadHint` — em SQL Server sem RCSI elas
  voltaram a tomar shared locks nas três tabelas quentes, contra o
  contrato declarado do endpoint ("monitoramento jamais disputa com o
  claim"). Achado do review do S6.5; perdido na reescrita da Phase 5,
  não introduzido aqui. O `GET /overview/stream` herda o mesmo.
  **Gatilho:** primeiro deployment em SQL Server sem RCSI com dashboard
  aberto e contenção observada, ou a fase que revisitar o `/overview`.
- **A sonda ocioso vira Seq Scan no plano GENÉRICO do Postgres quando há
  backlog não-visível.** Achado do db-tuner no S6.5, medido: o pgjdbc
  server-prepara a partir da 5ª execução e o Postgres migra pro plano
  genérico (`generic_plans=7, custom_plans=5` confirmado); sem histograma,
  `visible_at <= $N` recebe `DEFAULT_INEQ_SEL = 1/3` e o Seq Scan
  fast-start vence. Com 1M de entradas AINDA NÃO VISÍVEIS (retries em
  backoff, `at`/`delay` no futuro) e 64 shards: **12.049 buffers / 27,5ms
  por sonda**, contra **384 buffers / 0,4ms do lap inteiro** que ela
  substituiu — 31× mais buffers no estado que a bancada do S6.5 não
  mediu (ela mediu fila vazia, onde a sonda ganha 24×). Penhasco de plano
  entre 16 e 24 shards no `IN`: **≥4 nós ficam no índice; 1 ou 2 nós caem
  no Seq Scan** — o pior caso é o deployment mais simples. Nos defaults
  isso é 27,5ms a cada 2s = 1,4% de uma thread; vira sério só com
  `max-poll-interval == poll-interval` (54% do orçamento de cada tick com
  500k de backlog). **Não é o índice**: `(shard, visible_at)` foi medido e
  não muda o plano (o planner não escolhe Seq Scan por falta de índice, e
  sim por estimar 1/3), então nenhuma migração entrou. A raiz é o BIND de
  `:now`: com o instante como literal o plano volta a Index Only Scan
  (321 buffers / 0,074ms), ao custo de 0,29ms de planejamento por chamada.
  A variante com `ORDER BY shard, priority, visible_at LIMIT 1` — a
  hipótese de ancorar o índice sem literal — foi MEDIDA e não funciona: o
  Postgres descarta a ordenação dentro de `EXISTS`, e o plano sai
  idêntico (6.025 buffers, 13,2ms contra 13,4ms). **Gatilho:** primeiro
  deployment com `max-poll-interval == poll-interval` (onde são 54% do
  orçamento de cada tick, não 1,4% de uma thread) — e nesse caso o
  counter `mohs.claim.idle_probe` deixa de ser pendência e vira
  pré-requisito, porque é ele que detecta. O gatilho por
  `shared_blks_hit/calls` em `pg_stat_statements` fica como sinal
  secundário: sem métrica no Micrometer, ninguém está olhando pra lá.
- **Cluster com mais de 64 nós tem nós que nunca reivindicam.**
  `Shards.ownedBy` devolve lista VAZIA para índice ≥ `SHARD_COUNT`: esses
  nós heartbeatam, contam na atribuição (encolhendo a fatia dos demais) e
  nunca claimam. Pré-existente do S6.1. Formas: `WARN` no boot, ou
  `index % SHARD_COUNT` deliberado. **Gatilho:** primeiro cluster
  planejado acima de 64 nós — hoje a bancada não passa de 4.
- **A sonda do gate ocioso não aparece no Micrometer.** Nó com muitos
  SELECTs de claim e zero dispatch (o modo degradado logo abaixo) só é
  diagnosticável do lado do banco: `mohs.claim.latency`/
  `mohs.claim.batch.size` somem quando o gate arma e nada nasce no lugar.
  Forma: counter `mohs.claim.idle_probe{outcome=empty|work}` no padrão de
  counters pré-registrados que o `EngineMetrics` já usa. **Gatilho:**
  primeira investigação real de "o nó está vivo mas não processa".
- **A sonda do gate ocioso não aplica o filtro de inadmissíveis.** Ela
  pergunta "existe entrada visível nos meus shards?"; o lap reivindica
  com o filtro. Modo degradado consequente: se as ÚNICAS entradas
  visíveis forem de jobs inadmissíveis (rate limit sem saldo, janela
  fechada, handler ausente), a sonda responde `true` todo tick, o lap
  roda, reivindica 0 e o gate não desarma — custo = pré-S6.5 + 1
  statement por tick. Não é regressão (é exatamente o comportamento
  anterior) nem risco de correção. **Nota de 3 da manhã:** um nó com
  muitos SELECTs de claim e zero dispatch é ISTO, não fila suja.
  **Gatilho:** aparecer numa bancada, ou reclamação de custo ocioso num
  cluster com rate limit saturado.
- **O tick de manutenção são 7 statements, e agora ele É o custo ocioso.**
  Depois do S6.5 o claim ocioso custa 1 sonda por tick; o que sobra são
  heartbeat, `SELECT * FROM mohs_nodes`, duas varreduras de lease, duas
  de definições e o purge de nós mortos — ~3,5 consultas/s por nó, o que
  faz 10 nós ociosos extrapolarem para ~40/s contra o <10/s do gate
  literal do §21. Anterior à Phase 6 e fora do escopo dela. Formas na
  mesa: fundir as duas leituras de definições, e espaçar purge/reconcile
  por N ticks em vez de todo tick. **Gatilho:** requisito real de custo
  ocioso num cluster grande, ou banco cobrado por consulta.
- **`SHOW TRANSACTION ISOLATION LEVEL` por transação de claim.** O
  `setIsolationLevel(READ_COMMITTED)` explícito de `JdbcWorkQueue`
  (DBTUNE-4, existe pelo MySQL RR) faz o Spring ler a isolação corrente e
  o pgjdbc gastar um round trip — em Postgres e SQL Server, cujo default
  já é READ COMMITTED, é 1/3 dos statements do claim confirmando o
  óbvio. **Avaliado no S6.5 e NÃO feito**, de propósito: (a) com o lap
  ocioso morto, o que restava era ~1 round trip por tick ocioso e ~1 por
  sonda sob carga, contra um teto que é fsync de WAL — ganho não
  mensurável hoje; (b) trocar por um flag de dialeto perde a garantia
  atual, que vale contra a configuração do DataSource e não contra o
  default do banco (um `spring.datasource.hikari.transaction-isolation`
  diferente passaria a valer para o claim em silêncio). A forma que
  preservaria a garantia é ler a isolação real do DataSource UMA vez e só
  então decidir. **Gatilho:** medição que mostre o round trip pesando
  sob carga.
- **Handler-aware claiming + starvation floor/probe (§5.6/§11.1)** — fase
  própria. **Gatilho:** rolling update real com handler removido
  (retry queimado deixa de ser aceitável), ou starvation de prioridade
  observada (head de shard preso atrás de HIGH contínuo).
- **Partições semanais: create-ahead só no boot.** **Gatilho:** primeiro
  deploy com uptime esperado > 1 semana.
- **`cancelQueued` que fecha o lote não publica `BatchCompleted`** —
  dívida ADR-0043. **Gatilho:** registro formal na BATCH-ARCHITECTURE-REVIEW.
- **`MohsImpl.enqueueMembers` grava membro a membro.** **Gatilho:**
  medição com o harness de write amplification num lote ≥ 1k.
- **Churn de janela fechada no modo degradado do filtro** (>1000
  inadmissíveis). **Gatilho:** `mohs.claim.requeued{reason="window-closed"}`
  crescendo sustentado.
- **`hasLiveSchedulerOccurrence` escala com a história retida.**
  **Gatilho:** cura do upsert acima de dezenas de ms na retenção real.
- **`drainedBatchMembers`/aposentadoria em base grande.** **Gatilho:**
  `Mohs.remove` de job com milhões de execuções de lote passando de ~1s.
- **Cancel contra stray lease se perde no requeue** (best-effort,
  ADR-0034). **Gatilho:** primeira reclamação real de operador.
- **Bancada de bench não-controlada** (dispersão ~45% entre sessões).
  Mitigada nesta fase pelo ritual da decisão 7; bancada dedicada segue
  pendente. **Gatilho:** gate de vazão que o ritual não estabilizar.

## Gate da fase — veredito (S6.4 medido, S6.5 corrigido — 2026-08-23)

| Critério | Alvo | S6.4 | S6.5 | |
|---|---|---|---|---|
| Idle query rate | < 10/s num cluster de 10 nós | 96/s (1 nó) · 109/s (4) | **4,0/s por nó** · 16/s (4) · ~40/s extrapolado pra 10 | ⚠️ |
| p50 dispatch (ocioso) | < 5ms (era do tier NOTIFY) | 25ms (1 nó) · 461ms (4) | inalterado | ❌ |
| Escala relativa multi-processo | positiva, ~linear no que a bancada der | 1,37× (2 nós) · 2,29× (4); teto = fsync do WAL | inalterado | ✅ |
| Suíte + E6 | verdes | passam | passam | ✅ |

O ❌ que resta é consequência direta da ADR-0054: sem o tier NOTIFY, um
enqueue num cluster OCIOSO só é imediato se quem o recebeu for o dono do
shard (1/N) — os outros esperam o poll do dono, limitado por
`max-poll-interval`. Sob tráfego o backoff fica no piso e o efeito some.
O gatilho de retorno do NOTIFY está registrado acima.

Sobre o ⚠️ do ocioso: o alvo do §21 é por CLUSTER de 10 nós, e reler
como "por nó" depois de medir seria mover a trave — o mesmo S6.4 recusou
fazer isso com o gate de latência. O que é fato: o termo que ESTA fase
controla, o do claim, bateu na mosca (0,5/s por nó = os ~5/s em 10 nós da
conta da ADR-G), e os ~35/s que faltam pro alvo são o tick de manutenção,
anterior à fase e com pendência própria. Por nó o número está em 4,0/s.
