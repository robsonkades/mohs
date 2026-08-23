# PLAN — Phase 6 do redesign: sharding + adaptive poll (ADR-F/G; tier NOTIFY retirado — ADR-0054)

Estado: **em execução (S6.1–S6.3 concluídos)** · Base:
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
- [ ] **S6.4 — Validação e registro.** Bancada ritual (decisão 7):
      idle query rate 1 nó e 4 nós (gate: < 10/s com o backoff no teto);
      agregado 2–4 nós vs 1 (escala relativa; S1 de referência re-medido
      na mesma sessão); E6 re-rodado (S6/SUSPEND — a atribuição de shards
      muda o reaper? não — reaper é por liveness, mas o requeue
      re-derivado precisa do chaos verde); BASELINE "Phase 6";
      ADR-0055 (F, sharding) e ADR-0056 (G, adaptive poll — sem o tier
      NOTIFY, retirado pela ADR-0054); §21 com resultado.

## Pendências registradas (com gatilho) — herdadas e novas

- **Wakeup cross-nó (NOTIFY retirado — ADR-0054).** **Gatilho de
  retorno:** requisito real de latência de dispatch cross-nó menor que
  `max-poll-interval` num cluster OCIOSO (usuário/SLA nomeado, não
  hipótese). Ponto de partida: a forma P1-conflacionada em
  2fe9f08/e41ce96, com as pendências dela (half-open probe, janela ×
  tamanho do cluster) reabertas junto.
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

## Gate da fase (do plano, com a leitura da decisão 7)

Idle query rate < 10/s por cluster ocioso · p50 dispatch < 5ms (Tier-1,
NOTIFY) · escala relativa multi-processo positiva e próxima de linear no
que a bancada permitir (shape 6× fica lastreado no E3) · suíte + E6
verdes.
