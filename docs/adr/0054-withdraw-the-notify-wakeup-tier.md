# ADR-0054 — Retirada do tier de wakeup LISTEN/NOTIFY

Data: 2026-08-22 · Status: aceita · Fase: Phase 6 do redesign (revisa o tier 2 da ADR-G/§5.5 do plano; os tiers 1 e 3 permanecem)

## Contexto

O §5.5 do redesign definiu o wake-up em três camadas: (1) hand-off local
pós-commit na própria JVM, (2) `LISTEN/NOTIFY` para imediatismo cross-nó
no Postgres, (3) poll adaptativo como backstop de correção. O S6.3
implementou o tier 2 por completo — `pg_notify` na transação do offer,
listener em conexão dedicada fora do pool, filtro por shard, reconexão
com backoff — e ele FUNCIONAVA: o E2E contra Postgres real despachava um
enqueue de outra conexão em < 1s com o poll travado em 2s.

O custo apareceu onde ninguém tinha olhado, e foi o usuário quem sentiu
primeiro ("antes processava mais rápido"). Medido com A/B de binários na
mesma sessão (BASELINE "Phase 6 — S6.1–S6.3 A/B"):

- Transação notificante **não participa de group commit**: o Postgres
  segura um lock GLOBAL do notify queue (`PreCommit_Notify`) até o fim do
  flush de WAL do commit. Teto medido: ~500 commits notificantes/s na
  máquina de bench, contra ~7,6k não-notificantes (microbench pgbench do
  relatório db-tuner do S6.3 — INSERT com o shape de `mohs_ready`, 32
  conexões; não tabelado no BASELINE, que registra o fim a fim).
- Fim a fim, o ingest REST caiu de ~680 para ~345 req/s e a latência do
  POST de 7,7ms para **1,3s**. O drain (processamento) não mudou — a
  regressão era exclusivamente do caminho de enqueue.
- A mitigação P1 (conflação global por emissor, janela de 50ms, bitmask
  de shards pendentes carregada pelo vencedor) devolveu o baseline
  (14,8–15,9s / 9–18ms). O lock permanece, limitado a ~20 commits
  notificantes/s por nó.

## Decisão

**Retirar o tier NOTIFY inteiro** — emissão (`notifyReady`), listener
(`PostgresNotifyListener`), filtro de shard do engine (`ownsShard`/
bitmask) e o wiring do starter. Ficam o tier 1 (hand-off local, que cobre
o caso dominante com latência de ms) e o tier 3 (poll adaptativo,
25ms–2s), que passa a ser o único caminho cross-nó, com latência limitada
por `mohs.engine.max-poll-interval`.

O racional, na ordem que decidiu:

1. **Valor residual ≈ zero nos cenários que existem.** Pós-P1, o NOTIFY
   não melhorou nenhum número medido: drain igual, REST igual ao
   baseline. Seu único valor é dispatch cross-nó < `max-poll-interval`
   num cluster multi-nó OCIOSO — deployment que nenhum usuário tem hoje
   (YAGNI). Cluster com carga poll no piso de 25ms de qualquer forma.
2. **Custo permanente real.** Única feature que tocava o commit do
   enqueue; lock instância-wide compartilhado com QUALQUER outra
   aplicação no mesmo Postgres (o Mohs impunha custo a vizinhos);
   PG-only, quebrando a paridade de comportamento entre dialetos
   (ADR-0050); e quatro pendências operacionais próprias (half-open sem
   detecção, janela de conflação × tamanho do cluster, conexão dedicada
   fora do pool, driver `optional` no compile).
3. **A alternativa é boa o suficiente.** O poll adaptativo já entrega o
   que os concorrentes não têm: piso de 25ms sob carga (Quartz/JobRunr/
   db-scheduler pollam em segundos) e teto de 2s em idle, configurável
   para baixo por quem precisar de menos latência cross-nó.

## Consequências

- Latência de pickup cross-nó em cluster ocioso: até
  `max-poll-interval` (2s default). Documentada como característica, não
  bug.
- `org.postgresql` volta a `test` scope no `mohs-store-jdbc`; nenhuma
  classe de produção depende de tipos do driver.
- Reversível de verdade (a ADR-G já previa "reversibility: trivial"): a
  implementação completa, JÁ com a lição da conflação P1, vive nos
  commits 2fe9f08 e e41ce96. **Gatilho de retorno registrado no
  PLAN.md:** requisito real (usuário/SLA nomeado) de dispatch cross-nó
  menor que o teto do poll em cluster ocioso.
- As medições que condenaram o tier ficam no BASELINE ("Phase 6 —
  S6.1–S6.3 A/B") — a retirada é um resultado de engenharia, não um
  desvio: implementar → medir → remover o que não paga é exatamente o
  processo que o projeto contratou.
