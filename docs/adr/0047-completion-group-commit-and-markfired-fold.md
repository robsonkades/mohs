# ADR-0047 — Group commit da conclusão, fusão do `markFired` e leitura de payload em lote

Data: 2026-08-22 · Status: aceita · Fase: Phase 3 do `ARCHITECTURE_REDESIGN_PLAN.md` (⭐ ship first)

> **Emenda (2026-08-22, Phase 5):** esta É a ADR-C do plano. Com o split
> (ADR-0052), o mecanismo foi re-hospedado sem mudança de semântica: o
> `CompletionBatcher` flusha por `LeaseStore.complete` — a transação do
> §7.5-3 (DELETE cercado da posse + attempt + UPDATE advisory + retry na
> fila + contagem de lote + rearme) — e o veredito por resultado virou
> `Completion(owned, closedBatch)` com o fence `(node_id, epoch)`.
> `completeAll`/`CompletionRequest` morreram com a tabela única (S5.4).
> Caminhos frios (watchdog, pré-dispatch) concluem SÍNCRONOS fora do
> batcher — o contrato de erro do chamador exige exceção síncrona
> (review S5.3).

## Contexto

A Phase 0 do redesign mediu o custo de escrita por execução do motor atual
(BASELINE "Write amplification por execução"): **~3,9 commits engine-side**
por execução — 2,0 commits síncronos de escrita (`markFired` em autocommit +
a transação de conclusão) e ~1,9 round trips autocommit de leitura, dos
quais a maior fatia era o `findPayload` POR EXECUÇÃO no dispatch. O perfil
de waits do Postgres põe `LWLock:WALWrite` no topo (BASELINE "Tuning fim a
fim"): a latência de commit domina o custo por execução. O E1 (BASELINE
"E1") confirmou o group commit como a alavanca dominante de vazão: o braço
split-sync (sem group commit) ficou em 1,46× enquanto o split-group fez
25,8× no drain — e o §7.6 do plano desenha o mecanismo para o schema novo.
Esta fase o antecipa para o schema ATUAL, porque nada nele depende do split.

## Decisão

Três mudanças no caminho quente, uma propriedade nova:

1. **`fired_at` nasce no CAS do claim** (`transitionToRunning`, todos os
   dialetos). O `UPDATE` autocommit próprio do `markFired` desaparece;
   `ExecutionStore.markFired` foi removido. O instante "início do attempt"
   (evento `Started`, `JobContext.firedAt`, métrica de latência de
   dispatch) continua sendo lido no dispatch — o que muda é a COLUNA, que
   passa a registrar o claim (dezenas de ms antes do dispatch sob carga).

2. **Leitura de payload em lote**: `ExecutionStore.findPayloads(ids)` —
   uma consulta por round de claim, não uma por execução. O veredito é por
   linha: payload que não desserializa segue terminal por natureza
   (`failUnreadablePayload`, semântica preservada); **falha da consulta é
   infra** — o lote fica `RUNNING` e o reaper o devolve na expiração da
   lease. Consequência deliberada: o achado do S8 (BASELINE "S6/S8") no
   braço transiente é corrigido por construção — um soluço de banco na
   carga de payload nunca mais vira falha TERMINAL imediata. Precisão: o
   caminho de recuperação é o reaper, e o reclaim consome um attempt do
   orçamento como qualquer zumbi (ADR-0033) — job com `retries = 0` ainda
   termina FAILED por esse caminho, só que com o backoff da lease em vez
   de na hora. `findPayload` unitário permanece na porta para chamadores
   avulsos.

3. **`CompletionBatcher` (group commit da conclusão)**: resultados de
   dispatch entram numa fila limitada (4× o lote) e descarregam numa única
   transação de `completeAll` — 256 resultados ou 5 ms desde o primeiro
   pendente, o que vier primeiro. `completeAll` passou a devolver o
   veredito (`Completion`) por request: métrica, eventos (`Succeeded`/
   `Failed`/...) e a eleição do fechador de lote (`BatchCompleted`,
   ADR-0043) saem DEPOIS do commit do lote, na thread do flusher — a
   garantia "publica só o que ficou durável" atravessa o batcher. Fila
   cheia bloqueia o `submit` na thread do handler: o dispatch segue em
   voo e o claim enxerga a folga menor (ADR-0039) — backpressure
   estrutural, no espírito do §3.2 do plano. Falha do flush recai em
   conclusão individual por resultado; falha individual deixa a execução
   `RUNNING` para o reaper (ADR-0031: sem back-off interno). No shutdown,
   o `SmartLifecycle` para o engine antes da destruição de beans e o
   `close()` drena a fila; um zumbi que termina depois conclui síncrono
   pelo caminho antigo.

   **Knob único** (§7.6: "the only configuration knob added"):
   `mohs.engine.completion-flush-on-every-result=true` desliga o batcher e
   volta ao commit síncrono por resultado. N=256/T=5 ms são constantes de
   decisão, não configuração.

4. **Vagas de concorrência devolvidas em bloco**
   (`JobStore.decrementRunningExecutions(key, permits)`): a primeira
   bancada da fase entregou os commits (0,05/exec) mas não a vazão — o
   flush devolvia as vagas UMA POR EXECUÇÃO, ~256 round trips sequenciais
   na thread única do flusher (teto aritmético ~3-5k conclusões/s). O
   bloco usa `CASE` com piso em zero, não `GREATEST` (SQL Server 2019,
   Tier 2, não tem); N decrementos guardados e o bloco terminam no mesmo
   estado. É a §1.2 do plano de novo: o custo escondido era o contador.

## Custo semântico, declarado

- A janela entre "handler terminou" e "resultado durável" cresce de ~1 ms
  para ≤ 5 ms; um crash nessa janela re-executa até 256 resultados além dos
  em voo. O contrato já era at-least-once — muda a exposição a duplicata,
  não a garantia (medição E5 na BASELINE).
- `fired_at` (coluna) passa a significar "reivindicada", não "despachada".
  `Attempt.startedAt` continua sendo o início real do attempt — a janela de
  medição de vazão do BASELINE (attempts) não muda de semântica.
- Eventos de conclusão chegam até ~5 ms depois e na thread do flusher (já
  eram assíncronos via executor de eventos).
- Perda ACEITA: se o `completeAll` commitar e a exceção escapar mesmo
  assim (desfecho de commit desconhecido, DDIA cap. 8), o fallback
  individual perde o CAS e recebe `NOT_APPLIED` — estado e contadores de
  lote corretos e sem duplicata, mas o evento (`Succeeded`/
  `BatchCompleted`) dessa janela não sai. É a mesma classe de perda do
  crash-pós-commit que o sistema já aceita (eventos sem outbox); a fonte
  de verdade do lote é o `GET /batches`, o evento é best-effort.

## Alternativas consideradas

- **Group commit também do claim** — já existe (batch de até `batch-size`
  por transação desde sempre); nada a fazer.
- **`unnest()` multi-row no flush (Tier 1)** — o §7.6 o descreve para o
  schema novo; aqui o `completeAll` (DBTUNE-14) já amortiza o commit e faz
  batch JDBC por statement. Fica como alavanca seguinte SE a medição
  mostrar o flush dominado por round trips, com número (postura BASELINE).
- **`synchronous_commit=off` por sessão** — rejeitada de novo (janela de
  perda silenciosa; o group commit entrega o grosso do ganho sem mudar a
  durabilidade).
- **Batcher por runner/job** — YAGNI; um batcher por node é o desenho do
  §7.6 e nada mediu contenção na fila única.

## Consequências

- Commits síncronos de escrita por execução: 2 → ~1/256 (+1 do enqueue,
  por construção, fora do motor). Leituras autocommit: ~1,9 → ~amortizadas
  por lote. Números medidos na BASELINE (seção "Phase 3").
- O wart "o `completeAll` conta o lote e descarta o veredito" (item 2 do
  BATCH-ARCHITECTURE-REVIEW, metade dele) morre: o caminho batched publica
  `BatchCompleted` corretamente. O reclaim do reaper segue sem publicar
  (fora de escopo, como antes).
- O harness E1 (`TableSplitExperimentHarness`) preserva o caminho antigo
  por SQL inline no braço `current` — ele registra o motor DE ANTES desta
  ADR, de propósito.

## Medições (gate da fase)

Detalhe integral na BASELINE, "Phase 3 — group commit + fusões, medido"
(A/B pela própria propriedade, mesma base com ~1M de história — o dobro
da Phase 0):

- **commits/execução: 3,9 → 0,037-0,054** engine-side (gate ≤ 1,5 passa
  por 30×). O control (só as fusões) fica em ~2,0 — 1 transação de
  conclusão + a cerimônia `SHOW TRANSACTION ISOLATION LEVEL` do pgjdbc.
- **Vazão: 3,0-3,3k (Phase 0) → mediana ~5,7k = 1,7-1,9×** (gate 1,8× na
  linha, com a história dobrada jogando contra). Atribuição dividida:
  fusões ~1,45×; group commit +1,05-1,36×. O motor deixou de ser
  commit-bound — o teto novo é o tick serial (§1.3 do plano, assunto da
  Phase 5/6).
- **E5: zero exposição de duplicata além do em-voo** — S6 re-rodado com o
  batcher: 314 em voo no kill → exatamente 314 re-execuções, 0 violações,
  50k/50k; a fila do batcher ainda é RUNNING no banco, ou seja, já
  pertence ao conjunto que o contrato aceita re-executar.
- Rollback validado por construção: o control É o rollback (uma
  propriedade), medido no mesmo dia.
