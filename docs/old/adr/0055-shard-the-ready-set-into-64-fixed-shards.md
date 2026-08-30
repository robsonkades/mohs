# ADR-0055 — 64 shards fixos na fila e o claim em lap

Data: 2026-08-23 · Status: aceita · Fase: Phase 6 do redesign (ADR-F do plano; commits a32b8a6 → S6.4)

## Contexto

Depois da Phase 5, o engine não é mais commit-bound: o teto passou a ser
o tick serial (§1.3 do plano). O claim era UM statement por rodada sobre
`mohs_ready` inteira, e o E3 (BASELINE §E2/E3) mediu onde isso quebra:
com `SKIP LOCKED` e claimers concorrentes, a fila vira um convoy — cada
claimer varre e pula as linhas que os outros travaram, e o custo do skip
cresce com o número de nós. Com 64 shards e 8 claimers o micro passou de
345k para 487k linhas/s, e a curva deixou de achatar.

O que o E3 NÃO respondeu, e o S6.4 respondeu: o que essa forma custa num
cluster de verdade, ocioso e sob carga.

## Decisão

`shard = hash(execution_id) % 64`, com `SHARD_COUNT = 64` fixo (não
configurável), e três consequências que são a decisão inteira:

1. **O shard é FUNÇÃO do id, não um dado transportado.** Enqueue, retry,
   requeue, rearm e reaper RE-DERIVAM o shard pela mesma função
   (`io.mohs.engine.Shards`, FNV-1a com literais pinados por teste de
   contrato — determinismo entre JVMs é obrigatório, e `String.hashCode`
   não serve como contrato de persistência). Não há coluna de shard na
   lease e não houve migração para introduzir o conceito.
2. **A posse é DERIVADA a cada tick** dos ids de nós vivos ordenados
   (`DRAINING`/`STOPPED` fora) — nada persistido, nada negociado. Troca
   de membership produz sobreposição breve e benigna, que degrada para o
   comportamento pré-shard e se cura em um heartbeat.
3. **O claim vira um LAP round-robin**: um shard próprio por statement,
   `Admission` avaliada UMA vez por lap (§5.7), lap vazio encerra a
   rodada.

Alternativas descartadas: claim-per-runner e core-per-job
(`CLAIM-GRANULARITY.md` B e C — prioridade deixa de ser global entre
runners, que é o que o operador percebe); não fazer nada (o convoy do E3).

## Consequências

**O que foi medido** (BASELINE "Phase 6 — S6.4"):

- **Escala relativa positiva**: 1 → 2 → 4 processos-nó dão 6,6k → 9,0k →
  15,0k exec/s (1,00× / 1,37× / 2,29×), com duas passadas palindrômicas
  concordando dentro de ~5%. Sublinear, e a atribuição diz por quê: CPU
  do host ≤ 44%, waits do Postgres em `LWLock:WALWrite` + `IO:WalSync`
  na janela inteira. **O teto agora é o fsync do WAL de uma instância
  única, não o claim** — que é exatamente o resultado que a ADR-F
  perseguia. A escala 6× do gate S2 continua lastreada no E3, não nesta
  bancada (todos os nós no mesmo host, mesma instância).
- **O chaos sobrevive ao shard re-derivado**: E6 inteiro verde no
  binário shardado (S6, SUSPEND com 0 dupla-conclusão, S8 com 0
  re-execuções). Nenhuma linha apodreceu numa partição que ninguém sonda
  — o risco nº 1 da decisão de re-derivar.
- **O lap custava caro no ocioso, e o custo era do lap, não do
  sharding**: um cluster ocioso gastava 64 statements de claim por tick de
  cluster, independentemente de N (1.856 sondas em 29 ticks com 1 nó;
  1.909 em 120 ticks com 4) — ~96 consultas/s com um único nó parado,
  contra as ~5/s que a ADR-G projetava para 10. **Corrigido no S6.5 sem
  tocar na decisão acima**: enquanto a rodada anterior voltou vazia, o
  tick pergunta uma vez `EXISTS` sobre os shards próprios em vez de dar o
  lap; achou trabalho, o lap roda no mesmo tick. Contenção é fenômeno de
  carga — o lap existe para espalhá-la, e não há o que espalhar quando a
  resposta é "nada". Ocioso 96 → **4,0 consultas/s por nó**; latência de
  dispatch e vazão de drain inalteradas em A/B alternado (BASELINE
  "Phase 6 — S6.5"). **A troca não é favorável em todo estado**: com
  backlog grande de entradas AINDA NÃO VISÍVEIS, o plano genérico do
  Postgres (pgjdbc server-prepara na 5ª execução) estima 1/3 e escolhe
  Seq Scan — 12.049 buffers contra os 384 do lap, com 1M de linhas e 64
  shards. Nos defaults são 27,5ms a cada 2s, 1,4% de uma thread; a conta
  completa, o penhasco de plano em 16→24 shards e por que nenhum índice
  resolve estão na BASELINE "Phase 6 — S6.5" e na pendência do PLAN.md.

**O que pagamos, por contrato:**

- Prioridade é estritamente global só DENTRO de um shard; entre shards, o
  desvio é limitado por um intervalo de poll.
- Claim sem filtro por handler continua reivindicando o que este nó não
  sabe executar (handler-aware claiming não entra nesta fase — pendência
  com gatilho no PLAN.md).
- **Reversível apenas com a fila drenada.** A frase fácil — "`SHARD_COUNT
  = 1` desliga o lap" — é falsa e perigosa: o shard é FUNÇÃO do id
  (`floorMod(hash, SHARD_COUNT)`) e a posse itera `shard < SHARD_COUNT`,
  então toda entrada já persistida em `mohs_ready` com `shard != 0`
  deixaria de ser sondada por qualquer nó e apodreceria — o risco nº 1
  que a validação de faixa do `ReadyEntry` existe para impedir. Mudar
  `SHARD_COUNT` é re-particionar o backlog persistido: mesma classe de
  mudança que trocar a função de hash, e exige recompilar (é
  `public static final`, não propriedade).
- **Teto de nós úteis = `SHARD_COUNT`.** Com mais de 64 nós elegíveis, os
  de índice ≥ 64 recebem lista de shards VAZIA: heartbeatam, contam na
  atribuição (reduzindo a fatia dos demais) e nunca reivindicam. Não
  documentado no S6.1 e não corrigido aqui — pendência com gatilho no
  PLAN.md.

## Nota de custo colateral (não do sharding, revelado por ele)

Cada sonda são **três** round trips: `BEGIN`, `SHOW TRANSACTION ISOLATION
LEVEL` e a CTE do claim. O `SHOW` vem do
`setIsolationLevel(READ_COMMITTED)` explícito em `JdbcWorkQueue`
(DBTUNE-4, existe por causa do MySQL, que é REPEATABLE READ por default):
quando a `TransactionDefinition` pede isolação explícita, o Spring lê a
isolação corrente e o pgjdbc gasta um round trip. Em Postgres, cujo
default já é READ COMMITTED, é um terço do tráfego de claim gasto
confirmando o que já valia. Existia antes da Phase 6 e passou
despercebido porque era 1 statement por tick; o lap multiplicou por 64.
Registrado no PLAN.md como pendência com gatilho — não corrigido aqui.
