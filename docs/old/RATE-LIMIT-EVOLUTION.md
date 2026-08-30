# Evolução do rate limit — o que medir antes de mexer

Companheiro da ADR-0042 (a decisão). Este documento é a lista de melhorias
**deliberadamente adiadas**, cada uma com o gatilho que a torna necessária e
como validá-la. Existe para que a próxima pessoa (ou o próximo eu) não
redecida do zero nem otimize sem número.

Regra da casa que vale aqui inteira: **sem before/after medido, não é
otimização**. Os números abaixo marcados como MEDIDO vieram de pgbench sobre
Postgres 18.4 em container local (2026-08-18), exercitando a linha do balde
isolada; os marcados como ESTIMADO não valem como evidência.

## Linha de base do mecanismo (MEDIDO)

| Cenário | tps | latência média |
|---|---|---|
| Ciclo do balde como transação própria, 1 cliente | 457 | 2,19 ms |
| idem, 64 clientes | 398 | 161 ms |
| idem, `synchronous_commit=off`, 1 cliente | 5.198 | 0,19 ms |
| idem, `synchronous_commit=off`, 16 clientes | 7.370 | 2,17 ms |
| Transação de 10 ms SEM balde | 97,4 | 10,26 ms |
| Transação de 10 ms COM balde | 79,9 | 12,51 ms |

Três conclusões que não precisam ser redescobertas:

1. **A linha não é o gargalo.** Um cliente sozinho já satura em 457 tps e 64
   clientes não pioram a vazão — o teto era fsync do commit, não disputa de
   lock. Com commit assíncrono a MESMA linha sustenta 7.370 tps. Os planos
   são index scan na PK: 0,049 ms (SELECT ... FOR UPDATE) e 0,044 ms (UPDATE).
2. **Bloat não acontece.** 116.715 updates na linha do balde deram **99,5% de
   HOT updates**, 1 tupla morta e 256 kB de tabela. As colunas mutáveis
   (`tokens`, `refilled_at`) não entram em índice nenhum, que é exatamente a
   condição do HOT — o modo de falha catalogado na ADR-0009 não se
   materializa neste desenho. Se alguém indexar `tokens` "para consultar
   rápido", isso quebra: não faça.
3. **O custo marginal é 2,24 ms por rodada** (12,51 − 10,26), quase tudo
   round trip de rede, não trabalho de banco.

## CORREÇÃO (2026-08-18, medição posterior): o teto é ~33 rodadas/s, não ~80

A seção seguinte foi escrita contra uma transação de claim **modelada** em
10 ms. Uma medição posterior, com a rodada REAL a `batch=1000` (seleção de
candidatos com `FOR UPDATE SKIP LOCKED` + escrita nas 1000 linhas + commit,
sobre 1.005.000 execuções e 300 definições no Postgres 18.4), mostrou que a
rodada custa **~30 ms** — e o balde, consumido no INÍCIO dela, segura o lock
por ~95% desse tempo. Baseline não se sobrescreve: os números abaixo ficam
como estão, e esta é a leitura correta.

| clientes | sem balde | balde no início (código atual) | balde no fim (melhoria 1) |
|---|---|---|---|
| 1 | 34,0 tps / 29,4 ms | 32,7 / 30,6 ms | 31,8 / 31,5 ms |
| 2 | 47,9 / 41,8 ms | **32,6 / 61,4 ms** | 43,5 / 46,0 ms |
| 4 | 71,4 / 56,0 ms | **33,7 / 118,8 ms** | 76,1 / 52,5 ms |
| 8 | 88,6 / 90,3 ms | **25,9 / 309,0 ms** | 89,4 / 89,5 ms |

Consequências que mudam a decisão de operação:

- A vazão **congela em ~33 rodadas/s a partir de 2 clientes** e PIORA em 8
  (25,9 — a fila come mais do que entrega). Como cada nó faz 20 rodadas/s a
  `poll=50ms`, a folga acaba **entre 1 e 2 nós**, não em 4. A tabela de nós
  da seção seguinte está otimista por ~2,4×.
- A latência da rodada chega a 309 ms com 8 clientes — 6× o `poll-interval`.
  O tick que carrega heartbeat e renovação de lease atrasa junto: é o mesmo
  modo de falha que `BUCKET_LOCK_TIMEOUT` evita, só que por acúmulo em vez
  de travamento.
- Agravante concreto: o nó que espera pelo balde **já segura `FOR UPDATE` em
  até 1000 linhas de `mohs_executions`**. Durante toda a espera, essas
  execuções ficam invisíveis (`SKIP LOCKED`) para o cluster inteiro —
  inclusive as de jobs sem limite nenhum.
- A melhoria 1 (consumo no fim) foi medida no mesmo experimento: **2,3× com
  4 clientes e 3,5× com 8**, latência de 119 → 53 ms e de 309 → 89 ms,
  praticamente empatando com "sem balde". A estimativa de "~450 rodadas/s"
  daquela seção também estava errada: com `batch=1000` o teto vira a própria
  rodada, ~90/s. Continua sendo a diferença entre 1 nó e 4+.

Outras confirmações da mesma rodada de medição: adicionar `j.rate_limit` ao
SELECT de candidatos **não muda plano, custo nem buffers** (a tupla de
`mohs_job_definitions` já é lida no heap; `shared hit=3116` idêntico), e o
HOT update se mantém em **99,84%–99,99%** com a linha convivendo com outros
200 baldes e 8 clientes concorrentes. Nenhum índice novo se justificou.

## O teto real e quando ele chega

O que limita é a **serialização**: enquanto a transação de claim segura o
lock, as rodadas dos outros nós que tocam o MESMO limite esperam. Com
transação de 10 ms o teto medido é **~80 rodadas/s por limite**, plano em 4 e
em 16 clientes (só a latência cresce: 50 ms → 203 ms).

Cada nó faz `1 / poll-interval` rodadas por segundo — 20/s no ponto de
operação atual (`poll=50ms`).

| Nós | Rodadas/s desejadas | Situação |
|---|---|---|
| 1–2 | 20–40 | folga |
| 4 | 80 | no teto |
| 8 | 160 | fila: latência de claim dobra |

**Gatilho para agir:** cluster passando de ~4 nós COM um limite quente, ou
p99 de latência de claim subindo junto com a adoção de rate limits. Medir
antes: latência da rodada com um limite quente no ponto de operação de 4k/s.

Detalhe que agrava e precisa estar no radar: a fila atinge **jobs sem limite
nenhum**, porque o lock mora dentro da transação de claim compartilhada.

## Melhorias adiadas, em ordem de preferência

### 1. ~~Mover o consumo para o fim da transação~~ — FEITO em 2026-08-18

Implementado e medido (ver "Revisão 2026-08-18" na ADR-0042): escala linear
até 8 nós, 78k exec/s sem throttling, e sob throttling a vazão entregue bate
o orçamento exato do balde. O preço previsto aqui — perder a rodada quando o
CAS falha — se materializou em 48,8% das rodadas a 4 nós no regime
throttlado, e foi aceito: cada descarte custa um SELECT de candidatos, não
trabalho entregue. A estimativa de "~450 rodadas/s" desta seção era do proxy
SQL; o número real está na ADR.

### 1b. (histórico) O raciocínio original

Hoje o balde é consumido como segunda guarda, antes do mutex de job e do CAS
— o lock fica preso pelo resto da transação. Consumir por último, com
`UPDATE ... SET tokens = tokens - :n WHERE name = :x AND tokens >= :n`,
encurta a janela de lock para a cauda da transação: o teto sai de ~80 para a
faixa dos ~450 rodadas/s MEDIDOS na seção de linha de base.

- **Custo:** a rodada inteira é perdida quando o UPDATE condicional falha
  (outro nó consumiu no meio). Sob contenção alta isso vira trabalho jogado
  fora, e o desenho pessimista atual pode ficar melhor — é exatamente o
  trade-off que precisa de medição, não de intuição.
- **Como validar:** duas rodadas de bench e2e com um limite quente, medindo
  vazão E taxa de rodadas descartadas. Se o descarte passar de ~5%, não
  compensa.

### 1c. Rever o custo das rodadas descartadas sob throttling (ADIADO, com gatilho)

Medido em 2026-08-18 no caminho real (`RateLimitContentionHarness`, cenário
B): sob throttling, **33% das rodadas a 4 nós e 44% a 8 voltam vazias**. A
vazão entregue não sofre — fica cravada no orçamento do balde (911–913/s em
todos os níveis) —, mas cada rodada vazia gastou um `SELECT` de até
`batchSize` candidatos com `FOR UPDATE SKIP LOCKED` que não produziu nada.

**O que a métrica NÃO diz:** "rodada vazia" é teto, não descarte exato —
conflacia a rodada desfeita pelo CAS com a rodada em que o balde já estava
vazio e ninguém foi admitido (essa segunda é barata: a fase 1 não admite
nada, não há CAS, não há rollback). Antes de otimizar, **separar as duas** é
o primeiro trabalho, e ele custa instrumentar a produção — hoje o claimer
devolve lista vazia nos dois casos, de propósito.

**Gatilho para agir:** CPU ou I/O do banco subindo com a adoção de rate
limits sem aumento de vazão entregue; ou p99 de latência de claim degradando
para jobs SEM limite (o rollback é da rodada, não do candidato — ver
"Interação conhecida" abaixo).

**Saídas conhecidas, em ordem de preferência:**

1. **Limitar o `SELECT` de candidatos pelo saldo da fase 1**: se o balde só
   concede 40 tokens, selecionar 1000 candidatos é desperdício conhecido. O
   `batchSize` efetivo viraria `min(batchSize, saldo)` quando TODOS os
   candidatos elegíveis forem de jobs limitados — o caso difícil é o lote
   misto, e é por isso que não foi feito agora.
2. **Backoff proporcional ao saldo**: quando `available == 0`, pular a rodada
   inteira sem nem selecionar candidatos daquele limite.
3. Nada. Se a medição mostrar que o custo é ruído contra o `poll` de 50ms,
   esta seção vira histórico.

**Como validar:** cenário B do harness, comparando `rodadas`, `vazias%` e —
o critério de aceite que não pode mudar — a vazão entregue continuar batendo
o orçamento exato do balde.

### 2. Cota por node, estilo Temporal (escala, ao custo de precisão)

Dividir a taxa global pelo número de nós vivos (o registro de heartbeat da
ADR-0041 já dá o divisor) e limitar localmente, sem coordenação no caminho
quente. É como o `MaxTaskQueueActivitiesPerSecond` do Temporal resolve.

- **Custo:** deixa de ser exato. Nós desbalanceados (um ocioso, outro
  saturado) sub-entregam: o ocioso não usa a cota dele e ninguém aproveita.
  Além disso o divisor muda quando um nó morre, e a janela entre a morte e o
  purge é de sub-entrega.
- **Quando:** só se (1) não bastar. É a saída para cluster grande, não para
  cluster médio.

### 3. Sharding do balde (N linhas por limite)

Quebrar `smtp` em `smtp#0..smtp#7`, cada nó escolhendo um shard por hash. A
contenção cai por N, a exatidão se mantém no agregado, e o custo é que um
shard pode esvaziar enquanto outro sobra.

- **Quando:** se (1) resolver a latência mas o teto ainda incomodar, e a
  aproximação de (2) for inaceitável.
- **Sinal de alerta:** isto multiplica a complexidade de leitura do
  `GET /rate-limits` (somar shards) e do PATCH (redistribuir).

## Comportamentos conhecidos e aceitos (não são bugs)

Cada um destes foi decidido na ADR-0042; mudar exige nova decisão, não
"correção":

- **Token queimado.** Token concedido a candidato que depois perde o mutex do
  job ou o CAS não volta. Sub-entrega temporária, curada pelo refill. Trocar
  isso por devolução reintroduziria o drift que matou o desenho da ADR-0009.
- **Burst de até `max`.** Balde cheio admite `max` disparos instantâneos. É o
  teto de rajada, e é melhor que os ~2× de borda da janela fixa.
- **Reclaim e retry consomem de novo.** O medidor conta tentativa de
  disparo, porque é isso que o recurso externo enxerga.
- **Head-of-line.** Candidatos represados seguem no topo da ordem
  (prioridade, `scheduled_at`) e são re-selecionados a cada rodada do mesmo
  tick, gastando seleção sem produzir claim. A saída, se medir que importa, é
  excluir o limite exausto do predicado das rodadas seguintes daquele tick
  (ADR-0040) — não um índice novo.
- **Um limite por job, sem composição.** Nem job com dois limites, nem limite
  herdado do runner. Só generalizar com três casos de uso reais.

### Interação conhecida: teto de concorrência amplifica a queima (não corrigida)

Um job com `maxConcurrentExecutions = N` e backlog coloca **todas** as suas
execuções devidas no lote — o predicado de candidatos filtra por job elegível,
não por vaga livre. `admitByRate` cobra um token por candidato admitido, mas só
`N` vencem `tryIncrementRunningExecutions` logo adiante: o resto dos tokens
queima numa rodada só. Com `batch=1000` e um job de `cap=1`, isso é sistemático,
não incidental — e, como o lote vem ordenado por prioridade/`scheduled_at`, um
job represado desse tipo pode consumir o grant inteiro e **estrangular outro
job que divide o mesmo limite**.

Correção conhecida: expor a folga do job (`max_concurrent_executions -
running_execution_count`) no `Candidate` — o `SELECT` já faz o join com
`mohs_job_definitions` — e cobrar do balde só o que tem vaga. Custa uma coluna
nos 4 templates de dialeto.

**Gatilho:** primeiro job real que combine rate limit com
`maxConcurrentExecutions`/`preventOverlap`. Enquanto todo job limitado for de
concorrência livre, o cenário não existe. **Como validar:** job com `cap=1`,
5 enfileiradas e limite de 5 deve deixar 4 tokens no balde — hoje deixa 0.

## O que NÃO fazer

- Indexar `tokens` ou `refilled_at` — mata o HOT update, que é o que mantém o
  bloat em zero (item 2 da linha de base).
- Resetar o balde no upsert de boot. Cada nó subindo num rolling deploy
  devolveria um balde cheio e o deploy viraria burst.
- Consultar o saldo com escrita (refill persistido na leitura): transformaria
  leitura de dashboard em disputa pelo lock do caminho quente.
- Trocar o balde por contagem derivada (`count(*)` numa janela) porque
  "funcionou para concorrência na ADR-0009": para TAXA isso é varredura de
  histórico, com custo proporcional à janela em vez de a `max`.
