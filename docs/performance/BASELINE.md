# BASELINE.md — claim query por dialeto

Vale mais que intuição — inclusive a de quem escreveu isto (CLAUDE.md).
**Não editar retroativamente**: baseline só muda com um novo baseline
(nova seção, nunca sobrescrever a anterior). Planos de execução citados
aqui estão salvos neste mesmo diretório — os desta primeira rodada levam
o sufixo `-2026-08-14-before-priority-index` porque a rodada seguinte
mudou o índice de claim e reusou os nomes de arquivo originais para os
planos "depois".

## Metodologia

Harnesses: `src/test/java/io/mohs/jdbc/ClaimQueryLoadHarness.java`
(latência/throughput) e `ClaimQueryExplainHarness.java` (planos de
execução + investigação de lock). Comandos (fora da suíte unitária de
propósito — nome não bate o padrão de inclusão do Surefire):

```
mvn test -Dtest=ClaimQueryLoadHarness
mvn test -Dtest=ClaimQueryExplainHarness
```

Mede `JdbcClaimer.claim` fim-a-fim (bind de parâmetro + rede + plano de
execução do banco) contra os 4 dialetos suportados (ADR-0022/0023), via
Testcontainers para Postgres/MySQL/SQL Server e H2 embarcado. Dois
cenários por dialeto no harness de carga, cada um com `DataSource`/schema
isolado:

- **Latência**: 1 nó, `batchSize = 20`, backlog parado (1800 execuções
  em 150 jobs, `allowConcurrentExecutions = true`). 10 chamadas de
  aquecimento descartadas, 50 amostradas. min/p50/p99/max em ms.
- **Throughput**: 8 nós virtuais disputando o mesmo backlog (3000
  execuções em 300 jobs) até esvaziar, `batchSize = 20`. Métrica: linhas
  reivindicadas / segundo corrido.

Pool: HikariCP (`maximumPoolSize = 16`, `connectionTimeout = 2s`).

## Ambiente desta rodada — 2026-08-14

Máquina de desenvolvimento local (Windows, 24 threads lógicos), Docker
Desktop local via Testcontainers — **não é hardware de produção nem CI**.
Números não são portáveis pra outro ambiente; servem pra comparar
dialetos entre si nesta rodada e detectar regressão relativa em rodadas
futuras na mesma máquina. Imagens: `postgres:16-alpine`, `mysql:8.0`,
`mcr.microsoft.com/mssql/server:2022-latest`, H2 2.4.240 embarcado. JDK:
Temurin 25.0.4.

## Resultados

| Dialeto    | min (ms) | p50 (ms) | p99 (ms) | max (ms) | throughput (rows/s) |
|------------|---------:|---------:|---------:|---------:|---------------------:|
| H2         |     1.74 |     2.80 |     5.53 |     5.53 |               6325.7 |
| PostgreSQL |    17.64 |    23.11 |    27.54 |    27.54 |               3305.0 |
| MySQL      |    24.85 |    28.24 |    45.64 |    45.64 |                651.9 |
| SQL Server |    20.35 |    22.27 |    65.64 |    65.64 |               1148.2 |

(p99 com 50 amostras cai no último índice — na prática é o máximo
observado, não um percentil estatístico robusto.)

## Investigação: por que MySQL tem throughput ~5x pior que Postgres

Ponto de partida: latência por chamada do MySQL é parecida com
Postgres/SQL Server (~25-28ms), mas throughput sob 8 nós concorrentes é
desproporcionalmente pior (651.9 vs. 3305.0 rows/s). Índice usado na
claim query (`idx_mohs_executions_claim (state, scheduled_at)`) é
idêntico nos 4 schemas — não é índice ausente.

### Hipótese 1 — contenção de row lock sob `SKIP LOCKED`: **refutada**

`Innodb_row_lock_waits`/`Innodb_row_lock_time` (contadores globais
cumulativos do InnoDB, sempre disponíveis) medidos antes/depois do
mesmo cenário de 8 nós concorrentes drenando 3000 linhas — delta **zero**
nos quatro contadores (`explain-mysql-lock-investigation-2026-08-14-before-priority-index.txt`). Nenhuma
thread bloqueou esperando lock de linha; `SKIP LOCKED` está se
comportando exatamente como o nome promete. A hipótese inicial (registrada
na rodada anterior deste documento) estava errada.

### Achado real — full scan + full sort em toda chamada, não só no MySQL

`EXPLAIN ANALYZE` (MySQL, `explain-mysql-2026-08-14-before-priority-index.txt`) mostra que uma única
execução do `SELECT` sozinho leva **1.67ms** — muito abaixo dos ~28ms
medidos por chamada de `claim()`. O gargalo não é o tempo de CPU da
query em si; é o que ela faz:

```
Sort: e.priority, e.scheduled_at  (actual rows=20 loops=1)
  Filter: state=ENQUEUED AND scheduled_at<=now  (actual rows=3000 loops=1)
    Table scan on e  (actual rows=3000 loops=1)
```

MySQL varre a tabela inteira (`Table scan`, não usa
`idx_mohs_executions_claim`) e **ordena todas as ~3000 linhas
correspondentes antes de aplicar o `LIMIT 20`** — em **toda** chamada de
`claim()`, não uma vez só. Com ~150 chamadas pra drenar o backlog inteiro
sob 8 nós, isso é ordem de O(n²) trabalho de scan/sort acumulado.

O mesmo padrão aparece em SQL Server (`explain-sqlserver-2026-08-14-before-priority-index.txt`, plano
estimado via `SET SHOWPLAN_ALL`): `Clustered Index Scan` (varredura
completa, ignora `idx_mohs_executions_claim`) → `Sort` → só depois
`Filter`. Postgres (`explain-postgresql-2026-08-14-before-priority-index.txt`) usa o índice pra filtrar
(`Index Scan using idx_mohs_executions_claim`, só 62 buffer hits), mas
ainda assim faz `Sort Method: quicksort` de todas as linhas casadas antes
do `Limit` — sem otimização de top-N sort, porque o `LockRows` acima
impede o planner de aplicar esse atalho.

**Causa raiz compartilhada pelos 4 dialetos**: `idx_mohs_executions_claim
(state, scheduled_at)` não cobre `ORDER BY priority, scheduled_at` — o
motor não tem como caminhar pelas linhas já em ordem de prioridade
enquanto filtra por `state`/`scheduled_at`, então precisa materializar e
ordenar o conjunto inteiro que casa o filtro antes de conseguir aplicar o
`LIMIT`. H2 é o único que não paga esse custo de forma visível porque é
embarcado (sem round-trip de rede) e o dataset de 3000 linhas ainda cabe
inteiramente em memória sem I/O perceptível.

**Em aberto, não confirmado**: por que MySQL especificamente é ~5x pior
que Postgres sob concorrência, se SQL Server tem o mesmo padrão de plano
(scan completo + sort) e "só" fica ~3x pior. Hipótese não testada: custo
de CPU/latch do InnoDB sob varredura completa + MVCC concorrente
(8 threads escaneando a mesma tabela simultaneamente), ou limite de
recurso do container `mysql:8.0` do Testcontainers (buffer pool
default). Precisaria de profiling dentro do container (`SHOW ENGINE
INNODB STATUS` durante a carga, ou `perf`/flame graph do processo
`mysqld`) pra confirmar — não feito nesta rodada.

### Achado separado, ortogonal ao MySQL — round-trips não agrupados no claim

`JdbcClaimer.claimWithinTransaction` faz **um `UPDATE` por candidato
reivindicado**, em loop sequencial (`tryTransitionToRunning`), nunca em
lote — para `batchSize = 20` com todos os candidatos disponíveis, isso é
1 `SELECT` + até 20 `UPDATE`s = até 21 round-trips por chamada de
`claim()`. Isso é dialeto-agnóstico (mesmo código Java pros 4 bancos) e
provavelmente explica boa parte da diferença entre a latência real da
query (1.67ms medido no MySQL) e a latência fim-a-fim observada
(23-28ms em Postgres/MySQL/SQL Server) — H2 não paga esse custo de rede
por ser embarcado, consistente com sua latência ser ~10x menor que os
outros três mesmo em proporção.

## Recomendações (não implementadas nesta rodada — fora do escopo pedido)

1. ~~Estender o índice de claim pra cobrir a ordenação, ex.:
   `(state, priority, scheduled_at)`~~ — feito na rodada seguinte, ver
   "Depois do índice `(state, priority, scheduled_at)`" abaixo.
2. Investigar se agrupar os `UPDATE` de `tryTransitionToRunning` (batch
   update em vez de loop sequencial) reduz round-trips por chamada de
   `claim()` — motor de corretude (CAS, ADR-0018) não muda, só a forma
   de disparar os `UPDATE`s.
3. Se o achado em aberto do MySQL importar pra decisão de dialeto
   recomendado em produção, perfilar dentro do container sob a mesma
   carga (`SHOW ENGINE INNODB STATUS`, `performance_schema`).

## Arquivos desta rodada (índice original — `(state, scheduled_at)`)

- `explain-h2-2026-08-14-before-priority-index.txt` — plano do H2 (`EXPLAIN`).
- `explain-postgresql-2026-08-14-before-priority-index.txt` — plano real do Postgres (`EXPLAIN ANALYZE, BUFFERS`).
- `explain-mysql-2026-08-14-before-priority-index.txt` — plano real do MySQL (`EXPLAIN ANALYZE`).
- `explain-sqlserver-2026-08-14-before-priority-index.txt` — plano estimado do SQL Server (`SET SHOWPLAN_ALL ON`).
- `explain-mysql-lock-investigation-2026-08-14-before-priority-index.txt` — delta de `Innodb_row_lock_*`
  antes/depois do cenário de 8 nós concorrentes.

## Depois do índice `(state, priority, scheduled_at)` — 2026-08-14

Implementado nos 4 schemas (`schema-h2.sql`, `schema-postgresql.sql`,
`schema-mysql.sql`, `schema-sqlserver.sql`) — mesma metodologia/escala
desta rodada, ambiente idêntico (mesma máquina, mesmas imagens de
container).

### Resultados

| Dialeto    | p50 antes (ms) | p50 depois (ms) | throughput antes (rows/s) | throughput depois (rows/s) | ganho de throughput |
|------------|---------------:|-----------------:|---------------------------:|-----------------------------:|---------------------:|
| H2         |           2.80 |              2.39 |                      6325.7 |                        5958.6 |              ~igual (ruído) |
| PostgreSQL |          23.11 |             13.85 |                      3305.0 |                        5378.4 |                +63% |
| MySQL      |          28.24 |             27.94 |                       651.9 |                        3060.1 |               +369% |
| SQL Server |          22.27 |             20.78 |                      1148.2 |                        4879.5 |               +325% |

Confirma a hipótese: os 3 bancos reais deixaram de fazer scan completo +
sort da tabela em toda chamada de `claim()`.

- **MySQL** (`explain-mysql.txt`): trocou `Table scan` + `Sort` por
  `Index lookup on e using idx_mohs_executions_claim` — sem nó de sort
  nenhum. Tempo real do `SELECT` sozinho: **0.131ms** (era 1.67ms).
- **PostgreSQL** (`explain-postgresql.txt`): `Index Scan using
  idx_mohs_executions_claim` continua ali, mas o `Sort` explícito
  desapareceu — o índice já entrega na ordem que o `ORDER BY` precisa.
  `Execution Time: 0.074ms` (era 46.482ms — 600x, embora aquela medição
  original fosse de container recém-subido, sem cache aquecido; não é
  uma comparação limpa isolada do índice).
- **SQL Server** (`explain-sqlserver.txt`): `Clustered Index Scan` + `Sort`
  virou `Index Seek(idx_mohs_executions_claim, SEEK: state='ENQUEUED',
  ORDERED FORWARD)` — mesmo mecanismo, sem sort.

`Innodb_row_lock_*` seguem em zero (`explain-mysql-lock-investigation.txt`)
— confirma de novo que lock nunca foi a causa, nem antes nem depois.

### O que o antes/depois não explica sozinho

Latência **por chamada** do MySQL quase não mudou (28.24ms → 27.94ms)
mesmo com o `SELECT` isolado passando de 1.67ms para 0.131ms — a fase de
latência do harness usa 1 nó sequencial sem contenção, então o ganho do
índice apareceu quase todo em throughput sob concorrência, não em
latência sequencial. Ainda não medido: quanto da latência de ~28ms
remanescente é o achado separado já registrado acima (até 21 round-trips
de `UPDATE` sequenciais por chamada de `claim()`) — esse continua sem
alteração nesta rodada, é a próxima alavanca óbvia se a latência por
chamada (não só throughput) importar.

### Arquivos desta rodada (índice novo — `(state, priority, scheduled_at)`)

- `explain-h2.txt`, `explain-postgresql.txt`, `explain-mysql.txt`,
  `explain-sqlserver.txt`, `explain-mysql-lock-investigation.txt` —
  nomes sem sufixo de data = estado atual (mais recente).

## Índice parcial/filtrado (`WHERE state = 'ENQUEUED'`) — 2026-08-14

DBTUNE-5 (`docs/codereview-tuning.md`): `idx_mohs_executions_claim` full
(`state, priority, scheduled_at`) carrega toda a tabela, inclusive
execuções terminais que nunca mais são candidatas a claim — peso morto
que só cresce. Postgres e SQL Server suportam índice parcial/filtrado
(`state` sai das colunas, vira predicado do `WHERE`); MySQL e H2 não
têm esse recurso, ficam com a composta cheia.

Harness dedicado: `src/test/java/io/mohs/jdbc/ClaimIndexTuningHarness.java`
— dois métodos de teste (`postgres`, `sqlServer`), cada um seedando
20.000 execuções terminais de histórico + 300 jobs/3.000 execuções
`ENQUEUED` de backlog, medindo tamanho do índice e throughput do mesmo
cenário de 8 nós concorrentes do `ClaimQueryLoadHarness`, antes e depois
de trocar o índice no mesmo container. Mesma máquina/imagens desta
rodada.

```
mvn test -Dtest=ClaimIndexTuningHarness#postgres
mvn test -Dtest=ClaimIndexTuningHarness#sqlServer
```

### Resultados

| Dialeto    | índice antes (KB) | índice depois (KB) | redução | throughput antes (rows/s) | throughput depois (rows/s) |
|------------|-------------------:|---------------------:|--------:|----------------------------:|------------------------------:|
| PostgreSQL |               840.0 |                  40.0 |   95.2% |                     11602.3 |                        9731.3 |
| SQL Server |              3232.0 |                 512.0 |   84.2% |                      4511.8 |                        5058.8 |

Redução de tamanho é o ganho real e esperado — 20.000 linhas de
histórico saem do índice, sobra só o backlog vivo. Throughput não muda
de forma consistente (Postgres cai ~16%, SQL Server sobe ~12%) — dentro
do ruído de uma amostra curta (~3.000 claims), nem esperado subir: a
claim query já filtra por `state` como primeira coluna do índice full,
então o índice parcial não muda o plano de execução, só o volume de
páginas que o índice ocupa (menos WAL/manutenção por insere/completa,
melhor cache locality — não latência de claim).

Aplicado em `schema-postgresql.sql` e `schema-sqlserver.sql`. MySQL/H2
mantêm a composta cheia — sem suporte a índice parcial.

## Depois do retry claimável (ADR-0033) — 2026-08-15

`state IN ('ENQUEUED', 'RETRY_SCHEDULED')` entrou na claim query (retry
viaja pelo caminho do claim — ADR-0033), os índices parcial (Postgres) e
filtrado (SQL Server) acompanharam o predicado, e o CAS final ganhou
`scheduled_at <= :now`. Mesma metodologia/escala, mesma máquina — dia
diferente da rodada anterior. Imagens: `postgres:16-alpine`, `mysql:8.0`
e H2 embarcado como antes; **SQL Server subiu de `2022-latest` para
`2025-latest`** (`d2d5c7f`, depois da rodada 08-14) — o "~igual" do SQL
Server compara engines de major versions diferentes e vale o mesmo
asterisco de não-atribuição de H2/PG.

**Limitação declarada**: o backlog semeado é 100% `ENQUEUED` — esta
rodada mede o *predicado* novo, não o *cenário* com retries no dado.
A rodada que decidir a correção do MySQL (abaixo) precisa de seed misto
(ex.: ~20% `RETRY_SCHEDULED`), senão o braço/query do segundo estado
retorna vazio de graça e ambas as candidatas parecerão melhores do que
são.

**Correção de metodologia nesta rodada**, aplicada ao
`ClaimQueryExplainHarness` antes das capturas: as cópias literais da
query tinham sofrido drift (sem o par de estados desta rodada e — desde
`b09d0b9` — sem `j.retired`), e as estatísticas do otimizador passaram a
ser atualizadas após o seed nas **duas** tabelas do join (lição do
`FindPageQueryExplainHarness`, que é single-table e só precisava de uma).
Planos anteriores a esta correção mediam uma query que não existia mais.

### Resultados (ClaimQueryLoadHarness)

| Dialeto    | min (ms) | p50 (ms) | p99 (ms) | max (ms) | throughput (rows/s) | vs 08-14 (throughput) |
|------------|---------:|---------:|---------:|---------:|--------------------:|----------------------:|
| H2         |     1.42 |     2.11 |     5.15 |     5.15 |             10370.2 |       5958.6 → +74%* |
| PostgreSQL |     6.65 |     7.42 |    11.45 |    11.45 |             11259.2 |      5378.4 → +109%* |
| MySQL      |    20.92 |    23.85 |    90.81 |    90.81 |               987.7 |  3060.1 → **−68%**   |
| SQL Server |    16.75 |    17.83 |    29.63 |    29.63 |              4899.5 |      4879.5 → ~igual |

\* Ganhos de H2/Postgres nesta magnitude **não são atribuíveis à
mudança** (ampliar um `IN` não acelera nada por si) — dia diferente na
mesma máquina carrega ruído de ambiente. O que os planos sustentam é a
afirmação negativa: nenhuma regressão de plano nesses dialetos.

### Planos desta rodada

- **PostgreSQL** (`explain-postgresql.txt`): `Index Scan using
  idx_mohs_executions_claim` (parcial novo, `WHERE state IN (...)`), sem
  Sort — `state` não está nas colunas do índice parcial, então a ordem
  `(priority, scheduled_at)` satisfaz o `ORDER BY` globalmente,
  independente de quantos estados o predicado aceite. A regressão que o
  predicado novo causava com o índice antigo (Seq Scan + Sort da tabela
  inteira por tick — medida na revisão da ADR-0033) não existe com o
  índice acompanhando.
- **SQL Server** (`explain-sqlserver.txt`): `Index Scan(
  idx_mohs_executions_claim, ORDERED FORWARD)` — filtered index novo,
  sem Sort. Mesmo raciocínio do Postgres.
- **H2** (`explain-h2.txt`): o otimizador escolheu
  `idx_mohs_executions_reaper` pro filtro de estado; os números
  melhoraram mesmo assim (in-memory, sort de 3k linhas barato) — sem
  ação.
- **MySQL** (`explain-mysql.txt`): **regressão real e explicada** — com
  `state IN` de dois valores, os dois ranges do índice composto
  `(state, priority, scheduled_at)` não concatenam na ordem do
  `ORDER BY`, e o otimizador (estatísticas frescas) descartou o índice
  de claim: `Table scan on j` → lookup por `uq_mohs_executions_idem` →
  **`Sort` de 3.000 linhas por chamada**. É o mesmo par scan+sort que o
  índice composto tinha eliminado (+369% na rodada 08-14), desfeito pelo
  segundo estado claimável. p99 45.64 → 90.81ms (45.64 é da rodada 08-14
  inicial — a rodada do índice composto não registrou p99); throughput
  3060 → 988 rows/s.

### Próxima alavanca (MySQL) — não implementada nesta rodada

Duas candidatas, a decidir por medição (sem número, não é otimização):

1. Template próprio no `MySqlJdbcDialect` (a liberdade que o Javadoc de
   `ANSI_SKIP_LOCKED_CANDIDATES` já prevê): `UNION ALL` de dois braços
   `state = constante` — cada braço sai na ordem do índice com `LIMIT`,
   sort final sobre ≤ 2×batch linhas. Pré-requisito a validar: locking
   clause em query block parentesizado exige MySQL 8.0.31+.
2. Duas consultas por tick no dialeto (uma por estado), merge por
   `(priority, scheduled_at)` no lado Java — top-k da união ⊆ união dos
   top-k por partição, então é correto; custo de um round-trip extra só
   no MySQL.

Contexto de severidade: 988 rows/s ainda fica ordens de magnitude acima
de qualquer taxa de claim realista — correção veio primeiro, a
otimização entra com número próprio.

### Arquivos desta rodada

- `explain-h2.txt`, `explain-postgresql.txt`, `explain-mysql.txt`,
  `explain-sqlserver.txt`, `explain-mysql-lock-investigation.txt` —
  estado atual (query completa da ADR-0033, estatísticas atualizadas nas
  duas tabelas).
- Sufixo `-2026-08-14-before-retry-claim` — planos da rodada anterior,
  preservados (capturados ainda com as cópias em drift; ver a nota de
  metodologia acima).

## Candidata `UNION ALL` por estado no MySQL — medida e **rejeitada** — 2026-08-15

Executa a decisão pendente da rodada anterior. Metodologia atualizada
conforme a limitação lá declarada: os dois harnesses agora semeiam seed
misto determinístico (~20% `RETRY_SCHEDULED` — 25% exatos na fase de
latência, 3 de 12 por job; 20% exatos em throughput/explain). Mesma
máquina, mesmas imagens da rodada anterior desta data, mesmo dia.

### Antes — template ANSI com seed misto (a régua desta rodada)

| Dialeto    | p50 (ms) | p99 (ms) | throughput (rows/s) |
|------------|---------:|---------:|--------------------:|
| H2         |     2.35 |     4.99 |             11742.3 |
| PostgreSQL |     7.81 |     9.76 |             11299.6 |
| MySQL      |    23.51 |    47.78 |              1429.8 |
| SQL Server |    18.20 |    28.03 |              5811.6 |

(Os números de MySQL desta tabela **não** são comparáveis 1:1 com o
987.7/3060.1 das rodadas anteriores — seed, predicado das capturas e
estatísticas mudaram entre elas; a comparação controlada desta rodada é
a linha de cima contra a candidata abaixo, mesmo seed, mesmo dia.)

### Candidata: `UNION ALL` de dois braços `state = constante`

Implementada em template próprio do `MySqlJdbcDialect` e medida:

| Métrica MySQL | ANSI (antes) | UNION ALL | veredito |
|---|---:|---:|---|
| p50 (ms) | 23.51 | 23.60 | ~igual |
| p99 (ms) | 47.78 | 28.40 | melhor |
| throughput (rows/s) | **1429.8** | 970.0 | **−32% — rejeitada** |

O plano da candidata (capturado com `EXPLAIN ANALYZE`, estatísticas
frescas) mostra por que a premissa era incompleta: **nem com
`state = constante` o otimizador usa o índice de claim** — ele dirige o
join por `mohs_job_definitions` (table scan de 300 jobs) → lookup em
`mohs_executions` pelo índice de idempotência → filtro de estado →
**Sort dentro de cada braço** (2.400 e 600 linhas). A candidata virou
dois scan+sort em vez de um. O problema real não é o `IN` de dois
estados: é a **escolha de join order** do otimizador com estatísticas
frescas. Consequência: a candidata 2 da rodada anterior (duas queries +
merge em Java) herdaria exatamente o mesmo plano por query — descartada
sem medir, pelo mesmo mecanismo.

### Decisão

`MySqlJdbcDialect` permanece no template ANSI — melhor configuração
medida (1429.8 rows/s). A próxima alavanca nomeada, se o throughput de
claim do MySQL importar de verdade em produção, é **hint de join/índice**
no template do dialeto (`STRAIGHT_JOIN` ou
`FORCE INDEX (idx_mohs_executions_claim)`) — atacar a escolha de join
order, que é o mecanismo demonstrado — medida com este mesmo protocolo
(seed misto). Ganho colateral que fica: os 4 testes de contrato do par
de estados claimáveis em `JdbcClaimerMySqlTest` (retry devido, merge
global, truncamento de batch, prioridade cross-state) pinam o
comportamento que qualquer template substituto precisa preservar.

`explain-mysql.txt` desta rodada = plano ANSI final (recapturado após o
reverso da candidata); o plano da candidata está transcrito acima por
mecanismo, não preservado em arquivo — falha de ordem de operações
(revertido antes de salvar), não decisão. **Regra a partir daqui**: o
plano bruto de toda candidata medida é preservado em arquivo sufixado
*antes* de decidir/reverter — a evidência primária do achado central de
um ciclo não pode depender de reimplementar a candidata.

## CAS do claim em lote no Postgres (DBTUNE-16) — 2026-08-16

Motivação: o claim fazia 1 SELECT + **um UPDATE guardado por candidato**
dentro da mesma transação — com `batchSize` N, 1+N round trips segurando
os row locks do `SKIP LOCKED` o tempo inteiro. A mudança: candidato sem
mutex de job atravessa o CAS numa única chamada de
`JdbcDialect.transitionToRunning` — default continua linha a linha
(comportamento anterior, todos os dialetos), `PostgresJdbcDialect`
sobrescreve com `UPDATE ... WHERE id IN (...) ... RETURNING id` (mesmas
guardas por linha da ADR-0018; o `RETURNING` devolve quem venceu). Round
trips por claim no Postgres: 1+N → 2. Candidato com mutex mantém a
cadeia guardada (o slot precisa do veredito individual pra desfazer).
Semântica preservada: ordem por (prioridade, `scheduled_at`), janela de
exclusão, mutex — pinada por 2 testes novos em
`JdbcClaimerPostgresTest` (verdes antes E depois da mudança).

Alternativa considerada e rejeitada sem medir: lock-and-fetch em
statement única (estilo db-scheduler, `UPDATE ... WHERE id IN (SELECT
... FOR UPDATE SKIP LOCKED)`) — exigiria tirar mutex/janela do caminho
único de candidatos, mudando a fairness de prioridade entre jobs simples
e com mutex (mudança de comportamento observável, fora do escopo de uma
otimização).

Metodologia idêntica à rodada 08-15 (seed misto ~20% `RETRY_SCHEDULED`),
mesma máquina, mesmas imagens. **Duas rodadas "depois"** para variância
run-to-run. As três rodadas desta seção são do mesmo dia, em sequência.

### Resultados (ClaimQueryLoadHarness, batchSize=20)

| Dialeto    | antes p50/p99 (ms) | antes (rows/s) | depois-1 (rows/s) | depois-2 (rows/s) | depois p50 (ms) |
|------------|-------------------:|---------------:|------------------:|------------------:|----------------:|
| H2         |        2.81 / 4.96 |         9031.9 |            9005.2 |            8112.5 |     2.98 / 3.02 |
| PostgreSQL |       7.61 / 25.76 |         9947.4 |       **23453.8** |       **22010.2** | **4.90 / 3.90** |
| MySQL      |      24.30 / 37.06 |          880.5 |            2273.2 |            2340.5 |   23.81 / 23.39 |
| SQL Server |      14.69 / 26.11 |         1825.5 |            3882.3 |            1966.3 |   14.63 / 22.96 |

(coluna "depois p50" traz as duas rodadas: depois-1 / depois-2.)

### Atribuição

- **PostgreSQL — atribuível**: throughput ~2.2x (9947 → 22010–23454
  rows/s), p50 caindo de 7.61 pra 3.90–4.90 ms, estável entre as duas
  rodadas "depois". O mecanismo explica: por claim de 20 linhas, 21
  statements viraram 2. O "antes" desta rodada é consistente com o
  baseline 08-15 (11299.6) — não estava deprimido.
- **H2 — controle**: caminho inalterado (default linha a linha,
  in-process, imune a variância de Docker) e números estáveis
  (9032 → 9005/8113) — evidência de que as condições de JVM/máquina
  eram comparáveis entre as rodadas.
- **MySQL e SQL Server — não atribuível**: caminho de código inalterado.
  O MySQL subiu ~2.6x com a mesma sequência de statements — o "antes"
  (880.5, abaixo do próprio baseline 08-15 de 1429.8) foi a primeira
  sessão Testcontainers do dia (aquecimento de container/volume). SQL
  Server oscilou ±50% entre as duas rodadas "depois" (3882 → 1966) —
  ruído puro. Nenhuma conclusão sobre esses dois nesta rodada.

### Planos desta rodada

- `explain-postgresql-batch-cas.txt` (**novo**): o CAS em lote —
  `Bitmap Index Scan` na PK pros 20 ids → um único nó `Update`, guardas
  de estado/`scheduled_at` como `Filter` por linha; 20 linhas em
  0.414 ms de execução. Nenhum scan/sort.
- `explain-postgresql.txt` (recapturado): SELECT de candidatos idêntico
  ao da rodada 08-15 — `Index Scan using idx_mohs_executions_claim`,
  sem Sort. A mudança não toca a query de candidatos.
- `explain-h2.txt`, `explain-mysql.txt`, `explain-sqlserver.txt`,
  `explain-mysql-lock-investigation.txt`: recapturados como efeito
  colateral do harness completo — mesma query, planos equivalentes aos
  da rodada 08-15 (a mudança não toca esses dialetos).

### Limitação declarada

O harness mede `batchSize = 20`. O cenário que motivou a mudança
(`mohs.engine.batch-size = 500`, 4 nós) tem N maior — a redução
estrutural 1+N → 2 round trips vale para qualquer N, mas o ganho
fim-a-fim nessa escala não foi medido nesta rodada (o throughput de
jobs completos depende também do caminho de dispatch, fora do escopo
deste harness).

## Tuning fim a fim no Postgres (DBTUNE-17/18 + ADR-0039) — 2026-08-16

Primeira medição de vazão FIM A FIM (schedule → claim → dispatch →
conclusão persistida), meta declarada de 4.000 exec/s. Ambiente: node
único no host (Windows), Postgres 18.4 `postgres:latest` em Docker
Desktop, handler trivial (log). Cenário: 10k invocações via REST
(`load-test.ps1`, pwsh, 500 conexões) e drains de 50k linhas ENQUEUED
semeadas por SQL (forma idêntica à do REST). Medição pela janela de
timestamps de `mohs_attempts` (`min(started_at) → max(finished_at)` do
seed), nunca por latência de cliente; atribuição por amostragem de
`pg_stat_activity` (wait events) durante drain ativo, deltas de
`pg_stat_database`/`pg_stat_wal`, EXPLAIN ANALYZE dos statements
quentes e pgbench de controle na mesma instância.

### Rodadas (drains de 50k, exceto onde indicado)

| Rodada | Mudança | Vazão (exec/s) |
|---|---|---:|
| 0 | defaults (`poll=5s`, `batch=50`) | **10,0** (teto aritmético da config) |
| 1 | config: `poll=100ms`, `batch=500`, `dispatch=256`, Hikari 100 | 985,9 · 1106,2 |
| 1b | + DBTUNE-17 (índice, abaixo), mesmo código | 1243,0 · 1297,7 |
| 2 | + DBTUNE-18 (memoização de definição por tick) | claim 3,3k/s, mas **56.187 rejeições** do runner e 11.666 re-execuções via reaper — vazão efetiva ~1.839 com churn |
| 3 | + ADR-0039 (claim limitado pela folga de dispatch) | 1878,1 — **zero rejeição, zero retry** |
| 4 | config: `poll=50ms`, `dispatch=768`, eventos 128, Hikari 250 | 3830,9 |
| 5 | config: `dispatch=1024`, `batch=1000`, eventos 256, Hikari 300 | **4023,2 · 4221,8** ✅ meta |

Validação com o cenário real: `load-test.ps1` (10k via REST) termina em
~13,3s (~750 req/s, limitado pelo cliente pwsh; latência média do POST
7,8ms estável com o engine drenando simultaneamente) e o backlog zera
junto com o fim do script — o sistema ficou ingest-bound.

### O que mudou

- **DBTUNE-17 (banco)**: o índice parcial do reaper
  (`WHERE state = 'RUNNING'`) capturava o plano do CAS de conclusão por
  id (implicação de predicado + estatísticas de repouso): 8,4ms e 5.958
  buffers por conclusão varrendo as entradas RUNNING, 149.085 `idx_scan`
  no índice errado. Predicado novo:
  `WHERE state = 'RUNNING' AND lease_expires_at IS NOT NULL` — o CAS por
  id cai na PK (0,105ms), o reaper continua elegível (operador estrito
  implica IS NOT NULL). Trocado ao vivo com CREATE/DROP INDEX
  CONCURRENTLY; plano bruto antes/depois em
  `explain-postgresql-completion-cas.txt`. O índice filtrado do SQL
  Server tem o mesmo shape — investigar lá antes de mexer.
- **DBTUNE-18 (código)**: as 500 consultas de definição POR EXECUÇÃO,
  seriais na thread do tick (57% de um ciclo de 454ms — o lote inteiro
  gotejava pro dispatch a ~0,5ms por round trip), viraram uma consulta
  por `job_key` distinto do lote, memoizada num mapa que vive só dentro
  do tick (`Engine.submitDispatch`). Falha de consulta não é memoizada —
  caminho de recuperação preservado.
- **ADR-0039 (código)**: o claim rápido pós-DBTUNE-18 reivindicava além
  da capacidade de dispatch; a rejeição do runner deixava execução
  RUNNING presa até o reaper (56k rejeições/11,6k duplicatas num drain).
  O claim agora pede `min(batch-size, dispatch-concurrency − in-flight)`.
- **Config** (rodadas 4-5): ver tabela — nenhum desses valores é
  recomendação universal; são o ponto de operação desta máquina/carga.

### Limitações declaradas

- Node único, handler trivial, Postgres local em Docker Desktop — os
  números medem o overhead do motor, não uma instalação de produção.
- A ingestão REST está limitada pelo cliente de teste (~750 req/s);
  o servidor respondia a 7,8ms de média sob o drain a 4k/s.
- Variância run-to-run real (~±10%): comparar sempre com duas rodadas
  e com a perícia de ciclo (spread de `fired_at` por `lease_expires_at`),
  não só com a taxa bruta.
- Latência de commit domina o custo por execução (2 commits síncronos:
  `markFired` + conclusão; `LWLock:WALWrite` no topo do perfil de waits).
  As alavancas seguintes (fusão do `markFired`, conclusão em lote,
  `synchronous_commit` por sessão, fillfactor) mudam semântica ou
  fronteira de transação — registradas como propostas, não aplicadas.

## Reaper no SQL Server: covering index (DBTUNE-19) — 2026-08-16

Fechamento da pendência declarada no DBTUNE-17 ("o índice filtrado do SQL
Server tem o mesmo shape — medir lá antes de mexer"). Bancada: SQL Server
2022 CU26 em Docker, 150k linhas terminais + 500 RUNNING, estatísticas
controladas, statements na forma exata do JDBC (`sp_executesql`, `state`
literal); planos e IO em `explain-sqlserver-reaper-covering.txt`.

**Resultado negativo primeiro (igualmente registrado):** o trap do
DBTUNE-17 NÃO se manifesta no SQL Server — o CAS de conclusão por id caiu
na PK clusterizada nos dois regimes de estatística (5 reads; fenced 3).
O predicado do índice não precisa do `IS NOT NULL` do Postgres.

**Achado real, diferente do procurado:** o plano do *reaper* é instável
sob parameter sniffing. Com o índice antigo (não-covering), o caminho do
índice paga 1 key lookup por candidato e empata em custo com o clustered
scan quando o `@now` farejado casa muitos expirados — exatamente os
momentos de tempestade (morte de nó, boot pós-downtime) em que o reaper
compila com trabalho na fila. O scan então fica preso no plan cache:

| Cenário (índice antigo) | logical reads |
|---|---:|
| compilado com 0 expirados (seek filtrado) | 4-5 |
| compilado com 500 expirados (clustered scan) | 3.549 |
| tick seguinte, 0 expirados, plano do cache | **3.549 por tick, à toa** |

**Fix aplicado:** covering — `INCLUDE (job_key, cancel_requested)` no
índice filtrado (`id` é chave clusterizada, implícita). Sem lookups, o
caminho do índice domina o scan em qualquer compile:

| Cenário (covering) | logical reads |
|---|---:|
| compilado com 500 expirados | 6 |
| tick seguinte, 0 expirados (cache) | 3 |

CAS re-verificado com o índice novo: inalterado (PK). Custo do INCLUDE:
só linhas RUNNING carregam o índice filtrado — as colunas extras pesam
apenas no in-flight. Postgres/H2/MySQL não têm o fenômeno (Postgres
replaneja por execução; H2/MySQL usam composto cheio e o UPDATE por id
cai trivialmente na PK) — nenhuma mudança fora do SQL Server.

## Rounds de claim por tick (ADR-0040) — 2026-08-16

A proposta (e) da rodada de tuning, aprovada e implementada a partir do
padrão de outbox worker do autor (`mohs.engine.claim-rounds`, default 1 =
comportamento clássico). A metade "cursor por id sequencial" do padrão
foi rejeitada com argumentos registrados na ADR-0040 (fila de prioridade
≠ FIFO; o CAS de estado já é o cursor físico; UUIDv7 é keyset-ável se um
dia precisar). Mesma máquina/metodologia das rodadas anteriores, drains
de 50k, config base da rodada 5 (`batch=1000`, `dispatch=1024`,
`events=256`, Hikari 300), variando só poll e rounds:

| Poll | Rounds | Vazão (exec/s) |
|---|---|---:|
| 50ms | 1 (rodada 5, referência) | 4.023 · 4.222 |
| 250ms | 1 (controle) | 2.277,0 |
| 250ms | 8 | **3.604,6 · 3.739,0** (+58–64% vs controle) |
| 1s | 8 | 2.134,3 |

Leitura honesta: rounds **relaxam** o acoplamento da vazão com o
`poll-interval` (5x mais poll custou ~10% da vazão da rodada 5, em vez
dos ~45% do controle), mas **não o eliminam** — o tick enche o tanque de
in-flight (`dispatch-concurrency`, clamp da ADR-0039, recomputado a cada
round) e, com poll maior que o tempo de drenagem do tanque, o node ocioso
espera o próximo tick: a 1s o teto efetivo vira
`dispatch-concurrency / ciclo`. Zero retries e zero rejeições em todas as
rodadas — a interação rounds × clamp segurou a patologia pré-ADR-0039.
Formato do tick pinado por teste com trilha tick/claim
(`EngineTest#aFullBatchChainsAnotherClaimRoundWithinTheSameTick`).

## Queries do GET /overview (DBTUNE-20) — 2026-08-17

A promessa do endpoint é "barato por construção": custo proporcional ao
trabalho vivo e à janela de vazão, nunca ao histórico — é a razão de o
contrato NÃO ter contagem all-time de estados terminais (REST-API-DESIGN
v0.6). Três queries: backlog (`state IN (ENQUEUED, RETRY_SCHEDULED)
GROUP BY state`), RUNNING (`state = 'RUNNING' AND lease_expires_at IS
NOT NULL` — o predicado extra não filtra nada, existe só pra tornar o
índice parcial do reaper elegível, DBTUNE-17), e janela de attempts
(`finished_at >= :since AND outcome IN (SUCCEEDED, FAILED) GROUP BY
outcome`) sobre o índice novo `idx_mohs_attempts_throughput
(finished_at, outcome)` — criado nos 4 dialetos nesta rodada.

Cenário do harness: 100k execuções terminais (95/5 SUCCEEDED/FAILED),
cada uma com seu attempt espalhado ~28h (janela de 60s cobre ~61), 550
ENQUEUED, 50 RETRY_SCHEDULED, 100 RUNNING com lease — histórico
dominante, trabalho vivo pequeno. Estatísticas atualizadas após o seed.

| Dialeto | backlog | RUNNING | janela de vazão |
|---|---|---|---|
| Postgres | Index Scan `idx_claim`, 550 rows, 14 buffers, 0.15 ms | Index Only Scan `idx_reaper`, 0.04 ms | Index Only Scan `idx_throughput`, 61/100k rows, 0.06 ms |
| MySQL | Covering index range scan `idx_claim` (state líder) | Covering index range scan `idx_reaper` | Covering index range scan `idx_throughput`, 61 rows |
| SQL Server | Index Scan no filtrado `idx_claim` + key lookup (550) | Index Seek no filtrado `idx_reaper` | Index Seek `idx_throughput`, ~64 rows est. |
| H2 | `idx` de claim/reaper | `idx_reaper` | `idx_throughput` |

Leitura honesta: em Postgres/SQL Server a elegibilidade vem da implicação
de predicado dos índices parciais/filtrados (por isso os literais de
estado inlinados — bind quebraria o match do filtered index no SQL
Server); em MySQL/H2 não há índice parcial e quem serve é o prefixo
`state` dos compostos — mecanismo diferente, mesmo resultado. O key
lookup do backlog no SQL Server (o filtrado de claim não carrega `state`)
custa 1 lookup/linha de backlog — proporcional ao trabalho vivo,
aceitável; um `INCLUDE (state)` é alavanca futura SE o backlog crescer.
Todas as formas sub-ms no volume do cenário. O plano do SQL Server foi
capturado com o `WITH (NOLOCK)` de produção (dialeto: monitoramento não
adquire lock; anomalias aceitas documentadas no REST-API-DESIGN v0.7).

Planos completos: `explain-overview-{postgresql,mysql,sqlserver,h2}.txt`.

## Write amplification por execução (Phase 0 do redesign) — 2026-08-21

Os três números que o `ARCHITECTURE_REDESIGN_PLAN.md` exige antes de
qualquer fase (commits/execução, tuple versions/execução, WAL bytes/
execução) não existiam. Medidos aqui pela primeira vez, com
`mohs-benchmark/scripts/write-amplification.ps1`: app no ponto de
operação da rodada 5 (`poll=50ms`, `batch=1000`, `dispatch=1024`,
eventos 256, Hikari 300), Postgres 18.4 (`postgres:latest`, Docker),
drains de 50k semeados por SQL, deltas de `pg_stat_database`/
`pg_stat_user_tables`/`pg_stat_wal`, janela de calibração idle de 30s
para quantificar o ruído de fundo (~97 commits/s: polls vazios do tick
+ every-job PT1S + heartbeat — descontado nas leituras por execução).

Contexto que difere da rodada 08-16: a tabela já carregava ~500k
execuções terminais de rodadas anteriores (lá o histórico era menor) e
a vazão ficou em 3.006–3.343 exec/s, abaixo dos 4.0–4.2k de referência.
Os números abaixo são razões por execução — válidas nessa vazão — mas a
diferença fica registrada: histórico maior + dead tuples (~124k por
drain) no mesmo heap é exatamente o acoplamento que a Phase 5 do plano
quer remover.

| Métrica (por execução, engine-side) | Rodada 1 | Rodada 2 |
|---|---:|---:|
| commits (`xact_commit`) | 3,965 | 3,915 |
| tuple versions em `mohs_executions` (ins+upd) | 3,966 | 3,904 |
| WAL bytes | 3.177 | 2.226 |
| WAL records | 26,1 | 25,6 |
| updates em `mohs_executions` | 2,965 | 2,903 |
| — dos quais HOT | 0,518 | 0,494 |

A rodada 1 pagou 6.116 FPIs (checkpoint recente); a rodada 2 (4 FPIs) é
a leitura limpa de WAL: **~2,2 KB/execução**. O enqueue não está nos
números (seed por SQL): +1 commit e +1 tuple version por construção
(ADR-0003 §4) — total fim a fim ≈ 4,9 commits/execução.

**Atribuição dos ~3,9 commits** (amostragem de `pg_stat_activity`
durante drain + leitura do caminho em `Dispatcher`/`JdbcExecutionStore`):

- **2,0 são commits síncronos de escrita** — `markFired` (autocommit,
  round trip próprio; a amostra mais frequente do perfil ativo) e a
  transação de conclusão (CAS + `INSERT mohs_attempts` + decremento
  guardado). O claim amortiza a ~1/1000. Consistente com os updates:
  claim + markFired + CAS ≈ 2,97 upd/execução.
- **~1,9 são round trips autocommit de leitura** — a carga de attempts
  para montar o `Execution` do dispatch e cerimônia por transação
  (`SHOW TRANSACTION ISOLATION LEVEL` do pgJDBC). Não pagam WALWrite,
  mas pagam latência de rede — alavanca separada da fusão de commits.

**Divergências contra as previsões do plano (§1.1), registradas:**

1. **Tuple versions medidas: ~3,9, não ~9.** O Finding B previa ~5
   renovações de lease por execução; neste ponto de operação (handler
   trivial) a renovação é **≈ 0** — `renewOwnedLeases` só renova o que
   está em voo NO INSTANTE do tick, e o pipeline esvazia entre ticks. O
   custo de renovação é `in-flight sustentado × ticks/s`: com 1.024
   jobs lentos ocupando o tanque, o teto aritmético de ~20k updates/s
   do Finding A segue real — mas é carga-dependente, não constante. A
   medição com handler lento fica como pendência da Phase 4 (E6).
2. **Commits reais: ~3,9 engine-side, não 2.** A seção 08-16 dizia "2
   commits síncronos" — verdade para escrita, mas o caminho paga mais
   ~1,9 leituras autocommit por execução que ninguém tinha contado. O
   prêmio do group commit (Phase 3) é maior que o estimado.
3. **~0,5 update/execução é HOT** — claim e CAS terminal não-HOT
   (predicados de índice parcial), `markFired` parcialmente HOT.
   Confirma o mecanismo do Finding B, com números.

## E2/E3 — forma do claim e sharding (Phase 1 do redesign) — 2026-08-21

Experimentos do `ARCHITECTURE_REDESIGN_PLAN.md` §20.3 que decidem a
ADR-A (§5.4, forma nova do claim) e a ADR-F (§8.3, sharding), ANTES de
qualquer código de produção. `ClaimShapeExperimentHarness`
(`mohs-benchmark`), Postgres 16 (Testcontainers), mesmo container para
todos os braços: braço "current" = `JdbcClaimer` real sobre
`schema-postgresql.sql` (seed misto ~20% RETRY_SCHEDULED); braços novos
= `e2_ready`/`e2_lease`/`e2_execution` particionada, claim CTE
`DELETE … RETURNING` + `INSERT` **incluindo a leitura batched de
payload** (sem ela a comparação mediria uma query que não existe).
`batch=100`, backlog 100k por sweep, `ANALYZE` pós-seed, warmup de JVM
descartado (a rodada sem warmup mediu current@1 com p99 238ms — puro
C2/buffers frios; corrigido nesta).

### Rodada 1 — a forma literal do plano morreu (atribuição exata)

Sem `ANALYZE` pós-seed e sem warmup de JVM (corrigidos nas rodadas
seguintes). A forma proposta pelo §5.4 v1 — `shard = ANY(:owned)` com
ownership 8/64 shards a 8 claimers — mediu 116.178 rows/s = **1,01× o
engine atual**, abaixo do kill criterion (1,3×); a variante sem shard
(`ANY` de 1 elemento) mediu 0,37×. Mecanismo, capturado por EXPLAIN:
com `= ANY` o index scan do PG não fornece a ordem
`priority, visible_at` — varredura completa dos candidatos + **sort
externo em disco a cada rodada** (25,5 ms/rodada, `Sort Method:
external merge Disk: 5496kB`, captura da rodada 1 com ownership 64/64,
impressa na saída da rodada; o artefato commitado traz a captura da
rodada 3 — ANY com ownership 8/64, sort em memória, ~4 ms, e a forma
single-shard, 0,43 ms). A correção: **um shard por statement**
(igualdade única no prefixo do índice, scan ordenado), round-robin
sobre os shards do claimer.

### Rodada 2 — metodologia corrigida

Com `ANALYZE` pós-seed e warmup descartado, `any/64 @8` subiu a 1,51× —
o swing 1,01→1,51 é metodologia (autoanalyze sorteando plano no meio do
drain + JIT frio), e é por isso que o descarte da forma `ANY` se apoia
em **dominância e instabilidade de plano**, não no kill criterion bruto.
Rodada intermediária, não é a de registro.

### Rodada 3 — registro (mediana de 3 nas células com claimers ≥ 8)

| arm | shards | claimers | rows/s | p50 (ms) | p99 (ms) |
|---|---:|---:|---:|---:|---:|
| current | — | 1 | 7.144,2 | 13,67 | 49,29 |
| current | — | 4 | 50.356,3 | 6,81 | 29,73 |
| current | — | 8 | 118.660,5 | 6,52 | 11,64 |
| current | — | 16 | 145.007,3 | 10,51 | 22,32 |
| any (§5.4 v1) | 1 | 8 | 55.950,5 | 14,61 | 29,60 |
| any (§5.4 v1) | 64 | 8 | 170.350,4 | 4,28 | 8,63 |
| any (§5.4 v1) | 64 | 16 | 325.780,7 | 4,33 | 14,03 |
| rr (§5.4 v2) | 1 | 8 | 261.743,9 | 2,62 | 7,86 |
| rr (§5.4 v2) | 1 | 16 | **161.685,0** ↓ | 8,31 | 29,24 |
| rr (§5.4 v2) | 64 | 8 | 345.070,3 | 1,90 | 7,58 |
| rr (§5.4 v2) | 64 | 16 | 487.261,3 | 2,80 | 8,35 |

Tabela completa (1/4 claimers, WAL por linha) no artefato
`mohs-benchmark/docs/performance/e2-e3-claim-shape-results.txt`; planos
em `mohs-benchmark/docs/performance/explain-e2-ready-claim.txt`.

### Vereditos

- **E2 (gate da ADR-A): PASSA com a forma corrigida** — rr/current a 8
  claimers: **2,21×** (sem shard) e **2,91×** (64 shards), critério
  ≥ 1,3×. A forma `ANY(:owned)` foi descartada por **dominância e
  instabilidade de plano**, não pelo kill criterion: com o ownership
  proposto (8/64 shards) ela mediu 1,44× — acima da linha — mas ~2×
  pior que a forma single-shard na mesma célula, e seu plano degenera
  para varredura completa + sort externo conforme a fração de shards no
  predicado cresce (0,47× na variante sem shard, mantida como evidência
  do mecanismo).
- **E3 (gate da ADR-F): sharding NÃO é prematuro** — rr sem shard
  DEGRADA de 8 para 16 claimers (261,7k → 161,7k rows/s, medianas de 3;
  o convoy do `SKIP LOCKED` num índice só), rr com 64 shards segue
  escalando (345,1k → 487,3k). O kill criterion ("não-shardado já
  linear até 16") não se cumpriu.

### Limitações declaradas

- Claim-only: nenhum braço executa/conclui — mede a aquisição, não o
  fim a fim (esse é o E1).
- Os deltas de WAL por linha desta bancada são contaminados por FPI de
  checkpoint (variam 24–685 B/linha entre sweeps idênticos) — **não são
  evidência**; WAL por execução é entregável do E1.
- O colapso da forma `ANY` é comportamento do planner do PG 16 (index
  scan com `= ANY` não preserva ordem; o PG 17 muda isso). A decisão
  single-shard não depende dele — igualdade no prefixo do índice é a
  única forma com scan ordenado garantido nos três dialetos Tier 1/2 —
  mas re-verificar o braço `any` no PG 17 antes de fechar a ADR-A custa
  uma rodada e fica anotado como item da ADR.
- Claimers são virtual threads num só processo/host contra um container
  — mede contenção de banco, não rede entre nós.
- `rows/s` aqui é vazão de CLAIM (linhas adquiridas), não comparável
  com os exec/s fim a fim das seções anteriores.
- Células com 1/4 claimers são rodada única (só as de veredito têm
  mediana de 3) — os valores de 1 claimer variaram até 2,7× entre
  rodadas e não sustentam conclusão nenhuma.

## E1 — replay fim a fim nos dois schemas (Phase 1 do redesign) — 2026-08-22

Experimento E1 do `ARCHITECTURE_REDESIGN_PLAN.md` §20.3 — o gate da
Phase 5 (ADR-A, o split de tabelas do §7). `TableSplitExperimentHarness`
(`mohs-benchmark`), Postgres 16 (Testcontainers), mesmo container por
run: replay do MESMO workload de 500k execuções fim a fim (enqueue →
claim → conclusão), 100 jobs on-demand sem cap, 8 claimers, batch 100,
dispatch ≤ 256, pool Hikari 100, mediana de 3 por braço, warmup de JVM
descartado, `ANALYZE` pós-seed, `max_wal_size` 16GB via `ALTER SYSTEM`
(checkpoint fora da janela; FPI residual reportado por fase). Três
braços: **current** (caminho real — `JdbcExecutionStore.insert/markFired/
complete` + `JdbcClaimer`), **split-sync** (schema §7.2 `e1_ready`/
`e1_lease`/`e1_execution`/`e1_attempt` particionadas, claim single-shard
round-robin do E2 com leitura batched de payload, conclusão em transação
própria — a ADR-A isolada) e **split-group** (idem + group commit §7.6,
flush 256 via `unnest` com a cerca `RETURNING` — o alvo do plano, onde
os kill criteria se avaliam).

### As três rodadas foram refinamento de metodologia, cada delta com causa

- **Rodada 1** (payload `'{}'`): vazão drain 28,8×, WAL −31,6%, tuple
  versions 3,99→1,94. Dois defeitos de medição: payload trivial é o caso
  adversarial para WAL (o PG loga a tupla INTEIRA por versão — as ~3
  versões/exec do current crescem com o payload, a ~1 do split não) e
  ~6% dos contadores do braço group (drain de 3,3s) não tinham chegado
  ao `pg_stat` no snapshot. Artefato:
  `mohs-benchmark/docs/performance/e1-table-split-results-round1.txt`.
- **Rodada 2** (payload 491 B + loop de estabilização de snapshot): fim a
  fim 1,80×, WAL −41,2%. Mas os deltas de tabela do group ainda perdiam
  ~16%: backend idle do pool NÃO descarrega stats pendentes (o
  `pgstat_report_stat` roda antes do sono com throttle de 1s; quem
  termina o último comando e fica idle segura os pendentes para sempre)
  — o loop "convergia" num estado estável-porém-incompleto, e o WAL usa
  o mesmo mecanismo de flush, então o −41,2% estava inflado a favor do
  split. Artefato: `...-round2.txt`.
- **Rodada 3 — registro** (+ `softEvictConnections` antes de cada
  snapshot: backend encerrado descarrega tudo no
  `pgstat_beshutdown_hook`): contadores fecham em 1,000/execução exatos
  nos quatro grupos de tabela. Artefato:
  `mohs-benchmark/docs/performance/e1-table-split-results.txt`.

### Rodada 3 (medianas; payload 491 B)

| braço | fase | s | exec/s | WAL B/exec | WAL rec/exec |
|---|---|---:|---:|---:|---:|
| current | enqueue | 23,7 | 21.056 | 1.100 | 7,04 |
| current | drain | 94,5 | 5.291 | 2.729 | 21,67 |
| split-sync | enqueue | 63,4 | 7.887 | 1.237 | 8,13 |
| split-sync | drain | 64,9 | 7.707 | 1.240 | 13,74 |
| split-group | enqueue | 62,5 | 8.003 | 1.234 | 8,07 |
| split-group | drain | 3,7 | 136.583 | 1.203 | 12,59 |

Deltas de tabela por execução (ciclo completo): current
`mohs_executions` ins 1,00 + upd 3,00 (HOT 0,55); split `e1_execution`
ins 1,00 + upd 1,00 (HOT 0,76-0,79), `e1_ready` e `e1_lease` ins 1,00 +
del 1,00 cada.

### Vereditos

- **Vazão: PASSA.** Fim a fim (enqueue+drain, a leitura do §20.3):
  split-group/current = **1,79×** (critério ≥ 1,5×). Drain isolado:
  **25,8×** (5,3k → 136,6k exec/s — o drain do group não paga commit por
  execução). split-sync sozinho: 1,46× — abaixo da barra.
- **WAL: o kill criterion DISPARA.** Ciclo completo 3.829 → 2.437
  B/exec = **−36,4%**, abaixo da barra de ≥ 40% (split-sync: −35,3%). A
  redução é função do tamanho do payload por construção: −31,6% com
  `'{}'` (rodada 1), −36,4% com 491 B; cresce com payload inline maior
  (o current reescreve a tupla inteira ~3×, o split ~1×) e inverte de
  regime acima de ~2 KB (TOAST tira o payload da tupla — os UPDATEs do
  current param de reescrevê-lo). 491 B é escolha declarada de ponto de
  operação, não constante da natureza.
- **§3.3 (falsificável do plano): CONFIRMADO EXATO.** Tuple versions na
  tabela grande 4,00 → **2,00** (insert + update terminal; falsificaria
  se > 3).
- **Atribuição que a ADR-A deve carregar:** o split SEM group commit não
  passa o E1 (1,46× / −35,3%) — a vazão vem da ADR-C (§7.6), e a Phase 3
  entrega group commit também no schema atual. O que só o split dá:
  −1 KB/exec de WAL, 2 versões exatas na tabela grande (histórico não
  contamina mais o heap do claim), e ser o pré-requisito do sharding que
  o E3 já validou (487k rows/s a 16 claimers).

### Limitações declaradas

- O braço current NÃO paga a cerimônia de leitura do Dispatcher real
  (~1,9 round trips autocommit/exec, Phase 0) — viés a favor do current;
  e roda com pool 100/dispatch 256, abaixo do ponto de operação
  (300/1024). Os 5,3k exec/s de drain aqui não são comparáveis aos
  3,0-4,2k fim a fim do app real.
- No braço group o flush roda no thread do claimer (o design real usa
  fila) — viés contra o split-group.
- Replay de nó único: reaper, retry e renovação de lease não exercitados;
  vazio → cheio → vazio, não regime permanente (o vacuum tail entra só
  parcialmente, dentro da janela de estabilização do snapshot).
- O evict de conexões pré-snapshot faz cada fase recriar o pool dentro da
  janela seguinte (<1% do tempo de fase, simétrico entre braços).
- Decisão de registro: mediana escolhida por vazão de drain; o WAL do
  run mediano é o reportado (todas as rodadas impressas no artefato).
- **Governança do resultado:** rodadas 1→3 foram correção de metodologia
  (payload, depois flush); o −36,4% da rodada 3 é o número honesto e não
  haverá rodada 4 para "recuperar" o veredito.
- **Decisão (2026-08-22): ADR-A mantida, gate revisado.** O −40% era
  previsão pontual de uma grandeza payload-dependente; o gate da ADR-A
  passa a ser o medido: vazão fim a fim ≥ 1,5× (medido 1,79×), exatas 2
  tuple versions na tabela grande (§3.3, medido 2,00) e queda de WAL
  ≥ 30% na faixa de payload inline (medido −31,6% a −36,4%). A ADR-A em
  si nasce com a Phase 5, carregando este resultado e a atribuição acima
  (o ganho de vazão é da ADR-C; o split é o pré-requisito do sharding do
  E3 e da retenção por partição).

## S6/S8 — chaos: kill −9 e pausa de banco (Phase 0 do redesign) — 2026-08-22

Cenários §20.2 do `ARCHITECTURE_REDESIGN_PLAN.md` contra o motor ATUAL —
fecham a validação da Phase 0 (S1 já coberto pelas seções de tuning/write
amplification). `mohs-benchmark/scripts/chaos-recovery.ps1`: app demo no
ponto de operação da rodada 5, Postgres `postgres:latest` (Docker), seed
de 50k por SQL, gatilho a 40% drenado, duas rodadas por cenário,
`lease-ttl` no default (30s), `every-job` com `retries=10`.

### S6 — kill −9 do nó no meio do drain: **PASSA**

| Rodada | Em voo no kill | Re-executadas (attempts>1) | Violações* | Terminais | kill→drain | restart→drain |
|---|---:|---:|---:|---|---:|---:|
| 1 | 392 | 392 | 0 | 50.000 SUCCEEDED | 31,2 s | 26,1 s |
| 2 | 918 | 918 | 0 | 50.000 SUCCEEDED | 31,9 s | 26,4 s |

*multi-attempt que NÃO estava RUNNING no kill (critério: 0).

100% executado; duplicatas exatamente = em voo no kill (cada uma com o
`Attempt` FAILED sintético do reaper + a re-execução, ADR-0033); zero
exceções no log do node2. **O piso da recuperação é o lease TTL**: a onda
de reclaim começa aos +30,0/30,1 s do kill e dura ~1 s — os ENQUEUED
restantes drenam imediatamente no restart; só os em-voo esperam a lease.
É o número que o E6 vai comparar com o node-lease da ADR-B (detecção por
heartbeat de nó, `expires_at` ~5-15 s).

### S8 — pausa do banco por 30 s no meio do drain: **recuperação passa; dois achados**

| Rodada | FAILED terminais | Re-executadas | 1ª conclusão pós-unpause | Conclusões em 10 s | Linhas de exceção |
|---|---:|---:|---:|---:|---:|
| 1 | 18 | 598 | +12 ms | 29.196 | 3.066 |
| 2 | 3 | 486 | +6 ms | 28.972 | 2.446 |

- **Recuperação: passa com folga.** App sobrevive, primeira conclusão
  6-12 ms após o unpause (as conexões congeladas simplesmente continuam —
  `docker pause` é SIGSTOP, os sockets não caem), drain a plena taxa nos
  10 s seguintes. Critério "< 10 s" atendido por 3 ordens de grandeza.
- **ACHADO 1 — "sem perda" falha no espírito: 18/3 execuções queimadas
  terminalmente por falha transiente.** `Engine.failUnreadablePayload`
  trata QUALQUER falha na leitura do payload como terminal por natureza
  (`failBeforeDispatch` → FAILED, `exhausted=false` — decisão testada,
  `EngineTest` RESP-3). Certa para payload ilegível (deserialização,
  classe ausente); errada para `Failed to obtain JDBC Connection` durante
  o soluço — infra transiente virou FAILED no attempt 1 com `retries=10`
  intactos. Viola o espírito do contrato at-least-once. Reportado, não
  corrigido (mudança de comportamento); a classificação
  transiente × permanente é exatamente o que o redesign §4.3/§6 promete.
- **ACHADO 2 — self-reap race: 598/486 re-execuções.** Pausa (30 s) ==
  lease TTL (30 s): as leases dos em-voo expiram DURANTE a pausa e, ao
  acordar, o próprio nó reclama as execuções que ele mesmo está
  concluindo — duplicatas dentro do contrato (at-least-once), mas
  evitáveis; pausa < TTL não exibiria o fenômeno. O guard anti-ABA do CAS
  (ADR-0033) segurou a consistência: nenhuma conclusão dupla, só
  re-execução.
- **Tempestade de exceções: bounded, mas presente.** ~80-100 linhas/s
  durante a pausa, dominadas por `SQLTransientConnectionException`/
  `createTimeoutException` do Hikari (617/490) e
  `CannotCreateTransactionException` (536/460) — o tick continua tentando
  a cada 50 ms contra um pool que não conecta. Sem backoff no poll loop
  (o §5.5 do redesign introduz adaptive poll; ADR-J propõe o circuit
  breaker).

### Limitações declaradas

- Node único; a "recuperação" do S6 usa um segundo processo idêntico no
  mesmo host — não mede rede nem failover entre máquinas.
- O gatilho a 40% e a coincidência pausa == lease TTL são escolhas do
  bench; outras fases do drain / durações de pausa mudam os números de
  duplicatas, não os vereditos.
- Handler trivial: duplicata aqui custa um log; o custo real de
  re-execução é do handler do usuário.

## Phase 3 — group commit + fusões, medido (ADR-0047) — 2026-08-22

A Phase 3 do redesign no schema ATUAL: `fired_at` fundido no CAS do claim
(o `markFired` autocommit morre), leitura de payload em lote por round
(era ~1 round trip autocommit POR EXECUÇÃO) e `CompletionBatcher` — group
commit da conclusão via `completeAll` (256 resultados/5 ms), com opt-out
`mohs.engine.completion-flush-on-every-result`. Bancada: mesma dos
"Tuning fim a fim" (app demo no ponto de operação da rodada 5, Postgres
`postgres:latest`, drains de 50k), MAS com ~1M linhas de história
acumulada das rodadas de chaos/write-amp — o dobro da Phase 0 — e
`max_wal_size` 8GB/`checkpoint_timeout` 30min via ALTER SYSTEM (higiene
de FPI, lição do E1). A/B pela própria propriedade: braço "control" =
mesmo binário com flush síncrono por resultado (JÁ inclui as duas
fusões), braço "batched" = default novo.

### O achado do caminho: o flusher serializado pelo decremento

A primeira rodada do braço batched entregou os commits (0,05/exec) mas
NÃO a vazão (3,4k — igual à Phase 0): o `completeAll` devolvia a vaga de
concorrência com `decrementRunningExecutions` UMA POR EXECUÇÃO — ~256
round trips sequenciais por flush na thread única do flusher, teto
aritmético de ~3-5k conclusões/s. Corrigido no mesmo passo: decremento em
bloco por `job_key` distinto (`CASE` com piso em zero — portável; SQL
Server 2019 não tem `GREATEST`). É o padrão da §1.2 do plano de novo:
o custo escondido não era o commit, era o contador.

### Números (drains de 50k; rodadas na ordem em que rodaram)

| Braço | exec/s por rodada | commits/exec | tuple versions/exec | WAL B/exec (rodadas limpas*) |
|---|---|---:|---:|---:|
| batched, flusher serial | 3.372 · 2.869 | 0,050-0,054 | 4,2-4,3 | — (FPI contaminado) |
| **batched, corrigido** | 4.942 · 6.401 · 7.253 · 3.879 (mediana ~5,7k) | **0,037-0,048** | 3,2-3,4 | **2.337 · 2.880** |
| control (fusões, sem batcher) | 4.696 · 4.748 | 1,96-2,03 | 3,2 | 2.193 |

*rodadas com FPI < 0,1/exec; as demais pegaram checkpoint/vacuum no meio
(variância do braço batched, 3,9-7,3k, é vacuum de ~120k dead tuples por
drain interferindo — o control, mais lento, oscila menos).

### Vereditos dos gates da fase

- **commits/execução ≤ 1,5: PASSA por 30×** — 0,037-0,054 engine-side
  (era ~3,9 na Phase 0; o enqueue segue +1 por construção). A decomposição
  do control mostra o resto: 2,0/exec = a transação de conclusão + a
  cerimônia `SHOW TRANSACTION ISOLATION LEVEL` do pgjdbc por transação.
- **S1 ≥ 1,8×: na linha, com atribuição dividida.** Contra a régua da
  Phase 0 (3,0-3,3k, com METADE da história): mediana batched ~5,7k =
  **1,7-1,9×** — e o viés da história dobrada joga contra. O A/B
  mesmo-dia divide o mérito: as fusões sozinhas (control) fazem 4,7k
  (~1,45× da Phase 0); o group commit adiciona 1,05-1,36×. O motor
  deixou de ser commit-bound: com dispatch 1024, o Postgres já agrupava
  os WALWrites concorrentes — o teto novo (~5-7k) é o tick serial
  (renovação + claim + payload na mesma thread), exatamente a §1.3 do
  plano, que é problema da Phase 5/6.
- **E5 (duplicatas sob kill −9, batcher ON): nenhuma exposição além do
  em-voo.** S6 re-rodado no motor novo: 314 RUNNING no kill → exatamente
  314 re-execuções, 0 violações, 50k/50k SUCCEEDED, recuperação
  kill→drain 31,5s (piso do lease TTL, como antes). O que está na fila
  do batcher ainda é RUNNING no banco — já pertence ao conjunto em voo
  que o contrato aceita re-executar; o kill criterion do E5 ("duplicatas
  pior que linear no flush") não se manifesta.
- **Bônus de resiliência:** a falha TRANSIENTE na carga de payload deixou
  de ser terminal (o lote fica RUNNING pro reaper) — o braço transiente
  do achado do S8 morre por construção; payload ilegível por linha segue
  terminal, semântica preservada.

### Limitações declaradas

- História ~1M e autovacuum ativo tornam as rodadas ±30% — as medianas e
  o A/B same-day carregam o veredito, não uma rodada isolada.
- Handler trivial: o ganho do group commit cresce com handler curto e
  I/O-bound; com handler lento o commit nunca foi o gargalo.
- WAL bytes/exec não muda com o batcher (esperado: group commit amortiza
  fsync/commits, não bytes); os ~2,2-2,9 KB/exec seguem sendo o alvo da
  ADR-A (E1 mediu o split em −36,4%).
- `SHOW TRANSACTION ISOLATION LEVEL` (~1 xact contado por transação real)
  é cerimônia do driver — alavanca separada, não perseguida aqui.

## Phase 4 — node lease + fence de posse, medido (ADR-0051) — 2026-08-22

A Phase 4 do redesign: renovação de lease por execução deletada, liveness
por nó (`mohs_nodes.epoch`/`expires_at`, heartbeat 1×/tick,
`node-lease-ttl` 15s), reaper dead-node driven com tick
heartbeat-antes-do-reaper, e todo CAS de conclusão cercado pela posse da
encarnação (`node_id`, `fired_at`). Bancada: mesma do "Tuning fim a fim"
(app demo, `poll=50ms`, `batch=1000`, `dispatch=1024`, Hikari 300,
Postgres `postgres:latest`), workload **slow-job** (`Thread.sleep(500)`,
`retries=10`) — o cenário renewal-heavy que a Phase 0 deixou como
pendência: com 1.024 em voo sustentados, a renovação era o maior
componente de update da tabela quente. Medição ad-hoc por
`pg_stat_user_tables`/`pg_stat_wal` (mesmo par de snapshots do BEFORE,
tomado no mesmo dia com o engine pré-Phase-4).

### Write amplification (o gate do plano)

| Métrica (por execução) | BEFORE 20k | BEFORE 50k | AFTER 20k | AFTER 50k ×3 |
|---|---:|---:|---:|---:|
| updates em `mohs_executions` | 6,67 | 6,97 | **2,00** | **2,00** (nas 3 rodadas) |
| — dos quais HOT | 0,00 | 0,00 | 0,00 | 0,00 |
| WAL bytes | 8.410 | 4.753 | 5.472* | 2.112–4.564* |
| vazão (exec/s) | 1.465 | 1.525 | 1.748 | 921–1.829** |

*WAL contaminado por checkpoint/FPI conforme a rodada; a leitura limpa é
~2,1 KB/exec (rodada pós-VACUUM FULL). **A dispersão de vazão é
checkpoint/vacuum, não o mecanismo: a rodada quente fez 1.829/s com o
tanque cravado em 1.024 (curva amostrada) — acima do BEFORE; `nodes_upd`
≈ 1 por tick, como projetado.

- **`n_tup_upd`: 6,67–6,97 → 2,00 = −70,0% a −71,3%.** O gate do plano
  dizia ≥ 80% assumindo ~10 upd/exec de renovação sustentada; o BEFORE
  real era ~6,8 (≈ 4,8 de renovação + 2 de claim/CAS). A renovação foi a
  **zero** — o −80% literal era inalcançável porque os 2,00 restantes
  (claim + CAS terminal) nunca foram alvo da Phase 4: são o piso que a
  Phase 5 ataca. Registrado como gate de mecanismo atendido, gate
  numérico literal não (70% < 80%).
- A query nova do reaper (anti-join com `mohs_nodes`) custa **1,8 ms**
  com 1.024 RUNNING e 1,5M linhas (EXPLAIN ANALYZE no meio do drain,
  `idx_mohs_executions_owner` da migração V2) — de graça no tick.

### E6 — chaos, o gate de corretude (S6/SUSPEND/S8, `node-lease-ttl` 15s)

- **S6 (kill −9 a 40% do drain de 50k): PASSA.** 100% terminal; 2.048
  re-executadas = exatamente o em-voo no kill; violações 0; reclaim wave
  aos **+15,1 s** do kill (era +30 s com a lease de execução);
  **kill→drain 17,1 s** (critério do plano: recovery < 20 s — antes
  31,2–31,9 s); zero exceções.
- **SUSPEND (NtSuspendProcess 25 s > TTL, 2 nós): fence PASSA.** Na
  variante com o node2 ainda ativo no vencimento: 1.024 reclaims
  disparam exatamente no TTL, re-runs completam ~2 s depois, e ao
  retomar o node1 seus zumbis perdem TODOS os CAS cercados — **0
  conclusões duplas** em todas as rodadas; epoch bump WARN no node1.
  **Achado (pré-existente, não regressão):** freeze no MEIO da transação
  de claim deixa o lote travado-mas-ENQUEUED (uncommitted) — invisível
  ao reaper e pulado pelo SKIP LOCKED de outros nós até a sessão
  congelada morrer (confirmado por `pg_stat_activity`: backend
  `idle in transaction` no UPDATE do claim). O engine da renovação tinha
  o mesmo buraco. Mitigação DB-side (`idle_in_transaction_session_timeout`)
  reportada, não implementada.
- **S8 (docker pause 30 s — 2× o TTL): PASSA, self-reap MORTO.**
  Re-executadas **0** (Phase 0: 486–598 com pausa == TTL) — a ordem
  heartbeat-antes-do-reaper faz o reaper de cada tick enxergar a própria
  promessa recém-renovada; app sobrevive, 1ª conclusão no unpause+0 ms,
  27,6k conclusões nos 10 s seguintes, zero linhas de exceção no log
  (a tempestade da Phase 0 também sumiu deste cenário).

### Limitações declaradas

- Vazão slow-job é teto aritmético (`dispatch/0,5s` = 2.048/s) — as
  rodadas medem write amplification, não capacidade.
- O SUSPEND com gatilho a 40% cai com frequência no modo freeze-mid-claim
  (achado acima) — o critério "reclaims > 0" do script só vale quando o
  freeze pega o node fora do claim; a variante instrumentada é que provou
  o fence sob reclaim real.
- Um host, dois processos; sem partição de rede real.

## Phase 5 — the table split, medido (ADR-0052/0053) — 2026-08-22

A Phase 5 do redesign completa (S5.1–S5.4 commitados): o hot path saiu da
história — `mohs_ready` (fila, INSERT/DELETE), `mohs_lease` (posse,
fence `(node_id, epoch)`, INSERT/DELETE), `mohs_execution`/`mohs_attempt`
(história particionada semanal no PG; INSERT + um UPDATE advisory
terminal), `mohs_idempotency` (dedup por PK). Bancada: mesma máquina do
"Tuning fim a fim", Postgres em Docker, app demo no host. **Ponto de
operação novo**: `poll=20ms`, `batch=1000`, `claim-rounds=2` (ADR-0040,
ligada pela primeira vez num bench), `dispatch-concurrency=1024`,
`event-concurrency=256`, Hikari 300. Medição pelo
`write-amplification.ps1` portado pro split (stats somados sobre as
partições; seed = unidade de enqueue do §7.5-1).

### Os três gates da fase (plano §21)

| Gate | Critério | Medido | Veredito |
|---|---|---|---|
| S1 vazão | ≥ 12 k/s | **12,2–14,5 k/s** em 10 rodadas quentes de 3 sessões (12,2/12,3/12,4/12,6 · 13,3/13,9/14,1 · 14,0/14,5/14,2) — ver a limitação de dispersão entre sessões abaixo | ✅ |
| Tuple versions/execução (história) | = 2 | **2,000** (1 INSERT + 1 UPDATE advisory; média das rodadas — o corte da janela desloca conclusões em trânsito entre rodadas, ±0,1 por rodada, soma exata; estável em TODAS as sessões, degradadas incluídas) | ✅ |
| S5 — história não afeta claim | flat | par A/B limpo, MESMO binário e mesma sessão: **7,6–7,9k @ história ~0 ≈ 7,2–7,7k @ 2M** (flat dentro do ruído); o statement de claim só referencia `mohs_ready`/`mohs_lease` por construção | ✅ |

Contexto: a mesma bancada fazia ~4,0–4,2k/s na era da tabela única
(ponto de operação 50ms) e ~5,7k na Phase 3. No ponto de operação
antigo (50ms) o split faz 5,8k — o teto ali é o tick serial (achado da
Phase 3), não a escrita; `poll=20ms` multiplica os ticks e o split
sustenta **2,4× a Phase 3, 3,3× a era da tabela única** nas sessões de
pico.

Demais números por execução (rodadas quentes): commits 0,042–0,054 ·
WAL ~2,2–2,4 KB (payload `'{}'`) · `ready_ins/del`, `lease_ins/del`,
`att_ins` todos = 1,000 · `exec_hot` ~0,12 (HOT no UPDATE advisory).
`nodes_upd` ≈ 1/tick. E6 re-rodado sobre o split no S5.3/S5.4: S6
kill→drain 16,8s, SUSPEND 0 duplo-SUCCEEDED em 244 reclaims, S8 0
re-execuções.

### O achado do S5.5: o reconcile precisava de grace

A primeira rodada a 12k+/s expôs uma calibração errada do passe de
reconciliação de leases órfãs (nascido no review do S5.3): a premissa
"ausente do mapa em 2 ticks = órfã" quebra a alta vazão, porque SEMPRE
há dezenas de conclusões em trânsito no batcher entre o retorno do
dispatch e o commit do flush. Sintomas medidos: 199k WARNs "requeued 0",
requeues fantasma em blocos de 256/512 disputando lock com o flush até
**23 deadlocks**, e 10,7k conclusões descartadas pelo fence num round
frio (JIT atrasando o flusher além de 500ms). Fix em três camadas
(review do S5.5): candidata exige `claimed_at` mais velho que
`max(2s, 4×poll)`, consulta ao trânsito REAL do batcher
(`completionInTransit` — guard por estado, cobre job que roda mais que o
grace) e requeue com ordem canônica de locks (a mesma dos DELETEs do
flush — mata o AB-BA dos 23 deadlocks pela raiz). Depois do fix: zero
fantasmas/fences/deadlocks em todas as rodadas, frias incluídas, e a
sessão do fix mediu 14,0–14,5k contra 12,2–12,6k da sessão anterior na
mesma história (~+14% — sessões distintas, ver limitação abaixo; a
eliminação do churn é o mecanismo, o percentual carrega variância de
sessão). Recuperação de órfã legítima: grace + 2 rodadas (~2s no poll
de 20ms; ~30s no default de 5s — paridade com o lease-ttl da era
por-execução no pior caso, ordens de magnitude melhor com poll curto).

### Limitações declaradas

- **Dispersão ENTRE sessões maior que a variância intra-sessão**: horas
  depois das sessões de pico, a MESMA configuração mediu 7,2–8,2k — e o
  A/B decisivo (binário pré-fix vs pós-fix no mesmo estado da bancada)
  deu ~7k nos DOIS, provando que a queda é do host (Docker/WSL após
  horas de builds Testcontainers e ~15 boots de app), não do código.
  Bloat de índice da fila foi investigado e descartado (REINDEX zerou
  PKs de 11–13MB SEM recuperar a vazão). Intra-sessão a variância é
  ±10%; entre sessões chegou a ~45%. O gate S1 registra as rodadas ≥12k
  de duas sessões independentes; bancada dedicada/controlada fica como
  pendência de infraestrutura de bench.
- Um nó; payload trivial `'{}'`; Docker local; round frio (JIT)
  excluído das medianas e reportado à parte (1,0–5,1k).
- 410 eventos `Started` dropados por rodada de 50k com
  `event-concurrency=256` (publisher best-effort saturado) — nenhum
  `BatchCompleted` dropado nesta bancada; observação, não gate.
- O corte da janela de medição desloca ins/upd de conclusões em trânsito
  entre rodadas — os 2,000 exatos aparecem na média/soma, não em cada
  rodada isolada.

## Phase 6 — S6.1–S6.3 A/B e a conflação do NOTIFY (P1) — 2026-08-22

A/B de binários na MESMA sessão, alternados (metodologia do S5.5):
HEAD pós-S6.3 (`2fe9f08`) vs Phase 5 (`a6d9956`), mesmo ponto de
operação (`poll=20ms`, `batch=1000`, `claim-rounds=2`, `dispatch=1024`,
eventos 256, Hikari 300), Postgres 18.4 em Docker. Seed do drain FIEL a
cada binário: uniforme (`n % 64`) pro shardado, shard 0 pro pré-shard —
cada um com a distribuição que os próprios escritores dele produzem
(`write-amplification.ps1 -ShardMode`).

| Métrica | Phase 5 | S6.3 (pg_notify por enqueue) | S6.3 + P1 (conflação global) |
|---|---:|---:|---:|
| Drain 50k (exec/s, rodadas quentes) | 12,0 · 12,8 · 12,5 k | 12,9 · 13,1 k | 11,1 · 12,2 k |
| REST 10k wall (`load-test.ps1`, 1000 conexões) | 14,3 · 15,3 s | **29,0 · 29,2 s** | **15,9 · 15,1 · 14,8 s** |
| Latência média do POST | 7,7 · 18,7 ms | **1.281 · 1.514 ms** | 9 · 10 · 17,9 ms |
| commits/execução (drain) | 0,043–0,049 | 0,079–0,090 | 0,084–0,098 |

Leituras:
- **O drain (processamento) NÃO regrediu com o sharding** — o lap de
  probes single-shard mantém 11–13k/s no nó único com seed uniforme; o
  custo visível é commits/execução ~2× (uma transação por sonda em vez
  de uma por round), sem efeito na vazão neste ponto de operação.
- **O pg_notify por enqueue serializou o ingest** exatamente como o
  microbench previu (~500 notificantes/s de teto): wall 2×, latência
  ~170× — foi a regressão percebida em uso real. Causa: transação
  notificante não participa de group commit (lock global do notify
  queue seguro através do flush — `PreCommit_Notify`).
- **P1 (conflação global por emissor, janela de 50ms)** devolveu wall e
  latência ao baseline: ≤ ~20 commits notificantes/s por nó emissor,
  cada um carregando a bitmask de shards acumulada; a primeira versão
  (janela POR shard, 250ms) ficou no meio do caminho (wall 22s — ~52%
  dos commits ainda notificantes num burst uniforme) e foi substituída.
- Sinal conflacionado é entregue pelo próximo vencedor da janela; cauda
  de burst sem tráfego seguinte fica pro poll adaptativo — o best-effort
  contratado pela ADR-G, agora com o custo do caminho notificante
  limitado por construção.

## Phase 6 — S6.4: os gates da fase, medidos — 2026-08-23

Bancada: Postgres 18.4 em Docker (Rancher recém-reiniciado, ritual da
decisão 7 do PLAN.md), host de 24 threads/32 GB, todos os nós como
processos JVM no MESMO host. Config **por nó** constante em toda a
matriz (`poll=25ms`, `max-poll=2s`, `batch=1000`, `dispatch=1024`,
eventos 256, Hikari **100** — 4×100 cabe no `max_connections=500` do
container; o ponto de operação das fases anteriores usava 300 num nó só,
então o número absoluto de 1 nó aqui é MENOR que o do S6.1–S6.3 de
propósito: o que esta seção mede é a razão entre células, não o teto).
Script: `mohs-benchmark/scripts/cluster-scale.ps1`.

### Gate 1 — taxa de consulta com o cluster OCIOSO: **NÃO PASSA**

Ocioso de verdade: todas as definições pausadas (o `every-job` PT1S do
demo mediria outra coisa), 20 s de espera pro backoff estacionar no teto,
janela de 60 s, contagem por `pg_stat_statements`.

| Nós | Período do tick | Consultas/s (cluster) | Consultas/s (por nó) | Transações/s |
|---:|---:|---:|---:|---:|
| 1 | 2,07 s | **96,0** | 96,0 | 65,8 |
| 4 | 2,01 s | **108,8** | 27,2 | 79,3 |

Gate do plano (§21): < 10/s. Erra por ~10×. A atribuição diz exatamente
onde, e não é ambígua:

- **O lap de sondas é 96% do custo ocioso.** 1 nó: 1.856 sondas em 29
  ticks = 64 por tick — o lap inteiro, como projetado. 4 nós: 1.909 em
  120 ticks = 16 por tick por nó. Ou seja, o lap custa **64 statements
  por tick de cluster, independentemente do número de nós** — o custo
  ocioso do claim é constante em N, e é a manutenção (3,4 consultas/s por
  nó) que cresce linearmente.
- **Cada sonda são 3 round trips, não 1**: `BEGIN`, `SHOW TRANSACTION
  ISOLATION LEVEL` e a CTE do claim (1.856 chamadas de cada, exatamente).
  O `SHOW` é consequência do `setIsolationLevel(READ_COMMITTED)`
  explícito em `JdbcWorkQueue` (DBTUNE-4, para o MySQL, que é RR por
  default): quando a `TransactionDefinition` pede isolação explícita, o
  Spring lê a isolação corrente da conexão e o pgjdbc responde com um
  round trip — em Postgres, cujo default JÁ é READ COMMITTED, é um terço
  do tráfego ocioso do claim gasto para confirmar o que já valia.
- A ADR-G prometeu "cluster ocioso de 10 nós: 200 consultas/s → ~5". Essa
  conta era **um statement de claim por tick**; o lap multiplicou o termo
  do claim por 64. Nenhum outro termo da conta mudou nesta fase.

### Gate 2 — latência de dispatch num cluster ocioso: **NÃO PASSA**

Uma execução on-demand por vez pelo REST do nó 0, espaçadas 7 s (25 ms
dobrando até 2 s leva ~5 s: menos que isso e a sonda mede o loop ainda
acelerado pela sonda anterior). Latência = `scheduled_at → started_at`.

| Nós | p50 | p95 | máx | n |
|---:|---:|---:|---:|---:|
| 1 | **25,3 ms** | 59,8 ms | 65,5 ms | 15 |
| 4 | **461,3 ms** | 1.649 ms | 1.852 ms | 20 |

Gate do plano: p50 < 5 ms. A atribuição por nó despachante, com 4 nós,
mostra o mecanismo inteiro em duas linhas:

| Nó | Despachou | p50 |
|---|---:|---:|
| o que recebeu o POST | 6 de 20 | **25,2 ms** |
| os outros três | 14 de 20 | 504 · 612 · 844 ms |

O hand-off local (tier 1) entrega os mesmos ~25 ms do nó único — mas só
quando quem recebeu o enqueue é o dono do shard sorteado, o que é 1/N por
construção. Os outros N−1/N esperam o poll do dono: uniforme em [0,
`max-poll-interval`]. **É o preço da retirada do NOTIFY (ADR-0054),
agora com número:** num cluster ocioso de 4 nós, 70% dos enqueues pagam
meio segundo de p50 e até 1,85 s de cauda. Sob carga o efeito some (o
backoff fica no piso de 25 ms), e é por isso que a retirada continua
certa para quem tem tráfego — o custo é exatamente o cenário ocioso
multi-nó que a ADR-0054 declarou não ter dono.

### Gate 3 — escala relativa multi-processo: **PASSA, sublinear**

Drains de 50k, shard uniforme, 4 rodadas por célula (a 1ª é warmup e sai
do registro), `TRUNCATE` da história entre células e ordem palindrômica
(1-2-4-4-2-1) pra que a deriva de sessão atinja as duas passadas
igualmente. Mediana das 3 rodadas quentes:

| Nós | Passada A | Passada B | Mediana | Escala |
|---:|---:|---:|---:|---:|
| 1 | 6.717 | 6.400 | **6,6k/s** | 1,00× |
| 2 | 8.962 | 9.019 | **9,0k/s** | **1,37×** |
| 4 | 14.898 | 15.176 | **15,0k/s** | **2,29×** |

As duas passadas concordam dentro de ~5% por célula — a ordem
palindrômica e a espera pelos backends fecharem (abaixo) foi o que
estabilizou. Uma matriz anterior da mesma sessão, com a bancada ainda
mais fria, deu 9,0k / 18,0k / 24,9k (2,00× / 2,76×): a razão é sempre
positiva e crescente, o valor absoluto é o que a bancada não segura.
Com seed de 200k, 4 nós fazem **22,8k/s**.

Atribuição do teto, amostrada durante o drain de 4 nós: CPU do host ≤
44% de 24 threads (não é o host), e os waits do Postgres são
`LWLock:WALWrite` + `IO:WalSync` em toda a janela ativa — **o teto é o
fsync do WAL de uma instância única**, a mesma parede das Phases 3 e 5.
O sharding entregou o que a ADR-F prometeu (o claim deixou de ser o
gargalo); o que impede a linearidade não está no engine.

### Gate 4 — E6 (chaos) no binário shardado: **PASSA**

Re-rodado inteiro sobre o sharding + loop adaptativo, `node-lease-ttl`
15 s, seed de 50k, gatilho em 40% drenado
(`chaos-recovery.ps1`):

| Cenário | Resultado |
|---|---|
| **S6** (kill −9) | 50.000 terminais; re-execuções 827 = EXATAMENTE as RUNNING no kill; 0 fora disso; kill→fim 19,6 s (onda de reclaim 15,4 s após o kill — o piso do lease); 0 linhas de exceção |
| **SUSPEND** | 50.000 terminais; 244 reclamadas enquanto congelado; **0 dupla-conclusão** (o fence de posse descartou todo zumbi); 1 WARN de bump de epoch no resume; resume→fim 14,6 s |
| **S8** (pausa de 30 s do banco) | 50.000 terminais; **0 re-execuções**; 1ª conclusão 259 ms após o unpause; 24.109 conclusões nos 10 s seguintes; 0 exceções |

O requeue com shard RE-DERIVADO (decisão 1 do PLAN.md) sobrevive ao
chaos: nada apodreceu numa partição que ninguém sonda.

### Achados de bancada (metodologia, não do produto)

- **Matar o processo não fecha os backends do Postgres na hora.** Uma
  célula de 4 nós deixa até 400 conexões morrendo; a célula seguinte
  pedia as suas por cima do `max_connections=500` e saía degradada — uma
  passada inteira da matriz (2 nós a 10k contra 18k) foi isso, não o
  produto. O script agora espera os backends drenarem entre células.
- **A história retida cobra o drain.** Sem `TRUNCATE` entre células, a
  última célula da matriz mede uma base maior que a primeira e a
  comparação vira ordem de execução.

### Limitações declaradas

- Todos os "nós" são processos no MESMO host, contra a MESMA instância
  de Postgres: o experimento mede partição de trabalho e contenção de
  banco, **não** rede, nem NUMA, nem falha de zona. A escala 6× do gate
  S2 do plano continua lastreada só no E3 (micro, 64 shards, 8
  claimers) — nenhuma bancada aqui a alcança.
- Hikari 100 por nó (não os 300 do ponto de operação das fases
  anteriores) para caber 4 nós no `max_connections`: o absoluto de 1 nó
  aqui não é comparável ao das seções anteriores, só as razões entre
  células desta.
- Dispersão entre sessões continua real (~35% entre a primeira matriz e
  a palindrômica, na mesma hora, com a mesma config). O ritual da
  decisão 7 estabilizou a comparação DENTRO da sessão, não entre elas.
- A latência de dispatch foi medida com n=15/20 — o suficiente pra p50 e
  pra atribuição por nó, não pra uma cauda p99.


## Phase 6 — S6.5: o gate ocioso, medido A/B — 2026-08-23

Mesma bancada e mesma sessão do S6.4 (script `cluster-scale.ps1`), com
A/B de binários ALTERNADO — o pré-S6.5 foi reconstruído e re-medido
depois do pós, porque a sessão já tinha derivado ~20% e comparar contra
os números da manhã seria comparar horários, não código.

A mudança: enquanto a rodada de claim anterior voltou vazia, o tick
pergunta UMA vez `SELECT CASE WHEN EXISTS (… shard IN (próprios) AND
visible_at <= now)` em vez de dar o lap de 64 statements. Achou trabalho,
o lap roda no MESMO tick.

### O ocioso: 96 → 4,0 consultas/s por nó

| Nós | Antes (S6.4) | Depois (S6.5) | |
|---:|---:|---:|---|
| 1 | 96,0/s | **4,02/s** | 24× menos |
| 4 (cluster) | 108,8/s | **15,99/s** | 6,8× menos |
| 4 (por nó) | 27,2/s | **4,00/s** | |

A atribuição depois é oito statements por tick, todos a 0,50/s com o
backoff no teto: o heartbeat, `SELECT * FROM mohs_nodes`, as duas
varreduras de lease, as duas de definições, o purge de nós mortos — e
**um** probe. O termo que a ADR-G projetava ("cluster ocioso de 10 nós:
~5 consultas/s") era exatamente o do claim, e ele agora é 0,5/s por nó:
5/s em 10 nós, na mosca.

O que sobra é o tick de manutenção — 7 statements por tick por nó, ~3,5
consultas/s, **anterior à Phase 6 e intocado por ela**. É o que faz um
cluster de 10 nós ociosos extrapolar para ~40 consultas/s em vez dos <10
do gate literal do §21. Registrado como pendência própria no PLAN.md; não
é dívida do sharding nem do poll adaptativo.

Custo do probe: 0,006–0,010 ms de média server-side (`pg_stat_statements`)
— dentro do ruído de um round trip.

### O plano da sonda (Postgres 18.4, `mohs_ready` com 200k entradas)

O caso que importa não é a fila cheia — é a fila cheia **dos outros**:
os 16 shards deste nó vazios, 150k entradas nos shards alheios. O
`EXISTS` é um Index Only Scan sobre `idx_mohs_ready_claim`
`(shard, priority, visible_at)`:

| Estado | Plano | Tempo | Buffers |
|---|---|---:|---:|
| 200k na fila, shards próprios COM trabalho | Seq Scan curto-circuitado no 1º acerto | 0,025 ms | 2 |
| 150k na fila, shards próprios VAZIOS, pós-`VACUUM` | Index Only Scan, `Heap Fetches: 0` | 0,031 ms | 11 |
| idem, com 50k tuplas MORTAS nos shards próprios | Index Only Scan, `Heap Fetches: 50000` | 10,45 ms | 38.607 |

A terceira linha é a propriedade da tabela sob churn, não da sonda: como
`visible_at` não é prefixo do índice depois de `shard` (a prioridade está
no meio), provar a AUSÊNCIA percorre as entradas de índice dos shards
próprios — e antes do `VACUUM` cada uma custa uma visita à heap. É
**one-shot**, não por tick: repetindo o `EXPLAIN` na mesma sessão, o
`kill_prior_tuple` marca as entradas como `LP_DEAD` e a 2ª execução já
cai para `Heap Fetches: 0`, 61 buffers, 0,118 ms — 42× menos, sem
`VACUUM` nenhum.

### O estado que estas três linhas NÃO cobrem, e onde a sonda perde

Os três planos acima são **custom** (`EXPLAIN` ad-hoc com literais).
Produção roda o plano **genérico**: o pgjdbc server-prepara a partir da
5ª execução (`prepareThreshold=5`, sem override no repositório) e a sonda
executa uma vez por tick — `pg_prepared_statements` confirma a migração
(`generic_plans=7, custom_plans=5`). No genérico, `visible_at <= $N` não
tem histograma, o planner aplica `DEFAULT_INEQ_SEL = 1/3` e o Seq Scan
fast-start vence. Com backlog **não-visível** (retries em backoff,
`at`/`delay` no futuro) e 64 shards:

| Backlog não-visível | Plano | Buffers | Tempo |
|---:|---|---:|---:|
| 1.000 | Index Only Scan | 1 | 0,008 ms |
| 10.000 | **Seq Scan** | 121 | 0,29 ms |
| 200.000 | **Seq Scan** | 2.410 | 5,53 ms |
| 1.000.000 | **Seq Scan** | 12.049 | 27,5 ms |

**Neste estado o lap que a sonda substituiu é mais barato**: `shard = :shard`
é igualdade única e o `ORDER BY priority, visible_at LIMIT` ancora o
índice — 6 buffers por shard, **384 buffers e ~0,4 ms pelo lap inteiro de
64**, contra 12.049 buffers e 27,5 ms da sonda. 31× mais buffers, ~70×
mais CPU de banco. Há ainda um penhasco de plano entre 16 e 24 shards no
`IN`: **≥4 nós ficam no índice; 1 ou 2 nós caem no Seq Scan** — o pior
caso é o deployment mais simples.

Não é falta de índice: `(shard, visible_at)` foi medido e o plano não
muda (o planner não escolhe Seq Scan por falta de caminho, e sim por
estimar 1/3 das linhas), então nenhuma migração entrou. Também não é a
falta de `ORDER BY`: a variante `… AND visible_at <= $65 ORDER BY shard,
priority, visible_at LIMIT 1`, que ancoraria o índice sem literal, foi
medida e dá o **plano idêntico** — 6.025 buffers, 13,2 ms contra 13,4 ms
do original, com 500k não-visíveis. O Postgres descarta a ordenação
dentro de `EXISTS`, onde ela é semanticamente irrelevante. A raiz é o
BIND de `:now` — com o instante como literal o plano volta a Index Only
Scan (321 buffers, 0,074 ms), ao custo de 0,29 ms de planejamento por
chamada.

Ordem de grandeza do dano nos defaults: 27,5 ms a cada 2 s = **1,4% de
uma thread** e ~6.000 buffer hits/s. Tolerável, e é por isso que a troca
fica de pé. Vira sério com `max-poll-interval == poll-interval` (o
formato pré-Phase-6, suportado): a 25 ms fixos com 500k de backlog são
13,4 ms por tick — 54% do orçamento. Pendência com gatilho no PLAN.md.

**O A/B abaixo mediu a fila VAZIA**, que é onde a sonda ganha 24×. O
estado de backlog não-visível não foi medido fim a fim — só por plano.

### O que NÃO mudou (o ponto do A/B)

| Métrica | Pré-S6.5 | Pós-S6.5 |
|---|---:|---:|
| Latência de dispatch, 1 nó ocioso (p50, n=30) | 41,1 ms | 35,3 ms |
| idem (p95) | 54,9 ms | 58,4 ms |
| Drain 50k, 1 nó (rodadas quentes) | 12,2–12,7 k/s | 12,3–12,6 k/s |
| Drain 50k, 4 nós (mesma janela de sessão) | 20,0 · 22,3 · 20,3 k/s | 19,3 · 24,3 · 20,7 · 21,3 k/s |

A latência **não** regrediu: as duas medições estão dentro da dispersão
da bancada, e a n=15 da manhã (p50 25,3 ms) mostrou que com 15 amostras a
mediana desta distribuição é instável — por isso o A/B usou n=30 dos dois
lados. A vazão não regrediu. A explicação que eu tinha escrito aqui — "por
construção o probe não roda sob carga, porque todo tick de um drain
reivindica" — é forte demais: `claimLaps` devolve zero também com a folga
de dispatch esgotada ou o orçamento de tempo estourado, e nesses casos a
flag armaria. O S6.5 fechou isso no código (só uma volta COMPLETA e vazia
arma o gate), mas o registro honesto do A/B é o medido: **nesta bancada,
com este workload, o probe não apareceu no caminho quente**.

### E6 re-rodado no binário do S6.5: passa

S6: 50.000 terminais, 286 re-execuções = exatamente as RUNNING no kill, 0
fora disso. SUSPEND: 61 reclamadas enquanto congelado, **0
dupla-conclusão**, 1 bump de epoch. S8: **0 re-execuções**, 1ª conclusão
196 ms após o unpause, 27.002 conclusões nos 10 s seguintes. Zero linhas
de exceção nos três.

### Limitações declaradas

- A sessão derivou ~20% entre a matriz da manhã (4 nós a ~25,6k) e a da
  tarde (~20,5k nos DOIS binários). Todo par A/B aqui foi medido em
  janelas adjacentes por isso; nenhum número desta seção deve ser
  comparado com os absolutos da seção S6.4.
- Uma célula da repetição de 4 nós colapsou para 1,6–8 k/s (parada de
  ambiente, não do código) e foi descartada — a repetição seguinte, sem
  nenhuma mudança, voltou a ~21k.

## Como reproduzir

Os harnesses da era da tabela única (TableSplitExperiment, ClaimQueryLoad/
Explain, ClaimIndexTuning, OverviewQueryExplain, FindPageQueryExplain,
Liveness, RateLimitContention, InVsJoin, BatchCounter*) **caíram no S5.4**
junto com as tabelas que mediam — os resultados deles estão registrados
nas seções acima e o código vive no histórico do git (até o commit
ac20c28). Harnesses do modelo novo nascem quando um gatilho registrado no
PLAN.md disparar.

Write amplification fim a fim (commits/execução, tuple versions, WAL —
portado pro split no S5.5, stats somados sobre as partições):
`mohs-benchmark/scripts/write-amplification.ps1`, com o app de demo no
ar (boot manual: `java -cp "target/classes;$(cat target/cp.txt)"
io.mohs.MohsApplication` + overrides de datasource/engine — ponto de
operação da Phase 5 na seção acima); `-JobKey slow-job` para o workload
renewal-heavy da Phase 4.

Gates da Phase 6 — ocioso, latência de dispatch e escala relativa (o
script sobe e derruba os N nós sozinho, portas 8080+; `pwsh`):

```
pwsh mohs-benchmark/scripts/cluster-scale.ps1 -Mode Idle    -Nodes 4
pwsh mohs-benchmark/scripts/cluster-scale.ps1 -Mode Latency -Nodes 4 -Reset
foreach ($n in 1,2,4,4,2,1) {
  pwsh mohs-benchmark/scripts/cluster-scale.ps1 -Mode Drain -Nodes $n -Rounds 4 -Reset
}
```

`-Reset` trunca fila e história entre células (sem ele a última mede uma
base maior que a primeira); a ordem palindrômica é o que neutraliza a
deriva de sessão.

Chaos S6/S8/SUSPEND (portado pro split no S5.3; o script sobe e
mata/congela o app sozinho; portas 8080/8081 livres; rodar com `pwsh`,
não Windows PowerShell 5.1):
`mohs-benchmark/scripts/chaos-recovery.ps1 -Scenario S6` (ou `S8`,
`SUSPEND`).

Requer Docker local (Testcontainers sobe Postgres/MySQL/SQL Server).
