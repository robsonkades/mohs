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

## Como reproduzir

```
mvn test -Dtest=ClaimQueryLoadHarness
mvn test -Dtest=ClaimQueryExplainHarness
mvn test -Dtest=ClaimIndexTuningHarness#postgres
mvn test -Dtest=ClaimIndexTuningHarness#sqlServer
```

Requer Docker local (Testcontainers sobe Postgres/MySQL/SQL Server).
