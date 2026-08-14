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

## Como reproduzir

```
mvn test -Dtest=ClaimQueryLoadHarness
mvn test -Dtest=ClaimQueryExplainHarness
```

Requer Docker local (Testcontainers sobe Postgres/MySQL/SQL Server).
