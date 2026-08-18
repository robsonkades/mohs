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

## Como reproduzir

```
mvn test -Dtest=ClaimQueryLoadHarness
mvn test -Dtest=ClaimQueryExplainHarness
mvn test -Dtest=ClaimIndexTuningHarness#postgres
mvn test -Dtest=ClaimIndexTuningHarness#sqlServer
mvn test -Dtest=OverviewQueryExplainHarness            # ou #postgres/#mySql/#sqlServer/#h2
```

Requer Docker local (Testcontainers sobe Postgres/MySQL/SQL Server).
