# ADR-0023: Suporte a SQL Server e MySQL — `JdbcDialect` por banco

## Status
Decided — 2026-08-13

## Context
Depois da validação contra Postgres (ADR-0022), a pergunta foi: e com
SQL Server? A resposta cresceu — suportar pelo menos os mesmos bancos
que o Quartz Scheduler suporta hoje. Pesquisei o `org.quartz.impl.jdbcjobstore`
atual do Quartz (GitHub, `quartz-core/.../jdbcjobstore`) pra saber o que
isso significa de verdade: Delegates dedicados existem pra
**PostgreSQL** (`PostgreSQLDelegate`), **SQL Server**
(`MSSQLDelegate`, "for Microsoft SQL Server, and Sybase") e **Oracle**
(`oracle.OracleDelegate`); **MySQL** roda sobre o `StdJDBCDelegate`
genérico (ANSI-compatível o bastante, sem delegate próprio). O resto do
que o Quartz carrega (DB2 v6-v8, Sybase, PointBase, CUBRID, GaussDB,
WebLogic-specific) é peso de 20+ anos de compatibilidade histórica, não
escopo ativo — replicar isso seria copiar o passado do Quartz, não o
presente dele.

### Por que não Hibernate
A pergunta veio à tona: já que Hibernate tem `Dialect`/`LimitHandler`/
`LockingStrategy` prontos, não seria melhor usá-lo? Não — três motivos:
1. `LimitHandler` (`org.hibernate.dialect.pagination`) e
   `LockingStrategy` (`org.hibernate.dialect.lock`) — confirmados na
   árvore atual do `hibernate-orm` — não são utilitários standalone,
   vivem dentro do `Dialect`, que só existe depois de inicializar
   `SessionFactory`/`ServiceRegistry`. Usar só essas duas interfaces
   significa adotar boa parte do framework mesmo assim.
2. O mecanismo de corretude do Mohs (ADR-0018, `UPDATE ... WHERE
   running_execution_count < max_concurrent_executions` checando linhas
   afetadas) não tem representação natural em JPA/HQL — viraria query
   nativa de qualquer jeito, pagando o custo do ORM (sessão, cache de
   1º nível, dirty checking, flush order) sem ganhar nada na parte mais
   crítica do sistema.
3. Overkill de tamanho: o problema real são poucos fragmentos de SQL
   que variam por banco. Hibernate inteiro pra resolver isso troca uma
   dependência pequena (que nem existia) por uma enorme — e o projeto
   já tinha saído de JPA pra `NamedParameterJdbcTemplate` puro antes
   desta sessão.

O que vale aproveitar do Hibernate é a **forma** da abstração
(interfaces pequenas, uma preocupação cada — não um `Delegate`
monolítico por vendor como o Quartz faz), não a dependência.

## Decision

### `JdbcDialect` — uma interface, cada implementação dona do SQL inteiro
```java
interface JdbcDialect {
    List<Candidate> selectCandidates(NamedParameterJdbcTemplate jdbcTemplate, Instant now, int batchSize);
}
```
Não uma abstração de fragmentos concatenáveis (`limitClause()` +
`lockHint()` compostos por fora) — o `TOP` do SQL Server muda de
**posição** na query (logo após `SELECT`, não no fim como `LIMIT`),
então composição de fragmentos genéricos não fecha limpo. Cada
implementação é dona do template SQL inteiro de `JdbcClaimer.claim` —
mesmo padrão que o próprio Quartz usa (cada `Delegate` tem o SQL
completo de cada operação) e como Hibernate realmente implementa
`LimitHandler` por baixo (recebe o SQL e devolve o SQL reescrito, não
um fragmento).

**Uma classe por banco, mesmo com SQL idêntico entre elas hoje**
(`H2JdbcDialect`, `PostgresJdbcDialect`, `MySqlJdbcDialect`,
`SqlServerJdbcDialect`) — decisão deliberada, não a mais enxuta
possível: H2/Postgres/MySQL têm exatamente o mesmo SQL de seleção de
candidatos hoje (`LIMIT`/`SKIP LOCKED` nativos nos três), mas
compartilhar uma única classe entre eles seria acoplar três bancos
independentes a uma coincidência de sintaxe atual — se um dia um
precisar de um ajuste (um hint, uma otimização específica), alguém
teria que primeiro descobrir que a classe compartilhada serve três
donos antes de conseguir separar. "Prefira uma pequena duplicação a um
acoplamento errado" (CLAUDE.md) — e é exatamente o padrão do próprio
Quartz: `PostgreSQLDelegate` existe como classe própria mesmo herdando
quase tudo de `StdJDBCDelegate`.

`JdbcClaimer` ganha um parâmetro `JdbcDialect` no construtor — **config
explícita, nunca auto-detecção** (mesmo padrão do Quartz,
`driverDelegateClass`: nunca detectado por `Connection.getMetaData()`,
frágil entre forks/versões de driver). Quem decide qual dialeto usar é
quem monta o `JdbcClaimer` — nos testes, explícito; em produção, decisão
de `io.mohs.autoconfigure` (M3, ainda não construído — fora do escopo
desta ADR).

### Divergências reais de cada banco

**SQL Server**: `CREATE TABLE/INDEX IF NOT EXISTS` não existe (T-SQL
usa `IF OBJECT_ID(...) IS NULL CREATE TABLE ...`/`IF NOT EXISTS
(SELECT ... FROM sys.indexes) CREATE INDEX ...`, um único statement
condicionado, sem `BEGIN...END` — evita que `ResourceDatabasePopulator`
quebre o script ao dividir por `;`); `TIMESTAMP` colide com
`ROWVERSION` (usa `DATETIME2`); sem `CLOB`/`TEXT` de verdade — ambos
deprecados, usa `NVARCHAR(MAX)`; sem `BOOLEAN` nativo (usa `BIT` +
`1`/`0`); `VARCHAR` não é Unicode por padrão (usa `NVARCHAR` em tudo);
sem `LIMIT` (usa `TOP (:batchSize)`, logo após `SELECT`); sem `SKIP
LOCKED` (usa hint de tabela `WITH (UPDLOCK, ROWLOCK, READPAST)`,
confirmado via jOOQ — é o que jOOQ gera pra emular `SKIP LOCKED` em SQL
Server).

**MySQL** (8.0+): `TIMESTAMP` tem semântica própria de auto-init/
auto-update e alcance limitado (1970-2038) — usa `DATETIME`; sem
`CLOB` (usa `TEXT`, mesma troca da ADR-0022); `CHARACTER SET utf8mb4`
explícito por tabela — não depender do default do servidor;
`BOOLEAN`/`TRUE`/`FALSE` funcionam igual (alias de `TINYINT(1)`/`1`/
`0`); `LIMIT`/`SKIP LOCKED` nativos desde o 8.0, mesma forma de H2/
Postgres. **Achado empírico**: MySQL não tem `CREATE INDEX IF NOT
EXISTS` (só `CREATE TABLE IF NOT EXISTS`) — descoberto rodando contra
um MySQL real via Testcontainers, não documentação. Resolvido
mudando **todos** os `*TestSupport` (Postgres incluso, por consistência)
pra aplicar o schema uma vez só, no `static {}` do container singleton,
não a cada `freshSchema()` — o schema não precisa mesmo ser reaplicado
por teste, só as tabelas precisam ser limpas.

### Achado empírico: SQL Server e deadlock genuíno sob concorrência real
`JdbcClaimerSqlServerTest.claimIsMutuallyExclusiveAcrossConcurrentNodes`
(dois nós disputando o mesmo mutex de job) falhou na primeira rodada
contra o container real: `SQLServerException: Transaction ... was
deadlocked on lock resources ... has been chosen as the deadlock
victim`. Não é bug do SQL nem da ADR-0018 — é o próprio lock manager do
SQL Server escolhendo uma transação "vítima" sob contenção pessimista
genuína (`WITH (UPDLOCK, ROWLOCK, READPAST)`), comportamento normal e
documentado (Microsoft recomenda explicitamente capturar e reexecutar).
`JdbcClaimer.claim` ganhou um retry limitado (`MAX_DEADLOCK_RETRIES = 3`)
capturando `org.springframework.dao.PessimisticLockingFailureException`
(tradução portável do Spring pra esse SQL state, não algo específico de
SQL Server) — reexecuta o claim inteiro do zero (SELECT de novo, já que
a transação abortada não deixa nada reaproveitável). Postgres/MySQL/H2
não disparam isso na prática (MVCC evita essa classe de deadlock pro
mesmo padrão de acesso), mas o retry é inofensivo pra eles — o catch
nunca dispara.

## Consequences
`../../../pom.xml` ganha `mssql-jdbc`/`mysql-connector-j` (runtime, `optional`)
e `testcontainers-mssqlserver`/`testcontainers-mysql` (escopo `test`).
`schema.sql` vira quatro arquivos (`schema-h2.sql`, `schema-postgresql.sql`,
`schema-mysql.sql`, `schema-sqlserver.sql`) — convenção nativa do
Spring Boot (`spring.sql.init.platform`/`schema-${platform}.sql`,
confirmado no jar `spring-boot-jdbc`), sem depender de Flyway.
`JdbcClaimer` ganha o parâmetro `JdbcDialect` — mudança de construtor,
sem consumidor externo do jar ainda (mesmo raciocínio das mudanças de
shape anteriores desta sessão).

**Explicitamente fora de escopo, registrado não esquecido**: Oracle
(único ponto sem verificação com confiança — o equivalente Oracle de
`CREATE TABLE IF NOT EXISTS` precisa de bloco PL/SQL condicional ou
checar `ALL_TABLES`, e eu não vou escrever esse DDL sem confirmar
contra um Oracle real primeiro); DB2, Sybase, PointBase, CUBRID,
GaussDB, WebLogic-specific (peso histórico do Quartz, não escopo
ativo); `io.mohs.autoconfigure` decidir o dialeto em produção (M3,
ainda não construído — hoje só quem monta `JdbcClaimer` nos testes
escolhe o dialeto).

## Source
`0022-postgres-validation-and-dialect-scope.md`. Pesquisa desta
sessão: Quartz `StdJDBCDelegate`/`MSSQLDelegate`/`PostgreSQLDelegate`/
`Semaphore` (GitHub, `quartz-scheduler/quartz`), Hibernate
`Dialect`/`LimitHandler`/`LockingStrategy` (GitHub,
`hibernate/hibernate-orm`, pacotes `dialect/pagination`,
`dialect/lock`), jOOQ `SQLDialect`/emulação de `SKIP LOCKED` via
`READPAST`, convenção `schema-${platform}.sql` do Spring Boot
(`spring-boot-jdbc`, classe `DatabaseDriver`). Achados empíricos desta
sessão: deadlock genuíno do SQL Server sob concorrência real (via
Testcontainers, `mcr.microsoft.com/mssql/server:2022-latest`); ausência
de `CREATE INDEX IF NOT EXISTS` no MySQL (via Testcontainers,
`mysql:8.0`).
