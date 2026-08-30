# ADR-0022: Validação contra Postgres real e escopo deliberado do dialeto

## Status
Decided — 2026-08-13

## Context
`../codereview.md` registrou 11 achados de portabilidade (DB-1 a
DB-11) depois de um review completo da base — o projeto só rodava
contra H2 embarcado, sem driver de Postgres/SQL Server nem
Testcontainers (DB-8), e a alegação de portabilidade da ADR-0017 nunca
tinha sido testada contra um segundo banco de verdade (DB-10).

Reexaminando achado por achado contra o schema/código atuais (pós
ADR-0018/0020/0021), separando o que é genuinamente H2-vs-**Postgres**
do que é H2-vs-**SQL Server**:

| Achado | H2 vs. Postgres | H2 vs. SQL Server |
|---|---|---|
| DB-1 `CREATE TABLE/INDEX IF NOT EXISTS` | Postgres suporta (9.1+/9.5+) — sem divergência | T-SQL não suporta — real |
| DB-2 `TIMESTAMP` colide com `ROWVERSION` | não existe `ROWVERSION` em Postgres — sem divergência | real |
| DB-3 `CLOB` não existe | **real** — Postgres usa `TEXT` | real (`NVARCHAR(MAX)`) |
| DB-4 `BOOLEAN`/`TRUE`/`FALSE` (DDL) | Postgres tem `BOOLEAN` nativo — sem divergência | T-SQL usa `BIT` + `1`/`0` — real |
| DB-5 `VARCHAR` não-Unicode | Postgres é UTF-8 por padrão — sem divergência | real |
| DB-6 `LIMIT` | Postgres suporta — sem divergência | T-SQL usa `TOP`/`OFFSET FETCH` — real |
| DB-7 `FOR UPDATE ... SKIP LOCKED` | Postgres suporta nativamente (é de onde a sintaxe vem) — sem divergência | não existe em T-SQL — real |
| DB-8 sem driver/Testcontainers | resolvido nesta ADR | segue faltando |
| DB-9 `schema.sql` só auto-aplica em datasource embarcado | não é sobre dialeto, é sobre deploy |
| DB-10 ADR-0017 alegava portabilidade não sustentada | fechado por esta ADR |
| DB-11 `IN (:ids)` pode passar de 2100 params | Postgres não tem esse teto, mas é barato corrigir de qualquer forma |
| DB-4 (DML) `FALSE, FALSE, 0` literal no `INSERT` de `JdbcJobStore` | Postgres aceita, mas bindar como parâmetro é mais correto de qualquer forma |

**Achado central**: H2 já roda em modo compatível com Postgres por
desenho — de todos os 11 achados, só **DB-3** (`CLOB`) é uma divergência
real entre H2 e Postgres especificamente. Os outros seis (DB-1/2/4/5/6/7)
são incompatibilidades específicas de **SQL Server**. Isso muda o
escopo real de "resolver dialeto" — de construir uma camada de
abstração pra "trocar um tipo de coluna e ligar Testcontainers".

### Estado da arte

- **Quartz**: `StdJDBCDelegate` + subclasses por vendor
  (`MSSQLDelegate`/`PostgreSQLDelegate`/`OracleDelegate`), selecionadas
  por config explícita (`org.quartz.jobStore.driverDelegateClass`),
  nunca auto-detectada. Locking é uma preocupação separada via
  interface `Semaphore`: `StdRowLockSemaphore` faz `SELECT FOR UPDATE`;
  `UpdateLockRowSemaphore` faz um `UPDATE` de linha simples,
  especificamente porque MSSQL não faz `SELECT FOR UPDATE` do mesmo
  jeito — a mesma ideia de CAS via `UPDATE` que a ADR-0018 já convergiu
  independentemente, só que Quartz trata como caso especial de MSSQL;
  aqui virou o mecanismo universal (lock é otimização, nunca a
  garantia).
- **Hibernate**: `org.hibernate.dialect.Dialect` por vendor, mas o
  desenho real é a decomposição em interfaces pequenas de uma
  preocupação só — `getLimitHandler()` devolve um `LimitHandler` que só
  sabe `TOP` vs `LIMIT/OFFSET` vs `OFFSET/FETCH`; `getLockingSupport()`
  só sabe renderizar `FOR UPDATE`. Granularidade fina, não uma classe
  monolítica por dialeto.
- **jOOQ**: `SQLDialect` enum (eixo família+versão) — confirmado que
  jOOQ emula `FOR UPDATE SKIP LOCKED` em SQL Server gerando hints de
  tabela `READPAST` automaticamente. Confirma que o equivalente T-SQL de
  `SKIP LOCKED` é `UPDLOCK, ROWLOCK, READPAST`.
- **Flyway/Spring Boot**: convenção é uma pasta por vendor
  (`db/migration/{vendor}/`). O próprio mecanismo nativo de schema-init
  do Spring Boot (não-Flyway) já tem essa convenção embutida —
  `schema-${platform}.sql`, selecionado via `spring.sql.init.platform`.
  Mohs já usa `spring.sql.init`/`ResourceDatabasePopulator`, não
  Flyway — quando SQL Server entrar, a divisão por arquivo não precisa
  de dependência nova, só o que o Spring Boot já oferece.

## Decision

**Resolvido nesta rodada:**
1. **DB-8** — `org.postgresql:postgresql` (runtime, `optional`, mesmo
   padrão de `h2`) e Testcontainers (`spring-boot-testcontainers` +
   `org.testcontainers:testcontainers-postgresql` +
   `org.testcontainers:testcontainers-junit-jupiter`, escopo `test`).
2. **DB-3** — `CLOB` → `TEXT` nas duas colunas que usavam
   (`mohs_executions.payload`, `mohs_attempts.error`). Continua um
   `schema.sql` só — H2 e Postgres aceitam a mesma sintaxe pra tudo
   mais que já existia no schema.
3. **DB-11** — `JdbcExecutionStore.findByIds` particiona `IN (:ids)`
   em lotes de `MAX_IDS_PER_QUERY = 1000`, bem abaixo do teto do SQL
   Server, agregando os resultados em memória.
4. **DB-4 (DML)** — `JdbcJobStore.upsert`'s `INSERT` para de escrever
   `FALSE, FALSE, 0` como texto SQL literal; passa a bindar
   `orphaned`/`paused`/`runningExecutionCount` como parâmetros
   (mesmo valor, forma portável).
5. **Validação real** — `PostgresTestSupport` (singleton container,
   Testcontainers) + `JdbcClaimerPostgresTest` (reexecuta os dois
   testes que provam a garantia de corretude da ADR-0018/0020 — CAS
   guardado, `preventOverlap`/`maxConcurrentExecutions` — contra
   Postgres real) + `SchemaPostgresRoundTripTest` (um round-trip por
   store, incluindo o payload `TEXT` que é o ponto real do DB-3).

**Deliberadamente fora de escopo, não esquecido:** DB-1, DB-2, DB-5,
DB-6, DB-7 (todos específicos de SQL Server) e DB-9 (deploy de schema
em produção, problema de host app, não de dialeto) ficam em aberto —
não foi pedido suporte a SQL Server nesta rodada, só validação contra
Postgres. Constroem trabalho real só quando SQL Server entrar de
verdade no escopo.

**Caminho já mapeado para quando SQL Server entrar** (não implementado
agora — não vale abstração com uma única forma real de implementação
hoje, "interface com uma única implementação é indireção, não
abstração", CLAUDE.md):
- DDL: `schema-h2.sql`/`schema-postgresql.sql`/`schema-sqlserver.sql`
  via o mecanismo nativo `spring.sql.init.platform` do Spring Boot —
  sem Flyway, sem dependência nova.
- Query: um `LimitHandler`-like pequeno (2 métodos: cláusula de
  limite, hint de lock) isolando só `JdbcClaimer.selectCandidates` —
  estilo Hibernate (interfaces pequenas, uma preocupação por vez), não
  o `Delegate` monolítico do Quartz. O hint T-SQL equivalente a `SKIP
  LOCKED` é `UPDLOCK, ROWLOCK, READPAST` (confirmado via jOOQ).

## Consequences
Schema muda de forma observável só em dois tipos de coluna
(`CLOB`→`TEXT`) — sem migração porque não há consumidor externo do jar
ainda (mesmo raciocínio já aplicado às mudanças de shape desta sessão).
`JdbcExecutionStore.findByIds` e `JdbcJobStore.upsert` mudam
implementação, comportamento observável idêntico. Testcontainers só
roda em escopo `test` — nunca em produção, requer Docker local/CI (já
confirmado disponível neste ambiente).

DB-9 continua uma lacuna real — quando `io.mohs.autoconfigure` for
implementado (M3), essa é a hora natural de decidir como o host app
aplica `schema.sql` em produção contra um banco não-embarcado; até lá,
é responsabilidade de quem faz o deploy.

## Source
`../codereview.md` achados DB-1 a DB-11. Pesquisa desta sessão
(Quartz `StdJDBCDelegate`/`Semaphore`, Hibernate `Dialect`/
`LimitHandler`, jOOQ `SQLDialect`, convenção Flyway/Spring Boot
`schema-${platform}.sql`). Estende ADR-0018 (CAS guardado — a mesma
ideia que Quartz usa como fallback MSSQL, aqui generalizada) e fecha
formalmente a alegação de portabilidade não sustentada da ADR-0017
(DB-10).
