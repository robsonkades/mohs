# Code Review — Mohs

**Data:** 2026-08-13
**Escopo:** codebase completa (`src/main`, `src/test`, `schema.sql`, `../../pom.xml`), não apenas o diff mais recente.
**Foco solicitado:** melhorias de código, code smells, performance tuning, database tuning — com atenção
especial a portabilidade entre dialetos (o projeto roda em H2 hoje, mas precisa suportar os dialetos
padrão do Spring Data JPA, com Postgres e SQL Server como alvos explícitos).
**Estágio do projeto:** desenvolvimento ativo, pré-produção (M3 em andamento — claim acabou de ser
implementado). Boa parte dos achados abaixo é sobre código já escrito, não sobre lacunas de milestones
futuras; onde um achado é sobre trabalho ainda não iniciado, isso está marcado explicitamente para não
ser confundido com regressão.

---

## Sumário executivo

A suíte está verde (180/180, 0 falhas, 0 erros, confirmado por execução real de `./mvnw test` nesta
revisão) e a qualidade média do código é alta — validação consistente em value objects, cópias
defensivas onde o Effective Java pede, ArchUnit guardando fronteiras arquiteturais, documentação (ADRs)
incomum em profundidade para o estágio do projeto. Isso não é um projeto descuidado; os achados abaixo
são o tipo de coisa que só aparece numa revisão de 171 arquivos linha a linha, não sintomas de
negligência.

Dito isso, há um bloqueador real para o objetivo declarado ("suportar os dialetos padrão do Spring Data
JPA, Postgres e SQL Server"):

**O mecanismo de claim — o coração do motor — e o `schema.sql` inteiro são, hoje, incompatíveis com SQL
Server, e o `schema.sql` também não roda em Postgres (tipo `CLOB` não existe lá).** Isso não é uma
sutileza: `CREATE TABLE` falha logo na primeira linha em SQL Server (`IF NOT EXISTS` não é T-SQL), toda
coluna `TIMESTAMP` do schema colide com o tipo `ROWVERSION` do SQL Server (mesmo nome, semântica
completamente diferente — nem é um tipo de data), e a query de claim usa `LIMIT` + `FOR UPDATE OF ...
SKIP LOCKED`, nenhum dos dois válido em T-SQL. Pior: nada disso apareceria em CI hoje, porque o projeto
só depende do driver H2 — não há driver Postgres/SQL Server nem Testcontainers em lugar nenhum do
`../../pom.xml`. A Seção 3.1 detalha cada ocorrência, o erro exato que cada uma produz em cada banco, e a
correção.

Outros destaques que merecem atenção antes de produção (não apenas antes de suportar múltiplos bancos):

- **Vazamento de vaga de queue**: `running_count` só tem incremento (`tryIncrementRunning`), nenhum
  decremento existe em lugar nenhum do código — quando a etapa de conclusão de execução for
  implementada, se ela não devolver a vaga, toda queue trava permanentemente depois de N execuções
  (Seção 3.2, CONC-1).
- **Corrida real (TOCTOU) em três dos seis stores JDBC**: `upsert()` de job/queue/rate-limit faz
  UPDATE-depois-INSERT-se-zero-linhas sem transação — dois nós inicializando ao mesmo tempo podem
  colidir num `INSERT` duplicado (Seção 3.2, CONC-2).
- **Um teste de concorrência genuinamente flaky** no mecanismo mais crítico do sistema: o teste que
  prova exclusão mútua entre nós falha em ~1-2% das execuções reais, verificado empiricamente com ~120
  execuções repetidas nesta revisão (Seção 3.6, TEST-1).
- **N+1 não reconhecido** no próprio caminho de claim, que o projeto já sabe evitar em outro lugar do
  mesmo arquivo-irmão (Seção 3.3, PERF-1).
- Uma dependência do Spring Data JPA declarada e **nunca usada** (o projeto usa JDBC puro), puxando
  Hibernate à toa; e H2 vazando sem `optional=true` para quem consumir o Mohs como biblioteca — ambos
  relevantes justamente para o objetivo de suportar múltiplos bancos (Seção 3.7).

A tabela consolidada (Seção 4) lista os 70 achados distintos encontrados, com severidade, arquivo e
correção sugerida. A Seção 5 propõe uma ordem de ataque.

---

## Metodologia

Esta revisão combinou leitura direta (schema, camada `io.mohs.jdbc`, `io.mohs.engine`, as 17 ADRs, parte
de `io.mohs.core`, testes JDBC, `ArchitectureTest`) com nove revisões paralelas independentes, cada uma
cobrindo uma fatia coesa da codebase ou um ângulo específico:

1. `io.mohs.core` (API pública) — Effective Java, DDD, imutabilidade, null-safety.
2. `io.mohs.rest` — contrato REST, RFC 7807, design de DTO.
3. `io.mohs.cron` — parser vendorizado, comparado linha a linha contra o Spring Framework upstream.
4. Portabilidade de banco de dados — todo statement SQL avaliado individualmente contra H2/Postgres/SQL
   Server.
5. Concorrência e correção do motor — claim, queue, `DatabaseClock`.
6. Auditoria da suíte de testes — inclusive execução repetida (~120x) dos testes de concorrência para
   detectar flakiness real, não hipotética.
7. Performance — hot paths, índices, connection pool, alocação.
8. Conformidade com as regras de `../../CLAUDE.md` (concorrência, nulidade, naming, dependências).
9. Duplicação, código morto e divergência entre documentação e código.

Achados duplicados entre revisores foram unificados (quando isso aconteceu, é um sinal de confiança
maior, não de redundância — está anotado onde relevante). Nenhum arquivo de produção foi alterado por
esta revisão.

---

## 3.1 Portabilidade de banco de dados (H2 → PostgreSQL / SQL Server)

Esta é a seção mais importante deste documento, dado o contexto explícito do pedido. A persistência do
Mohs (`io.mohs.jdbc`) **não usa JPA/Hibernate** — é JDBC manual via `NamedParameterJdbcTemplate`/
`JdbcTemplate`, com um único `schema.sql` de texto cru. Isso significa que não existe camada de
`Dialect` do Hibernate absorvendo nada: cada string SQL escrita à mão precisa ser portável por si só, ou
simplesmente não roda no banco alvo. Hoje, nenhuma delas foi escrita pensando nisso — o projeto foi
construído e testado exclusivamente contra H2, e boa parte da sintaxe usada é H2/Postgres-específica.

Cada item abaixo foi verificado individualmente contra os três dialetos.

### DB-1 — `CREATE TABLE/INDEX IF NOT EXISTS` não existe em T-SQL — CRÍTICO
- **Onde:** `src/main/resources/schema.sql:10,34,44,65,77,83` (6 `CREATE TABLE`) e `:60-63` (4 `CREATE
  INDEX`).
- **Quebra em:** SQL Server. (H2 e Postgres suportam ambos.)
- **Erro exato:** `Incorrect syntax near 'NOT'.` — falha já no primeiro statement do script.
- **Impacto:** nenhuma tabela chega a ser criada em SQL Server; o script inteiro aborta na primeira
  linha.
- **Correção:** `IF OBJECT_ID('mohs_job_definitions', 'U') IS NULL BEGIN CREATE TABLE ... END` por
  tabela, e o equivalente com `sys.indexes` por índice — ou, melhor, migrar para Flyway/Liquibase com
  pastas de migration por vendor (resolve este item e vários outros abaixo de uma vez).

### DB-2 — `TIMESTAMP` como tipo de coluna colide com `ROWVERSION` no SQL Server — CRÍTICO
- **Onde:** `schema.sql:30,31` (`mohs_job_definitions.created_at/updated_at`), `:39`
  (`mohs_batches.created_at`), `:48,49,54,58` (`mohs_executions.scheduled_at/fired_at/
  lease_expires_at/created_at`), `:68,69` (`mohs_attempts.started_at/finished_at`) — 9 colunas ao todo.
- **Quebra em:** SQL Server.
- **Problema:** em T-SQL, `TIMESTAMP` é um sinônimo depreciado de `ROWVERSION` — um valor binário de 8
  bytes gerado automaticamente para controle de concorrência otimista, **não é um tipo de data/hora**.
  Uma tabela só pode ter **uma** coluna desse tipo.
- **Erro exato:** `CREATE TABLE` já falha no segundo `TIMESTAMP` de cada tabela com múltiplas
  ocorrências (`Msg 2731: A table can only have one timestamp column.`). No único caso que sobreviveria
  (`mohs_batches`, uma coluna só), todo `INSERT` que tenta gravar `Timestamp.from(clock.instant())`
  falha (`Msg 273: Cannot insert an explicit value into a timestamp column.`).
- **Correção:** `DATETIME2` (ou `DATETIME2(3)`) nas 9 colunas, nunca `TIMESTAMP` bare, na variante SQL
  Server do schema.

### DB-3 — `CLOB` não existe em PostgreSQL nem em SQL Server — CRÍTICO
- **Onde:** `schema.sql:56` (`mohs_executions.payload`), `:71` (`mohs_attempts.error`).
- **Quebra em:** PostgreSQL **e** SQL Server (os dois, não só um).
- **Erro exato:** Postgres — `ERROR: type "clob" does not exist`. SQL Server — `Msg 2715: Cannot find
  data type clob.` `CREATE TABLE` falha nos dois.
- **Correção:** não existe uma única palavra-chave que funcione nos três — precisa de DDL por vendor:
  Postgres `TEXT`, SQL Server `NVARCHAR(MAX)` (não `VARCHAR(MAX)`, ver DB-5), H2 mantém `CLOB`.

### DB-4 — `BOOLEAN` e literais `TRUE`/`FALSE` não existem em T-SQL — CRÍTICO
- **Onde, tipo de coluna:** `schema.sql:18,23,28,29` (`interval_after_finish`,
  `allow_concurrent_executions`, `orphaned`, `paused`).
- **Onde, literais `DEFAULT`:** `schema.sql:23,28,29` (`DEFAULT FALSE`).
- **Onde, literais inline em SQL da aplicação:** `src/main/java/io/mohs/jdbc/JdbcJobStore.java:108`
  (`VALUES (..., FALSE, FALSE, ...)` no INSERT de `upsert`), `:134` (`SET orphaned = TRUE` em
  `markOrphaned`), `:139` (`SET paused = TRUE` em `pause`), `:145` (`SET paused = FALSE` em `resume`);
  e `src/main/java/io/mohs/jdbc/JdbcClaimer.java:133` (`j.allow_concurrent_executions = TRUE` no WHERE
  do claim).
- **Quebra em:** SQL Server, em todas as ocorrências acima.
- **Problema:** T-SQL não tem tipo `BOOLEAN` (usa `BIT`) nem aceita as palavras `TRUE`/`FALSE` como
  literais — nem mesmo como valor de coluna `BIT` (precisa ser `1`/`0`).
- **Erro exato:** `Msg 2715: Cannot find data type boolean` (na criação da coluna) e `Msg 207: Invalid
  column name 'FALSE'` (em todo `DEFAULT`/`SET`/`WHERE`/`VALUES` que usa o literal bare — SQL Server
  interpreta `TRUE`/`FALSE` como nome de coluna inexistente, não como booleano).
- **Nota sobre `JdbcClaimer.java:133` especificamente:** não existe uma única grafia que funcione nos
  três bancos — SQL Server também rejeita uma coluna `BIT` usada nua como predicado
  (`WHERE some_bit_column` sem comparação é erro de sintaxe lá). A correção real é parametrizar o valor
  em vez de embutir o literal (um `boolean` do Java já se liga corretamente a `BIT`/`BOOLEAN` via bind
  parameter em qualquer driver).
- **Correção:** `BIT` no lugar de `BOOLEAN`; `DEFAULT 1`/`DEFAULT 0` no lugar de `DEFAULT TRUE`/`FALSE`;
  nos quatro pontos de `JdbcJobStore`, trocar o literal inline por parâmetro nomeado
  (`.addValue("orphaned", false)` etc.) — mais portável e também mais consistente com o resto do
  arquivo, que já é 100% parametrizado.

### DB-5 — `VARCHAR` não-Unicode corrompe silenciosamente caracteres acentuados no SQL Server — CRÍTICO
- **Onde:** todas as colunas `VARCHAR` de `schema.sql`, com destaque para `payload` (`:56`), `error`
  (`:71`), `name`/`actor` e os demais campos de texto livre.
- **Quebra em:** SQL Server apenas — mas de forma silenciosa, sem exceção.
- **Problema:** `VARCHAR`/`CHAR` do SQL Server não são Unicode — dependem de code page/collation. Valores
  `String` do Java chegam via JDBC como Unicode; ao gravar numa coluna `VARCHAR`, o SQL Server faz um
  downcast implícito para o code page da coluna.
- **Impacto real:** qualquer caractere fora desse code page — "ç", "é", "ã" (a própria convenção deste
  projeto é documentação em português!), ou qualquer script não-Latin num payload — é **silenciosamente
  trocado por `?`** no INSERT/UPDATE. Sem exceção, sem warning, nada que a aplicação consiga observar.
  Isto é corrupção de dado silenciosa, não uma falha de sintaxe — o caso mais grave que este documento
  cataloga, mesmo não sendo o item citado no pedido original. `payload` (JSON arbitrário serializado do
  job) e `error` (mensagem de exceção, pode conter o que o handler do usuário escrever) são os alvos mais
  prováveis.
- **Correção:** usar `NVARCHAR` em vez de `VARCHAR` na variante SQL Server do schema — no mínimo em
  `payload`/`error` (→ `NVARCHAR(MAX)`) e `name`/`actor`; recomendação é aplicar uniformemente em todo o
  DDL do SQL Server em vez de decidir coluna a coluna o que é "seguro".

### DB-6 — `LIMIT` não é sintaxe T-SQL válida — CRÍTICO
- **Onde:** `src/main/java/io/mohs/jdbc/JdbcClaimer.java:144`.
- **Quebra em:** SQL Server.
- **Erro exato:** `Incorrect syntax near 'LIMIT'.`
- **Correção:** `SELECT TOP (:batchSize)` logo após o `SELECT`, ou `OFFSET 0 ROWS FETCH NEXT
  (:batchSize) ROWS ONLY` após o `ORDER BY` já existente (T-SQL exige `ORDER BY` para usar
  `OFFSET/FETCH`, e a query já tem um).

### DB-7 — `FOR UPDATE OF ... SKIP LOCKED` não existe em T-SQL — CRÍTICO
- **Onde:** `src/main/java/io/mohs/jdbc/JdbcClaimer.java:145` — a cláusula final da query de claim, o
  mecanismo de aquisição sem contenção que é o núcleo do motor inteiro.
- **Quebra em:** SQL Server. **Não** quebra em Postgres — essa é exatamente a sintaxe nativa do Postgres
  (`FOR UPDATE [OF tabela[, ...]] [SKIP LOCKED]`, desde a 9.5), e o H2 2.4.240 foi confirmado
  empiricamente (spike descartável, ver ADR-0017) a suportá-la também no mesmo formato de duas tabelas.
- **Problema:** T-SQL não tem cláusula `FOR UPDATE` em `SELECT` de forma alguma (o `FOR UPDATE` que
  existe em T-SQL pertence a `DECLARE CURSOR`, outra coisa). Locking em SQL Server é expresso como
  *table hints* em cada referência de tabela, não como cláusula final.
- **Erro exato:** `Incorrect syntax near 'FOR'.` — some com F8 na mesma query.
- **Impacto:** este é o achado mais estruturalmente significativo do documento. Não é um ajuste de
  linha — a query inteira precisa de um formato diferente para SQL Server, não um patch pontual.
- **Correção:**
  ```sql
  FROM mohs_executions e WITH (UPDLOCK, ROWLOCK, READPAST)
  JOIN mohs_job_definitions j WITH (UPDLOCK, ROWLOCK, READPAST) ON j.job_key = e.job_key
  ```
  `READPAST` é o equivalente de `SKIP LOCKED`; `UPDLOCK` dá a intenção de "for update"; `ROWLOCK` força
  granularidade de linha (sem isso, o SQL Server pode fazer escalonamento de lock para página/tabela sob
  volume, contenção que este desenho existe justamente para evitar). Combinar com a correção de DB-6.
  Uma vez reescrita, vale uma verificação à parte da interação com Read Committed Snapshot Isolation
  (default no Azure SQL) contra uma instância real — isso é uma checagem semântica, não mais um problema
  de sintaxe.

### DB-8 — Nenhum driver JDBC de Postgres/SQL Server, nem Testcontainers — ALTO
- **Onde:** `../../pom.xml` — só `com.h2database:h2` (runtime) é declarado. Nenhum `org.postgresql:postgresql`,
  `com.microsoft.sqlserver:mssql-jdbc`, nem módulo Testcontainers (`postgresql`/`mssqlserver`/
  `junit-jupiter`). Nenhuma referência a Flyway/Liquibase em `src/`.
- **Impacto:** todos os achados DB-1 a DB-7 são **invisíveis para CI hoje**. Nada no pipeline atual
  quebraria se a compatibilidade com SQL Server regredisse ainda mais do que já está. A única
  "verificação empírica" que a ADR-0017 cita foi um spike manual e descartável contra H2, não parte da
  suíte.
- **Correção:** adicionar `org.testcontainers:postgresql`, `org.testcontainers:mssqlserver`,
  `org.testcontainers:junit-jupiter` (escopo teste) e rodar a suíte `Jdbc*StoreTest`/`JdbcClaimerTest`
  contra os três bancos reais em CI — isso teria pego todos os achados acima automaticamente, e é o
  jeito de garantir que não voltem.

### DB-9 — `schema.sql` só é aplicado automaticamente contra datasource embarcado — ALTO
- **Onde:** `src/main/resources/application.yaml` (sem `spring.sql.init.mode`, sem
  `spring.datasource.*`); `src/main/java/io/mohs/autoconfigure/` (só tem `package-info.java`, nenhuma
  classe de fato ainda).
- **Problema:** o default do Spring Boot, `spring.sql.init.mode=embedded`, só roda `schema.sql`
  automaticamente quando o `DataSource` configurado é detectado como embarcado (H2/HSQLDB/Derby). Hoje
  isso funciona só porque o Boot autoconfigura um H2 em memória a partir da dependência runtime, sem URL
  explícita — o que satisfaz essa checagem por acidente, não por desenho.
- **Impacto:** no momento em que uma aplicação hospedeira apontar o Mohs para
  `spring.datasource.url=jdbc:postgresql://...` ou `jdbc:sqlserver://...`, o datasource deixa de ser
  "embarcado", `sql.init.mode=embedded` vira no-op silencioso, `schema.sql` nunca roda, e nenhuma tabela
  é criada. A primeira query falha imediatamente (`relation "mohs_job_definitions" does not exist` no
  Postgres; `Invalid object name 'mohs_job_definitions'` no SQL Server) sem nada no log explicando o
  motivo — é um comportamento documentado do Boot, não um erro.
- **Correção (decisão de design, não é patch de uma linha):** documentar que apps hospedeiras devem
  setar `spring.sql.init.mode=always` para Postgres/SQL Server; ou fazer `io.mohs.autoconfigure`
  registrar um `DataSourceScriptDatabaseInitializer` incondicional; ou adotar Flyway/Liquibase (que
  aplica migrations em qualquer datasource, independente de ser embarcado) — o que também resolve DB-1 a
  DB-5 de uma vez, com pastas de migration por vendor. Como `io.mohs.autoconfigure` ainda é um esqueleto
  vazio, este é o momento certo de decidir isso antes que mais código dependa do schema "já resolvido".

### DB-10 — ADR-0017 alega uma portabilidade que não se sustenta contra os três bancos exigidos — MÉDIO
- **Onde:** `adr/0017-claim-per-job-mutex-and-queue-admission.md`, seção "Alternativas consideradas
  e rejeitadas" — rejeita advisory lock (`pg_advisory_xact_lock`) citando, entre outros motivos,
  "portabilidade pior entre bancos" em relação ao `FOR UPDATE OF ... SKIP LOCKED` escolhido.
- **Problema:** DB-7 mostra que `FOR UPDATE OF ... SKIP LOCKED` não é expressável em T-SQL de forma
  alguma. A comparação de portabilidade da ADR só funciona entre H2 e Postgres (onde advisory lock
  realmente perderia, porque `pg_advisory_xact_lock` não tem equivalente em H2) — nunca foi reavaliada
  contra o requisito real de três bancos, onde **os dois** mecanismos candidatos precisam de uma
  implementação específica para SQL Server do zero. A escolha atual não é mais portável no total; só
  adiantou dois bancos de graça e empurrou exatamente a mesma quantidade de trabalho específico de SQL
  Server para depois.
- **Nota:** isso não invalida o *algoritmo* da ADR-0017 (seleção de candidatos transacional + dedupe de
  siblings em memória + UPDATE guardado para admissão de queue) — esse raciocínio é sólido e
  vendor-agnóstico no nível de desenho. O que não se sustenta é tratar `FOR UPDATE OF ... SKIP LOCKED`
  como detalhe de *implementação* já resolvido sob status "Decided". Vale notar que o próprio time já
  aplica esse cuidado em outro lugar — a ADR-0012 rejeita renovação de lease por-job citando
  textualmente "Postgres `INTERVAL` ≠ SQL Server `DATEADD`" — só não foi carregado para a ADR-0017, que é
  o SQL de maior tráfego e mais crítico para concorrência do sistema.
- **Correção sugerida:** uma ADR de acompanhamento (ou emenda à 0017) que escopa o "Decided" atual
  explicitamente para H2/PostgreSQL, e abre a decisão de como o `JdbcClaimer` vai despachar para uma
  variante SQL-Server-específica (por `DatabaseMetaData.getDatabaseProductName()` ou enum de vendor
  configurado).

### DB-11 — Limite de 2100 parâmetros do SQL Server pode ser excedido pelo `IN (:ids)` do claim — BAIXO
- **Onde:** `src/main/java/io/mohs/jdbc/JdbcClaimer.java:113-117` — o `UPDATE ... WHERE id IN (:ids)`
  final do claim, alimentado pela lista de `claimedIds`.
- **Problema:** `NamedParameterJdbcTemplate` expande `:ids` em `IN (?, ?, ?, ...)` — um parâmetro
  posicional por id, mais 2 (`leaseExpiresAt`, `nodeId`). SQL Server tem um teto rígido de 2100
  parâmetros por statement; Postgres tolera 65535; H2 é, na prática, ilimitado para este uso.
- **Impacto:** só se manifesta se `batchSize` (parâmetro de `JdbcClaimer.claim`) for configurado acima de
  ~2097 — não é um problema com os defaults atuais, mas é uma armadilha de configuração específica de SQL
  Server que nada documenta hoje.
- **Correção:** limitar `batchSize` com folga abaixo de 2100 ao mirar SQL Server, ou trocar este `UPDATE`
  por uma junção com tabela temporária/table-valued parameter se lotes muito grandes forem necessários.

### DB-12 — `SELECT CURRENT_TIMESTAMP` do `DatabaseClock` é portável — confirmado, sem ação necessária
- **Onde:** `src/main/java/io/mohs/jdbc/DatabaseClock.java:41,103`.
- **Verificado:** `CURRENT_TIMESTAMP` é a função niládica do SQL padrão, aceita sem parênteses nos três
  bancos. `rs.getTimestamp()`/`getObject(Timestamp.class)` funciona corretamente nos três drivers.
- **Nuance de precisão (informativo, não é bug):** H2 retorna `TIMESTAMP WITH TIME ZONE`
  (nanossegundos); Postgres retorna `timestamptz` (microssegundos); SQL Server retorna o tipo legado
  **`datetime`** (não `datetime2`!), com granularidade de arredondamento de ~3,33ms. Isso não afeta a
  lógica de amostragem de offset (`Duration.between(...)` comparado contra `skewWarnThreshold`, que numa
  configuração sã está na casa de segundos) — mas vale um comentário no código para o próximo
  desenvolvedor que rodar isso contra SQL Server de verdade não se surpreender.

---

## 3.2 Concorrência e correção do motor (claim, queue, clock)

### CONC-1 — Vazamento de vaga de queue: `running_count` nunca é decrementado — ALTO (latente)
- **Onde:** `src/main/java/io/mohs/jdbc/JdbcQueueStore.java:59-66` (`tryIncrementRunning`);
  `src/main/java/io/mohs/engine/QueueStore.java` (interface — não declara nenhum método de liberação).
- **Confirmado:** busca exaustiva em `main` e `test` por qualquer decremento (`decrementRunning`,
  `releaseSlot`, `running_count - 1` etc.) não encontra nada. Mais além: o único `SET state` de
  `mohs_executions` em todo o projeto é o próprio `UPDATE ... SET state = 'RUNNING'` do claim — não existe
  hoje, em lugar nenhum, um método capaz de tirar uma `Execution` de `RUNNING`.
- **Por que isso importa agora, mesmo sendo trabalho futuro:** o comentário em
  `schema.sql:75-76` já documenta que esta etapa só cria a coluna, e a ADR-0009 (Proposed) nomeia o
  reaper como o mecanismo de autocura — mas nem `QueueStore` (a porta) nem `JdbcQueueStore` (o adapter)
  têm sequer a *assinatura* de um `releaseSlot`/`decrementRunning` hoje. Um leitor do código atual não
  tem nenhum sinal de compilação ou teste de que isto é parcial por design.
- **Cenário concreto:** quando a etapa de conclusão de execução for implementada, se ela não decrementar
  `running_count` no mesmo movimento, toda execução que passar por `tryIncrementRunning` consome uma
  vaga para sempre — sucesso, falha ou crash do nó, tanto faz. Uma queue com `max_concurrent=10` trava
  permanentemente em `running_count=10` depois de 10 execuções, mesmo horas depois de todas terem
  terminado.
- **Correção:** adicionar a assinatura de `releaseSlot`/`decrementRunning` a `QueueStore` já agora
  (mesmo sem implementação plena), tratando o decremento como dependência rígida da etapa de conclusão,
  não como follow-up opcional; crash de nó precisa do reaper (ADR-0012) para devolver a vaga.

### CONC-2 — TOCTOU sem transação em três dos seis `upsert()` de `io.mohs.jdbc` — ALTO
- **Onde:** `src/main/java/io/mohs/jdbc/JdbcQueueStore.java:27-41`,
  `src/main/java/io/mohs/jdbc/JdbcJobStore.java:57-112`,
  `src/main/java/io/mohs/jdbc/JdbcRateLimitStore.java:27-40` — todos implementam "tenta UPDATE, se 0
  linhas afetadas faz INSERT" sem `TransactionTemplate`/`@Transactional`.
- **Cenário concreto:** dois nós (ou duas threads) sobem ao mesmo tempo e ambos chamam
  `queueStore.upsert(JobQueue.named("emails")...)` para um nome ainda não persistido (primeiro deploy,
  ou uma queue nova declarada em código):
  1. T1: `UPDATE ... WHERE name='emails'` → 0 linhas (não existe ainda).
  2. T2: `UPDATE ... WHERE name='emails'` → 0 linhas (T1 ainda não inseriu).
  3. T1: `INSERT INTO mohs_job_queues (...) VALUES ('emails', ...)` → sucesso.
  4. T2: mesmo `INSERT` → falha por violação de chave primária (`DuplicateKeyException`/
     `DataIntegrityViolationException`, não tratada), que propaga direto para o bootstrap de registro de
     queues no startup do Spring context.
  Rollout multi-réplica é um cenário realista para isso, não exótico.
- **Nota:** o comentário de `JdbcJobStore.java:82-84` afirma que este padrão "evita... a corrida TOCTOU
  de um SELECT COUNT prévio" — a premissa está incorreta: trocar o `SELECT COUNT` por um `UPDATE` que
  retorna contagem de linhas não fecha a janela, só muda quais duas instruções competem.
- **Correção:** upsert nativo de uma instrução só (`MERGE` em SQL Server/H2, `INSERT ... ON CONFLICT
  (name) DO UPDATE` em Postgres — de novo, três grafias diferentes, tema recorrente desta revisão), ou
  envolver o par UPDATE+INSERT numa transação com retry-como-UPDATE na violação de unicidade do INSERT.

### CONC-3 — Race condition real em `DatabaseClock.applyIfMonotonic` — MÉDIO (mecanismo confirmado, gatilho ainda não alcançável)
- **Onde:** `src/main/java/io/mohs/jdbc/DatabaseClock.java:47` (campo `volatile Duration offset`),
  `:135-140` (`applyIfMonotonic`).
- **Interleaving concreto que quebra o clamp monotônico** (a garantia central desta classe):
  1. Estado inicial: `offset = 0s`.
  2. T1 amostra o banco, calcula `sampledOffset O1 = +3s`.
  3. T2 amostra concorrentemente, calcula `sampledOffset O2 = +5s` (amostra mais nova e maior).
  4. T1 entra em `applyIfMonotonic(O1)`: lê `offset` (ainda `0`) → não é regressão → mas é preemptado
     antes de escrever.
  5. T2 entra em `applyIfMonotonic(O2)`: lê `offset` (ainda `0`, T1 não escreveu) → não é regressão →
     escreve `offset = +5s`.
  6. T1 retoma e completa sua escrita, agora obsoleta: `offset = +3s` — **sobrescreve** o valor mais
     novo de T2.
  7. Resultado: `offset = +3s`, mesmo com uma amostra válida e mais recente de `+5s` já publicada. Um
     chamador lendo `instant()` repetidamente veria o tempo avançar e depois **retroceder** 2 segundos —
     exatamente a regressão que o clamp existe para evitar.
- **Por que não é explorável hoje:** ver CONC-4 — nada chama `sync()` de mais de uma thread ainda, então
  o gatilho não existe no código atual. Mas a segurança depende inteiramente de uma disciplina de
  chamador documentada só em Javadoc ("quem agenda a chamada a `sync()` não chama de duas threads ao
  mesmo tempo"), sem nenhuma garantia estrutural no tipo.
- **Correção:** trocar `volatile Duration offset` por `AtomicReference<Duration>` com
  `accumulateAndGet(sampledOffset, (current, sampled) -> sampled.compareTo(current) < 0 ? current :
  sampled)` — torna o clamp atômico independente da disciplina do chamador, sem custo de lock (não há
  I/O na seção crítica).

### CONC-4 — `DatabaseClock.sync()` não é agendado em lugar nenhum ainda — MÉDIO (esperado neste estágio)
- **Onde:** busca por `.sync()` em todo o repositório encontra só chamadas manuais em
  `DatabaseClockTest`. `io.mohs.autoconfigure` não tem nenhuma classe além de `package-info.java`.
- **Situação:** consistente com o estágio do milestone — o modo `mohs.time.source=database` (ADR-0008)
  ainda não tem bean nem agendamento. Registrado para que, quando `io.mohs.autoconfigure` for
  implementado, o agendamento seja feito por um único mecanismo dedicado (ex.: `ScheduledExecutorService`
  single-thread próprio), fazendo a invariante "escritor único" de CONC-3 valer por construção, não só
  por comentário.

### CONC-5 — Sem renovação de lease, heartbeat nem reaper — `RUNNING` é beco sem saída hoje — ALTO (operacional, esperado)
- **Onde:** todo `io.mohs.engine`/`io.mohs.jdbc`; `lease_expires_at` é escrito pelo claim
  (`JdbcClaimer.java:110`) mas nenhuma query no código lê essa coluna de volta.
- **Situação:** confirma exatamente o que a ADR-0012 já declara como "fica pra depois" — não é uma
  surpresa, mas vale registrar o estado concreto: hoje, um nó que reivindica uma execução e morre (ou
  trava) deixa a linha permanentemente `RUNNING`, indistinguível de uma execução saudável de longa
  duração para qualquer query existente. Isso também significa que o vazamento de CONC-1 não tem cura
  automática ainda (o reaper é quem devolveria a vaga).
- **Recomendação:** tratar lease renewal + reaper como bloqueantes para tráfego real, não como polish
  opcional de M3 — a recuperação at-least-once é promessa central do produto.

### CONC-6 — Teste do mecanismo mais crítico do sistema é empiricamente flaky — ver TEST-1 (Seção 3.6)
Este achado é sobre um teste, não sobre a lógica de produção em si (o mutex por job foi verificado linha
a linha nesta revisão e está correto) — está detalhado na Seção 3.6 para não duplicar, mas é listado
aqui porque a *causa* pode estar tanto no teste quanto numa característica de timing do `SKIP LOCKED` do
H2 que ainda não foi isolada.

---

## 3.3 Performance tuning

### PERF-1 — N+1 não reconhecido no próprio caminho de claim — CRÍTICO
- **Onde:** `src/main/java/io/mohs/jdbc/JdbcClaimer.java:67-71` (`claim`), alimentando
  `src/main/java/io/mohs/jdbc/JdbcExecutionStore.java:79-89` (`find`) → `:125-130` (`fetchAttempts`).
- **Descrição:** depois do `UPDATE` em lote de `claimWithinTransaction`, `claim()` faz
  `claimedIds.stream().map(id -> executionStore.find(ExecutionId.of(id)).orElseThrow()).toList()` — um
  SELECT por PK para cada linha reivindicada. Cada `find()` dispara uma *segunda* query aninhada contra
  `mohs_attempts` via `fetchAttempts`. O ponto notável: `JdbcExecutionStore.findAll()`/`findByJobKey()`
  já têm um comentário reconhecendo esse mesmo formato como "N+1 real... deliberadamente não otimizado
  ainda" porque "nada chama esses dois em produção nesta etapa" (`JdbcExecutionStore.java:105-111`) —
  `JdbcClaimer.claim()` não tem comentário equivalente, e ao contrário desses dois métodos, o claim **é**
  chamado pelo loop de poll do motor, conforme o próprio Javadoc da classe declara.
- **Impacto:** para um lote de N claims, isso soma até 2N round-trips extras (N buscas de execução + até
  N buscas de attempts) em cima do 1 SELECT + 1 UPDATE em lote — por exemplo, ~202 round-trips para um
  único ciclo de poll reivindicando 100 linhas. Isso reintroduz exatamente o custo "por execução, não por
  nó × frequência" que a ADR-0009 diz explicitamente que o claim deve evitar.
- **Correção:** substituir o loop por-id por uma única query de acompanhamento em lote
  (`SELECT * FROM mohs_executions WHERE id IN (:ids)`, reaproveitando `mapRow`), espelhando o batching já
  usado no próprio `UPDATE` do claim; ou fazer o `UPDATE` final devolver os dados via `RETURNING`/`OUTPUT`
  (dialect-dependent).

### PERF-2 — Falta índice composto `(job_key, state)` para a subquery do mutex por job — ALTO (plausível, raciocinado a partir do schema)
- **Onde:** `src/main/resources/schema.sql:61` (só existe `idx_mohs_executions_job_key` em coluna
  única); consultado por `src/main/java/io/mohs/jdbc/JdbcClaimer.java:134-137` (o `NOT EXISTS`
  correlacionado que verifica `r.job_key = e.job_key AND r.state = 'RUNNING'`).
- **Impacto:** conforme `mohs_executions` acumula histórico por `job_key` (linhas `SUCCEEDED`/`FAILED`
  nunca saem da tabela — é a tabela mais quente do sistema, por desenho, per o comentário do próprio
  schema), essa checagem de existência degrada de um seek quase-O(1) para uma varredura O(linhas-por-
  job_key), repetida por candidato, a cada poll.
- **Correção:** adicionar índice composto `(job_key, state)` em `mohs_executions`, permitindo que a
  subquery faça seek direto em `(job_key, 'RUNNING')` e pare no primeiro match.

### PERF-3 — HikariCP sem nenhuma configuração — ALTO (lacuna futura, não bug em código pronto)
- **Onde:** `src/main/resources/application.yaml` (5 linhas, nenhuma `spring.datasource.hikari.*`);
  `io.mohs.autoconfigure` vazio.
- **Situação:** os defaults do Spring Boot (`maximumPoolSize=10`, `connectionTimeout=30000ms`) miram
  concorrência modesta em platform threads. O próprio `../../CLAUDE.md` do projeto já define o alvo
  (`maximumPoolSize` 100+, `connectionTimeout` <3s para virtual threads) — isso precisa estar pronto
  antes ou junto do primeiro dispatcher em virtual threads, senão o pool vira o teto de throughput e
  timeouts lentos substituem backpressure rápida.

### PERF-4 — `ORDER BY` do claim não é servido por índice — MÉDIO
- **Onde:** `src/main/java/io/mohs/jdbc/JdbcClaimer.java:138-144` — `idx_mohs_executions_claim(state,
  scheduled_at)` serve bem o `WHERE`, mas o `ORDER BY` é `CASE e.priority ... END DESC, e.scheduled_at
  ASC`, e `priority` não está indexado; o `CASE` também é opaco para o otimizador.
- **Impacto:** o motor precisa materializar *todas* as linhas `ENQUEUED`-e-devidas que casam o `WHERE`
  antes de ordenar e aplicar o `LIMIT`/`TOP` — o custo se desacopla de `batchSize` e reacopla ao tamanho
  do backlog, justamente no cenário (recuperação pós-outage) em que a latência do claim mais importa.
- **Correção:** não é urgente agora; se o crescimento de backlog virar real, considerar uma coluna
  `priority_rank` inteira persistida/computada, indexada como `(state, priority_rank, scheduled_at)`.

### PERF-5 — `JdbcBatchStore` compartilha o padrão de hot-row que a ADR-0009 já nomeia como risco para queues, sem ADR equivalente — MÉDIO
- **Onde:** `src/main/java/io/mohs/jdbc/JdbcBatchStore.java:56-65` (`incrementSucceeded`/
  `incrementFailed` — corretamente atômicos, sem SELECT prévio) vs. `adr/0009-queue-enforcement.md`,
  que nomeia exatamente este padrão (`UPDATE running_count = running_count + 1`) como tendo três modos de
  falha conhecidos (hot row, bloat de tupla, drift).
- **Impacto:** para um lote grande cujas execuções completam concorrentemente (o próprio propósito da
  feature `Batch`/`BatchBuilder`), toda conclusão serializa na mesma linha de `mohs_batches` — o mesmo
  mecanismo que o time já está preocupado em outro lugar, só que ainda não nomeado aqui.
- **Recomendação:** sem ação sem gargalo medido (YAGNI) — mas vale dobrar isso no mesmo guarda-chuva de
  benchmark/ADR da ADR-0009 em vez de redescobrir o problema separadamente depois.

### PERF-6 — Coleções do loop de claim sem capacidade pré-dimensionada — BAIXO
- **Onde:** `src/main/java/io/mohs/jdbc/JdbcClaimer.java:93-94` — `claimedIds` (`ArrayList`) e
  `claimedJobKeys` (`HashSet`) usam capacidade default, apesar de `candidates.size()` (limitado por
  `batchSize`) já ser conhecido nesse ponto.
- **Correção:** `new ArrayList<>(candidates.size())` e `HashSet.newHashSet(candidates.size())` (JDK 19+).

### PERF-7 — Cache de cron sem eviction e com lifecycle de singleton não garantido — BAIXO
- **Onde:** `src/main/java/io/mohs/engine/NextFireCalculator.java:32,50`.
- **Situação:** `CRON_CACHE` nunca evita, inclusive quando um `JobDefinition` é removido. Hoje isso é de
  baixo risco (cron expressions são atribuídas uma vez por definição de job, não sintetizadas por
  instância de execução — o espaço de chaves é naturalmente limitado). O risco real é outro: nada no
  código hoje garante que `NextFireCalculator` será construído uma vez como singleton quando o motor for
  ligado — se for construído por chamada/por ciclo de poll, o cache nunca acumula hit nenhum, sem erro,
  sem teste quebrando, só uma premissa de performance documentada em prosa e nunca verificada em código.
  (Ver também API-18/CONV — o nome do campo, `CRON_CACHE`, é outro problema à parte.)
- **Correção:** ao conectar `NextFireCalculator` ao motor, construir uma vez como singleton (bean Spring
  ou campo mantido pelo dono do poll loop) e garantir isso no call site; eviction fica para quando
  padrões reais de churn forem conhecidos (YAGNI).

### PERF-8 — `Class.forName` por linha em `JdbcJobStore` — BAIXO (não é hot path hoje)
- **Onde:** `src/main/java/io/mohs/jdbc/JdbcJobStore.java:169` (`mapRowOrNull`).
- **Situação:** cardinalidade = número de tipos de job distintos registrados, não o caminho de claim/
  execução; nada chama `findAll()` aqui em produção ainda (REST ainda não está ligado ao engine). Sem
  ação necessária agora; se este caminho ficar quente, cachear a `Class<?>` resolvida por
  `handler_type`, espelhando o padrão que `NextFireCalculator` já usa para cron.

---

## 3.4 API pública e code smells (`io.mohs.core`)

### API-1 — `@CheckReturnValue` ausente exatamente onde o bug mais comum aconteceria — ALTO
- **Onde:** `src/main/java/io/mohs/core/Mohs.java:19,22` (`schedule`, as duas sobrecargas);
  `src/main/java/io/mohs/core/ScheduleCommand.java:23,25,27` (`priority`, `as`, `idempotencyKey`) — só
  os terminais (`now/at/after`, linhas 29-36) e `Mohs.batch` carregam a anotação.
- **Problema:** `mohs.schedule(ref, payload);` ou `mohs.schedule(ref, payload).priority(HIGH);` como
  statement solto **compila sem nenhum warning**, mas nada é agendado — o Javadoc de `ScheduleCommand` é
  claro que só os terminais "fecham a cadeia e persistem a execução". O mecanismo hoje só pega o caso
  mais raro (chamar um terminal e descartar o recibo), não o mais comum (parar a cadeia um passo antes).
- **Correção:** anotar também as duas sobrecargas de `Mohs.schedule` e os três métodos intermediários de
  `ScheduleCommand` com `@CheckReturnValue`.

### API-2 — `JobSpecImpl`: mutex "garantido pelo compilador" é falso para o estilo não encadeado — ALTO
- **Onde:** `src/main/java/io/mohs/core/definition/JobSpecImpl.java:34-56,106-114`.
- **Problema:** o Javadoc de `JobSpec` e `API-DESIGN.md` afirmam que a exclusão mútua entre
  `cron`/`every` "é garantida pelo compilador" no caminho programático. Isso só vale para o estilo
  encadeado (`spec.cron(...).every(...)` de fato não compila, porque `PolicySpec` não expõe métodos de
  gatilho). É **falso** para o estilo de statements separados — `cron()`/`every()`/`everyAfterFinish()`/
  `onDemand()` todos sobrescrevem `this.schedule` incondicionalmente e retornam `this`:
  ```java
  JobDefinition.of("x", Handler.class, spec -> {
      spec.cron("0 0 2 * * *", ZoneId.of("UTC"));
      spec.every(Duration.ofSeconds(30)); // sobrescreve silenciosamente; nenhum erro em lugar nenhum
  });
  ```
  `toDefinition` só checa `schedule == null`, nunca "foi setado mais de uma vez".
- **Impacto:** um refactor ou uma resolução de merge conflict que deixe tanto um `.cron(...)` quanto um
  `.every(...)` no mesmo configurador produz um job silenciosamente rodando na agenda errada — nenhuma
  das três defesas que o projeto valoriza (erro de compilação, erro de validação de boot, exceção em
  runtime) dispara.
- **Correção:** adicionar uma guarda "gatilho já escolhido" aos quatro métodos de gatilho em
  `JobSpecImpl` (`if (schedule != null) throw new IllegalStateException(...)`), consistente com o fato de
  que o caminho de annotation já é esperado para validar isso no boot.

### API-3 — `MohsRunner`: construtor canônico público contorna a garantia dos builders — ALTO
- **Onde:** `src/main/java/io/mohs/core/resource/MohsRunner.java:34-61`.
- **Problema:** o compact constructor só valida os campos relevantes ao `mode` dado — nunca valida ou
  normaliza os campos do *outro* modo. Isso contradiz diretamente o Javadoc do próprio tipo ("o campo do
  modo errado fica zerado e ignorado") e a ADR-0014, que atribui a garantia de "impossível de usar
  errado" inteiramente aos dois builders (`IoBuilder`/`CpuBuilder`) — mas o construtor canônico de um
  record é sempre público, e está diretamente acessível, contornando os dois builders por completo:
  ```java
  new MohsRunner("x", RunnerMode.IO, 5, -1, -1, -1, Duration.ofSeconds(-1));
  // sucesso — coreSize/maxSize/queueCapacity/keepAlive negativos, nenhum checado (branch IO nunca roda)

  new MohsRunner("x", RunnerMode.CPU, -999, 4, 4, 0, Duration.ofSeconds(60));
  // sucesso — maxConcurrent = -999, o branch CPU nunca toca em maxConcurrent
  ```
- **Correção:** estender o compact constructor para também validar/exigir zero nos campos do modo
  inativo (ex.: `else`-side checar `if (maxConcurrent != 0) throw ...`; lado IO checar que
  `coreSize == 0 && maxSize == 0 && queueCapacity == 0 && keepAlive.isZero()`).

### API-4 — `ExecutionWindow`: record cuja `equals()`/`hashCode()` são efetivamente identity-based — MÉDIO-ALTO
- **Onde:** `src/main/java/io/mohs/core/resource/ExecutionWindow.java:25` (componente `exclusions`).
- **Problema:** `exclusions` é copiado defensivamente (`List.copyOf`, correto), mas seu tipo de elemento
  é `Predicate<Instant>`, populado só via lambdas (`excludeWeekends()`, `excludeDaily()`,
  `excludeDates()`). Lambdas usam `equals()` de identidade padrão do `Object`. Como `ExecutionWindow` é
  um `record`, seu `equals()`/`hashCode()` gerados delegam para `List.equals()` sobre `exclusions`, que
  portanto é efetivamente baseado em identidade:
  ```java
  ExecutionWindow a = ExecutionWindow.named("x").excludeWeekends().build();
  ExecutionWindow b = ExecutionWindow.named("x").excludeWeekends().build();
  a.equals(b); // false — cada chamada de excludeWeekends() cria uma lambda nova e distinta
  ```
  Isso quebra silenciosamente a expectativa de semântica de valor que um `record` normalmente garante.
- **Correção:** no mínimo, documentar essa limitação de forma proeminente no tipo. Se igualdade/dedupe
  algum dia importar de fato, modelar as exclusões como dado selado (`Weekends`, `Dates(Set<LocalDate>)`,
  `DailyRange(LocalTime, LocalTime)`, `Custom(Predicate<Instant>)`) em vez de predicados crus — mudança
  maior, provavelmente YAGNI hoje, mas vale registrar que o tipo atual faz uma promessa implícita
  (record ⇒ semântica de valor) que não consegue cumprir.

### API-5 — `JobDefinition.timeout` sem validação de positividade — MÉDIO (confirmado independentemente por dois revisores)
- **Onde:** `src/main/java/io/mohs/core/definition/JobDefinition.java:37,41-50`.
- **Problema:** inconsistente com os irmãos diretos no mesmo escopo — `IntervalSpec.interval`
  (`src/main/java/io/mohs/core/schedule/IntervalSpec.java:15-17`) rejeita zero/negativo, assim como
  `RateLimit.window`. `JobDefinition.timeout` não tem checagem equivalente — `Duration.ZERO` ou negativo
  constroem sem erro.
- **Correção:** `if (timeout != null && !timeout.isPositive()) throw new IllegalArgumentException(...)`.

### API-6 — `runner`/`queue`/`window`/`retryPolicy` sem checagem de blank — MÉDIO
- **Onde:** `src/main/java/io/mohs/core/definition/JobDefinition.java:31-33,38`.
- **Problema:** todo outro nome de recurso neste escopo rejeita string em branco (`JobKey`, `ExecutionId`,
  `JobQueue.name`, `MohsRunner.name`, `RateLimit.name`, `ExecutionWindow.name`) — estes quatro campos,
  não. `.runner("")` produz uma definição silenciosamente apontando para um recurso vazio, distinto de
  "não definido" (`null`) mas presumivelmente com a mesma intenção.
- **Correção:** mesma guarda usada em `key`, aplicada aos quatro campos.

### API-7 — `Attempt.error` presente sem relação com `outcome` — MÉDIO
- **Onde:** `src/main/java/io/mohs/core/execution/Attempt.java:15,17-23`.
- **Problema:** o Javadoc diz "`error` é a mensagem da falha, presente só quando `outcome` é `FAILED`",
  mas nada no compact constructor garante isso — `new Attempt(1, now, now, SUCCEEDED, "boom")` e
  `new Attempt(1, now, now, FAILED, null)` constroem sem erro, ambos violando o invariante documentado.
- **Correção:** `if ((outcome == ExecutionState.FAILED) != (error != null)) throw new
  IllegalArgumentException(...)`.

### API-8 — `Attempt.outcome` aceita valores sem sentido para uma tentativa individual — MÉDIO
- **Onde:** `src/main/java/io/mohs/core/execution/Attempt.java:15`.
- **Problema:** reaproveita o enum `ExecutionState` inteiro (6 valores), mas `ENQUEUED` nunca faz sentido
  para uma tentativa que já tem `startedAt` não-nulo, e `RETRY_SCHEDULED` descreve o estado da *Execution*
  pai, não o desfecho da própria tentativa.
- **Correção:** guarda rejeitando `ENQUEUED`/`RETRY_SCHEDULED` no compact constructor — não introduzir um
  enum novo só para isto (violaria o próprio limiar de 3-usos-reais do projeto).

### API-9 — `actor` nunca validado como non-blank, apesar de ser "inegociável" na doc — MÉDIO
- **Onde:** `src/main/java/io/mohs/core/execution/Execution.java:27,29-36`;
  `src/main/java/io/mohs/core/event/Enqueued.java:16,18-23`.
- **Problema:** `API-DESIGN.md` chama a trilha de actor de "inegociável"; ambos os tipos exigem
  `actor` non-null mas nunca checam non-blank — `actor=""` passa silenciosamente.
- **Correção:** `if (actor.isBlank()) throw new IllegalArgumentException(...)`, espelhando o padrão de
  `JobKey`.

### API-10 — Seis eventos com `attempt`/`nextAttempt` sem validação `>= 1` — MÉDIO
- **Onde:** `AttemptFailed.java:9`, `Failed.java:14`, `Started.java:10`, `Succeeded.java:9`,
  `Cancelled.java:10`, `RetryScheduled.java:10` (campo `nextAttempt`) — todos em
  `src/main/java/io/mohs/core/event/`.
- **Problema:** `io.mohs.core.execution.Attempt.number`, o conceito semanticamente idêntico duas
  pastas ao lado, é validado (`number < 1` lança exceção); nenhum destes seis é. `new Started(id, key,
  -5, Instant.now())` constrói sem erro.
- **Correção:** reaproveitar a mesma guarda nos seis compact constructors — 6 ocorrências (mais
  `Attempt.number` e o contrato documentado de `JobContext.attempt()`) supera com folga o limiar de "3
  usos reais" deste projeto para extrair um helper minúsculo compartilhado.

### API-11 — `BatchCompleted` não valida `succeeded + failed <= total` — MÉDIO
- **Onde:** `src/main/java/io/mohs/core/event/BatchCompleted.java:13,15-23`.
- **Problema:** valida `batchId` non-blank e cada contador individualmente non-negative, mas nunca a
  invariante cruzada — `new BatchCompleted("b1", 1, 10, 10)` (10+10 > 1) constrói sem erro, apesar de ser
  impossível.
- **Correção:** `if (succeeded + failed > total) throw new IllegalArgumentException(...)`.

### API-12 — Mesmo conceito representado como `String` num tipo e `Throwable` noutro — MÉDIO
- **Onde:** `src/main/java/io/mohs/core/execution/Attempt.java:15` (`@Nullable String error`, "a
  mensagem da falha") vs. `src/main/java/io/mohs/core/event/AttemptFailed.java:9` e
  `Failed.java:14` (`Throwable error`, non-nullable) — o mesmo conceito ("o erro de uma tentativa
  falhada"), duas representações diferentes em partes próximas da mesma API pública.
- **Avaliação:** parcialmente intencional e defensável — `Attempt` é histórico persistido (bate com a
  coluna `CLOB` de `mohs_attempts.error`), `AttemptFailed`/`Failed` são eventos transitórios in-process
  para listeners logarem/alertarem, então carregar o `Throwable` completo ali faz sentido para quem
  consome o evento. O ponto de atenção real não é qual dos dois "está errado", é que **nada documenta ou
  implementa hoje** a conversão `Throwable → String` que vai ser necessária quando o motor gravar um
  `Attempt` a partir de um evento de falha — e reter um `Throwable` bruto num objeto que pode ficar
  retido em memória por um listener (stack trace completo + cadeia de causas) é mais pesado do que reter
  uma `String`.
- **Correção:** nenhuma mudança de tipo necessária agora; documentar explicitamente onde a conversão
  `Throwable → String` (mensagem? stack trace resumida?) vai acontecer quando M3 ligar os dois lados.

### API-13 — `ExecutionEventType` espelha manualmente `ExecutionEvent` sem link em tempo de compilação — BAIXO
- **Onde:** `src/main/java/io/mohs/core/event/ExecutionEventType.java` (8 constantes) vs.
  `ExecutionEvent.java:10` (8 `permits`).
- **Impacto:** hoje sincronizados, mas nada barra uma nova variante `sealed` de ser adicionada sem a
  constante correspondente (ou vice-versa) — o filtro de `@OnExecution` simplesmente nunca casaria com a
  variante nova, silenciosamente.
- **Correção sugerida (não claramente um defeito, é uma sugestão):** tipar `OnExecution.event()` como
  `Class<? extends ExecutionEvent>` em vez de manter o enum-sombra; ou, mantendo o enum, adicionar um
  teste que afirme mecanicamente a paridade (`ExecutionEventType.values().length == 8`, com comentário
  apontando para o `permits`).

### API-14 — Interfaces funcionais sem `@FunctionalInterface` — BAIXO
- **Onde:** `ExecutionListener.java:12`, `ExecutionInterceptor.java:14,18` (e `Chain.proceed`),
  `src/main/java/io/mohs/core/BatchBuilder.java:7` — todas de método único, pensadas para uso com lambda
  (Observer/Chain of Responsibility, per os próprios package-info), mas nenhuma anotada. Busca no
  repositório inteiro não encontra `@FunctionalInterface` em lugar nenhum.
- **Correção:** anotar as quatro — previne que um segundo método abstrato adicionado por engano quebre
  silenciosamente todo call-site com lambda, com um erro confuso no call site em vez de um erro claro na
  declaração da interface.

### API-15 — Validação "non-null então non-blank" duplicada 7x sem helper — BAIXO
- **Onde:** `JobKey.java`, `ExecutionId.java`, `event/BatchCompleted.java`, `ExecutionWindow.java`,
  `JobQueue.java`, `MohsRunner.java`, `RateLimit.java` — o mesmo par de checagens copiado à mão em cada
  um.
- **Nota:** o limiar de generalização deste projeto é "três usos reais" — 7 ocorrências supera isso
  com folga. Um `Strings.requireNonBlank(value, field)` minúsculo removeria a duplicação sem adicionar
  peso de abstração real.

### API-16 — `Batch.onCompletion`: contrato ambíguo sobre múltiplas chamadas — BAIXO
- **Onde:** `src/main/java/io/mohs/core/Batch.java:20`.
- **Problema:** nem o tipo nem o Javadoc dizem se `batch.onCompletion(a).onCompletion(b)` registra dois
  listeners independentes ou substitui o anterior, nem se o `Batch` retornado é `this` ou uma cópia.
- **Correção:** esclarecer no Javadoc (ex.: "cada chamada registra um listener independente").

### API-17 — `JobDefinition` sem Javadoc por campo — BAIXO
- **Onde:** `src/main/java/io/mohs/core/definition/JobDefinition.java:26-39` — 13 componentes, nenhum
  documentado individualmente, enquanto `@MohsJob` (o mapeamento 1:1 via annotation) documenta todo
  atributo.
- **Correção:** Javadoc inline pelo menos nos campos menos autoexplicativos (`misfire`, `window`,
  `retryPolicy`).

### API-18 — `NextFireCalculator.CRON_CACHE`: campo de instância nomeado como constante estática — MÉDIO (confirmado por seis fontes independentes nesta revisão)
- **Onde:** `src/main/java/io/mohs/engine/NextFireCalculator.java:32` —
  `private final Map<String, CronExpression> CRON_CACHE = new ConcurrentHashMap<>();`.
- **Problema:** SCREAMING_SNAKE_CASE, por convenção Java, sinaliza `static final` — uma constante
  verdadeiramente compartilhada. O campo é `final` mas **não** `static`: cada instância de
  `NextFireCalculator` tem seu próprio mapa. O nome induz o leitor ao engano justamente num projeto que
  trata concorrência como prioridade nº 1 — confundir "estado por instância" com "constante
  compartilhada" é exatamente o tipo de leitura errada que produz bug de concorrência depois.
- **Correção:** renomear para `cronCache`.

---

## 3.5 Camada REST (`io.mohs.rest`)

Lembrete de contexto: esta camada é contrato M2 — a maioria dos handlers hoje são stubs
(`throw new UnsupportedOperationException("M3: ainda não implementado")`). Os achados abaixo são todos
sobre a *assinatura*/contrato já travado ou sobre infraestrutura cross-cutting (`RestExceptionHandler`,
DTOs) que segue valendo quando M3 ligar a implementação real — não é crítica a lógica de negócio
ausente, que é esperada neste estágio.

### REST-1 — `@ResponseStatus(ACCEPTED)` é código morto nos handlers que retornam `ResponseEntity` — ALTO
- **Onde:** `src/main/java/io/mohs/rest/job/JobsController.java:42-46` (`schedule`),
  `src/main/java/io/mohs/rest/execution/ExecutionsController.java:52-56` (`retry`).
- **Verificado contra o código-fonte do Spring Framework 7.0.8:** `HttpEntityMethodProcessor
  .handleReturnValue()` sempre sobrescreve o status da resposta a partir do próprio
  `ResponseEntity.getStatusCode()` — a anotação `@ResponseStatus` não tem efeito nenhum em runtime quando
  o tipo de retorno é `ResponseEntity`.
- **Impacto:** um implementador de M3 vendo a anotação presume, razoavelmente, que 202 já está garantido,
  e escreve `ResponseEntity.ok(...)` (o atalho mais idiomático) em vez de
  `ResponseEntity.accepted()...` — o endpoint passa a devolver 200 em vez de 202, quebrando o princípio 1
  do design REST ("202 como contrato") sem nenhum sinal de compilador ou teste (os testes de contrato
  atuais só verificam que `UnsupportedOperationException` propaga, não códigos de status).
- **Correção:** remover a anotação enganosa dos dois métodos (ou adicionar um teste MockMvc afirmando
  `status().isAccepted()` assim que implementado).

### REST-2 — Sem exception handler catch-all — RFC 7807 só vale para 3 exceções nomeadas — ALTO
- **Onde:** `src/main/java/io/mohs/rest/error/RestExceptionHandler.java:25-52`.
- **Verificado:** `BasicErrorController` do Boot 4.1.0 devolve um `ResponseEntity<Map<String,Object>>`
  simples, não `ProblemDetail`, independente de `spring.mvc.problemdetails.enabled` — e os próprios
  testes de contrato do projeto confirmam empiricamente que hoje nenhuma exceção fora as três nomeadas é
  interceptada.
- **Impacto:** qualquer exceção não prevista — inclusive falhas de infraestrutura que vão começar a
  acontecer assim que M3 ligar um engine/JDBC real (`IllegalStateException`, `DataAccessException`,
  exceções de concorrência) — cai no `/error` padrão do Boot, com corpo estruturalmente diferente
  (`{timestamp,status,error,path}`, não `{type,title,status,detail,...}`). Viola o princípio "erros em
  RFC 7807" como afirmação geral.
- **Correção:** adicionar `@ExceptionHandler(Exception.class)` retornando um `ProblemDetail` 500
  genérico e seguro (logando a causa real no servidor, nunca `ex.getMessage()` para exceções
  desconhecidas), para que todo caminho de erro — conhecido ou não — produza o mesmo envelope.

### REST-3 — Mensagens de validação dos DTOs nunca chegam ao cliente — ALTO
- **Onde:** `src/main/java/io/mohs/rest/queue/QueuePatchRequest.java:6-9`,
  `src/main/java/io/mohs/rest/ratelimit/RateLimitPatchRequest.java:9-16`.
- **Verificado contra o código-fonte do Spring:** `ResponseEntityExceptionHandler
  .handleHttpMessageNotReadable()` usa a string fixa `"Failed to read request"` como `detail`,
  descartando a mensagem original da causa.
- **Problema:** esses records lançam `IllegalArgumentException` bem redigida em seus compact
  constructors, mas isso roda durante a desserialização do Jackson em `@RequestBody`. O Jackson embrulha
  a exceção, o Spring classifica como `HttpMessageNotReadableException`, e o handler base substitui a
  mensagem antes de chegar ao cliente.
- **Impacto:** `PATCH /queues/{name}` com `{"maxConcurrent": 0}` devolve 400 com "Failed to read
  request" em vez de "maxConcurrent must be at least 1" — contradiz diretamente o padrão que o próprio
  projeto define ("detail que ensina a corrigir"). Não há `spring-boot-starter-validation` no
  classpath, então "adicionar `@Min`/`@Valid`" não é uma correção grátis (dependência nova, precisa de
  aprovação por regra do CLAUDE.md).
- **Correção (sem dependência nova):** mover esta checagem para fora do caminho de desserialização —
  validar dentro do corpo do futuro controller/service de M3 e lançar `PayloadValidationException`
  (já produz o formato 422 + campo apontado que o design doc promete), deixando a checagem no compact
  constructor só como defesa-em-profundidade para construção direta em Java.

### REST-4 — `retry()` não tem suporte a `Idempotency-Key`, apesar do próprio Javadoc afirmar paridade — MÉDIO-ALTO
- **Onde:** `src/main/java/io/mohs/rest/execution/ExecutionsController.java:51-56` vs.
  `src/main/java/io/mohs/rest/job/JobsController.java:42-46`.
- **Problema:** o Javadoc acima de `retry()` diz "mesmo contrato de aceite de `schedule`". `schedule()`
  declara `@RequestHeader(value = "Idempotency-Key", required = false)`; `retry(String id,
  HttpServletRequest request)` não tem parâmetro equivalente — mas cria uma nova execução/tentativa do
  mesmo jeito, com o mesmo risco de dupla submissão em retry de cliente.
- **Correção:** adicionar o mesmo `@RequestHeader` a `retry()`, ou corrigir o Javadoc explicando por que
  retry é uma exceção deliberada.

### REST-5 — `cancel()` não pode satisfazer o `Location` do princípio 1; formato diverge de `retry()` — MÉDIO
- **Onde:** `src/main/java/io/mohs/rest/execution/ExecutionsController.java:44-49`.
- **Problema:** `cancel()` retorna `ExecutionResponse` puro (não `ResponseEntity`), sem acesso a nada
  onde anexar um header `Location`. O design doc declara "toda invocação" responde com `Location:
  /executions/{id}" sem excetuar cancel explicitamente. Enquanto isso, `retry()` (o outro endpoint 202)
  retorna o formato menor `AcceptedExecutionResponse`.
- **Correção:** ou escopar o princípio 1 explicitamente a "invocações que criam uma execução nova"
  (schedule/retry), ou mudar `cancel()` para `ResponseEntity<ExecutionResponse>` agora, enquanto ainda é
  de graça.

### REST-6 — `ScheduleJobRequest.payload`: cópia defensiva só rasa — MÉDIO
- **Onde:** `src/main/java/io/mohs/rest/job/ScheduleJobRequest.java:22-27`.
- **Problema:** o mapa de topo é corretamente envolvido (`Collections.unmodifiableMap(new
  LinkedHashMap<>(payload))`, com justificativa documentada para não usar `Map.copyOf`), mas valores
  aninhados (`Map`/`List` de JSON aninhado) não são copiados — permanecem mutáveis e compartilhados por
  referência.
- **Correção:** copiar recursivamente `Map`/`List` aninhados, ou documentar explicitamente o
  trade-off ao lado da justificativa já existente do `Map.copyOf`.

### REST-7 — Endpoints de cursor sem parâmetro de tamanho de página no contrato M2 — MÉDIO
- **Onde:** `src/main/java/io/mohs/rest/execution/ExecutionsController.java:29-37`,
  `src/main/java/io/mohs/rest/job/JobsController.java:58-61`.
- **Problema:** nem `search()` nem `executions()` declara `size`/`limit`; `CursorPage` não documenta
  default/máximo. Dado que M2 existe para travar a assinatura antes de M3, este é exatamente o tipo de
  parâmetro operacionalmente crítico que deveria estar decidido agora.
- **Impacto:** risco real de DoS por página ilimitada se M3 não pensar nisso de forma independente.
- **Correção:** adicionar `@RequestParam(required = false) Integer size` a ambos os endpoints agora, e
  documentar default/máximo no Javadoc de `CursorPage`.
- **Adicional (BAIXO-MÉDIO):** `CursorPage.nextCursor` (`src/main/java/io/mohs/rest/CursorPage.java:16-
  22`) não tem contrato de opacidade/encoding definido — nada impede uma futura implementação de expor
  um identificador interno de banco cru. Definir como token opaco (ex.: Base64) desde já.

### REST-8 — Resolução de actor duplicada em 7 handlers, sem hook compartilhado — MÉDIO
- **Onde:** `JobsController.java:44,49,54`, `ExecutionsController.java:47,54`,
  `QueuesController.java:31`, `RateLimitsController.java:28` — todos recebem `HttpServletRequest` só
  para resolver o actor via `ActorResolver`.
- **Impacto:** correção barata agora (assinatura ainda não tem 7 call-sites reais rodando), cara depois
  — exatamente o cross-cutting concern que `HandlerMethodArgumentResolver` do Spring existe para
  resolver.
- **Correção:** introduzir um argument resolver customizado (`@ResolvedActor String actor`) quando
  `io.mohs.autoconfigure` existir, substituindo `HttpServletRequest request` nos 7 métodos.

### REST-9 — `/api/mohs/v1` duplicado como literal em 8 controllers — BAIXO-MÉDIO
- **Onde:** um `@RequestMapping` por controller (`BatchesController.java:10`,
  `ExecutionsController.java:26`, `JobsController.java:29`, `NodesController.java:11`,
  `OverviewController.java:13`, `QueuesController.java:22`, `RateLimitsController.java:18`,
  `RunnersController.java:11`).
- **Nota:** tornar o prefixo configurável via `mohs.api.base-path` já está explicitamente escopado para
  M3 (`plans/003-optional-webmvc-dependency.md`) — o hardcode em si não é o achado. O achado é que nem a
  string hardcoded tem uma constante compartilhada hoje: um typo ou mudança de prefixo exige tocar 8
  arquivos.
- **Correção:** extrair `static final String API_V1 = "/api/mohs/v1"` agora — trivial, e é a costura
  natural que M3 depois troca por um placeholder de property.

### REST-10 — `HeaderActorResolver` é spoofável por desenho — confirmado intencional, gap residual pequeno — BAIXO
- **Onde:** `src/main/java/io/mohs/rest/HeaderActorResolver.java:11-20`.
- **Avaliação:** decisão consciente e documentada (Javadoc + ADR-0010, com discordância do líder técnico
  já registrada nos docs e mitigada por três camadas: fechado por padrão, WARN ao habilitar, guia de
  deployment) — não é uma lacuna silenciosa, não deve ser tratado como bug novo.
- **Gap residual real:** nenhum limite de tamanho/caracteres no valor do header antes de usá-lo como
  está — mitigado em boa parte pelo limite de tamanho de header do container servlet (~8KB no Tomcat),
  então o risco prático é baixo.

### REST-11 — `PayloadValidationException` não valida `message` non-null — BAIXO
- **Onde:** `src/main/java/io/mohs/rest/error/PayloadValidationException.java:18-21`.
- **Problema:** `field` é validado (`Objects.requireNonNull`), `message` não — inconsistente com a
  disciplina do resto do pacote. Um `message=null` produz um `ProblemDetail` sem texto de `detail`.

### REST-12 — Testes de erro não passam pelo MockMvc real — BAIXO
- **Onde:** `src/test/java/io/mohs/rest/error/RestExceptionHandlerTest.java` chama os métodos do
  handler diretamente como POJO, nunca via `MockMvc`/`@WebMvcTest` — nada hoje afirma que a resposta HTTP
  real tem `Content-Type: application/problem+json` ponta a ponta. Trade-off razoável para M2, vale um
  smoke test `@WebMvcTest` quando M3 ligar a implementação.

### REST-13 — Primitive obsession em `@PathVariable String` — BAIXO (provavelmente proposital neste estágio)
- **Onde:** todos os controllers, `@PathVariable String jobKey/id/name`.
- **Avaliação:** `io.mohs.autoconfigure` ainda não existe, então nenhum `Converter<String,JobKey>` está
  fiado ainda — consistente com "não ligado ao motor ainda". Registrado para consciência, não é um
  achado real neste milestone.

**Verificado e limpo, sem achados:** JSpecify/`@NullMarked` em todos os 10 pacotes de `io.mohs.rest`;
serialização polimórfica de `ScheduleView` (inclusive o caso historicamente delicado de
`OnDemandView` com um único componente); imutabilidade profunda de `OverviewResponse`/`QueueDepthView`/
`ThroughputView`; ausência de superfície de injeção (nenhuma string de query montada à mão, persistência
corretamente fora de alcance via a regra ArchUnit `rest_only_sees_public_api`); toda rota bate 1:1 com a
tabela de `REST-API-DESIGN.md`.

---

## 3.6 Testes

A suíte está genuinamente em bom estado — verde (180/180), sem `Thread.sleep`, sem `@MockBean` (a
anotação removida no Boot 4), sem `@Disabled`/testes comentados, nomes de teste consistentemente
descritivos. Os achados abaixo concentram-se em duas áreas que a revisão pediu escrutínio extra
(`JdbcClaimerTest` e `DatabaseClockTest`) e num punhado de branches de validação não testados — um padrão
que quebra a disciplina que o resto da suíte já demonstra (testar cada branch de validação
individualmente).

### TEST-1 — `JdbcClaimerTest.claimIsMutuallyExclusiveAcrossConcurrentNodes` é empiricamente flaky — ALTO
- **Onde:** `src/test/java/io/mohs/jdbc/JdbcClaimerTest.java:220-247` — o teste que o próprio Javadoc do
  arquivo chama de "o mais importante desta etapa", provando que o mutex por job da ADR-0017 se sustenta
  sob duas transações concorrentes reais (`CyclicBarrier` + `Executors.newVirtualThreadPerTaskExecutor()`
  — desenho de concorrência correto, não simulação sequencial).
  Verificado nesta revisão: reproduzido duas vezes de forma independente em ~120 execuções locais
  repetidas (`./mvnw test -Dtest=...` em loop) — taxa de falha de ~1-2%, sempre como falha de asserção
  pura (não timeout, não deadlock).
- **Impacto:** falha intermitente de CI nesse teste específico corrói a garantia "suíte verde após cada
  etapa, sem exceção" que o próprio projeto exige de si mesmo — e o comportamento natural (rodar de novo
  em vez de investigar) é exatamente como uma regressão real e rara no mutex passaria despercebida. A
  ADR-0017 também só reivindica ter verificado o comportamento de `SKIP LOCKED` do H2 via um spike
  descartável, não pela suíte — a causa raiz pode estar tanto no teste quanto num detalhe de timing do H2
  ainda não isolado.
- **Correção:** adicionar mensagem de asserção descritiva capturando o conteúdo de `claimedA`/`claimedB`
  (para diferenciar "os dois reivindicaram" de "nenhum reivindicou" quando falhar); rodar em loop
  isolado (200-1000x) para determinar se a soma alguma vez chega a 2 (bug real no mutex) ou não (artefato
  de timing do gerenciador de lock do H2) — e, se for o segundo caso, isso merece nota explícita, porque
  a ADR-0017 hoje implica mais confiança no comportamento de `SKIP LOCKED` do H2 do que uma taxa de
  flake de ~1% sustenta.

### TEST-2 — Teste do clamp monotônico do `DatabaseClock` não testa o clamp de fato — ALTO
- **Onde:** `src/test/java/io/mohs/jdbc/DatabaseClockTest.java:83-99`
  (`instantNeverGoesBackwardsAcrossAResampleThatWouldMoveItBackward`); produção:
  `DatabaseClock.java:135-140` (`applyIfMonotonic`).
- **Problema, traçado passo a passo:** `MutableClock` é congelado (não avança com o tempo real). Quando o
  teste faz `appClock.advance(Duration.ofHours(1))`, esse salto de +1h já está embutido diretamente em
  `appClock.instant()`. A asserção final é sobre `clock.instant()`, que é `appClock.instant() + offset`.
  A asserção (`second.isAfterOrEqualTo(first)`) passaria **de qualquer jeito**, mesmo no contrafactual
  onde `applyIfMonotonic` aplicasse cegamente a nova amostra incorreta (~-1h): o `offset` viraria ≈-1h e
  cancelaria o salto de +1h, deixando `second` de volta perto de `first` — um empate na casa dos
  milissegundos, não uma falha confiável.
- **Impacto:** uma regressão que apagasse ou quebrasse o clamp (ex.: alguém "simplifica"
  `applyIfMonotonic` para sempre aplicar a amostra nova) não seria pega por este teste.
- **Correção:** afirmar sobre `clock.currentOffset()` diretamente, antes/depois do segundo `sync()` —
  deveria continuar igual ao valor pós-primeiro-sync, não sobre o `instant()` derivado.

### TEST-3 — Teste de "mantém offset anterior sob falha" não prova isso — ALTO
- **Onde:** `src/test/java/io/mohs/jdbc/DatabaseClockTest.java:101-116`
  (`keepsThePreviousOffsetWhenTheDatabaseIsUnreachable`).
- **Problema:** o teste constrói um `DatabaseClock` **novo** (`brokenClock`) diretamente contra o mock
  quebrado — ele nunca teve um `sync()` bem-sucedido. `assertThat(brokenClock.currentOffset())
  .isEqualTo(Duration.ZERO)` só prova que o valor default do campo (`private volatile Duration offset =
  Duration.ZERO`) sobrevive a uma `DataAccessException` capturada — não que um offset **não-zero
  previamente aprendido** sobrevive a uma resincronização que falha depois. O valor `offsetAfterSuccess`,
  de uma instância `clock` separada, é afirmado independentemente e nunca conectado a `brokenClock`.
- **Impacto:** a propriedade de resiliência que de fato importa — "uma indisponibilidade temporária do
  banco não zera/corrompe a sincronização de relógio do cluster" — tem cobertura zero. Uma regressão que
  resetasse `offset` para `Duration.ZERO` (ou qualquer outra coisa) numa resync falha, em vez de deixá-lo
  intocado, não seria pega.
- **Correção:** usar uma única instância de clock cuja conexão pode ser feita para falhar só na
  *segunda* chamada (proxy fino sobre o `DataSource`, ou mock com `.thenReturn(...).thenThrow(...)`),
  sincronizar com sucesso uma vez, então falhar, então afirmar que `currentOffset()` continua igual ao
  valor pós-primeiro-sync.

### TEST-4 — Prioridades `HIGH`/`BACKGROUND` nunca exercitadas; null-como-`NORMAL` não verificado em contexto — ALTO
- **Onde:** `src/test/java/io/mohs/jdbc/JdbcClaimerTest.java`, `claimOrdersHigherPriorityFirst:144-154`;
  produção: `JdbcClaimer.java:138-142` (o `CASE` de priorização no `ORDER BY`).
- **Problema:** confirmado por leitura completa do arquivo — só `'LOW'`, `'CRITICAL'`, `'NORMAL'`
  aparecem como literais em todo `JdbcClaimerTest`. `'HIGH'` e `'BACKGROUND'` nunca aparecem. Vários
  testes usam a sobrecarga de 3 argumentos de `seedExecution` (prioridade nula), mas nenhum combina uma
  execução com prioridade nula com siblings explicitamente priorizados no mesmo `claim()` para provar que
  null ordena onde o `ELSE 3` da SQL pretende (como `NORMAL`, entre `LOW` e `HIGH`).
- **Impacto:** um typo no branch `HIGH`/`BACKGROUND` do `CASE`, uma troca de ranking, ou quebrar o
  fallback null→NORMAL passariam completamente despercebidos — numa garantia de ordenação de claim que o
  projeto documenta explicitamente (ADR-0017) e que determina diretamente quais jobs rodam primeiro sob
  carga.
- **Correção:** estender `claimOrdersHigherPriorityFirst` (ou adicionar um teste irmão) semeando as 5
  prioridades explícitas mais uma execução com prioridade nula no mesmo lote, e afirmar a ordem completa
  resultante num único `containsExactly(...)`.

### TEST-5 — Matemática de compensação de round-trip do `DatabaseClock` sem teste que a distinga de uma versão quebrada — MÉDIO
- **Onde:** nenhum teste em `DatabaseClockTest.java` mira `DatabaseClock.java:101-113`
  (`appNowAtMidpoint = beforeQuery.plus(roundTrip.dividedBy(2))`).
- **Problema:** todo teste do arquivo usa `TOLERANCE = Duration.ofSeconds(2)`, mas o round-trip contra um
  H2 in-process é sub-milissegundo — nada distingue "midpoint correto" de "esqueceu de dividir por 2" ou
  "usou `t1` em vez do midpoint", ambos caberiam dentro da tolerância de 2s.
- **Correção:** envolver o `DataSource`/conexão com um delay artificial (proxy que dorme antes de
  devolver da query) e afirmar que o offset computado reflete o midpoint, não uma das pontas do
  round-trip.

### TEST-6 — `DatabaseClock.withZone(...)` sem nenhuma cobertura — MÉDIO
- **Onde:** produção `DatabaseClock.java:66-85`; zero referências a "withZone" em
  `DatabaseClockTest.java`.
- **Nota:** exatamente o tipo de delegação que `MutableClockTest.withZoneKeepsTheSameInstantUnderADifferentZone`
  já testa para a classe irmã — não existe equivalente aqui.

### TEST-7 — `AttemptTest` cobre 1 de 3 branches de validação — MÉDIO
- **Onde:** `src/test/java/io/mohs/core/execution/AttemptTest.java` (17 linhas, 1 teste); produção
  `Attempt.java:17-23` tem três checagens (`number < 1`, `startedAt` non-null, `outcome` non-null), só a
  primeira é testada — quebra o padrão que todo outro value object comparável neste projeto segue
  (`CronSpecTest`, `IntervalSpecTest`, `RateLimitTest`, `JobQueueTest`, `MohsRunnerTest`,
  `ExecutionTest.rejectsNullActor` testam cada branch individualmente).

### TEST-8 — `JobDefinitionTest` cobre 1 de 6 branches de validação — MÉDIO
- **Onde:** `src/test/java/io/mohs/core/definition/JobDefinitionTest.java`; produção
  `JobDefinition.java:41-50` — só `retries < 0` é testado; os `Objects.requireNonNull` de `key`,
  `handlerType`, `schedule`, `misfire`, `source` não têm teste direto, no record mais central do modelo
  de domínio.

### TEST-9 — `ExecutionWindow.excludeDaily` com `from > to` é um no-op silencioso — achado de produção, não só de teste — MÉDIO
- **Onde:** `src/main/java/io/mohs/core/resource/ExecutionWindow.java:67-75`; nenhum teste em
  `ExecutionWindowTest.java` cobre `from > to`.
- **Problema:** `excludeDaily(from, to)` implementa `!time.isBefore(from) && time.isBefore(to)`. Para
  `from > to` (ex.: `excludeDaily(22:00, 02:00)`, uma janela de manutenção noturna — configuração
  perfeitamente razoável), nenhum `LocalTime` satisfaz as duas condições ao mesmo tempo, então a exclusão
  silenciosamente não bate com **nada**, sem validação e sem documentação dessa limitação além de
  "meio-aberto `[from, to)`".
- **Impacto:** quem configurar uma janela noturna assim tem o job disparando exatamente quando achava que
  estava excluído — silenciosamente.
- **Correção:** suportar faixas que cruzam a meia-noite (dividir em dois predicados internamente), ou
  validar `from < to` e lançar com mensagem que ensina a dividir o intervalo — de qualquer forma,
  adicionar um teste para `from > to`.

### TEST-10 — `io.mohs.engine.BatchCounters` sem nenhum teste de validação — MÉDIO
- **Onde:** não existe `BatchCountersTest.java`; produção `src/main/java/io/mohs/engine/
  BatchCounters.java:12-17` tem a mesma checagem `total < 0 || succeeded < 0 || failed < 0` que
  `io.mohs.core.event.BatchCompleted` (essa sim testada, `BatchCompletedTest.rejectsNegativeCounters`) —
  não confundir os dois tipos, são registros diferentes com a mesma forma.

### TEST-11 — Guarda de `total` negativo em `JdbcBatchStore.create` sem teste — MÉDIO
- **Onde:** produção `src/main/java/io/mohs/jdbc/JdbcBatchStore.java:30-34`; nenhum teste em
  `JdbcBatchStoreTest.java` cobre isso.

### TEST-12 — `ArchitectureTest` sem regra para `synchronized`/`ThreadLocal` — MÉDIO
- **Onde:** `src/test/java/io/mohs/ArchitectureTest.java` (68 linhas inteiras).
- **Situação:** hoje inofensivo — busca em `src/main` por `synchronized`/`ThreadLocal` não encontra
  nada. Mas CLAUDE.md é enfático sobre os dois ("Proibido `synchronized` em caminho que bloqueia... Use
  `ReentrantLock`"; "`ScopedValue` em vez de `ThreadLocal`"), e o arquivo já estabelece o padrão mecânico
  exato para isso — `engine_never_reads_wall_clock_directly` (linhas 62-67) é um template pronto para
  copiar.
- **Impacto:** nada hoje, mas o primeiro `synchronized`/`ThreadLocal` que aparecer em
  `io.mohs.engine`/`io.mohs.jdbc` (o código concorrencial crítico que M3 está prestes a construir) sobe
  sem nenhuma guarda automatizada.
- **Correção:** adicionar as duas regras agora, antes de `io.mohs.engine` se preencher — barato, e a
  janela de retorno (implementação do motor em M3) é iminente.

### TEST-13 — `ArchitectureTest` sem regra verificando `@NullMarked` em todo pacote — BAIXO
- **Onde:** mesmo arquivo. Hoje inofensivo (os 23 `package-info.java` de `src/main` já têm
  `@NullMarked`, confirmado por grep) — prioridade menor que TEST-12 porque a ausência degrada sinal de
  análise estática, não causa bug de concorrência em runtime.

### TEST-14 — Interação entre dedupe de mutex por job e admissão de queue no mesmo lote não testada — BAIXO (plausível, não confirmado como bug)
- **Onde:** `JdbcClaimerTest.java` inteiro — cada eixo (queue cheia, dedupe de sibling) é testado
  isoladamente; nenhum teste combina os dois no mesmo lote (ex.: candidato 1 descartado pelo dedupe de
  sibling, candidato 2 da mesma queue então ocupa a vaga liberada). Dado que a implementação é um loop
  sequencial simples sobre as duas preocupações juntas, o risco é provavelmente baixo — registrado por
  ser exatamente a interação que a instrução de "olhar o que o teste cobre vs. o que a Javadoc/ADR
  afirma" pede para flagar.

### TEST-15 — Quatro testes de concorrência vazam o `ExecutorService` no caminho de exceção — BAIXO
- **Onde:** `JdbcClaimerTest.java:230,258`, `JdbcQueueStoreTest.java:98`, `JdbcBatchStoreTest.java:79`.
- **Problema:** `.shutdown()`/`.awaitTermination()` só rodam depois que os `Future.get(...)` têm
  sucesso — se uma asserção ou `TimeoutException`/`ExecutionException` disparar antes, o executor nunca é
  encerrado. Impacto prático mínimo (virtual threads são baratas, a JVM encerra ao fim da suíte de
  qualquer forma) — vale um `try (var executor = ...)` (`ExecutorService` é `AutoCloseable` desde o Java
  25) por higiene.

### TEST-16 — `DatabaseClockTest` usa nome de banco H2 fixo, ao contrário do resto do pacote — BAIXO
- **Onde:** `src/test/java/io/mohs/jdbc/DatabaseClockTest.java:56` — sem `UUID.randomUUID()`, diferente
  dos outros seis arquivos `jdbc/*Test.java`. Sem impacto prático hoje (esta classe nunca escreve em
  tabela nenhuma), mas é uma inconsistência que morde no dia em que alguém adicionar um teste aqui que
  toque schema.

### TEST-17 — Imutabilidade pós-construção de `Execution.attempts()` não testada diretamente — BAIXO
- **Onde:** `ExecutionTest.java` testa cópia defensiva na entrada (`copiesAttemptsDefensively`), não que
  a lista retornada rejeita mutação. Risco muito baixo — `List.copyOf` é uma garantia do próprio JDK, não
  lógica do projeto.

---

## 3.7 Conformidade com `../../CLAUDE.md` e higiene de dependências

A auditoria dedicada de conformidade verificou 16 regras específicas do `../../CLAUDE.md` do projeto contra a
árvore `src/main` inteira. **13 das 16 regras não têm nenhuma violação** — inclusive regras de
concorrência de alta prioridade (`synchronized`, `ThreadLocal`, `Semaphore`-vs-tamanho-de-pool, virtual
threads nomeadas) que hoje não têm código nenhum para violar, já que o motor concorrente ainda não foi
ligado. `@NullMarked` está presente nos 23 `package-info.java` de produção sem exceção. Os achados reais:

### CONV-1 — `spring-boot-starter-data-jpa` declarada e 100% não usada — MÉDIO
- **Onde:** `pom.xml:37-40`.
- **Confirmado:** busca exaustiva por `jakarta.persistence`/`javax.persistence`, `@Entity`, `@Table`,
  `JpaRepository`, `CrudRepository`, `EntityManager` em `src/main` e `src/test` não encontra nada. Toda a
  persistência é JDBC manual (24 usos de `NamedParameterJdbcTemplate`/`JdbcTemplate` em 8 arquivos),
  exatamente como `io.mohs.jdbc/package-info.java` já documenta ("Data Mapper, nunca JPA/Hibernate"). O
  único rastro é `spring.jpa.open-in-view: false` em `application.yaml`, que existe só para calar um
  warning que a *presença* do starter no classpath dispara — não configura uso real de JPA.
- **Relevância direta para o objetivo de multi-banco:** este starter puxa Hibernate como transitiva à
  toa — peso de classpath e tempo de boot sem nenhum benefício, além de ser potencialmente confuso (um
  leitor pode presumir que existem entidades JPA em algum lugar).
- **Correção:** trocar por `spring-boot-starter-jdbc` (mais leve, cobre `NamedParameterJdbcTemplate`/
  `JdbcTemplate`/`DataSource` sem trazer Hibernate/JPA/`spring-orm`), removendo também `open-in-view:
  false` se a dependência sair.

### CONV-2 — `spring-boot-starter-data-jpa-test` — mesmo problema, escopo teste — MÉDIO
- **Onde:** `pom.xml:67-71`. Zero uso de `@DataJpaTest`/`TestEntityManager`/`@AutoConfigureTestDatabase`
  em `src/test` — todos os `Jdbc*Test.java` configuram H2 manualmente. Remover junto de CONV-1.

### CONV-3 — `h2` e `spring-boot-h2console` sem `optional=true` — MÉDIO
- **Onde:** `pom.xml:34-36,62-66`. `spring-boot-devtools` está corretamente marcado
  `<scope>runtime</scope><optional>true</optional>` (não vaza transitivamente); `spring-boot-h2console` e
  `h2` não têm nem `scope` nem `optional` equivalentes (`h2` tem `scope=runtime` mas não `optional`).
- **Relevância direta para o objetivo de multi-banco:** como `io.mohs:mohs` é consumido como biblioteca
  por outras aplicações (ADR-0001), todo consumidor do build herda transitivamente `spring-boot-h2console`
  e um banco embarcado real (`h2`) no classpath, mesmo mirando Postgres/SQL Server exclusivamente.
- **Correção:** marcar ambos `optional=true`, a menos que H2 seja intencionalmente um backend de
  produção suportado (não afirmado em nenhum doc lido nesta revisão).

### CONV-4 — `io.github.robsonkades:uuidv7` declarada, zero uso em código — BAIXO
- **Onde:** `pom.xml:50-54`; `schema.sql:42` cita a lib num comentário, mas nenhum import/uso real existe
  em `src/main` ou `src/test`.
- **Confirmado:** `ExecutionId.of(...)` só é chamado lendo um id já existente do `ResultSet`
  (`JdbcExecutionStore`, `JdbcClaimer`) — nada em `src/main` gera um `ExecutionId` novo hoje. Diferente de
  outras dependências não usadas, esta é transparentemente documentada como trabalho futuro (o Javadoc de
  `ExecutionId` já diz "o motor decide o formato concreto... quando começar a gerá-los").
- **Correção:** manter se M3 realmente vai usar esta lib para gerar `ExecutionId`; senão, remover até a
  etapa que a consome chegar (YAGNI).

**Confirmado limpo, sem violação:** JSpecify (`@NullMarked` em todo pacote, `@Nullable` nunca em local,
nunca coexistindo com `Optional` para o mesmo conceito); zero `synchronized`; zero
`Executors.newFixedThreadPool`/`newCachedThreadPool`; zero `CompletableFuture` fazendo fan-out; `Semaphore`
citado corretamente como o mecanismo pretendido em `MohsRunner` (sem pool sizing usado como substituto);
zero `ThreadLocal`; nenhuma abstração especulativa (toda interface de implementação única tem
justificativa documentada — Repository/PoEAA, SPI nomeada, ou contrato aguardando M3); uso de
`Class.forName` em `JdbcJobStore` julgado legítimo (única forma não-reflexiva de resolver um tipo a partir
de string persistida, com tratamento de falha explícito); idioma (comentários/Javadoc em português fora
de `io.mohs.cron`, que preserva atribuição de licença Apache 2.0 em inglês por exigência legal — correto).

---

## 3.8 Duplicação, código morto e divergência de documentação

### DUP-1 — Padrão `Optional<X> find()` duplicado 5x — vale um helper — MÉDIO
- **Onde:** `JdbcJobStore.java:119-123`, `JdbcQueueStore.java:46-50`, `JdbcRateLimitStore.java:45-49`,
  `JdbcBatchStore.java:48-52`, `JdbcExecutionStore.java:84-88` — todos fazem
  `jdbcTemplate.query(sql, params, rs -> rs.next() ? mapRow(rs) : null)` seguido de
  `Optional.ofNullable(result)`, idêntico nas 5 ocorrências (todos os stores que têm `find()`).
- **Custo concreto:** o padrão certo (`rs.next()` guardado) já foi copiado corretamente 5 vezes por
  disciplina manual — não é uma garantia durável contra um sexto store usar por engano
  `jdbcTemplate.queryForObject(...)` (que lança `EmptyResultDataAccessException` em vez de devolver
  vazio).
- **Correção:** extrair um helper estático — o próprio `io.mohs.cron` já estabelece o padrão idiomático
  certo para isso (`Assert`/`StringUtils`, classes finais package-private com métodos estáticos) — um
  `JdbcSupport.findOne(jdbcTemplate, sql, params, mapper)` em `io.mohs.jdbc` seguiria a mesma convenção já
  usada no projeto.

### DUP-2 — Padrão upsert (UPDATE-depois-INSERT) duplicado 3x — cosmético — BAIXO
- **Onde:** `JdbcJobStore.java:85-110` (56 linhas), `JdbcQueueStore.java:36-39` (15 linhas),
  `JdbcRateLimitStore.java:35-38` (14 linhas).
- **Avaliação:** só a *forma* de controle (`int updated = ...; if (updated == 0) { ... }`) se repete —
  a maior parte de cada método (listas de coluna, texto SQL) é necessariamente diferente por tabela, e o
  risco real (CONC-2, colunas de UPDATE/INSERT divergindo) mora no texto SQL de qualquer forma, não no
  controle de fluxo. Extrair um helper aqui economiza ~2 linhas por call site sem reduzir o risco real —
  aceitável deixar como está, a menos que um quarto store adote a mesma forma.

### DUP-3 — `JdbcJobStore.mapRowOrNull` esconde falha em vez de usar o mecanismo ORPHANED que o próprio sistema já tem — MÉDIO-ALTO
- **Onde:** `src/main/java/io/mohs/jdbc/JdbcJobStore.java:163-173`; contraste com
  `adr/0006-registration-lifecycle-and-conflict-policy.md:36-42`.
- **Problema:** quando `handler_type` não resolve (`ClassNotFoundException`), a linha é logada em WARN e
  a linha vira `null`, que `find()` transforma em `Optional.empty()` e `findAll()` filtra silenciosamente
  (`.filter(Objects::nonNull)`). Mas a ADR-0006 já define um conceito de primeira classe — **ORPHANED** —
  exatamente para o cenário irmão (definição por annotation presente no store, ausente do código): ela é
  desenhada para continuar visível (`markOrphaned`, "não dispara, destaca no dashboard, WARN"), nunca
  desaparecer silenciosamente. Uma classe que falha ao carregar é uma falha *mais* severa que uma
  annotation simplesmente ausente (a classe nem existe/compila mais), mas recebe *menos* visibilidade:
  sem flag `orphaned`, sem entrada no dashboard, invisível para `find(key)` (que devolveria "job not
  found" em vez de mostrar um job orphaned), só um WARN de log de servidor.
- **Impacto:** assim que REST for ligado ao `JobStore` real (M3), um job cujo handler sumiu (deploy ruim,
  descompasso de jar, rename de pacote) some de `GET /jobs`/`GET /jobs/{jobKey}` por completo, em vez de
  aparecer pelo mecanismo ORPHANED que o sistema já construiu para o caso análogo.
- **Correção:** rotear essa falha para o mesmo estado `orphaned` (ex.: `mapRowOrNull` devolver um
  `StoredJob` com `orphaned=true` e um marcador, ou o chamador invocar `markOrphaned` quando isso
  acontecer), em vez de um modo de falha distinto e invisível.

### DUP-4 — `BatchResponse.pending` armazenado em vez de derivado — MÉDIO
- **Onde:** `src/main/java/io/mohs/rest/batch/BatchResponse.java:12-20` vs.
  `src/main/java/io/mohs/engine/BatchCounters.java:10-21`.
- **Problema:** `BatchCounters` (interno ao motor) acerta isso — `pending()` é método computado
  (`total - succeeded - failed`), nunca persistido, com Javadoc explícito ("`pending()` é derivado, nunca
  persistido"). `BatchResponse` (DTO REST) armazena `pending` como componente plano, sem validar
  `pending == total - succeeded - failed`; e como `io.mohs.rest` é proibido pelo ArchUnit de depender de
  `io.mohs.engine`, também não pode simplesmente delegar para `BatchCounters.pending()`.
- **Confirmado de graça para corrigir agora:** `BatchResponse` nunca é construído em lugar nenhum ainda
  (`BatchesController.get()` ainda lança `UnsupportedOperationException`) — corrigir antes que exista
  qualquer call site custa zero de migração.
- **Correção:** adicionar uma fábrica estática `BatchResponse.of(String batchId, int total, int
  succeeded, int failed)` que computa `pending` (e `state`, também derivável:
  `pending == 0 ? COMPLETED : RUNNING`) internamente — mesmo padrão de fábrica estática já usado em
  `JobDefinition.of`/`ExecutionId.of` neste projeto.

### DUP-5 — Divergência de doc: `@Internal` citado em três lugares, não existe no código — MÉDIO
- **Onde:** `docs/MOHS-DOCUMENTO-MESTRE.md:181`, `docs/API-DESIGN.md:622`,
  `adr/0001-single-module-packaging.md:31` — todos afirmam que `io.mohs.engine`/`io.mohs.jdbc` são
  marcados internos "(`@Internal`)", como se existisse uma annotation marcadora. Busca no `src/` inteiro
  não encontra `@Internal`/`interface Internal` em lugar nenhum — o mecanismo real
  (`ArchitectureTest.java:26-30`) é uma regra ArchUnit batendo contra uma lista fixa de nomes de pacote.
- **Correção:** implementar uma annotation `@Internal` leve (se a intenção é eventualmente checá-la via
  ArchUnit também), ou editar os três documentos para descrever o mecanismo real.

### DUP-6 — Divergência de doc: contagem de subpacotes de `io.mohs.rest` desatualizada — BAIXO
- **Onde:** `docs/MOHS-DOCUMENTO-MESTRE.md:638-641` diz "5 pacotes: raiz + `error`/`job`/`execution`/
  `resource`" — o pacote real tem **10** áreas (raiz + `batch`, `error`, `execution`, `job`, `node`,
  `overview`, `queue`, `ratelimit`, `runner`), e não existe `io.mohs.rest.resource` nenhum.
  `../../CLAUDE.md` (a fonte de verdade do projeto) já tem a lista correta e atual.
- **Correção:** atualizar a linha para bater com `../../CLAUDE.md`.

### DUP-7 — Divergência de doc: tabela de ADRs não lista a ADR-0017 — BAIXO
- **Onde:** `docs/MOHS-DOCUMENTO-MESTRE.md:566-583`, §8 "Plano de ADRs" — lista 0001-0016 e para. A
  `adr-claim-per-job-mutex-and-queue-admission.md` existe, está **Decided** (2026-08-13), e não
  é rascunho avulso — `JdbcClaimer.java` e `io.mohs.jdbc/package-info.java` citam "ADR-0017" repetidamente
  como autoridade do algoritmo em produção.
- **Correção:** adicionar a linha da ADR-0017 à tabela.

**Confirmado limpo, sem achados:** varredura de código morto em praticamente todo `io.mohs.core`,
`io.mohs.engine`, `io.mohs.jdbc`, `io.mohs.rest`, `io.mohs.test` não encontrou nenhum método/campo
privado ou package-private sem uso, nenhum bloco comentado, nenhuma classe de scaffolding esquecida —
achado positivo real, não ausência de esforço de busca. Interfaces de implementação única
(`ActorResolver`, `SyncableClock`, `Claimer`, `JobSpec`/`PolicySpec`) todas têm justificativa
arquitetural explícita no próprio Javadoc — não são abstração especulativa.

---

## 3.9 Parser de cron vendorizado (`io.mohs.cron`)

Este pacote foi comparado linha a linha contra o `org.springframework.scheduling.support` upstream (fonte
buscada diretamente do repositório `spring-projects/spring-framework`). Veredito: **portado com fidelidade
quase byte-a-byte** — ranges de campo, tratamento de `L`/`W`/`#`, o caso "30 de fevereiro" (nunca dispara,
`MAX_ATTEMPTS = 366` limita a busca corretamente), DST, atribuição de licença Apache 2.0 — tudo verificado
e correto. Dois achados reais, ambos sobre a portabilidade da anotação `@Nullable`, não sobre a lógica em
si:

### CRON-1 — `@Nullable` perdido em 9 assinaturas durante a portabilidade para JSpecify — MÉDIO
- **Onde:** `CronExpression.java:205` (`isValidExpression`), `:266` (`equals`);
  `BitsCronField.java:257` (`equals`); `CompositeCronField.java:80` (`equals`);
  `QuartzCronField.java:375` (`equals`); `Assert.java:36,42,48` (`notNull`, `hasLength`, `notEmpty`);
  `StringUtils.java:35,51` (`tokenizeToStringArray`, `delimitedListToStringArray`).
- **Confirmado:** o pacote é `@NullMarked`, então todo parâmetro sem anotação é contrato non-null. O
  Spring upstream marca `@Nullable` em todos os pontos acima — para `Assert`/`StringUtils` porque o
  propósito inteiro dos métodos é validar um valor que pode ser null; para `equals(Object)` porque é o
  próprio contrato de `Object`. A portagem removeu `@Nullable` nos nove pontos preservando a lógica
  (cada cabeçalho de arquivo até declara "no other functional changes").
- **Evidência concreta, não especulação:** o próprio teste do projeto,
  `CronExpressionTest.java:161`, já chama `CronExpression.isValidExpression(null)` diretamente — exatamente
  o caminho que a assinatura agora afirma que não pode acontecer. Por regra do próprio CLAUDE.md
  ("um parâmetro só leva `@Nullable` se puder genuinamente ser null em algum caminho real"), este é
  precisamente esse caso.
- **Impacto:** hoje nenhum plugin de análise estática (NullAway/Error Prone/Checker Framework) está
  fiado no `../../pom.xml`, então nada quebra o build — mas um futuro validador de config/REST que passe uma
  string possivelmente nula para `isValidExpression` seria sinalizado por inspeção JSpecify do IDE (ou
  NullAway, se algum dia adotado) como violação, apesar do método tratar null corretamente — obrigando um
  desenvolvedor a suprimir um warning real ou "corrigir" removendo uma guarda de null legítima.
- **Correção:** devolver `@Nullable` (já importado nos 4 arquivos que usam no tipo de retorno de
  `nextOrSame`) aos nove parâmetros; `Assert.java`/`StringUtils.java` precisam do import adicionado. Para
  `Assert.notEmpty`, o upstream usa `@Nullable Object @Nullable [] array` (array E elementos nuláveis) —
  mas todo call site real neste pacote sempre popula elementos non-null, então, por regra própria do
  projeto ("não anote por garantia"), `@Nullable Object[] array` (só a referência nulável) é o ajuste
  mínimo mais apropriado, não copiar a assinatura do Spring ao pé da letra.

### CRON-2 — `BitsCronField.bits` não é `final` — BAIXO (seguro hoje, hardening opcional)
- **Onde:** `src/main/java/io/mohs/cron/BitsCronField.java:41`.
- **Situação:** mutado por `setBit`/`clearBit`/`setBits` durante construção (idêntico ao upstream), nunca
  depois — `nextOrSame`/`toString`/`equals`/`hashCode` são todos somente-leitura. Seguro hoje porque o
  único call site (`NextFireCalculator.CRON_CACHE.computeIfAbsent`, via `ConcurrentHashMap`) publica o
  grafo de objeto já construído com a garantia de happens-before que `ConcurrentHashMap` documenta entre
  inserção e leituras subsequentes por outras threads.
- **Correção (opcional, não urgente):** acumular o padrão de bits numa variável local dentro do loop de
  `parseField` e passá-lo ao construtor de `BitsCronField`, então declarar `bits` `final` — bate com o
  próprio padrão Item 17 (Effective Java) que este projeto cita como referência obrigatória.

---

## 4. Tabela consolidada de achados

| ID | Severidade | Área | Arquivo(s) principal(is) | Resumo |
|---|---|---|---|---|
| DB-1 | Crítico | DB | schema.sql | `CREATE TABLE/INDEX IF NOT EXISTS` inválido em T-SQL |
| DB-2 | Crítico | DB | schema.sql | `TIMESTAMP` colide com `ROWVERSION` no SQL Server (9 colunas) |
| DB-3 | Crítico | DB | schema.sql | `CLOB` não existe em Postgres nem SQL Server |
| DB-4 | Crítico | DB | schema.sql, JdbcJobStore, JdbcClaimer | `BOOLEAN`/`TRUE`/`FALSE` inválidos em T-SQL |
| DB-5 | Crítico | DB | schema.sql | `VARCHAR` não-Unicode corrompe acentos silenciosamente no SQL Server |
| DB-6 | Crítico | DB | JdbcClaimer.java:144 | `LIMIT` inválido em T-SQL |
| DB-7 | Crítico | DB | JdbcClaimer.java:145 | `FOR UPDATE OF ... SKIP LOCKED` inválido em T-SQL — reescrita completa necessária |
| DB-8 | Alto | DB | pom.xml | Sem driver Postgres/SQL Server nem Testcontainers — achados acima invisíveis a CI |
| DB-9 | Alto | DB | application.yaml, autoconfigure | schema.sql não roda contra datasource não-embarcado |
| DB-10 | Médio | DB | docs/adr/0017 | Alegação de portabilidade da ADR não se sustenta com 3 bancos |
| DB-11 | Baixo | DB | JdbcClaimer.java:116 | Limite de 2100 parâmetros do SQL Server em `IN (:ids)` |
| DB-12 | Info | DB | DatabaseClock.java | `CURRENT_TIMESTAMP` confirmado portável |
| CONC-1 | Alto | Concorrência | JdbcQueueStore/QueueStore | `running_count` nunca decrementado — vazamento de vaga garantido |
| CONC-2 | Alto | Concorrência | JdbcJobStore/QueueStore/RateLimitStore | TOCTOU sem transação em upsert |
| CONC-3 | Médio | Concorrência | DatabaseClock.java:135-140 | Race condition no clamp monotônico (RMW não atômico) |
| CONC-4 | Médio | Concorrência | DatabaseClock.java | `sync()` sem agendamento, invariante de escritor único não garantida |
| CONC-5 | Alto | Concorrência | io.mohs.engine/jdbc | Sem lease renewal/heartbeat/reaper — RUNNING é beco sem saída |
| PERF-1 | Crítico | Performance | JdbcClaimer.java:67-71 | N+1 não reconhecido na hidratação pós-claim |
| PERF-2 | Alto | Performance | schema.sql, JdbcClaimer | Falta índice composto `(job_key, state)` |
| PERF-3 | Alto | Performance | application.yaml | HikariCP sem configuração para virtual threads |
| PERF-4 | Médio | Performance | JdbcClaimer.java:138-144 | ORDER BY de prioridade não servido por índice |
| PERF-5 | Médio | Performance | JdbcBatchStore.java | Mesmo padrão de hot-row que ADR-0009 já flagou para queues |
| PERF-6 | Baixo | Performance | JdbcClaimer.java:93-94 | Coleções sem capacidade pré-dimensionada |
| PERF-7 | Baixo | Performance | NextFireCalculator.java | Cache de cron sem eviction, lifecycle de singleton não garantido |
| PERF-8 | Baixo | Performance | JdbcJobStore.java:169 | `Class.forName` por linha, não é hot path hoje |
| API-1 | Alto | API/core | Mohs.java, ScheduleCommand.java | `@CheckReturnValue` ausente no caso mais comum de cadeia abandonada |
| API-2 | Alto | API/core | JobSpecImpl.java | Mutex cron×every "garantido pelo compilador" é falso no estilo não encadeado |
| API-3 | Alto | API/core | MohsRunner.java | Construtor canônico público contorna garantia dos builders |
| API-4 | Médio-Alto | API/core | ExecutionWindow.java | Record com equals()/hashCode() efetivamente identity-based |
| API-5 | Médio | API/core | JobDefinition.java:37 | `timeout` sem validação de positividade |
| API-6 | Médio | API/core | JobDefinition.java:31-33,38 | `runner`/`queue`/`window`/`retryPolicy` sem checagem de blank |
| API-7 | Médio | API/core | Attempt.java | `error` sem relação validada com `outcome == FAILED` |
| API-8 | Médio | API/core | Attempt.java | `outcome` aceita valores sem sentido (ENQUEUED, RETRY_SCHEDULED) |
| API-9 | Médio | API/core | Execution.java, Enqueued.java | `actor` não validado non-blank apesar de "inegociável" |
| API-10 | Médio | API/core | 6 arquivos em io.mohs.core.event | `attempt`/`nextAttempt` sem validação >= 1 |
| API-11 | Médio | API/core | BatchCompleted.java | Sem validação `succeeded + failed <= total` |
| API-12 | Médio | API/core | Attempt.java vs AttemptFailed/Failed.java | Mesmo conceito como String num tipo, Throwable noutro |
| API-13 | Baixo | API/core | ExecutionEventType.java | Espelha ExecutionEvent sem link em compile-time |
| API-14 | Baixo | API/core | ExecutionListener, ExecutionInterceptor, BatchBuilder | Sem `@FunctionalInterface` |
| API-15 | Baixo | API/core | 7 value objects | Validação non-blank duplicada sem helper |
| API-16 | Baixo | API/core | Batch.java:20 | `onCompletion` — contrato ambíguo em múltiplas chamadas |
| API-17 | Baixo | API/core | JobDefinition.java | Sem Javadoc por campo |
| API-18 | Médio | API/core | NextFireCalculator.java:32 | `CRON_CACHE` — campo de instância nomeado como constante |
| REST-1 | Alto | REST | JobsController, ExecutionsController | `@ResponseStatus(ACCEPTED)` é código morto sobre `ResponseEntity` |
| REST-2 | Alto | REST | RestExceptionHandler.java | Sem exception handler catch-all — RFC 7807 só para 3 exceções |
| REST-3 | Alto | REST | QueuePatchRequest, RateLimitPatchRequest | Mensagens de validação nunca chegam ao cliente |
| REST-4 | Médio-Alto | REST | ExecutionsController.java | `retry()` sem `Idempotency-Key` apesar do Javadoc afirmar paridade |
| REST-5 | Médio | REST | ExecutionsController.java:44-49 | `cancel()` não satisfaz `Location`, formato diverge de `retry()` |
| REST-6 | Médio | REST | ScheduleJobRequest.java | Cópia defensiva só rasa em `payload` |
| REST-7 | Médio | REST | ExecutionsController, JobsController, CursorPage | Sem parâmetro de tamanho de página; cursor sem contrato de opacidade |
| REST-8 | Médio | REST | 4 controllers, 7 handlers | Resolução de actor duplicada, sem hook compartilhado |
| REST-9 | Baixo-Médio | REST | 8 controllers | `/api/mohs/v1` duplicado como literal |
| REST-10 | Baixo | REST | HeaderActorResolver.java | Spoofável — intencional/documentado, gap residual pequeno |
| REST-11 | Baixo | REST | PayloadValidationException.java | `message` não validado non-null |
| REST-12 | Baixo | REST | RestExceptionHandlerTest.java | Testes de erro não passam por MockMvc real |
| REST-13 | Baixo | REST | todos os controllers | Primitive obsession em `@PathVariable String` |
| TEST-1 | Alto | Testes | JdbcClaimerTest.java:220-247 | Teste do mutex por job é empiricamente flaky (~1-2%) |
| TEST-2 | Alto | Testes | DatabaseClockTest.java:83-99 | Teste do clamp monotônico não testa o clamp de fato |
| TEST-3 | Alto | Testes | DatabaseClockTest.java:101-116 | Teste de "mantém offset" não prova isso |
| TEST-4 | Alto | Testes | JdbcClaimerTest.java | Prioridades HIGH/BACKGROUND nunca testadas no claim |
| TEST-5 | Médio | Testes | DatabaseClockTest.java | Matemática de midpoint sem teste discriminante |
| TEST-6 | Médio | Testes | DatabaseClockTest.java | `withZone` sem cobertura |
| TEST-7 | Médio | Testes | AttemptTest.java | 1 de 3 branches de validação testados |
| TEST-8 | Médio | Testes | JobDefinitionTest.java | 1 de 6 branches de validação testados |
| TEST-9 | Médio | Testes/Produção | ExecutionWindow.java:67-75 | `excludeDaily` com from>to é no-op silencioso |
| TEST-10 | Médio | Testes | BatchCounters.java | Sem nenhum teste de validação |
| TEST-11 | Médio | Testes | JdbcBatchStore.java:30-34 | Guarda de total negativo sem teste |
| TEST-12 | Médio | Testes | ArchitectureTest.java | Sem regra para synchronized/ThreadLocal |
| TEST-13 | Baixo | Testes | ArchitectureTest.java | Sem regra para @NullMarked |
| TEST-14 | Baixo | Testes | JdbcClaimerTest.java | Interação mutex+admissão de queue não testada junta |
| TEST-15 | Baixo | Testes | 4 testes de concorrência | ExecutorService vaza no caminho de exceção |
| TEST-16 | Baixo | Testes | DatabaseClockTest.java:56 | Nome de banco H2 fixo, inconsistente com o pacote |
| TEST-17 | Baixo | Testes | ExecutionTest.java | Imutabilidade pós-construção não testada diretamente |
| CONV-1 | Médio | Dependências | pom.xml:37-40 | `spring-boot-starter-data-jpa` declarada, nunca usada |
| CONV-2 | Médio | Dependências | pom.xml:67-71 | `spring-boot-starter-data-jpa-test` — mesmo problema |
| CONV-3 | Médio | Dependências | pom.xml:34-36,62-66 | `h2`/`h2console` sem `optional=true` — vaza para consumidores da lib |
| CONV-4 | Baixo | Dependências | pom.xml:50-54 | `uuidv7` declarada, zero uso |
| DUP-1 | Médio | Duplicação | 5 arquivos jdbc | Padrão `Optional<X> find()` duplicado — vale helper |
| DUP-2 | Baixo | Duplicação | 3 arquivos jdbc | Padrão upsert duplicado — cosmético |
| DUP-3 | Médio-Alto | Altitude | JdbcJobStore.java:163-173 | ClassNotFoundException some silenciosamente em vez de virar ORPHANED |
| DUP-4 | Médio | Simplificação | BatchResponse.java | `pending` armazenado em vez de derivado |
| DUP-5 | Médio | Doc drift | 3 docs | `@Internal` citado mas não existe no código |
| DUP-6 | Baixo | Doc drift | MOHS-DOCUMENTO-MESTRE.md:638-641 | Contagem de subpacotes REST desatualizada (5 vs 10) |
| DUP-7 | Baixo | Doc drift | MOHS-DOCUMENTO-MESTRE.md:566-583 | Tabela de ADRs não lista ADR-0017 |
| CRON-1 | Médio | Cron | 7 arquivos em io.mohs.cron | `@Nullable` perdido em 9 assinaturas na portabilidade JSpecify |
| CRON-2 | Baixo | Cron | BitsCronField.java:41 | Campo mutável não-final (seguro hoje, hardening opcional) |

**Total: 70 achados distintos** (12 DB, 5 Concorrência, 8 Performance, 18 API/core, 13 REST, 17 Testes,
4 Dependências, 7 Duplicação/doc, 2 Cron) — Crítico: 8 · Alto: 15 · Médio-Alto: 3 · Médio: 30 ·
Baixo-Médio: 2 · Baixo: 21 · Info: 1.

---

## 5. Recomendações — ordem de ataque sugerida

Dado que o objetivo declarado é suportar múltiplos dialetos e o projeto ainda está em desenvolvimento
(sem produção para proteger), a ordem abaixo prioriza desbloquear esse objetivo primeiro, depois fechar
os riscos de correção que já existem no código atual, deixando polish para o final.

1. **Decidir a estratégia de portabilidade de banco antes de escrever mais SQL cru.** DB-1 a DB-10 têm
   uma raiz comum: um único `schema.sql` de texto livre, sem camada de dialeto, tentando servir três
   bancos com sintaxes incompatíveis em pontos centrais (DDL inteiro, mais a query de claim). Adotar
   Flyway/Liquibase com migrations por vendor resolve DB-1, DB-2, DB-3, DB-4, DB-5 e DB-9 de uma vez só;
   a query de claim (DB-6, DB-7, DB-8) ainda vai precisar de um `JdbcClaimer` por vendor ou um despacho
   condicional, independente da escolha de ferramenta de migration — esse é o item de maior risco
   arquitetural do documento e vale uma ADR própria (ver DB-10) antes de mais código ser escrito em cima
   da SQL atual.
2. **Adicionar Testcontainers de Postgres e SQL Server à suíte (DB-8) antes ou junto do item 1** — sem
   isso, qualquer correção de portabilidade fica tão não-verificada quanto o código está hoje, e a
   próxima query nova volta a vazar sintaxe H2/Postgres-específica sem ninguém notar.
3. **Fechar CONC-1 (vazamento de vaga de queue) e CONC-2 (TOCTOU em upsert) como parte do design da
   etapa de conclusão de execução**, não como follow-up — são exatamente o tipo de "modo de falha entre o
   claim e a execução" que o próprio projeto se compromete a responder antes de considerar uma etapa
   pronta.
4. **Investigar TEST-1 (teste de mutex flaky) com prioridade alta, isoladamente.** É o teste do
   mecanismo mais crítico do sistema; até ficar determinístico (ou até a causa ser isolada como
   artefato de H2 e documentada como tal), a confiança na garantia de exclusão mútua sob concorrência real
   está mais fraca do que a ADR-0017 declara.
5. **Corrigir PERF-1 (N+1 no claim) e PERF-2 (índice faltando) junto do item 3** — ambos tocam o mesmo
   método (`JdbcClaimer`) que já vai ser revisado.
6. Os achados de **API/core** (Seção 3.4) e **REST** (Seção 3.5) são principalmente sobre travar
   contratos e invariantes agora, enquanto ainda são baratos de mudar (nada em produção depende deles
   ainda) — vale um passe dedicado antes de M3 começar a construir código real em cima dessas
   assinaturas, especialmente API-1, API-2, API-3 e REST-1/REST-2/REST-3, que são armadilhas concretas
   para quem for implementar por cima.
7. Os achados de **Testes** (Seção 3.6, exceto TEST-1) formam um padrão consistente e barato de
   corrigir: testar os branches de validação que já existem no código, seguindo a disciplina que o
   resto da suíte já demonstra.
8. **CONV-1/CONV-2/CONV-3** (dependências) e **DUP-5/DUP-6/DUP-7** (documentação) são baixo esforço,
   qualquer momento serve — não bloqueiam nada, mas ficam mais baratos de corrigir agora do que depois
   que mais código/documentação se acumular em cima da inconsistência atual.

---

## 6. Pontos fortes observados

Para contexto — esta revisão foi deliberadamente configurada para maximizar recall de achados (nove
ângulos paralelos, instrução explícita de super-reportar), então o volume de itens acima não deve ser
lido como "codebase problemática". Vale registrar o que a revisão confirmou como sólido:

- Suíte de testes verde (180/180), sem `Thread.sleep`, sem testes desabilitados, nomenclatura
  consistente e descritiva.
- Disciplina de imutabilidade e cópia defensiva (Effective Java Item 50) aplicada de forma consistente
  em praticamente todo `io.mohs.core` — as exceções encontradas (API-4, REST-6) são sutis o bastante
  para terem escapado de revisão manual normal.
- ArchUnit guardando fronteiras arquiteturais reais (API pública vs. interno, REST vs. engine/jdbc,
  test kit vs. produção, leitura direta de relógio no motor) — não é decoração, é enforcement executável.
- Dezessete ADRs, todas com contexto, decisão, alternativas rejeitadas e consequências — nível de
  documentação de decisão incomum para o estágio do projeto, e que tornou esta revisão sensivelmente
  mais precisa (foi possível avaliar código contra a intenção documentada, não só contra convenção
  genérica).
- O parser de cron vendorizado é fiel ao upstream do Spring Framework a ponto de resistir a uma
  comparação linha a linha contra o código-fonte original.
- O mecanismo de mutex por job do `JdbcClaimer` (ADR-0017) foi verificado linha a linha nesta revisão e
  está logicamente correto — o único problema encontrado nele é de portabilidade (Seção 3.1) e de teste
  (TEST-1), não de lógica.
