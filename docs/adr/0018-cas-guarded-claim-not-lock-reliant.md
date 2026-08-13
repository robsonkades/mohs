# ADR-0018: Mutex por job via CAS guardado, não dependente de lock especializado

## Status
Decided — 2026-08-13

## Context
Um code review completo da codebase (`docs/codereview.md`, achado
TEST-1) flagou `JdbcClaimerTest.claimIsMutuallyExclusiveAcrossConcurrentNodes`
— o teste que prova a ADR-0017 — como empiricamente flaky (~1-2% de
falha em ~120 execuções repetidas), sempre por falha de asserção pura
(nunca timeout, nunca deadlock). O review também apontou (DB-10) que a
comparação de portabilidade da ADR-0017 (`FOR UPDATE OF ... SKIP LOCKED`
vs. advisory lock) só valia entre H2/Postgres — nunca foi avaliada
contra SQL Server, que não tem `FOR UPDATE` de forma alguma.

Investigação: repetir o cenário 300x numa JVM só (não via `./mvnw test`
em loop — lento demais) confirmou uma taxa real de ~5% de violação no
algoritmo completo. Isolando a causa: uma consulta mínima — uma tabela,
uma linha, sem join nenhum, duas conexões JDBC cruas (zero código
Spring) disputando `SELECT ... FOR UPDATE SKIP LOCKED` via `CyclicBarrier`
(contenção genuína, não sequencial) — reproduziu o mesmo defeito a
**~33%**. Uma versão puramente sequencial da mesma consulta (uma
transação completa antes da outra começar) nunca falhou.

**Conclusão**: `SELECT ... FOR UPDATE SKIP LOCKED` do H2 2.4.240 tem uma
corrida real no próprio gerenciador de lock sob contenção genuína — sob
duas transações verdadeiramente concorrentes, a checagem "esta linha já
está travada?" e a aquisição do lock não são atômicas entre si com
frequência suficiente pra serem observáveis em centenas de tentativas.
Isso não é uma característica de timing do teste nem um problema do
desenho da ADR-0017 — é um defeito do motor de lock do H2 especificamente,
que a suíte usa como única verificação empírica disponível hoje.

Isso significa que a garantia central da ADR-0017 ("o lock de `j`
serializa claims entre nós") nunca foi realmente provada — o teste
passava na maioria das vezes por sorte, não porque o mecanismo fosse
correto pra valer. Não há como confiar em H2 pra verificar propriedades
de locking sob concorrência real até isso ser investigado/reportado
upstream — e mesmo com Postgres real (via Testcontainers, ainda não
adicionado), qualquer garantia de corretude que dependa inteiramente de
um lock especializado (`FOR UPDATE`, `SKIP LOCKED`) é, por definição,
tão confiável quanto a implementação de lock daquele banco especificamente.

## Decision
O lock (`SELECT ... FOR UPDATE OF e SKIP LOCKED`) continua existindo,
mas vira **puramente uma otimização** — reduz quantas transações
perdedoras fazem trabalho à toa competindo pelo mesmo candidato. A
**garantia de corretude real** passa a vir inteiramente de uma cadeia de
`UPDATE`s guardados (CAS, compare-and-set via linhas afetadas), cada um
atômico por construção — porque é uma escrita simples que qualquer
motor relacional serializa nativamente (bloqueio padrão de linha em
`UPDATE`), não uma leitura com lock especializado:

1. **Mutex por job**: `UPDATE mohs_job_definitions SET
   running_execution_id = :executionId WHERE job_key = :jobKey AND
   running_execution_id IS NULL` — nova coluna
   `mohs_job_definitions.running_execution_id`. 0 linhas afetadas = outra
   execução deste job já segura a vaga; candidato descartado.
2. **Admissão de queue**: `QueueStore.tryIncrementRunning` (já existia,
   etapa 3a) — sem mudança, já era um CAS guardado, nunca dependeu de
   lock de linha.
3. **Transição final pra `RUNNING`**: `UPDATE mohs_executions SET state =
   'RUNNING', ... WHERE id = :id AND state = 'ENQUEUED'` — deixou de ser
   um `UPDATE` em lote (`WHERE id IN (:ids)`) sobre a lista de candidatos
   já "aprovados" pelo lock; agora é um CAS por candidato, a garantia
   final contra double-claim da própria linha de execução — igual
   necessária mesmo pra jobs com `allowConcurrentExecutions = true`, que
   não têm mutex de job nenhum protegendo a linha.

Se um candidato adquire o mutex de job e/ou a vaga de queue mas falha
num passo seguinte (queue cheia, ou a transição final perde a corrida
pra outra transação), a reserva parcial é **desfeita explicitamente**
(`running_execution_id` volta a `NULL`, `QueueStore.decrementRunning`)
dentro da mesma transação — nunca fica presa. `QueueStore` ganhou
`decrementRunning` por causa disso (não existia até agora — etapa 3a só
tinha o incremento, porque não havia chamador pro decremento ainda).

O `SELECT` também trocou o filtro de mutex: em vez de `NOT EXISTS
(SELECT 1 FROM mohs_executions WHERE job_key = ? AND state = 'RUNNING')`
(uma subquery correlacionada, sem índice dedicado — PERF-2 do review),
passa a ser `j.running_execution_id IS NULL` — leitura direta de coluna,
mesma fonte de verdade que o CAS usa, sem subquery. E o lock deixa de
incluir `j` (`FOR UPDATE OF e` em vez de `FOR UPDATE OF e, j`) — não há
mais necessidade de travar a linha do job pra correção, então jobs com
`allowConcurrentExecutions = true` param de pagar um custo de
serialização que nunca precisaram (o desperdício que você apontou na
conversa que levou a esta ADR).

**Alternativa considerada e rejeitada: reportar/aguardar correção do H2.**
Mesmo que fosse um bug pontual do H2 corrigível, o argumento maior
permanece — uma garantia de corretude que depende inteiramente de um
mecanismo de lock especializado é, por construção, tão forte quanto a
implementação de lock de cada dialeto specific. CAS via `UPDATE` guardado
é o primitivo mais simples que todo banco relacional implementa
corretamente (é a base de qualquer transação), então é a escolha certa
como fundamento — lock/`SKIP LOCKED` como otimização em cima disso,
nunca como a própria garantia.

## Consequences
`JdbcClaimer` reescrito: `claimWithinTransaction` percorre candidatos
chamando `tryClaimCandidate` (mutex → queue → transição final, com
desfazimento em qualquer falha no meio do caminho) em vez do desenho
anterior (lock cobre tudo, um `UPDATE` em lote no fim). O dedupe de
siblings em memória (`claimedJobKeys`, um `HashSet` dentro de
`claimWithinTransaction`) **foi removido** — o CAS de mutex por job já
lida com dois siblings do mesmo job no mesmo lote automaticamente
(leitura-da-própria-escrita dentro da mesma transação), sem precisar de
bookkeeping em Java à parte.

Reverificado: 300 repetições do cenário de dois nós concorrentes
(mesmo desenho de antes — `CyclicBarrier`, `Executors
.newVirtualThreadPerTaskExecutor()`, threads virtuais genuinamente
concorrentes) rodadas numa JVM só após a correção — **0 violações em
300**, contra 15/300 (5%) antes. Toda a suíte `JdbcClaimerTest` já
existente passa sem alteração de asserção nenhuma — é regressão pura do
ponto de vista de comportamento observável.

Fica pendente: `running_execution_id` só é **adquirido** pelo claim;
**liberá-lo** quando uma execução termina (sucesso, falha ou timeout) é
responsabilidade da etapa de conclusão de execução (3b, ainda não
implementada) — sem isso, um job com mutex roda exatamente uma vez na
vida do processo e trava pra sempre depois. Mesma classe de dependência
rígida que o review já registrou pra `QueueStore.tryIncrementRunning`
(CONC-1) — agora com um segundo mecanismo (`running_execution_id`) na
mesma situação. Verificação real contra Postgres/SQL Server via
Testcontainers continua fora do escopo desta correção (plano de
multi-dialeto, ainda não iniciado).

## Source
`docs/codereview.md`, achados TEST-1 e DB-10. Investigação empírica
desta sessão (spike descartável com JDBC cru, não parte da suíte).
ADR-0017 (mecanismo original, superseded).
