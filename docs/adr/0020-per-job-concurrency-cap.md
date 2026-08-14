# ADR-0020: Teto de concorrência por job (maxConcurrentExecutions)

## Status
Decided — 2026-08-13

## Context
`allowConcurrentExecutions` (ADR-0019, default `true`) só expressa dois
pontos: "1" (`preventOverlap()`) ou "ilimitado". Um caso concreto expõe
a lacuna: dois jobs distintos, `report-summary` e `report-complete`,
cada um com handler que compartilha um recurso externo com capacidade
própria por `job_key` (não entre jobs diferentes — para isso já existe
`JobQueue`) — um quer no máximo 10 execuções simultâneas, o outro no
máximo 5. Nenhum valor de `allowConcurrentExecutions` sozinho expressa
"até N", só "1 ou ilimitado".

A generalização óbvia seria trocar por `maxConcurrentExecutions:
Integer` (`null` = ilimitado) — mas isso não funciona limpo em
`@MohsJob`: atributo de annotation não aceita tipo boxed, só primitivo.
Um `int` sozinho precisaria de um valor sentinela ambíguo (`0`? `-1`?)
pra "sem limite" na própria annotation, misturando "não configurado"
com "configurado para zero".

## Decision
Mantidos **dois campos** em `JobDefinition`: `allowConcurrentExecutions`
(boolean, liga/desliga o teto) e `maxConcurrentExecutions` (int, só lido
quando o primeiro é `false`). Validados no construtor canônico
espelhando exatamente o padrão já usado em `MohsRunner` (ADR-0014,
API-3) — campos condicionados por um modo/flag são validados como par,
não isoladamente: o campo do modo inativo precisa ser um sentinela fixo
(`0`), não qualquer valor "razoável":

```java
if (allowConcurrentExecutions) {
    if (maxConcurrentExecutions != 0) throw new IllegalArgumentException(...);
} else if (maxConcurrentExecutions < 1) {
    throw new IllegalArgumentException(...);
}
```

`PolicySpec.preventOverlap()` continua como açúcar sintático pra
`maxConcurrentExecutions(1)` — mesmo default (`true`/concorrência
permitida) da ADR-0019, mesmo nome, mesmo caso comum bem coberto sem
argumento. `PolicySpec.maxConcurrentExecutions(int max)` é o novo
método pro caso geral. `@MohsJob.maxConcurrentExecutions()` tem default
`0` (mesmo sentinela) — se alguém marcar `allowConcurrentExecutions =
false` sem setar um valor, o boot falha com mensagem clara em vez de
assumir 1 silenciosamente.

Mecanicamente, isso generaliza o CAS de "um dono" (`running_execution_id`,
ADR-0018) pra um CAS de contador — `running_execution_count <
max_concurrent_executions`, mesmo idioma que `QueueStore.tryIncrementRunning`/
`decrementRunning` já usa e já tem concorrência provada (`JdbcQueueStoreTest`),
agora numa coluna própria de `mohs_job_definitions`
(`JobStore.tryIncrementRunningExecutions`/`decrementRunningExecutions`).
Deliberadamente **não acoplado** à tabela `mohs_job_queues`: `job_key` e
nome de queue continuam sendo conceitos distintos — um recurso privado
de um job (`maxConcurrentExecutions`) vs. um recurso compartilhado entre
jobs diferentes (`JobQueue`). O mesmo idioma de SQL se repete nas duas
tabelas, mas isso é duplicação pequena e aceitável (CLAUDE.md: "prefira
uma pequena duplicação a um acoplamento errado") — forçar as duas coisas
por trás de uma tabela só criaria uma dependência entre dois conceitos
que o domínio trata como independentes.

`JdbcClaimer` ganha uma segunda vantagem colateral desta mudança: hoje
ele já depende de `QueueStore` (porta) pra admissão de queue, mas fazia
SQL cru inline pro mutex de job (`tryAcquireJobMutex`/`releaseJobMutex`).
Essa inconsistência é resolvida junto — `JdbcClaimer` passa a depender
também de `JobStore` e delega os dois lados (job e queue) pra suas
respectivas portas, mantendo só a query de seleção de candidatos como
SQL cru próprio (é join entre duas tabelas, não cabe em porta nenhuma —
já resolvido assim desde a ADR-0016).

**Alternativa considerada e rejeitada: `Integer maxConcurrentExecutions`
sozinho, sem `allowConcurrentExecutions`.** Funcionaria na API
programática (`JobSpec`), mas não em `@MohsJob` — atributo de annotation
não aceita tipo boxed. Forçaria dois formatos de configuração
incompatíveis (um pra cada caminho) só pra evitar um segundo campo
booleano, pior troca do que manter os dois campos com a disciplina de
validação em par.

## Consequences
`running_execution_id VARCHAR(255)` (ADR-0018) é substituído por
`running_execution_count INT NOT NULL DEFAULT 0` — mudança de schema
direta, sem migração, já que a coluna anterior foi adicionada na mesma
sessão sem consumidor externo algum. `StoredJob` ganha
`runningExecutionCount`, mesmo tratamento que `StoredQueue.runningCount`
já tinha. O dedupe de siblings do mesmo job no mesmo lote de claim
continua correto sem lógica nova: com `max = 1`, o segundo sibling
tenta incrementar de `1` pra `2`, falha (`1 < 1` é falso) — igual antes.
Com `max = 3` e 5 siblings, os 3 primeiros conseguem, os 2 últimos
falham (`3 < 3` é falso) — o CAS generaliza pra qualquer N sem
bookkeeping extra em Java.

Fica pendente, na mesma situação já registrada pela ADR-0018 (CONC-1):
liberar a vaga quando uma execução **termina de verdade** (sucesso,
falha ou timeout) é responsabilidade da etapa de conclusão de execução
(3b, ainda não implementada) — o que esta ADR cobre é aquisição e
desfazimento dentro da mesma transação de claim (quando um passo
seguinte falha), não liberação pós-execução.

## Source
Conversa desta sessão, motivada por um exemplo concreto (dois jobs de
relatório com tetos de concorrência distintos). Estende a ADR-0018 (CAS
guardado como garantia de corretude) e a ADR-0014/API-3 (campos
condicionados por modo, validados no construtor canônico).
