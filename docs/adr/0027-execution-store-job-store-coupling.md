# ADR-0027: Acoplamento deliberado entre `ExecutionStore` e `JobStore` em `complete`/`completeAll`

## Status
Decided — 2026-08-14

## Context
`Claimer` e `Reaper` (`io.mohs.engine`) justificam explicitamente, nos mesmos termos um do outro, por
que não são um `*Store`: "não é Repository de uma entidade só... não cabe numa porta de entidade só"
— os dois cruzam `mohs_executions` e uma segunda tabela/porta (`mohs_job_definitions`/`JobStore`)
dentro de uma transação própria que eles mesmos abrem, então ganham uma porta dedicada em vez de
entrar em qualquer `*Store` existente.

`ExecutionStore.complete`/`completeAll` (ADR-0024, ADR-0025) fazem, à primeira vista, a mesma coisa:
recebem `JobStore` como parâmetro e chamam `jobStore.decrementRunningExecutions(...)` na mesma
operação que grava o `Attempt` e transiciona o estado da execução — um cruzamento de porta dentro de
um tipo nomeado como se cuidasse só de uma entidade (`Execution`). Um review de nomenclatura e
organização (`docs/codereview-naming.md`, achado RESP-1) identificou essa tensão entre a regra
articulada (Claimer/Reaper) e o que parece uma exceção não articulada (`ExecutionStore`).

## Decision
`ExecutionStore.complete`/`completeAll` continuam recebendo `JobStore` como colaborador por chamada,
em vez de virarem uma porta própria (`ExecutionCompleter` ou similar). A diferença que justifica o
tratamento diferente de `Claimer`/`Reaper` não é "cruza ou não cruza tabela/porta" — é *quem abre a
transação*: `Claimer`/`Reaper` abrem a própria transação e decidem, por si, cruzar duas tabelas dentro
dela; `ExecutionStore.complete`/`completeAll` não abrem transação nenhuma — só participam de uma que o
chamador (`JdbcReaper`, e futuramente o dispatch) já abriu, recebendo `JobStore` como o colaborador que
o chamador escolhe (mesmo `DataSource`, mesma transação). Extrair uma terceira porta para isso, sem um
segundo formato de uso real além de "grava `Attempt` + libera vaga", seria abstração sem o segundo caso
que a justificaria (`CLAUDE.md`: "interface com uma única implementação é indireção, não abstração").

## Consequences
`ExecutionStore` continua sendo a única porta que sabe transicionar uma `Execution` para estado
terminal — reaper e dispatch (quando ligado) compartilham exatamente o mesmo caminho, sem duplicar a
lógica de "grava Attempt, libera vaga, tudo atômico". O custo aceito: a assinatura de
`complete`/`completeAll` é mais larga que a de um Repository comum de uma entidade só, e um leitor que
só olhar a lista de métodos de `ExecutionStore` (sem ler o Javadoc) pode estranhar por que `JobStore`
aparece ali. Se um segundo consumidor real do mesmo tipo de cruzamento aparecer fora de
`complete`/`completeAll` — não hipotético, um caso concreto — a decisão desta ADR deve ser revisitada;
até lá, manter como está.

## Source
`docs/codereview-naming.md`, achado RESP-1 (2026-08-14); `io.mohs.engine.Claimer`/`Reaper` (Javadoc,
"não é Repository de uma entidade só"); ADR-0024, ADR-0025.
