# ADR-0024: Transição de conclusão de execução pertence a `ExecutionStore`

## Status
Decided — 2026-08-14

## Context
`ExecutionStore` deixa escrito, de propósito, que transição de estado
não é responsabilidade da etapa atual: "Transição de estado (claim,
conclusão, retry) não é responsabilidade desta etapa — entra junto do
claim/dispatch de verdade" (`ExecutionStore.java:19-22`). É um
adiamento deliberado, não um esquecimento — mas também não decide
**onde** a transição de conclusão vai morar quando 3b (dispatch, ainda
não implementada) chegar.

Já existe um precedente na base pra transição de estado de `Execution`:
`JdbcClaimer.tryTransitionToRunning` escreve `ENQUEUED → RUNNING` como
SQL cru direto em `mohs_executions`, **sem passar por `ExecutionStore`**
(`JdbcClaimer.java:174-184`). Isso foi uma decisão consciente da
ADR-0016, reafirmada na ADR-0018 e na ADR-0020: a consulta de claim
cruza duas tabelas (`mohs_executions` + `mohs_job_definitions`) — não
cabe numa porta de entidade só, então fica como SQL próprio do
`Claimer`. A ADR-0020 é explícita sobre isso ser a única exceção: "só a
query de seleção de candidatos como SQL cru próprio (é join entre duas
tabelas, não cabe em porta nenhuma — já resolvido assim desde a
ADR-0016)".

A transição de **conclusão** (`RUNNING` → `SUCCEEDED`/`FAILED`/
`RETRY_SCHEDULED`) é estruturalmente diferente: toca **só**
`mohs_executions`. Não há join, não há segunda tabela — a mesma
justificativa que tirou `RUNNING` de `ExecutionStore` não se aplica
aqui. Sem esta ADR, alguém implementando 3b teria que decidir isso
ad-hoc: repetir o padrão de SQL cru do `Claimer` (copiando a forma sem
copiar o motivo que a gerou) ou perceber por conta própria que
`ExecutionStore` é o dono natural da tabela.

## Decision
A transição de conclusão de execução vira responsabilidade de
`ExecutionStore` — um método novo (assinatura exata fica pra quando 3b
for desenhada de verdade; um formato provável é algo como `boolean
complete(ExecutionId id, ExecutionState expectedFrom, Attempt attempt)`,
mas esta ADR decide o **local** e o **porquê**, não a assinatura final)
em vez de SQL cru na camada de dispatch/reaper.

Mantém a disciplina de CAS guardado da ADR-0018: a escrita guarda pelo
estado atual esperado (`WHERE state = :expectedFrom`), nunca confia em
lock especializado — mesmo primitivo que `tryTransitionToRunning` já
usa pra `RUNNING`, só que dentro da porta certa desta vez, porque não
há segunda tabela justificando SQL cru.

`JdbcExecutionStore` passa a ser dono de **toda** escrita de ciclo de
vida em `mohs_executions`, com uma única exceção já justificada e
inalterada por esta ADR: a transição pra `RUNNING` continua em
`JdbcClaimer`, porque continua cruzando duas tabelas.

## Consequences
Nenhum código muda hoje — 3b (dispatch) ainda não existe, `io.mohs.engine`
segue esqueleto. Esta ADR existe pra que a implementação de 3b não
precise redescobrir essa decisão no meio do caminho, e pra dar à
ADR-0025 (reaper) uma operação concreta pra chamar em vez de duplicar
lógica de conclusão em paralelo.

Quando 3b for desenhada, a assinatura exata do método de conclusão
(quais estados de origem são válidos, o que acontece com `Attempt`,
se `RETRY_SCHEDULED` é uma "conclusão" ou um caso à parte) fica pra
aquele momento — esta ADR não antecipa esse desenho, só fixa que ele
mora em `ExecutionStore`.

## Source
`ExecutionStore.java:19-22` (adiamento explícito da transição de
estado). ADR-0016 (`0016-claim-and-running-are-atomic.md`) e
ADR-0018 (`0018-cas-guarded-claim-not-lock-reliant.md`) —
precedente do SQL cru do `Claimer` pra `RUNNING` e o motivo (join entre
duas tabelas) que não se repete na conclusão. ADR-0020
(`0020-per-job-concurrency-cap.md`) — confirma que a exceção
de SQL cru é só a consulta de candidatos, nada mais.
