# ADR-0026: Reaper sempre transiciona para FAILED terminal até retry scheduling existir

## Status
Superseded pela ADR-0033 — 2026-08-15 (os dois pré-requisitos nomeados abaixo foram
entregues; o reaper honra `retries` desde então). Decided — 2026-08-14

## Context
A ADR-0012 (`docs/adr/0012-liveness-heartbeat-lease-reaper.md`) decide
que, ao reclamar uma `Execution RUNNING` com lease expirada, "o
reclaimer trata o Attempt como falho, e a Retry Policy decide o resto"
— implicando que o reaper poderia transicionar a execução para
`RETRY_SCHEDULED` quando `JobDefinition.retries()` ainda permite mais
tentativas.

Implementando o reaper (`io.mohs.jdbc.JdbcReaper`), achei que essa
implicação não é executável hoje: a claim query
(`JdbcDialect.selectCandidates`, replicada nos 4 dialetos —
`H2JdbcDialect`, `PostgresJdbcDialect`, `MySqlJdbcDialect`,
`SqlServerJdbcDialect`) só seleciona candidatos com `WHERE e.state =
'ENQUEUED'`. Nenhum código, em nenhum lugar da base, jamais reivindica
uma execução `RETRY_SCHEDULED`. Se o reaper transicionasse uma execução
órfã para esse estado, ela ficaria **presa para sempre** — pior do que
ficar `RUNNING`, estado que ao menos o próprio reaper revisita a cada
ciclo.

Honrar `retries` de verdade exigiria decidir, agora, duas coisas que
pertencem ao desenho de retry/dispatch (3b, ainda não implementado), não
ao de liveness:
- Estender a claim query nos 4 dialetos para reconhecer
  `RETRY_SCHEDULED` como candidato (e decidir se isso é o mesmo caminho
  do claim normal ou um caminho separado — mistura prioridade de retry
  com prioridade de primeira execução?).
- Um algoritmo de backoff entre tentativas — hoje não existe nenhuma
  implementação de `RetryPolicy` na base (`JobDefinition.retryPolicy` é
  só o nome de um bean Spring opcional, sem SPI resolvida; a política
  default não tem forma alguma definida ainda).

Inventar as duas coisas agora, só para destravar o reaper, seria
antecipar decisão de dispatch sem o dispatch existir — exatamente o
tipo de "decisão ad-hoc no meio do caminho" que a ADR-0024 já registrou
que não quer para a assinatura de conclusão.

## Decision
O reaper desta rodada (`JdbcReaper.reclaimExpired`) sempre transiciona
a execução reclamada para `FAILED` terminal, nunca para
`RETRY_SCHEDULED` — independente de `JobDefinition.retries()`. Grava um
`Attempt` sintético (`outcome = FAILED`, `error` descrevendo lease
expirada) via `ExecutionStore.complete` (ADR-0024), liberando a vaga de
concorrência do job na mesma operação (ADR-0025).

`JobDefinition.retries()` fica **sem efeito no caminho de reclaim**
até que duas coisas aconteçam juntas, no momento em que 3b/dispatch for
desenhado: (1) a claim query passa a reconhecer `RETRY_SCHEDULED` como
candidato, nos 4 dialetos; (2) uma forma mínima de decisão de backoff
existe (nem que seja "retry imediato, sem backoff" como primeira
versão — mas isso é decisão de dispatch, não desta ADR).

## Consequences
Execuções cujo nó morre sempre esgotam o orçamento de retry em uma
única tentativa reclamada, mesmo que `retries` configure mais — uma
regressão comportamental *aparente* em relação ao que a ADR-0012
descreve em prosa, mas não uma regressão real: hoje não há caminho
nenhum (nem reclaim, nem dispatch) que já implemente retry de verdade,
então não existe comportamento anterior para regredir. Documentar isso
evita que alguém leia a ADR-0012 isoladamente, assuma que `retries` já
é honrado pelo reaper, e não descubra o gap até um incidente.

Quando a claim query for estendida para `RETRY_SCHEDULED`, a mudança
no reaper é local: trocar o `ExecutionState.FAILED` fixo por uma
decisão `attempts.size() < retries + 1 ? RETRY_SCHEDULED : FAILED` — a
assinatura de `ExecutionStore.complete` já suporta ambos os estados,
não precisa mudar.

## Source
ADR-0012 (`docs/adr/0012-liveness-heartbeat-lease-reaper.md`) — decisão
original que este ADR restringe. ADR-0024
(`docs/adr/0024-execution-completion-owned-by-execution-store.md`) —
`ExecutionStore.complete` que este reaper chama. ADR-0025
(`docs/adr/0025-reaper-releases-per-job-concurrency-slot.md`) —
liberação de vaga de concorrência, inalterada por esta ADR.
`JdbcDialect.selectCandidates` (`io.mohs.jdbc.dialect`, ADR-0022/0023)
— confirma `WHERE state = 'ENQUEUED'` como único filtro de estado nos 4
dialetos.
