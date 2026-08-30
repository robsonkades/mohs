# ADR-0025: Reaper libera a vaga de concorrência por job ao reivindicar execução órfã

## Status
Decided — 2026-08-14

## Context
A ADR-0018 (`docs/adr/0018-cas-guarded-claim-not-lock-reliant.md:122-128`)
e a ADR-0020 (`docs/adr/0020-per-job-concurrency-cap.md:94-99`) já
registram, cada uma na própria seção "Consequences", que liberar
`running_execution_count` quando uma execução **termina de verdade**
(sucesso, falha ou timeout) é responsabilidade de "3b, ainda não
implementada". `JobStore.decrementRunningExecutions` já existe com essa
intenção documentada no próprio Javadoc: "Não é o decremento de
conclusão de execução (etapa de dispatch, ainda não implementada)"
(`JobStore.java:54-55`) — ou seja, o método certo já existe, só falta
alguém chamá-lo no momento certo.

Isso cobre o caminho **normal**: o próprio nó que está executando
termina e chama a conclusão. Existe um segundo caminho, estruturalmente
diferente, que nenhuma ADR cobre: a ADR-0012
(`0012-liveness-heartbeat-lease-reaper.md`) define o
reclaimer — quando a lease de uma `Execution RUNNING` expira (nó morto,
partição de rede, ou Handler zumbi que ignora cancelamento), "o
reclaimer trata o Attempt como falho, e a Retry Policy decide o resto".
**Nada na ADR-0012 menciona `running_execution_count`.** Esse é
justamente o caminho onde a conclusão normal **nunca vai acontecer** —
o processo original que deveria chamá-la está morto ou não coopera.

Isso é exatamente a classe de falha que a ADR-0009
(`docs/adr/0009-queue-enforcement.md:13-17`, hoje superseded, mas o
raciocínio permanece válido) registrou pro `JobQueue` antigo: "drift do
contador (um nó morre entre incrementar e decrementar → a vaga vaza
para sempre, sem reconciliação)". A própria ADR-0009 propunha contagem
derivada (sem contador mantido) especificamente pra eliminar essa
classe de bug, com dependência explícita do reaper pra devolver vagas
de nós mortos quando um contador mantido fosse usado: "execuções
RUNNING de um nó morto seguram a vaga até o reaper de órfãs (ADR-0012)
devolvê-las". A ADR-0020 reintroduziu um contador mantido
(`running_execution_count`) pro caso per-job **sem** referenciar esse
risco nem fechar essa dependência — a lacuna ficou implícita, não
decidida.

Sem esta ADR, o reaper pode ser implementado sem essa chamada, e o bug
só aparece em produção: um job com `preventOverlap()`/
`maxConcurrentExecutions` cujo nó morre no meio de uma execução trava
permanentemente com uma vaga a menos — exatamente o modo de falha "o
que acontece se o processo morrer entre o claim e a execução?" que o
CLAUDE.md do projeto cobra que todo código de concorrência responda.

## Decision
O reclaimer do reaper, ao reivindicar uma execução com lease expirada,
chama a **mesma operação de conclusão** decidida na ADR-0024
(`ExecutionStore`, não uma liberação de contador em separado) —
tratando o reclaim como uma conclusão como qualquer outra, mesmo que o
zumbi ainda esteja rodando fisicamente. Mesmo raciocínio que a
ADR-0012 já usa pra justificar a escrita terminal do zumbi perder a
CAS de versão em vez de corromper a do retry: o reclaim é definitivo
do ponto de vista do motor, independente do que o processo morto ainda
esteja fazendo.

Concretamente: a operação de conclusão de `ExecutionStore` (ADR-0024)
é responsável por chamar `JobStore.decrementRunningExecutions(jobKey)`
como parte do mesmo passo — não dois códigos independentes (um em 3b,
outro no reaper) que podem divergir silenciosamente se só um dos dois
for atualizado no futuro. Um único caminho de código libera a vaga,
chamado tanto pela conclusão normal quanto pelo reclaim.

## Consequences
Fecha o risco de drift que a ADR-0009 registrou pro `JobQueue` antigo e
que a ADR-0020 reintroduziu pro contador per-job sem endereçar. Nenhum
código muda hoje — reaper e 3b ainda não existem (M3 em andamento);
esta ADR existe pra que a implementação do reaper nasça com essa
chamada, não a esqueça e precise de um incidente de produção pra
descobrir a lacuna.

Depende da ADR-0024 existir de fato (o reclaimer chama a operação de
conclusão de `ExecutionStore`, não reimplementa a liberação por conta
própria) — as duas ADRs devem ser implementadas juntas quando 3b/reaper
entrarem em desenvolvimento, não uma sem a outra.

## Source
ADR-0009 (`0009-queue-enforcement.md`) — precedente do risco
de drift de contador sob morte de nó. ADR-0012
(`0012-liveness-heartbeat-lease-reaper.md`) — mecanismo do
reclaimer. ADR-0018 (`0018-cas-guarded-claim-not-lock-reliant.md`)
e ADR-0020 (`0020-per-job-concurrency-cap.md`) — pendência já
registrada em "Consequences" de cada uma, nunca fechada. ADR-0024
(`0024-execution-completion-owned-by-execution-store.md`) —
operação compartilhada que este reclaim chama. `JobStore.java:39-57`
(`tryIncrementRunningExecutions`/`decrementRunningExecutions`, já
existentes e documentados como pendentes de um chamador de conclusão).
