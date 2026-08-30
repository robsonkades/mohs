# ADR-0016: Claim e transição para RUNNING são atômicos — sem estado intermediário

## Status
Decided — 2026-08-13

## Context
A Fase 1 (auditoria de 5 lentes sobre os contratos do M1, ver
`../MOHS-DOCUMENTO-MESTRE.md` §2) levantou uma lacuna: `ExecutionState`
(`ENQUEUED`, `RUNNING`, `RETRY_SCHEDULED`, `SUCCEEDED`, `FAILED`,
`CANCELLED`) não distingue "reivindicado por um nó" (claim bem-sucedido)
de "efetivamente executando" (handler já disparado). `Execution.firedAt`
é `@Nullable` "enquanto a execução ainda não disparou" — o que deixa
ambíguo se, entre o SQL de claim e o disparo real do handler, a linha
ainda parece `ENQUEUED` (livre pra outro nó reivindicar) ou já `RUNNING`
(protegida por lease). Se um nó morre exatamente nessa janela, o reaper
(M3, ADR-0012) precisa saber tratar esse caso — e o contrato, como
estava, não respondia isso.

## Decision
Claim e a transição para `RUNNING` são um único `UPDATE` atômico: a
mesma query que aplica `FOR UPDATE SKIP LOCKED` já grava
`ExecutionState.RUNNING` e inicializa a lease/heartbeat (ADR-0012) no
mesmo statement — não em dois passos. Não existe, e não é preciso
existir, um estado `ExecutionState.CLAIMED` separado: do ponto de vista
do banco, o momento em que uma execução deixa de estar disponível pra
outro nó **é** o momento em que ela já carrega lease. Um nó que morre a
qualquer momento depois do claim — mesmo antes do handler literalmente
começar a rodar — já deixou a linha em `RUNNING` com um heartbeat que vai
expirar; o reaper de execuções órfãs (ADR-0012) trata esse caso
exatamente como trata qualquer outro `RUNNING` com lease vencida, sem
lógica especial. `firedAt` continua `null` nesse intervalo (ele marca
quando o handler de fato disparou, não quando a linha foi reivindicada)
— o estado, não o timestamp, é quem carrega a garantia de exclusividade.

**Alternativa considerada e rejeitada: adicionar `ExecutionState.CLAIMED`.**
Daria visibilidade independente do momento do claim (útil pra dashboard
mostrar "reivindicado, prestes a rodar"), mas não muda nenhuma decisão do
reaper nem de nenhum outro consumidor do estado — `CLAIMED` e
`RUNNING`-com-heartbeat-vencido seriam tratados de forma idêntica em toda
lógica real. Estado que não muda nenhuma decisão observável é
complexidade sem retorno (YAGNI) — e o custo se propaga:
`ExecutionState` já é reaproveitado direto por `JobResponse`/
`ExecutionResponse` (M2), então um estado novo tocaria a API REST
publicada sem nenhum ganho comportamental.

## Consequences
M3 implementa o claim como um único `UPDATE` (`SET state = 'RUNNING',
lease_expires_at = ?, node_id = ? WHERE state = 'ENQUEUED' AND
next_fire_at <= now() ... FOR UPDATE SKIP LOCKED`, ou o equivalente de
contagem derivada se o gate de benchmark do ADR-0009 confirmar essa
direção) — não um claim seguido de um segundo `UPDATE` pra "iniciar". O
reaper (ADR-0012) não precisa de um caso especial pra "claimed mas nunca
chegou a rodar": ele já cobre isso tratando qualquer `RUNNING` com lease
vencida. `ExecutionState` permanece com 6 variantes, sem mudança de
contrato pra M2 (REST) nem pra nenhum consumidor já existente.

## Source
Fase 1 — Auditoria de 5 lentes sobre os contratos do M1
(`../MOHS-DOCUMENTO-MESTRE.md` §2), lente "Modos de falha", 2026-08-13.
