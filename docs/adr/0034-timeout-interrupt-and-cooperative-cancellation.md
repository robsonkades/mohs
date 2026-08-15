# ADR-0034: Timeout por interrupt e cancelamento cooperativo

## Status
Decided — 2026-08-15. Completa a escada de liveness da ADR-0012 (que decidiu o Watchdog
Bound como último degrau e adiou os anteriores) e implementa o passo 3 do shutdown gracioso
de `docs/API-DESIGN.md` ("estouro do grace: interrupt pela maquinaria de timeout").

## Context
`JobDefinition.timeout` era persistido mas sem efeito; `JobContext.cancellationRequested()`
retornava `false` fixo; `POST /executions/{id}/cancel` respondia 501; o estouro do grace de
drain deixava o in-flight rodando com um WARN. O Javadoc do `Engine` declarava a limitação:
"sem mecanismo de interrupt — zumbi continua rodando até terminar sozinho". A escada
decidida na ADR-0012 exigia os degraus que faltavam: `timeout` do job avaliado **em
memória** (nunca SQL — a renovação de lease é deliberadamente cluster-wide e em lote),
interrupt, e só então o Watchdog Bound como rede de segurança.

## Decision

**Um sinal por attempt, em memória.** Cada dispatch cria um `CancellationSignal`
(engine-interno): flag de cancelamento + razão (`TIMEOUT`/`SHUTDOWN`/`MANUAL`, a primeira
vence) + registro da thread do handler. O interrupt só é entregue enquanto a thread está
registrada — registro, interrupt e desregistro compartilham o mesmo lock explícito (o
problema clássico do `FutureTask.cancel`, JCIP cap. 7), e o desregistro limpa o status de
interrupt **antes** da escrita de conclusão: JDBC nunca roda com interrupt pendente e uma
thread de plataforma de runner CPU nunca volta envenenada ao pool.
`JobContext.cancellationRequested()` passa a ler esse sinal — deixa de ser `false` fixo.

**Timeout: sinaliza + interrompe; desfecho passivo.** O deadline é verificado de carona no
tick (a varredura do in-flight que a renovação de lease já faz) — granularidade de até 1
`poll-interval` de atraso, irrelevante para timeouts de job (minutos), zero thread nova, e
continua ativo em `PAUSED`/`DRAINING`. Medido do **início real do handler** (carimbo
monotônico no dispatch — fila de runner não conta; o Watchdog Bound continua medindo
submit→agora de propósito: fila entupida também é problema de liveness). Ao disparar:
flag + interrupt. O desfecho é gravado quando o handler **realmente parar** — nunca
ativamente no deadline: falhar o attempt com o handler vivo liberaria o slot de concorrência
por job com a thread ainda ocupada (um `preventOverlap` rodaria sobreposto no mesmo node) e
duplicaria a corrida de conclusão que o CAS já resolve. Handler surdo ao interrupt é
exatamente o caso do Watchdog Bound — a rede de segurança já existe, não se duplica aqui.

**Mapeamento de desfecho pela razão do sinal, só em saída anormal.** Exceção do handler com
sinal disparado: `TIMEOUT` → falha com causa `attempt exceeded job timeout` e **segue o
orçamento de retry** normal; `SHUTDOWN` → falha com causa NodeShutdown e segue o retry
normal (at-least-once honesto até no desligamento — API-DESIGN); `MANUAL` → `CANCELLED`
terminal + evento `Cancelled`, **sem retry** (reagendar o que o operador mandou parar
contradiz a ordem; cancel vence orçamento). Retorno normal com sinal disparado é
`SUCCEEDED` — o trabalho terminou; registrar falha mentiria e agendaria uma duplicata.

**Cancel manual viaja pelo banco.** `POST /executions/{id}/cancel` → `Mohs.cancel(id)`:
primeiro o CAS `ENQUEUED/RETRY_SCHEDULED → CANCELLED` (o mesmo padrão que `remove()` já
usa; pendente cancelado nunca rodou, não publica `Cancelled` — o evento exige attempt ≥ 1);
se a execução está `RUNNING`, grava `cancel_requested = TRUE` na linha — o único canal que
alcança o node dono em outro processo. O node dono lê as flags do próprio in-flight em lote
a cada tick (SELECT por PK, só quando há in-flight) e levanta o sinal `MANUAL` — **flag,
sem interrupt**: cancel é cooperativo por contrato ("não imediato", REST-API-DESIGN); quem
força é o `timeout` do job e o Watchdog, que continuam valendo. Staleness real ≤ 1
`poll-interval` (default 5s) — o Javadoc de `JobContext` dizia "~1s" e passa a dizer a
verdade. 202 com o estado corrente, `Location`, 404 se não existe.

**Reaper honra `cancel_requested`.** Reclaim de lease expirada com a flag ligada termina em
`CANCELLED` (não `RETRY_SCHEDULED`, não `FAILED`): o nó morreu, mas a ordem do operador já
estava dada — reagendar seria desobedecê-la; `Failed.attemptsExhausted` não se aplica
(publica `Cancelled`).

**Escalada no drain.** Grace estourado: cada in-flight recebe `SHUTDOWN` (flag + interrupt)
e o `awaitInFlight` retorna — os attempts falham assincronamente com causa NodeShutdown e
seguem o retry normal pelo caminho de conclusão de sempre; não há segunda janela de espera
configurável (YAGNI). Durante o grace, nada muda: drain ≠ cancel (ADR-0007).

## Consequences
- A escada completa passa a existir: `timeout` (flag + interrupt) → Watchdog Bound (para de
  renovar) → lease expira → reaper. Cancel manual e shutdown pegam carona nos mesmos
  degraus.
- (Revisão do ciclo) Drop de renovação **marca** a encarnação (`renewalStopped`) em vez de
  removê-la do mapa de in-flight — remoção só na conclusão. O zumbi continua alcançável
  pela escalada de drain e pelo poll de cancel: pra job sem timeout, o interrupt do
  shutdown é a única chance de pará-lo. Exceção única: re-claim do próprio node pro mesmo
  id substitui a entrada marcada (o mapa comporta uma encarnação por id) — o zumbi antigo
  volta a ficar inalcançável, mesma lacuna de antes da marca. Complementos do mesmo review: checagem pré-start
  do sinal no dispatch (task enfileirada com MANUAL/SHUTDOWN nem roda o handler) e
  `succeed()` fora do try do mapeamento — falha da escrita de sucesso propaga (RUNNING até
  o reaper, indistinguível de crash) em vez de ser reclassificada pelo sinal.
- `Mohs` ganha `cancel(ExecutionId)` (mudança de API pública aprovada); `mohs_executions`
  ganha `cancel_requested BOOLEAN NOT NULL DEFAULT FALSE` nos 4 dialetos.
- A flag persiste entre attempts: se o attempt falhar antes do node observar a flag, o
  retry pode **começar** — e é cancelado em ≤ 1 poll-interval pelo poll do tick seguinte.
  Aceito: a alternativa (predicado novo na claim query, a mais quente do sistema) não entra
  sem medição — candidata a DBTUNE se o harness mostrar que importa.
- `cancel_requested` fica `TRUE` em execução que terminou `SUCCEEDED` (o trabalho venceu a
  corrida) — registro histórico honesto, não é limpo.
- O SELECT de flags por tick é query nova na tabela mais quente (por PK, batelada,
  condicional a in-flight não vazio) — candidata a medição no `LivenessLoadHarness`, sem
  números alegados aqui.
- Estado da arte: Quartz exige `InterruptableJob` (opt-in por implementação); JobRunr faz
  cancel cooperativo via polling de estado no banco, como aqui. A escada com teto
  (timeout → interrupt → watchdog → reaper) de série continua diferencial do Mohs.

## Source
docs/API-DESIGN.md "Shutdown gracioso" (passo 3) e "Watchdog Bound" (lines 211-240);
docs/adr/0012-liveness-heartbeat-lease-reaper.md; docs/REST-API-DESIGN.md
(`POST /executions/{id}/cancel`); decisões de escopo/desfecho/mecanismo aprovadas em
2026-08-15 (escopo completo com REST + banco; desfecho passivo; carona no tick).
