# ADR-0005: Listeners × Interceptors

## Status
Decided — 2026-08-12

## Context
O Mohs precisa de duas capacidades de extensão distintas: observar o que
acontece com uma execução (métricas, alertas, integrações) e
interceptar/envolver a execução em si (MDC, tracing, contexto). O Quartz
funde as duas em uma única SPI (`JobListener`), citado como o
anti-exemplo a evitar: observar e interceptar têm contratos opostos — um
não deveria poder afetar o job, o outro precisa poder participar do
desfecho.

## Decision
Duas SPIs separadas:

- **`ExecutionListener`** — observar, nunca interferir. Eventos `sealed`:
  `Enqueued, Started, AttemptFailed, RetryScheduled, Succeeded, Failed,
  Cancelled, BatchCompleted` (pattern matching; release novo = compilador
  avisa). Exceção de listener é capturada, logada e metrificada — jamais
  afeta o job. Dispatch assíncrono em executor próprio de virtual threads
  (`mohs-events`), ordem preservada por execution. Açúcar:
  `@OnExecution(job=..., event=FAILED)` em método. Bridge para
  `ApplicationEvent` do Spring é opt-in (`mohs.events.spring-bridge=true`).
- **`ExecutionInterceptor`** — envolver a execução (`intercept(ctx,
  chain)`), na thread do attempt, em cadeia ordenada (`@Order`); é o
  lugar de MDC, spans de tracing e `ScopedValue`. Exceção de interceptor
  É falha do attempt e segue o fluxo normal de retry.

## Consequences
Eventos in-process via `ExecutionListener` são **best-effort**: um crash
entre persistir o desfecho e despachar o evento pode perdê-lo. Reação
garantida a um desfecho não pode depender de listener — o handler precisa
enfileirar o job de continuação dentro da própria transação (cláusula 4
do contrato assíncrono, ADR-0003); o listener observa, o job encadeado
reage. As integrações oficiais de Micrometer e OpenTelemetry do Mohs são
implementadas sobre estas duas SPIs (dogfooding) — se a observabilidade
oficial precisar de um hook que a SPI não oferece, a conclusão é que a
SPI está errada e deve ser corrigida, não que uma porta interna deve ser
aberta. Entrega durável/cluster-wide de eventos (SSE, webhooks) fica fora
do escopo atual e nasce de uma futura tabela de eventos.

## Source
docs/API-DESIGN.md "Observação e extensão — Listeners e Interceptors
[DECIDIDO]" (lines 402-465); docs/MOHS-DOCUMENTO-MESTRE.md §5.10 (lines
397-417)
