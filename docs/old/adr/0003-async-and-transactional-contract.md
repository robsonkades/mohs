# ADR-0003: Contrato assíncrono e transacional

## Status
Decided — 2026-08-12

## Context
Toda invocação (`schedule`, `batch`) precisa de uma semântica clara sobre
o que o chamador pode esperar do retorno da chamada: se o handler roda
in-line ou não, se o retorno garante durabilidade, o que o valor de
retorno representa, como isso interage com transações do chamador, e o
que acontece quando o sistema está sob pressão de capacidade.

## Decision
O contrato assíncrono das invocações tem cinco cláusulas:

1. **Execução sempre assíncrona** — o handler jamais roda na thread do
   chamador, nem como atalho para jobs imediatos.
2. **Durabilidade sempre síncrona** — o retorno do terminal significa que
   o job está persistido e sob custódia do Mohs; o at-least-once começa
   aqui; não existe fire-and-forget em memória.
3. **Retorno é recibo (`Enqueued`), nunca `Future` do resultado** —
   desfecho se observa por eventos/listeners, `mohs.execution(id)` ou
   dashboard; `awaitExecution` só existe no test kit.
4. **Transacional por participação** — dentro de uma transação ativa
   (mesmo DataSource), o insert do terminal entra na transação do
   chamador: commit publica, rollback apaga — transactional outbox
   nativo, sem broker; sem transação ativa, auto-commit com a mesma
   durabilidade.
5. **Admissão nunca espera capacidade** — queue, rate limit e runner
   limitam a execução (no claim), não o aceite; o terminal não bloqueia
   por fila cheia; p99 do terminal ≈ custo do insert (metrificado no
   BASELINE.md).

## Consequences
O chamador nunca obtém o resultado do handler pela própria chamada de
invocação — observar desfecho exige um mecanismo à parte (listener,
polling do execution ou dashboard), escolha deliberada para não acoplar o
chamador a uma execução potencialmente remota. Em compensação, o Mohs
oferece transactional outbox nativo de graça para quem já invoca dentro
de uma transação Spring, sem precisar de um broker de mensagens. A
cláusula 5 implica que overload de capacidade nunca se manifesta como
latência no ponto de chamada — apenas como atraso na execução — o que
exige que backpressure e limites sejam tratados inteiramente no lado do
claim/dispatch, não na admissão.

## Source
docs/API-DESIGN.md "Contrato assíncrono das invocações [DECIDIDO]"
(lines 286-306); docs/MOHS-DOCUMENTO-MESTRE.md §5.6 (lines 332-347)
