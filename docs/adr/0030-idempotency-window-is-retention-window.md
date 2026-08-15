# ADR-0030: Janela da Idempotency-Key é a janela de retenção de execuções

## Status
Decided — 2026-08-15

## Context
O design REST prometia "mesma key nas próximas ~24h → mesma resposta" (`REST-API-DESIGN.md`;
documento mestre §5). A implementação entregue no M3 é o índice único
`uq_mohs_executions_idem (job_key, idempotency_key)` + Idempotent Receiver em
`ScheduleCommandImpl` — e a chave vive na própria linha de `mohs_executions`. Dois fatos
estruturais decidem o problema:

1. **Índice único não expressa "único dentro de 24h"**: uma chave reutilizada depois da
   janela colidiria pra sempre, a menos que alguém apague/anule a chave antiga. A janela é,
   por construção, uma pergunta de purga — não de query.
2. **A purga da chave é a purga da execução**: a chave some exatamente quando a linha sair.
   E "linhas terminais nunca saem, por desenho" já é decisão de produto pendente apontada
   pelo review de tuning (DBTUNE-9: política de retenção precisa de ADR; job interno do
   próprio Mohs é o candidato a mecanismo).

A pendência 1 de `docs/PENDENCIAS.md` perguntava: TTL dedicado ainda no M3, ou junto com a
retenção?

## Decision
Não existe TTL/purga dedicada de Idempotency-Key. A janela de dedupe é a janela de retenção
de execuções: **a chave deduplica enquanto a linha da execução existir**. Enquanto a política
de retenção não existir (ADR futura, DBTUNE-9), a janela é ilimitada — direção fail-safe:
dedupe eterno suprime *mais* duplicatas, nunca menos. A ADR de retenção herda um requisito
desta: janela de retenção ≥ ~24h, para o mínimo prometido pelo design REST continuar de pé.

Alternativas rejeitadas:
- **Job dedicado anulando `idempotency_key` > 24h**: write amplification na tabela mais
  quente do sistema, um segundo mecanismo de limpeza que a retenção vai duplicar ou
  substituir, e sem devolver capacidade nenhuma (a linha fica); só quebraria "mesma
  resposta" para retries tardios.
- **Janela via predicado de timestamp no lookup de dedupe**: o índice único continuaria
  colidindo pra sempre no insert — a janela seria mentira no caminho de escrita.

## Consequences
- Cliente trata a chave como única por operação lógica (semântica Stripe) — não como token
  reutilizável após a janela. Reutilizar uma chave antiga devolve a execução antiga.
- `REST-API-DESIGN.md`, documento mestre e o Javadoc de `ScheduleCommand.idempotencyKey`
  passam a dizer "janela = retenção (mínimo ~24h quando a retenção existir)" em vez de
  "~24h" seco.
- A pendência de retenção (DBTUNE-9) vira a única dona da purga, com o piso de ~24h como
  requisito adicional.

## Source
`docs/PENDENCIAS.md` item 1 (origem: `codereview-20260815-0332.md`);
`docs/codereview-tuning.md` DBTUNE-9; `docs/REST-API-DESIGN.md` "Idempotency-Key";
`io.mohs.engine.ScheduleCommandImpl` (Idempotent Receiver).
