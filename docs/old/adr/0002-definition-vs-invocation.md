# ADR-0002: Arquitetura definição × invocação

## Status
Decided — 2026-08-12

## Context
A API do Mohs precisa separar duas responsabilidades que muitos
schedulers confundem: descrever o que um job faz e suas políticas
(handler, cron, retries, runner, queue) versus disparar uma execução
concreta dele. Sem essa separação, cada chamador poderia redefinir
comportamento do job ad-hoc, tornando o sistema de políticas
inconsistente entre origens de disparo (cron automático, chamada
programática, dashboard).

## Decision
Um Job é DEFINIDO uma única vez — handler + políticas, via `@MohsJob`
(annotation, forma canônica) ou `define` programático — e é INVOCADO de N
formas: cron automático, `schedule(...)`, `batch(...)`, dashboard. A
invocação nunca redefine política: apenas parâmetros da instância
(`now/at/after`, `priority`, `as(actor)`, `idempotencyKey`) pertencem à
chamada de invocação; retry, runner e queue pertencem exclusivamente à
definição.

## Consequences
Toda política de execução tem uma única fonte de verdade — a definição —
o que elimina divergência entre disparos do mesmo job vindos de origens
diferentes. Isso exige que toda invocação aconteça sobre uma definição já
existente: invocar um `JobRef` sem definição correspondente falha
imediatamente, com sugestão de ids próximos. É uma restrição deliberada
em troca de consistência — não há "disparo ad-hoc" sem registro prévio.

## Source
docs/API-DESIGN.md "Princípios de design" ponto 1 (lines 24-26);
docs/MOHS-DOCUMENTO-MESTRE.md §5.1 ponto 1 (lines 230-232); exemplo de
código em §5.0 (lines 187-226)
