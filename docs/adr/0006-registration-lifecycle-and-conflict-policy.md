# ADR-0006: Ciclo de registro e `on-conflict`

## Status
Decided — 2026-08-12

## Context
A cada boot, o Mohs precisa reconciliar as definições encontradas no
código (via `@MohsJob`) com o que já está persistido no store — sem
perder estado operacional acumulado em runtime. Exemplo motivador: um job
pausado manualmente pelas operações às 3h da manhã não pode voltar a
rodar sozinho só porque um deploy das 9h reaplicou a definição do código.

## Decision
Ordem de boot: (1) scan de `@MohsJob` → um `define` por annotation; (2)
validações fatais; (3) engine inicia (SmartLifecycle, fase tardia) —
nenhum claim acontece antes de todas as definições anotadas estarem
registradas; (4) app ready — `define`/`remove` dinâmicos liberados daqui
em diante.

O upsert distingue precisamente dois tipos de estado: **estado
definicional** (agenda, políticas, runner, queue, window, name, binding
do handler) pertence ao código, e o upsert sempre aplica; **estado
operacional** (paused/resumed, histórico, contadores, last fire) pertence
ao runtime, e o upsert sempre preserva.

Conflito definicional (jobs e recursos como queues/rate-limits) é
governado por `mohs.registration.on-conflict`, com três modos:

- `override` (default): código vence; toda mudança é logada com diff —
  auditoria de drift de definição.
- `preserve`: o store vence; versão do código é ignorada com WARN —
  PATCHes de runtime sobrevivem a deploys.
- `fail`: divergência derruba o boot exibindo o diff — para ambientes
  que exigem migração explícita de agenda.

Órfãs e aposentadoria: toda definição carrega `source` (`ANNOTATION` |
`PROGRAMMATIC`). No boot, uma definição `ANNOTATION` presente no store e
ausente do código vira **ORPHANED** — não dispara, é destacada no
dashboard, gera WARN no log (nem fogo no vazio, nem delete silencioso de
histórico). `PROGRAMMATIC` fica fora da varredura; aposentadoria é
explícita via `mohs.remove(jobKey)`, que cancela fires futuros e
preserva histórico.

## Consequences
Um deploy nunca reverte silenciosamente uma ação operacional (pause,
PATCH de runtime) tomada pelas operações, sob o modo default — mas sob
`preserve`, mudanças de código legítimas também podem ser silenciosamente
ignoradas até alguém perceber o WARN, trade-off documentado desse modo.
O modo `fail` existe para ambientes que preferem quebrar o boot a
conviver com qualquer divergência não resolvida. Definições removidas do
código não apagam histórico por padrão — ficam visíveis como ORPHANED —
o que evita perda acidental de dados de auditoria, mas exige que
operadores monitorem o dashboard para notar e agir sobre órfãs.

## Source
docs/API-DESIGN.md "Ciclo de registro e política de conflito [DECIDIDO]"
(lines 118-155); docs/MOHS-DOCUMENTO-MESTRE.md §5.4 primeira metade
(lines 274-296)
