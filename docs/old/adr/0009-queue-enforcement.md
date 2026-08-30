# ADR-0009: Enforcement de queue

## Status
Superseded pela ADR-0021 — 2026-08-13. `JobQueue` foi removida; o
problema que esta ADR descrevia (enforcement de admissão de queue)
deixou de existir. Mantida por histórico.

## Context
`JobQueue` impõe um cap de concorrência cluster-wide sobre um recurso
compartilhado (ex.: SMTP, API parceira) — papel distinto do Runner, que
protege apenas o nó local; escalar nós não pode escalar a pressão sobre o
recurso compartilhado. O enforcement atual, baseado em contador
(`UPDATE ... WHERE running_count < max`), tem três modos de falha
conhecidos: hot row (toda partida/conclusão serializa na mesma linha),
bloat de tuplas no Postgres (milhares de versões de tupla por segundo), e
drift do contador (um nó morre entre incrementar e decrementar → a vaga
vaza para sempre, sem reconciliação).

## Decision
Proposta em avaliação — **contagem derivada, sem contador mantido**: a
cláusula de claim verificaria `(SELECT count(*) WHERE queue=? AND
status='RUNNING') < max`, servida por um índice parcial em `(queue)
WHERE status='RUNNING'`. Como o conjunto RUNNING é limitado pelo próprio
`max`, a contagem é O(max) via index-only scan, e claims são batelados no
poll (custo por nó × frequência, não por execução). Sem estado mantido:
sem hot row, sem bloat, sem drift, sem job de reconciliação. A semântica
resultante seria **soft cap** — overshoot transitório ≤ nós−1 sob claims
simultâneos, documentado como adequado à proteção de recurso, não a um
limite rígido. Dependência explícita: execuções RUNNING de um nó morto
seguram a vaga até o reaper de órfãs (ADR-0012) devolvê-las ao retry — o
enforcement derivado se auto-cura via reaper, enquanto o contador vazava
a vaga permanentemente.

Esta ADR permanece **Proposed**, não Decided, porque a escolha entre as
duas estratégias está gated a um benchmark ainda não executado.

## Consequences
O gate é: a **Fase 0/1** mede o enforcement por contador atual sob
**carga-alvo (10k+ jobs concorrentes, queue quente)**; se essa medição
**confirmar contenção** (hot row/bloat/drift observados sob a
carga-alvo), o enforcement troca para contagem derivada. A superfície
pública da API é agnóstica a qual estratégia vence — a troca, se
acontecer, não muda contratos públicos. Até a resolução do gate, o Mohs
opera com o enforcement por contador, com os três modos de falha
conhecidos documentados acima como risco aceito temporariamente.

## Source
docs/API-DESIGN.md "Enforcement da queue [EM REVISÃO → ADR com gate de
benchmark]" (lines 359-386); docs/MOHS-DOCUMENTO-MESTRE.md §5.8 (lines
367-390) e §7 "Em revisão com gate" (lines 537-538)
