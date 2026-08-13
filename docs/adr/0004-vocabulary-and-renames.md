# ADR-0004: Vocabulário e renames

## Status
Decided — 2026-08-12

## Context
A escolha de nomes públicos é vocabulário do domínio, permanente após
1.0. Dois nomes candidatos originais — `Queue` e `Calendar` — colidem com
tipos do JDK (`java.util.Queue`, `java.util.Calendar`), o que forçaria
todo consumidor da API a lidar com import ambíguo ou qualificação total
do nome para sempre.

## Decision
Vocabulário público fechado:

| Conceito | Nome | Nota |
|---|---|---|
| Definição | `JobDefinition` / `@MohsJob` | annotation = forma canônica |
| Identidade | `JobKey` (`id`) | estável, chave de persistência; `name` = rótulo mutável |
| Referência tipada | `JobRef<T>` | amarra id + tipo do payload em compilação |
| Agenda | `Schedule` | `cron` · `every` (rate) · `everyAfterFinish` (delay) · `onDemand` |
| Instância | `Execution` / `Attempt` | retry incrementa attempt, id permanece |
| Capacidade node-local | `MohsRunner` (`mode: io\|cpu`) | built-ins `io`/`cpu`; customs = bulkheads |
| Cap cluster-wide | `JobQueue` | **[DECIDIDO]** rename de `Queue` (colisão JDK) |
| Janelas de exclusão | `ExecutionWindow` | **[DECIDIDO]** rename de `Calendar` (colisão JDK) |
| Misfire / Retry / RateLimit / Priority | — | espelham o motor |

Os renames `Queue` → `JobQueue` e `Calendar` → `ExecutionWindow` foram
aprovados pelo PO (12/08/2026) com a justificativa: "colisão de import
com o JDK custava centavos agora e uma fortuna depois do 1.0".

## Consequences
Nenhum tipo público do Mohs colide com `java.util.Queue` ou
`java.util.Calendar` — usuários nunca precisam de import totalmente
qualificado nem de aliasing para usar as duas APIs lado a lado. O custo é
pago agora, antes do 1.0, quando renomear é barato (nenhum consumidor
externo ainda depende do nome antigo); a alternativa — manter os nomes
colidentes até alguém reclamar — teria custo crescente e seria breaking
change depois do lançamento.

## Source
docs/API-DESIGN.md "Vocabulário" (lines 41-53); docs/MOHS-DOCUMENTO-MESTRE.md
§7 "Resolvidas" item 1 (lines 524-527)
