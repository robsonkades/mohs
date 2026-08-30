# ADR-0039: Claim limitado pela folga de dispatch

## Status
Decided — 2026-08-16, durante o ciclo de tuning de throughput (BASELINE, seção
"Tuning fim a fim no Postgres", rodadas DBTUNE-17/18 + esta ADR). Aprovação de
mudança de comportamento coberta pelo mandato explícito do ciclo ("fazer os
ajustes até a meta de 4k/s").

## Context
Depois da DBTUNE-18 (memoização das consultas de definição no tick), o claim
ficou rápido o bastante para reivindicar muito além da capacidade de dispatch
do node: com `batch-size=500`, `poll-interval=100ms` e runner `io` em 256, o
tick reivindicava ~3.300 execuções/s contra uma capacidade de conclusão de
~1.800/s. O excedente estourava o teto do runner — e a rejeição do executor
(backpressure deliberado, ver `MohsExecutors.ioBoundExecutor`) deixava a
execução RUNNING no banco, presa até a lease expirar e o reaper reclamá-la
(ADR-0033). Medido num drain de 50k no Postgres (2026-08-16): **56.187
rejeições** e **11.666 execuções com attempt > 1** (re-execuções por reclaim),
com vazão efetiva estagnada em ~1.800/s e churn massivo de retry.

O motor já conhecia o teto (`mohs.engine.dispatch-concurrency` dimensiona o
runner `io` built-in) mas o tick o ignorava na hora de reivindicar.

## Decision
O claim de cada tick pede no máximo a folga de dispatch:
`min(batch-size, dispatch-concurrency − in-flight)`. Folga zero ou negativa =
tick sem claim. `dispatchConcurrency` entra em `EngineSettings` (mesmo valor
que dimensiona o runner `io` — uma única fonte para o teto do node); os
construtores de conveniência de teste preservam o comportamento pré-ADR
(sem teto). A rejeição do executor continua existindo como rede de segurança
(runners nomeados podem ter tetos menores que o global).

## Consequences
- Node saturado deixa de fabricar o próprio churn: nada de RUNNING preso por
  rejeição, nada de re-execução via reaper em operação normal.
- O excedente fica ENQUEUED no banco — visível, e reivindicável por qualquer
  outro node com folga (Competing Consumers, EIP): a limitação é por node,
  não do cluster.
- Throughput do node passa a ser governado pela capacidade real de conclusão
  (concorrência × latência do pipeline de escrita), não pela agressividade do
  claim — o knob honesto para subir vazão vira `dispatch-concurrency` (+ pool).
- Cenário multi-runner: o clamp usa o teto global; um runner nomeado menor
  ainda pode rejeitar (caminho de recuperação inalterado). Refinamento por
  runner só com caso real (YAGNI).
- Sobrescrever o runner `io` com `max` menor que `dispatch-concurrency`
  quebra a premissa de fonte única e reintroduz a rejeição no cano principal:
  `MohsRunners` emite WARN de boot nomeando os dois valores (review desta
  ADR). WARN, não erro: capar o `io` é escolha operacional legítima e o
  caminho de recuperação existe.
