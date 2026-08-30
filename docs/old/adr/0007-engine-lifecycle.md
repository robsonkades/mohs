# ADR-0007: Lifecycle do engine

## Status
Decided — 2026-08-12

## Context
O motor precisa de um ciclo de vida controlável independente do ciclo de
vida de jobs individuais — casos como warm-up antes de aceitar carga,
leader election externa, deploy canário ou shutdown gracioso sob
Kubernetes exigem controlar o nó inteiro, não um job por vez. É fácil
confundir esse controle node-local com a operação de pausar um job
específico, que é cluster-wide.

## Decision
O lifecycle do engine é **node-local por natureza** — não deve ser
confundido com pause de job, que é **cluster-wide e por job** (via
REST/dashboard). Máquina de estados:

```
CREATED → RUNNING ⇄ PAUSED → DRAINING → STOPPED
```

Exposta via `mohs.lifecycle()`: `state() · start() · pause() · resume() ·
drain(grace) · stop(grace)`.

Configuração: `mohs.enabled` (false desliga toda a auto-config);
`mohs.lifecycle.start-mode` (`auto`, default, ou `manual` — chamada
explícita de `lifecycle().start()`); `mohs.lifecycle.shutdown.grace-period`
(default 30s).

Shutdown gracioso roda em fase antecipada do `SmartLifecycle` — o engine
drena **antes** do Spring fechar o DataSource: (1) SIGTERM → estado
`DRAINING`, claim loop para, readiness cai; (2) in-flight tem até o
`grace-period` para concluir — **drain ≠ cancel**, nenhum sinal de
cancelamento é enviado; (3) no estouro do grace, interrupt via
maquinaria de timeout, attempt falha com causa `NodeShutdown` e segue o
retry normal — at-least-once honesto até no desligamento; (4) runners
desligam, só então o Spring fecha pools e contexto.

`start-mode: manual` permite que registro e validações aconteçam
normalmente no boot, mas o engine aguarda `start()` explícito — casos:
warm-up, leader election externa, canário observador, test kit (que já
opera assim).

Transições são publicadas como `ApplicationEvent` do Spring
(`MohsLifecycleEvent`) — lifecycle é node-local e in-process, escopo
natural dos eventos Spring; o bus de `ExecutionListener` permanece
exclusivo do domínio de execução.

## Consequences
Operabilidade fica embutida no design: health indicator `mohs` reflete
estado e conectividade do store; readiness reflete `DRAINING`;
`grace-period` é documentado lado a lado com
`terminationGracePeriodSeconds` do Kubernetes, evitando que operadores
configurem os dois de forma inconsistente. `GET /nodes` (v1, ADR-0010) e
o roadmap de drain remoto (`POST /nodes/{id}/drain`) reusam o mesmo
registro de heartbeat por node que a liveness (ADR-0012) já precisa
construir — nenhuma infraestrutura nova é necessária para esses recursos.

## Source
docs/API-DESIGN.md "Ciclo de vida do engine [DECIDIDO]" (lines 157-208);
docs/MOHS-DOCUMENTO-MESTRE.md §5.4 segunda metade (lines 297-320)
