# ADR-0012: Liveness — heartbeat, lease e reaper (Watchdog Bound)

## Status
Decided — 2026-08-12

## Context
Numa execução distribuída, um Attempt pode parar de progredir sem que o
motor saiba — o processo pode morrer, a rede pode particionar, ou o
Handler pode ignorar um sinal de interrupt e rodar indefinidamente (a JVM
não mata uma thread que não coopera). Sem um mecanismo de liveness,
execuções órfãs ficam presas para sempre e o cluster não tem como
diferenciar um Attempt lento de um abandonado.

## Decision
O motor renova a lease de toda Execution RUNNING a cada ciclo de poll;
sem isso, qualquer Attempt mais longo que a lease pareceria abandonado ao
reclaimer mesmo estando saudável. O **Watchdog Bound** é o teto opcional
dessa proteção: passado o bound, o node para de renovar a lease; a lease
expira, o reclaimer trata o Attempt como falho, e a Retry Policy decide o
resto. O zumbi pode ainda estar rodando quando o retry começa —
consistente com o contrato at-least-once; a escrita terminal dele perde a
CAS de versão em vez de corromper a do retry.

**Sub-decisão: cluster-wide, não por Job [DECIDIDO — per-job avaliado e
rejeitado].** A renovação de lease é desenhada em lote — uma query
cobrindo toda Execution RUNNING do node de uma vez, o que mantém a tabela
mais quente do sistema barata de tocar a cada poll. Per-job quebraria
esse lote: exigiria aritmética de data dialect-specific na SQL de
renovação (Postgres `INTERVAL` ≠ SQL Server `DATEADD`) ou uma coluna nova
pré-calculada na Execution, em todo backend de storage. O lever certo
para job lento já está especificado e é mais barato: `timeout` do
`@MohsJob`, avaliado em memória, sem tocar o motor. O Watchdog só entra
depois que o `timeout` falhou (o Handler nem assim parou) — é rede de
segurança de último caso, não afinação por job; um valor cluster-wide
maior que o timeout mais folgado do app + margem cobre bem.

Configuração: `mohs.engine.lease-ttl` (30s), `mohs.engine.watchdog-timeout`
(10m; null = sem teto por default; deve ser > `lease-ttl`).

**Nome `lease-ttl`, não `liveness` [DECIDIDO].** "Liveness" é o termo
guarda-chuva do documento mestre para heartbeat + lease + reaper juntos —
nomear o parâmetro assim perderia precisão. Heartbeat de node (só
informativo — nenhuma lógica de claim/reclaim consulta) e lease de
Execution (funcional — é o que o reclaimer usa) são dois relógios
distintos por design; `lease-ttl` nomeia só o segundo. O intervalo do
heartbeat de node ainda não tem property definida (fica em aberto —
`node-heartbeat-interval` é o nome provável, não decidido).

## Consequences
Este mecanismo sustenta quatro capacidades do produto, por isso entra em
M3 e não fica para depois: **recuperação real at-least-once** (execuções
órfãs voltam ao retry, não ficam presas); **`GET /nodes`** (reusa o mesmo
registro de heartbeat); **auto-cura do soft cap da queue** (dependência
explícita do ADR-0009 — o enforcement derivado se apoia no reaper para
devolver vagas de nós mortos); e **honestidade do contrato de execução**
(o motor nunca finge que uma execução travada está progredindo). Estado
da arte: nem Quartz nem JobRunr documentam um teto equivalente ao
Watchdog Bound — é um diferencial real do Mohs, não commodity.

## Source
docs/API-DESIGN.md "Watchdog Bound — teto contra Attempt zumbi
[DECIDIDO]" (lines 210-251); docs/MOHS-DOCUMENTO-MESTRE.md §7
"Resolvidas" item 3 (lines 531-535)
