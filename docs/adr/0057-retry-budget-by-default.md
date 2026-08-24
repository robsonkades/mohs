# ADR-0057 — O job nasce com orçamento de retry (`retries` default 1)

Data: 2026-08-23 · Status: aceita · Origem: bancada de validação de release

## Contexto

`retries` valia **0** por default nas três anotações (`@MohsJob`,
`@RecurringJob`, `@OnDemandJob`) e no builder programático
(`JobSpecImpl`). Esse zero nunca foi decidido: `docs/API-DESIGN.md` e o
documento mestre mostram `retries = 8` em exemplos e não declaram default
nenhum — o valor nasceu de um campo `int` não inicializado e virou
contrato por omissão.

O problema não é o retry de falha de handler, onde "não retente sem eu
pedir" é uma posição defensável. É que o MESMO orçamento governa o
**reclaim de posse perdida**: `Engine#decideReclaim` reagenda o órfão via
`RetrySchedule.nextRetryAt(attempt, definition.retries(), now)` e, sem
orçamento, o reclaim não tem para onde reagendar e grava `FAILED`
terminal. Ou seja, no default:

- nó morre → o trabalho em voo dele é **perdido**, não reentregue;
- lease vence sob pausa longa → idem;
- janela de shutdown (a que a bancada expôs e o `Engine.stop` fechou) → idem.

A ADR-0003 promete at-least-once sob falha de nó. Com `retries = 0` essa
promessa não vale, e o usuário que segue o caminho feliz do builder —
sem declarar política alguma — cai exatamente no caso em que ela não vale.
A bancada mediu o efeito: o mesmo mecanismo de reclaim aparece como
"32 execuções perdidas" com orçamento zero e como "0 perdidas" com
orçamento 1.

## Decisão

**O default passa a ser `retries = 1`**, nos quatro pontos de declaração.
Um job que não pede política nenhuma nasce com orçamento suficiente para
sobreviver a UMA perda de posse.

`retries = 0` continua expressável e continua significando exatamente o
que significava: no máximo uma invocação por execução (at-most-once), com
perda assumida sob falha de nó. O que muda é quem precisa declarar —
antes era quem queria a garantia, agora é quem quer abrir mão dela.

## Alternativas consideradas

- **Manter 0 e só documentar.** Rejeitada: o default silencioso É o
  problema; documentar uma armadilha não a remove.
- **Default maior (3, 10 como o JobRunr).** Rejeitada por YAGNI e por
  custo de surpresa: 1 é o mínimo que honra o contrato anunciado, e
  multiplicar invocações de handler não-idempotente por 10 no default
  seria trocar uma armadilha por outra.
- **Separar o orçamento de reclaim do orçamento de falha de handler** —
  tecnicamente a resposta mais correta: um reclaim não é uma tentativa
  que falhou, é trabalho que nem chegou a ser julgado, e consumir
  orçamento por ele contraria a intuição. Rejeitada AGORA por tamanho:
  mexe na ADR-0033 (o attempt sintético consome orçamento como qualquer
  falha), no `decideReclaim` e no contrato de eventos. Fica registrada
  como a evolução natural desta decisão, com gatilho: primeira
  reclamação de orçamento queimado por reclaim em job de retry curto.

## Consequências

- **Revisa a consequência da ADR-0033** que dizia "com o default
  `retries = 0` continua at-most-once": o default deixou de ser 0. A
  frase segue válida para quem declarar `retries = 0` explicitamente.
- **Mudança de comportamento visível**, e precisa de nota de release:
  - **caso comum:** um job que falha e não declarava política passa a ser
    invocado duas vezes em SEQUÊNCIA, separadas pelo backoff (≤ 1s na
    primeira falha);
  - **caso agudo, e é o que esta mudança torna alcançável:** quando a
    detecção de morte dá falso positivo — nó vivo com heartbeat atrasado
    (pausa de GC, stall de banco, `node-lease-ttl` menor que o `timeout`
    do job) — o reclaim agora RE-DESPACHA a execução enquanto o handler
    original ainda roda. O fence `(node_id, epoch)` descarta o resultado
    do zumbi, mas não desfaz o efeito colateral dele; e como o `complete`
    cercado apaga a lease, o teto de `preventOverlap`/
    `maxConcurrentExecutions` (derivado de `mohs_lease`) deixa de
    contá-lo — **dois handlers concorrentes da mesma execução são
    possíveis nessa aresta**. Antes, com orçamento zero, a execução
    virava `FAILED` terminal e ninguém re-despachava: o custo era perda
    silenciosa. Handlers já precisavam ser idempotentes (ADR-0003); quem
    não é deve declarar `retries = 0` e aceitar a perda, ou alinhar
    `node-lease-ttl` ao `timeout` (o WARN de boot da ADR-0033);
  - **rate limit (ADR-0042):** uma execução que falha passa a consumir
    DOIS tokens em vez de um — o token é cobrado no claim. Num bucket
    apertado durante uma tempestade de falhas, a vazão de trabalho novo
    cai até pela metade. Limitado a +1 por execução, e o jitter espalha;
  - **timeout:** job com `timeout` mal dimensionado passa a pagar o
    trabalho duas vezes antes do desfecho terminal.
- Oito pontos de declaração passaram a dizer `retries(0)`: duas fixtures
  compartilhadas (`DispatcherTest#onDemand`, `EngineTest#seedEnqueuedExecution`,
  que cobrem dezenas de testes do caminho terminal) e seis declarações
  pontuais — o teste de timeout e os cenários de bancada. É o orçamento
  que essas fixtures sempre entregaram, agora explícito. Nenhuma asserção
  foi enfraquecida; a mudança tornou a intenção de cada uma legível. O
  caso que prova a necessidade: o `ColdStartScenario` herdava o default e,
  com o novo, sua asserção `failed == 0` passaria a TOLERAR um evento de
  perda por execução — um detector afrouxando em silêncio.
- **Pendência herdada, com gatilho:** o DDL ainda carrega
  `retries INT NOT NULL DEFAULT 0` nos quatro `schema-*.sql` e na
  `V1__mohs_baseline`. Não é alcançado em operação normal (`JdbcJobStore`
  sempre faz bind da coluna), mas deixa a garantia escrita em dois
  valores: qualquer linha inserida sem a coluna — INSERT de operador,
  migração de terceiro — nasce at-most-once. Alinhar exige uma `V5` nos
  quatro dialetos. **Gatilho:** primeira definição de job criada fora do
  `JdbcJobStore`, ou a próxima migração que tocar `mohs_job_definitions`.
- O demo (`Demo.java`) declara `retries = 10` explicitamente e não muda.
