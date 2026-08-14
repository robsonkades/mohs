# ADR-0028: Papel duplo de `Enqueued` e `BatchCompleted` — recibo síncrono e evento assíncrono no mesmo tipo

## Status
Decided — 2026-08-14

## Context
`Enqueued` (`io.mohs.core.event`) é, ao mesmo tempo, o recibo devolvido pelos terminais de
`ScheduleCommand` (`now`/`at`/`after`) e a variante de `ExecutionEvent` publicada para
`ExecutionListener`s. `BatchCompleted` tem o mesmo papel duplo: dado passado para
`Batch#onCompletion` e variante de `ExecutionEvent`. Os dois já documentam a escolha em Javadoc
("evita duplicar o mesmo formato sob dois nomes") mas nunca em ADR — apesar de o projeto já ter
ADRs de peso comparável ou menor (ex. ADR-0004, sobre dois renames).

O trade-off real: o recibo síncrono serve quem acabou de chamar `schedule(...).now()` e quer
confirmação; o evento assíncrono serve um `ExecutionListener` que nunca viu a chamada original — dois
consumidores com pressões de evolução diferentes, hoje coincidindo por acaso no mesmo conjunto de
campos (4 para cada tipo). Um review de nomenclatura e organização (`docs/codereview-naming.md`,
achado RESP-2) identificou que essa decisão atinge o próprio critério do projeto para virar ADR, sem
ter uma.

## Decision
Manter o papel duplo. A alternativa — dois tipos distintos (`Enqueued` como recibo puro,
`EnqueuedEvent` como variante de `ExecutionEvent`) — duplicaria o mesmo formato sob dois nomes hoje,
sem nenhum consumidor concreto precisando de campos diferentes entre os dois papéis; é o mesmo
raciocínio de YAGNI que o projeto já aplica em outras decisões desta sessão (ex. ADR-0021).

**Gatilho explícito para revisitar esta decisão:** o dia em que qualquer um dos dois lados precisar de
um campo que só faz sentido pra ele — ex. o listener querendo `idempotencyKey` (irrelevante pro
chamador síncrono, que já a forneceu), ou o recibo querendo algo específico da borda REST (o
`Location` header já é responsabilidade de `AcceptedExecutionResponse`, não de `Enqueued`) — é o
sinal de que os dois papéis divergiram de verdade e o tipo único deve virar dois. Até lá, um campo
que serve só metade dos consumidores é o cheiro a observar, não uma regra rígida proibindo mudança
nenhuma.

## Consequences
Nenhuma mudança de código — `Enqueued`/`BatchCompleted` continuam exatamente como estão. O ganho é
só ter o trade-off registrado e um gatilho concreto de quando revisitar, em vez de a decisão ficar
implícita em duas linhas de Javadoc que um novo campo poderia violar sem ninguém perceber que está
violando uma decisão.

## Source
`docs/codereview-naming.md`, achado RESP-2 (2026-08-14); `io.mohs.core.event.Enqueued`/
`BatchCompleted` (Javadoc); ADR-0002 (contrato assíncrono das invocações), ADR-0005 (listeners vs.
interceptors).
