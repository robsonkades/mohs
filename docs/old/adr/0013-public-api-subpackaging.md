# ADR-0013: Subpacotes da API pública

> **Nota (2026-08-13):** esta ADR foi revisada pela
> [ADR-0015](0015-consolidate-public-api-under-core.md): a fachada e
> identidade que aqui ficavam soltas em `io.mohs` (raiz) migraram para
> `io.mohs.core`, junto com os subpacotes já descritos abaixo (agora
> `io.mohs.core.schedule`, `.definition`, `.execution`, `.event`,
> `.resource`). A regra ArchUnit descrita aqui não mudou — só passou a
> cobrir a nova localização automaticamente.

## Status
Decided — 2026-08-13

## Context
M1 fechou o vocabulário público inteiro (~40 tipos — identidade, agenda,
definição, execução, eventos/listeners/interceptors, recursos nomeados,
fachada) direto em `io.mohs`, um pacote só, seguindo a decisão original da
ADR-0001 §4 ponto 1 ("`io.mohs` (API pública)" listado como pacote único).
Com o vocabulário todo implementado, `io.mohs` ficou difícil de navegar —
~40 arquivos sem nenhuma subdivisão visual/estrutural, apesar de já
existirem agrupamentos coesos e óbvios (agenda, definição, execução,
eventos, recursos nomeados), os mesmos sete grupos em que M1 foi
implementado incrementalmente.

A escolha original de manter `io.mohs` como pacote único não era sobre
navegação — era para manter a regra ArchUnit de fronteira (`interno não
vaza para a API pública`) trivial: `resideInAPackage("io.mohs")` (pacote
exato, não recursivo) já significava "é API pública", sem lista de
exclusão. Subpacotes públicos dentro de `io.mohs` quebram essa checagem
ingênua, porque `io.mohs.engine`/`io.mohs.jdbc` (internos) também
"começam com" `io.mohs.`.

## Decision
`io.mohs` público se divide em subpacotes coesos por concern:
`io.mohs.schedule` (agenda: `Schedule` selado, `Misfire`),
`io.mohs.definition` (`JobDefinition`, `@MohsJob`, builder staged
`JobSpec`/`PolicySpec`), `io.mohs.execution` (`Execution`, `Attempt`,
`ExecutionState`, `JobContext`, `Priority`), `io.mohs.event`
(`ExecutionEvent` selado, `ExecutionListener`, `ExecutionInterceptor`,
`@OnExecution`) e `io.mohs.resource` (`MohsRunner`, `JobQueue`,
`ExecutionWindow`). A raiz `io.mohs` fica só com a fachada e a identidade
compartilhada (`Mohs`, `MohsLifecycle`, `ScheduleCommand`, `Batch`,
`BatchBuilder`, `EngineState`, `JobKey`, `ExecutionId`, `JobRef`) — os
tipos que um consumidor típico importa primeiro.

A regra ArchUnit correspondente (`ArchitectureTest.internal_packages_do_not_leak_into_public_api`)
muda de "é exatamente `io.mohs`" para "está em `io.mohs..` e não é nenhum
dos 5 pacotes internos conhecidos" (`engine`, `jdbc`, `autoconfigure`,
`rest`, `test`). A lista de exclusão usa os pacotes **internos** (fixados
desde M0, cinco nomes estáveis) em vez de tentar listar os subpacotes
**públicos** (cresceria a cada milestone, e um esquecimento deixaria um
subpacote novo fora da checagem).

Isto **revisa o ponto 1 da ADR-0001** ("Fronteira por pacote, guardada por
ArchUnit: `io.mohs` (API pública)... listado como um pacote só"). A
ADR-0001 permanece como registro histórico da decisão original; esta ADR
é quem a supera nesse ponto específico.

## Consequences
A regra ArchUnit, na prática, **ganha cobertura em vez de perder**: antes,
só a raiz `io.mohs` era checada contra vazamento de `engine`/`jdbc`; os
subpacotes novos passam a ser cobertos pela mesma regra pela primeira vez.
Nenhuma assinatura pública, comportamento ou contrato mudou — só o pacote
totalmente qualificado de cada tipo (FQN), o que já era esperado e é o
motivo desta ADR existir antes de qualquer consumidor externo depender do
FQN antigo.

Custo: mais imports cruzados entre os subpacotes do que existiam antes
(quando tudo estava no mesmo pacote, nada precisava de import). O grafo de
dependência resultante é acíclico por construção: `schedule` e `resource`
não dependem de nenhum subpacote novo; `definition` depende de `schedule`;
`event` depende de `execution`; a raiz depende de `definition`, `event` e
`execution` (a fachada referencia o que expõe).

## Source
Conversa de reorganização pós-M1 (2026-08-13); revisa
`0001-single-module-packaging.md` §ponto 1;
`src/test/java/io/mohs/ArchitectureTest.java` é a implementação executável
desta decisão.
