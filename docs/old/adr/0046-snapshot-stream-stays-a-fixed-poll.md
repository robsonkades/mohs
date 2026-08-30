# ADR-0046: O snapshot stream continua um poll fixo — o que foi rejeitado, e por quê


> **Nota (2026-08-29):** o *mecanismo* decidido aqui — poll fixo de 2s, cadência
> única, custo compartilhado por tick, e "sem número, não muda" como critério —
> segue valendo integralmente. O que mudou foi o CONJUNTO de frames: a
> [ADR-0063](0063-the-overview-carries-a-rate-not-only-gauges.md) acrescentou
> `runners` (leitura em memória, zero statement) e uma segunda leitura de vazão
> ao frame `overview`. Onde esta ADR diz "quatro frames por tick" e "nenhuma
> linha de `OverviewStreamBroadcaster` muda", leia-se hoje **cinco frames e 7
> statements/tick** — e a ADR-0063 traz o número medido dessa diferença.
## Status
Decided — 2026-08-21 · **nada muda em `OverviewStreamBroadcaster`**; esta ADR
existe para registrar três alternativas investigadas e rejeitadas, duas delas
depois de implementadas e revisadas

## Context
`GET /overview/stream` empurra um retrato completo a cada 2 segundos: quatro
leituras em fan-out (`overview`, `jobs`, `nodes`, `executions`) envelopadas
com um `asOf` comum, o que dá **seis statements** no banco — a contagem ativa
são duas queries de propósito (os predicados batem com os índices parciais de
claim e do reaper; um `IN` dos três estados não implica predicado nenhum e
degrada para scan), mais a de throughput, mais jobs, nodes e a lista recente.
Zero disso acontece sem assinante.

Duas perguntas legítimas surgiram sobre esse desenho:

1. O motor já publica `Started`, `Succeeded`, `Failed` e companhia no instante
   em que acontecem (`ExecutionEventPublisher`) — por que o stream é
   alimentado por uma query em vez desses eventos?
2. Por que ler as quatro coisas na velocidade da mais volátil?

As duas foram implementadas, revisadas e revertidas. O que segue é o registro,
porque as perguntas vão voltar — são boas.

## Decision
**O stream fica como está.** Poll fixo de 2s, quatro frames por tick, custo
zero sem dashboard aberto. Nenhuma linha de `OverviewStreamBroadcaster` muda.

A regra que decidiu as três rejeições é a mesma, e é a da casa: *"Performance
claims come with before/after benchmarks. Without a number, it's not an
optimization."* Nunca houve número dizendo que este endpoint custa caro ou
que 2s de latência atrapalham alguém. A queixa real que originou a
investigação era visual — o dashboard *parecia* recarregar a cada atualização
— e foi resolvida inteiramente no frontend (ADR-0045), sem tocar no servidor.

## Alternativas rejeitadas

### 1. Entregar o evento como payload do SSE
A proposta óbvia, e quebra em três lugares independentes:

- **Multi-nó.** O `ExecutionEventPublisher` entrega dentro de UMA JVM, e o
  dashboard mantém uma conexão com UM nó. Duas abas atrás do load balancer,
  ligadas a nós diferentes, mostrariam números diferentes e ambos errados. A
  query no banco é a única coisa que enxerga o cluster inteiro.
- **O overview é agregado, não delta.** `executionCountsByStatus` é um `COUNT`
  sobre a tabela toda. Reconstruir isso a partir de eventos é manter estado
  derivado ao lado da verdade — e a entrega é best-effort por contrato: o
  publisher **descarta** o evento quando o executor satura, exatamente sob a
  carga em que o número importa. Um retrato perdido se corrige no tick
  seguinte; um delta perdido é divergência permanente até alguém apertar F5.
- **Reconexão.** Hoje cair e voltar não perde nada, porque o próximo frame é o
  retrato inteiro. Entrega de evento exige `Last-Event-ID` e replay a partir de
  um log durável — que é precisamente o que a decisão v0.3 do REST-API-DESIGN
  recusou, e a v0.7 só liberou o stream porque um retrato periódico **não
  promete durabilidade nenhuma**.

O estado da arte divide as águas no mesmo lugar: JobRunr e db-scheduler
alimentam o dashboard por polling do storage; Temporal empurra evento ao vivo
porque tem *event history* durável e centralizada. Quem empurra evento tem log
durável; quem não tem, lê o estado.

### 2. O evento como GATILHO do retrato (implementada e revertida)
O evento não viraria payload, só faria o tick ler mais cedo: um
`ExecutionListener` levanta uma bandeira, o tick lê quando ela está levantada
ou quando um teto vence. Latência de 2s para 250ms.

- **O ganho é 1/N num cluster.** O gatilho só enxerga o nó que atende a
  conexão. Com três nós, dois terços do trabalho continuam aparecendo no teto.
- **Custo 8× onde mais dói.** Sob atividade a bandeira está sempre levantada:
  4 retratos/s contra 0,5, e as contagens são proporcionais ao trabalho vivo.
- **Acopla a camada REST ao pipeline de eventos do motor** por esse 1/N.

A implementação ainda cobrou o preço da própria complexidade antes de chegar a
produção: a primeira versão usava thread em laço com `ReentrantLock`/
`Condition` e trocou as duas esperas (condição no piso, onde precisava ser
duração), o que sob rajada media **406 retratos em 3s** onde o desenho
prometia 12 — um limitador de taxa que amplificava carga. Uma segunda versão,
muito mais simples (uma `AtomicBoolean` e duas guardas sobre o timer que já
existia), funcionava — e ainda assim rendia 1/N.

### 3. Cadência por frame — cada um na velocidade do que mostra (implementada e revertida)
Ler `jobs` a cada 5 ticks em vez de todo tick, já que definição muda por ação
de operador. Chegou a ser implementada com testes e ADR.

O que a matou foi olhar os dois lados com número:

- **Ganho: 0,4 query por segundo** (3/s → 2,6/s), e só com dashboard aberto.
  No ponto de operação do BASELINE são 4k execuções/s, cada uma custando claim,
  update de estado e escrita de attempt — 0,4/s é ruído.
- **Custo: o painel "Up next" passa a mostrar disparo vencido.** O frame `jobs`
  carrega `nextFireAt`, e o Overview renderiza `relativeTime(nextFireAt)`. Como
  a página re-renderiza a cada 2s (os outros frames continuam chegando), o
  número não congela: ele **continua contando para além do zero** — "em 1s",
  "agora", "há 1s"… "há 8s" — até o frame novo chegar. Um job que disparou na
  hora certa aparece como atrasado, no topo da lista, por até 10 segundos.

Uma versão anterior escalonava `nodes` junto, e era pior: com `poll-interval`
de 5s e orçamento de frescor de 15s no cliente, 10s de atraso consumiam a
margem inteira e o painel **"Needs attention" acusava um nó saudável de ter
parado**. Alarme falso periódico é como se ensina um operador a ignorar o
painel.

**A lição que generaliza as duas:** atrasar um dado cujo significado é o tempo
faz o atraso virar parte do dado. E o retrato não tem nenhum frame frio —
`overview` são contagens que mudam a cada transição, `executions` é a lista
viva, `nodes` é heartbeat, `jobs` carrega `nextFireAt`. Cadência por frame
precisa de um frame sem componente temporal, e aqui não existe um.

### 4. Notificação do banco (LISTEN/NOTIFY)
Daria alcance cluster-wide ao gatilho da alternativa 2, mas não é portável
entre os três dialetos do `mohs-jdbc`. Trocar uma limitação conhecida por uma
capacidade que só existe no PostgreSQL não é bom negócio nesta camada.

## Consequences
- **Nada muda em produção.** O custo, a latência e o contrato SSE continuam os
  mesmos; esta ADR é documentação, não mudança.
- **A próxima pessoa que tiver uma dessas ideias encontra a resposta pronta**,
  com os números que a matam — inclusive os dois defeitos que só apareceram
  depois de implementar.
- **Pendências reais que a investigação deixou visíveis**, nenhuma delas
  urgente:
  - O frame `executions` é lido e enviado embora o cliente descarte o payload
    (vem sem filtro, a tela está quase sempre filtrada; serve só como aviso de
    "algo mudou" — `useLiveUpdates.ts`). É a query mais cara e o maior payload
    do tick, gastos para transmitir um sinal. Eliminá-lo tira um statement de
    todo tick, mas muda o contrato SSE público (um evento nomeado a menos).
  - `nodeStatus.ts` julga frescor com `Date.now() - lastHeartbeatAt` e um
    orçamento fixo de 15s: depende do relógio do browser e quebra sozinho se um
    operador subir `mohs.engine.poll-interval`. A correção é envelhecer contra
    o `asOf` que o envelope já carrega e o `seed()` descarta hoje.
  - `guardedTick` captura só `RuntimeException`; um `Error` cancela a tarefa
    periódica em silêncio e o stream morre sem log.
- **Se um dia a latência de 2s for medida como problema**, o caminho barato é
  baixar `STREAM_INTERVAL` — acelera o cluster inteiro, não 1/N dele.
