# ADR-0046: O evento dispara o retrato, não substitui o retrato

## Status
Decided — 2026-08-21 · refina o stream criado na revisão v0.7 do
`docs/REST-API-DESIGN.md` e implementado em `OverviewStreamBroadcaster`
(ADR-0045 é quem consome do outro lado)

## Context
`GET /overview/stream` empurra um retrato completo a cada 2 segundos: quatro
leituras em fan-out (`overview`, `jobs`, `nodes`, `executions`), envelopadas
com um `asOf` comum. O timer é `scheduleWithFixedDelay` — cadência fixa,
independente de qualquer coisa ter acontecido.

Duas consequências disso incomodam, em pontas opostas da carga:

- **Ocioso**: com um dashboard aberto e nada executando, cada instância que
  atende a conexão faz 2 leituras por segundo, 24 horas por dia, para
  responder sempre a mesma coisa. O guard de `subscribers.isEmpty()` já zera
  o custo sem ninguém olhando; ninguém zera o custo de olhar para um cluster
  parado.
- **Ativo**: um job que começa aparece no dashboard em até 2s (1s na média).
  Para uma ferramenta cujo argumento é confiabilidade de execução, o atraso
  entre "começou" e "aparece" é justamente o que o operador está medindo às
  3h da manhã.

E o motor já sabe a resposta: o `ExecutionEventPublisher` publica `Started`,
`Succeeded`, `AttemptFailed`, `RetryScheduled`, `Failed`, `Cancelled` e
`BatchCompleted` no instante em que acontecem. A pergunta natural — e é ela
que esta ADR responde — é por que o stream não é alimentado por esses
eventos em vez de por uma query.

## Decision
**O evento passa a decidir *quando* o retrato é lido. O retrato continua
sendo lido do banco.**

1. Um `OverviewStreamSignal` — `ExecutionListener` sem nenhuma dependência —
   apenas sinaliza que algo aconteceu neste nó.
2. O `OverviewStreamBroadcaster` troca o `scheduleWithFixedDelay` por uma
   espera com **piso** (`MIN_INTERVAL`, 250 ms) e **teto**
   (`MAX_INTERVAL`, os mesmos 2 s de hoje): acorda por sinal quando o piso
   já passou, e por timeout quando o teto vence sem sinal nenhum.
3. O conteúdo do frame não muda em nada — as mesmas quatro leituras, o
   mesmo envelope, o mesmo `asOf`.

O piso não é detalhe de afinação, é o que impede o desenho de se voltar
contra si: no ponto de operação do BASELINE (4k execuções/s) cada execução
publica ao menos dois eventos, e um reclaim em massa publica 2×500 de uma
vez. Sem piso, isso viraria milhares de retratos por segundo — o gatilho
derrubaria o banco que o timer protegia. Com piso, alta carga converge para
~4 frames/s e o comportamento degrada *para* o de hoje, não para pior.

## Alternativas rejeitadas

### Entregar o evento como payload do SSE
É a proposta óbvia, e ela quebra em três lugares independentes:

1. **Multi-nó.** O `ExecutionEventPublisher` entrega dentro de uma JVM. O
   dashboard mantém UMA conexão SSE com UM nó. Num cluster de três, esse nó
   observa cerca de um terço dos `Started` — e os outros dois terços não
   existem para ele. Não é perda de precisão: duas abas atrás do load
   balancer, ligadas a nós diferentes, mostrariam números diferentes e ambos
   errados. A query no banco é hoje a única coisa que enxerga o cluster
   inteiro.
2. **O overview é agregado, não delta.** `executionCountsByStatus` é um
   `COUNT` sobre a tabela toda. Reconstruir isso a partir de eventos é
   manter estado derivado ao lado da verdade — e a entrega é best-effort por
   contrato: o publisher **descarta** o evento quando o executor satura
   (WARN, `RejectedExecutionException`), exatamente sob a carga em que o
   número importa. Um retrato perdido se corrige no tick seguinte; um delta
   perdido é divergência permanente até alguém apertar F5.
3. **Reconexão.** Hoje cair e voltar não perde nada, porque o próximo frame
   é o retrato inteiro. Entrega de evento exige `Last-Event-ID` e replay a
   partir de um log durável — que é precisamente o que a decisão v0.3 do
   REST-API-DESIGN recusou ("sem SSE na v1"), e a v0.7 só liberou porque um
   retrato periódico **não promete durabilidade nenhuma**.

O estado da arte divide as águas no mesmo lugar: JobRunr e db-scheduler
alimentam o dashboard por polling do storage, pelo mesmo motivo
multi-servidor; Temporal empurra evento ao vivo porque tem *event history*
durável e centralizada no servidor. Quem empurra evento tem log durável;
quem não tem, lê o estado.

### Notificação do banco (LISTEN/NOTIFY)
Resolveria o alcance cluster-wide do gatilho. Não é portável entre os três
dialetos que o `mohs-jdbc` suporta, e trocar uma limitação conhecida por uma
capacidade que só existe no PostgreSQL não é um bom negócio nesta camada.

## Consequences
- **Latência**: de ~1s na média para ~ms + o tempo da leitura, para o que
  acontece no nó que atende o stream.
- **Custo ocioso**: dashboard aberto com cluster parado passa a custar um
  retrato a cada `MAX_INTERVAL`, não dois por segundo.
- **Evento de outro nó não acorda este nó.** É a limitação central e ela é
  deliberada: o teto de 2s é o que cobre esse caso, então o pior desfecho é
  exatamente o comportamento de hoje. Vale a pena escrever porque um leitor
  futuro vai medir a latência num cluster e achar que encontrou um bug.
- **`Enqueued` não passa pelo publisher.** `ScheduleCommandImpl` o *retorna*
  como valor de retorno; ninguém o publica. Agendar via REST, portanto, não
  antecipa o retrato — cai no teto. Mudar isso altera o que os listeners de
  usuário recebem (pattern matching exaustivo passa a ver uma variante que
  nunca chegava) e fica para uma decisão própria.
- O timer deixa de ser um `ScheduledExecutorService` e vira uma thread em
  laço com `ReentrantLock`/`Condition` — mais código do que uma linha de
  agendamento. É o preço, e ele está pago em latência e em leituras que não
  acontecem.
