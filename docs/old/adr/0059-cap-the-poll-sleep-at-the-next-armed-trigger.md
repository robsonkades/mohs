# ADR-0059 — O sono do poll é limitado pelo próximo gatilho armado

Data: 2026-08-29 · Status: aceita · Complementa a ADR-0056 (não a revoga)

## Contexto

Um job `@RecurringJob(every = "PT1S")` no `mohs-demo` não executa de
segundo em segundo: executa em pares. Medido com `logging.level.io.mohs=DEBUG`,
23 disparos em 21,4s — a CONTAGEM está certa, um por segundo — mas os
intervalos de parede entre eles são `0,43 · 1,61 · 0,40 · 1,60 · 0,39 ·
1,60 …`, nunca 1,0s. Cada disparo materializa exatamente uma ocorrência e
`next_fire_at` avança exatamente 1s (`…36.036911Z → 37.036911Z →
38.036911Z`): não há duplicação, nem CAS perdido, nem reclaim. O que está
errado é a PONTUALIDADE.

A causa é o backoff adaptativo da ADR-0056 lido junto com a agenda. Depois
de um tick com trabalho o intervalo volta ao piso e dobra a cada tick
vazio, então o loop acorda nas somas parciais:

```
25ms · 75 · 175 · 375 · 775 · 1575 · (teto de 2s)…
```

A ocorrência fica devida em `+1000ms`, que não é nenhum desses pontos: o
tick de 775ms vê a fila vazia, dobra, e o próximo só chega em 1575ms — o
disparo sai ~575ms atrasado. O backoff então reseta, e a ocorrência
seguinte (devida em `+2000ms`, ou `+425ms` deste tick) cai perto do ponto
de 375ms e sai quase em dia. O par se repete indefinidamente. Os `0,39` e
`1,59` medidos são literalmente os pontos `375/775` e `1575`.

O ponto é estrutural, não numérico: **o backoff é cego a deadline.** Ele
foi desenhado inteiro sobre latência de *enqueue* — hand-off local, o tier
NOTIFY retirado pela ADR-0054, descoberta cross-nó — onde nada é sabido de
antemão e o backoff é a resposta certa. O gatilho recorrente é o caso
oposto: `next_fire_at` é um instante FUTURO conhecido, lido do banco a
cada tick por `loadDefinitions()`, e o cálculo do sono simplesmente o
ignorava.

É onde estávamos atrás do estado da arte. O `QuartzSchedulerThread` calcula
`timeUntilTrigger` e dorme até o próximo gatilho, nunca um intervalo cego;
o db-scheduler faz o mesmo sobre o `executionTime` mais próximo. Nós
tínhamos o dado na mão e não o usávamos.

## Decisão

1. **O sono de cada volta é limitado pelo `next_fire_at` armado mais
   próximo** — `Engine.cappedByNextFire`, aplicado sobre o resultado do
   backoff em `runLoop`.
2. **O cap encurta o SONO, nunca o estado do backoff.** Um tick antecipado
   por gatilho não conta como tick com trabalho, e a progressão da
   ADR-0056 segue intacta — mesmo tratamento que o cap de liveness
   (`node-lease-ttl / 3`) já recebia. Os dois caps compõem sem se conhecer.
3. **O horizonte tem duas fontes**, porque o snapshot de definições do
   tick é ANTERIOR ao disparo: os gatilhos que o tick não tocou valem pelo
   que o snapshot diz — só os ainda futuros; um devido que não disparamos
   já está atrasado e drena no tick seguinte, sem encurtar sono nenhum — e
   os que o tick disparou valem pelo instante que o CAS acabou de armar
   (`FiringOutcome.rearmedAt`). Sem a segunda fonte a correção não existe:
   em regime, o gatilho relevante é sempre o que ACABOU de ser armado.
4. **Nenhuma consulta nova.** As duas fontes já estavam em memória.
5. **O piso do sono continua sendo `poll-interval`.** O cap encurta o sono
   ATÉ o piso, nunca abaixo dele — é o que impede a agenda de redefinir a
   cadência de tick: sem piso, N jobs recorrentes de intervalo T acordariam
   o nó a cada ~`T/N`, cada gatilho pagando os ~7 statements de manutenção
   de um tick inteiro sozinho em vez de sair no mesmo lote que os vizinhos
   (é para isso que `FIRE_LIMIT` e `BATCH_SIZE` existem). Com o piso, o
   atraso máximo de um gatilho é o que o knob promete — 25ms no default — e
   a decisão 2 da ADR-0056 (`poll-interval` É o piso do sono) segue
   intacta. O piso também subsome a guarda de busy-spin: gatilho devido,
   vencido ou a microssegundos dorme o poll, nunca `await(0)` em laço
   contra o banco.
6. **Fora de `RUNNING` não há horizonte.** Quem não dispara não tem
   deadline a honrar — `PAUSED`/`DRAINING` devem só o heartbeat. Um tick
   que falhou também não publica horizonte: ele não chegou a apurar um.
7. **Nenhum knob novo.** `poll-interval` e `max-poll-interval` mantêm o
   significado que a ADR-0056 lhes deu.

## Consequências

- Um job recorrente passa a disparar com atraso limitado pelo
  `poll-interval`, não pelo backoff. O regressor
  (`EngineTest#recurringJobFiresOnItsOwnIntervalNotOnTheBackoffPoints`)
  reproduz o sintoma reportado no formato que o produziu — piso 25ms, teto
  2s, job `PT1S` — e afirma o ESPAÇAMENTO entre execuções sucessivas
  (~1,0s), não a contagem: no defeito a contagem sempre esteve certa.
  Medido com o cap desligado, o teste falha; com ele, passa em ~4,4s.
- **O atraso limitado é o do DISPARO**, não o do dispatch. A ocorrência
  materializada ainda precisa ser reivindicada pelo dono do shard, que pode
  ser outro nó — e esse só recupera horizonte no tick seguinte (o backoff
  dele já zerou: `due` não veio vazio). É por isso que a remedição do
  `RecurringTriggerScenario` pedida no PLAN.md continua necessária para
  separar o que sobra de fato para a ADR-0054.
- **Um nó com jobs recorrentes acorda pelo menos uma vez por gatilho
  devido.** É o preço, é o que Quartz cobra, e é proporcional à agenda que
  o usuário declarou — não a um teto arbitrário. O gate de ocioso do §21
  não muda: sem gatilho armado não há cap a aplicar, e o caminho ocioso
  medido no S6.5 continua sendo o dos 7 statements de manutenção.
- O relógio do cap é o `Clock` injetado e a distância sai de
  `Duration.between`, não de `nanoTime`: a distância até um INSTANTE de
  parede só existe na escala de parede. A espera em si continua monotônica
  (`Condition.awaitNanos`). Mesmo cálculo do `RetrySchedule`, não uma
  medição de duração decorrida.
- **O que esta ADR NÃO cobre:** `mohs_ready.visible_at`. Um retry marcado
  para `now + 2s`, ou uma ocorrência com visibilidade adiada, continua
  sendo descoberto pelos mesmos pontos de backoff — mesma cegueira, outra
  tabela. O horizonte das definições é de graça (já está em memória); o da
  fila exigiria um `min(visible_at)` por tick, que não é. Fica como
  pendência com gatilho no PLAN.md, não como escopo silenciosamente
  omitido.
- **O filtro `!paused && !orphaned` de `earliestArmedFire` DUPLICA em Java
  o `WHERE` de `JdbcJobStore#findDueRecurring`.** É duplicação deliberada —
  o horizonte sai do snapshot já em memória, e consultá-lo de novo custaria
  a query que a decisão 4 recusa —, mas divergir os dois lados é falha
  silenciosa: o nó acorda na cadência de um gatilho que ninguém dispara. O
  pareamento está ancorado em `EngineSleepTest.EarliestArmedFire` (a regra
  em Java) e em `EngineTest#theHorizonSeesExactlyWhatFindDueRecurringWouldFire`,
  o único que submete a mesma fixture aos dois lados — inclusive ao
  `retired = false` que o horizonte herda de `findAll` e não sabe repetir.
- Reversível: o cap é uma função pura de quatro argumentos; devolver
  `delay` sem olhar o horizonte restaura o comportamento da ADR-0056.

## Referências

`Engine#cappedByNextFire`, `Engine#earliestArmedFire`,
`Engine.FiringOutcome`, `Engine.TickOutcome`; ADR-0056 (o backoff que
esta complementa), ADR-0054 (por que o poll é o único caminho cross-nó),
ADR-0035 (o disparo do gatilho devido).
