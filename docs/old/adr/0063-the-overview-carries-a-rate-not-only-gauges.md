# ADR-0063 — O overview carrega uma TAXA, não só medidores

Data: 2026-08-29 · Status: aceita · Complementa a ADR-0046 (que decidiu não mexer no stream)

## Contexto

O painel "Live work" do dashboard não condizia com a realidade. Investigado
subindo o `mohs-demo` contra Postgres e amostrando a API real — não por leitura
de código.

O painel plota três **medidores instantâneos**, amostrados a cada 2s pelo stream:

| série | consulta | quanto tempo uma execução fica ali |
|---|---|---|
| `ENQUEUED` | `COUNT(*) FROM mohs_ready` | até ser reivindicada — ~12ms (poll de 25ms) |
| `RUNNING` | `COUNT(*) FROM mohs_lease` | a duração do handler |
| `RETRY_WAITING` | ready com `attempt > 1` | só em retry |

O job do demo é `@RecurringJob(every = "PT1S")` com handler de um `log.info`.
Pela Lei de Little, `L = λ × W`:

- `RUNNING`: `1/s × 0,001s = 0,001` — uma amostra em mil.
- `ENQUEUED`: `1/s × 0,0125s = 0,0125` — uma em oitenta.

**Medido**, amostrando `/overview` 60 vezes em 30s (cadência mais rápida que a do
stream):

```
com trabalho vivo   = 4 de 60   (6,7%)
pico do empilhado   = 1
succeeded na janela = 39/min
```

(39/min é o que a janela deslizante contou naquela amostra; o disparo nominal do
job é 60/min, e é esse o número que aparece na tela mais abaixo. E o modelo de
Little subestima: `0,001 + 0,0125 ≈ 1,3%` contra os 6,7% observados, o que diz
que o `W` real do handler é de dezenas de ms, não de 1. A conclusão não muda —
medidor instantâneo é inútil para job rápido, com folga de 5× — mas a aritmética
é a ordem de grandeza, não a previsão.)

Na tela: o painel renderiza **um retângulo vazio** — não uma linha no zero, nada
— ocupando o maior espaço do Overview, enquanto o painel "Throughput" logo abaixo
mostra 60/min. Três dos seis tiles do topo ficam permanentemente em zero.

**As contagens estavam certas.** O erro era de pergunta: o título promete "quanto
trabalho está acontecendo" e o dado responde "quanto está enfileirado ou em posse
NESTE INSTANTE". Para job rápido, essas duas coisas são zero quase sempre.

E o dashboard **não tinha λ em lugar nenhum**, nem podia derivá-lo:
`succeededInWindow` é contagem sobre janela DESLIZANTE, então diferenciar duas
amostras consecutivas dá `(o que entrou) − (o que saiu pela outra ponta)`, que em
regime é ~zero. O cliente estava estruturalmente impedido de calcular a taxa.

## Decisão

1. **`OverviewSnapshot` passa a carregar DUAS leituras de vazão.** A longa
   (`throughput`, janela do usuário, default PT1M) responde "quanto foi feito"; a
   curta (`recent`) responde "está acontecendo algo agora". Elas coexistem porque
   são perguntas diferentes — a longa não vira taxa por diferenciação, e a curta
   não serve para o painel de vazão histórica.

2. **A janela curta é fixa em PT10S e NÃO é knob.** Ela não existe para o usuário
   escolher um recorte; existe para ser **dividida**. 10s é curto o bastante para
   significar "agora" e longo o bastante para não virar ruído de amostragem a uma
   execução por segundo. Configurá-la seria a "configuração para cenário
   hipotético" que o CLAUDE.md proíbe.

3. **`ThroughputReading(window, succeeded, failed)` é extraído**, com
   `perSecond()`. O trio janela+sucesso+falha passou a aparecer duas vezes — Data
   Clump (Fowler) — e a janela viajar junto com a contagem é o que torna a
   contagem interpretável.

4. **O wire carrega `ratePerSecond` já calculado.** A divisão é trivial, mas um
   cliente JSON teria de parsear a duração ISO-8601 para fazê-la, e um cliente que
   erra essa conta desenha um gráfico errado sem nada acusar. O denominador
   continua na resposta para quem quiser conferir.

5. **O stream ganha o frame `runners`.** `GET /runners` já publicava
   `{name, mode, max, running}` — o teto por runner. Concorrência sem denominador
   não diz se 59 é folga ou saturação, e o dado já existia; só não chegava ao
   Overview ao vivo. É leitura em memória do `RunnerRegistry`, **sem consulta** —
   o mais barato dos cinco frames.

6. **O medidor de concorrência é declaradamente POR NÓ.** Medido lado a lado sob
   carga: `overview.RUNNING` (`COUNT(*) FROM mohs_lease`, **cluster inteiro**) deu
   59/64/61 enquanto `runners[io].running` (contador em memória, **este nó**) deu
   55/57/59. Acompanham, mas dividir um pelo outro é errado: `max` é por nó, e
   `/runners` responde pelo nó que atendeu a requisição — atrás de um load
   balancer, um nó arbitrário. O painel rotula o escopo em vez de fingir um número
   de cluster.

## Consequências

- **Quebra binária** em `OverviewSnapshot` (construtor canônico e desconstrução) e
  em `ThroughputView`. Aceita por ser pré-release, pelo mesmo argumento da
  ADR-0062: antes do release custa zero, depois é major.
- `GET /overview` passa de **2 para 3 counts** por chamada, e o tick do SSE paga a
  terceira também. O custo está medido — ver a nota de medição abaixo.
- Com tick de 2s e janela de 10s, frames consecutivos se sobrepõem em 80%: a taxa
  é uma **média móvel de 10s amostrada a cada 2s**. É suavização deliberada — o
  painel quer tendência, não o ruído de uma janela de 2s a 1 execução/s.
- **Capacidade cluster-wide continua não derivável.** `/nodes` não carrega
  capacidade, e somar `max` exigiria que ele viajasse no heartbeat. **Gatilho:**
  primeiro operador que peça saturação do cluster, não do nó.
- **O consumidor chegou no mesmo dia.** O lote do backend nasceu sem consumidor
  (o dado viajava e o dashboard descartava); o passo seguinte fechou isso e a
  mitigação da decisão 6 saiu do papel. `types/api.ts` carrega `recent` e
  `ratePerSecond`, `useLiveUpdates` semeia o frame `runners`, o painel "Live
  work" virou "Activity" com a taxa em eixo próprio, e o tile "Live executions"
  (soma dos três medidores, permanentemente zero) deu lugar a "Executions/s" ao
  lado de "Running · io @ this node". O escopo é rotulado **nos dois lados** —
  o tile diz `this node`, a legenda do gráfico diz `· cluster` — porque rotular
  só um deixa o operador lendo a divergência de 59/64/61 contra 55/57/59 como
  defeito.
- **`runners` passou a ter escritor único enquanto o stream está vivo.** O
  `refetchInterval` de 2s da página de Runners saiu: com o stream fixado num nó
  e o poll em round-robin atrás do load balancer, os dois escreveriam
  contadores de **nós diferentes** na mesma chave, duas vezes por segundo. Fora
  desse estado, as invalidações sem escopo (volta à aba, fallback de 15s,
  refresh manual) refazem a busca contra um nó arbitrário — e isso fica: uma
  troca por ação do operador é legítima, duas por segundo são ruído.
- **O detector de queda passou a observar os dados, não o socket.** Efeito
  colateral do item acima, e ele vale para as cinco chaves: o pulso de 2s da
  página de Runners era a única query com batimento independente do SSE, e sem
  ele uma conexão semi-aberta (proxy que bufferiza, TCP meio-aberto — nunca
  dispara `error`) deixaria o header afirmando "live" sobre números congelados.
  Cada frame recebido agora rearma o mesmo temporizador da queda.
- **Limitação de skew que fica registrada:** contra um servidor anterior a esta
  ADR, o frame `runners` nunca chega, o stream segue entregando os outros
  quatro, e a página de Runners exibe o retrato do mount indefinidamente. O
  Overview ao lado acusa ("This dashboard needs a newer Mohs server"), então o
  operador tem o sinal — mas ele não está na página que congelou. Devolver um
  `refetchInterval` lento não é o conserto: 30s não elimina o interleaving entre
  nós, só o torna raro e inexplicável.
- **O guard de `recent` vale para as DUAS portas.** A mesma chave de cache é
  escrita pelo frame SSE e pelo `GET /overview`; validar só uma é conforto
  falso. `isOverview` mora em `lib/api.ts` e `fetchOverview` falha a query com
  uma frase acionável quando o servidor não manda `recent` — o caso real é o
  loop de dev (`npm run dev` contra um backend qualquer), não uma hipótese.
- O que esta ADR **não** cobre: histórico da taxa. A série continua viva só na
  aba, morre no reload, e leva 4s (duas amostras) para desenhar. Um endpoint de
  baldes (`GET /overview/rate?buckets=…`) resolveria os três, ao custo de um
  `GROUP BY` por balde — foi considerado e adiado. **Gatilho:** primeira
  reclamação de "perdi o gráfico ao atualizar a página".

## O custo, medido

Contra PostgreSQL 18.4 real, dois bancos de rascunho com o DDL exato de produção
de `mohs_attempt`, em duas densidades — porque **a resposta muda de sinal entre
elas**. Latências por `pgbench` (round trip), não `EXPLAIN` de tiro único.

**Por tick: é grátis.** `tick()` chama `buildFrames()` UMA vez e distribui a mesma
lista para todos os assinantes — a leitura curta já nasce cacheada por tick e
compartilhada. O frame `runners` não toca o banco.

| | statements/tick | consultas/s |
|---|---:|---:|
| antes (4 frames) | 6 | 3,0 |
| depois (5 frames) | 7 | **3,5** |

**+0,5 consulta/s, e o número é o mesmo com 1 ou com 64 assinantes** — contra os
4,0/s por nó ocioso do BASELINE. Em tempo de backend a 4k/s: 2,4 ms/s, 0,24% de
um backend.

**Por chamada: não é ruído no ponto de operação.**

| densidade | janela longa | janela curta | par | marginal |
|---|---:|---:|---:|---:|
| 1 exec/s (2M, 23 dias) | 0,109 ms | 0,093 ms | 0,198 ms | **+0,089 ms** |
| 4.000 exec/s (6M, 25 min) | 16,20 ms | 4,55 ms | 20,99 ms | **+4,79 ms (+30%)** |

Uma quinta parte do orçamento de 100 ms do §20.2 — **com a ressalva de que as
duas medidas não são a mesma coisa**: aquele orçamento é p99 sob carga a 10⁹
linhas de histórico, e este número é média de round trip em banco ocioso a 6M
com cache quente. Não leia como "há 4× de folga"; leia como "o piso cabe cinco
vezes". Subproduto que
corrige o registro anterior: a query custa **0,027 ms** de banco (`EXPLAIN
ANALYZE`) e **0,109 ms** de round trip (`pgbench`) a 1 exec/s — ou seja, dos
1,6 ms registrados antes, ~93% eram overhead de rede + JDBC, não tempo de banco.

**O índice serve, e a fronteira é fração da tabela, não contagem.**
`idx_mohs_attempt_throughput` resolve as duas janelas com Index Only Scan e
`Heap Fetches: 0`. Vira Parallel Seq Scan acima de ~60% da tabela: PT10S só
chegaria lá se a retenção de `mohs_attempt` caísse abaixo de **~20 segundos**.
Triplicar o acervo (2M → 6M) na mesma densidade não moveu um buffer — o §5.3
continua valendo: custa a janela, não o acervo.

**A ressalva honesta: a janela de 10s é justamente a mais fria.** Os números acima
são pós-`VACUUM`. Com 40k linhas recém-inseridas e sem vacuum, o mesmo PT10S vai a
646 buffers e `Heap Fetches: 40000` — +233% de buffers, +37% de tempo, marginal de
+5,96 ms. E esse é o estado PERMANENTE sob carga: `mohs_attempt` não declara
`autovacuum_*` próprios, então são ~400k inserts entre vacuums, ou 100s a 4k/s. Os
números quentes são o **piso otimista** da leitura curta.

**A armadilha para quem for "otimizar" isto depois.** Juntar as duas janelas numa
query só é ~27% mais barato, mas a forma óbvia — `COUNT(*) FILTER (WHERE
finished_at >= :recent)` sobre a janela longa — **está errada**:
`MIN_THROUGHPUT_WINDOW` é PT1S, então `?window=1s..9s` produz uma janela longa
MENOR que a curta e a contagem recente sai truncada. A forma correta precisa de
`WHERE finished_at >= LEAST(:since, :recent)`, e o `LEAST` é o que documenta no
SQL que as janelas não são aninhadas. Medido, ela dá −5,71 ms a 4k/s.
**Não adotada:** 5,7 ms num orçamento de 100 ms não paga trocar duas leituras
óbvias por uma com predicado que precisa de explicação (KISS). Se alguém adotar,
tem de ser com `LEAST`.

**Onde o custo do frame realmente está:** `countActiveByState` custa 13,2 ms com
500k de backlog — ~3× a chamada que esta ADR acrescenta. Se sobrar orçamento para
otimizar `/overview`, é ali.

**Ainda falta:** o endpoint sob carga com assinante SSE conectado — o gatilho do
`DASHBOARD-STREAM-REVIEW` segue de pé. Tudo acima é banco ocioso: nenhum engine
ticando, nenhum claim disputando as mesmas páginas. É o piso, não a produção.
Quando essa medição for feita, o baseline correto é o de TRÊS counts.

## Referências

`OverviewSnapshot`, `ThroughputReading`, `MohsImpl#overview`,
`io.mohs.rest.overview.ThroughputView`, `OverviewResponse`,
`OverviewStreamBroadcaster` (fork de `runners`); ADR-0046 (a decisão de NÃO mexer
no stream, que esta complementa sem revogar — o custo por tick continua sendo o
critério), ADR-0062 (o mesmo argumento de quebra binária pré-release),
`../DASHBOARD-STREAM-REVIEW.md`, `../REST-API-DESIGN.md`.
