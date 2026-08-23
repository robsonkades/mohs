# ADR-0056 — Poll adaptativo com hand-off local; o poll é o único caminho cross-nó

Data: 2026-08-23 · Status: aceita · Fase: Phase 6 do redesign (ADR-G do plano, sem o tier 2 — retirado pela ADR-0054; commits bf8738d → S6.4)

## Contexto

O poll era fixo em 5s por default: latência de dispatch de ~2,5s no caso
médio e um teto arbitrário sob carga. Baixar o piso resolve a latência e
piora o ocioso; a ADR-G propôs resolver os dois com um intervalo
adaptativo mais wake-ups por sinal.

O §5.5 previa três tiers de wake-up. O tier 2 (`LISTEN/NOTIFY`) foi
implementado, medido e **retirado** (ADR-0054): `pg_notify` na transação
do enqueue serializa o commit e derrubou o ingest REST pela metade. Esta
ADR registra o que sobrou — e o que a retirada custa, agora com número.

## Decisão

1. **O tick vira uma platform thread própria** (`mohs-engine-loop`,
   daemon), no lugar do `ThreadPoolTaskScheduler` de intervalo fixo. Ela
   espera em `Condition.await(nextDelay)` — interrompível por sinal, com
   duração por relógio monotônico. `ReentrantLock` + `Condition` pelas
   capacidades (JCIP cap. 13), não por pinning.
2. **Backoff adaptativo**: `mohs.engine.poll-interval` passa a ser o PISO
   (default **25ms**, era 5s como intervalo fixo) e nasce
   `mohs.engine.max-poll-interval` (default 2s) como TETO. Dobra a cada
   tick vazio, volta ao piso no primeiro tick com trabalho.
   `max <= poll` desliga o adaptativo.
3. **O sono é sempre limitado por `node-lease-ttl / 3`**, nas duas
   pontas: o heartbeat sai uma vez por tick, então a cadência do tick É a
   cadência da promessa de liveness. Um teto de backoff maior que o cap é
   engolido com WARN no start — liveness vence configuração. Este era o
   risco nº 1 da fase.
4. **Tier 1 — hand-off local**: um enqueue já devido feito NESTA JVM
   acorda o loop pós-commit (`afterCommit` quando há transação ativa,
   imediato quando não há). Best-effort por contrato: sinal perdido é
   coberto pelo poll, nunca por correção.
5. **Não há tier 2.** O poll adaptativo é o único caminho de descoberta
   cross-nó.
6. **Interrupt na thread do loop é engolido** de propósito: a Engine é
   dona da thread e o protocolo de parada é estado + wake (JCIP 7.1.3);
   re-armar a flag viraria busy-spin.

## Consequências

**Medido no S6.4** (BASELINE "Phase 6 — S6.4"):

- **Latência de dispatch num cluster ocioso de 1 nó: p50 25,3ms**
  (p95 59,8ms). Contra ~2,5s do poll fixo de 5s — a alavanca principal da
  ADR-G entregou.
- **Latência num cluster ocioso de 4 nós: p50 461ms, p95 1,65s, máx
  1,85s.** A atribuição por nó despachante fecha o mecanismo: o nó que
  recebeu o POST despachou 6 de 20 a p50 25,2ms (hand-off local); os
  outros três despacharam 14 de 20 a p50 504–844ms (o poll deles). **O
  hand-off local só ajuda quando quem recebe o enqueue é o dono do shard
  — 1/N por construção.** Este é o preço da ADR-0054, e ele é
  exclusivamente do cluster multi-nó OCIOSO: sob tráfego o backoff fica
  no piso de 25ms e o efeito some.
- **O gate "p50 dispatch < 5ms" não passa**, e não passaria: era o
  número do tier NOTIFY, que não existe mais. Fica registrado como o que
  é — um gate órfão da decisão que o originava, não uma regressão.
- **O gate "ocioso < 10/s" melhorou 24× no S6.5, e ainda assim não passa
  ao pé da letra** — o alvo do §21 é por CLUSTER de 10 nós, e reler como
  "por nó" depois de medir seria mover a trave. Na primeira medição
  eram 96 consultas/s com 1 nó e 109/s com 4 — 96% disso era o lap de 64
  sondas da ADR-0055, não a frequência do tick. Com o gate ocioso do S6.5
  (uma sonda `EXISTS` no lugar do lap enquanto a rodada anterior voltou
  vazia): **4,0 consultas/s por nó**, 16/s num cluster de 4 — e o termo
  que a ADR-G projetava, o do claim, ficou em 0,5/s por nó, ou seja 5/s
  em 10 nós, exatamente a conta original. O termo que ESTA ADR controla
  sempre esteve onde foi projetado: período de tick medido em 2,07s com o
  backoff no teto.
- O que sobra no ocioso são os 7 statements de manutenção por tick
  (heartbeat, nós, leases, definições, purge) — ~3,5 consultas/s por nó,
  anteriores à Phase 6 e fora do escopo desta ADR. São eles que fazem 10
  nós ociosos extrapolarem para ~40/s em vez dos <10 do gate literal do
  §21; pendência própria no PLAN.md.

**Trade-offs assumidos:**

- Mudança de default visível: `poll-interval` 5s → 25ms. Quem se importa
  com o ocioso pina `poll-interval`/`max-poll-interval`; quem se importa
  com latência ganha 100× sem tocar em nada.
- Duas fontes de acordar (sinal e timeout) num loop só; a correção não
  depende de nenhuma das duas — depende da fila no banco.
- `max-poll-interval` é o limite superior honesto da latência de
  descoberta cross-nó. Um requisito real abaixo disso é o gatilho de
  retorno do NOTIFY registrado no PLAN.md.
- Reversível: `max-poll-interval = poll-interval` volta ao poll fixo.
