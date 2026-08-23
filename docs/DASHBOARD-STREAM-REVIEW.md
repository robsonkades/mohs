# Revisão futura do stream do dashboard

Companheiro da ADR-0046, no mesmo espírito de `docs/RATE-LIMIT-EVOLUTION.md` e
`docs/BATCH-ARCHITECTURE-REVIEW.md`: a ADR registra o que foi **decidido** — no
caso dela, decidido *não mudar* —; este arquivo registra o que ficou **aberto**,
e o gatilho concreto que faz cada item deixar de poder esperar.

Não é backlog. Item sem gatilho mensurável não entra aqui.

Estado em 2026-08-21: `GET /overview/stream` é um poll fixo de 2s, quatro frames
por tick, seis statements, zero sem assinante. Três alternativas foram
investigadas e rejeitadas (ADR-0046), duas delas depois de implementadas. Nada
está quebrado; o que segue é dívida conhecida e uma pergunta de desenho.

---

## 1. Ninguém nunca mediu o que este endpoint custa

**O que é.** Toda discussão de eficiência do stream até hoje — inclusive as duas
implementações revertidas — girou sobre aritmética de statements, não sobre
tempo de banco. Não existe número de quanto custam as seis leituras no ponto de
operação do BASELINE (4k execuções/s) com dashboard aberto, nem quanto a
contagem de throughput (`countTerminalOutcomesSince`, range de 1 min em
`mohs_attempts`) pesa quando essa janela tem centenas de milhares de linhas.

**Por que importa.** É o pré-requisito de qualquer decisão futura aqui. A regra
da casa é explícita — sem número, não é otimização —, e foi ignorada duas vezes
seguidas nesta área justamente por não haver número nenhum para ancorar.

**Gatilho.** Antes de qualquer nova proposta de otimização do stream. A receita
de bench e2e já existe; o que falta é a variante com assinante SSE conectado,
comparando com/sem.

---

## 2. O frame `executions` transmite um payload que o cliente joga fora

**O que é.** O tick lê as 50 execuções mais recentes, serializa e envia. O
cliente descarta: o frame vem sem filtro e a tela está quase sempre filtrada por
status/jobKey/janela, então `useLiveUpdates.ts` usa o evento só como aviso de
"algo mudou" e refaz a própria busca. É a query mais cara e o maior payload do
tick, gastos para transmitir um sinal de um bit.

**Conserto proposto.** Remover o frame e deixar o cliente invalidar a lista ao
receber `overview`, que chega no mesmo tick. Tira um statement de *todo* tick
(6 → 5) e o maior payload da rede.

**Por que não foi feito.** Remove um evento SSE nomeado — mudança do contrato
público documentado em `docs/REST-API-DESIGN.md`, que precisa de decisão do dono
do projeto e, se houver consumidor externo, de janela de versão.

**Gatilho.** A próxima mudança de versão do contrato REST, ou o item 1 mostrando
que o custo do tick importa — o que vier primeiro.

---

## 3. `nodeStatus.ts` julga frescor com o relógio do browser

**O que é.** O dashboard classifica um nó em Online/Stale/Lease expired com
`Date.now() - lastHeartbeatAt` e um orçamento fixo de 15s, contra a cadência
de heartbeat do nó — desde a Phase 6, `node-lease-ttl/3` (5s no default) em
idle, mais rápida sob carga (`mohs-ui/frontend/src/lib/nodeStatus.ts`).
Duas fragilidades, nenhuma introduzida hoje:

- o relógio do cliente não é o do servidor, e a margem toda é de 8s;
- um operador que suba `node-lease-ttl` para 30s estoura o orçamento sozinho —
  o próprio comentário do arquivo já admite isso.

**Por que importa mais do que parece.** `staleNodes` alimenta o painel "Needs
attention" com "stopped sending heartbeats". Um falso positivo ali é alarme
falso no painel cuja única função é ser confiável às 3h. Foi essa margem que
matou a alternativa 3 da ADR-0046: qualquer atraso no frame `nodes` consome
diretamente esses 8s.

**Conserto proposto.** Envelhecer o heartbeat contra o `asOf` do frame — o
envelope já o carrega e o `seed()` o descarta hoje. A idade volta a ser
`[0, poll-interval)` independente da cadência do canal e do relógio do cliente.

**Gatilho.** O primeiro relato de "Stale"/"stopped sending heartbeats" que o
operador diz ser falso; ou qualquer mudança que atrase o frame `nodes`; ou a
API passar a expor seus próprios thresholds.

---

## 4. `guardedTick` deixa um `Error` matar o stream em silêncio

**O que é.** `guardedTick` captura `RuntimeException`. Um `Error` — um
`OutOfMemoryError` numa serialização, por exemplo — escapa, e o
`ScheduledThreadPoolExecutor` **cancela a tarefa periódica** sem avisar
ninguém. O bean segue vivo, os `SseEmitter` não têm timeout (`new SseEmitter(0L)`),
e todo dashboard conectado congela em dados velhos: sem erro, sem fim de
stream, sem uma linha de log.

**Conserto proposto.** Capturar `Throwable`, logar em ERROR com a consequência
operacional na mensagem, e relançar se for `Error`. Ou um `finally` no laço que
denuncie a saída não planejada.

**Gatilho.** Qualquer relato de "o dashboard parou de atualizar e não achei
nada no log". Também vale antecipar se o tick um dia ficar mais frequente —
mais execuções, mais exposição.

---

## 5. A pergunta de desenho: o stream empurra tudo para todos

**O que é.** Todo assinante recebe os quatro frames, esteja em que página
estiver. Quem está na página de Jobs recebe `executions`; quem está em
Executions recebe `jobs`. O modelo do JobRunr, que a ADR-0046 cita como
referência, é diferente no detalhe: o servidor empurra **estatísticas** (o que
muda sempre, barato) e cada página busca as listas de que precisa.

**Por que ainda não decidimos.** Com quatro páginas e quatro frames o
desperdício é pequeno e o desenho atual é mais simples — um tick compartilhado,
custo independente do número de dashboards, uma conexão por aba. A pergunta
fica de pé porque o custo cresce com o número de frames, não com o de páginas:
quando `GET /batches` existir e a página de Batches entrar (ADR-0045 §2), o
critério de "o que entra no stream" precisará estar escrito — e hoje não está.

**Gatilho.** A quinta página, ou o segundo frame novo, ou o item 1 mostrando que
o tick pesa. Nesse ponto a decisão é entre: (a) manter o retrato completo e
aceitar o crescimento; (b) frames por assinatura, o cliente declara o que quer
ao conectar; (c) só estatísticas no stream e listas por polling da página.

**Estado (2026-08-21) — o gatilho disparou.** Runners é a quinta página, e o
caso dela foi resolvido por (c): polling próprio de 2s, nada no stream.

A regra que ficou **não** é "listas saem do stream" — é **dado node-local não
entra em canal cluster-wide**. O stream promete uma visão de cluster
(`overview`, `jobs`, `nodes` são estado no banco, iguais de qualquer nó);
ocupação de runner é do processo. Enfiá-la ali entregaria um número cujo
significado depende de qual nó terminou o handshake do SSE, e o operador não
tem como saber qual foi. Não é caro — é errado.

Para dado **compartilhado** (Batches, quando `GET /batches` existir) a escolha
entre (a), (b) e (c) continua aberta, com o mesmo gatilho de antes.

---

## O que aprendemos aqui, e vale além do stream

**Atrasar um dado cujo significado é o tempo faz o atraso virar parte do dado.**
Foi o que matou as duas tentativas de cadência escalonada: `lastHeartbeatAt`
vira falso alarme de nó parado, `nextFireAt` vira falso atraso de disparo. Vale
para qualquer painel futuro que mostre "há quanto tempo" ou "daqui a quanto".
