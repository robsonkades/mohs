# Mohs — Design da API REST · draft v0.9

Fonte de contrato do **M2** do Plano de Desenvolvimento (MOHS-DOCUMENTO-MESTRE.md §9):
DTOs e assinatura dos endpoints antes da implementação real (M3).

Plano OPERACIONAL do Mohs. Definições de job (handler, agenda, políticas)
pertencem ao código — não existem endpoints de criação/alteração de
definição, por design.
v0.2: `/enqueue` renomeado para `/schedule` — um vocabulário em todas as superfícies.
v0.3: sem SSE na v1 (polling-first); sem autenticação embutida na v1 (guardrails
abaixo); dashboard consome esta mesma API (+ `GET /overview`).
v0.4: `GET /nodes` promovido de roadmap pra v1 — registro de heartbeat por
node já é parte obrigatória do design de liveness (M3); endpoint é leitura
fina por cima, não infraestrutura nova; Idempotency-Key com durabilidade
explícita [DECIDIDO].
v0.5: `GET/PATCH /queues` removido — `JobQueue` foi removida por completo (ADR-0021) [DECIDIDO].
v0.6: `GET /overview` — contagens só do trabalho vivo [DECIDIDO], ver seção própria.
v0.7: `GET /overview/stream` (SSE de snapshot) — REVISA parcialmente a decisão 1 da v0.3,
ver seção do overview; contagens do overview sem lock em todo dialeto [DECIDIDO].
v0.8: `?window=` no `GET /overview` (default 60s, clamp 1s–1h); o stream fica no
default — o tick é compartilhado, janela por assinante quebraria o modelo de custo.
v0.9: listas de execuções viram SUMÁRIO sem `attempts` (alinha o wire ao que a
tabela sempre disse: attempts são do detalhe); eventos do stream envelopados em
`{asOf, data}` — o carimbo do retrato pro frontend ordenar/distribuir [DECIDIDO].
v0.10: `GET/PATCH /rate-limits` implementados sobre o enforcement da ADR-0042 —
`currentCount` do contrato M2 vira `available` (tokens no balde; "usado" não é
grandeza de balde, o refill é contínuo e não tem virada de janela onde zerar).
PATCH em nome não declarado é 404, nunca criação implícita: declarar é ato de
boot [DECIDIDO].

## Princípios

1. **202 como contrato.** Toda invocação responde `202 Accepted` com o
   recibo (`executionId`, `scheduledAt`) e `Location: /executions/{id}` —
   a expressão HTTP de "durabilidade síncrona, execução assíncrona".
2. **Actor via SPI (`ActorResolver`).** Toda ação mutável grava quem fez:
   com segurança plugada, o principal; sem ela (v1), o header `X-Mohs-Actor`
   — atribuição declarativa, não autenticada — ou `anonymous`.
3. **Erros em RFC 7807** (`application/problem+json`), com `detail` que
   ensina a corrigir — o equivalente HTTP das validações de boot.
4. **Paginação por cursor** em toda listagem (executions cresce sem teto).
5. **Fechada por padrão, sem auth embutida na v1:** `mohs.api.enabled=false`
   é lei — habilitar é ato consciente e gera **WARN destacado no boot**
   ("API operacional sem autenticação; não exponha publicamente"). Guia de
   deployment: rede interna, gateway ou mTLS à frente. A 1.x poderá plugar
   segurança trocando apenas o `ActorResolver` — zero mudança de contrato.
   Prefixo configurável (`mohs.api.base-path`, default `/api/mohs/v1`).

## Superfície

| Método e caminho | Efeito |
|---|---|
| `GET /overview?window=` | Âncora de polling do dashboard: contagens por status, throughput da janela (`window` opcional: `15m`/`PT15M`, default 60s, clamp 1s–1h) — barato por construção |
| `GET /overview/stream` | O mesmo retrato, empurrado por SSE a cada 2s: eventos nomeados `overview`/`jobs`/`nodes`/`executions` — polling movido pro servidor, não entrega de eventos |
| `GET /jobs` · `GET /jobs/{jobKey}` | Definições registradas: agenda, políticas, estado, próximo fire |
| `POST /jobs/{jobKey}/schedule` | Invoca. Body: `{ "payload": {...}, "at"?, "delay"?, "priority"? }` — `at` (absoluto) × `delay` (ISO-8601, computado no servidor) exclusivos; ambos ausentes = agora; `priority` ausente = `NORMAL`. → 202 |
| `PATCH /jobs/{jobKey}/schedule` | Muda a agenda em runtime (ADR-0036). Body: `ScheduleView` (`CRON`/`INTERVAL`/`ON_DEMAND`; `ON_DEMAND` desarma a recorrência). Emergência: boot reverte sob `on-conflict=override` (aviso no envelope). Cron irrealizável → 422 |
| `POST /jobs/{jobKey}/pause` · `/resume` | Suspende/retoma disparos automáticos (schedule manual segue permitido) |
| `GET /jobs/{jobKey}/executions` | Histórico do job (cursor) — sumário, sem `attempts` (v0.9) |
| `GET /executions?status=&jobKey=&from=&to=` | Busca global (cursor) — sumário, sem `attempts` (v0.9) |
| `GET /executions/{id}` | Detalhe: attempts, timestamps, erro, actor |
| `POST /executions/{id}/cancel` | Cancelamento cooperativo |
| `POST /executions/{id}/retry` | Retry manual de uma execução `FAILED` (ADR-0033): a MESMA linha rearma como `RETRY_SCHEDULED` devida agora e disputa o claim normal — bypassa a política exaurida. Sem `Idempotency-Key` (retry não deduplica; a idempotência é o CAS). → 202; estado ≠ `FAILED` → 409 |
| `GET /rate-limits` · `PATCH /rate-limits/{name}` | Estado (`max`, `window`, `available`) e ajuste runtime de vazão, cluster-wide (ADR-0042). PATCH em limite não declarado → 404: declarar é ato de boot |
| `GET /runners` | Visão por nó: modo, max, em execução (node-local por natureza) |
| `GET /nodes` | Visão de cluster: nodes com heartbeat recente, last-seen |
| `GET /batches/{id}` | Contadores agregados e estado do lote |

## Invocação — exemplo do contrato

```http
POST /api/mohs/v1/jobs/welcome-email/schedule
Idempotency-Key: 7f3a…
X-Mohs-Actor: ana.ops
Content-Type: application/json

{ "payload": { "user": "u1", "name": "Ana", "age": 31 },
  "at": "2026-09-01T05:00:00Z" }
```

```http
HTTP/1.1 202 Accepted
Location: /api/mohs/v1/executions/01J4X…
Content-Type: application/json

{ "executionId": "01J4X…", "jobKey": "welcome-email",
  "scheduledAt": "2026-09-01T05:00:00Z", "actor": "ana.ops" }
```

- **Idempotency-Key**: mesma key → mesma resposta, zero duplicação, enquanto
  a execução existir — janela = retenção de execuções, mínimo ~24h quando a
  política de retenção existir (ADR-0030). Obrigatória para clientes que
  fazem retry (todos deveriam).
  Na API Java, o equivalente é o pré-terminal `.idempotencyKey(...)`.
  **Durabilidade [DECIDIDO]:** persistida junto da Execution, mesma garantia
  de custódia da cláusula assíncrona — sobrevive restart, é cluster-wide,
  não é cache local de node.
- Payload é validado contra o tipo da definição; incompatível → 422
  problem+json apontando o campo.
- Job pausado aceita schedule manual (espelha o motor); job inexistente →
  404 com sugestão de jobKeys próximos.

## PATCH runtime × configuração de boot [DECIDIDO]

`PATCH /rate-limits/{name}` altera o valor cluster-wide imediatamente e de forma
durável — **até o próximo boot**, quando a configuração do código/properties
reaplica (boot vence — comportamento do default `mohs.registration.on-conflict:
override`; sob `preserve`, o PATCH sobrevive a deploys). A resposta do PATCH inclui o aviso: mudança de
emergência; codifique em properties para torná-la permanente. Alternativa
rejeitada (ops-pin vence o boot): institucionaliza drift entre repositório e
produção.

## `GET /overview` — contagens só do trabalho vivo [DECIDIDO]

`executionCountsByStatus` carrega só `ENQUEUED`/`RUNNING`/`RETRY_SCHEDULED`
(sempre os três, zero incluso). Não há contagem all-time de estados
terminais, de propósito: o histórico cresce sem teto e o endpoint é âncora
de polling — "barato por construção" significa custo proporcional ao
trabalho vivo e à janela de throughput, nunca ao tamanho da tabela. A
atividade terminal recente está em `throughput` (`succeeded`/`failed` na
janela). A janela é selecionável por `?window=` (estilo simples `15m` ou
ISO-8601 `PT15M`; default 60s) com clamp em `1s–1h` — o custo da contagem
é proporcional à janela, e sem teto um `?window=30d` num sistema a 4k
exec/s varreria centenas de milhões de attempts, quebrando o "barato por
construção". Fora do intervalo, aplica-se o limite e a janela APLICADA
viaja na resposta (`throughput.window`) — mesmo idioma do clamp de
`size` na paginação. `CANCELLED` fica fora da vazão de propósito: vazão é trabalho
*concluído*, e cancelamento é decisão do operador, não desfecho de
execução — um cancel em massa aparece como backlog caindo com throughput
estável, que é a leitura correta (um `throughput.cancelled` é candidato a
v0.7 se a operação sentir falta). As contagens são leituras independentes,
não um corte transacional — números podem divergir entre si por execuções
transitando durante a consulta (read skew aceitável para polling). Planos
de execução por dialeto: `docs/performance/explain-overview-*.txt`.

**Sem lock em todo dialeto [DECIDIDO]:** as contagens do overview nunca
adquirem lock — monitoramento jamais disputa com o caminho quente do
claim/conclusão. Em H2/Postgres/MySQL o SELECT MVCC já é não-bloqueante;
no SQL Server (READ COMMITTED default toma shared locks) as contagens
levam `WITH (NOLOCK)` via dialeto (`READPAST` subcontaria
sistematicamente sob carga). O erro aceito é o do pior caso do
mecanismo: linha em transição, dupla contagem/perda sob page split
(allocation-order scan) e, raramente, o erro 601 ("data movement") —
que aparece como falha transitória da leitura e é aceito (o stream loga
e tenta no próximo tick; o GET responde 500 transitório). Deployment com
RCSI torna o hint redundante — decisão do operador. Só contagem: leitura
que hidrata entidade nunca usa o hint.

**`GET /overview/stream` [DECIDIDO] — revisão parcial da decisão 1 da
v0.3:** aquela decisão rejeitou *entrega de eventos* best-effort sem a
futura tabela durável — e continua valendo para eventos. O stream é outra
coisa: **snapshot periódico** (polling movido pro servidor) — cada frame é
o retrato completo, desconexão não perde nada, durabilidade não é
prometida. Eventos SSE nomeados, um por tipo (`overview`, `jobs`,
`nodes`, `executions`), cadência fixa de 2s; um tick compartilhado
alimenta todos os assinantes (custo independe do número de dashboards;
zero sem assinante). Todo evento é envelopado em `{asOf, data}` (v0.9):
`asOf` é o instante do retrato — um só pros 4 eventos do mesmo tick, do
Clock do servidor — pro frontend distribuir atualizações sem depender do
relógio do cliente. Contrato do carimbo: `asOf` INFORMA frescor (lower
bound — as leituras começam depois dele) e desambigua o único reorder
real (retrato inicial do subscribe × tick concorrente); o frontend NÃO
deve descartar por comparação estrita entre ticks — o SSE já entrega em
ordem, e o Clock do servidor pode recuar no resync (wall clock carimba,
não ordena). Se um dia o frontend precisar de descarte estrito, o
envelope ganha um `seq` monotônico — decisão adiada até esse requisito
existir. O frame
`executions` é o mesmo sumário das listas (sem `attempts`, v0.9) e usa a
mesma primeira página do `GET /executions`. Vocabulário: `nodes` é a visão de cluster (não
"workers" — vocabulário travado no API-DESIGN); `queues` não existe
(ADR-0021 removeu `JobQueue` por completo); `runners` entra no stream
quando ganhar leitura na fachada (hoje é contrato M2 sem implementação).
SSE `/executions/stream` sobre a tabela durável de eventos continua no
roadmap, inalterado.

## O que NÃO existe, por design

- `POST/PUT/DELETE /jobs` — definição é código (handler não trafega em HTTP).
- Alteração de cron/retry/runner via API — mesma razão; fonte única de verdade.
- Endpoint síncrono "executa e espera resultado" — violaria o contrato
  assíncrono; desfecho se observa em `GET /executions/{id}`.

## Decisões v0.3 [DECIDIDO]

1. **Sem SSE/webhooks na v1** — dashboard e integrações operam por polling
   (cursor + `/overview`). Entrega push entra no roadmap sobre a futura
   tabela de eventos (entrega durável), não sobre eventos best-effort.
2. **Sem autenticação embutida na v1** — sob os guardrails do princípio 5.
   Discordância do líder técnico registrada: API operacional exposta sem
   auth é risco; mitigada por default-off + WARN + guia de deployment.
3. **Dashboard consome exatamente esta API** — dogfooding estrutural: toda
   feature de tela nasce scriptável, e a suficiência da API é provada em
   produção diariamente.

## Roadmap (fora da v1)

- SSE `/executions/stream` e webhooks sobre tabela de eventos durável.
- `POST /nodes/{id}/drain` — drenagem remota de nó; `GET /nodes`
  (visibilidade) promovido pra v1 — reusa o registro de heartbeat que a
  liveness (M3) já precisa construir, só falta o endpoint de comando.
- Autorização fina (papel de leitura × papel de operação) via módulo de
  segurança plugando o `ActorResolver`.
