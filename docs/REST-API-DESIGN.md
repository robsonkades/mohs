# Mohs — Design da API REST · draft v0.5

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
| `GET /overview` | Âncora de polling do dashboard: contagens por status, throughput da janela recente — barato por construção |
| `GET /jobs` · `GET /jobs/{jobKey}` | Definições registradas: agenda, políticas, estado, próximo fire |
| `POST /jobs/{jobKey}/schedule` | Invoca. Body: `{ "payload": {...}, "at"?, "delay"?, "priority"? }` — `at` (absoluto) × `delay` (ISO-8601, computado no servidor) exclusivos; ambos ausentes = agora; `priority` ausente = `NORMAL`. → 202 |
| `POST /jobs/{jobKey}/pause` · `/resume` | Suspende/retoma disparos automáticos (schedule manual segue permitido) |
| `GET /jobs/{jobKey}/executions` | Histórico do job (cursor) |
| `GET /executions?status=&jobKey=&from=&to=` | Busca global (cursor) |
| `GET /executions/{id}` | Detalhe: attempts, timestamps, erro, actor |
| `POST /executions/{id}/cancel` | Cancelamento cooperativo |
| `POST /executions/{id}/retry` | Retry manual de ops (bypassa política exaurida) |
| `GET /rate-limits` · `PATCH /rate-limits/{name}` | Estado e ajuste runtime de vazão (cluster-wide) |
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
