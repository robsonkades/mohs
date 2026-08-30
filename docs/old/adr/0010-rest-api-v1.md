# ADR-0010: API REST v1

## Status
Decided — 2026-08-12

## Context
O Mohs precisa de uma API HTTP operacional — para dashboard e
integrações — mas as definições de job (handler, agenda, políticas) já
pertencem ao código por decisão de arquitetura (ADR-0002); a API REST não
pode reabrir esse canal sem quebrar a fonte única de verdade. Ao mesmo
tempo, a API precisa expressar em HTTP o mesmo contrato assíncrono já
decidido para a API Java (ADR-0003), e decidir posturas sobre
autenticação, formato de erro e paginação antes do lançamento da v1.

## Decision
Cinco princípios fecham o design da v1:

1. **202 como contrato** — toda invocação responde `202 Accepted` com o
   recibo (`executionId`, `scheduledAt`) e `Location: /executions/{id}` —
   a expressão HTTP de "durabilidade síncrona, execução assíncrona".
2. **Actor via SPI (`ActorResolver`)** — toda ação mutável grava quem
   fez: com segurança plugada, o principal; sem ela (v1), o header
   `X-Mohs-Actor` (atribuição declarativa, não autenticada) ou
   `anonymous`.
3. **Erros em RFC 7807** (`application/problem+json`), com `detail` que
   ensina a corrigir — equivalente HTTP das validações de boot.
4. **Paginação por cursor** em toda listagem (executions cresce sem
   teto).
5. **Fechada por padrão, sem auth embutida na v1** — `mohs.api.enabled=false`
   é lei; habilitar é ato consciente que gera WARN destacado no boot
   ("API operacional sem autenticação; não exponha publicamente"); guia
   de deployment recomenda rede interna, gateway ou mTLS à frente; a 1.x
   poderá plugar segurança trocando apenas o `ActorResolver`, sem mudança
   de contrato. Prefixo configurável (`mohs.api.base-path`, default
   `/api/mohs/v1`).

Superfície v1 é puramente operacional: sem `POST/PUT/DELETE /jobs`, sem
alteração de cron/retry/runner via API, sem endpoint síncrono de "executa
e espera resultado" — todos violariam a fonte única de verdade (código)
ou o contrato assíncrono.

Decisões v0.3 adicionais: **sem SSE/webhooks na v1** (dashboard e
integrações operam por polling — cursor + `GET /overview`; push entra no
roadmap sobre uma futura tabela de eventos durável, não sobre eventos
best-effort); **sem autenticação embutida na v1** (discordância do líder
técnico registrada — API operacional exposta sem auth é risco — mitigada
pelos guardrails do princípio 5: default-off + WARN + guia de
deployment); **dashboard consome exatamente esta mesma API** (dogfooding
estrutural — toda feature de tela nasce scriptável, e a suficiência da
API é provada em produção diariamente).

## Consequences
A ausência de auth embutida na v1 é um risco aceito conscientemente, não
ignorado — mitigado por três camadas (fechada por padrão, WARN ao abrir,
guia de deployment), com o caminho de migração para autenticação real já
desenhado (troca de `ActorResolver`, zero mudança de contrato). A
ausência de SSE mantém a v1 mais simples às custas de latência de
descoberta de eventos (dependente do intervalo de polling do dashboard);
`GET /nodes` já foi promovido para v1 porque reusa o registro de
heartbeat que a liveness de M3 (ADR-0012) já precisa construir — não é
infraestrutura nova.

## Source
docs/REST-API-DESIGN.md "Princípios" (lines 17-33); docs/REST-API-DESIGN.md
"Decisões v0.3" (lines 103-113)
