# ADR-0001: Empacotamento — módulo único, full Spring Boot

> **Nota (2026-08-13):** o ponto 1 da Decision (fronteira por pacote —
> `io.mohs` como pacote único) foi revisado pela
> [ADR-0013](0013-public-api-subpackaging.md): `io.mohs` deixou de ser um
> pacote único e se dividiu em subpacotes coesos. O restante desta ADR
> (módulo Maven único, web opcional, test kit no jar) permanece decidido
> e inalterado.

## Status
Decided — 2026-08-12

## Context
Mohs precisa escolher entre um layout multi-módulo (core desacoplado de
Spring) e um módulo único full Spring Boot. O motor quer usar a
infraestrutura Spring livremente — em particular transações via
`TransactionSynchronizationManager`, base da cláusula transacional do
contrato assíncrono (ADR-0003). Um layout multi-módulo devolveria
disciplina de fronteiras "de graça" via build separado, mas exigiria
reimplementar transação/DI sem depender do Spring.

## Decision
Um artefato único: `io.mohs:mohs`. Módulo Maven único, full Spring Boot —
aposta estratégica registrada: Quarkus/Micronaut/standalone ficam fora do
escopo, e reabrir essa porta depois será caro.

O que substitui a disciplina que o multi-módulo dava de graça:

1. **Fronteira por pacote, guardada por ArchUnit:** `io.mohs` (API pública:
   annotations, `Mohs`, `JobRef`, specs, eventos) · `io.mohs.engine` e
   `io.mohs.jdbc` (internos, `@Internal`) · `io.mohs.autoconfigure` ·
   `io.mohs.rest` · `io.mohs.test`. Regras no build: interno não vaza para
   a API pública; `rest` só enxerga a API pública; `test` não vaza para
   produção.
2. **Web opcional:** `spring-web` como `<optional>`; REST/dashboard ativam
   via `@ConditionalOnClass` + `mohs.api.enabled` (padrão actuator). Teste
   de contrato garante que app sem web no classpath sobe.
3. **Test kit no jar** (`io.mohs.test`), `spring-test` opcional — nenhum
   segundo artefato para o usuário gerenciar.

## Consequences
Full Spring Boot fecha a porta pra quem não usa Spring — fatia real de
mercado (db-scheduler, JobRunr e Quartz servem esse público) fica fora do
alcance do Mohs. Essa é uma discordância registrada do líder técnico,
superada por decisão do PO (12/08/2026): decisão mantida, execução segue.

Em troca, o motor ganha acesso direto à infraestrutura transacional do
Spring sem reimplementá-la, o que torna o "transactional outbox nativo"
(cláusula 4 do contrato assíncrono, ADR-0003) infraestrutura pronta, não
artesanato. Web opcional e test kit no mesmo jar preservam parte da
modularidade de deployment sem pagar o custo de múltiplos artefatos Maven.

## Source
docs/MOHS-DOCUMENTO-MESTRE.md §4 (lines 157-181); docs/API-DESIGN.md
"Empacotamento — módulo único, full Spring Boot [DECIDIDO]" (lines
589-611)
