# ADR-0050 — Tiering de dialetos: Postgres é a referência, H2 sai de produção

Data: 2026-08-22 · Status: aceita · Fase: Phase 2 do redesign (declara o ADR-H do plano, §7.9)

## Contexto

Os 4 dialetos eram pares (ADR-0022/0023) — e a paridade cobrava: o SQL
Server sem `SKIP LOCKED` real pôs um retry de deadlock no caminho mais
quente do sistema; o `FOR UPDATE SKIP LOCKED` do H2 tem corrida real
medida (~33% de double-lock, Javadoc de `JdbcClaimer`) e moldou a
ADR-0018 inteira; e capacidades que removeriam subsistemas no Postgres
(particionamento declarativo, `LISTEN/NOTIFY`, `DELETE … RETURNING` como
pop) ficavam fora da mesa porque o par menos capaz não acompanha. O
próprio benchmark publicado do projeto roda em Postgres.

## Decisão

| Tier | Bancos | Compromisso |
|---|---|---|
| **1 — referência** | PostgreSQL 14+ | Feature set completo; todo trabalho de performance e o BASELINE rodam aqui; particionamento/`NOTIFY`/`RETURNING` liberados para as fases 5-6. |
| **2 — suportado** | SQL Server 2019+, MySQL 8.0+ | Corretos e testados na suíte inteira (Testcontainers); podem não ter retenção por partição (fallback: delete em lote) nem `NOTIFY` (só poll); performance medida, não co-otimizada. |
| **3 — teste** | H2 | **Não é backend de produção.** Vive para teste rápido e dev loop (demo). Boot com `mohs.jdbc.dialect=h2` loga WARN nominal. |

Nada é removido: `H2JdbcDialect`, o schema e as migrações H2 continuam
(os testes e o demo dependem deles de propósito). Muda o COMPROMISSO — o
que o projeto promete otimizar, e o que um usuário pode assumir.

## Consequências

- As fases 5-6 podem usar capacidades Tier 1 sem consultar o mínimo
  denominador; Tier 2 ganha fallbacks documentados por feature.
- A matriz de tuning encolhe (~-75% na conta do plano §0.2): trabalho de
  performance novo mira Postgres primeiro, Tier 2 recebe a forma correta
  e medição, não co-otimização.
- A corretude continua vindo do CAS guardado (ADR-0018) em todo dialeto —
  o tiering não relaxa correção, só o investimento de performance.
- Ressalva registrada (bancada da Phase 2): o Flyway community aplica gate
  pela janela de suporte do fornecedor — os mínimos prometidos (PG 14+,
  SQL Server 2019+) estão perto da borda (PG 14 EOL em nov/2026), e uma
  versão futura do Flyway via BOM pode passar a recusar. Mitigação já
  existente: `mohs.jdbc.migrate=false` (schema gerenciado por fora).
- Ressalva registrada (bancada de validação de release, 2026-08-23): o
  "testados na suíte inteira" do Tier 2 se apoia nos testes de store e de
  schema por dialeto (Testcontainers) mais o CAS guardado da ADR-0018 —
  **não** nos cenários de SISTEMA. Os nove `*Scenario` de
  `mohs-benchmark` (rate limit sob cluster, fechamento de lote, churn de
  nó, arranque a frio, migração concorrente, trigger recorrente,
  shutdown) montam N engines reais contra um Postgres e rodam **só em
  Tier 1**: nenhum deles jamais executou em SQL Server ou MySQL. O que
  isso significa em concreto: claim concorrente, reclaim de posse e
  fechamento de lote sob contenção real estão medidos apenas no dialeto
  de referência. **Gatilho para fechar a lacuna:** primeiro usuário de
  produção anunciado em Tier 2, ou primeira reclamação de corretude
  nesses bancos. O trabalho é generalizar o `ScenarioCluster`, hoje
  amarrado a `PostgresJdbcDialect` + `PostgresTestSupport`.
