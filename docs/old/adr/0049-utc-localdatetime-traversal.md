# ADR-0049 — Travessia temporal por `LocalDateTime` UTC (JDBC 4.2), nunca `java.sql.Timestamp`

Data: 2026-08-22 · Status: aceita · Fase: Phase 2 do redesign

## Contexto

Todas as colunas temporais guardam wall-clock UTC em tipos sem fuso
(`TIMESTAMP` H2/Postgres, `DATETIME(6)` MySQL, `DATETIME2` SQL Server —
DBTUNE-1). A travessia era `java.sql.Timestamp`, que converte pelo fuso
default da JVM nas duas pontas; o offset constante se cancela, mas o
**gap de horário de verão não**: `Timestamp.valueOf` resolve um
`LocalDateTime` inexistente empurrando-o 1h pra frente — bug latente
reportado em 2026-08-19 (em `refilled_at`, vira balde 1h "velho" = cheio:
burst acima do limite, o modo de falha que a ADR-0042 impede; em
`scheduled_at`/lease, pior). A Phase 2 do plano manda remover o defeito
por construção ("timestamptz semantics").

## Decisão

- **Travessia por `LocalDateTime` via JDBC 4.2** (`setObject`/`getObject`)
  em todos os binds e leituras (`JdbcTimestamps.toUtcLocalDateTime`/
  `fromUtcLocalDateTime`): `LocalDateTime` não consulta fuso nenhum — o
  wall-clock UTC atravessa verbatim nos 4 dialetos, e as duas funções são
  inversas em TODO instante, gap incluso. Regressão fixada por
  `JdbcTimestampsTest` (unit + round trip JDBC real com o default da JVM
  em zona com DST, instante dentro do gap).
- **Os tipos de coluna NÃO mudam nesta fase** — desvio deliberado da letra
  do plano ("migrate all four schemas to timestamptz"), pelo gate revisado:
  1. A letra era impossível como escrita: `TIMESTAMP` do MySQL termina em
     2038 — inaceitável para `next_fire_at` de um scheduler — então a
     uniformidade tz-aware nunca existiria nos 4 dialetos.
  2. O objetivo declarado da migração era o defeito de DST, e ele morre na
     travessia, não no tipo: coluna tz-aware com travessia errada continua
     errada; coluna zoneless-UTC com travessia `LocalDateTime` é correta
     por construção.
  3. `ALTER TYPE` nas 12 colunas temporais custaria rebuild dos índices
     mais quentes do sistema (claim, reaper) em 3 dialetos, por um ganho
     que se resume a ergonomia de query ad-hoc no psql.
  4. As tabelas novas da Phase 5 (§7.2) **nascem** `TIMESTAMPTZ` no Tier 1
     — o tz-aware de verdade chega com as tabelas que o exigem, como
     migração V2+ sobre a infra da ADR-0048.
- `DatabaseClock` continua lendo `Timestamp` do `CURRENT_TIMESTAMP` —
  comportamento pré-existente, fora do escopo desta fase. Precisão por
  dialeto (review da Phase 2): tz-aware de verdade em Postgres/H2; em
  MySQL depende da config do driver (session tz); em **SQL Server é
  `DATETIME` zoneless** interpretado pelo fuso da JVM — offset errado com
  fusos distintos, e uma amostra colhida no gap de DST sai +1h e o clamp
  monotônico a perpetua. Correção esboçada (pendente de aprovação — muda
  comportamento): ler o wall-clock UTC por dialeto (`SYSUTCDATETIME()`,
  `UTC_TIMESTAMP(6)`, `now() AT TIME ZONE 'utc'`) como `LocalDateTime`,
  a mesma travessia desta ADR.

## Consequências

- O bug de DST (memória/registro 2026-08-19) morre; a validação de boot da
  precisão de colunas (achado relacionado) continua não existindo — segue
  aberto, candidato à Phase 4+.
- Nenhuma migração de dados; V2 de tipo não existe. Rollback = reverter o
  jar.
- Operadores seguem vendo wall-clock UTC em queries ad-hoc (status quo).
