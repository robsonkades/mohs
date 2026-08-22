# ADR-0048 — Migrações de schema próprias, via Flyway com `mohs_schema_history`

Data: 2026-08-22 · Status: aceita · Fase: Phase 2 do redesign ("make schema change possible before making schema changes")

## Contexto

Até aqui o schema do Mohs era estático: `schema-*.sql` idempotentes por
dialeto, aplicados por quem consumisse (o demo via `spring.sql.init`, os
testes via `ResourceDatabasePopulator`, produção à mão). As fases 4-6 do
redesign mudam o schema de verdade (node lease, split de tabelas) — a
Phase 2 chegou a prever um ALTER (timestamptz), revisado pela ADR-0049
para a travessia; sem versionamento de schema, cada mudança das fases
seguintes viraria instrução de upgrade manual no release
note, o modo de falha que a ADR-0042 já experimentou ("o operador roda
DDL à mão no upgrade" passa despercebido por teste e por boot).

## Decisão

- **Flyway, instância e histórico PRÓPRIOS da biblioteca**
  (`mohs_schema_history`), nunca o Flyway do host: o Mohs é embarcado e
  compartilha banco/schema com a aplicação hospedeira, que pode ter a
  cadeia de migração dela — sequestrá-la ou colidir com o
  `flyway_schema_history` dela é o defeito clássico da lib embarcada.
  Localização fora do `db/migration` default
  (`classpath:io/mohs/store/jdbc/migration/<dialeto>`) para o Flyway do
  host também nunca varrer as nossas.
- **`V1__mohs_baseline` por dialeto = o schema corrente verbatim**,
  idempotente — é a migração de ADOÇÃO: instalação existente roda a V1
  como no-op e ganha o histórico. No MySQL a idempotência exigiu guardar
  os `CREATE INDEX` via `information_schema` + SQL dinâmico (não existe
  `CREATE INDEX IF NOT EXISTS` lá): sem a guarda, a adoção falhava com
  1061 no meio do script — DDL do MySQL comita implicitamente, a migração
  ficava `success=false` e todo boot seguinte morria na validação, loop
  permanente (achado da bancada da Phase 2, com reprodução; regressão em
  `MohsFlywayMySqlTest`/`MohsFlywaySqlServerTest`). `baselineOnMigrate(true)` + `baselineVersion(0)`: o Flyway
  exige baseline para migrar schema não-vazio sem histórico, e schema
  não-vazio é a REGRA aqui (as tabelas do host); baseline em 0 não pula
  migração nenhuma — o default (1) marcaria a V1 como aplicada num banco
  onde só existem tabelas do host e as do Mohs nunca nasceriam.
- **Roda no boot, na criação do bean `mohsFlyway`** — antes de qualquer
  escrita por construção (scanner de jobs e registrador de rate limits são
  `afterSingletonsInstantiated`; o engine é `SmartLifecycle`). Um knob:
  `mohs.jdbc.migrate=false` para quem gerencia schema por fora (DBA) — as
  migrações continuam no jar como fonte da verdade.
- **Dependências novas** (previstas pelo plano, Phase 2): `flyway-core` +
  os módulos por banco (`flyway-database-postgresql`, `flyway-mysql`,
  `flyway-sqlserver`), versões pelo BOM do Spring Boot. Não-opcionais em
  `mohs-jdbc`: quem tem o módulo de persistência tem o mecanismo de
  migração dele.
- **`schema-*.sql` continuam existindo** como snapshot canônico do schema
  CORRENTE (fast-path dos testes); as migrações são o delta histórico.
  Toda mudança de schema daqui em diante edita os dois — o teste de
  migração compara os resultados.

## Alternativas consideradas

- **Liquibase** — sem vantagem que pague divergir do que o ecossistema
  Spring Boot já gerencia por BOM; o formato SQL puro do Flyway é o que os
  schemas já são.
- **Migrador caseiro (tabela de versão + scripts)** — reimplementar lock
  de migração multi-nó, checksum e baseline é exatamente o wheel que o
  Flyway já é; NIH sem trade-off a favor.
- **Deixar como está (DDL manual)** — inviabiliza as fases 4-6; já mordeu
  na ADR-0042.

## Consequências

- Fases seguintes entregam schema como `V2__...` normal — o primeiro
  delta real chega com as tabelas do §7.2 (a ADR-0049 decidiu que a
  Phase 2 NÃO altera tipo de coluna).
- Boot concorrente multi-nó no primeiro deploy: coberto pelo lock de
  migração do próprio Flyway, por especificação — não exercitado na
  bancada (registrado; os cenários testados são fresh/adoção/re-execução
  sequenciais nos 4 dialetos).
- Boot em banco vazio cria o schema sozinho (o demo perdeu o
  `spring.sql.init`); multi-nó concorrente é seguro pelo lock do próprio
  Flyway.
- Adoção testada nos três cenários (banco novo, instalação existente
  pré-Flyway, re-execução) em H2 e Postgres (`MohsFlywayTest`/
  `MohsFlywayPostgresTest`).
