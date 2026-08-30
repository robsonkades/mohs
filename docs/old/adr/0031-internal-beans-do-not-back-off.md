# ADR-0031: Beans internos não recuam com `@ConditionalOnMissingBean`

## Status
Decided — 2026-08-15

## Context
Nenhum bean de `MohsAutoConfiguration` é condicional — o consumidor não consegue substituir
`Clock`, stores, executores nem o `Engine` declarando um bean próprio. Isso desvia do idioma
Spring Boot ("auto-configuration recua diante de bean do usuário"), e starters de pares
(Quartz, JobRunr) recuam em seus objetos centrais — então todo leitor formado em Boot espera
o recuo. O review 20260815 (item 5 de `../PENDENCIAS.md`, via o plano de refactor de
`io.mohs.autoconfigure`) perguntou se a ausência é deliberada ou omissão.

A superfície de extensão que já existe, e onde ela vive:

- **Vocabulário de domínio coletado como beans**: `MohsRunner`, `ExecutionWindow`,
  `ExecutionListener`, `ExecutionInterceptor` — o host contribui specs e observadores, nunca
  infraestrutura.
- **`ActorResolver`** (`MohsRestAutoConfiguration`): o único `@ConditionalOnMissingBean`,
  porque é SPI declarada (ADR-0010: a 1.x troca a atribuição por header por segurança real
  sem mudar contrato).
- **Propriedades validadas** onde configurar é legítimo: `mohs.jdbc.dialect` (ADR-0023),
  `mohs.time.mode` (ADR-0008) — falham no boot com mensagem que ensina.

## Decision
A ausência é deliberada e passa a ser regra: **bean interno de infraestrutura nunca recua**.
A fronteira do produto é a fachada `Mohs` + vocabulário `io.mohs.core`; stores, `Clock`,
executores, `Claimer`, `Dispatcher` e `Engine` são mecânica de correção, não pontos de
extensão. Três razões:

1. **Substituição anula garantia documentada em silêncio.** A correção do motor depende dos
   stores cooperarem em contratos transacionais precisos (claim+RUNNING atômico — ADR-0016;
   completion dono do decremento de vaga — ADR-0024/ADR-0027). Um `ExecutionStore` do host
   que não os honre quebra at-least-once sem nenhum erro. E runners são spec, nunca
   `Executor` do host — o Mohs é dono das threads (cancelamento, timeout, métricas).
2. **Back-off por tipo falha às 3h da manhã; propriedade falha no boot.**
   `@ConditionalOnMissingBean(Clock.class)` faria qualquer `Clock` inocente do host engolir
   o relógio do motor — a mesma degradação silenciosa que `defaultCandidate = false` impede
   na direção oposta (ver Javadoc de `MohsAutoConfiguration`).
3. **Nos pares que recuam, o objeto substituível é a fronteira do produto deles** (o
   `Scheduler` do Quartz, o `StorageProvider` do JobRunr). A nossa é a fachada e o
   vocabulário — e nesses o host já manda.

Demanda futura de customização (dialeto de banco não suportado, serializer de payload —
ADR-0029, tuning de executor) vira **SPI explícita caso a caso**: porta nomeada, contrato
documentado, testes — nunca back-off silencioso por tipo. Mesma régua já aplicada em
ADR-0011, ADR-0029 e ADR-0030.

## Consequences
- Quem quer comportamento diferente de persistência/tempo usa as propriedades nomeadas; o
  que não tem propriedade não é configurável — por decisão, não por esquecimento.
- Custo aceito: o host não consegue stubar internos via override de bean nos próprios
  testes; o caminho é o test kit (`io.mohs.test`).
- Custo aceito: desvio do idioma Boot — mitigado por esta ADR e pelo ponteiro no Javadoc de
  `MohsAutoConfiguration`, o lugar onde alguém adicionaria o `@ConditionalOnMissingBean`.
- `ActorResolver` permanece a exceção que confirma a regra: recua porque é SPI declarada,
  com contrato próprio (ADR-0010).

## Source
`../PENDENCIAS.md` item 5 (origem: plano de refactor de `io.mohs.autoconfigure`, removido
em 709d5b2); ADR-0008, ADR-0010, ADR-0016, ADR-0023, ADR-0024, ADR-0027, ADR-0029.
