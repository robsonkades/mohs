# ADR-0029: Mapper de payload persistido isolado do `ObjectMapper` do host

## Status
Decided — 2026-08-15

## Context
`MohsAutoConfiguration.mohsExecutionStore` serializa o payload persistido com
`JsonMapper.builder().build()` — um Jackson cru, separado do `ObjectMapper` que o host
customiza — enquanto a REST v1 converte corpos de request com o mapper do contexto. O review
20260815 (pendência 2 de `../PENDENCIAS.md`) apontou que a intenção estava invisível: fácil
alguém "consertar" trocando pro mapper do host e quebrar a leitura de payloads já gravados.

O mapper do contexto é moldado pela superfície HTTP do app — naming strategy, inclusion
(`NON_NULL`), mixins, módulos, views. O payload persistido é formato durável compartilhado
entre nós: gravado no `schedule()` por um nó, lido no dispatch por outro — possivelmente outro
serviço embarcando o Mohs com outra config de host sobre o mesmo banco.

## Decision
O formato persistido de payload pertence ao Mohs, não ao host: `JdbcExecutionStore` usa um
`JsonMapper` cru próprio, isolado de qualquer customização do contexto Spring. Um "vamos usar
snake_case na nossa API" do host não pode mudar silenciosamente como payloads são gravados nem
tornar ilegíveis os já persistidos; dois nós com hosts configurados diferente precisam ler e
escrever o mesmo formato. É a mesma postura dos pares — db-scheduler (`Serializer` própria),
JobRunr (`JsonMapper` próprio), Spring Batch (`ExecutionContextSerializer`), Temporal
(`DataConverter`): nenhum deixa o mapper web do host definir formato de storage.

O round-trip é simétrico: o mesmo mapper cru grava e lê — a config do host não participa de
nenhum dos dois lados.

## Consequences
- Mudança na config web do host não altera o formato persistido nem quebra execuções
  pendentes; o formato independe de qual nó escreveu a linha.
- Payload que exija serializer customizado do host não funciona — e falha rápido, na
  serialização do `schedule()`, não silenciosamente na leitura (com Jackson 3, records e
  `java.time` já funcionam no mapper cru). Se aparecer demanda real, o caminho é uma SPI de
  serializer do Mohs (estilo `Serializer` do db-scheduler) — decisão futura com caso concreto,
  nunca o mapper do contexto.
- ADR-0011 continua valendo e é complementar: evolução de forma do payload entre deploys segue
  obrigação da aplicação; esta ADR fixa apenas *quem* define o formato de escrita.

## Source
`../PENDENCIAS.md` item 2 (origem: `codereview-20260815-0332.md`); ADR-0011;
`io.mohs.jdbc.JdbcExecutionStore` (round-trip em `insert`/`findPayload`).
