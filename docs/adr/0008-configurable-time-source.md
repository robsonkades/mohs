# ADR-0008: Fonte de tempo configurável

## Status
Decided — 2026-08-12

## Context
Um scheduler distribuído depende de "agora" para decidir o que disparar
— mas ler o relógio de cada JVM diretamente (`Instant.now()`,
`System.currentTimeMillis()`) espalha a autoridade de tempo por todos os
nós e torna clock skew entre nós um bug silencioso. Ao mesmo tempo, a
decisão de claim já acontece em SQL (`WHERE next_fire_at <= now()`), onde
a autoridade de tempo já é o banco.

## Decision
Todo "agora" do motor passa por um único relógio injetado
(`java.time.Clock`); leitura direta de `Instant.now()`/
`System.currentTimeMillis()` é proibida no código do Mohs, regra
verificada por ArchUnit. Três implementações da mesma costura:

- **application** (default): relógio da JVM em UTC — zero custo,
  pressupõe NTP saudável no cluster.
- **database**: o banco é a autoridade de tempo do cluster —
  implementado por amostragem de offset estilo NTP a cada
  `sync-interval`, medindo `SELECT now()` com compensação de
  ida-e-volta e aplicando `app + offset`; leituras de tempo são locais e
  O(1), **nunca** uma round-trip por leitura; clamp monotônico
  (reamostragem não anda para trás); banco indisponível na amostragem →
  mantém o último offset e avisa, leitura de tempo jamais bloqueia em
  I/O.
- **test** (`MutableClock`, em `mohs-test`): mesma costura que habilita
  `clock().advance(...)` no test kit.

Configuração: `mohs.time.source` (`application` default | `database`),
`mohs.time.sync-interval` (30s), `mohs.time.skew-warn-threshold` (500ms).

Em qualquer modo, o offset app × banco é amostrado e exposto como
métrica (`mohs.time.offset`), com WARN acima de `skew-warn-threshold`.

Disciplina de dois tempos: wall clock (o `Clock` acima) responde
"quando"; durações (timeout de execução, benchmark) usam tempo
monotônico (`System.nanoTime`) — duração nunca é subtração de wall
clock.

## Consequences
Clock skew entre nós deixa de ser silencioso mesmo no modo default,
porque a métrica de offset e o WARN existem independentemente de qual
fonte está ativa. O modo `database` alinha a aplicação à mesma
autoridade que já decide o claim em SQL, mas custa uma amostragem
periódica em vez de leitura de relógio local. A proibição ArchUnit de
`Instant.now()`/`currentTimeMillis()` no motor é o mecanismo que torna
essa disciplina obrigatória em vez de convencional — qualquer violação
quebra o build.

## Source
docs/API-DESIGN.md "Tempo — fonte configurável [DECIDIDO]" (lines
504-542); docs/MOHS-DOCUMENTO-MESTRE.md §5.12 (lines 430-448)
