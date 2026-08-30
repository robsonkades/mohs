# ADR-0061 — Fronteira de módulo executável, e por que o split de `io.mohs.engine` não veio junto

Data: 2026-08-29 · Status: aceita (parcial — ver "O que ficou de fora")

## Contexto

A revisão de codebase de 2026-08-29 achou a maior lacuna estrutural do projeto:
**a fronteira interno/público não era executável para quem consome o jar.**

- Não existia `module-info.java` em nenhum módulo.
- **29 dos 33 tipos** de `io.mohs.engine` são `public` — 18 de 21 em
  `io.mohs.store.jdbc`. Não por decisão de API: `mohs-store-jdbc` implementa as
  portas e o starter constrói o `Engine`, e a linguagem não tem "público para os
  meus outros módulos".
- Consequência: para um consumidor do `mohs-engine` publicado, `Engine`,
  `Dispatcher` e `CompletionBatcher` são API — a IDE dele autocompleta os três.

O ArchUnit guarda a fronteira *dentro* deste repositório
(`internal_packages_do_not_leak_into_public_api`, `rest_only_sees_public_api`).
Fora dele não havia nada. Num projeto cujo CLAUDE.md declara a API pública
imutável e cita o Item 15 (minimize accessibility), isso era o oposto do que a
casa diz praticar.

## Decisão

1. **`module-info.java` em `mohs-cron`, `mohs-api` e `mohs-engine`.** São os três
   que ganham fronteira real. O que importa está em `mohs-engine`:

   ```java
   exports io.mohs.engine to io.mohs.store.jdbc, io.mohs.autoconfigure, io.mohs.test;
   ```

   `mohs-cron` e `mohs-api` exportam tudo de propósito — o primeiro É uma
   biblioteca de parsing, o segundo É o contrato. O valor deles é declarar a
   dependência e fechar a lista.

2. **`Automatic-Module-Name` nos demais publicados** (`mohs-store-jdbc`,
   `mohs-test`, `mohs-rest`, `mohs-spring-boot-starter`), com o nome canônico
   `io.mohs.*`. Sem isso o nome derivaria do arquivo do jar, e o
   `exports ... to io.mohs.store.jdbc` acima furaria em silêncio.

3. **Duas dependências entram como automatic module** (`uuidv7`, `flyway.core`):
   nenhuma das duas declara `Automatic-Module-Name`, então o nome vem do arquivo.
   É frágil por natureza e está anotado nos dois `module-info` — se o artefato
   passar a declarar o nome, a linha muda junto.

4. **`LICENSE`, `NOTICE` e `<licenses>` no pom pai.** Não é formalidade:
   `io.mohs.cron` é obra derivada de `org.springframework.scheduling.support`, e
   a Apache 2.0 §4 obriga obra derivada que redistribui a carregar os avisos.
   Também é pré-requisito do Maven Central, que recusa artefato sem `<licenses>`.

## Consequências

- **A fronteira agora é declarada, e a declaração é verificável no build.**
  Quem consumir pelo *module path* fica de fato barrado de `io.mohs.engine`.
- **Limitação honesta: no classpath, `module-info` é ignorado.** Uma aplicação
  Spring Boot típica roda no classpath e continua enxergando todo `public`. O que
  a mudança entrega é a declaração formal, o sinal para IDE e ferramentas, e a
  barreira para quem usa module path — não uma trava universal. A trava universal
  exigiria o passo 2 abaixo, que não veio.

## O que ficou de fora, e o motivo exato

**O split de `io.mohs.engine` em subpacotes não foi feito**, embora estivesse no
mesmo escopo pedido. O motivo não é preferência: é um bloqueio concreto que a
tentativa expôs.

`io.mohs.engine` é o único módulo que ficou plano — 33 classes num pacote, contra
7 subpacotes de `io.mohs.core` e 9 de `io.mohs.rest`. O agrupamento natural já
está latente (portas · política pura · runtime · registries · fachada), e
subpacotes aqui não violam a regra 1:1 da ADR-0044, que mapeia pacote-RAIZ para
módulo.

O bloqueio: **cinco classes de teste do motor vivem em OUTROS módulos, no pacote
`io.mohs.engine`** — `EngineTest`, `DispatcherTest`, `CompletionBatcherTest` e
`ScheduleCommandImplTest` em `mohs-store-jdbc`, `MohsImplTest` em `mohs-test`.
Elas moram lá porque precisam de banco e o engine não pode ver JDBC (regra
ArchUnit `engine_is_free_of_jdbc`), e uma dependência de teste de `mohs-engine`
para `mohs-store-jdbc` fecharia ciclo no reator.

Isso produz dois efeitos:

1. **Split package.** Foi o que impediu `module-info` nesses dois módulos —
   `package exists in another module: io.mohs.engine`, verificado. Daí o
   `Automatic-Module-Name` da decisão 2.
2. **`package-private` no engine é, na prática, "público para o reator".** Três
   módulos alcançam esses internos. Mover `Engine` para
   `io.mohs.engine.runtime` tiraria o acesso dessas cinco classes a
   `cappedByNextFire`, `earliestArmedFire` e ao construtor de `EngineSettings` —
   e "compensar" alargando a visibilidade deixaria o código PIOR do que está,
   trocando organização por vazamento.

Ou seja: o split depende de resolver antes "o teste do motor precisa de banco e o
motor não pode ver banco". Os dois caminhos, ambos maiores que uma mudança de
pacote:

- **Módulo de teste dedicado** (`mohs-engine-it`, nunca publicado) que dependa de
  `mohs-engine` e `mohs-store-jdbc` e hospede as cinco classes. Resolve o ciclo e
  o split package, ao custo de um módulo novo no reator.
- **Seams públicos e explícitos** no lugar do acesso package-private, movendo os
  testes para caixa-preta. Mais limpo conceitualmente, mas apaga testes de função
  pura que hoje pegam defeito real (`EngineSleepTest` é o exemplo).

Fica como passo do PLAN.md, com a decisão de qual caminho seguir em aberto —
é escolha de arquitetura, não trabalho mecânico.

## Referências

`../../../mohs-cron/src/main/java/module-info.java`,
`../../../mohs-api/src/main/java/module-info.java`,
`../../../mohs-engine/src/main/java/module-info.java`, os `Automatic-Module-Name` nos poms
de `mohs-store-jdbc`/`mohs-test`/`mohs-rest`/`mohs-spring-boot-starter`,
`LICENSE`, `NOTICE`; ADR-0044 (o reator multi-módulo e a regra pacote↔módulo),
ADR-0015 (a API pública consolidada em `io.mohs.core`).
