# ADR-0044: Reator multi-módulo e BOM

## Status
Decided — 2026-08-21 · substitui a decisão de módulo único da
[ADR-0001](0001-single-module-packaging.md)

## Context
A ADR-0001 escolheu um artefato único, `io.mohs:mohs`, e trocou a disciplina
de fronteira que o multi-módulo dá de graça por regras de ArchUnit sobre
pacotes. A troca funcionou como controle — as regras existem, pegam
violação e falham o build — mas ela só age *depois* que o código foi
escrito e compilado. Nada impede um `import io.mohs.engine.Dispatcher`
dentro de `io.mohs.core` na hora de digitar; o compilador aceita, e a
fronteira só reaparece na execução do teste.

O jar único também carrega para todo consumidor coisas que nem todo
consumidor quer: o kit de teste (`io.mohs.test`), os controllers REST, o
motor inteiro. `<optional>` resolve as *dependências* opcionais (o stack
web não é herdado), não as classes: elas viajam no jar de qualquer jeito.

## Decision
Nove módulos Maven sob o pai `io.mohs:mohs-parent`, um por fronteira que a
ADR-0001 já declarava em prosa:

| módulo | pacote | depende de |
|---|---|---|
| `mohs-cron` | `io.mohs.cron` | — |
| `mohs-core` | `io.mohs.core..` | spring-core |
| `mohs-engine` | `io.mohs.engine` | core, cron |
| `mohs-jdbc` | `io.mohs.jdbc..` + schemas DDL | engine |
| `mohs-rest` | `io.mohs.rest..` | core |
| `mohs-test` | `io.mohs.test` | engine |
| `mohs-spring-boot-starter` | `io.mohs.autoconfigure` | engine, jdbc, rest |
| `mohs-demo` | `io.mohs` (bootstrap), `io.mohs.demo` | starter |
| `mohs-bom` | — | — |

> **Nota (2026-08-21):** um décimo módulo, `mohs-ui`, entrou depois pela
> [ADR-0045](0045-dashboard-consumes-the-public-rest-api.md) — jar só com o
> bundle do dashboard. A tabela acima é o que esta decisão criou.

Três consequências de desenho decorrem disso:

1. **`mohs-core` é só contrato.** O motor saiu para `mohs-engine`, então a
   regra `internal_packages_do_not_leak_into_public_api` deixa de ser
   apenas um teste: `io.mohs.core` não tem `io.mohs.engine` no classpath de
   compilação. A regra de ArchUnit continua, agora como rede redundante e
   não como única guarda. Divergência consciente do Cadrix, que mantém o
   motor dentro de `cadrix-core`.
2. **O padrão actuator da ADR-0001 §2 sobrevive intacto.** `mohs-rest`
   declara `spring-boot-starter-webmvc` como `<optional>`; o starter depende
   de `mohs-rest` sem `<optional>` porque `MohsRestAutoConfiguration`
   referencia os controllers fora de qualquer condição de classe deles — o
   que a condição checa é `DispatcherServlet`, e é ele que é opcional. Que
   um consumidor sem stack web sobe em vez de estourar `NoClassDefFoundError`
   deixou de ser argumento e virou teste:
   `MohsRestAutoConfigurationTest.restApiStaysSilentWithoutDispatcherServletInsteadOfFailingTheBoot`
   roda o contexto com `FilteredClassLoader(DispatcherServlet.class)`.
3. **O pai não é `spring-boot-starter-parent`.** Este reator é um conjunto
   de bibliotecas: importa `spring-boot-dependencies` como BOM e configura
   compiler/surefire por conta própria. `mohs-demo` — o único módulo que é
   aplicação — declara o `repackage` sozinho.

A **`mohs-bom`** é o único módulo sem `<parent>`, e isso é o ponto dela: um
`<scope>import</scope>` resolve o modelo *efetivo*, então tudo que ela
herdasse viajaria junto — inclusive o import de `spring-boot-dependencies`
que o pai faz para se construir. Um aplicativo que importasse `mohs-bom`
antes da própria BOM do Boot teria as versões dele fixadas nas nossas, em
silêncio. O preço é a versão literal repetida no `../../../mohs-bom/pom.xml`.

## Consequences
O que se paga:

- Nove `../../../pom.xml` em vez de um, e uma versão a mais para manter sincronizada
  (a da BOM).
- **Ciclo de teste no reator.** Quatro testes de `io.mohs.engine` montavam o
  motor sobre stores JDBC e o kit de teste — dependência que fecharia ciclo
  `engine → jdbc → engine`. `EngineTest`, `DispatcherTest` e
  `ScheduleCommandImplTest` passam a morar em `mohs-jdbc` (são testes de
  motor-sobre-JDBC, e é lá que os dois lados existem); `MohsImplTest` vai
  para `mohs-test`, onde o kit em memória que ele usa é o próprio módulo.
  Nenhum teste foi enfraquecido, desabilitado ou removido.
- **`ArchitectureTest` desce para `mohs-demo`**, o único módulo com todos os
  outros num classpath só. A varredura de fonte da ADR-0043 (escrita de
  estado terminal) não podia ir junto: ela lê `src/main/java` do módulo em
  que roda, e todo o SQL que ela guarda está em `mohs-jdbc`. Virou
  `TerminalStateWriteScanTest`, naquele módulo.
- **Os `@WebMvcTest` de `mohs-rest` perderam a `@SpringBootConfiguration`
  que achavam subindo até `io.mohs.MohsApplication`.** Ganharam uma própria,
  `RestSliceConfiguration`, em escopo de teste — sem `@ComponentScan`, que é
  exatamente a exclusão de `io.mohs.rest..*` que a `MohsApplication`
  precisava declarar à mão.

O que se ganha, além da fronteira executável do item 1: um consumidor que só
quer declarar jobs pode depender de `mohs-core` e nada mais; o kit de teste
sai do jar de produção e vira `<scope>test</scope>` de quem o quiser; e a
porta que a ADR-0001 fechou explicitamente — Quarkus/Micronaut/standalone —
deixa de estar trancada pelo empacotamento. Ela continua fechada pelo
conteúdo: `mohs-engine` e `mohs-jdbc` usam a infraestrutura transacional do
Spring de propósito (ADR-0003 cláusula 4), e isso não muda aqui. O que muda
é que reabrir a discussão depois passou a ser barato.

A aposta estratégica da ADR-0001 — full Spring Boot — **permanece**. Só o
empacotamento em artefato único foi revogado.

## Pendências registradas
- `../MOHS-DOCUMENTO-MESTRE.md` §4 e `../API-DESIGN.md`
  ("Empacotamento — módulo único") ainda descrevem o layout antigo. São as
  fontes que a ADR-0001 citava; ficam desatualizadas até serem revisadas.
- Este reator não tem plugins de publicação (source/javadoc/gpg/Central),
  `Automatic-Module-Name`, checkstyle nem enforcer. O Cadrix tem, e são o
  próximo passo natural quando houver release — não entraram aqui para não
  misturar migração de layout com pipeline de publicação.
