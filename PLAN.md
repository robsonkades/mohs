# PLAN — Refactor de `io.mohs.autoconfigure` + `RunnerRegistry`

Objetivo: reduzir a lógica de negócio embutida na classe `@AutoConfiguration`,
alinhar `MohsProperties` às preferências do projeto (records, imutabilidade),
eliminar redundância e fechar as lacunas de ciclo de vida do `RunnerRegistry`
(Etapas 6–7) — **sem nenhuma mudança de comportamento observável** (mensagens
de erro incluídas), exceto onde marcado como hardening de caminho de falha.
Uma etapa por commit, suíte verde em cada uma (`./mvnw test`).

---

## Etapa 0 — Teste de contexto para `mohs.time.mode=database` (pré-requisito)

**Problema:** o caminho `database` do relógio (`mohsClockSyncScheduler` +
`DatabaseClock` + resync agendado) não tem nenhum teste via
`ApplicationContextRunner` — só o unitário `DatabaseClockTest`. A Etapa 3 mexe
exatamente nesse wiring, e a regra do projeto é "trecho sem teste → escreva o
teste primeiro".

**Mudança:** em `MohsAutoConfigurationTest`, dois testes novos:
- `mohs.time.mode=database` → contexto sobe, o bean `Clock` é `DatabaseClock`,
  o scheduler `mohs-clock-sync` existe.
- default (`application`) → `Clock` é o do sistema, **nenhum** bean
  `mohsClockSyncScheduler` no contexto.

**Arquivos:** `MohsAutoConfigurationTest.java`.

---

## Etapa 1 — `MohsProperties`: JavaBeans → records com constructor binding

**Problema:** 322 linhas, das quais ~200 são getters/setters. O projeto declara
"records para value objects e DTOs; imutabilidade por padrão", e propriedades
mutáveis pós-boot são um estado que ninguém deveria poder alterar. Boot 4 faz
constructor binding de records nativamente.

**Mudança:** `MohsProperties` e todos os nested (`Jdbc`, `Engine`, `Lifecycle`,
`Shutdown`, `Time`, `Registration`, `Runner`) viram records:
- Defaults via `@DefaultValue` (nested records precisam de `@DefaultValue` no
  parâmetro para instanciar vazio quando a seção inteira falta no config).
- `@Nullable Dialect` permanece — a obrigatoriedade continua checada em
  `mohsJdbcDialect`, porque a mensagem que ensina ("must be set … ADR-0023")
  é melhor do que qualquer validação genérica de binding.
- Enums, Javadoc e referências a ADR preservados componente a componente.

**Impacto:** ~322 → ~110 linhas. Call sites mudam de `getJdbc().getDialect()`
para `jdbc().dialect()` em `MohsAutoConfiguration` e `MohsJobScanner`.
`MohsJobScannerTest` constrói `MohsProperties` na mão com setter — passa a
construir o record.

**Por que primeiro:** é a etapa com maior churn de assinatura; tudo que vem
depois já nasce em cima da forma final.

**Trade-off:** nenhum funcional — relaxed binding e nomes de propriedade não
mudam. O custo é o diff grande num arquivo só, por isso etapa isolada.

---

## Etapa 2 — Extrair a montagem de runners para `MohsRunners` (sem Spring)

**Problema:** `mohsRunnerRegistry` + `toMohsRunner` + `requireUnset` +
`requireNoRunnerConflict` são ~85 linhas de lógica real (defaults built-in,
override, conflito de fonte, validação por modo) dentro da classe de
configuração — hoje só testáveis subindo o contexto inteiro com H2 e motor
real. Há ainda o smell dos dois mapas paralelos (`byName`/`sourceOf`)
carregando a mesma chave.

**Mudança:** classe package-private `MohsRunners` (mesmo padrão de `MohsJobs`:
vocabulário puro, estático, sem Spring), com um único
`LinkedHashMap<String, SourcedRunner>` onde `SourcedRunner(MohsRunner, String
source)` é um record local — um mapa, não dois. O `@Bean` vira ~3 linhas:

```java
@Bean(destroyMethod = "close")
public RunnerRegistry mohsRunnerRegistry(MohsProperties properties, List<MohsRunner> mohsRunnerBeans) {
    return new RunnerRegistry(MohsRunners.assemble(properties, mohsRunnerBeans));
}
```

**Comportamento preservado (verificar por teste, texto idêntico):**
- built-in `io`/`cpu` sempre presentes; built-in pode ser sobrescrito por
  propriedade ou `@Bean`; duplicata entre fontes não-built-in falha o boot;
- campo do modo errado falha nomeando a propriedade;
- IAE do builder embrulhada com o prefixo `mohs.runners.<nome>.*`.

**Testes:** os cenários de validação migram de `ApplicationContextRunner`
(boot completo + H2) para unitários diretos em `MohsRunnersTest` — rápidos e
apontando a causa. Ficam como integração apenas `propertyDefinedRunnerIsResolvable`
e `beanDeclaredRunnerIsCollected` (provam o fio de ponta a ponta).

---

## Etapa 3 — Wiring do relógio: condição única, sem `if` + `ObjectProvider`

**Problema:** o conhecimento "modo database" vive em dois lugares que podem
divergir: o `@ConditionalOnProperty` do `mohsClockSyncScheduler` e o
`if (mode != DATABASE)` dentro de `mohsClock` — que ainda paga o
`ObjectProvider` + `@Qualifier` para alcançar o scheduler condicional.

**Mudança:** duas nested `@Configuration` estáticas, condições mutuamente
exclusivas e exaustivas:
- `DatabaseTimeConfiguration` (`havingValue = "database"`): beans
  `mohsClockSyncScheduler` e `mohsClock` (constrói `DatabaseClock`, `sync()` no
  boot — bloqueio deliberado: o Engine não pode partir com relógio não
  sincronizado —, agenda o resync). Injeção direta do scheduler, sem
  `ObjectProvider` nem `Qualifier`.
- `SystemTimeConfiguration` (`havingValue = "application"`,
  `matchIfMissing = true`): `mohsClock` = `Clock.systemUTC()`.

**Rede de segurança:** os testes da Etapa 0.

**Consequência:** a condição tem fonte única; o scheduler só existe na fatia
que o usa; some um `@Nullable`-dance do caminho feliz.

---

## Etapa 4 — `MohsJobScanner`: mapa por identidade em vez de lista + varredura

**Problema:** `scanned` é `List` varrida linearmente a cada método anotado
(O(n²) no boot) e `reconcileOrphans` reconstrói um `HashSet` das chaves na mão.
A estrutura certa para "identidade → job" é um mapa que preserva ordem.

**Mudança:** `List<ScannedJob>` → `LinkedHashMap<JobKey, ScannedJob>`;
`putIfAbsent` detecta a duplicata (mensagem idêntica, o valor existente carrega
o `declaringMethod`); `reconcileOrphans` usa `scanned.keySet()` direto.

Incluir também guarda de concorrência mínima (sincronizar o acesso ao mapa),
pelo mesmo motivo que `ScheduledAnnotationBeanPostProcessor` guarda suas
coleções: com o bootstrap em background do Spring Framework 6.2+
(`bootstrapExecutor`), `postProcessAfterInitialization` pode rodar em threads
concorrentes na aplicação hospedeira — biblioteca embarcada não controla isso.
É a única parte da etapa que não é refactor puro; custo zero fora do boot.

---

## Etapa 5 — `MohsEngineLifecycle`: remover redundância com a interface

**Problema:** `stop(Runnable)` reproduz byte a byte o default de
`SmartLifecycle`; `getPhase()` retorna o literal `Integer.MAX_VALUE`, que é
exatamente `SmartLifecycle.DEFAULT_PHASE`.

**Mudança:** apagar `stop(Runnable)`. Manter `getPhase()` explícito — a fase é
garantia arquitetural documentada (ADR-0006: sobe por último, desce primeiro),
e garantia documentada não fica implícita em default de interface — mas
retornando `SmartLifecycle.DEFAULT_PHASE` em vez do número mágico.

---

## Etapa 6 — `RunnerRegistry`: protocolo de desligamento nasce junto com o executor

**Problema:** o conhecimento "como desligar cada executor" está duplicado e
separado de onde ele nasce. `build()` decide o tipo concreto
(`SimpleAsyncTaskExecutor` para IO, `ThreadPoolTaskExecutor` para CPU) e
`close()` **re-deriva** esse tipo com um switch por `instanceof` — inclusive
com um braço `default -> throw` para um estado que a própria classe torna
impossível por construção. Se um modo novo de runner surgir, `build()` e
`close()` precisam mudar juntos (Shotgun Surgery em miniatura), e esquecer o
`close()` só aparece em runtime, no shutdown.

**Mudança:** record interno
`LiveRunner(AsyncTaskExecutor executor, Runnable shutdown)`; `build()` devolve
o par já com a ação de desligamento certa (`io::close` / `cpu::destroy`),
`resolve()` lê `.executor()` e `close()` vira um loop que só executa
`shutdown.run()` — o switch, o braço impossível e os imports dos tipos
concretos saem do caminho de desligamento.

**Testes:** `RunnerRegistryTest` existente permanece verde sem alteração
(`closeShutsDownBothIoAndCpuExecutors` já cobre o comportamento externo).

---

## Etapa 7 — `RunnerRegistry`: falha no meio da construção/desligamento não vaza executor

**Problema (a promessa não cumprida):** o comentário do construtor diz que a
validação antecipada evita "pools já inicializados órfãos, sem ninguém pra
chamar close()/destroy()" — mas a pré-validação cobre só duplicata e default
ausente. O loop `specs.forEach((name, spec) -> built.put(name, build(spec)))`
roda **depois**, e qualquer exceção no meio (hoje improvável porque o spec já
foi validado por `MohsRunner`; amanhã, um modo novo ou falha de `initialize()`)
deixa órfãos exatamente os executores que o comentário afirma proteger. Código
que promete uma garantia estrutural tem que entregá-la estruturalmente.

**Problema (desligamento parcial):** `close()` para na primeira exceção — os
runners seguintes ficam vivos, e pool CPU usa platform threads não-daemon, que
seguram o shutdown da JVM.

**Mudança:**
- Construtor: `try/catch` em volta do loop de construção — em falha, fecha os
  `LiveRunner` já construídos (via o `shutdown` da Etapa 6) e relança a causa
  original intacta.
- `close()`: best-effort — `try/catch` por item, exceções acumuladas como
  suppressed numa única relançada no fim. Nenhum runner fica vivo porque o
  vizinho falhou ao morrer.

**Teste (seam mínimo):** esses caminhos de falha não são atingíveis com os
builders reais (o spec já chega válido). Construtor package-private extra
recebendo a fábrica `Function<MohsRunner, LiveRunner>` — usado só pelo teste
para injetar uma fábrica/shutdown que lança. Trade-off assumido: um seam de
teste de uma linha contra deixar exatamente os caminhos "das 3h da manhã" sem
teste; o seam ganha.

**Nota:** hardening de caminho de falha — comportamento no caminho feliz
idêntico; depende da Etapa 6.

---

## Fora do escopo — reportado, aguardando decisão

1. **Nenhum bean recua com `@ConditionalOnMissingBean`** — o consumidor não
   consegue substituir `Clock`, stores ou o `JsonMapper`. Pode ser deliberado
   (internos não são SPI), mas é uma decisão de superfície de API que merece
   mini-ADR, não um refactor silencioso.
2. **`JsonMapper.builder().build()` embutido em `mohsExecutionStore`** — a
   política de serialização de payload fica invisível. Extrair para bean
   próprio só quando o segundo consumidor (REST, M3) aparecer — YAGNI agora.
3. **Payload de tipo errado em `MohsJobs.adaptHandler`** estoura como
   `IllegalArgumentException` crua do reflection ("argument type mismatch"),
   sem dizer job nem método — melhorar muda o conteúdo de `Attempt.error()`
   (comportamento observável), então precisa de aprovação explícita.

## Considerado e rejeitado

- **Fábrica do dialeto no próprio enum `Dialect`** — acoplaria o tipo de
  propriedades a `io.mohs.jdbc.dialect`; o switch exaustivo no `@Bean` já é
  verificado pelo compilador. KISS.
- **Quebrar `MohsAutoConfiguration` em várias auto-configurations** — após as
  Etapas 2–3 o que sobra é wiring declarativo puro (~200 linhas); o custo de
  ordenar auto-configs entre si supera o ganho de navegação hoje.
- **`RunnerRegistry`: flag de "fechado" guardando `resolve()`** — a ordem já é
  garantida pelo Spring (o `SmartLifecycle` para o Engine em `phase MAX_VALUE`
  antes da destruição de singletons chamar o `destroyMethod = "close"`);
  estado defensivo para uma sequência que o container já sequencia é YAGNI.
- **`RunnerRegistry`: exceção própria no lugar de `NoSuchElementException`** —
  o único consumidor (`Engine.submitDispatch`) já captura e converte em falha
  terminal da execução; tipo novo seria superfície sem segundo uso.
