# ADR-0038: Estereótipos `@RecurringJob`/`@OnDemandJob` e o contrato de payload por papel

## Status
Decided — 2026-08-15. Aditiva sobre o vocabulário M1 (`@MohsJob` permanece); depende da
ADR-0035 (ocorrência automática carrega payload vazio) e entrega a validação de boot
prevista no §5.13 e adiada na ADR-0035.

## Context
Um job do Mohs vive um de dois papéis com contratos genuinamente diferentes: o
**automático** (agenda declarada, dispara sozinho, ocorrência sem payload — não existe
fonte de dado num disparo do motor) e o **sob demanda** (invocado via
`Mohs.schedule`/API, payload tipado fornecido na invocação, cada invocação avulsa).
`@MohsJob` expressa os dois com os mesmos atributos, o que permite o híbrido sem sentido —
job com cron cujo handler exige payload tipado — que hoje só falha em runtime, por
tentativa. Pedido do autor: separar os papéis na declaração, no espírito dos estereótipos
do Spring (`@Component` × `@Service`). Nomes: alinhados ao vocabulário que o core já tem
(`OnDemandSpec`), não à nomenclatura coloquial ("job agendado" seria ambíguo — *scheduled*
em inglês é justamente o automático).

## Decision

**Dois estereótipos como META-anotações de `@MohsJob` — o padrão `@Service`/`@Component`
literal.** Cada estereótipo é declarado `@MohsJob(id = "")` com todos os atributos em
`@AliasFor(annotation = MohsJob.class)`; o scanner resolve por merged annotations
(`AnnotatedElementUtils.findMergedAnnotation`), então continua procurando **uma** anotação
e enxerga através dos estereótipos — uma única tradução, nenhuma mecânica própria.
`@MohsJob` ganhou `ANNOTATION_TYPE` no target (mesmo desenho do `@Scheduled` do Spring) —
de brinde, o consumidor pode compor os próprios estereótipos sobre os nossos.

- `@RecurringJob` — exige exatamente um gatilho (`cron`+`zone` | `every` |
  `everyAfterFinish`; nenhum → erro de boot que ensina a usar `@OnDemandJob` — a checagem
  é do scanner: a meta-anotação não expressa "pelo menos um"). Carrega as políticas de
  trigger (`misfire`, `startPaused`) e as gerais.
- `@OnDemandJob` — não expõe atributos de agenda, `misfire` (não há disparo a perder) nem
  `startPaused` (pause não afeta invocação manual): ficam fixos nos defaults da
  meta-anotação. Só as políticas gerais.
- Ambos têm o par `value`/`id` em alias (forma concisa `@OnDemandJob("import-file")`) —
  custo aceito: o id dos estereótipos é obrigatório em *boot* (em branco falha com erro
  que ensina), não em compilação como na forma geral, cujo `id()` sem default permanece.
  `value` e `id` com valores divergentes falham pelo fail-fast do próprio Spring
  (`AnnotationConfigurationException`, "mirror values", nomeando anotação e método —
  verificado empiricamente no review; sem tradução Mohs e sem teste próprio: bytecode com
  mirror inválido sob `io.mohs.**` envenenaria o component scan de todo teste de
  contexto).
- Mais de uma forma no mesmo método — direta OU via estereótipo composto — é erro de boot,
  contado no grafo de merged annotations: colisão de identidade, mesma família do
  "duplicate id". (Achado do review: contar só formas diretas deixava "composto + direto"
  resolver em silêncio pela ordem de declaração no fonte — não pela forma direta, como se
  presumiria; verificado empiricamente.)
- `@MohsJob` continua válido como forma geral de baixo nível (o `@Component` da analogia)
  — compatibilidade e o caso raro que os estereótipos não expressem.

**O contrato de payload vira validação de boot (§5.13), por definição — não por
anotação.** Job cuja agenda é recorrente (qualquer anotação, ou programático via scan)
com handler que declara parâmetro de payload **incapaz de receber o payload vazio do
trigger** (não atribuível de `LinkedHashMap`) derruba o boot com erro que ensina.
Parâmetro `Map`/`Object` é permitido de propósito: ocorrência automática entrega mapa
vazio e uma invocação manual avulsa do mesmo job pode entregar dados — o padrão
"parâmetro opcional" é legítimo. A validação olha a `JobDefinition` + `payloadType`
adaptado, então cobre `@MohsJob` com cron igualmente — o estereótipo torna o contrato
legível; a validação o torna executável.

**Semântica que os estereótipos NÃO mudam:** `@RecurringJob` continua aceitando invocação
manual avulsa (reprocessamento operacional — "roda agora a sincronização"); a anotação
nomeia o papel primário, não uma exclusividade. `@OnDemandJob` é açúcar exato para
`@MohsJob` sem gatilho.

**Alternativas rejeitadas:**
- Fragmentar por tipo de gatilho (`@CronJob`/`@EveryJob`...): o precedente relevante é o
  próprio Spring `@Scheduled` (uma anotação, três gatilhos) — fragmentação sem diferença
  de contrato.
- Substituir `@MohsJob` pelos estereótipos: quebraria vocabulário M1 congelado sem ganho —
  aditivo primeiro, depreciação só com dado de uso real.
- `@RecurringJob` proibir invocação manual: removeria capacidade documentada e útil
  (documento mestre §3, "execução manual sob demanda").

## Consequences
- Custo aceito e registrado: atributos redeclarados (como aliases) nas três anotações —
  toda política nova toca até três arquivos de anotação (hoje `startPaused` tocou um). É o
  preço da legibilidade por papel; mitigado por a tradução e a mecânica de scan
  convergirem num único caminho (`@MohsJob` mesclada), e o `@AliasFor` é validado pelo
  Spring em runtime (alias para atributo inexistente falha alto).
- A validação de payload muda comportamento de boot: um `@MohsJob(cron=...)` com handler
  de payload tipado que hoje sobe (e falha por tentativa) passa a falhar o boot — pré-GA,
  sem usuários externos; é a promessa fail-fast do §5.13.
- Scanner passa a rejeitar métodos com múltiplas anotações de job — colisão de
  identidade, mesma família do "duplicate id".
- Registro programático manual (`HandlerRegistry.register` direto, test kit) segue sem a
  validação — ela pertence ao scan de boot; a nota fica no Javadoc da validação.

## Source
ADR-0035 (payload vazio da ocorrência; §5.13 adiado), ADR-0037 (`startPaused` — atributo
que só o estereótipo recorrente carrega), documento mestre §5.3/§5.13; Spring
(`@Component`/`@Service` como padrão de estereótipo; `@Scheduled` como contra-exemplo de
fragmentação por gatilho); conversa de design de 2026-08-15.
