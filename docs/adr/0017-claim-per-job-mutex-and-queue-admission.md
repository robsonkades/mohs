# ADR-0017: Mutex por job e admissão de queue no claim

## Status
Superseded by ADR-0018 — 2026-08-13. O mecanismo de mutex por job (lock
de `mohs_job_definitions` + `SKIP LOCKED`) descrito aqui se provou não
confiável sob concorrência real no H2 2.4.240 (código review + spike
empírico com JDBC cru, ~33% de violação numa única linha disputada, sem
join nenhum) — a ADR-0018 substitui o mecanismo por um CAS guardado, sem
depender de lock especializado. A admissão de queue (segunda metade
desta ADR) também foi removida, pela ADR-0021 — 2026-08-13 (`JobQueue`
removida por completo). Mantida por histórico — explica o raciocínio
original e por que ele não se sustentou.

## Context
O doc mestre nomeia exclusão mútua por job (`allowConcurrentExecutions`,
default `false`) e admissão de queue (ADR-0009, contador) como dois dos
quatro eixos de controle que o claim precisa aplicar — mas nenhuma ADR
descrevia *como*, dentro do `UPDATE` atômico da ADR-0016, essas duas
regras se sustentam sob concorrência real entre nós. As perguntas
concretas: (1) o que impede dois nós de reivindicarem, ao mesmo tempo,
duas execuções *diferentes* do *mesmo* job cujo `allowConcurrentExecutions`
é `false`? (2) o que impede dois nós de estourarem o `max_concurrent` de
uma queue reivindicando simultaneamente?

Sem resposta pra (1), um `NOT EXISTS (SELECT 1 FROM mohs_executions WHERE
job_key = ? AND state = 'RUNNING')` isolado no `WHERE` do claim é
insuficiente por dois motivos: entre nós, é uma corrida clássica TOCTOU —
duas transações concorrentes podem ler "nenhuma RUNNING ainda" antes de
qualquer uma commitar. E, menos óbvio, **dentro da mesma transação**: se
um job tem múltiplos siblings `ENQUEUED` devidos, uma única chamada de
claim pode selecionar vários deles como candidatos válidos (nenhum está
`RUNNING` ainda, todos passam o `NOT EXISTS`) e reivindicar todos no mesmo
lote — violando a exclusão mútua sozinha, sem precisar de um segundo nó.

## Decision

**Mutex por job**: o `SELECT` de candidatos do claim trava a linha de
`mohs_job_definitions` do job junto com a(s) linha(s) de
`mohs_executions` candidatas, no mesmo `FOR UPDATE OF e, j SKIP LOCKED`.
Um segundo nó tentando reivindicar outra execução do mesmo `job_key`
precisa da mesma linha de `j` — já travada, não commitada — e com `SKIP
LOCKED` simplesmente pula esse candidato neste ciclo (não bloqueia, não
espera; tenta de novo no próximo poll). Isso fecha a corrida entre nós.

Pra corrida **dentro da mesma transação** (múltiplos siblings do mesmo
job no mesmo lote), o `JdbcClaimer` mantém um `Set<jobKey>` em memória
enquanto percorre os candidatos em ordem: uma vez que um sibling de um
job com `allowConcurrentExecutions = false` é efetivamente admitido
(passou também a admissão de queue), qualquer sibling seguinte do mesmo
job neste lote é descartado — fica `ENQUEUED`, reaparece no próximo poll.

Custo aceito: jobs com `allowConcurrentExecutions = true` também travam
brevemente a linha de `j` durante o claim (não durante a execução) —
simplificação deliberada (trava sempre, sem `if` condicional no SQL); o
preço é serialização mínima na janela da transação de claim, não na
duração do job.

**Admissão de queue**: candidato a candidato, na mesma ordem de
prioridade/`scheduled_at` da consulta, o claim tenta
`QueueStore.tryIncrementRunning(name)` — um `UPDATE ... SET running_count
= running_count + 1 WHERE name = ? AND running_count < max_concurrent`
guardado, atômico, sem `SELECT` prévio. Se a queue está cheia, o
candidato fica de fora deste lote (sua trava de linha é liberada no
commit; ele reaparece no próximo poll). Isso acontece **dentro** da
mesma transação do claim — nunca um `SELECT`/decide em memória antes do
`UPDATE` final, que reabriria entre nós exatamente a classe de corrida
que o mutex por job resolve.

**Alternativas consideradas e rejeitadas:**
- *Advisory lock explícito* (`pg_advisory_xact_lock` ou equivalente) pro
  mutex por job: resolveria a corrida entre nós, mas é mecanismo à parte
  do resto do claim (que já usa `FOR UPDATE SKIP LOCKED` pra tudo),
  portabilidade pior entre bancos, e não fecha sozinho a corrida
  *dentro* da mesma transação (o `Set` em memória seria necessário de
  qualquer forma) — complexidade extra sem ganho aqui.
- *Coluna de versão + CAS otimista* pro mutex por job: exigiria retry em
  loop no chamador quando a versão mudasse sob concorrência — pior sob
  contenção real do que simplesmente pular o candidato e deixá-lo pro
  próximo poll (que é o que `SKIP LOCKED` já faz de graça).
- *Admissão de queue via `SELECT running_count` seguido de decisão em
  memória* (sem `UPDATE` guardado): reabre a corrida entre nós — dois
  nós podem ler o mesmo `running_count` antes de qualquer um escrever,
  ambos decidirem que há vaga, e estourar o `max_concurrent`. Rejeitado
  pela mesma razão que motivou o mutex por job.

**Verificação empírica**: H2 2.4.240 (versão resolvida via
`spring-boot-starter-parent` 4.1.0, usada nos testes) foi confirmado —
por um spike descartável, não parte da suíte — a suportar
`FOR UPDATE OF <tabela1, tabela2> ... SKIP LOCKED` num `JOIN` de duas
tabelas, e a efetivamente pular (não bloquear, não expor) uma linha
travada por outra conexão. A cobertura real desse comportamento vive nos
testes de `JdbcClaimerTest`, não no spike.

## Consequences
`JdbcClaimer` (`io.mohs.jdbc`) implementa `Claimer` (`io.mohs.engine`)
com o algoritmo: `SELECT` de candidatos com o duplo `FOR UPDATE OF`,
admissão de queue candidato a candidato via `QueueStore
.tryIncrementRunning`, dedupe de siblings em memória, `UPDATE` em lote
final pra `RUNNING` (ADR-0016) só nos que sobreviveram. `QueueStore`
ganhou `tryIncrementRunning(String name)` como parte desta ADR.

Efeito colateral pequeno, documentado aqui pra não virar ADR à parte: a
ordenação do claim (`ORDER BY priority DESC, scheduled_at ASC`) usa uma
expressão `CASE` sobre o nome do enum (a coluna é `VARCHAR`, não um
inteiro) — ordenação alfabética simples daria resultado errado
(`BACKGROUND` viria depois de `NORMAL`). `priority` nula (nada escreve
nela ainda — isso é a etapa 6) é tratada como `NORMAL` na ordenação.

Fica pra depois: renovação periódica da lease e reaper de execuções
órfãs (ADR-0012) — o `lease_expires_at` que o claim inicializa aqui só
importa quando esse mecanismo existir.

## Source
Doc mestre §3 (quatro eixos de controle), §5.8 (papéis runner vs. queue).
`docs/MOHS-DOCUMENTO-MESTRE.md` §9 (escopo de M3: "aquisição sem
contenção (claim)... enforcement de queue"). ADR-0016 (claim/RUNNING
atômico), ADR-0009 (enforcement de queue por contador, ainda Proposed).
