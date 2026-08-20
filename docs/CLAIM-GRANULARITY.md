# Granularidade do claim — núcleo único, por runner, ou por job

Documento de exploração, não decisão. Nasceu da pergunta: *"hoje o núcleo
gerencia todos os jobs de uma vez; e se cada job tivesse um núcleo
independente?"*

Não vira ADR enquanto o número que decide não existir — e ele está identificado
no fim, com o que precisa ser medido para obtê-lo.

---

## O que é verdade hoje

**Um claim, global, por tick.** `JdbcDialect.ANSI_SKIP_LOCKED_CANDIDATES`:

```sql
SELECT e.id, e.job_key, j.allow_concurrent_executions, j.window_name, j.rate_limit
FROM mohs_executions e JOIN mohs_job_definitions j ON j.job_key = e.job_key
WHERE e.state IN ('ENQUEUED', 'RETRY_SCHEDULED')
  AND e.scheduled_at <= :now
  AND j.retired = FALSE
  AND (j.allow_concurrent_executions = TRUE OR j.running_execution_count < j.max_concurrent_executions)
ORDER BY e.priority ASC, e.scheduled_at ASC
LIMIT :batchSize
FOR UPDATE OF e SKIP LOCKED
```

Uma query serve o sistema inteiro. Um índice parcial
(`idx_mohs_executions_claim (priority, scheduled_at) WHERE state IN (...)`)
serve essa query. O ponto de operação do `docs/performance/BASELINE.md` —
**4.0–4.2k exec/s** — é `poll=50ms`, `batch=1000`, `dispatch-concurrency=1024`,
Hikari 300, com rodadas encadeadas dentro do tick (ADR-0040).

**Isolamento já existente, por eixos separados do claim:**

| mecanismo | granularidade | o que limita |
|---|---|---|
| `maxConcurrentExecutions` (ADR-0018/0020) | por job | execuções simultâneas do mesmo job |
| `RateLimit` (ADR-0042) | por limite nomeado | taxa de disparo, cluster-wide |
| `ExecutionWindow` | por janela nomeada | quando pode rodar |
| `MohsRunner` + `RunnerRegistry` | por runner | **o pool que executa** |

`MohsRunner` já é uma especificação de pool completa — `mode` (IO/CPU),
`maxConcurrent` para I/O, `coreSize`/`maxSize`/`queueCapacity`/`keepAlive` para
CPU. O `RunnerRegistry` existe, é `AutoCloseable`, tem `resolve(runnerName)` e é
injetado no `Engine`. O que **não** existe é o claim saber de runner: o registry
decide onde a execução roda, depois de ela já ter sido reivindicada pela query
global.

---

## As três formas

### A — núcleo único (hoje)

Uma query, um lote grande, prioridade global real.

**Custa:** nenhum isolamento no claim. Um job com muitas execuções vencidas
ocupa o lote da rodada e atrasa os outros — o `SKIP LOCKED` evita bloqueio, não
evita monopólio de `LIMIT`.

### B — um claim por runner

Uma query por runner, cada uma filtrando `j.runner = :runner`.

**Custa:** a prioridade passa a valer **dentro** do runner. Um job urgente num
runner lotado não fura fila de um runner ocioso. É perda semântica real e é o
principal preço a decidir.

**Compra:** isolamento de verdade no claim, com custo O(runners). O lote grande
sobrevive dentro de cada runner.

**Exige:** índice de claim ganhando `runner` (ou `(runner, priority,
scheduled_at)` parcial), o que é uma quinta árvore na tabela mais quente — o
mesmo tipo de conta do item 7 de `docs/BATCH-ARCHITECTURE-REVIEW.md`.

### C — um núcleo por job

**Custa, em ordem de gravidade:**

1. **O custo escala com o número de jobs; o benefício, com o número de jobs
   problemáticos.** Denominador errado — é o defeito estrutural da ideia, não um
   detalhe de implementação.
2. **O lote morre.** `batch=1000` funciona porque uma rodada colhe mil linhas de
   jobs diferentes. Por job, quase toda rodada seria de 1 ou 2 linhas: troca-se
   um round trip por mil linhas por mil round trips de uma linha. Todo o ganho
   medido no BASELINE vem exatamente daí.
3. **A prioridade deixa de existir** como botão de operação — vira ordenação
   interna de cada job.
4. **Conexões.** Hikari 300 no ponto de operação, contra N núcleos querendo
   poll independente.

**Aritmética, não medição** (marcada como tal de propósito): a 20 ticks/s, com a
ordem de grandeza de 4k jobs que já usamos ao dimensionar o dashboard, são ~80k
queries/s por nó contra as 20 de hoje.

---

## Estado da arte

Ninguém isola por job. A granularidade é sempre pool/fila:

- **Temporal** — task queues, workers por fila.
- **Sidekiq / Celery** — filas nomeadas, workers por fila.
- **Kafka** — consumer groups.
- **Quartz / JobRunr** — uma thread de scheduler e um pool, como o nosso A.

Isso é evidência a favor de B, não de C: a indústria convergiu para isolamento
por pool porque o custo acompanha o número de pools, que é pequeno e declarado,
não o número de unidades de trabalho, que é grande e cresce sozinho.

---

## O número que decide

**Quantos runners um sistema real declara?**

- Com 3–5 runners, B é barato e provavelmente certo.
- Com centenas, B degenera no problema de C e a resposta vira A.

Não temos esse número, e ele não é medível no nosso banco — é característica de
uso. Fontes possíveis: quantos pools distintos os projetos deste workspace
declaram hoje em Spring (`spring.task.execution`, `@Async` com qualificador), e
quantas filas um Sidekiq/Celery típico tem em produção (a literatura fala de
unidades, não dezenas).

## O que medir, se B for adiante

1. **Custo do claim por runner contra o global**, mesmo backlog: 1 query com
   `LIMIT 1000` contra R queries com `LIMIT 1000/R`. Reusar
   `ClaimQueryLoadHarness`, variando R em {1, 3, 10, 30}. O que interessa é onde
   a vazão agregada deixa de acompanhar a de hoje.
2. **O imposto do índice** com `runner` na chave — contrafactual certo
   (substituir o índice de claim, não acrescentar um), pela lição já registrada
   na errata da ADR-0043.
3. **Fome entre runners**: um runner com backlog gigante e outro com uma
   execução urgente; medir a latência da urgente em A e em B. É o cenário que
   justifica B, então é o que precisa mostrar diferença.

## Minha recomendação, hoje

**Não fazer C.** Não é uma questão de esforço: o desenho troca a propriedade que
sustenta os 4k exec/s por um isolamento que os mecanismos existentes
(`maxConcurrentExecutions`, `RateLimit`, `ExecutionWindow`, runner) já dão por
eixos mais baratos.

**B fica em aberto**, e o caminho natural para chegar nele passa por terminar o
eixo de runner: `GET /runners` é o último stub de M3, e é o que dá visibilidade
de quantos runners existem e quão carregado cada um está — exatamente o dado que
falta para decidir.

**O que me faria mudar de ideia sobre C:** um caso real em que um único job
precise de isolamento que nenhum dos quatro mecanismos existentes consegue dar.
Não consegui construir esse caso; se aparecer, ele é o argumento, e aí a
pergunta certa provavelmente não é "núcleo por job" e sim "por que este job não
cabe num runner só dele".
