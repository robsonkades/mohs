# ADR-0043: Contador de lote mantido, incrementado com `RETURNING` na transação de conclusão

## Status
Decided — 2026-08-19

## Context

`Mohs.batch` (`MohsImpl:90`) e `GET /batches/{id}` (`BatchesController:17`) são os dois
últimos stubs de M3 — os únicos pontos do sistema em que a API pública ainda lança
`UnsupportedOperationException`. O que falta não é persistência: `mohs_batches` já existe
com `total`/`succeeded`/`failed`, o `JdbcBatchStore` já existe com incrementos atômicos,
`mohs_executions.batch_id` já registra a pertinência e já tem índice. Falta a fiação — e a
decisão que ela depende.

A pergunta é onde vivem os contadores de progresso do lote:

- **MANTIDO** — a linha de `mohs_batches`, somada a cada membro que termina. É o que o
  esqueleto faz hoje.
- **DERIVADO** — nada é guardado; os contadores são agregados sob demanda de
  `mohs_executions WHERE batch_id = ?`.

O derivado é atraente porque elimina, de saída, três coisas: a linha quente, a
possibilidade de o contador divergir do estado real, e a necessidade de uma chave de
idempotência que impeça dupla contagem. Não é hipótese: o predecessor deste projeto
(`cadrix`, hoje em `opentask`) precisou de todas as três — contador com lock otimista
versionado, retry em conflito, e uma tabela `batch_job_member` inteira só para deduplicar
recontagem (ADR-0001 deles). O cadrix não tinha escolha, porque a pertinência ao lote não
estava registrada em lugar nenhum consultável. Aqui está.

Além disso, o dia em que o lote fecha tem dono: `BatchCompleted` (ADR-0028) tem que ser
disparado **exatamente uma vez**, por exatamente um nó, num cluster onde vários concluem
membros do mesmo lote ao mesmo tempo.

## Decision

**Manter o contador**, e incrementá-lo com `UPDATE ... RETURNING` **dentro da mesma
transação** que faz o CAS de estado da execução.

```sql
UPDATE mohs_batches SET succeeded = succeeded + 1
 WHERE id = :batchId
RETURNING total, succeeded, failed
```

Um statement, um row lock, e o valor pós-incremento volta na mesma ida ao banco. O banco
serializa os incrementos concorrentes, e exatamente um chamador enxerga
`succeeded + failed == total`. **É esse que dispara `BatchCompleted`** — sem
compare-and-set, sem laço de retry, sem rodada descartada.

Consequência na porta interna: `BatchStore.incrementSucceeded`/`incrementFailed` deixam de
ser `void` e devolvem `BatchCounters`, que já existe e já tem `pending()`. Quem receber
`pending() == 0` fechou o lote. A mudança vive inteira em `io.mohs.engine`/`io.mohs.jdbc`:
`BatchCounters`, `BatchResponse`, `BatchState` e `BatchCompleted` ficam intactos, e
`BatchResponse.of(batchId, total, succeeded, failed)` já deriva `pending` e `state` sozinho.

## Por que — é corretude, não performance

**O desenho derivado não consegue detectar a conclusão do lote sob READ COMMITTED**, que é
o isolamento default de todos os dialetos suportados.

Sem contador guardado, saber que o lote acabou exige perguntar. A pergunta barata é uma
sonda de membro vivo (`... WHERE batch_id = ? AND state IN (ENQUEUED, RUNNING,
RETRY_SCHEDULED) LIMIT 1`), rodada por cada nó ao concluir seu membro. O modo de falha:

1. O lote tem 1000 membros; 998 já terminaram.
2. Nó A conclui o membro 999 e sonda. Seu snapshot é anterior ao commit de B, então A
   ainda vê o membro 1000 vivo → **"ainda não acabou"**.
3. Nó B conclui o membro 1000 e sonda. Seu snapshot é anterior ao commit de A, então B
   ainda vê o 999 vivo → **"ainda não acabou"**.
4. Os dois comitam. O lote está inteiro terminado e **`BatchCompleted` nunca dispara.**

Não é caso raro nem exótico: é exatamente a janela das últimas conclusões concorrentes, ou
seja, o único momento em que a sonda existe para responder. E falha em silêncio — nenhum
erro, nenhum log, só um lote que fica `RUNNING` para sempre e um callback que não veio.

Consertar isso exige `SELECT ... FOR UPDATE` sobre os membros, ou serialização externa —
isto é, exige de volta a linha quente e o ponto de contenção que o derivado prometia
eliminar, agora sobre N linhas em vez de uma.

O desenho mantido não tem essa janela. O row lock de `mohs_batches` serializa os
incrementos; `succeeded + 1` reavalia sobre a tupla mais nova; um só chamador vê o total.

**E a idempotência sai de graça pela transação**, o que dispensa a `batch_job_member` do
cadrix: o incremento compartilha a transação com o CAS de estado
(`WHERE id = :id AND state = 'RUNNING'`, ADR-0024). Se o CAS não pegou a linha, não há
incremento. Se pegou e a transação aborta, o incremento volta junto. Não há a dupla escrita
não atômica que gerou a ADR-0001 deles, e portanto não há membro encalhado nem recontagem
a deduplicar. Isso é atomicidade sob guarda de CAS, não "idempotência" genérica — a
distinção importa, porque é a guarda que faz o trabalho.

## O que foi medido

Harnesses `BatchCounterDesignHarness` (ef887cd) e `BatchCounterWriteCostHarness` (e50f230),
Postgres 16-alpine via Testcontainers, i7-13700K / 24 threads / Temurin 25.0.4 — mesma
máquina do BASELINE, e **não** hardware de produção.

**Custo por conclusão** (lote de 1000, dentro da transação, p50 em ms, mediana de 5
repetições):

| nós | derivado (sonda) | cego | `RETURNING` | read-decide-write |
|---|---|---|---|---|
| 1 | 0,719 | 0,655 | 0,681 | 0,862 |
| 2 | 0,738 | 0,724 | 0,705 | 1,297 |
| 4 | 0,828 | 0,965 | 0,999 | 2,616 |
| 8 | 1,363 | 1,429 | **1,308** | **5,621** |

Duas leituras, e só a segunda é conclusão:

- Derivado, cego e `RETURNING` ficam dentro de um round trip mais fsync de commit uns dos
  outros. Isso **não** é "custam o mesmo": os três são estruturalmente idênticos (um
  statement, uma ida ao banco), então o arranjo não consegue separá-los. A afirmação
  honesta é que a diferença entre eles é menor que o piso do transporte — aqui e, a
  fortiori, numa rede real.
- **Read-decide-write é o único que degrada com o número de nós**, e com dispersão apertada
  ([5,554-5,793] a 8 nós). O desenho do cadrix está descartado com medição própria, no
  escopo transacional certo. Ressalva: o braço medido retenta em laço; a forma forte
  (`SELECT ... FOR UPDATE`, sem retry) não foi medida, e perderia do `RETURNING` por uma
  ida a mais, não por contenção.

**Imposto do índice: não existe, e a primeira medição estava errada.** O desenho derivado
exigiria `(batch_id, state)`, sem o qual sua sonda vira `Seq Scan` da tabela inteira (39ms
contra 0,24ms, medido). A primeira rodada atribuiu a esse índice um custo de 6-7% em toda
escrita de `mohs_executions` — errado por comparar o schema atual contra o schema atual
**mais** um índice. O derivado não acrescenta uma árvore: ele **alarga** `idx_mohs_executions_batch_id`,
do qual `(batch_id, state)` é prefixo. Medido no contrafactual certo (5 índices contra 5,
um deles mais largo), o alargamento é gratuito — −2,5%, intervalo dentro do baseline.

Fica registrado porque a tentação de reusar o número é real: **custo de índice não foi
argumento nesta decisão, em nenhuma direção.**

## Consequences

- `BatchStore.incrementSucceeded`/`incrementFailed`: `void` → `BatchCounters`. Porta
  interna; a superfície pública não muda.
- `mohs_batches` fica como está. Nenhuma migração de schema, o que evita o item 10 de
  `docs/PENDENCIAS.md` (os `CREATE TABLE IF NOT EXISTS` não ganham coluna nova).
- Sem tabela de idempotência, sem lock otimista, sem varredura de reconciliação — três
  peças que o cadrix carrega e que aqui não nascem.
- **Divergência de dialeto**, absorvida pelo `JdbcDialect` (ADR-0023), que já tem o padrão
  de constante ANSI com override: `RETURNING` no Postgres; `OUTPUT INSERTED.*` no SQL
  Server; no MySQL não há nenhum dos dois, e o equivalente é `SELECT` subsequente **na
  mesma transação** — seguro, porque o row lock do `UPDATE` é mantido até o commit, então a
  releitura é estável. **H2 não foi verificado** e é o primeiro passo da implementação: se
  não suportar `RETURNING`, cai no mesmo caminho do MySQL.
- O lote continua sem varredura de reconciliação. Se uma transação de conclusão for perdida
  de forma que o CAS de estado também se perca, a execução volta pelo reaper e o par
  estado+contador se reconstrói junto. Não há caminho em que um avance sem o outro.

## O que esta ADR não estabelece

- A transação modelada nos harnesses ainda não é a do motor: falta a guarda
  `AND state = 'RUNNING'` e o `INSERT INTO mohs_attempts`. A propriedade de
  exatamente-uma-vez está argumentada, **não demonstrada por medição** — demonstrá-la é
  tarefa da implementação, com teste de concorrência determinístico.
- O incremento entra em `completeWithinTransaction`. Se também entrar em
  `completeAllWithinTransaction` (DBTUNE-14), `SET succeeded = succeeded + :n` colapsa N
  incrementos num statement e um lock, e isso deve ser medido antes.
- O tamanho de lote alvo foi fixado em 1.000 membros. Acima disso o desenho não muda (a
  leitura do mantido é seek de PK, plana no tamanho), mas os números acima não foram
  medidos na cauda.
- Os eixos rodaram só no Postgres.

## Source

`BatchCounterDesignHarness` (ef887cd), `BatchCounterWriteCostHarness` (e50f230) e as duas
revisões que os corrigiram — a primeira apontou que `RETURNING` faltava no espaço de busca,
a segunda que o imposto de índice media o contrafactual errado. `io.mohs.core.Batch`,
`BatchBuilder`, `io.mohs.engine.BatchStore`/`BatchCounters`, `io.mohs.rest.batch`.
ADR-0003 (cláusula 4, dual-write), ADR-0009 (derivar em vez de guardar, quando dá),
ADR-0018 (UPDATE guardado em vez de lock especializado), ADR-0023 (dialetos),
ADR-0024 (CAS de estado), ADR-0028 (`BatchCompleted`), ADR-0042 (a decisão simétrica para
rate limit, onde derivar era impossível). Comparação com `cadrix`
(`opentask/docs/adr/0001-batch-job-outcome-idempotency.md`).
