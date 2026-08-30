# ADR-0062 — O nome do lote vira dado durável (e o que o NVARCHAR cobrou junto)

Data: 2026-08-29 · Status: aceita · Complementa a ADR-0043 (contadores de lote)

## Contexto

`Mohs.batch(String name, Consumer<BatchBuilder>)` exigia um nome, fazia
`Objects.requireNonNull` nele e **o descartava**. Ele não ia para o
`BatchStore.insert(batchId, total)`, não aparecia em `BatchSnapshot` (quatro
componentes: `batchId/total/succeeded/failed`), não aparecia em `BatchCompleted`
— o dado entregue a `Batch#onCompletion` — e não existia em
`GET /batches/{id}`.

O caso concreto: o usuário escreve `mohs.batch("nightly-invoices", …)`, um job
falha às 3h, ele abre o dashboard e encontra `batchId = 0198f2c1-…`. O nome que
ele deu justamente para não precisar decorar um UUID não existe em lugar nenhum,
e ele vai procurar por onde consultar por nome. Não havia.

Parâmetro em API pública é promessa. `requireNonNull` num argumento descartado é
teatro de validação — e `Mohs` é fachada publicada, ou seja, o erro seria
permanente.

## Decisão

1. **O nome é persistido**: coluna `name NOT NULL` em `mohs_batches` nos quatro
   dialetos, `BatchStore.insert(batchId, name, total)`, e componente novo em
   `BatchCounters`, `BatchSnapshot`, `BatchCompleted` e `BatchResponse`.
   `BatchCompleted` carrega o nome porque um callback de fim de lote que não sabe
   QUAL lote terminou é meio callback: quem registra `onCompletion` em mais de um
   lote casava UUIDs à mão.

2. **A assinatura de `Mohs.batch` não muda** — a correção é source e binary
   compatible do lado da fachada. O que quebra é o construtor canônico dos
   records (e os padrões de desconstrução), e é por isso que a decisão foi tomada
   AGORA: antes do release custa zero, depois é major. É o mesmo custo que o
   Javadoc de `RunnerSnapshot` documenta ao recusar um componente `queued`.

3. **Backfill dá o próprio id ao lote antigo** (`V6__batch_name.sql`, guarda
   idempotente na forma da V2). Lote que já existe não tem nome a recuperar, e
   preencher antes do `NOT NULL` mantém a migração segura numa base com dados.

4. **A alternativa recusada foi remover o parâmetro.** Ela também era honesta —
   melhor que mantê-lo mentindo —, e perderia a única ligação entre o lote e a
   intenção de quem o criou. Persistir custou uma coluna numa tabela que tem uma
   linha por LOTE, não por execução.

## A consequência que veio de carona: a chave clusterizada do SQL Server

O mesmo lote trocou `VARCHAR` por `NVARCHAR` em todo o DDL do SQL Server
(restaurando o invariante DB-5 — `VARCHAR` não é Unicode, e o
`SqlServerUnicodeScanTest` agora guarda isso em toda build). Medido: o parâmetro
`nvarchar` do driver contra coluna `varchar` converte **a coluna**, e o claim
perde o seek — `1538 → 3 logical reads` numa tabela de 200k linhas.

O dobro de bytes empurrou **uma** chave para fora de um limite:
`mohs_idempotency (job_key, idempotency_key)` passa a medir
`2 × (255 × 2) = 1020` bytes, contra o teto de **900** do índice CLUSTERIZADO (o
não-clusterizado subiu para 1700 no SQL Server 2016+, o clusterizado não).
Medido: `225+225` caracteres entra, `256+255` falha no INSERT do enqueue com
`Msg 1946 … exceeds the maximum length of 900 bytes for clustered indexes`.

`idempotency_key` vem de header do cliente e o schema declara aceitar 255 — um
corte em ~450 somados seria armadilha. **Decisão:** a PK deixa de ser a chave
clusterizada (`V8__idempotency_clustered_key.sql`), e quem clusteriza é
`created_at` — monotônico (`Clock` injetado), então o INSERT segue na cauda e a
poda por retenção vira range delete na própria clusterizada, o que torna
`idx_mohs_idempotency_created` da V7 redundante NESTE dialeto.

**O que esta troca cobra, e ainda não tem número.** Ela não é gratuita, e a
versão anterior deste texto dizia "não é neutro, é melhor" contabilizando só o
lado bom — o que viola a regra da casa (sem before/after, não é otimização).
Os dois custos, ambos no enqueue idempotente:

1. o INSERT passa a manter **duas** estruturas (clusterizada em `created_at` +
   PK não-clusterizada) onde antes mantinha uma;
2. `SELECT execution_id … WHERE job_key = ? AND idempotency_key = ?`
   (`JdbcHistoryStore`) deixa de ser seek na clusterizada e vira **seek + Key
   Lookup** — `execution_id` não está na chave nova e não entra por `INCLUDE`
   numa constraint de PK;
3. a monotonicidade de `created_at` é o argumento a favor E o custo: ela mantém o
   INSERT na cauda, e é exatamente isso que concentra todos os nós na MESMA página
   final — `PAGELATCH_EX` de cauda no enqueue concorrente, mais o uniquifier de
   uma clusterizada não única. A PK antiga espalhava.

A troca é obrigatória de qualquer forma (o limite de 900 bytes é duro), então o
que falta medir não é SE fazer, e sim se a PK deve virar um índice único
COBRINDO (`INCLUDE (execution_id)`), que mataria o Key Lookup. **Gatilho:**
`logical reads` do INSERT e do SELECT de dedupe, antes e depois, no SQL Server.

Alternativas recusadas: estreitar a coluna para `NVARCHAR(200)` divergiria dos
outros três dialetos e truncaria em silêncio; deixar a tabela heap resolveria o
limite, mas o DELETE de retenção num heap não desaloca páginas sem `TABLOCK` —
e esta é justamente a tabela que mais poda.

## Consequências

- `GET /batches/{id}` e `BatchSnapshot` passam a responder "qual lote é este" sem
  o operador precisar cruzar UUID com o código que o criou.
- **Quebra binária** em `BatchSnapshot`, `BatchCompleted`, `BatchCounters` e
  `BatchResponse` — construtor canônico e desconstrução. Aceita por ser
  pré-release.
- `MohsImplTest` afirma que o nome chega ao store
  (`verify(batchStore).insert(batch.batchId(), "nightly", 2)`) — era o teste que
  já existia e passava sem ele.
- O schema do SQL Server ficou com um shape físico diferente dos outros três em
  `mohs_idempotency` (clusterizada por `created_at`). É divergência de dialeto
  registrada, não acidente — e o `SqlServerUnicodeScanTest` impede a volta do
  `VARCHAR` que a originou.

## Referências

`Mohs#batch`, `BatchSnapshot`, `BatchCompleted`, `BatchCounters`, `BatchStore`,
`JdbcBatchStore`, `BatchResponse`; `V6__batch_name.sql` e
`V8__idempotency_clustered_key.sql` (SQL Server), `SqlServerUnicodeScanTest`;
ADR-0043 (os contadores de lote e a regra de quem conta), ADR-0030 (a dedupe por
`Idempotency-Key`), `../codereview.md` DB-5.
