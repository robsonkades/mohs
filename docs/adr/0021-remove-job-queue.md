# ADR-0021: Remoção de JobQueue/QueueStore

## Status
Decided — 2026-08-13

## Context
`JobQueue` (ADR-0004) modelava um cap de concorrência cluster-wide sobre
um recurso compartilhado **entre `job_key`s diferentes** (ex.: um relay
SMTP usado por três jobs de e-mail distintos) — papel distinto de
`allowConcurrentExecutions`/`maxConcurrentExecutions` (ADR-0018/0020),
que limitam a concorrência de um `job_key` consigo mesmo. Na prática,
esse eixo nunca saiu do papel de contrato: `QueuesController` era um
stub (`M3: ainda não implementado`), o enforcement de admissão
(ADR-0009) seguia `Proposed — gated on benchmark` desde que foi
proposto, e nenhum consumidor real do jar dependia dele.

Depois de fechar `maxConcurrentExecutions` (ADR-0020), a pergunta surgiu
naturalmente: `JobQueue`/`QueueStore` ainda fazem sentido? O argumento
técnico a favor de manter é real — recurso compartilhado entre jobs
diferentes é um caso genuíno que um cap por-job estruturalmente não
cobre (discordância registrada na conversa desta sessão). A decisão,
tomada pelo usuário, foi remover mesmo assim: os casos observados na
prática até agora são todos "um job, seu próprio teto" —
`maxConcurrentExecutions`/`allowConcurrentExecutions` resolvem isso sem
precisar manter um segundo eixo, uma tabela, um controller REST e um
campo (`queue`) em `JobDefinition` carregando um cenário hipotético
ainda sem uso real.

## Decision
Remoção completa, não deprecação: tipo público `JobQueue`
(`io.mohs.core.resource`), porta `QueueStore`/`StoredQueue`, adaptador
`JdbcQueueStore`, a integração de admissão de queue em `JdbcClaimer`, a
tabela `mohs_job_queues`, o campo `queue`/coluna `queue_name` em
`JobDefinition`/`PolicySpec`/`@MohsJob`/`mohs_job_definitions`, e o
controller REST `QueuesController` (`GET`/`PATCH /queues/{name}`) com
seus DTOs e a seção `queueDepths` de `GET /overview`.

**Alternativa considerada e rejeitada: manter como contrato "adormecido"
pra quando o caso real aparecer.** Rejeitada por YAGNI — o custo de
manter uma tabela, uma porta, um controller stub e um campo extra em
todo `JobDefinition` supera o custo de recriar o desenho quando (e se)
um caso real de recurso compartilhado entre jobs aparecer. O desenho
original (ADR-0004, ADR-0009, ADR-0017 metade queue) fica preservado no
histórico do git — reimplementar não é partir do zero.

## Consequences
Breaking change de API pública (`JobQueue` removida, `JobDefinition`
perde o componente `queue`, `PolicySpec.queue(String)`/
`@MohsJob.queue()` removidos) e de schema (`mohs_job_queues` dropada,
`queue_name` removida de `mohs_job_definitions`) — aceitável porque não
há consumidor externo do jar ainda (mesmo raciocínio já aplicado às
mudanças de shape desta sessão, ex. ADR-0018's `running_execution_id`).
`JdbcClaimer` simplifica: volta a depender só de `JobStore` (antes
dependia de `JobStore` **e** `QueueStore`), a query de candidatos perde
uma coluna, `tryClaimCandidate` perde o branch de admissão de queue e
seu desfazimento associado.

Se um caso real de recurso compartilhado entre `job_key`s aparecer no
futuro, o desenho a recriar é essencialmente o mesmo (nome + teto +
contador guardado, mesmo idioma de `JobStore.tryIncrementRunningExecutions`)
— não um problema novo, só uma tabela/porta a mais quando o benefício
for concreto, não hipotético.

## Source
Conversa desta sessão, decisão explícita do usuário após discordância
registrada ("acho que não precisamos mais disso" → confirmado: "pode
remover tudo referente ao Queue — Vamos manter apenas Job com
maxConcurrentExecutions e allowConcurrentExecutions. isso resolve 99%
dos casos"). Estende a linha de raciocínio da ADR-0020 (per-job cap) e
supersede a metade "admissão de queue" da ADR-0017, e a ADR-0009 por
completo.
