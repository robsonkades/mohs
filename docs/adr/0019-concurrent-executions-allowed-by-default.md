# ADR-0019: Execuções concorrentes do mesmo job são permitidas por padrão

## Status
Decided — 2026-08-13

## Context
`allowConcurrentExecutions` (adicionado na etapa 3a) nasceu com default
`false` — exclusão mútua por `job_key` como comportamento padrão de todo
job. Revisando esse default à luz de um caso concreto: um job
`send-email` (um `job_key` só) invocado via `Mohs.schedule(ref, payload)`
uma vez por destinatário — `joao@` e `maria@` são duas `Execution`s
diferentes do mesmo `job_key`, com payloads independentes, sem relação
nenhuma entre si além de compartilhar o handler.

Com o default `false`, essas duas execuções **serializam** — a segunda
fica bloqueada atrás da primeira (e, sem liberação de lease ainda
implementada — CONC-1 — bloqueada para sempre). Esse não é um caso de
borda: é o padrão mais comum pra jobs disparados via `Mohs.schedule`, que
por desenho carregam um payload por invocação (`docs/adr/
0002-definition-vs-invocation.md`). `job_key` identifica um *tipo* de
trabalho (o handler, a agenda, a política) — não uma fila de um item só.

O cenário que genuinamente motiva exclusão mútua é mais estreito: um job
cron/interval cujo próprio disparo seguinte pode ocorrer antes do
anterior terminar (ex.: sincronização que às vezes demora mais que o
intervalo configurado). Aí as duas "execuções" são a mesma tarefa se
sobrepondo consigo mesma, não trabalho independente — e nesse caso
específico, rodar duas cópias ao mesmo tempo pode genuinamente corromper
dado (ex.: dois processos escrevendo no mesmo destino).

Prior art: o Quartz tem exatamente essa distinção — `@DisallowConcurrentExecution`
é uma anotação **opt-in**, não o default. A maioria dos jobs no ecossistema
Quartz permite concorrência; você marca explicitamente os poucos jobs que
não podem se sobrepor.

## Decision
`allowConcurrentExecutions` passa a ter default `true`. O método builder
que antes ligava concorrência (`PolicySpec.allowConcurrentExecutions()`,
no-arg, setava `true`) deixa de fazer sentido — foi substituído por
`PolicySpec.preventOverlap()` (no-arg, seta `false`), o novo opt-in pro
caso estreito de exclusividade. `@MohsJob.allowConcurrentExecutions()`
segue com o mesmo nome de atributo (ainda descreve corretamente "este job
permite execuções concorrentes"), só o default vira `true`.
`schema.sql` acompanha (`allow_concurrent_executions BOOLEAN NOT NULL
DEFAULT TRUE`), embora `JdbcJobStore.upsert` sempre grave o valor
explícito vindo da definição — o default SQL só importa pra alguma
inserção manual fora do caminho normal.

**Alternativa considerada e rejeitada: manter `false`, só documentar
melhor.** Resolveria o problema pra quem lê a Javadoc antes de usar, mas
não pra quem não lê — o comportamento errado (serialização silenciosa de
trabalho independente) continuaria sendo o que acontece por padrão pro
caso mais comum, exigindo que every job on-demand se lembre de chamar
`allowConcurrentExecutions()` pra funcionar como esperado. Um default que
exige opt-in pra o caso comum é o tipo de armadilha que este projeto
tenta evitar em outros lugares (ex.: `JobQueue.maxConcurrent` default 1,
não ilimitado — mas ali o raciocínio é o oposto: falhar cedo é mais
seguro que vazar concorrência; aqui, falhar cedo seria vazar
serialização indevida, sem sinal nenhum de que algo está errado).

## Consequences
Nenhuma mudança no mecanismo de claim (ADR-0016, ADR-0017, ADR-0018) —
o CAS em `running_execution_id` continua sendo a garantia de corretude
pra jobs com `allowConcurrentExecutions = false`; só o valor default que
chega até ele muda. Testes que dependiam do default antigo pra verificar
mutex (`JdbcClaimerTest`) foram atualizados pra optar explicitamente via
`preventOverlap()`; os que dependiam de concorrência (a maioria dos
testes de prioridade/batch size) simplificaram, já que não precisam mais
chamar nada.

Como o campo foi adicionado nesta mesma sessão, sem nenhum consumidor
fora deste repositório, o custo da inversão é zero em termos de migração
— só o trabalho de atualizar os call sites já existentes aqui dentro.

## Source
Conversa desta sessão, motivada por um exemplo concreto (job de e-mail
com um destinatário por execução). `docs/adr/0002-definition-vs-invocation.md`
(payload por invocação). Quartz `@DisallowConcurrentExecution` como
prior art de default oposto.
