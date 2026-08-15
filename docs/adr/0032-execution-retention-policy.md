# ADR-0032: Política de retenção de execuções

## Status
Decided — 2026-08-15 (implementação: M3, ver §9 do documento mestre)

## Context
Linhas terminais de `mohs_executions` nunca saem, por desenho — o modo de degradação lenta
que aparece na semana 3 de produção, não no teste (DBTUNE-9, que já pedia esta ADR). A
ADR-0030 amarrou a janela da Idempotency-Key à retenção (a chave deduplica enquanto a linha
existir) e legou um requisito: janela ≥ ~24h.

Fatos do schema que moldam a decisão:

- `mohs_executions.id` é **UUIDv7, time-ordered de propósito** — idade equivale a faixa de
  PK; um corte por idade é range scan na própria chave primária, sem coluna nem índice novo.
- Não existe `finished_at` na execução (só em `mohs_attempts`).
- `mohs_attempts.execution_id` referencia a execução sem cascade; `mohs_executions.batch_id`
  referencia `mohs_batches` — attempts saem antes da execução, batch só depois das
  execuções dele.
- Estados terminais: `SUCCEEDED`, `FAILED`, `CANCELLED`.

## Decision

**Delete puro, sem arquivamento.** Padrão dos pares (JobRunr purga sucedidos após 36h;
db-scheduler remove ao completar; Temporal expira por retention e arquiva só via opt-in
externo). Arquivar dobraria schema e stores e apenas moveria o crescimento de lugar —
retenção longa é problema do banco do host (CDC, particionamento, backup), não do
componente.

**Janela única, configurável, ligada por default.** `mohs.retention.enabled=true`,
`mohs.retention.window=7d` — uma semana cobre o postmortem de segunda-feira sobre o fim de
semana, que 36h não cobre. Boot valida janela ≥ 24h com mensagem que ensina (piso herdado
da ADR-0030 — a promessa de dedupe do design REST). Crescimento infinito como default de
fábrica seria o pior silêncio; quem faz a própria retenção desliga com uma propriedade.
Sem janela por estado terminal (um knob só — KISS).

**Mecanismo: job interno do próprio Mohs** (dogfooding, o candidato do DBTUNE-9). Job
`PROGRAMMATIC` de key `mohs-retention`, registrado pela auto-configuração com
`allowConcurrentExecutions=false` e schedule de intervalo fixo (1h — não configurável até
demanda real): o claim dá exclusão mútua cluster-wide de graça (Competing Consumers — um nó
por vez, sem eleição de líder), a purga fica observável na própria história/REST, e colisão
de key com job do host falha no boot pela regra existente de conflito de identidade. As
execuções do próprio job de purga também expiram pela janela — o mecanismo se limpa.

**Corte por faixa de PK UUIDv7, guardado por estado terminal.** Candidatas:
`id < fronteira-uuidv7(agora − janela) AND state IN (SUCCEEDED, FAILED, CANCELLED)` — idade
conta do agendamento (mesma base temporal da janela de idempotência, ADR-0030); nada
pendente/rodando sai jamais, independente da idade (execução agendada pro futuro distante
não é terminal). Deletes em lote pequeno (attempts → executions na mesma transação por
lote; tamanho de lote abaixo do limiar de lock escalation do SQL Server, medido com o
harness antes de fixar). `mohs_batches` órfãos e mais velhos que a janela saem no mesmo
passo. Edge aceito: execução que termina muito depois de criada (retries longos) é medida
pela criação — em troca de zero coluna e zero índice novo na tabela mais quente.

## Alternativas rejeitadas
- **Tabela de arquivo** — dobra a superfície e não resolve o crescimento, só o move.
- **Tick de purga no reaper, por nó** — rodaria concorrente em todos os nós; deletes são
  idempotentes, mas é contenção desperdiçada na mesma faixa de PK, e sem a observabilidade
  que o job dá.
- **Coluna `finished_at` + índice parcial** — mede "idade desde o término" (mais precisa
  pro caso raro), mas adiciona coluna e índice ao hot path de escrita — o review de tuning
  só admite índice novo com medição, e o ganho é marginal.

## Consequences
- O horizonte de história do REST (`GET /executions`) é a janela de retenção — documentar
  no design REST quando a implementação entrar.
- A janela de idempotência (ADR-0030) passa a ser exatamente `mohs.retention.window`,
  com o piso de 24h garantido no boot.
- `GET /jobs` passa a exibir o job interno `mohs-retention` — custo aceito do dogfooding;
  é também a interface de operação da purga (pause/resume/history de graça).
- Implementação entra no M3 (documento mestre §9), com números de lote/intervalo validados
  pelo harness de explain antes de produção.

## Source
`docs/PENDENCIAS.md` item 1; `docs/codereview-tuning.md` DBTUNE-9; ADR-0030;
`schema-*.sql` (UUIDv7 na PK, FKs de attempts/batches).
