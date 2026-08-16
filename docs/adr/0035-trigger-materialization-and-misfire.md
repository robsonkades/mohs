# ADR-0035: Materialização de disparos recorrentes e política de misfire

## Status
Decided — 2026-08-15.

## Context
`CronSpec`/`IntervalSpec` eram persistidas mas nenhum componente as disparava: execução só
nascia por `Mohs.schedule` (on-demand), `NextFireCalculator` existia apenas para exibir
`nextFireAt` no snapshot, e `Misfire` era vocabulário persistido sem consumidor — um job
`FIRE_ALL_MISSED` se comportava como `IGNORE` porque disparo nenhum acontecia. É o último
pilar do M3 ("claim, poll/dispatch, misfire, retry") sem implementação, e misfire não é
implementável isolado: é a política do laço de disparo que faltava. O documento mestre já
fixa o modelo (`WHERE next_fire_at <= now()`, §5.12 — a autoridade da decisão é o banco) e
a semântica (`misfire Ignore (default) / FireNow / FireAllMissed (replay com cap 1.440 por
ciclo, drenado, nunca descartado)`, §3).

## Decision

**Uma coluna `next_fire_at` em `mohs_job_definitions` é o estado do trigger** — a forma do
`NEXT_FIRE_TIME` do Quartz, não a do db-scheduler (linha única reagendada na conclusão):
com `allowConcurrentExecutions = true` por default (ADR-0019), a ocorrência seguinte de um
cron precisa disparar mesmo com a anterior ainda rodando — encadear materialização na
conclusão não expressa isso, e morre quando a ocorrência pendente é cancelada. `NULL` =
nada a disparar (on-demand; fixed-delay aguardando o fim da execução anterior — abaixo).

**A varredura de disparo roda no tick, antes do claim, só em `RUNNING`.**
`JobStore.findDueRecurring(now, limit)` lê jobs com `next_fire_at <= :now` (excluídos
`paused`/`orphaned`/`retired` — pause bloqueia exatamente o trigger, on-demand continua
valendo mesmo pausado, §3 do mestre), mais antigo primeiro, teto de 500 por tick (mesma
razão do `RECLAIM_LIMIT`: backpressure em toda borda; o excedente continua devido e drena
nos ticks seguintes). Ocorrência materializada é linha `ENQUEUED` comum — `scheduled_at` =
instante da ocorrência, `actor = "scheduler"`, prioridade `NORMAL`, payload vazio, sem
`idempotency_key` — e o claim do mesmo tick já pode reivindicá-la. Retry, mutex por job,
janelas e prioridade valem sem mudança nenhuma (ADR-0033: "ocorrência perdida reagendada é
só outra linha com `scheduled_at` devido").

**Exclusão mútua cluster-wide por CAS transacional, não por índice de idempotência.**
`TriggerFirer.fire` (porta engine, adapter `JdbcTriggerFirer`) executa numa transação:
`UPDATE ... SET next_fire_at = :novo WHERE job_key = :key AND next_fire_at = :observado
AND retired = FALSE` (o predicado de `retired` fecha a janela varredura→CAS contra um
`Mohs.remove` concorrente — ocorrência inserida depois do cancel-varredura do remove ficaria
zumbi); 0 linhas = outro nó venceu (ou o job aposentou), nada a inserir; 1 linha = insere
as ocorrências planejadas via
`ExecutionStore.insert` (participa da transação ativa — contrato da cláusula 4 da
ADR-0003). Avanço e inserção atômicos: crash entre os dois não perde nem duplica
ocorrência. Idempotency-Key ficaria sujeita à janela de retenção (ADR-0030) e gastaria o
índice único com chave sintética — o CAS resolve com o que já existe.

**`FiringPlan` decide a política — função pura, sem I/O.** Dado
(`schedule`, `misfire`, `next_fire_at`, `now`, threshold), produz os instantes a
materializar e o novo `next_fire_at`:

- **Perdida** = ocorrência mais velha que `mohs.engine.misfire-threshold` (default 60s —
  precedente Quartz). Ocorrência devida dentro do threshold **não** é misfire: dispara
  atrasada, em qualquer política — atraso de até um poll-interval é operação normal, não
  falha.
- **`IGNORE`** (default): descarta as perdidas, materializa as recentes, retoma na próxima
  ocorrência regular.
- **`FIRE_NOW`**: as perdidas viram **um** disparo imediato (`scheduled_at = now`); as
  recentes disparam normalmente.
- **`FIRE_ALL_MISSED`**: materializa todas as devidas, perdidas incluídas, com cap de
  1.440 por job por ciclo (§3 do mestre); capado, `next_fire_at` avança só até a última
  materializada — continua devido e drena nos próximos ticks, nunca descarta.
- O cap de 1.440 vale para **toda** materialização (não só replay): agenda patológica
  (intervalo de milissegundos) não transforma um tick em inserção sem teto.
- O salto sobre perdidas é O(1)/O(poucos passos), nunca O(perdidas): cron recalcula de
  `now - threshold`; fixed-rate salta por aritmética preservando a âncora da série.

**Fixed-delay (`afterFinish`) ancora no fim — `NULL` até a conclusão.** Na materialização
o `next_fire_at` vai a `NULL` (a próxima âncora é desconhecida até o fim); a conclusão
terminal rearma. O rearme viaja no `CompletionRequest` (`rearmNextFireAt`) e aterrissa na
mesma transação do CAS de conclusão (mesmo racional do `retryAt` da ADR-0033: escrita
separada entre CAS e crash se perde), guardado por `next_fire_at IS NULL` — nunca clobra
uma série que um upsert de mudança de agenda já rearmou. Quem calcula é quem tem a
definição em mãos: `Dispatcher` (fim real do attempt) e reaper (colunas de agenda no mesmo
SELECT que já lê `retries`/`retired`). `RETRY_SCHEDULED` não rearma — a corrente continua
pelo retry. Fugas da corrente e suas curas:

- Ocorrência cancelada ainda `ENQUEUED` (`cancelIfPending` não passa pelo caminho de
  conclusão): `MohsImpl.cancel` rearma (`now + interval`, mesmo guard `IS NULL`). Janela
  residual aceita: crash entre o cancel e o rearme deixa a corrente desarmada até a cura do
  upsert (boot/define) — transacionar vazaria a fronteira de storage pra fachada; mesma
  postura da janela residual da ADR-0033.
- Job aposentado/ressuscitado e coluna nova em base velha: o upsert cura `NULL` de agenda
  recorrente — para `afterFinish`, só sem execução viva do job (senão o rearme do boot
  criaria sobreposição que fixed-delay promete não ter).
- Definição removida entre claim e conclusão: sem cura imediata — job retired não dispara;
  a ressurreição via upsert cura (mesma janela residual documentada na ADR-0033).

**Upsert é o dono do estado inicial.** Agenda nova ou alterada → recalcula (`cron.next(now)`;
intervalo → `now + interval` — primeiro disparo de fixed-delay ancora na definição, não há
"fim anterior"); agenda inalterada → preserva; on-demand → `NULL`. Um SELECT prévio no
upsert decide — caminho de boot, frio. **Preservar é não escrever a coluna** (duas variantes
do UPDATE, precedente do par `COMPLETE_CAS`/`COMPLETE_CAS_FENCED`): reescrever o valor lido
seria lost update (DDIA cap. 7) contra o CAS do disparo (regrediria a série e re-dispararia
lote já materializado) e contra o rearme guardado da conclusão (mataria a corrente
fixed-delay em silêncio) — achado do review deste ciclo. As escritas que restam são
deliberadas: agenda alterada sobrescreve (reconfiguração explícita vence disparo
concorrente); a cura de `NULL` corre só contra o rearme, com valores quase idênticos
(trigger desarmado não é candidato do CAS de disparo); corrida upsert×upsert no boot é
benigna pelo mesmo argumento.

**O actor `"scheduler"` é nome reservado** (`Execution.SCHEDULER_ACTOR`). Ele deixou de ser
só trilha de auditoria: carrega decisão de motor (rearme fixed-delay e cura do upsert só
valem para ocorrência do scheduler) — e é como operador e API distinguem disparo do trigger
de agendamento manual. Por isso é rejeitado nas duas bordas de entrada: `ScheduleCommand.as`
(IAE que ensina, cobre o caminho programático) e `HeaderActorResolver` (400 na borda REST,
antes do IAE virar 500 no handler genérico) — um agendamento manual jamais pode se passar
pelo motor.

**`nextFireAt` do snapshot passa a ser o estado real.** `StoredJob` ganha `nextFireAt`;
`MohsImpl.toSnapshot` deixa de recalcular por cima do relógio (que mentia para
fixed-delay) e lê o armazenado. `NULL` em voo de fixed-delay é honesto: o próximo disparo
é desconhecido até a execução terminar.

**Fora do escopo, deliberadamente:**
- Validação de boot "job recorrente cujo handler declara parâmetro de payload" — a
  ocorrência materializada carrega payload vazio; handler tipado falha por attempt com a
  mensagem que ensina (`MohsJobs.adaptHandler`). Entra com as validações de boot (§5.13).
- Índice em `next_fire_at` — tabela de definições é pequena por natureza; entra como
  DBTUNE com número próprio se um harness mostrar que importa.
- SPI de triggers/misfire custom (§3 do mestre) — três usos reais antes de generalizar.
- `ExecutionWindow` segue decisão de claim, não de materialização — ocorrência fora da
  janela fica `ENQUEUED` até a janela abrir.

## Consequences
- Schema: coluna nova nos 4 dialetos — pré-GA é drop-and-recreate (PENDENCIAS item 10).
  Para jobs `ANNOTATION`, a cura de `NULL` no upsert do boot torna a coluna auto-inicializável
  sem recriar a base.
- Engine pausado/nó fora do ar não materializa; ao voltar, cada job responde pela própria
  política — downtime, pause de engine e pause de job convergem no mesmo mecanismo
  (`next_fire_at` no passado → `FiringPlan` decide).
- Job pausado **depois** de uma ocorrência materializada ainda executa essa ocorrência
  (pause bloqueia o trigger, não o claim — consistente com "sob demanda mesmo pausado").
- `EngineSettings`/`MohsProperties` ganham `misfire-threshold`; `Engine` ganha a porta
  `TriggerFirer` no construtor.
- Cron inválido só-em-runtime ("nunca dispara", `IllegalArgumentException` do
  `NextFireCalculator`) falha o plano do job no tick com log de erro — não derruba a
  varredura dos demais (mesma postura de `submitDispatch`).

## Source
`docs/MOHS-DOCUMENTO-MESTRE.md` §3 (semântica de misfire, cap 1.440, "sob demanda mesmo
pausado") e §5.12 (`WHERE next_fire_at <= now()`); ADR-0019 (concorrência default permite
sobreposição de ocorrências); ADR-0030 (por que não Idempotency-Key sintética); ADR-0033
(retry claimável — o caminho que as ocorrências herdam; `retryAt` no CAS de conclusão como
precedente do `rearmNextFireAt`); Quartz (`NEXT_FIRE_TIME`, misfire threshold) e
db-scheduler (forma rejeitada e por quê) como estado da arte.
