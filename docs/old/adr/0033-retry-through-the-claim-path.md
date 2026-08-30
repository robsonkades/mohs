# ADR-0033: Retry pelo caminho do claim, com backoff exponencial e full jitter

## Status
Decided — 2026-08-15. Revoga a restrição da ADR-0026 (que se declarava temporária e nomeou
os dois pré-requisitos aqui entregues).

## Context
`retries`/`retryPolicy` eram persistidos mas sem efeito: `Dispatcher` e reaper sempre
terminavam em `FAILED` (ADR-0026), porque a claim query só reconhecia `ENQUEUED` e não
existia forma alguma de backoff. A garantia efetiva sob falha de nó era **at-most-once** —
declarada no Javadoc do `Engine`. A ADR-0026 condicionou o destravamento a duas coisas
juntas: claim query reconhecendo `RETRY_SCHEDULED` nos 4 dialetos e uma forma mínima de
decisão de backoff.

## Decision

**Retry viaja pelo mesmo caminho do claim.** `RETRY_SCHEDULED` entra ao lado de `ENQUEUED`
no template ANSI, no template próprio do SQL Server e no CAS final pra `RUNNING`. O backoff
é apenas o `scheduled_at` reescrito da mesma linha — due-time, prioridade, mutex por job,
janelas e `SKIP LOCKED` valem sem mudança nenhuma (a forma do db-scheduler: linha única
reagendada; não os trigger states do Quartz). Sem fila separada, sem prioridade especial de
retry: retry disputa com primeira execução como qualquer candidato devido.

**A hora do retry aterrissa junto do CAS de conclusão.** `CompletionRequest` ganhou
`retryAt`; a transição pra `RETRY_SCHEDULED` grava o novo `scheduled_at` no mesmo `UPDATE`
guardado — nunca numa escrita separada que poderia se perder entre o CAS e um crash. O
próprio record rejeita as duas combinações inválidas na construção (`RETRY_SCHEDULED` sem
hora; hora em estado terminal).

**Uma decisão só, compartilhada.** `RetrySchedule` decide orçamento e backoff para o
`Dispatcher` (falha de attempt) e para o reaper (reclaim de lease expirada) — dois caminhos
de falha com cópias próprias divergiriam na primeira mudança. O attempt sintético do
reclaim consome orçamento como qualquer outro: job cujo nó morre repetidamente ainda
termina após `retries + 1` tentativas.

**Backoff: exponencial com full jitter.** Delay uniforme em
`[0, min(1s × 2^(tentativa−1), 10min)]` — o documento mestre pede "exponencial com jitter",
e full jitter (estilo AWS) porque o caso das 3h da manhã é um recurso compartilhado caindo e
derrubando muitas execuções juntas; sem jitter todas voltariam em sincronia contra o recurso
ainda se recuperando. Constantes internas, sem propriedade de configuração até demanda real.

**Fora do orçamento, deliberadamente:**
- `failBeforeDispatch` continua terminal: definição removida não tem `retries` confiável a
  consultar, e payload ilegível não sara relendo os mesmos bytes.
- `retryPolicy` (bean customizado por job) segue SPI futura, não honrada — o boot avisa por
  job (`MohsEngineLifecycle`), mesma honestidade do fail-fast de `@OnExecution`.

**Eventos por caminho:** falha com orçamento publica `AttemptFailed` + `RetryScheduled`
(nunca `Failed`); orçamento esgotado publica `Failed(attemptsExhausted = true)`.

**Consequências estruturais de `RETRY_SCHEDULED` ser claimável** (achados do review desta
mesma rodada, aplicados juntos):

- Os índices parciais/filtrados do claim (Postgres/SQL Server) acompanham o predicado novo
  — `IN (E, R)` não implica `= E`, e sem o par o plano degradava pra Seq Scan + Sort da
  tabela inteira por tick (medido em Postgres 16). Re-medição de baseline pendente.
- `Mohs.remove` cancela os **dois** estados claimáveis; o reaper lê `j.retired` no mesmo
  SELECT e nunca reagenda job aposentado — sem isso, `RETRY_SCHEDULED` de job removido
  ficava preso pra sempre. Janela residual documentada: dispatcher commitando
  `RETRY_SCHEDULED` depois do remove (definição pré-remove em mãos) deixa a linha presa até
  um futuro `upsert` ressuscitar o job; fechar exige guarda `EXISTS(retired = FALSE)` no
  CAS de conclusão — adiada até haver caso real.
- **Fence anti-ABA no reclaim**: `RUNNING` deixou de ser não-reentrante, então
  `WHERE state = 'RUNNING'` não identifica mais *qual* encarnação — o reaper carrega a
  lease que observou expirada e o CAS só vence se ela ainda for a mesma (re-claim
  concorrente troca a lease e protege a encarnação nova). O caminho do dispatcher segue sem
  fence (conclui a encarnação que ele mesmo executou; o zumbi é bloqueado pela PK de
  attempts). Fencing token de verdade (`claim_epoch` incrementado no claim, carregado em
  todo CAS — DDIA cap. 8) fica pro trabalho do watchdog, com mini-ADR próprio.
- O CAS final do claim reverifica `scheduled_at <= now`: o retry tornou `scheduled_at`
  mutável, e toda condição de elegibilidade que muda entre SELECT e CAS pertence ao CAS
  (ADR-0018).

## Consequences
- ~~Sob falha de nó, a garantia com `retries > 0` é **at-least-once**; com o default
  `retries = 0` continua at-most-once~~ — **revisado em 2026-08-23 (ADR-0057): o
  default passou a ser `retries = 1`, então at-least-once vale POR DEFAULT. A frase
  original segue valendo para quem declarar `retries = 0` explicitamente.**
  Javadoc do `Engine` atualizado com a nuance.
  Reclaim prematuro de handler vivo (lease curta demais, sem watchdog ainda) consome
  orçamento de retry — mais uma razão pro WARN de lease × timeout do boot.
- `POST /executions/{id}/retry` (REST, stub) ganha o mecanismo por baixo quando for ligado.
- Retry não re-passa pela dedupe de Idempotency-Key (a linha é a mesma execução — nada novo
  é inserido), consistente com ADR-0030.
- Quando o enfileirador recorrente (cron/interval) e misfire forem desenhados, herdam este
  caminho pronto: ocorrência perdida reagendada é só outra linha com `scheduled_at` devido.

## Source
ADR-0026 (restrição revogada; nomeou os pré-requisitos); ADR-0012 (semântica original de
reclaim, agora executável); `../MOHS-DOCUMENTO-MESTRE.md` §5 ("retry fixo e exponencial
com jitter"); `io.mohs.engine.RetrySchedule`; `JdbcDialect.ANSI_SKIP_LOCKED_CANDIDATES`.
