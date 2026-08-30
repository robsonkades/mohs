# ADR-0052 — O split da tabela de execuções: fila, posse e história

Data: 2026-08-22 · Status: aceita · Fase: Phase 5 do redesign ("the table split", ADR-A do plano; commits 97d781a → 3a3bdd4)

> **Nota (2026-08-23):** o item 3 da Decisão foi revisado pela **ADR-0058** —
> `mohs_execution`/`mohs_attempt` NÃO são mais particionadas por semana no
> Postgres (são planas em todos os dialetos), e a retenção futura deixou de
> ser DROP de partição. O resto da decisão — o split em quatro tabelas por
> perfil de escrita, o `state` advisory, o fence — segue em vigor.

## Contexto

`mohs_executions` era uma tabela só com três perfis de escrita em guerra:
o backlog claimável (linhas nascendo e mudando de estado por segundo), a
posse em voo (a linha mais quente do sistema) e a história terminal (que
só cresce e um dia é retida). O custo era medido, não teórico: ~3,9
versões de tupla por execução na Phase 0 (~9 projetadas no cenário
renewal-heavy antes da Phase 4), dois updates non-HOT por execução
batendo em todos os índices, e retenção por DELETE em lote — o pior
padrão de vacuum possível na tabela que menos pode parar. Os índices
parciais (DBTUNE-5/17/22) já tinham extraído tudo o que implicação de
predicado compra. O experimento E1 (BASELINE §E1) validou o split num
replay de 500k execuções: 1,79× fim a fim, tuple versions 4,00 → 2,00
exato, WAL −31,6% a −36,4% na faixa de payload inline — com o gate
revisado e aprovado em 2026-08-22.

## Decisão

Quatro tabelas, quatro perfis de escrita (§7.2 do plano):

1. **`mohs_ready`** — a fila: só INSERT (enqueue/retry/requeue) e DELETE
   (claim/cancel/dreno de aposentadoria). `visible_at` é a única regra de
   visibilidade (§4.3): `RETRY_SCHEDULED` morreu como estado persistido —
   retry é uma linha de fila com `visible_at` futuro (`RETRY_WAITING` é
   estado DERIVADO na leitura). Fillfactor 70 + autovacuum agressivo no
   PG: a tabela vive pequena por construção (só o backlog).
2. **`mohs_lease`** — a posse: INSERT no claim, DELETE na conclusão, com
   o fence `(node_id, epoch)` (§6.3 — o sucessor do
   `(node_id, fired_at)` da ADR-0051). Carrega `attempt_number` e
   `priority` para que requeue/reaper reconstruam a entrada de fila sem
   ler história. Deletar a lease É liberar a vaga (ver ADR-0053).
3. **`mohs_execution` + `mohs_attempt`** — a história: INSERT + UM update
   advisory terminal na primeira; INSERT-only na segunda. Particionadas
   por semana no Postgres (Tier 1; `created_at`/`finished_at` como chave,
   poda do UPDATE terminal por IGUALDADE de `created_at`), tabelas planas
   nos equivalentes funcionais Tier 2/3 (ADR-0050). Retenção futura =
   DROP de partição (Phase 8). O `state` da história é ADVISORY: fica
   `'PENDING'` até o desfecho terminal; `RUNNING`/`ENQUEUED`/
   `RETRY_WAITING` são derivados por LEFT JOIN com posse e fila no read
   model.
4. **`mohs_idempotency`** — o dedup do Idempotent Receiver vira conflito
   de PK próprio, componível com a transação do host via savepoint
   (`PROPAGATION_NESTED` — review S5.3).

As três transações nomeadas do §7.5: **enqueue** (história + fila,
REQUIRED/NESTED — junta-se à transação do host, ADR-0003 §4), **claim**
(statement único CTE no Postgres: picked com `SKIP LOCKED` → DELETE da
fila → INSERT da posse → SELECT final ordenado; forma portátil de três
statements nos demais; REQUIRES_NEW + READ COMMITTED) e **conclusão**
(DELETE cercado da posse + INSERT do attempt + UPDATE advisory podado +
renascimento do retry na fila + contagem de lote ADR-0043 + rearme
ADR-0035 — TUDO numa transação; ver ADR-0047 para o group commit que a
transporta).

Cada preocupação virou porta própria do engine (§18.3): `WorkQueue`,
`LeaseStore`, `HistoryStore`, `StoreTransactions`.

## Alternativas consideradas

- **Mais índices parciais na tabela única** — esgotado; não ataca versões
  de tupla nem retenção.
- **Particionar a tabela única por `state`** — Postgres não particiona
  por coluna que muda sem row move, e o row move É o custo a remover.
- **Fila fora do banco (Redis/Kafka)** — quebra o outbox transacional
  (§14.2/ADR-0003), a promessa central da biblioteca.
- **Dual-write/shadow-read por uma release** (a mitigação do plano) —
  dispensada com registro: pré-GA não há dado vivo a proteger; o gate foi
  suíte completa + E6 re-rodado sobre o split (PLAN.md, decisão 1).

## Consequências

- **Gates medidos (BASELINE "Phase 5")**: S1 = 13,9k exec/s mediana
  quente (2,4× a Phase 3, 3,3× a era da tabela única); tuple
  versions/execução = **2,000 exato** (§3.3); S5 — vazão de pico com
  2,05M de história retida igual/melhor que com 0,45M (o claim não
  referencia a história por construção). Commits 0,042–0,054/exec; WAL
  ~2,2KB (payload trivial).
- **O read model mente por ≤ um flush**: o `state` advisory atrasa a
  verdade (posse/fila) pela janela do batcher. Quem precisa de verdade
  faz o JOIN — é o que `JdbcHistoryStore` entrega; o dashboard consome
  isso.
- Quatro tabelas para manter consistentes em vez de uma linha — o preço
  é pago pelas três transações nomeadas e pelos invariantes executáveis
  (`CompletionResult` terminal-XOR-retry; `TerminalStateWriteScanTest`).
- Recuperação ganhou um ator novo: leases presas num nó VIVO (falha entre
  claim e dispatch) não têm mais expiração própria — o passe de
  reconciliação do Engine as devolve à fila, com três guardas na ordem
  estado > estado > tempo: encarnação em voo, conclusão em trânsito no
  batcher (`completionInTransit`) e o grace `max(2s, 4×poll-interval)` +
  duas rodadas ausentes (calibração e o guard por estado medidos no
  S5.5).
- Reversibilidade **baixa** — é a fundação; por isso o E1 rodou ANTES da
  fase (gate revisado registrado no BASELINE §E1).
