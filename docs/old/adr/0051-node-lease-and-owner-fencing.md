# ADR-0051 — Lease de nó com fence de posse por encarnação

Data: 2026-08-22 · Status: aceita · Fase: Phase 4 do redesign ("node lease + epoch fencing")

## Contexto

A liveness da ADR-0012 era por EXECUÇÃO: o claim gravava
`lease_expires_at`, e cada tick renovava a lease de tudo que o node
executava — ~5 updates por execução na tabela mais quente do sistema
(Finding A do redesign; medido na BASELINE da Phase 4: 6,67–6,97
`n_tup_upd`/execução no cenário slow-handler). No ponto de operação alvo,
isso é a maior fonte única de write amplification do engine. A renovação
também carregava o único mecanismo de detecção de posse perdida (o drop
do zumbi) e alimentava o reaper, cujo critério era `lease_expires_at <
now` — o que produziu o self-reap do S8: um node que estala (pausa de GC,
DB pause) mais que o TTL reclamava o PRÓPRIO trabalho no tick seguinte.

## Decisão

1. **A liveness mora no nó** (`mohs_nodes`, migração V2): o heartbeat de
   cada tick grava `epoch` e `expires_at = now + node-lease-ttl`
   (`mohs.engine.node-lease-ttl`, default 15s) — UMA escrita por node por
   tick, em qualquer volume de execuções. A renovação por execução foi
   **deletada** (`ExecutionStore.renewLeases` saiu da porta);
   `lease_expires_at` continua sendo escrita no claim e retida no schema
   (rollback da Phase 4 sem migração), mas nenhum caminho a renova nem a
   consulta para decidir morte.
2. **Reaper dead-node driven**: candidato é execução `RUNNING` cujo dono
   NÃO está vivo — vivo é `expires_at > now`, ou, em linha de node de jar
   antigo sem `expires_at`, `last_heartbeat_at > now − lease-ttl`
   (tolerância de versão mista para rolling upgrade); node ausente de
   `mohs_nodes` é morto por definição. A ordem do tick é
   **heartbeat-antes-do-reaper**, o que mata o self-reap do S8 por
   construção: o reaper de um tick sempre enxerga a própria promessa
   recém-renovada.
3. **Fence de posse por encarnação** — `OwnerFence(node_id, fired_at)`:
   toda conclusão que sai de uma encarnação (dispatcher, reaper, watchdog)
   carrega o par que o claim gravou, e o CAS exige
   `state = 'RUNNING' AND node_id = :nodeId AND fired_at = :firedAt`.
   Re-claim grava `fired_at` novo (único por claim desde a ADR-0047),
   então o resultado tardio de um zumbi perde o CAS sem jamais tocar a
   encarnação nova — o fencing token do DDIA cap. 8, sem coluna nova em
   `mohs_executions`.
4. **Watchdog Bound vira liberação explícita**
   (`Dispatcher.abandonOwnership`): vencido o bound, o node LIBERA a
   posse — attempt sintético que consome orçamento de retry, cercado pelo
   fence — em vez de "parar de renovar e esperar o reaper". Uma escrita
   por zumbi raro; o handler local segue como zumbi e o resultado é
   descartado pelo fence.
5. **Epoch** vive só em `mohs_nodes`: contador de reencarnação por
   `node_id`, incrementado quando o node percebe a própria lease vencida
   (stall > TTL) — hoje diagnóstico (WARN + visível em `GET /nodes`);
   passa a ser guardado por escrita quando `mohs_lease` existir (Phase 5).

## Desvio registrado do plano

O plano (§21, Phase 4 / ADR-B) previa "every ownership write gains the
epoch guard". A implementação cerca por `(node_id, fired_at)` em vez de
epoch: `fired_at` já é único por claim (ADR-0047 o dobrou no CAS do
claim), o que dá um token por ENCARNAÇÃO DE EXECUÇÃO — mais fino que o
epoch por node — sem adicionar coluna nem escrita a `mohs_executions`.
O epoch continua existindo no nó e assume o papel de guarda quando a
Phase 5 criar `mohs_lease`.

## Consequências

- ~5 renovações/execução → 0; a única escrita periódica é 1 heartbeat
  por node por tick. Medição antes/depois na BASELINE (Phase 4).
- Handler saudável mais lento que qualquer TTL nunca é reclamado
  enquanto o node ticka — a lease deixa de ser teto de runtime implícito.
- Detecção de posse perdida deixou de existir como aviso proativo (o
  drop da renovação): o zumbi agora só descobre na conclusão, quando o
  fence perde. O WARN do drop foi substituído pelo descarte silencioso +
  o WARN de epoch bump no próprio node estalado.
- Recuperação de morte de nó passa a depender de `node-lease-ttl` (15s
  default) em vez de `lease-ttl` (30s) — recovery mais rápido por default.
- `watchdog-timeout` agora valida contra `node-lease-ttl` (antes:
  `lease-ttl`).
- Gate (medido 2026-08-22, BASELINE "Phase 4"): E6 passa — S6 recupera em
  17,1s (< 20s; era ~31s), SUSPEND com zero conclusões duplas sob zumbi
  real (o fence segura), S8 com re-execuções 0 (self-reap morto; era
  486–598). `n_tup_upd`: 6,67–6,97 → 2,00/execução (−70%) — o ≥ 80%
  literal do plano assumia ~10 antes; a renovação foi a zero e os 2,00
  restantes são o alvo da Phase 5. Lacuna pré-existente documentada:
  freeze no meio do claim deixa o lote travado-mas-ENQUEUED até a sessão
  morrer (mitigação DB-side: `idle_in_transaction_session_timeout`).
- Janela de rolling upgrade (jar antigo + novo no mesmo banco): o reaper
  do jar antigo ainda decide por `lease_expires_at`, que o jar novo grava
  no claim e não renova — handler do jar novo mais longo que `lease-ttl`
  (30s default) pode ser reclamado por um node antigo durante a janela
  (retry consumido, trabalho duplicado; o fence impede dupla conclusão).
  Mitigação: subir `mohs.engine.lease-ttl` acima do handler mais lento
  antes do deploy, ou aceitar a janela (at-least-once).
- `node-lease-ttl` (15s) é comparado entre relógios de nós distintos: o
  TTL efetivo é `15s − skew`. Deve exceder o skew máximo da frota mais o
  atraso de tick; a pendência do `DatabaseClock` UTC por dialeto
  (ADR-0049) colapsa o skew quando aprovada.
- Rollback: jar anterior funciona contra o schema novo (colunas
  `epoch`/`expires_at` ignoradas; `lease_expires_at` intacto e ainda
  escrito no claim); o reaper novo tolera nodes antigos pela staleness.
