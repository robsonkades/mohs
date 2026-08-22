# PLAN — Phase 5 do redesign: the table split (ADR-A)

Estado: **em execução** · Base: `ARCHITECTURE_REDESIGN_PLAN.md` §4.3, §5.3–5.8,
§6.2–6.4, §7.2–7.6, §16.3, §18.3, §21 (Phase 5) · Pré-requisitos verdes:
Phases 0–4 commitadas (1756933), E1/E2 decididos (BASELINE).

## Decisões de execução (desvios registrados, sujeitos a veto)

1. **Sem dual-write/shadow-read.** O plano mitigava risco de migração VIVA;
   pré-GA não há usuário nem dado a preservar (§21.1 — o "drop and recreate"
   já foi usado na Phase 2; daqui é expand→flip→contract DENTRO da release).
   O risco de corretude é coberto pelo gate: suíte inteira + E5/E6 re-rodados
   + S1/S5 antes do commit final da fase. Registrar na ADR-A.
2. **Escopo do split = hot path.** Entram: `mohs_ready`, `mohs_lease`,
   `mohs_idempotency`, história particionada (`mohs_execution`/`mohs_attempt`
   novas). NÃO entram nesta fase (ficam onde estão, com trigger registrado):
   `mohs_job` JSONB (o control plane atual `mohs_job_definitions` é frio e
   funciona — reshape junto com tenant/circuitBreaker, §16.3-4, fase própria),
   `mohs_trigger`/shard ownership (Phase 6 — `shard` nasce na `mohs_ready`
   com valor 0), `mohs_node` extras (`shards`/`capacity`/`in_flight` — Phase 6/§11.1).
3. **Partições só no Tier 1 (Postgres).** H2 (Tier 3) e MySQL/SQL Server
   (Tier 2) ganham equivalentes funcionais SEM partição; a retenção deles
   continua no mecanismo ADR-0032 até o trigger da
   `BATCH-ARCHITECTURE-REVIEW`/Phase 8. É o argumento central da ADR-0050.
4. **Fence vira `(node_id, epoch)`** na `mohs_lease` (§6.3) — o que a
   ADR-0051 antecipou; o fence `(node_id, fired_at)` da Phase 4 morre junto
   com a tabela que o carregava. Epoch é o de `mohs_nodes` (Phase 4).
5. **API pública (§16.3, pré-aprovada no plano commitado):**
   `RETRY_SCHEDULED` → `RETRY_WAITING` (não-claimável) e
   `Execution.leaseExpiresAt()` → `Execution.owner()`. Os breaks de `Batch`
   são da Phase 8, não desta.
6. **Timestamps:** novas tabelas PG usam `TIMESTAMPTZ` (o que a ADR-0049
   adiou para cá) com travessia `OffsetDateTime` UTC (JDBC 4.2); demais
   dialetos seguem `LocalDateTime` UTC como hoje.
7. **Ids continuam `VARCHAR`** (não o `UUID` nativo do §7.2): `ExecutionId`
   é string na API pública e o vocabulário de teste inteiro usa ids não-UUID
   (`exec-1`); o ganho do tipo nativo (16 vs ~37 bytes/id na história) fica
   registrado como otimização de dialeto com trigger de storage medido.
   `payload` continua `TEXT` (JSONB é upgrade de queryabilidade — "when
   someone needs it", §7.3 — não pré-requisito do split).

## Passos (um por commit; suíte verde ao fim de cada um)

- [x] **S5.1 — Expand: schema novo ao lado do velho.** *(2026-08-22; db-tuner:
      INCLUDE do índice de claim removido — FOR UPDATE força heap access, o
      INCLUDE era só +43% WAL/2,7× índice; `idx_mohs_attempt_exec` no PG —
      detail view 19ms→0,035ms; MySQL payload/error MEDIUMTEXT. Requisitos
      herdados pro S5.2: gestor cria semanais NO BOOT antes do flip +
      rotina de move-out da DEFAULT; `created_at` DEVE ser derivado do
      UUIDv7 no enqueue — a poda do UPDATE terminal é por IGUALDADE; o SQL
      do §7.6 do plano tem bug: `x.created_at` fora do unnest.)* Migração V3 nos 4
      dialetos + `schema-*.sql`: `mohs_ready` (+ `ix_ready_claim`),
      `mohs_lease` (+ `ix_lease_node`, `ix_lease_job`), `mohs_idempotency`,
      `mohs_execution`/`mohs_attempt` (particionadas por RANGE no PG com
      bootstrap de partições; tabelas planas nos demais). Storage options
      (fillfactor 70, autovacuum agressivo) só no PG. Nenhum código muda.
- [ ] **S5.2 — As quatro portas + stores novos, sem chamador.** `WorkQueue`,
      `LeaseStore`, `HistoryStore`, `ControlStore` em `io.mohs.engine`;
      implementações em `io.mohs.store.jdbc` (claim §5.4 single-statement no
      PG; forma portátil SELECT FOR UPDATE SKIP LOCKED + DELETE nos demais),
      enqueue §7.5-1, completion em lote §7.6 (lease delete fenced RETURNING
      + attempt insert + update advisory), reaper §4.3 (delete leases do nó
      morto + reinsert ready com attempt+1), gestão de partição (criação
      antecipada). Testes de store completos. Engine ainda no caminho velho.
- [ ] **S5.3 — Flip do engine.** Poll loop/dispatch/completion/reaper/firer/
      cancel sobre as portas novas; `RETRY_WAITING` na API; `Execution.owner()`;
      caps derivados de `mohs_lease` (ADR-D) e fim do contador
      `running_execution_count`; facade/REST/dashboard leem o modelo novo
      (estado advisory + join de lease onde precisa verdade). Testes de
      engine/REST reformulados. E5/E6 re-rodados.
- [ ] **S5.4 — Contract.** Caem: `mohs_executions`/`mohs_attempts` (tabelas e
      código), portas velhas (`ExecutionStore`/`Claimer`/`Reaper`… → as 4),
      `TerminalStateWriteScanTest` re-apontado, ArchUnit atualizado,
      migração V4 de drop.
- [ ] **S5.5 — Validação e registro.** S1 ≥ 12k/s, tuple versions = 2 na
      história, S5 (claim independe do tamanho da história); BASELINE
      "Phase 5"; ADR-A/C/D escritas; plano §21 com o resultado.

## Gate da fase (do plano)

S1 ≥ 12 k/s · tuple versions/execução = 2 · S5: história não afeta latência
de claim · suíte + E5/E6 verdes.
