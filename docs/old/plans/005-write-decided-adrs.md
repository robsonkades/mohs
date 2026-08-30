# Plan 005: Write `docs/adr/` files for the 12 already-decided ADRs

> **Executor instructions**: Follow this plan step by step. This plan
> transcribes decisions that have **already been made** — your job is
> faithful summarization into the ADR format below, not new judgment. If
> anything in the "STOP conditions" section occurs, stop and report — do not
> invent or resolve a decision that the source docs leave ambiguous. When
> done, update the status row for this plan in `E.md`.
>
> **Drift check (run first)**: this repository has no commits yet. Open
> `docs/MOHS-DOCUMENTO-MESTRE.md` and confirm its §8 "Plano de ADRs" table
> still lists the same 12 rows (0001-0012) described below. If the table has
> changed (rows added/removed/retitled), treat that as a STOP condition —
> re-derive the ADR list from the live table rather than the list below.

## Status

- **Priority**: P3
- **Effort**: M
- **Risk**: LOW
- **Depends on**: none
- **Category**: docs
- **Planned at**: no commits yet (pre-initial-commit); working tree as of 2026-08-13

## Why this matters

Both the user's global `../../../CLAUDE.md` and this project's `../../../CLAUDE.md` mandate
that every relevant decision becomes a mini-ADR (context → decision →
consequences) in `docs/adr/`. The project's own master doc,
`docs/MOHS-DOCUMENTO-MESTRE.md` §8, already lists 12 ADRs with a status
column, 11 marked "decidido" and one ("Enforcement de queue") "em revisão" —
but `docs/adr/` doesn't exist on disk. The decisions are real and already
made; they just live as prose scattered across three living documents
instead of as the durable, individually-referenceable records the project's
own process calls for. This plan doesn't make any new decisions — it gives
the 12 existing ones the format the project committed to for itself.

## Current state

- `docs/adr/` does not exist.
- `docs/MOHS-DOCUMENTO-MESTRE.md` §8 (lines 546-561) lists:

  | ADR | Tema | Status |
  |---|---|---|
  | 0001 | Empacotamento: módulo único full Spring Boot | decidido |
  | 0002 | Arquitetura definição × invocação | decidido |
  | 0003 | Contrato assíncrono e transacional | decidido |
  | 0004 | Vocabulário e renames (Runner, JobQueue, ExecutionWindow) | decidido |
  | 0005 | Listeners × Interceptors | decidido |
  | 0006 | Ciclo de registro e `on-conflict` | decidido |
  | 0007 | Lifecycle do engine | decidido |
  | 0008 | Fonte de tempo configurável | decidido |
  | 0009 | Enforcement de queue | em revisão (gate de benchmark) |
  | 0010 | API REST v1 | decidido |
  | 0011 | Serialização e versionamento de payload | decidido |
  | 0012 | Liveness: heartbeat, lease e reaper (Watchdog Bound) | decidido |

## ADR template

Use this exact structure for every file (matches `../../../CLAUDE.md`'s "mini-ADR
(contexto → decisão → consequências)"):

```markdown
# ADR-NNNN: <Title>

## Status
Decided — 2026-08-12
<!-- for ADR 0009 only, use: Proposed — gated on benchmark (see Consequences) -->

## Context
<2-5 sentences: what problem/tension prompted this decision. Transcribe
from the source sections listed below — do not add reasoning that isn't
in the source.>

## Decision
<The decision itself, stated plainly. Transcribe/summarize from source.>

## Consequences
<What this costs, what it enables, what alternatives were rejected and why,
if the source states them. For 0009, state the benchmark gate explicitly:
what's measured, in which phase, and what triggers a change of enforcement
strategy.>

## Source
<Exact section references, e.g.:
docs/MOHS-DOCUMENTO-MESTRE.md §4 (lines 157-181); docs/API-DESIGN.md
"Empacotamento — módulo único, full Spring Boot [DECIDIDO]" (lines 589-611)>
```

File naming: `docs/adr/NNNN-kebab-case-slug.md`, 4-digit zero-padded number
matching the table above.

## Scope

**In scope** (create exactly these 12 files):
- `docs/adr/0001-single-module-packaging.md`
- `docs/adr/0002-definition-vs-invocation.md`
- `docs/adr/0003-async-and-transactional-contract.md`
- `docs/adr/0004-vocabulary-and-renames.md`
- `docs/adr/0005-listeners-vs-interceptors.md`
- `docs/adr/0006-registration-lifecycle-and-conflict-policy.md`
- `docs/adr/0007-engine-lifecycle.md`
- `docs/adr/0008-configurable-time-source.md`
- `docs/adr/0009-queue-enforcement.md`
- `docs/adr/0010-rest-api-v1.md`
- `docs/adr/0011-payload-serialization-and-versioning.md`
- `docs/adr/0012-liveness-heartbeat-lease-reaper.md`

**Out of scope** (do NOT touch):
- Do not edit `docs/MOHS-DOCUMENTO-MESTRE.md`, `docs/API-DESIGN.md`, or
  `docs/REST-API-DESIGN.md` — they remain the living/detailed source; the
  ADRs are a distilled, durable summary layered on top, not a replacement.
- Do not make any decision the source docs don't already make. If a source
  section is ambiguous or you can't find enough material to write Context/
  Decision/Consequences honestly, STOP on that specific ADR and report which
  one and why, rather than inventing content — finish the others.

## Git workflow

This repository has no commits yet (or follows whatever convention prior
plans' commits established, if the operator committed them). Do not commit
unless the operator explicitly asks you to.

## Steps

Write each ADR file using the template above. For each, the source material
already exists — this is transcription/summarization, not new writing. Use
the section pointers below (all verified against the docs as of this plan's
writing):

### Step 1: ADR-0001 — Empacotamento: módulo único full Spring Boot

Source: `docs/MOHS-DOCUMENTO-MESTRE.md` §4 "Empacotamento — módulo único,
full Spring Boot" (lines 157-181, includes the registered disagreement from
the tech lead persona and the PO's decision); `docs/API-DESIGN.md` section
"Empacotamento — módulo único, full Spring Boot [DECIDIDO]" (lines 589-611,
same content, slightly more detail on the three ArchUnit-backed
substitutes for multi-module discipline).

**Verify**: `docs/adr/0001-single-module-packaging.md` exists, has all four
required sections (Status/Context/Decision/Consequences/Source), and the
Consequences section mentions the registered disagreement ("full Spring
Boot fecha a porta pra quem não usa Spring") since the source explicitly
records it as a trade-off, not a hidden cost.

### Step 2: ADR-0002 — Arquitetura definição × invocação

Source: `docs/API-DESIGN.md` "Princípios de design" point 1 (lines 24-26);
`docs/MOHS-DOCUMENTO-MESTRE.md` §5.1 point 1 (lines 230-232); the code
example in `docs/MOHS-DOCUMENTO-MESTRE.md` §5.0 (lines 187-226) is useful
illustrative material for Context but the decision itself is the
"definição uma vez, invocação de N formas" principle.

**Verify**: file exists with all sections; Decision section states plainly
that invocation never redefines policy.

### Step 3: ADR-0003 — Contrato assíncrono e transacional

Source: `docs/API-DESIGN.md` "Contrato assíncrono das invocações
[DECIDIDO]" (lines 286-306, the 5 numbered clauses); `docs/MOHS-DOCUMENTO-MESTRE.md`
§5.6 (lines 332-347, same 5 clauses, slightly condensed).

**Verify**: file exists; Decision section enumerates the same 5 clauses as
the source (execução sempre assíncrona; durabilidade sempre síncrona;
retorno é recibo nunca Future; transacional por participação; admissão
nunca espera capacidade).

### Step 4: ADR-0004 — Vocabulário e renames

Source: `docs/API-DESIGN.md` "Vocabulário" table (lines 41-53);
`docs/MOHS-DOCUMENTO-MESTRE.md` §7 "Resolvidas" item 1 (lines 524-527, the
`JobQueue`/`ExecutionWindow` rename rationale specifically: "colisão de
import com o JDK custava centavos agora e uma fortuna depois do 1.0").

**Verify**: file exists; Decision section names both renames (`Queue` →
`JobQueue`, `Calendar` → `ExecutionWindow`) and the JDK-collision rationale.

### Step 5: ADR-0005 — Listeners × Interceptors

Source: `docs/API-DESIGN.md` "Observação e extensão — Listeners e
Interceptors [DECIDIDO]" (lines 402-465, includes the Quartz
anti-example and the "garantias e dogfooding" subsection);
`docs/MOHS-DOCUMENTO-MESTRE.md` §5.10 (lines 397-417, condensed version).

**Verify**: file exists; Consequences section notes the best-effort nature
of in-process listener delivery and that guaranteed reaction requires the
handler to enqueue a continuation within its own transaction (not the
listener).

### Step 6: ADR-0006 — Ciclo de registro e `on-conflict`

Source: `docs/API-DESIGN.md` "Ciclo de registro e política de conflito
[DECIDIDO]" (lines 118-155, includes the `override`/`preserve`/`fail`
semantics and the ORPHANED-definition handling); `docs/MOHS-DOCUMENTO-MESTRE.md`
§5.4 first half (lines 274-296).

**Verify**: file exists; Decision section covers both the three
`on-conflict` modes and the definitional-vs-operational-state upsert
distinction (the "job pausado às 3h continua pausado após o deploy das 9h"
example is good Context material).

### Step 7: ADR-0007 — Lifecycle do engine

Source: `docs/API-DESIGN.md` "Ciclo de vida do engine [DECIDIDO]" (lines
157-208, includes the state machine, the graceful shutdown sequence, and
`start-mode: manual` use cases); `docs/MOHS-DOCUMENTO-MESTRE.md` §5.4
second half (lines 297-320).

**Verify**: file exists; Decision section includes the state machine
(`CREATED → RUNNING ⇄ PAUSED → DRAINING → STOPPED`) and explicitly
distinguishes engine lifecycle (node-local) from job pause (cluster-wide) —
the source calls this distinction out deliberately, don't drop it.

### Step 8: ADR-0008 — Fonte de tempo configurável

Source: `docs/API-DESIGN.md` "Tempo — fonte configurável [DECIDIDO]" (lines
504-542, the three Clock implementations and the two-clocks discipline —
wall clock via `Clock`, duration via `nanoTime`); `docs/MOHS-DOCUMENTO-MESTRE.md`
§5.12 (lines 430-448, same content condensed).

**Verify**: file exists; Decision section names all three implementations
(`application`, `database`, `test`) and states the `Instant.now()`/
`System.currentTimeMillis()` prohibition (ArchUnit-enforced per the source).

### Step 9: ADR-0009 — Enforcement de queue (STATUS: proposed, not decided)

Source: `docs/API-DESIGN.md` "Enforcement da queue [EM REVISÃO → ADR com
gate de benchmark]" (lines 359-386, the counter-vs-derived-count comparison
and the soft-cap semantics); `docs/MOHS-DOCUMENTO-MESTRE.md` §5.8 (lines
367-390, same content) and §7 "Em revisão com gate" (lines 537-538).

Use `Status: Proposed — gated on benchmark` (not "Decided") for this ADR
only, per the source table marking it "em revisão". The Consequences
section must state the gate explicitly: Fase 0/1 measures the current
counter-based enforcement under target load (10k+ concurrent, hot queue);
confirmed contention triggers the switch to derived counting; the API
surface is agnostic to which wins, so the switch (if it happens) doesn't
change public contracts.

**Verify**: file exists; Status line says "Proposed — gated on benchmark",
not "Decided"; Consequences names the specific gate condition from the
source (contention confirmed under target load in Fase 0/1).

### Step 10: ADR-0010 — API REST v1

Source: `docs/REST-API-DESIGN.md` in full — particularly "Princípios"
(lines 17-33, the five numbered principles: 202 as contract, actor via
ActorResolver, RFC 7807 errors, cursor pagination, closed-by-default) and
"Decisões v0.3" (lines 103-113, no SSE, no built-in auth, dashboard
dogfoods the same API).

**Verify**: file exists; Decision section covers all five principles from
"Princípios" plus the "no SSE / no built-in auth in v1" decisions and their
stated rationale (mitigated by default-off + WARN + deployment guidance).

### Step 11: ADR-0011 — Serialização e versionamento de payload

Source: `docs/MOHS-DOCUMENTO-MESTRE.md` §7 "Resolvidas" item 2 (lines
528-530); `docs/API-DESIGN.md` "Decidido (12/08/2026)" first bullet (lines
621-626, more detail: the engine guarantees only boot-time round-trip
serialization, validation 4, not migration of already-persisted Executions).

**Verify**: file exists; Decision section states plainly that
compatibility across deploys is the handler/application's responsibility,
not the engine's, and that the engine's only guarantee is the boot-time
round-trip check.

### Step 12: ADR-0012 — Liveness: heartbeat, lease e reaper (Watchdog Bound)

Source: `docs/API-DESIGN.md` "Watchdog Bound — teto contra Attempt zumbi
[DECIDIDO]" (lines 210-251, includes the "cluster-wide, not per-job"
sub-decision and its rejected alternative, and the `lease-ttl` vs
`liveness` naming rationale); `docs/MOHS-DOCUMENTO-MESTRE.md` §7
"Resolvidas" item 3 (lines 531-535, the four capabilities this sustains:
at-least-once recovery, `GET /nodes`, queue soft-cap self-healing, and
execution-contract honesty).

**Verify**: file exists; Decision section covers both the Watchdog Bound
mechanism itself and the "cluster-wide, not per-job" sub-decision with its
stated reason (per-job would break the batched lease-renewal query with
dialect-specific date arithmetic); Consequences section lists the four
capabilities this sustains, from the master doc.

## Test plan

Not applicable — this is documentation, not code. "Verify" per step above
is a manual content check (does the file exist, does it have the required
sections, does it cover the specific points called out), not an automated
test.

## Done criteria

Machine-checkable / directly checkable. ALL must hold:

- [ ] All 12 files listed in Scope exist under `docs/adr/`
- [ ] Each file has all five sections: `## Status`, `## Context`,
      `## Decision`, `## Consequences`, `## Source`
      (check: `grep -L "## Status" docs/adr/*.md` and similarly for the
      other four headers — each should return no output, meaning every file
      has every header)
- [ ] `docs/adr/0009-queue-enforcement.md` is the only one whose `## Status`
      line does not say "Decided" (it says "Proposed — gated on benchmark")
- [ ] No files outside `docs/adr/*.md` are created or modified
      (`git status`)
- [ ] `E.md` status row for plan 005 updated

## STOP conditions

Stop and report back (do not improvise) if:

- Any of the source line ranges cited above no longer contain the content
  described (the docs have been edited since this plan was written) —
  report which ADR and re-locate the content in the current doc rather than
  guessing.
- You cannot honestly fill in Context/Decision/Consequences for a given ADR
  from the cited source material without adding reasoning that isn't there
  — write the other 11, skip that one, and report which one and why.
- The §8 table in `docs/MOHS-DOCUMENTO-MESTRE.md` has changed (different
  ADR count, renumbered, or a status other than "decidido"/"em revisão")
  since this plan assumes exactly the 12-row table quoted in "Current
  state".

## Maintenance notes

- Going forward, new decisions should get an ADR file directly (per
  `../../../CLAUDE.md`'s standing rule) rather than accumulating in the master doc
  first and being back-filled later — this plan is a one-time catch-up, not
  the intended steady-state workflow.
- `docs/MOHS-DOCUMENTO-MESTRE.md` §8 already numbers the next ADR slot
  implicitly (13+) — whoever adds ADR-0013 should also update the §8 table
  to keep it as the index/summary, with the new `docs/adr/0013-*.md` file
  as the canonical source.
- ADR-0009 will need a follow-up edit (flip Status to "Decided", fill in the
  actual outcome under Consequences) once the Fase 0/1 benchmark gate
  resolves — that's real future work, not part of this plan.
