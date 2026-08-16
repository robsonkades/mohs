# ADR-0036: Mudança de agenda em runtime (`PATCH /jobs/{jobKey}/schedule`)

## Status
Proposed — 2026-08-15, aguardando aprovação. Depende da ADR-0035 (materialização de
disparos; é a maquinaria de "agenda alterada → recalcula o trigger" que esta ADR reusa).

## Context
A agenda de um job nasce na definição (`@MohsJob`/`Mohs.define`) e, desde a ADR-0035,
dispara sozinha. A invocação manual (`POST /jobs/{id}/schedule`) é deliberadamente avulsa
(ADR-0002: invocação nunca redefine política). Falta o caso operacional legítimo: mudar a
recorrência **sem deploy** — baixar a frequência de um job durante um incidente, ajustar um
horário de cron em produção. A tensão central: o código é a fonte da verdade da agenda e o
scanner reconcilia no boot (`mohs.registration.on-conflict`, ADR-0006) — qualquer mudança
de runtime encontra o boot seguinte pela frente.

Estado da arte: Quartz (`rescheduleJob`) e db-scheduler (`reschedule`) mudam agenda em
runtime porque a agenda deles é dado sem reconciliação de código; Temporal resolve tornando
a agenda uma entidade servidor com CRUD; JobRunr re-registra do código no boot — mudança de
runtime não é durável lá também. Nenhum deles tem exatamente o nosso problema porque nenhum
tem o nosso par código-declarado + store reconciliado.

A casa já respondeu a essa tensão uma vez: o PATCH de rate limit
(`RuntimePatchResponse.BOOT_REVERSION_NOTICE`, `docs/REST-API-DESIGN.md` §"PATCH runtime ×
configuração de boot") — **mudança de runtime é de emergência, vale até o próximo boot, e a
resposta diz isso na cara do operador**; durabilidade se obtém codificando a mudança, ou
com `on-conflict: preserve`.

## Decision

**`PATCH /jobs/{jobKey}/schedule` muda a agenda armazenada da definição, sob o mesmo
contrato de PATCH runtime que o rate limit já estabeleceu.** Corpo = `ScheduleView`
(o sealed com discriminador `type` já usado na leitura — `CRON`/`INTERVAL`/`ON_DEMAND`);
resposta = `RuntimePatchResponse<JobResponse>` com o aviso de reversão no boot. Fachada:
`Mohs.reschedule(JobKey, Schedule)` — o REST só enxerga `io.mohs.core`.

**Sem estado paralelo de "override".** Alternativa considerada e rejeitada por ora: uma
agenda de override operacional separada da definicional (o padrão do `paused`), que
sobreviveria a deploys por construção. Custo: duas agendas por job no modelo, na API e na
cabeça do operador ("qual está valendo?"), mais colunas, mais pontos de decisão de agenda
efetiva em engine/dispatcher/reaper. O padrão de PATCH da casa já dá a semântica de
emergência com aviso explícito, e a durabilidade já tem dial (`on-conflict: preserve`).
Se surgir demanda real por override que sobrevive a deploy (ex.: dashboard editando agendas
como rotina, agendas por tenant mudadas por operador), reabrimos como revisão desta ADR —
com o vocabulário de "override operacional" já esboçado aqui.

**Semântica no boot, por linhagem da definição (nada novo a construir):**
- `ANNOTATION`: o scanner vê o drift e aplica `on-conflict` — `override` (default) reverte
  para o código **com diff logado** (nunca silencioso), `preserve` mantém o patch, `fail`
  derruba o boot. O aviso da resposta é o contrato: "codifique na anotação para tornar
  permanente".
- `PROGRAMMATIC`: o scanner não reconcilia — o patch dura até a própria aplicação chamar
  `define` de novo. O aviso continua verdadeiro no espírito (a fonte programática pode
  reafirmar a agenda a qualquer boot).

**Semântica no motor (herda a ADR-0035 por inteiro):**
- O reschedule grava as quatro colunas de agenda **e** `next_fire_at = inicial(now)` no
  mesmo UPDATE — exatamente o caso "agenda alterada sobrescreve: reconfiguração explícita
  vence disparo concorrente" já decidido e documentado na ADR-0035; a disciplina de lost
  update (escrever só o que se decidiu escrever) vale igual.
- `ON_DEMAND` desarma o trigger (`next_fire_at = NULL`) — desligar a recorrência em runtime
  é um caso válido do mesmo endpoint.
- Fixed-delay com ocorrência em voo: mesma semântica da mudança de agenda no upsert — arma
  a série nova; o rearme da conclusão perde no guard `IS NULL` (série única, janela de
  sobreposição pontual aceita e já documentada).
- Job pausado pode ser reagendado (o trigger recomputado segue bloqueado pelo filtro de
  `paused` até o resume). Job aposentado não: o guard `retired = FALSE` no UPDATE faz o
  PATCH devolver 404 — mesma invisibilidade de `retired` do resto da API.
- Misfire, retries, runner, window, timeout: **fora do PATCH** — só a agenda tem caso de
  uso de emergência declarado; cada outro campo entra quando tiver o seu (YAGNI).

**Porta e validação:**
- `JobStore.reschedule(JobKey, Schedule)` → `boolean` (linha encontrada e não-retired);
  implementações JDBC e in-memory. O upsert continua dono do estado inicial; o reschedule é
  a única outra escrita legítima de agenda — ambos documentados no contrato da porta.
- Cron irrealizável falha na borda (422 com a mensagem do `NextFireCalculator`) — mesmo
  fail-fast do upsert; nunca chega ao banco.
- Actor da mudança: resolvido na borda (`ActorResolver`) e **logado** — mesmo nível de
  trilha de pause/resume; não vira coluna (definição não carrega actor).

## Consequences
- O operador ganha o lever de emergência sem quebrar "código é a fonte da verdade": o boot
  seguinte restaura o código por default, com diff logado e aviso prévio na resposta do
  PATCH.
- `docs/REST-API-DESIGN.md` ganha a linha do endpoint; `ScheduleView` passa a ser corpo de
  request além de resposta (o `@JsonTypeInfo` existente já desserializa; falta só o mapping
  view→`Schedule`).
- `GET /jobs/{key}` reflete imediatamente a agenda nova e o `nextFireAt` recomputado — o
  estado real do trigger (ADR-0035), sem campo novo.
- Interação com invocação manual permanece ortogonal: `POST /schedule` continua avulso;
  `PATCH /schedule` muda a recorrência. Nomes distintos no path evitam confusão de verbo.
- Risco aceito: sob `on-conflict: override`, um patch de emergência esquecido morre no
  próximo deploy — é o comportamento anunciado pelo aviso, idêntico ao rate limit; o log de
  diff do scanner é o rastro.

## Source
ADR-0002 (definição × invocação), ADR-0006 (reconciliação e on-conflict), ADR-0035 (estado
do trigger; "agenda alterada sobrescreve"); `docs/REST-API-DESIGN.md` §"PATCH runtime ×
configuração de boot" e `RuntimePatchResponse` (o contrato de emergência que este endpoint
adere); Quartz `rescheduleJob`/db-scheduler `reschedule`/Temporal Schedules como estado da
arte comparado.
