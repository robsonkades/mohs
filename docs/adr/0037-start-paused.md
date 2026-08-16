# ADR-0037: `startPaused` — agenda declarada que nasce desarmada

## Status
Decided — 2026-08-15. Registro de disagree & commit: a recomendação técnica era adiar por
YAGNI (nenhum caso concreto pedia ainda); decisão do autor: implementar agora. A forma
implementada é a recomendada tecnicamente, não a proposta original (`autostart`, default
`false`) — ver "Alternativas rejeitadas".

## Context
Desde a ADR-0035, agenda declarada (`cron`/`every`) dispara sozinha. Não existia forma
**declarativa** de "agenda registrada, mas dormindo até o operador ligar": declarar
on-demand joga fora a configuração de cron; declarar a agenda e pausar via API é raceiro
(dispara até o pause chegar) e não é declarativo. A preocupação central do autor:
qualquer solução não pode gerar confusão de vocabulário ou de estado.

## Decision

**Um atributo definicional `startPaused` (default `false`), mapeado sobre o estado
`paused` que já existe — nenhum conceito novo no motor.**

- `@MohsJob(startPaused = true)` e `PolicySpec.startPaused()` (paridade programática);
  componente novo em `JobDefinition`. O nome nomeia exatamente o estado que produz:
  código diz `startPaused`, API mostra `paused: true`, operador liga com o
  `POST /jobs/{id}/resume` que já existe. Uma palavra — `paused` — de ponta a ponta;
  era a resposta à preocupação de confusão (a alternativa `autostart` criava vocabulário
  duplo: `autostart` no código, `paused` na API).
- **Aplica só no primeiro registro da definição** (INSERT do upsert): o job nasce com
  `paused = startPaused`. Depois disso, `paused` é decisão de operador e o upsert nunca a
  toca (regra da ADR-0006, inalterada) — **redeploy nunca re-pausa**; re-aplicar o
  atributo a cada boot brigaria com o operador, o mesmo lost-update-de-intenção que a
  ADR-0035 eliminou do trigger.
- Efeito no motor: nenhum código novo — `findDueRecurring` já exclui `paused`; o trigger
  de um job nascido pausado nunca é varrido até o resume, e a execução manual sob demanda
  continua valendo mesmo pausado (§3 do documento mestre).
- `startPaused` é coluna definicional (`start_paused`), persistida como `retries` etc.:
  entra na igualdade de `JobDefinition`, no diff do scanner e na reconciliação
  `on-conflict` — mudá-la no código é drift definicional normal, mas **só afeta a criação
  da LINHA**. Consequências explícitas (review deste ciclo): ressurreição pós-retire
  **não** re-aplica `startPaused` — o soft-retire preserva a linha, e `paused` volta como
  o operador o deixou ("a linha é a memória", ADR-0006); e mudar
  `startPaused = true → false` no código **não acorda** um job que nasceu pausado e nunca
  recebeu resume — depois do nascimento, acordar é sempre ato de operador (`resume`).
- `JobResponse` não ganha campo: o que importa ao operador é a verdade corrente
  (`paused`), já exposta; o valor declarativo inicial é detalhe de código.
- `startPaused` em job on-demand é permitido e inócuo (pause não afeta invocação manual)
  — validar contra seria regra a mais sem dano a prevenir.

**Alternativas rejeitadas:**
- `autostart` default `false` (proposta original): inverteria o comportamento de toda
  agenda declarada — quem escreve `cron` está dizendo *quando rodar*, e o default de menor
  surpresa (e de todo o mercado: Quartz, Spring `@Scheduled`, JobRunr, db-scheduler) é
  rodar. Também criava o vocabulário duplo apontado acima.
- Flag operacional paralela ("autostart" vivo no motor ao lado de `paused`): dois
  interruptores para a mesma pergunta ("está pausado mas autostart true — roda?").

## Consequences
- O ciclo de vida completo do trigger fica: agenda (quando recorre) · `startPaused`
  (nasce armada?) · `pause/resume` (dispara agora? — operador) · `POST /schedule` (roda
  uma vez) · futuro `PATCH /schedule` (ADR-0036, proposto: muda o quando sem deploy).
  Cada lever responde uma pergunta distinta; esta ADR não adiciona conceito novo ao motor.
- Schema: coluna nova nos 4 dialetos — pré-GA, drop-and-recreate (PENDENCIAS item 10);
  `DEFAULT FALSE` mantém bases recriadas idênticas ao comportamento atual.
- O trigger de um job dormente é armado no nascimento e envelhece durante a dormência: no
  `resume`, a política de misfire do job decide o encontro (ADR-0035 — pause converge no
  mesmo mecanismo). Com `IGNORE` (default) a série pula limpa pro presente; com
  `FIRE_ALL_MISSED`, o resume reproduz TODA a janela de dormência (cap 1.440/ciclo,
  drenando) — combinação a usar de olhos abertos.
- Construtor canônico de `JobDefinition` cresce um componente; a assinatura anterior
  permanece como construtor de conveniência (`startPaused = false`) — mudança aditiva,
  nenhum chamador existente quebra.

## Source
ADR-0006 (paused é operacional; upsert nunca o toca — o mecanismo que esta ADR reusa);
ADR-0035 (por que re-aplicar intenção declarativa por cima de decisão de runtime é lost
update); conversa de design de 2026-08-15 (proposta `autostart` do autor, preocupação de
confusão, contra-proposta `startPaused`).
