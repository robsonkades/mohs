# ADR-0040: Rounds de claim por tick — e por que não um cursor por id

## Status
Decided — 2026-08-16. Origem: proposta do autor a partir do padrão de outbox
worker dele (keyset cursor por id sequencial + rounds por ciclo, "funciona
muito bem"). A metade dos rounds foi adotada (era a proposta (e) da rodada de
tuning do BASELINE, pendente de aprovação); a metade do cursor foi rejeitada
com os argumentos abaixo — esta ADR registra os dois lados.

## Context
Depois da ADR-0039, o tick faz um claim por ciclo, limitado pela folga de
dispatch. Sob backlog, a vazão fica acoplada ao `poll-interval`: para 4k/s
foi preciso operar a 50ms de poll — o que em idle vira 20 SELECTs vazios por
segundo por node. O padrão de outbox sugerido resolve isso com um loop de
rounds no mesmo ciclo; a versão dele usa também um cursor `id > :cursor`
(keyset) para minimizar re-varredura sob `SKIP LOCKED`.

## Decision
**Rounds: sim.** `mohs.engine.claim-rounds` (default 1 — formato clássico
preservado): o tick encadeia claims enquanto (a) houver rounds, (b) o lote
voltar cheio e (c) houver folga de dispatch — a folga é recomputada a cada
round e encolhe conforme o dispatch assíncrono enche, então os rounds param
sozinhos no teto da ADR-0039. Lote menor que o pedido encerra: a fila
drenou, o round seguinte seria um SELECT vazio.

Medido (drains de 50k, mesma máquina do BASELINE): a 250ms de poll, rounds=8
rendeu 3.605–3.739/s contra 2.277/s de rounds=1 (+58–64%); a 1s de poll,
2.134/s. Ou seja: rounds RELAXAM o acoplamento com o poll (5x mais poll
custou ~10% da vazão da rodada 5, não ~45%), mas não o eliminam — quando o
poll excede o tempo de drenagem de um "tanque" de in-flight, o teto vira
`dispatch-concurrency / ciclo de poll`. O knob de poll continua relevante;
rounds o tornam muito mais barato de relaxar.

**Cursor por id: não.** Três razões, em ordem de peso:
1. **Contrato de ordenação.** A outbox é FIFO por id; o claim do Mohs é uma
   fila de prioridade `(priority, scheduled_at)`. `id > :cursor` filtra por
   ordem de inserção enquanto ordena por prioridade — keyset só pagina
   quando o cursor é a chave do ORDER BY. Consequências concretas: retry
   preserva o id antigo e ficaria invisível depois que o cursor passasse;
   execução agendada para o futuro tem id velho ao ficar devida (idem); job
   de prioridade alta com id abaixo do cursor sofre inversão até o reset.
2. **O cursor já existe, físico.** O CAS de estado do claim tira a linha do
   predicado — e do índice parcial (DBTUNE-5) — no commit: o SELECT do
   round seguinte começa na cabeça real da fila sem reler nada. O que o
   cursor pouparia é só o skip-walk sobre linhas travadas por OUTRO node
   durante a transação de claim dele (janela de ms, limitada ao batch) — e
   a medição diz que o claim não é o gargalo (DBTUNE-16: 22k rows/s; o
   perfil é commit-bound no caminho de escrita).
3. **A premissa "id precisa ser number" é falsa aqui.** UUIDv7 canônico é
   lexicograficamente ordenado no tempo — se um dia um modo FIFO estrito
   existir, `id > :cursor` funciona no varchar atual, sem sequence (que
   custaria alocação no banco; UUIDv7 é gerado no cliente de graça, e é
   decisão registrada no schema: inserts localizados no fim do índice).

## Consequences
- `poll-interval` pesa muito menos na vazão (não zero — ver medição acima):
  sob backlog os rounds drenam até o teto de in-flight por ciclo; em idle o
  custo segue um SELECT vazio por tick, como antes.
- Dois guards entre rounds (review desta ADR): `drain()`/`pause()` no meio
  dos rounds interrompe o encadeamento — a janela de "trabalho aceito após
  o sinal" volta a ser de 1 lote, como antes (ADR-0007; Burns, shutdown
  coordenado); e um orçamento monotônico de `leaseTtl/4` limita a duração
  total dos rounds, porque a renovação de lease roda uma vez por tick,
  antes deles — sem o teto, `claim-rounds` alto o bastante faria um handler
  longo de tick anterior perder a renovação no meio dos rounds e ser
  duplicado pelo reaper de outro node (a patologia da ADR-0039, de volta
  por outra porta). O orçamento deriva do TTL existente — nenhum knob novo.
- O tick mais longo sob backlog adia heartbeat e os sinais de timeout/
  cancel (ADR-0034) em até a duração dos rounds — staleness efetiva vira
  "poll-interval + duração do tick", limitada pelo orçamento acima.
- Interação com ADR-0039 é a salvaguarda central: rounds nunca reivindicam
  além da folga — sem risco de reviver a patologia das 56k rejeições.
- Fairness multi-node: um node com rounds altos drena mais por tick; o
  `SKIP LOCKED` mantém os nodes fora do caminho um do outro e o excedente
  continua reivindicável por qualquer um. Rounds é teto, não cota.
- Medição da rodada no BASELINE (seção ADR-0040) — inclusive o custo de
  idle e a validação de que poll maior + rounds mantém a vazão da rodada 5.
