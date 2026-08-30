# ADR-0060 — Progresso estrito do cron e semântica sob transição de DST

Data: 2026-08-29 · Status: aceita · Complementa a ADR-0035 (não a revoga)

## Contexto

A revisão de codebase de 2026-08-29 achou dois defeitos no caminho que decide
**quando cada job recorrente dispara**. Os dois eram silenciosos, nenhum tinha
teste, e nenhum ADR falava do assunto.

**1. `CronExpression.next()` podia devolver o próprio argumento.** O contrato é
"o próximo instante estritamente depois da semente". Para expressões `L-n` de
dia-do-mês a promessa quebrava:

```
expr="0 0 0 L-28 * *"
  next(2026-01-01T00:00:00Z) = 2026-01-03T00:00:00Z   ok
  next(2026-01-03T00:00:00Z) = 2026-01-31T00:00:00Z   ok
  next(2026-01-31T00:00:00Z) = 2026-01-31T00:00:00Z   *** ponto fixo ***
```

`QuartzCronField.nextOrSame` tentava o roll-forward **uma vez** e não
reverificava; para `L-n`, `rollbackToMidnight` compara só `DAY_OF_MONTH` e não a
data inteira, então a única retentativa ainda aterrissava antes da semente e o
laço de `CronExpression.nextOrSame` convergia num ponto fixo. `L-30` travava já
no segundo passo. O defeito é do upstream — reproduzido contra
`org.springframework.scheduling.support.CronExpression` 7.0.8 —, mas o código é
vendorizado e a consequência aqui é muito pior.

**A amplificação.** `FiringPlanner.planSeries` assume progresso estrito no laço
de materialização. Sem ele, medido:

```
occurrences = 1440   distintas = 1
nextFireAt  = 2026-01-29T00:00:00Z   (semente era 2026-01-29T00:00:00Z)
ainda devido no proximo tick? true
```

Ou seja: 1.440 linhas em `mohs_execution` e `mohs_ready` por tick, o job
executando milhares de vezes, o poll loop nunca dormindo, e **nada no log**. A
ADR-0035 previu o cron que *nunca dispara* (IAE, log de erro, pula o job); não
previu o que *para de andar*. `L-28` é uma expressão que qualquer pessoa escreve
sem malícia, e `CronSpec` não valida nada além de `isBlank`.

**2. Horário de verão duplicava e pulava.** `CronSpec("0 0 2 * * *",
Europe/Berlin)` — o cron canônico de fechamento diário:

```
recuo  25/10/2026:  02:00+02:00  e  02:00+01:00   -> DUAS execuções no mesmo dia
avanço 29/03/2026:  02:00 não existe              -> o dia não roda
```

Em toda zona com DST, um job diário de hora fixa dentro da janela executava duas
vezes por ano num dia e zero noutro. Para faturamento ou fechamento diário, a
duplicata é o pior desfecho possível num scheduler que se propõe referência de
confiabilidade de execução. O Quartz dispara **uma** vez nos dois casos — nós
divergíamos dele nas duas direções, por herança, não por decisão: não havia uma
linha sobre DST em nenhum ADR, e o único teste com fuso nomeado usava
`America/Sao_Paulo`, que não tem DST desde 2019.

## Decisão

1. **Progresso estrito é invariante, verificado dos dois lados.** A causa raiz é
   corrigida em `QuartzCronField.nextOrSame`, que passa a repetir o roll-forward
   em laço contra a SEMENTE e a devolver `null` no esgotamento. E
   `NextFireCalculator` verifica o resultado antes de devolvê-lo: um contrato
   consumido por um laço não pode depender só de o produtor se comportar. O
   esgotamento vira `IllegalArgumentException` com a mensagem dizendo o que
   fazer — falha alta, que a ADR-0035 já sabe rotear (erro no plano deste job,
   sem derrubar a varredura dos demais).

2. **No recuo de DST, a repetição só é suprimida quando ela é REPETIÇÃO.** O
   critério é a **densidade UNIFORME da série em torno do slot ambíguo**: quando o
   próximo disparo tem o mesmo `LocalDateTime` da semente e só o offset muda,
   `Duration.between(seed, next)` É o deslocamento, e a série precisa ser ao menos
   tão densa quanto ele **dos dois lados** — há ocorrência a ≤ deslocamento antes
   E depois. Se sim, a segunda ocorrência é trabalho de verdade e dispara; se não,
   é o mesmo slot repetido e some.

   O efeito é preservar a CADÊNCIA em tempo real, medido em Europe/Berlin,
   25/10/2026 (disparos locais às 02:00):

   | expressão | disparos | por quê |
   |---|---|---|
   | `0 0 2 * * *` (diário) | 1 | duplicar um fechamento diário é o pior desfecho |
   | `0 0 2,23 * * *` | 1 | 2×/dia esparso: o mesmo fechamento, não trabalho novo |
   | `0 0 2,3 * * *` | 1 | horas ADJACENTES: denso depois, vazio antes — é o mesmo fechamento |
   | `0 0 1,2 * * *` | 1 | idem, do outro lado |
   | `0 0 2 * * SUN` (semanal) | 1 | idem |
   | `0 0 */2 * * *` | 1 | manter as duas daria gaps de 1h numa cadência de 2h |
   | `0 0 * * * *` (horário) | **2** | suprimir deixaria 2h de tempo real com um disparo |
   | `0 */30 * * * *` | **2** | idem |

   **Duas versões erradas desta regra ficam registradas**, porque as duas passavam
   nos testes que existiam na hora. A primeira usava "tem outra ocorrência hoje" —
   proxy de calendário, que não responde a pergunta — e fazia `0 0 2,23` duplicar.
   A segunda olhava só o passo PARA A FRENTE e fazia `0 0 2,3` duplicar: 1.346
   ocorrências numa varredura do TZDB inteiro. Um proxy unidirecional sobre uma
   série responde pela metade uma pergunta que é bidirecional.

   **Perda é pior que atraso** continua valendo para o caso diário, que motivou a
   decisão; converge com o Quartz ali.

3. **No avanço de DST, o horário inexistente NÃO é recuperado.** O dia da
   transição simplesmente não dispara, e a próxima ocorrência é a do dia
   seguinte. **Diverge do Quartz**, que recupera o disparo pulado. A escolha é
   por simplicidade e previsibilidade: recuperar exige inventar um horário que o
   usuário não escreveu, e um job que roda às 03:00 "porque era 02:00" é mais
   difícil de explicar às 3h da manhã do que um job que não rodou naquele dia.
   Quem precisa da execução no dia da transição usa UTC, que não tem transição.

4. **A dedupe do recuo mora no `NextFireCalculator`, não no cron.** É ele que
   conhece o `ZoneId` — `CronExpression` opera sobre `Temporal` e não deve saber
   o que é uma transição de fuso. O parser vendorizado fica o mais próximo
   possível do upstream, o que mantém a próxima resincronização auditável.

5. **A divergência funcional do upstream é declarada no cabeçalho do arquivo.**
   Ele dizia "no other functional changes"; passa a nomear exatamente a mudança
   de `nextOrSame` e por quê. Vale abrir issue no spring-framework com o caso
   `L-28`.

## Consequências

- Um `L-n` que antes travava agora avança: `L-28` produz
  `03/01 · 31/01 · 03/03 · 02/04`, `L-30` produz `29/01 · 01/03 · 31/03 · 01/05`.
  Nenhuma expressão que já funcionava muda de resultado — a mudança só alcança o
  caminho que antes devolvia a semente.
- **O regressor é um teste-propriedade**, não um caso: `next()` iterado 200 vezes
  sobre onze expressões (as extensões Quartz inteiras mais `29 2` bissexto),
  afirmando "estritamente posterior" a cada passo. Foi a ausência dele que deixou
  o defeito entrar — `L-3` era testado e passava, `L-28` não era.
- **Um job diário em zona com DST executa exatamente uma vez por dia**, exceto no
  dia do avanço, em que não executa. Isso é mudança de comportamento observável e
  está pinado em testes nomeados pela decisão.
- **Validado por varredura, não por caso:** 247.415 verificações sobre TODAS as
  zonas do TZDB × todas as transições de recuo de 2024–2030 × 11 formas de cron.
  Zero travamentos (o ponto fixo de `CronExpression.next()` em zonas de offset de
  meia hora — `Australia/Lord_Howe`, `Pacific/Chatham` — foi corrigido junto, ver
  `reanchoredThroughWallClock`; ele fazia um job diário parar PARA SEMPRE, e é o
  único caminho pelo qual a guarda de progresso não tinha recuperação).
- **Resíduo conhecido, não corrigido:** em `Pacific/Chatham` (offset `:45`,
  deslocamento de 1h), `0 0,45 * * * *` deixa um vão de 1h45 na janela ambígua em
  vez de dois de 45min. É a única forma que a varredura acusou depois do fix, numa
  zona de ~600 habitantes, e sai da aritmética de quartos de hora contra um
  deslocamento de hora cheia. **Gatilho:** primeira instalação em zona de offset
  fracionário. `Antarctica/Troll` (deslocamento de 2h) e `Australia/Lord_Howe`
  (30min) estão corretos — o que a varredura acusou neles era o oráculo do teste
  assumindo deslocamento de 1h, não o código.
- A guarda de progresso do `NextFireCalculator` é redundante hoje, por
  construção: com a causa raiz corrigida não há caminho conhecido que a dispare.
  Ela fica porque o custo é uma comparação por disparo e o que ela previne é uma
  tempestade silenciosa — e porque o cron é vendorizado, ou seja, uma
  resincronização futura pode reintroduzir o defeito sem ninguém notar.
- **O que esta ADR NÃO cobre:** validação de expressão no momento do
  `upsert`/`reschedule`. Uma expressão que nunca dispara continua sendo
  descoberta no primeiro cálculo, não na escrita. O caminho de escrita já chama
  `nextFireAfter` (`JdbcJobStore`), então na prática a maioria dos casos falha
  cedo — mas isso é incidental, não contratual.

## Referências

`QuartzCronField#nextOrSame`, `NextFireCalculator#nextCronFire`,
`CronExpressionTest#nextIsAlwaysStrictlyAfterTheSeed`,
`NextFireCalculatorTest#dstFallBackFiresTheRepeatedWallClockTimeOnlyOnce`,
`NextFireCalculatorTest#dstSpringForwardSkipsTheDayWhoseWallClockTimeDoesNotExist`;
ADR-0035 (o disparo do gatilho devido e a política de misfire), ADR-0049
(timestamps na fronteira JDBC — DST ali, não aqui).
