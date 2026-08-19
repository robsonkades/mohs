# ADR-0042: Enforcement de rate limit — token bucket consumido por rodada de claim

## Status
Decided — 2026-08-18. O vínculo job→limite (item 1) e o mecanismo (itens 2–4)
foram aprovados pelo autor, nesta ordem: a ADR foi escrita e aprovada ANTES
de qualquer linha de código; a revisão de duas fases do item 4 foi aprovada
depois, contra a medição no caminho real (ver "Revisão 2026-08-18").

## Context
`RateLimit` é hoje uma **spec órfã**: o record existe em
`io.mohs.core.resource`, a tabela `mohs_rate_limits` e o `JdbcRateLimitStore`
estão completos — e não há um único chamador em `src/main`. Nada registra
limites no boot, nada os consulta no claim, e `JobDefinition` sequer tem como
apontar um limite (tem `runner` e `window`, não tem `rateLimit`). A API
pública promete um comportamento que o motor ignora em silêncio, que é pior
do que não oferecer o comportamento.

Duas restrições moldam a solução:

**Taxa não é concorrência.** A ADR-0009 (superseded pela ADR-0021) resolveria
o cap cluster-wide de *concorrência* por contagem derivada — `count(*) WHERE
state = 'RUNNING'`, barata porque o conjunto RUNNING é limitado pelo próprio
`max`, sem estado mantido e portanto sem hot row, sem bloat e sem drift. Para
*vazão* essa saída não existe: contar disparos numa janela é varrer histórico
— é literalmente a query de throughput do `GET /overview`, cujo custo é
proporcional à janela, não a `max`. Estado mantido é inevitável aqui, e com
ele voltam os três modos de falha que a ADR-0009 catalogou.

**O que o mestre já decidiu.** `docs/API-DESIGN.md` fixa que rate limit
limita a **execução (no claim)**, nunca o aceite ("Admissão nunca espera
capacidade"), e que a definição no boot é upsert cluster-wide sob a política
`on-conflict` da ADR-0006. `docs/MOHS-DOCUMENTO-MESTRE.md` §5 previa "janela
fixa" e já registrava o defeito conhecido dessa escolha: **burst de ~2× na
virada da janela**, com token bucket/sliding window listados como evolução.
Esta ADR adota a evolução direto, antes de o defeito existir em produção.

**Estado da arte.** Quartz, JobRunr e db-scheduler não têm rate limit —
só concorrência; não há prior art para copiar. Temporal tem
(`MaxTaskQueueActivitiesPerSecond`) e resolve com **cota por worker**: a
taxa global dividida pelo número de workers, sem coordenação no caminho
quente, ao custo de ser aproximada quando os nós estão desbalanceados.

## Decision

1. **Vínculo por job, espelhando `window`.** `JobDefinition` ganha
   `@Nullable String rateLimit`, irmão de `runner`/`window`; `@MohsJob(rateLimit
   = "smtp")`, `JobSpec.rateLimit("smtp")`, coluna `rate_limit` nos 4 schemas,
   campo em `JobResponse`, e o `Candidate` do claim passa a carregar o nome.
   Alternativa rejeitada: pendurar o limite no `MohsRunner` — casaria com o
   vocabulário de "recurso compartilhado", mas obrigaria a criar um runner
   só para limitar um job isolado, e funde dois eixos que o design mantém
   ortogonais (runner protege ESTE nó; limite protege o recurso externo).

2. **Token bucket na própria linha do limite.** `mohs_rate_limits` ganha
   `tokens INT NOT NULL` e `refilled_at TIMESTAMP NOT NULL`. Capacidade do
   balde = `max`; um token a cada `window / max`. O refill só acontece em
   tokens **inteiros**, e `refilled_at` avança em `n × (window / max)` — nunca
   para "agora". Sem aritmética fracionária, sem coluna de resto e sem drift
   acumulado. Alternativa rejeitada: GCRA (uma única coluna de TAT) — igual em
   precisão e mais enxuta, mas ilegível para quem depura às 3h e sem leitura
   natural para expor no `GET /rate-limits`.

3. **Consumo por RODADA DE CLAIM, não por execução.** Esta é a decisão que
   torna um ponto de serialização global viável, e é a diferença estrutural
   para o contador que a ADR-0009 condenou (que escrevia na partida **e** na
   conclusão de cada execução). No ponto de operação já medido — `batch=1000`,
   `poll=50ms`, ~4k exec/s — a linha do limite recebe **~20 UPDATEs/s**, não
   4.000. O lote já existe por outros motivos (ADR-0039/0040); aqui ele vira
   também o amortizador do contador.

4. **Duas fases dentro da transação de claim** (revisado em 2026-08-18; a
   versão original consumia numa fase só, no início — ver "Revisão" abaixo).
   **Fase 1**, antes das guardas: uma leitura SEM LOCK do saldo por limite
   distinto (`RateLimitStore#available`), que decide a admissão —
   `min(saldo, candidatos)`. **Fase 2**, depois do mutex de job e do CAS:
   `RateLimitStore#charge` cobra, por limite, exatamente o que foi
   REIVINDICADO, via `UPDATE` guardado por CAS sobre `(tokens, refilled_at)`
   — atômico por construção, mesma disciplina da ADR-0018, sem lock
   especializado. Limites são visitados em **ordem determinística de nome**
   (lock ordering, JCIP §10.1.1). Se o CAS não fechar após 3 tentativas
   internas, a rodada inteira é desfeita por rollback: as execuções já estão
   `RUNNING` na transação, e entregá-las sem token seria sobre-entrega.

5. **Token consumido não volta.** Não existe decremento pareado. Um nó que
   morre depois de consumir e antes de disparar **queima** os tokens: o
   cluster entrega menos que o limite por uma janela e o refill cura sozinho.
   É a diferença qualitativa para o drift da ADR-0009, onde a morte do nó
   vazava a vaga **para sempre**, sem reconciliação. Sub-entrega temporária é
   um erro seguro para proteção de recurso; sobre-entrega é a violação.

6. **O medidor conta tentativa de disparo, não execução distinta.** Reclaim
   do reaper (ADR-0012) e retry (ADR-0033) consomem token de novo, porque o
   recurso externo enxerga a tentativa repetida como mais uma chamada.
   At-least-once é o contrato (DDIA); um limite que só contasse execuções
   distintas mentiria justamente no cenário em que o recurso está sofrendo.

7. **Limite estourado não é erro nem misfire.** O candidato simplesmente não é
   reivindicado: continua `ENQUEUED`, sem transição, sem attempt, sem log de
   falha. Ele não está atrasado por gatilho (o que caracteriza misfire), está
   represado por política — confundir os dois encheria o dashboard de falso
   alarme exatamente quando o limite está funcionando.

8. **Registro no boot espelha o de jobs.** `mohs.rate-limits.<nome>.max/.window`
   e `@Bean RateLimit` fazem upsert na subida, sob a política `on-conflict` da
   ADR-0006: `override` (default) reaplica o valor do código — o PATCH de
   emergência morre no próximo boot, como o design REST promete; `preserve`
   mantém o PATCH. O upsert atualiza **apenas** `max`/`window` e **não reseta o
   balde** (tokens são clampados ao novo `max`) — caso contrário todo rolling
   deploy devolveria um balde cheio por nó que sobe, transformando deploy em
   burst. Consequência documental: o Javadoc de `RateLimitStore` afirma hoje
   que não há estado operacional a preservar; passa a haver, e o texto muda
   junto.

9. **Nome desconhecido bloqueia (fail-safe), com WARN** — mesma postura de
   `ExecutionWindowRegistry.excludes` para janela inexistente. Um job que
   aponta limite inexistente para de rodar de forma barulhenta, em vez de
   rodar sem limite nenhum contra o recurso que alguém quis proteger. A
   validação de boot (§5.13) deve promover isso a erro de startup.

10. **Relógio.** O refill lê o `Clock` injetado (invariante da casa).
    Relógio para trás ⇒ elapsed negativo ⇒ zero refill, e `refilled_at`
    nunca anda para trás — um NTP step para o passado atrasa a liberação,
    nunca libera dobrado.

11. **Wire.** `RateLimitResponse.currentCount` (stub M2, nunca implementado)
    vira `available` — tokens no balde. "Usado" não é grandeza de balde:
    o operador que abre o dashboard quer saber quanto ainda pode disparar
    agora. Registrado como v0.10 em `docs/REST-API-DESIGN.md`.

## Consequences

- **Burst máximo = `max`**, admitido de propósito: é o teto de rajada de um
  balde cheio, e é estritamente melhor que os ~2× de borda da janela fixa que
  o mestre listava como risco.
- **Contenção — MEDIDA** (Postgres 18.4 em container local, pgbench, sessão de
  2026-08-18; a linha do balde exercitada isolada, sem o resto do motor):

  | Cenário | tps | latência média |
  |---|---|---|
  | Ciclo do balde como transação própria, 1 cliente | 457 | 2,19 ms |
  | idem, 64 clientes | 398 | 161 ms |
  | idem, `synchronous_commit=off`, 1 cliente | 5.198 | 0,19 ms |
  | idem, `synchronous_commit=off`, 16 clientes | 7.370 | 2,17 ms |
  | Transação de 10 ms SEM balde | 97,4 | 10,26 ms |
  | Transação de 10 ms COM balde | 79,9 | 12,51 ms |

  Leitura: **a linha não é o gargalo**. Um cliente sozinho já satura em 457
  tps, e com commit assíncrono a MESMA linha sustenta 7.370 tps — ou seja, o
  teto de 457 era fsync do commit, não o lock. O custo marginal do balde
  dentro de uma transação que já existe é **+2,24 ms** (12,51 − 10,26), e
  quase tudo disso é round trip: os planos são index scan na PK com 0,049 ms
  e 0,044 ms de execução.

  O que realmente limita é a **serialização**: enquanto a transação de claim
  segura o lock, as rodadas dos outros nós que tocam o MESMO limite esperam.
  Com transação de 10 ms o teto medido é **~80 rodadas/s por limite**, plano
  em 4 e em 16 clientes (só a latência cresce: 50 ms → 203 ms).
  **REVISADO em 2026-08-18** por medição com a rodada real a `batch=1000`:
  ela custa ~30 ms, não 10, e o teto cai para **~33 rodadas/s** — a folga
  acaba entre 1 e 2 nós, não em 4 (tabela completa em
  `docs/RATE-LIMIT-EVOLUTION.md`). Como cada nó
  faz 20 rodadas/s a `poll=50ms`, isso significa folga até ~4 nós e fila a
  partir daí — e a fila atinge inclusive jobs SEM limite, porque o lock mora
  dentro da transação de claim compartilhada. Limites diferentes são linhas
  diferentes e não disputam entre si; o problema é um limite quente, não
  muitos limites.

  Escape hatches, na ordem: (i) consumir num `UPDATE ... WHERE tokens >= :n`
  condicional no fim da transação, encurtando a janela de lock para a cauda
  (teto sobe para a faixa dos 450/s medidos) ao custo de perder a rodada
  quando o CAS condicional falha; (ii) cota por node no estilo Temporal,
  usando o registro de heartbeat da ADR-0041 como divisor — aproximada, porém
  sem coordenação no caminho quente. Nenhuma das duas entra agora: a v1
  assume o desenho exato e o teto de ~80 rodadas/s fica registrado aqui como
  o gatilho para revisitar.
- **Bloat de tupla — MEDIDO, e não é problema**: 116.715 updates na linha do
  balde produziram **99,5% de HOT updates**, 1 tupla morta e tabela de
  256 kB. As colunas mutáveis não entram em índice nenhum, que é exatamente a
  condição do HOT — o modo de falha que a ADR-0009 catalogou não se
  materializa neste desenho. Fillfactor não precisa entrar.
- **Head-of-line**: candidatos represados continuam no topo da ordem
  (prioridade, `scheduled_at`) e serão re-selecionados a cada rodada do mesmo
  tick, gastando seleção sem produzir claim. Aceito na v1 — a saída, se medir
  que importa, é excluir o limite exausto do predicado das rodadas seguintes
  daquele tick (ADR-0040), não um índice novo.
- **`GET /rate-limits` ganha fonte real** para `available`; o `PATCH` altera
  `max`/`window` e clampa os tokens ao novo teto.
- **A API pública cresce e QUEBRA em dois pontos** — aditivo em
  comportamento, não em assinatura: (1) `rateLimit` entra no MEIO do record
  `JobDefinition`, o que muda o construtor canônico (o overload de 13
  argumentos pré-ADR-0037 continua existindo; quem chamava a forma de 14
  precisa recompilar) e qualquer record pattern que desconstrua o tipo;
  (2) `Mohs` ganha dois métodos sem `default`, quebrando implementadores
  externos da interface. Aceito enquanto a API não estiver congelada — e a
  inserção no meio foi escolhida de propósito: ela provoca ERRO DE
  COMPILAÇÃO em vez de trocar argumentos em silêncio, que é o que anexar no
  fim causaria em quem passa posicionalmente. Revisitar antes da 1.0.
- **Um job por limite.** Não há composição (job apontando dois limites) nem
  limite por runner. Se aparecer o terceiro caso de uso real, aí se
  generaliza — não antes.

## Revisão 2026-08-18: consumo em duas fases (medido no caminho real)

A versão original consumia o balde numa fase só, no INÍCIO da transação de
claim, sob `SELECT ... FOR UPDATE`. Medição posterior no caminho real
(`RateLimitContentionHarness`, Postgres, `batch=1000`) mostrou que o lock
ficava preso pela transação inteira (~30 ms) e a vazão congelava em ~33
rodadas/s a partir de 2 nós — folga que acabava entre 1 e 2 nós, não em 4.
Movendo a cobrança para o FIM (CAS em vez de lock pessimista):

| nós | exec/s (balde nunca throttla) | rodadas vazias |
|---|---|---|
| 1 | 13.313 | 0 |
| 2 | 26.467 | 0 |
| 4 | 49.229 | 0 |
| 8 | **77.957** | 0 |

Escala linear, ~20× de folga sobre o ponto de operação de 4k/s. No regime
que interessa — **throttlado** (balde 5.000/min, 8 nós disputando):

| nós | exec/s | rodadas | rodadas vazias | vazias% |
|---|---|---|---|---|
| 1 | 913 | 158 | 0 | 0,0% |
| 2 | 913 | 524 | 49 | 8,6% |
| 4 | 912 | 952 | 470 | 33,1% |
| 8 | 911 | 2406 | 1910 | **44,3%** |

Sobre a métrica: "rodada vazia" é **teto** de descarte, não descarte exato —
conflaciona a rodada desfeita pelo CAS com a rodada em que o balde estava
vazio, ninguém foi admitido e nada chegou a ser cobrado. Separar exigiria
instrumentar a produção só para medir. (Uma primeira versão desta tabela
contava descartes capturando a exceção; quando a correção do gate fez a
rodada desfeita voltar VAZIA em vez de propagar, aquele instrumento passou a
medir zero — números refeitos com o contador correto.)

Duas leituras que decidem a favor do desenho, e uma que é preço aceito:

- **A vazão entregue é idêntica em 1, 2, 4 e 8 nós (911-913/s) e bate o
  orçamento exato do balde** (5.000 iniciais + ~83/s de refill em 6 s ≈
  5.500) — invariante que se manteve em TODAS as versões do código medidas. Sobre-entrega não acontece sob 8 nós de pressão — é a prova de
  corretude sob carga, não só por raciocínio.
- **Rodadas vazias são frequentes sob throttling** (33% a 4 nós, 44% a 8). Cada um é barato em trabalho entregue (a admissão da fase 1 já
  limitou o lote ao saldo pequeno), mas desperdiça o `SELECT` de candidatos
  daquela rodada. Aceito: o limitador continua entregando o orçamento exato,
  e reduzir o lote pelo saldo antes de selecionar é otimização sem medição
  que a justifique.
- A contenção NÃO usa o backoff de deadlock do SQL Server: os perfis são
  opostos (deadlock é raro e espera o par soltar; balde esgotado é o caso
  normal e só o refill resolve). Uma retentativa imediata — que reexecuta a
  fase 1 contra o saldo novo — e desiste. Dormir 20-50ms na thread do tick
  custava vazão de pico sem mudar o veredito: com o sleep, 38.551 exec/s a 4
  nós; sem ele, 49.229.

Efeito colateral que a revisão corrige de graça: cobrando o REIVINDICADO em
vez do admitido, job com `maxConcurrentExecutions` deixa de queimar o balde
com candidatos que perderiam o mutex logo adiante.

## Atualização de schema numa instalação existente

Os `schema-*.sql` do projeto são `CREATE TABLE IF NOT EXISTS` (e
`IF OBJECT_ID(...) IS NULL` no SQL Server): **eles não alteram tabela que já
existe**. Numa instalação anterior a esta ADR, `mohs_job_definitions` e
`mohs_rate_limits` continuarão sem as colunas novas, e o `SELECT` de
candidatos passa a referenciar `j.rate_limit` — resultado: `BadSqlGrammarException`
a cada tick, nenhum job reivindicado, e a mensagem que o operador vê é
`engine tick failed`, não "seu schema está desatualizado". O projeto não tem
Flyway/Liquibase (decisão pendente até a 1.0), então o upgrade é DDL manual:

```sql
ALTER TABLE mohs_job_definitions ADD rate_limit VARCHAR(255);
-- balde herdado nasce VAZIO de propósito: sub-entregar é o erro seguro,
-- e o primeiro refill o recompõe em uma janela
ALTER TABLE mohs_rate_limits ADD tokens INT NOT NULL DEFAULT 0;
ALTER TABLE mohs_rate_limits ADD refilled_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;
```

(SQL Server: `ADD rate_limit NVARCHAR(255)` e `refilled_at DATETIME2 NOT NULL
DEFAULT SYSUTCDATETIME()`; MySQL: `refilled_at DATETIME(6)`.) A ausência de
uma história de migração automatizada é lacuna estrutural conhecida do
projeto, anterior a esta ADR — que só a torna visível por ser a primeira a
adicionar coluna a uma tabela já existente.

## Source
`docs/API-DESIGN.md` (Recursos nomeados; "Admissão nunca espera capacidade";
semântica cluster-wide do upsert), `docs/MOHS-DOCUMENTO-MESTRE.md` §5
(rate limiter cluster-wide e o risco de burst de 2× da janela fixa),
ADR-0009 (superseded — catálogo de hot row/bloat/drift), ADR-0006
(`on-conflict`), ADR-0012 e ADR-0033 (reclaim e retry como novas tentativas),
ADR-0039 e ADR-0040 (lote e rodadas de claim), ADR-0041 (registro de nodes,
insumo do plano B de cota por node).
