# Revisão futura da arquitetura de lote

Companheiro da ADR-0043, no mesmo espírito de `docs/RATE-LIMIT-EVOLUTION.md`:
a ADR registra o que foi **decidido**; este arquivo registra o que ficou
**aberto**, e o gatilho concreto que faz cada item deixar de poder esperar.

Não é backlog. Item sem gatilho mensurável não entra aqui — vira PENDENCIAS ou
não existe. E quando um item for resolvido, ele sai daqui e a decisão vira ADR
ou Javadoc, como manda `docs/PENDENCIAS.md`.

Estado em 2026-08-19: feature entregue e verde (667 testes), quatro modos de
falha corrigidos durante o pipeline de qualidade, nenhum crítico aberto.

---

## 1. `Mohs.batch` grava fora de transação

**O que é.** `batchStore.insert` seguido de um `executionStore.insert` por
membro, cada um em autocommit — não existe `TransactionTemplate` em
`io.mohs.engine`. Crash, perda de conexão ou falha de serialização do payload
do membro *k* deixa o lote com `total = N` e `M < N` membros gravados. Os `M`
rodam normalmente; os outros nunca existem; o lote nunca fecha.

**Por que é grave.** É incurável. A ADR-0043 dispensou a varredura de
reconciliação, então nada percebe e nada conserta. O operador vê um lote
parcialmente executado, eternamente `RUNNING`.

**Atenuante.** A janela é curta (o laço de inserts) e a causa lógica mais
provável — job inexistente — já foi eliminada: `requireAllDefined` valida todos
os membros antes de qualquer escrita (`43241de`).

**Conserto proposto.** Uma porta `UnitOfWork` (Unit of Work, PoEAA) de três
linhas com implementação JDBC sobre a mesma `DataSource`, envolvendo
`batchStore.insert` + os membros. Não inverter a ordem: a FK
`batch_id → mohs_batches(id)` proíbe, e sem a FK o `countIntoBatch` passaria a
lançar na conclusão, prendendo execuções em `RUNNING` num laço de reaper.

**Gatilho.** Antes do primeiro usuário externo — é modo de falha sem cura, não
degradação. Se for adiado além disso, o Javadoc de `Mohs.batch` tem que dizer
que a criação não é atômica, porque hoje ele promete "o total é fixado na
criação" sem essa ressalva.

**Ganho de brinde.** Um commit em vez de N. No tamanho-alvo de 1.000 membros
são 1.000 commits com fsync hoje; a estimativa aritmética a partir dos ~0,6 ms
por statement+commit medidos no `BatchCounterDesignHarness` põe a criação de um
lote em ~600 ms.

---

## 2. O reaper conta e descarta a eleição do fechador

**O que é.** `completeAllWithinTransaction` chama `countIntoBatch` (desde
`274061b`) mas ignora o retorno. Quando a conclusão que zera o `pending`
acontece pelo caminho do reaper, aquele `pending() == 0` era a **única** vez que
alguém veria zero — não há conclusão futura. O `BatchCompleted` daquele lote não
existe em nó nenhum, para sempre.

**Por que é grave.** `GET /batches/{id}` diz `COMPLETED`, o `onCompletion` nunca
roda, e **não há uma linha de log** dizendo que algo se perdeu. O suporte
investiga o callback, o listener, a rede — e o culpado é um valor de retorno
descartado. O caminho do reaper não é exótico: é o de qualquer nó que morre,
o que num rolling deploy é rotina.

**Piso barato (fazer já se o conserto demorar).** Um WARN no ponto do descarte,
com o `batchId`, dizendo que o lote fechou por reclaim sem publicar o evento.
Transforma mistério em fato pesquisável por ~4 linhas.

**Conserto proposto (~30 linhas, o encanamento existe).** `completeAll` devolve
os lotes fechados junto dos ids (simétrico ao `Completion` de `complete`);
`Reaper.reclaimExpired` os repassa; `Engine.publishReclaimOutcome` — que já
publica os eventos do reclaim pelo mesmo publisher — publica um
`BatchCompleted` por elemento.

**Gatilho.** Qualquer relato de "o lote fechou mas o callback não rodou", ou o
primeiro uso de `onCompletion` em produção com mais de um nó.

**Adjacente.** `BatchCompleted` passa pelo `ExecutionEventPublisher`, que
**descarta** eventos quando o executor satura. Para `Succeeded`/`Failed` isso é
perda de observação, tolerável e documentada; para `BatchCompleted` é a perda da
eleição única — estruturalmente mais grave que os demais eventos do mesmo
pipeline. O log de descarte imprime só `getSimpleName()`, sem o `batchId`.

---

## 3. `BatchCompletionCallbacks` vaza em cluster e perde callback numa corrida

**Três problemas distintos:**

1. **Corrida de registro.** `Mohs.batch` comita os membros antes de retornar o
   `Batch`; `onCompletion` só registra depois. Um lote pequeno de job rápido
   pode fechar nessa janela — o evento passa com o mapa vazio, e o callback
   registrado logo em seguida nunca dispara e nunca sai do mapa. Janela pequena,
   mas determinística sob carga.
2. **Vazamento em cluster.** O Javadoc afirma que o mapa "não cresce com o
   tráfego — só com lotes vivos que alguém observou". **Isso é falso com mais de
   um nó:** lote fechado por outro nó nunca remove a entrada local. Com N nós,
   vaza (N−1)/N do que foi registrado.
3. Evento descartado pelo publisher saturado tem o mesmo efeito do item 2.

**Conserto do item 1.** Depois de registrar, reler o lote e disparar se já
estiver fechado — o `remove` do registro já dá a semântica one-shot, então quem
chegar segundo não faz nada. Isso também limita bastante o item 2, porque colhe
os lotes já fechados no momento do registro.

**Conserto mínimo do item 2, hoje.** Corrigir o Javadoc, que afirma o oposto do
comportamento real em cluster.

**Gatilho.** Item 1: primeiro uso sério de `onCompletion`. Item 2: qualquer
crescimento de heap não explicado num nó com muitos lotes observados — ou
simplesmente antes do GA, porque é um mapa com chave `String` sem teto.

---

## 4. Ordem de locks em `completeAll` abre classe nova de deadlock

**O que é.** `completeWithinTransaction` estabelece a ordem execução → job →
lote. O laço de `completeAllWithinTransaction` **intercala**: `J1, B, J2, B…`.
Basta um ciclo: reaper pega `J1`, pega `B`, pede `J2`; dispatcher concluindo
membro de `J2` do mesmo lote pega `J2`, pede `B`.

**Consequência.** O `completeAll` inteiro (até 500 candidatos,
`JdbcReaper.RECLAIM_LIMIT`) faz rollback e reexecuta no tick seguinte.
Auto-cicatriza, mas gera erro em log, paga `deadlock_timeout` (1s no default do
Postgres) e perde um tick do reaper exatamente quando o reaper mais importa:
morte de nó em massa.

**Segundo efeito.** O lock da linha do lote é adquirido no meio da transação do
reaper e segurado até o commit dela — uma transação com até 500 membros, 500
decrementos e um `batchUpdate` de attempts. Todo dispatcher do cluster
concluindo membro daquele lote fica na fila atrás disso.

**Conserto.** Agrupar os lotes **por último**, em ordem estável de `batchId`
(ordem global de aquisição, JCIP cap. 10). A ordenação sozinha já elimina o
ciclo e é gratuita. Colapsar em `SET succeeded = succeeded + :n` por lote é o
passo seguinte — é o que a própria ADR-0043 antecipa em "O que esta ADR não
estabelece", com a ressalva de medir antes.

**Gatilho.** A ordenação: fazer junto do item 2, é a mesma função. A agregação
`+ :n`: quando houver medição contra o `LivenessLoadHarness`.

---

## 5. `RETURNING`/`OUTPUT INSERTED` por dialeto

**Estado.** O implementado é `UPDATE` + `SELECT`, portável nos quatro dialetos,
porque H2 e MySQL não têm a cláusula (medido). Ver errata 2 da ADR-0043: a
justificativa original se apoiava numa medição que não cobria este arranjo.

**Hipótese medida.** No H2 o `SELECT` de releitura custa **+22,3%** sobre o
`UPDATE` sozinho — só o custo de executar o statement, sem rede. Num Postgres
real é um round trip inteiro; contra os 1,308 ms/conclusão do braço `RETURNING`
a 8 nós, seria da ordem de **+45%**.

**Conserto.** Método no `JdbcDialect` (ADR-0023) com o `UPDATE`+`SELECT` como
default ANSI e override em `PostgresJdbcDialect` (`RETURNING`) e
`SqlServerJdbcDialect` (`OUTPUT INSERTED.*`).

**Gatilho.** Confirmar os +45% no Postgres primeiro — quinto braço do
`BatchCounterWriteCostHarness`, na transação real. Sem esse número, é
generalização prematura; com ele, é a coisa certa em dois dos quatro dialetos.

---

## 6. Contenção da linha do lote no ponto de operação real

**O que é.** A transação de conclusão segura, até o commit, um segundo row lock
— numa **única** linha compartilhada por todos os membros do lote. Os
1,3 ms/conclusão da ADR foram medidos com **8** escritores. O ponto de operação
do `BASELINE.md` é `dispatch=1024`: com um lote de 1.000 membros em voo, a ordem
de grandeza de escritores disputando aquela linha é centenas.

**Por que importa.** Contenção em linha única não escala linearmente — cresce
com a fila de espera. Não é motivo para separar o incremento do CAS (a transação
compartilhada é o mecanismo de correção), é motivo para **saber o número**.

**Como medir.** `BatchCounterWriteCostHarness` com
`NODE_COUNTS = {8, 32, 128, 512}` e a transação completa. O que interessa é onde
a mediana por conclusão deixa de ser plana. Durante a corrida, amostrar
`pg_stat_activity` e `pg_locks` filtrando `mohs_batches`.

**Gatilho.** Antes do GA, ou antes de qualquer promessa pública de throughput
com lotes.

---

## 7. `idx_mohs_executions_batch_id` não tem leitor

**O que é.** Nenhuma query em `src/main` lê `mohs_executions` por `batch_id`.
`GET /batches/{id}` responde por seek de PK em `mohs_batches` — é o ponto
inteiro do contador mantido. O índice existe só ao lado da FK.

**Custo.** É uma árvore a mais na tabela mais quente do sistema, mantida em toda
inserção, e quase toda execução tem `batch_id IS NULL` — que o btree do Postgres
armazena.

**Conserto proposto.** Índice parcial `WHERE batch_id IS NOT NULL` no Postgres e
no SQL Server (mesmo padrão do índice de claim). H2 e MySQL ficam como estão —
nenhum tem índice parcial, e o InnoDB recria o índice da coluna-filha da FK
sozinho de qualquer forma.

**Gatilho.** Medir o imposto no contrafactual certo (cheio vs. parcial vs.
ausente) antes de mexer — o prior de 6-7% que circula vem de uma medição
**retratada**, feita para outra pergunta. Se o dashboard ganhar uma rota
"execuções deste lote", o índice passa a ter leitor e o item morre.

---

## 8. O `name` do lote é exigido e descartado

`Mohs.batch(String name, ...)` faz `requireNonNull` no `name` e nunca o usa:
`mohs_batches` não tem a coluna, `BatchStore.insert(batchId, total)` não o
recebe, `BatchSnapshot`/`BatchResponse` não o devolvem, e não há nem um log.
Hoje `batch("folha-de-pagamento", ...)` e `batch("x", ...)` são indistinguíveis
em qualquer lugar do sistema — a API promete rastreabilidade que não existe.

**Saídas.** Coluna nova (esbarra no item 10 de `docs/PENDENCIAS.md`), um
`log.info` na criação (forense barata, resolve 80% do valor), ou remover o
parâmetro (quebra de API pública).

**Gatilho.** Antes do GA — parâmetro público que não faz nada é dívida que
cresce com cada usuário.

---

## 9. Exatamente-uma-vez demonstrado só em H2

`JdbcBatchStoreTest.exactlyOneConcurrentCompletionSeesTheBatchClose` tem a
estrutura certa — transação por conclusão, 100 virtual threads, sem `sleep` —
mas roda em H2, que não é dialeto de produção. A própria ADR-0043 registra que
"a propriedade de exatamente-uma-vez está argumentada, não demonstrada por
medição — demonstrá-la é tarefa da implementação".

**Conserto.** Variante Postgres via Testcontainers, no mesmo molde dos
`Schema*RoundTripTest`. É a mesma lição que a sessão de 2026-08-19 aprendeu com
o rate limit: caminho crítico validado só em H2 é promessa sem prova.

**Gatilho.** Fazer junto do próximo item de lote que tocar o `JdbcBatchStore`.

---

## 10. Retry manual de membro de lote é recusado

**Decisão atual** (`3cef5cb`): recusa explícita, com mensagem que ensina a
reagendar o job avulso. A alternativa — decremento simétrico — reabriria o lote
e faria `BatchCompleted` deixar de ser terminal; pior, o segundo evento não
encontraria o callback one-shot, e quem salvou o membro é justamente quem
ficaria sem a notificação do fim real.

**Gatilho para revisitar.** Demanda real de operador por resgatar membro
individual. Se vier, o conserto certo é a A completa — decremento simétrico,
registro de callbacks deixando de ser one-shot, e `BatchCompleted` documentado
como repetível — e isso pede ADR própria, porque muda o que a ADR-0028 diz que o
evento significa. Não fazer pela metade.

---

## A lição transversal

Os quatro modos de falha corrigidos no pipeline eram **o mesmo erro**: a
fronteira escolhida foi *"quem chama `complete()`"*, quando a fronteira real é
*"quem escreve estado terminal em `mohs_executions`"*.

Não há nada hoje que force essa regra — só revisão. Um teste ArchUnit, ou um
teste que varra `src/main` por escritas de `SUCCEEDED`/`FAILED`/`CANCELLED`
fora dos caminhos que contam, faria a regra valer para o próximo caminho que
nascer. **É a maior alavanca desta lista**, porque protege contra a classe
inteira em vez de contra os quatro casos conhecidos — e porque a premissa que
dispensou a varredura de reconciliação (ADR-0043, Consequences) depende dela
para continuar verdadeira.
