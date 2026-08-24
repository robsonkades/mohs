# ADR-0058 — O particionamento semanal do Postgres sai

Data: 2026-08-23 · Status: aceita · Revisa: ADR-0052 (item 3), ADR-0050

## Contexto

A ADR-0052 fez `mohs_execution` e `mohs_attempt` nascerem particionadas por
semana no Postgres (`PARTITION BY RANGE` em `created_at`/`finished_at`, com
uma partição DEFAULT de backstop), e a ADR-0050 usou essa capacidade como
uma das justificativas do tiering — "particionamento declarativo" era o tipo
de coisa que só o Tier 1 poderia ter.

O que a operação mostrou desde então:

1. **É a única divergência estrutural entre os tiers.** MySQL, SQL Server e
   H2 sempre foram tabelas planas nas mesmas duas tabelas. O caminho plano,
   portanto, já é o caminho principal do produto — testado em três dos
   quatro dialetos — e o particionado é a exceção que custa uma classe de
   produção (`PostgresPartitionManager`), um bean condicional, um teste e
   quatro chamadas de setup espalhadas por testes de outros assuntos.
2. **O benefício principal ainda não existe.** Retenção por `DROP` de
   partição era item da Phase 8. Estamos pagando a complexidade hoje por um
   ganho que não foi construído.
3. **O mecanismo tem um modo de falha próprio, conhecido e aberto:** o
   create-ahead roda só no boot, então um nó com uptime maior que uma semana
   para de criar partições e passa a gravar na DEFAULT em silêncio. A
   pendência estava registrada com gatilho e nunca foi fechada.
4. **A medição não sustenta o custo.** O `OverviewLatencyScenario`
   (2026-08-23) rodou com 2M de linhas em `mohs_attempt` — todas na DEFAULT,
   sem pruning nenhum — e a contagem de throughput custou **1,6 ms**. O
   índice `(finished_at, outcome)` resolve o caso sozinho; o pruning seria
   segunda linha de defesa sobre um custo que já é constante na janela.

## Decisão

Remover o particionamento: as duas tabelas viram normais no Postgres, como
já são nos demais dialetos. Saem o `PARTITION BY RANGE`, as partições
DEFAULT, o `PostgresPartitionManager`, o bean da auto-config e o teste dele.
A migração `V5__drop_partitioning` converte bases existentes **preservando
dados** — não há `ALTER` que converta particionada em normal, então a tabela
é recriada e as linhas copiadas.

## Consequências

- **O checksum da `V3` muda, e isso quebra o boot de quem já a aplicou.**
  As duas linhas `CREATE TABLE … PARTITION OF … DEFAULT` da V3 tiveram de
  virar condicionais: quem aplica o schema por fora
  (`mohs.jdbc.migrate=false`) e liga o Flyway depois chega com as tabelas
  já normais, e `PARTITION OF` sobre pai não-particionado é erro duro — o
  `IF NOT EXISTS` não cobre, porque a queixa é sobre o pai. Verificado: com
  a V3 anterior, essa base morre no boot com `ERROR: "mohs_execution" is
  not partitioned`. Não existe correção que não mexa na V3, porque o
  statement que falha está dentro dela. Como `validateOnMigrate` é o
  default e `MohsFlyway` não expõe `repair()`, toda base com a V3 antiga
  precisa de um `UPDATE mohs_schema_history SET checksum = …` manual.
  **Aceito porque o projeto é pré-1.0 e o universo dessas bases é
  desenvolvimento** — o mesmo raciocínio da cadeia destrutiva registrada na
  ADR-0048. Alternativa levantada e NÃO adotada: chamar `repair()` antes de
  `migrate()` no `MohsFlyway`, que resolveria este caso e todos os futuros,
  ao custo de aceitar em silêncio qualquer edição de migração aplicada.

- **Os quatro dialetos passam a ter a mesma forma física** nas tabelas de
  história. A matriz de tuning encolhe e some a categoria de bug "só acontece
  no Tier 1".
- **A retenção futura fica mais cara, e essa é a conta desta decisão.**
  Apagar história vira `DELETE` em lote — bloat e vacuum — em vez de um
  `DROP` de partição O(1). É o que o Tier 2 já teria de fazer de qualquer
  forma, e a ADR-0032 já colocava retenção longa como problema do banco do
  host (CDC, particionamento próprio, backup). **Se a Phase 8 concluir que
  o `DROP` é necessário**, o caminho é reintroduzir o particionamento com
  create-ahead resolvido — e aí com um número que justifique, que é o que
  falta hoje.
- **Revisa o item 3 da ADR-0052** ("particionadas por semana no Postgres…
  Retenção futura = DROP de partição") e a menção a particionamento como
  capacidade Tier 1 na ADR-0050. As duas seguem válidas no resto.
- **O `findPage` sem filtro MELHORA, e a melhora escala** (medido, PG 18,
  500k linhas, cache quente): o plano sai de um `Merge Append` sobre 11
  `Index Scan Backward` — um índice ABERTO POR PARTIÇÃO — para um único
  `Index Scan Backward`. 293 → 257 buffers e 0,236 → 0,175 ms hoje, mas o
  que importa é a forma: o custo do `Merge Append` crescia linearmente com
  a retenção (11 partições aqui, 53 num ano); a tabela plana é O(1) em
  partições.
- **Confirmado que o pruning nunca ia ajudar a contagem de throughput:** o
  `Index Only Scan` sobre `(finished_at, outcome)` fecha com
  `Heap Fetches: 0` e custa as linhas DA JANELA — que cabe dentro de uma
  partição de qualquer forma.
- **CUSTO OPERACIONAL da V5, e é sério numa base grande:** a tabela fica
  selada do `LOCK TABLE` até o `COMMIT` — leitura inclusive, não só
  escrita —, o pico de espaço é 2× a maior tabela mais os índices, e o WAL
  é proporcional ao volume copiado. O `lock_timeout` limita a AQUISIÇÃO do
  lock, não a POSSE: se a cópia leva 8 minutos, são 8 minutos de história
  indisponível. **Em base grande isto é janela de manutenção, não deploy de
  rotina** — e como a migração é no-op sobre tabela já plana, o caminho
  recomendado é o operador rodar a V5 à mão quando quiser, deixando o boot
  seguinte passar por cima sem fazer nada.
- **O `LOCK TABLE` não é zelo:** sem ele, um escritor concorrente que
  commite entre o `INSERT … SELECT` e o `DROP` tem a linha destruída e a
  migração reporta sucesso. Reproduzido em PG 18 durante a revisão desta
  decisão. E o lock só basta sob **READ COMMITTED** — sob REPEATABLE READ
  o snapshot é anterior a ele e a perda volta, por isso a V5 verifica o
  nível e se recusa a rodar cega: o isolamento vem do `DataSource` do
  host, não nosso.
- **A tabela é RECRIADA, e não carrega junto o que não está no script:**
  `GRANT`s (qualquer role de BI/relatório com `SELECT` perde acesso —
  e o sintoma aparece no time de dados, não no log do Mohs), índices
  criados à mão pelo operador, comments, políticas RLS e triggers. O Mohs
  vive no schema do HOST; supor que ninguém encostou nas tabelas é a
  premissa que a ADR-0048 recusa. Antes de rodar em base compartilhada,
  capture (`\d+ mohs_execution`, `\dp mohs_execution`) e reaplique depois.
- **O que acontece com os OUTROS nós durante a cópia:** claim e reaper
  seguem (`mohs_ready`/`mohs_lease` não são travadas); `findPayloads` e as
  conclusões BLOQUEIAM — e bloqueiam **segurando conexão**. Numa base
  grande isso esgota o pool, e pool esgotado impede o HEARTBEAT: o nó vivo
  é reapado pelos pares e suas execuções em voo rodam DUPLICADAS. Nada
  corrompe (payload read é infra e a lease fica de pé, ADR-0047; conclusão
  perdida cai no fence), mas é a razão concreta de "janela de manutenção,
  não deploy de rotina" — **drene o cluster antes**.
- **Pendência com gatilho — as PKs continuam com a coluna de tempo à
  frente**, e a medição revisou o motivo: `mohs_execution
  (created_at, execution_id)` e `mohs_attempt (finished_at, execution_id,
  number)` têm essa forma porque o Postgres exigia a chave de partição na
  PK. Descoberto ao medir: **a PK não serve query nenhuma**. No
  `TERMINAL_UPDATE`, com igualdade nas duas colunas, o planner escolhe
  `idx_mohs_execution_id` e rebaixa `created_at` a `Filter`; os planos de
  `TERMINAL_UPDATE` e `TERMINAL_UPDATE_UNPRUNED` são idênticos (4 buffers,
  0,047 ms), ou seja a distinção pruned/unpruned virou vocabulário morto.
  A PK é hoje um índice de duas colunas mantido em toda escrita servindo
  só à unicidade. **Não foi normalizada nesta rodada** porque muda garantia
  (o schema deixaria de permitir dois `mohs_execution` com o mesmo id e
  `created_at` diferente — impossível na prática com UUIDv7, mas é o schema
  que fala) e toca `JdbcLeaseStore` e o scanner da ADR-0043. **Gatilho
  revisado:** a parte cara — recriar a tabela — é justamente o que a V5 já
  faz; se a normalização for aceita, o lugar barato é dentro dela, não numa
  V6 que copia tudo de novo.
