-- ADR-0058: o particionamento semanal do Postgres sai. Ele era a única
-- divergência estrutural entre o Tier 1 e os Tier 2/3 (MySQL, SQL Server e
-- H2 sempre foram tabelas planas), custava uma classe de produção com modo
-- de falha próprio (create-ahead só no boot) e comprava um benefício que
-- ainda não existe — retenção por DROP de partição era da Phase 8.
--
-- Não há ALTER que converta uma tabela particionada em normal: a tabela é
-- recriada e os dados COPIADOS, e é por isso que esta é a única migração do
-- projeto que move linhas. A ordem importa — selar, criar a nova, copiar,
-- dropar a antiga (o DROP leva as partições junto) e só então renomear,
-- para que uma falha no meio deixe a antiga intacta. O Flyway roda cada
-- migração em UMA transação e o Postgres tem DDL transacional: falha depois
-- do DROP faz ROLLBACK da conversão inteira (verificado) — não existe
-- schema meio-convertido.
--
-- O LOCK vem ANTES da cópia, e é o que separa esta migração de uma perda
-- silenciosa de dados: sem ele, um OUTRO nó ainda no ar consegue commitar
-- um INSERT entre o snapshot do INSERT..SELECT e o DROP, e essa linha morre
-- junto com a tabela antiga — com a migração reportando sucesso (reproduzido
-- em PG 18: 1000 linhas copiadas, 1 concorrente commitada, 1000 no final).
-- Selando antes, o escritor concorrente BLOQUEIA em vez de perder a escrita.
-- O lock_timeout é o par obrigatório: um pedido de AccessExclusiveLock que
-- entra na fila bloqueia todo mundo atrás dele, leitura inclusive — melhor a
-- migração falhar rápido e visível (e ser repetida) do que virar outage da
-- tabela de história atrás de uma transação longa qualquer.
--
-- Guardada por "é particionada?" via to_regclass (resolve pelo search_path
-- do DDL, ao contrário de relname, que confunde schemas homônimos): quem tem
-- o schema aplicado por fora (mohs.jdbc.migrate=false) já chega aqui com
-- tabelas normais, e nesse caso copiar milhões de linhas para chegar ao
-- mesmo lugar seria caro e inútil.
--
-- CUSTO OPERACIONAL, e ele não é pequeno: a história inteira é copiada
-- dentro de uma transação. Pico de espaço = 2x a maior das duas tabelas
-- (mais os índices), WAL proporcional, e a tabela fica SELADA (nem leitura)
-- do LOCK até o COMMIT. Numa base grande isto é janela de manutenção, não
-- deploy de rotina — ver as consequências na ADR-0058.
--
-- As PKs são normalizadas AQUI, na mesma janela. A coluna de tempo à frente
-- era exigência do particionamento, e sem ele a medição diz que a PK não
-- serve consulta nenhuma: o planner escolhe idx_mohs_execution_id e rebaixa
-- created_at a Filter, e os dois UPDATEs terminais (com e sem created_at) dão
-- o mesmo plano — 4 buffers, 0,047 ms. Era um índice de duas colunas mantido
-- em toda escrita só pela unicidade.
--
-- O momento não é escolha: a parte cara — recriar a tabela e copiar a
-- história — esta migração já está pagando. Normalizar junto custa ~zero;
-- normalizar depois custa uma V6 que copia tudo de novo, com a mesma janela
-- de indisponibilidade, e aí numa base de produção.
--
-- MUDA UMA GARANTIA, e é a única parte que não é neutra: antes o schema
-- permitia dois mohs_execution com o mesmo id e created_at diferentes (nunca
-- produzidos pelo código, mas possíveis pelo schema); agora o id é único de
-- verdade. É por isso que a cópia abaixo não pode ser um INSERT cego — uma
-- base que de alguma forma contenha essa duplicata falharia no meio da
-- migração, sem dizer o porquê. A checagem vem antes e nomeia o problema.
--
-- Com PK (execution_id) e (execution_id, number), idx_mohs_execution_id e
-- idx_mohs_attempt_exec deixam de ser criados: viraram prefixo exato da PK.
-- O schema passa a ser IDÊNTICO ao dos outros três dialetos, que sempre
-- tiveram PK natural.

DO $$
DECLARE
    constraint_name       text;
    duplicate_id          text;
    execution_partitioned boolean := EXISTS (SELECT 1 FROM pg_partitioned_table WHERE partrelid = to_regclass('mohs_execution'));
    attempt_partitioned   boolean := EXISTS (SELECT 1 FROM pg_partitioned_table WHERE partrelid = to_regclass('mohs_attempt'));
BEGIN
    -- Nada a converter — schema aplicado por fora, V5 já rodada à mão, banco
    -- sem as tabelas — sai ANTES de exigir isolamento e ANTES de selar. Sem
    -- linha para copiar não há perda a evitar, e um no-op não pode derrubar o
    -- boot: nem por lock_timeout, nem por nível de transação. É o que faz o
    -- caminho recomendado da ADR-0058 (rodar à mão, boot passa por cima)
    -- realmente passar por cima.
    IF NOT (execution_partitioned OR attempt_partitioned) THEN
        RETURN;
    END IF;

    -- O selo depende de snapshot POR STATEMENT: sob REPEATABLE READ ou
    -- SERIALIZABLE o INSERT … SELECT enxerga o snapshot ANTERIOR ao LOCK (o
    -- Flyway já leu mohs_schema_history nesta transação) e o DROP leva junto
    -- tudo que foi commitado no meio — a mesma perda silenciosa que o LOCK
    -- existe para fechar. Não dá para trocar o nível aqui (PL/pgSQL recusa
    -- SET TRANSACTION ISOLATION LEVEL), e o nível vem do DataSource do HOST
    -- (spring.datasource.hikari.transaction-isolation), então a migração se
    -- recusa a rodar cega em vez de apostar no default.
    -- READ UNCOMMITTED entra junto porque o Postgres o EXECUTA como READ
    -- COMMITTED (snapshot por statement), que é a única propriedade de que a
    -- cópia depende — recusá-lo seria barrar uma sessão segura.
    IF current_setting('transaction_isolation') NOT IN ('read committed', 'read uncommitted') THEN
        RAISE EXCEPTION 'V5 exige transaction_isolation = read committed (atual: %). O DataSource do '
            'host está configurado com outro nível — sob ele esta migração PERDE, em silêncio, as '
            'linhas escritas por outros nós durante a cópia. Rode a V5 à mão numa sessão READ '
            'COMMITTED, ou remova a configuração durante o upgrade (ADR-0058).',
            current_setting('transaction_isolation');
    END IF;

    -- lock_timeout curto e as DUAS tabelas de uma vez, na ordem em que os
    -- escritores as tomam (JdbcLeaseStore conclui attempt → execution): travar
    -- na ordem inversa dá deadlock com a transação de conclusão, e travar
    -- mohs_attempt só depois deixaria a cópia de mohs_execution inteira como
    -- janela de perda. 2s e não 10s porque um pedido de ACCESS EXCLUSIVE
    -- enfileira TODA leitura atrás de si: se o lock não vem rápido, não vem.
    -- Chegou aqui, há conversão pela frente: o selo tem de COBRIR tudo o que
    -- a cópia vai tocar. Uma guarda que apenas PULASSE o lock com o par
    -- incompleto deixaria a cópia rodar destravada — a perda silenciosa de
    -- volta, e calada. Schema meio-existente falha alto.
    IF to_regclass('mohs_attempt') IS NULL OR to_regclass('mohs_execution') IS NULL THEN
        RAISE EXCEPTION 'V5: história particionada com o par de tabelas incompleto (mohs_execution=%, '
            'mohs_attempt=%) — schema inconsistente, migração abortada.',
            to_regclass('mohs_execution'), to_regclass('mohs_attempt');
    END IF;
    SET LOCAL lock_timeout = '2s';
    LOCK TABLE mohs_attempt, mohs_execution IN ACCESS EXCLUSIVE MODE;

    -- Depois do selo (ninguém mais escreve) e antes da cópia: a PK nova é mais
    -- estrita que a antiga, e uma violação de unicidade no meio do INSERT
    -- diria apenas "duplicate key". Aqui ela diz QUAL id e o que fazer.
    IF execution_partitioned THEN
        SELECT execution_id INTO duplicate_id
          FROM mohs_execution GROUP BY execution_id HAVING count(*) > 1 LIMIT 1;
        IF duplicate_id IS NOT NULL THEN
            RAISE EXCEPTION 'V5: mohs_execution tem mais de uma linha com execution_id = % — a PK '
                'antiga (created_at, execution_id) permitia isso e a nova, (execution_id), não. '
                'Resolva a duplicata (mantenha a linha correta) e rode a migração de novo.', duplicate_id;
        END IF;
    END IF;
    IF attempt_partitioned THEN
        SELECT execution_id INTO duplicate_id
          FROM mohs_attempt GROUP BY execution_id, number HAVING count(*) > 1 LIMIT 1;
        IF duplicate_id IS NOT NULL THEN
            RAISE EXCEPTION 'V5: mohs_attempt tem mais de uma linha com o mesmo (execution_id, number) '
                'para execution_id = % — a PK antiga incluía finished_at e a nova não. Resolva a '
                'duplicata e rode a migração de novo.', duplicate_id;
        END IF;
    END IF;

    IF execution_partitioned THEN

        CREATE TABLE mohs_execution_flat (
            execution_id    VARCHAR(255) NOT NULL,
            job_key         VARCHAR(255) NOT NULL,
            shard           SMALLINT     NOT NULL DEFAULT 0,
            priority        INT          NOT NULL DEFAULT 20,
            state           VARCHAR(20)  NOT NULL,
            scheduled_at    TIMESTAMPTZ  NOT NULL,
            created_at      TIMESTAMPTZ  NOT NULL,
            finished_at     TIMESTAMPTZ,
            actor           VARCHAR(255) NOT NULL,
            correlation_id  VARCHAR(255),
            idempotency_key VARCHAR(255),
            payload         TEXT         NOT NULL,
            payload_type    VARCHAR(500) NOT NULL,
            PRIMARY KEY (execution_id)
        );
        -- lista de colunas nos DOIS lados: a cópia não pode depender da ordem
        -- de declaração coincidir entre origem e destino
        INSERT INTO mohs_execution_flat (execution_id, job_key, shard, priority, state, scheduled_at,
                                         created_at, finished_at, actor, correlation_id, idempotency_key,
                                         payload, payload_type)
        SELECT execution_id, job_key, shard, priority, state, scheduled_at, created_at, finished_at,
               actor, correlation_id, idempotency_key, payload, payload_type
          FROM mohs_execution;
        DROP TABLE mohs_execution;
        ALTER TABLE mohs_execution_flat RENAME TO mohs_execution;
        -- RENAME TO move a tabela, NÃO as constraints dela: sem isto a PK
        -- fica mohs_execution_flat_pkey e, do PG 17 em diante, os NOT NULL
        -- (que passaram a ter entrada em pg_constraint) herdam o "_flat_"
        -- junto. Nome de constraint é contrato — aparece na mensagem de erro
        -- e no ALTER ... DROP CONSTRAINT — e o guardião estrutural do schema
        -- compara os dois caminhos de instalação. O laço cobre a família
        -- inteira; no PG <= 16 ele só encontra a PK, e o resultado é o mesmo.
        FOR constraint_name IN
            SELECT conname FROM pg_constraint
             WHERE conrelid = to_regclass('mohs_execution') AND strpos(conname, '_flat_') > 0
        LOOP
            EXECUTE format('ALTER TABLE mohs_execution RENAME CONSTRAINT %I TO %I',
                           constraint_name, replace(constraint_name, '_flat_', '_'));
        END LOOP;

        -- idx_mohs_execution_id não é recriado: virou a PK
        CREATE INDEX idx_mohs_execution_job  ON mohs_execution (job_key, execution_id DESC);
        CREATE INDEX idx_mohs_execution_corr ON mohs_execution (correlation_id)
            WHERE correlation_id IS NOT NULL;
        -- tabela recriada acorda com reltuples = -1 e zero estatística de
        -- coluna; o autoanalyze só passa no próximo naptime (até 60s), e é
        -- justamente o minuto seguinte ao deploy. ANALYZE roda dentro de
        -- transação (só VACUUM não roda) e custa uma fração da cópia.
        EXECUTE 'ANALYZE mohs_execution';
    END IF;

    IF attempt_partitioned THEN
        CREATE TABLE mohs_attempt_flat (
            execution_id VARCHAR(255) NOT NULL,
            number       INT          NOT NULL,
            node_id      VARCHAR(255) NOT NULL,
            started_at   TIMESTAMPTZ  NOT NULL,
            finished_at  TIMESTAMPTZ  NOT NULL,
            outcome      VARCHAR(20)  NOT NULL,
            error_type   VARCHAR(500),
            error        TEXT,
            PRIMARY KEY (execution_id, number)
        );
        INSERT INTO mohs_attempt_flat (execution_id, number, node_id, started_at, finished_at,
                                       outcome, error_type, error)
        SELECT execution_id, number, node_id, started_at, finished_at, outcome, error_type, error
          FROM mohs_attempt;
        DROP TABLE mohs_attempt;
        ALTER TABLE mohs_attempt_flat RENAME TO mohs_attempt;
        FOR constraint_name IN
            SELECT conname FROM pg_constraint
             WHERE conrelid = to_regclass('mohs_attempt') AND strpos(conname, '_flat_') > 0
        LOOP
            EXECUTE format('ALTER TABLE mohs_attempt RENAME CONSTRAINT %I TO %I',
                           constraint_name, replace(constraint_name, '_flat_', '_'));
        END LOOP;

        -- idx_mohs_attempt_exec não é recriado: (execution_id) é prefixo exato da PK
        CREATE INDEX idx_mohs_attempt_throughput ON mohs_attempt (finished_at, outcome);
        EXECUTE 'ANALYZE mohs_attempt';
    END IF;
END $$;
