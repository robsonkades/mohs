# Tipos e comparação

As quatro instalações usam a mesma semântica de dados. Os formatos anteriores ao primeiro release não são contratos de compatibilidade.

| Dado | Java | Persistência |
| --- | --- | --- |
| Instante operacional | `Instant`, convertido por `JdbcTimestamps` em `LocalDateTime` UTC | PostgreSQL/H2 `TIMESTAMP(6)`, MySQL `DATETIME(6)`, SQL Server `DATETIME2(6)` |
| Intervalo, timeout, janela de taxa | `Duration` com quantidade exata de microssegundos | `BIGINT`: `interval_micros`, `timeout_micros`, `window_micros` |
| Contadores de batch | `long` | `BIGINT`; `pending = total - succeeded - failed` |
| Chave técnica | String ASCII de 1 a 255 caracteres | Comparação exata: PostgreSQL `C`, MySQL `ascii_bin`, SQL Server `Latin1_General_100_BIN2`, H2 binária padrão |
| Identidade gerada | UUIDv7 canônico, hexadecimal minúsculo com hífens, exatamente 36 caracteres | `VARCHAR(36)` ou `NVARCHAR(36)` no SQL Server, com a mesma comparação exata |
| Contrato de entrada | `JobRef.inputContract`, independente de `payloadType` local | `input_contract`, chave técnica obrigatória de 255 caracteres na definição e no snapshot de execução |
| Catálogo de executáveis | `ExecutableCatalog`, imutável ao entrar no cluster | `mohs_catalog`: singleton e SHA-256 textual de 64 caracteres |
| Rótulo, ator, conteúdo do job | Texto Unicode | Tipos Unicode do banco, incluindo `NVARCHAR` no SQL Server |

Chaves técnicas aceitam apenas `[A-Za-z0-9._:/-]`. A biblioteca não remove espaços nem muda maiúsculas e minúsculas. `Alpha` e `alpha` identificam jobs distintos. Nomes de runners, janelas, limites de taxa, políticas de retry e chaves de idempotência seguem a mesma regra. O nome de exibição do job e o ator de auditoria continuam aceitando texto humano Unicode.

Identidades de execução, batch, sessão e aquisição seguem `GeneratedId`: UUIDv7 de 36 caracteres, incluindo versão e variante válidas, sem normalização. Identificadores de execução e cursores inválidos são recusados antes do acesso ao banco; a API HTTP responde 422. Um cursor ausente ou vazio inicia a primeira página. O prefixo temporal é o horário de geração da identidade, não uma promessa de igualdade com `created_at` quando o relógio da aplicação foi substituído. Chaves de idempotência fornecidas pelo usuário continuam sendo chaves técnicas, separadas dessas identidades.

Todos os instantes persistidos estão no intervalo comum de anos UTC 1000 a 9999. Um horário de elegibilidade que exige nanossegundos é arredondado **para cima** ao próximo microssegundo. O recibo, o histórico e a fila usam esse mesmo valor. Um instante observado usado em comparação é arredondado para baixo, para não tornar elegível um trabalho futuro. O fuso de um calendário cron continua explícito em `ZoneId`; o instante resultante é UTC.

Durações que exigem precisão menor que um microssegundo ou excedem um `long` de microssegundos são rejeitadas. Cada contrato define também se zero é permitido; intervalos e janelas de taxa precisam ser positivos. `null` no timeout significa ausência de timeout específico do job.

O limitador de taxa conserva a fração da recarga em `refill_remainder`. Após `elapsedMicros`, o crédito é `elapsedMicros * maxCount + remainder`; a divisão por `windowMicros` produz tokens inteiros e a nova fração. A atualização condicional verifica capacidade, janela, saldo, instante e fração juntos. Um balde cheio descarta crédito excedente. Uma mudança de configuração mantém o saldo, limitado à nova capacidade, e inicia a contagem fracionária no instante da alteração. Repetir a mesma configuração preserva a recarga em andamento. Configuração e cobrança usam transações próprias e CAS sobre toda a configuração observada; os novos valores são calculados no Java, sem depender da ordem de atribuições do SQL. O relógio do banco decide a recarga, e a API de cobrança não recebe um instante do chamador.

Os recibos de idempotência contêm `scheduled_at`, `actor` e `expires_at` próprios. `expires_at` é calculado na aceitação com o relógio do banco e a duração configurada naquele momento. Em `now == expires_at`, o recibo expirou. Uma nova aceitação pode substituir a chave expirada dentro da mesma transação que grava seu histórico. A limpeza física não define a janela de deduplicação. Zero na configuração cria novos recibos sem expiração; não modifica prazos de recibos existentes. A leitura do recibo não depende da permanência do histórico.

SQL Server usa `COUNT_BIG` nas agregações e parâmetros JDBC `BIGINT` para limites. Isso alinha o tipo do parâmetro ao operador; a ausência de conversões prejudiciais em colunas indexadas precisa ser conferida nos planos reais, com a carga e a versão do banco usadas pela instalação.

Entradas persistidas são snapshots JSON identificados por contrato. `JobRef.of("charge", ChargeInput.class, "charge-input-v1")` declara o formato; o overload sem o terceiro argumento usa o ID do job como contrato. O nome da classe permanece no catálogo local. O JDBC não carrega classes nem decodifica JSON. A submissão valida a reconstrução antes de abrir a transação, e a aceitação compara o contrato com a definição sob lock. O dispatcher decodifica usando o tipo local e recusa entrada incompatível antes de executar o handler.

Renomear uma classe preservando o formato e o identificador de contrato não exige mudar o banco. Um contrato incompatível exige outro ID de job: a identidade retida não pode mudar de contrato, inclusive após retirada. A compatibilidade semântica de duas classes declaradas com o mesmo identificador é responsabilidade de quem publica os jobs; igualdade do identificador não prova compatibilidade de formatos arbitrários.

Recorrências nomeadas usam o contrato de seu executável, armazenado uma única vez na definição, junto do JSON de entrada automática. O disparo lê ambos na mesma transação que avança o trigger. Handlers anotados sem parâmetro usam `NoInput`; seu JSON é `{}`. Entradas `Map` declaradas explicitamente continuam possíveis. Não há nome de classe persistido para selecionar o decodificador.

A publicação e o ingresso de sessões seguem o [protocolo de catálogo](../04-engineering/executable-catalog.md). A retirada persiste entre reinícios; a reativação é uma operação explícita com validação do catálogo instalado.

O formato JSON é controlado por `InputCodec.JSON`, com rejeição de propriedades desconhecidas, tanto na leitura dos snapshots quanto na conversão HTTP. Não há decodificador tolerante separado para versões históricas. O mapper da aplicação não altera o formato persistido.

Os limites locais de polling, TTL da sessão e espera de encerramento também precisam caber em um `long` de nanossegundos, usado pelos deadlines monotônicos. Essa restrição operacional é validada antes de iniciar o ciclo ou mudar seu estado. Ela não reduz o intervalo representável das durações persistidas: a checagem de timeout compara `Duration`, sem converter um timeout muito longo para nanossegundos.


O driver Microsoft 13.4 ainda declara parâmetros temporais com escala 7, mesmo recebendo escala 6 no setObject. O teste de metadata reproduz isso; a normalização residual ocorre no parâmetro nos planos examinados. O schema permanece em seis casas. A causa, os limites da evidência e alternativas estão em [dialects.md](dialects.md#binding-precision-and-remaining-optimizer-conversions).
