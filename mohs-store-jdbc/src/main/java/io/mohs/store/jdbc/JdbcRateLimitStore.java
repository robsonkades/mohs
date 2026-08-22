package io.mohs.store.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import io.mohs.core.RateLimitSnapshot;
import io.mohs.core.resource.RateLimit;
import io.mohs.engine.RateLimitStore;

/** {@link RateLimitStore} sobre {@code mohs_rate_limits} (Data Mapper, PoEAA), incluindo o balde de tokens da ADR-0042. */
public final class JdbcRateLimitStore implements RateLimitStore {

    private static final Logger log = LoggerFactory.getLogger(JdbcRateLimitStore.class);

    /**
     * Teto da espera pelo lock da linha do balde (hoje só o {@code UPDATE}
     * de {@link #charge} a segura, e só até o commit) — a ÚNICA espera
     * incondicional do caminho de claim, que até a ADR-0042 nunca bloqueava
     * (o {@code SKIP LOCKED}/{@code READPAST} da seleção de candidatos pula
     * o que está travado, nunca aguarda). Sem teto, um nó travado segurando
     * a linha atrasa o tick dos OUTROS — e o tick é quem bate heartbeat e
     * renova lease (ADR-0012), então a contenção viraria falso positivo de
     * morte e o reaper reivindicaria execuções ainda rodando: duplicação de
     * trabalho causada justamente pelo mecanismo que existe pra proteger o
     * recurso externo. Estourado o teto, sai {@code QueryTimeoutException}
     * — MEDIDO em H2 (SQLState 50200, 2013ms) e Postgres 18 (57014, 2022ms),
     * não {@code CannotAcquireLockException}: o tradutor do Spring manda
     * timeout de statement e deadlock para ramos IRMÃOS da hierarquia. Por
     * isso {@code JdbcClaimer#claimIdsWithDeadlockRetry} captura os dois
     * explicitamente — perde-se a rodada, nunca o heartbeat.
     *
     * <p>Dois segundos assumem {@code lease-ttl} folgado (30s no default): a
     * espera precisa caber com sobra dentro do TTL, senão o teto que protege
     * o heartbeat vira o que o consome. {@code lease-ttl} abaixo de ~10s
     * pede revisão deste valor.
     */
    static final Duration BUCKET_LOCK_TIMEOUT = Duration.ofSeconds(2);

    /**
     * Tentativas do CAS de {@link #charge} antes de devolver o fracasso pro
     * chamador. Três porque o custo assimétrico manda: cada tentativa custa
     * duas idas ao banco, e desistir custa a rodada inteira de claim (o CAS
     * de até {@code batchSize} execuções). Colisão exige dois nós cobrando o
     * MESMO limite na mesma janela de microssegundos entre leitura e
     * escrita — três tentativas cobrem folgado o que não é contenção
     * patológica; se for, a rodada perdida é o sinal certo.
     */
    private static final int MAX_CHARGE_ATTEMPTS = 3;

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final Clock clock;
    private final Set<String> unknownLimitsAlreadyWarned = ConcurrentHashMap.newKeySet();

    public JdbcRateLimitStore(DataSource dataSource, Clock clock) {
        // template próprio, não JdbcSupport.namedTemplateWithStreamFetchSize:
        // é o único store que precisa de teto de tempo (ver BUCKET_LOCK_TIMEOUT).
        // O fetch size da convenção continua — findAll devolve Stream.
        JdbcTemplate template = new JdbcTemplate(Objects.requireNonNull(dataSource, "dataSource"));
        template.setFetchSize(JdbcSupport.STREAM_FETCH_SIZE);
        template.setQueryTimeout((int) BUCKET_LOCK_TIMEOUT.toSeconds());
        this.jdbcTemplate = new NamedParameterJdbcTemplate(template);
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * O UPDATE toca só a spec e clampa o saldo ao novo teto; o INSERT nasce
     * com o balde CHEIO — um limite recém-declarado não tem histórico de
     * consumo para cobrar, e começar vazio faria o primeiro job esperar uma
     * janela inteira por um limite que nunca foi excedido.
     */
    @Override
    public RateLimit upsert(RateLimit rateLimit) {
        Objects.requireNonNull(rateLimit, "rateLimit");
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("name", rateLimit.name())
                .addValue("maxCount", rateLimit.max())
                .addValue("windowDuration", rateLimit.window().toString())
                .addValue("refilledAt", JdbcTimestamps.toUtcLocalDateTime(clock.instant()));

        // ver CONC-2 em JdbcJobStore.upsert — mesma corrida, mesma correção.
        int updated = jdbcTemplate.update(UPDATE_SPEC, params);
        if (updated == 0) {
            try {
                jdbcTemplate.update(INSERT_FULL_BUCKET, params);
            } catch (DuplicateKeyException _) {
                jdbcTemplate.update(UPDATE_SPEC, params);
            }
        }
        return rateLimit;
    }

    private static final String UPDATE_SPEC = """
            UPDATE mohs_rate_limits
               SET max_count = :maxCount, window_duration = :windowDuration,
                   tokens = CASE WHEN tokens > :maxCount THEN :maxCount ELSE tokens END
             WHERE name = :name
            """;

    private static final String INSERT_FULL_BUCKET = """
            INSERT INTO mohs_rate_limits (name, max_count, window_duration, tokens, refilled_at)
            VALUES (:name, :maxCount, :windowDuration, :maxCount, :refilledAt)
            """;

    @Override
    public Optional<RateLimitSnapshot> find(String name) {
        Objects.requireNonNull(name, "name");
        Instant now = clock.instant();
        return JdbcSupport.findOne(jdbcTemplate,
                "SELECT * FROM mohs_rate_limits WHERE name = :name",
                new MapSqlParameterSource("name", name),
                rs -> toSnapshot(rs, now));
    }

    @Override
    public Stream<RateLimitSnapshot> findAll() {
        Instant now = clock.instant();
        return jdbcTemplate.queryForStream("SELECT * FROM mohs_rate_limits", new MapSqlParameterSource(),
                (rs, _) -> toSnapshot(rs, now));
    }

    /**
     * O refill é aplicado em MEMÓRIA na leitura: a linha só guarda o saldo
     * do último consumo, e mostrar esse número cru faria o dashboard exibir
     * um balde vazio muito depois de ele ter se recomposto. Escrever aqui
     * seria pior — transformaria leitura de monitoramento em disputa pelo
     * lock do caminho quente de claim.
     */
    private static RateLimitSnapshot toSnapshot(ResultSet rs, Instant now) throws SQLException {
        RateLimit rateLimit = mapRow(rs);
        // clamp SÓ aqui: uma linha adulterada (tokens > max_count) não pode
        // derrubar o GET /rate-limits das outras — e nesta leitura o número
        // não guarda CAS nenhum, ao contrário do saldo cru de mapBucket.
        return new RateLimitSnapshot(rateLimit, Math.min(mapBucket(rs).refill(now).tokens(), rateLimit.max()));
    }

    /** Leitura sem lock nenhum: fase 1 não pode custar serialização, é justamente o que a revisão da ADR-0042 comprou. */
    @Override
    public int available(String name, Instant now) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(now, "now");
        Optional<Bucket> stored = readBucket(name);
        if (stored.isEmpty()) {
            warnOnceAboutUnknownLimit(name);
            return 0;
        }
        return stored.get().refill(now).tokens();
    }

    /**
     * CAS sobre o par {@code (tokens, refilled_at)} — atômico por
     * construção, como todo {@code UPDATE} guardado do motor (ADR-0018), e
     * não por lock especializado. Refill e cobrança viajam no MESMO
     * statement: aplicar o refill separado abriria a janela para outro nó
     * cobrar em cima de um saldo já recomposto, que é sobre-entrega.
     *
     * <p>Retry interno em vez de derrubar a rodada na primeira colisão: um
     * CAS perdido custa duas idas ao banco, enquanto desfazer a rodada joga
     * fora o CAS de até {@code batchSize} execuções. Só depois de
     * {@link #MAX_CHARGE_ATTEMPTS} o chamador é obrigado a desfazer.
     */
    @Override
    public boolean charge(String name, int permits, Instant now) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(now, "now");
        if (permits < 1) {
            throw new IllegalArgumentException("permits must be at least 1, got " + permits);
        }

        for (int attempt = 0; attempt < MAX_CHARGE_ATTEMPTS; attempt++) {
            Optional<Bucket> stored = readBucket(name);
            if (stored.isEmpty()) {
                warnOnceAboutUnknownLimit(name);
                return false;
            }
            Bucket expected = stored.get();
            Bucket refilled = expected.refill(now);
            if (refilled.tokens() < permits) {
                return false;
            }
            if (chargeIfUnchanged(name, expected, refilled, permits)) {
                return true;
            }
        }
        return false;
    }

    /**
     * {@code false} quando a linha já não é mais {@code expected}: outro nó
     * cobrou — ou um {@code PATCH} mudou a spec — entre a leitura e este
     * {@code UPDATE}. A guarda cobre as QUATRO colunas que entraram no
     * cálculo, não só as duas que ele escreve: {@code max_count} e
     * {@code window_duration} definem o intervalo de refill, e um PATCH que
     * alargasse a janela no meio da rodada faria o saldo ser recomposto na
     * taxa velha — burst silencioso acima do limite novo.
     */
    private boolean chargeIfUnchanged(String name, Bucket expected, Bucket refilled, int permits) {
        return jdbcTemplate.update(CHARGE_GUARDED_BY_CAS, new MapSqlParameterSource("name", name)
                .addValue("tokens", refilled.tokens() - permits)
                .addValue("refilledAt", JdbcTimestamps.toUtcLocalDateTime(refilled.refilledAt()))
                .addValue("expectedTokens", expected.tokens())
                .addValue("expectedRefilledAt", JdbcTimestamps.toUtcLocalDateTime(expected.refilledAt()))
                .addValue("expectedMax", expected.max())
                .addValue("expectedWindow", expected.windowText())) == 1;
    }

    private static final String CHARGE_GUARDED_BY_CAS = """
            UPDATE mohs_rate_limits
               SET tokens = :tokens, refilled_at = :refilledAt
             WHERE name = :name AND tokens = :expectedTokens AND refilled_at = :expectedRefilledAt
               AND max_count = :expectedMax AND window_duration = :expectedWindow
            """;

    private Optional<Bucket> readBucket(String name) {
        return JdbcSupport.findOne(jdbcTemplate,
                "SELECT max_count, window_duration, tokens, refilled_at FROM mohs_rate_limits WHERE name = :name",
                new MapSqlParameterSource("name", name),
                JdbcRateLimitStore::mapBucket);
    }

    /**
     * Uma vez por nome, não uma por rodada: {@link #available} roda a cada
     * claim (20×/s por nó no ponto de operação de {@code poll=50ms}), e um
     * único job com o nome errado transformaria o WARN que deveria chamar
     * atenção às 3h na enchente que esconde todo o resto do log. O conjunto
     * é limitado pelos nomes realmente referenciados por jobs — não cresce
     * com o tráfego.
     */
    private void warnOnceAboutUnknownLimit(String name) {
        if (unknownLimitsAlreadyWarned.add(name)) {
            log.warn("job references unknown rate limit '{}' — granting nothing (fail-safe) until it is declared "
                    + "with mohs.rate-limits.{}.max/.window or a @Bean RateLimit", name, name);
        }
    }

    private static RateLimit mapRow(ResultSet rs) throws SQLException {
        return new RateLimit(rs.getString("name"), rs.getInt("max_count"), Duration.parse(rs.getString("window_duration")));
    }

    /**
     * A fronteira de desserialização também é fronteira de confiança: a
     * invariante de {@link RateLimit} protege quem ESCREVE (boot, {@code
     * @Bean}, PATCH), e o {@code ResultSet} não passa por lá. Uma linha
     * editada à mão — cenário real, já que a ADR-0042 manda o operador rodar
     * DDL manual nesta tabela no upgrade — com {@code max_count} maior que a
     * janela em nanos ressuscitaria a divisão por zero do refill, derrubando
     * a rodada de claim inteira. Aqui isso vira erro que diz o que fazer, e
     * o saldo é clampado para uma linha ruim não cegar o {@code GET
     * /rate-limits} das demais.
     */
    private static Bucket mapBucket(ResultSet rs) throws SQLException {
        int max = rs.getInt("max_count");
        Duration window = Duration.parse(rs.getString("window_duration"));
        try {
            RateLimit.requireRefillable(max, window);
        } catch (IllegalArgumentException notRefillable) {
            throw new IllegalStateException("rate limit row (max_count=" + max + ", window_duration=" + window
                    + ") cannot issue tokens — it was not written by Mohs; fix the row", notRefillable);
        }
        // saldo CRU, sem clamp: o valor lido aqui vira o `expectedTokens` do
        // CAS, e clampar em memória cegaria a guarda — numa linha com
        // tokens > max_count o predicado nunca casaria e `charge` falharia
        // para sempre, derrubando toda rodada que tocasse o limite. Quem
        // clampa é a leitura de dashboard (`toSnapshot`), que não guarda nada.
        // windowText é o texto CRU da linha, não window.toString(): o CAS compara
        // texto, e Duration.parse é tolerante enquanto toString é canônico
        // ('PT60S' escrito à mão volta como 'PT1M'). Re-serializar aqui faria o
        // predicado nunca casar numa linha editada fora do Mohs — o limite
        // ficaria incobrável para sempre, em silêncio.
        return new Bucket(max, window, rs.getString("window_duration"), rs.getInt("tokens"),
                JdbcTimestamps.fromUtcLocalDateTime(rs.getObject("refilled_at", LocalDateTime.class)));
    }

    /**
     * O balde da ADR-0042: um token a cada {@code window / max}, capacidade
     * {@code max}.
     */
    private record Bucket(int max, Duration window, String windowText, int tokens, Instant refilledAt) {

        /**
         * O tempo decorrido convertido em tokens INTEIROS, com
         * {@code refilledAt} avançando pelo que foi convertido — nunca para
         * "agora". Guardar a fração pendente é o que impede o balde de
         * entregar menos que {@code max} por janela para sempre (a cada
         * chamada se perderia o resto da divisão). Relógio que anda para
         * trás dá {@code earned <= 0} e não move nada: NTP step para o
         * passado atrasa a liberação, jamais libera dobrado.
         */
        Bucket refill(Instant now) {
            if (tokens > max) {
                // linha adulterada: o saldo cru continua sendo o `expected` do
                // CAS (quem guarda é `expected`, não este retorno), mas o que
                // se pode GASTAR nunca passa da capacidade — sub-entregar é o
                // erro seguro, sobre-entregar é a violação.
                return new Bucket(max, window, windowText, max, now);
            }
            Duration perToken = window.dividedBy(max);
            long earned = Duration.between(refilledAt, now).dividedBy(perToken);
            if (earned <= 0) {
                return this;
            }
            if (earned >= max - tokens) {
                // balde cheio não tem fração pendente a preservar — e o atalho
                // evita multiplicar uma duração por um número de tokens
                // arbitrariamente grande (linha parada desde o boot anterior).
                return new Bucket(max, window, windowText, max, now);
            }
            return new Bucket(max, window, windowText, tokens + (int) earned, refilledAt.plus(perToken.multipliedBy(earned)));
        }
    }
}
