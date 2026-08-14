package io.mohs.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Convenções JDBC compartilhadas entre os stores de {@code io.mohs.jdbc} —
 * mesmo padrão idiomático já usado em {@code io.mohs.cron}
 * ({@code Assert}/{@code StringUtils}: classe final package-private,
 * métodos estáticos.
 */
final class JdbcSupport {

    /**
     * DBTUNE-7: sem isto, {@code queryForStream} não é um cursor de verdade
     * nos drivers reais — pgjdbc materializa a consulta inteira em memória
     * no {@code executeQuery} sem {@code fetchSize > 0} (e ainda exige
     * {@code autoCommit = false} pra valer; Connector/J precisa da mesma
     * coisa, ou {@code useCursorFetch=true}). O valor é um meio-termo
     * (100-500 é a faixa recomendada) — nunca medido neste projeto, só
     * fecha a lacuna entre o Javadoc de {@code ExecutionStore#findAll} e o
     * que os drivers realmente fazem sem isto configurado.
     */
    private static final int STREAM_FETCH_SIZE = 200;

    private JdbcSupport() {
    }

    /** {@code NamedParameterJdbcTemplate} com {@link #STREAM_FETCH_SIZE} — todo store que tem método {@code queryForStream} usa este construtor, não {@code new NamedParameterJdbcTemplate(dataSource)} direto. */
    static NamedParameterJdbcTemplate namedTemplateWithStreamFetchSize(DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.setFetchSize(STREAM_FETCH_SIZE);
        return new NamedParameterJdbcTemplate(jdbcTemplate);
    }

    @FunctionalInterface
    interface SingleRowMapper<T> {
        T map(ResultSet rs) throws SQLException;
    }

    /**
     * Busca uma linha por uma condição que casa no máximo uma (chave
     * primária ou única) — {@code ResultSetExtractor} guardado por
     * {@code rs.next()} lê essa linha direto, sem passar por
     * {@code List}/{@code stream}/{@code findFirst}, e sem o risco de
     * {@code queryForObject} (que lança {@code EmptyResultDataAccessException}
     * em vez de devolver vazio quando não há linha nenhuma).
     */
    static <T> Optional<T> findOne(NamedParameterJdbcTemplate jdbcTemplate, String sql, MapSqlParameterSource params, SingleRowMapper<T> mapper) {
        T result = jdbcTemplate.query(sql, params, rs -> rs.next() ? mapper.map(rs) : null);
        return Optional.ofNullable(result);
    }
}
