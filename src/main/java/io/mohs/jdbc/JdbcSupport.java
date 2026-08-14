package io.mohs.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Convenções JDBC compartilhadas entre os stores de {@code io.mohs.jdbc} —
 * mesmo padrão idiomático já usado em {@code io.mohs.cron}
 * ({@code Assert}/{@code StringUtils}: classe final package-private,
 * métodos estáticos.
 */
final class JdbcSupport {

    private JdbcSupport() {
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
