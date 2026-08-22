package io.mohs.store.jdbc.dialect;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Uma linha reivindicada da fila da Phase 5 ({@code mohs_ready} →
 * {@code mohs_lease}, §5.4 do redesign) — identidade e o attempt que a
 * entrada vira, nunca payload (o dispatcher segue com UMA leitura em lote
 * na história). Par de {@link Candidate}, que é a forma da era da tabela
 * única e morre com ela no S5.4 (PLAN.md).
 */
public record ClaimedReady(String executionId, String jobKey, int attempt, int priority) {

    /** Linha de {@code mohs_ready} (colunas {@code attempt}/{@code priority}) — mapeamento único dos quatro dialetos: desde o review do S5.2, o statement único do Postgres também devolve do SELECT ordenado sobre {@code picked}, não do {@code RETURNING} da lease. */
    static ClaimedReady fromReadyRow(ResultSet rs, int rowNum) throws SQLException {
        return new ClaimedReady(rs.getString("execution_id"), rs.getString("job_key"), rs.getInt("attempt"), rs.getInt("priority"));
    }
}
