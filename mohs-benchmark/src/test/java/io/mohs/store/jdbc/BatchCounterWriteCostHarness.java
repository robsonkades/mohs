package io.mohs.store.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Segunda rodada da decisão de contagem de lote, depois que o tamanho alvo
 * foi fixado em {@value #BATCH_MEMBERS} membros. Nesse tamanho o eixo de
 * leitura de {@link BatchCounterDesignHarness} morreu — derivado e mantido
 * empatam dentro do round trip (p95 de 0,623 contra 0,601 ms) — e sobraram
 * exatamente duas perguntas, que são as que este harness responde.
 *
 * <p><b>E) O imposto do índice.</b> O desenho DERIVADO exige
 * {@code (batch_id, state)}, e esse índice é mantido em TODA escrita de
 * {@code mohs_executions}, não só nas de membro de lote — a tabela mais
 * quente do sistema, a 4k/s. As transições de estado são não-HOT (o
 * {@code state} já participa de dois índices), então cada
 * {@code ENQUEUED -> RUNNING -> SUCCEEDED} paga manutenção nele também. É um
 * imposto sistêmico, contra o do MANTIDO, confinado aos membros.
 *
 * <p><b>F) O custo por conclusão, no escopo transacional real.</b> A rodada
 * anterior mediu o incremento em autocommit, o que segura o row lock por
 * microssegundos; a cláusula 4 da ADR-0003 obriga o incremento a viver na
 * MESMA transação da transição de estado, e é essa transação que segura o
 * lock até o commit. Aqui os quatro candidatos correm dentro dela:
 * <ul>
 * <li>{@code derived} — nada é incrementado; paga a sonda de conclusão.
 * <li>{@code blind} — {@code SET succeeded = succeeded + 1}, atômico e mudo:
 * não diz quem fechou o lote.
 * <li>{@code returning} — o mesmo incremento com {@code RETURNING}: um
 * statement, um row lock, e o valor pós-incremento volta na mesma ida, então
 * exatamente um chamador enxerga {@code succeeded + failed == total}. O banco
 * elege o vencedor sem CAS.
 * <li>{@code cas} — read-decide-write, o desenho do cadrix (ADR-0001 deles).
 * </ul>
 *
 * <p>Latência por conclusão, não vazão em saturação: a pergunta não é qual o
 * teto, e sim quanto cada candidato ACRESCENTA à conclusão no ponto de
 * operação. Cada nível repete {@value #REPETITIONS} vezes com {@code VACUUM}
 * entre as repetições — sem ele cada rodada herda o bloat da anterior — e o
 * resultado é mediana com [min-max], porque a rodada anterior variou 48%
 * entre execuções e um número sem dispersão é anedota, não medida.
 *
 * <p>Fora da suíte unitária de propósito. Sob demanda:
 * {@code mvn test -Dtest=BatchCounterWriteCostHarness}. Precisa do Docker.
 */
class BatchCounterWriteCostHarness {

    /** Tamanho alvo de lote decidido pelo autor — o eixo de leitura empata aqui. */
    private static final int BATCH_MEMBERS = 1_000;

    /** Histórico terminal fora de lote: sem ele o índice não trabalha e a sonda parece grátis. */
    private static final int BACKGROUND_ROWS = 500_000;

    private static final int[] NODE_COUNTS = {1, 2, 4, 8};

    private static final int REPETITIONS = 5;

    /** Bloco escrito por repetição do eixo E — grande o bastante para o custo do índice sair do ruído. */
    private static final int WRITE_BLOCK = 20_000;

    private static final String COMPOSITE_INDEX = "idx_bench_batch_state";

    private static final String LIVE_STATES = "'ENQUEUED', 'RUNNING', 'RETRY_WAITING'";

    private static final String COMPLETE_EXECUTION =
            "UPDATE mohs_executions SET state = 'SUCCEEDED' WHERE id = ?";

    private static final String COMPLETION_PROBE =
            "SELECT 1 FROM mohs_executions WHERE batch_id = ? AND state IN (" + LIVE_STATES + ") LIMIT 1";

    private static final String BLIND_INCREMENT =
            "UPDATE mohs_batches SET succeeded = succeeded + 1 WHERE id = ?";

    private static final String RETURNING_INCREMENT =
            "UPDATE mohs_batches SET succeeded = succeeded + 1 WHERE id = ? RETURNING total, succeeded, failed";

    private static final String READ_COUNTER = "SELECT succeeded FROM mohs_batches WHERE id = ?";

    private static final String CAS_INCREMENT =
            "UPDATE mohs_batches SET succeeded = ? WHERE id = ? AND succeeded = ?";

    private static final String TAX_ROW = "%-22s %26s %26s%n";

    private static final String COMPLETION_ROW = "%-6s %20s %20s %20s %20s%n";

    private DataSource dataSource;

    @Test
    void run() throws Exception {
        dataSource = PostgresTestSupport.freshSchema();
        seedJobDefinition();
        seedBackgroundNoise();

        try {
            indexMaintenanceTax();
            costPerCompletion();
        } finally {
            restoreSchemaIndexes();
        }
    }

    /**
     * O container é singleton por JVM e {@code freshSchema()} só faz
     * {@code TRUNCATE}, que não derruba índice: o composto sobrevivente mudaria
     * o plano — e o custo de escrita — de toda classe Postgres que rodasse
     * depois nesta JVM, em silêncio. Em {@code finally} porque índice criado
     * por teste é recurso alocado, e falha no meio é justamente quando ele
     * vazaria.
     */
    private void restoreSchemaIndexes() {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("DROP INDEX IF EXISTS " + COMPOSITE_INDEX);
        jdbc.execute("CREATE INDEX IF NOT EXISTS idx_mohs_executions_batch_id ON mohs_executions (batch_id)");
    }

    // ------------------------------------------------- E) imposto do índice

    /**
     * O que o desenho derivado cobra de quem nunca usou um lote: com o índice
     * composto presente, toda inserção e toda transição de estado da tabela
     * mais quente passa a mantê-lo.
     */
    private void indexMaintenanceTax() throws Exception {
        System.out.printf("%n=== E) Imposto de (batch_id, state) na tabela quente (blocos de %d) ===%n", WRITE_BLOCK);
        System.out.printf(TAX_ROW, "operação", "sem índice (linhas/s)", "com índice (linhas/s)");

        printTaxRow("INSERT", this::replaceBlock);
        printTaxRow("UPDATE de state", this::transitionBlock);
    }

    /** A mesma escrita medida sem e com o índice: a diferença entre as duas colunas É o imposto. */
    private void printTaxRow(String operation, TimedWrite write) throws Exception {
        System.out.printf(TAX_ROW, operation,
                repeatWrites(IndexPresence.ABSENT, write).asRowsPerSecond(),
                repeatWrites(IndexPresence.PRESENT, write).asRowsPerSecond());
    }

    /** O composto é a hipótese em avaliação, não o schema: ele é criado e derrubado a cada nível. */
    private enum IndexPresence {
        PRESENT, ABSENT
    }

    /** @return linhas/s por repetição. */
    private Spread repeatWrites(IndexPresence index, TimedWrite write) throws Exception {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("DROP INDEX IF EXISTS " + COMPOSITE_INDEX);
        if (index == IndexPresence.PRESENT) {
            jdbc.execute("CREATE INDEX " + COMPOSITE_INDEX + " ON mohs_executions (batch_id, state)");
        }
        double[] rowsPerSecond = new double[REPETITIONS];
        for (int i = 0; i < REPETITIONS; i++) {
            jdbc.execute("VACUUM mohs_executions");
            String prefix = blockPrefix(index, i);
            insertBlock(jdbc, prefix);
            long startedAt = System.nanoTime();
            write.applyTo(jdbc, prefix);
            rowsPerSecond[i] = Math.round(WRITE_BLOCK / secondsSince(startedAt));
            deleteBlock(jdbc, prefix);
        }
        return Spread.of(rowsPerSecond);
    }

    /**
     * A escrita cronometrada de uma repetição do eixo E. Quando o cronômetro
     * começa, o bloco de {@value #WRITE_BLOCK} linhas JÁ existe: {@link
     * #repeatWrites} semeia fora da medição porque o UPDATE precisa de linhas
     * para transicionar. Daí a forma do lado do INSERT — as chaves são as
     * mesmas, então ele apaga o bloco semeado antes de inseri-lo de novo, e
     * esse DELETE cai dentro do tempo medido.
     */
    @FunctionalInterface
    private interface TimedWrite {

        void applyTo(JdbcTemplate jdbc, String prefix);
    }

    /** Prefixo próprio por nível e repetição: blocos com a mesma chave colidiriam no re-INSERT. */
    private static String blockPrefix(IndexPresence index, int repetition) {
        return "tax-" + (index == IndexPresence.PRESENT ? "i" : "n") + "-" + repetition + "-";
    }

    /** @see TimedWrite */
    private void replaceBlock(JdbcTemplate jdbc, String prefix) {
        deleteBlock(jdbc, prefix);
        insertBlock(jdbc, prefix);
    }

    /** A transição é o que o motor faz a cada conclusão, e é não-HOT porque {@code state} é indexado. */
    private void transitionBlock(JdbcTemplate jdbc, String prefix) {
        jdbc.update("UPDATE mohs_executions SET state = 'SUCCEEDED' WHERE id LIKE ?", prefix + "%");
    }

    private void insertBlock(JdbcTemplate jdbc, String prefix) {
        jdbc.update("""
                INSERT INTO mohs_executions
                    (id, job_key, state, scheduled_at, actor, priority, payload, payload_type, created_at)
                SELECT ? || lpad(n::text, 9, '0'), 'bench-job', 'ENQUEUED', now(), 'bench', 20,
                       '{}', 'java.util.Map', now()
                FROM generate_series(1, ?) n
                """, prefix, WRITE_BLOCK);
    }

    private void deleteBlock(JdbcTemplate jdbc, String prefix) {
        jdbc.update("DELETE FROM mohs_executions WHERE id LIKE ?", prefix + "%");
    }

    // --------------------------------------------- F) custo por conclusão

    /**
     * Os quatro candidatos dentro da transação de conclusão real. O índice
     * composto fica PRESENTE o tempo todo aqui: é requisito do derivado, e
     * medi-lo sem ele mediria um desenho que não existe.
     */
    private void costPerCompletion() throws Exception {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("DROP INDEX IF EXISTS " + COMPOSITE_INDEX);
        jdbc.execute("CREATE INDEX " + COMPOSITE_INDEX + " ON mohs_executions (batch_id, state)");

        System.out.printf("%n=== F) Latência por conclusão, lote de %d membros (ms, p50 / mediana de %d repetições) ===%n",
                BATCH_MEMBERS, REPETITIONS);
        System.out.printf(COMPLETION_ROW, "nós", "derived (sonda)", "blind", "returning", "cas");
        for (int nodes : NODE_COUNTS) {
            System.out.printf(COMPLETION_ROW, nodes,
                    repeatCompletions(nodes, Candidate.DERIVED),
                    repeatCompletions(nodes, Candidate.BLIND),
                    repeatCompletions(nodes, Candidate.RETURNING),
                    repeatCompletions(nodes, Candidate.CAS));
        }
    }

    /** O que cada desenho faz com a linha do lote quando um membro conclui. */
    private enum Candidate {
        DERIVED, BLIND, RETURNING, CAS
    }

    /** @return p50 por conclusão, em ms, por repetição. */
    private String repeatCompletions(int nodes, Candidate candidate) throws Exception {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        double[] p50Millis = new double[REPETITIONS];
        for (int i = 0; i < REPETITIONS; i++) {
            jdbc.execute("VACUUM mohs_batches");
            String batchId = "batch-" + candidate + "-" + nodes + "-" + i;
            seedBatch(jdbc, batchId);
            p50Millis[i] = drainBatch(nodes, candidate, batchId);
        }
        return Spread.of(p50Millis).asMillis();
    }

    /**
     * Um lote novo por repetição: reusar o mesmo faria a segunda rodada
     * começar com os membros já terminais, e a sonda do derivado pararia de
     * achar membro vivo — que é justamente o trabalho que ela faz.
     */
    private void seedBatch(JdbcTemplate jdbc, String batchId) {
        jdbc.update("INSERT INTO mohs_batches (id, total, succeeded, failed, created_at) VALUES (?, ?, 0, 0, now())",
                batchId, BATCH_MEMBERS);
        jdbc.update("""
                INSERT INTO mohs_executions
                    (id, job_key, state, scheduled_at, actor, priority, batch_id, payload, payload_type, created_at)
                SELECT ? || '-' || lpad(n::text, 9, '0'), 'bench-job', 'RUNNING', now(), 'bench', 20, ?,
                       '{}', 'java.util.Map', now()
                FROM generate_series(1, ?) n
                """, batchId, batchId, BATCH_MEMBERS);
    }

    /** @return p50 da latência de conclusão, em ms. */
    private double drainBatch(int nodes, Candidate candidate, String batchId) throws Exception {
        AtomicInteger nextMember = new AtomicInteger(1);
        long[] timings = new long[BATCH_MEMBERS];
        CyclicBarrier start = new CyclicBarrier(nodes);
        List<Callable<Void>> workers = new ArrayList<>(nodes);
        for (int node = 0; node < nodes; node++) {
            workers.add(completeUntilDrained(candidate, batchId, start, nextMember, timings));
        }
        ExecutorService pool = Executors.newFixedThreadPool(nodes);
        try {
            for (Future<Void> future : pool.invokeAll(workers)) {
                future.get();
            }
        } finally {
            pool.shutdownNow();
        }
        Arrays.sort(timings);
        return timings[timings.length / 2] / 1_000_000.0;
    }

    /**
     * Um nó: pega o próximo membro ainda não concluído e cronometra a
     * conclusão dele, até o lote drenar. Os nós largam juntos (barrier) —
     * quem largasse antes concluiria sem disputa, o oposto do cenário.
     */
    private Callable<Void> completeUntilDrained(Candidate candidate, String batchId, CyclicBarrier start,
            AtomicInteger nextMember, long[] timings) {
        return () -> {
            try (Connection connection = dataSource.getConnection()) {
                connection.setAutoCommit(false);
                start.await(1, TimeUnit.MINUTES);
                int member;
                while ((member = nextMember.getAndIncrement()) <= BATCH_MEMBERS) {
                    long startedAt = System.nanoTime();
                    completeMember(connection, candidate, batchId, member);
                    timings[member - 1] = System.nanoTime() - startedAt;
                }
            }
            return null;
        };
    }

    /**
     * A transação de conclusão: transição de estado da execução mais o que
     * cada candidato faz com o lote, num commit só. Separar os dois seria o
     * dual-write que a cláusula 4 da ADR-0003 proíbe — e mediria um lock
     * segurado por microssegundos, que a produção nunca terá.
     */
    private void completeMember(Connection connection, Candidate candidate, String batchId, int member)
            throws SQLException {
        try {
            transitionToSucceeded(connection, memberId(batchId, member));
            // O veredito de conclusão que o derivado e o returning devolvem é
            // descartado de propósito: mede-se o custo de obtê-lo dentro da
            // transação, não o que o motor faria com ele depois.
            switch (candidate) {
                case DERIVED -> hasLiveMember(connection, batchId);
                case BLIND -> blindIncrement(connection, batchId);
                case RETURNING -> returningIncrement(connection, batchId);
                case CAS -> casIncrement(connection, batchId);
            }
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        }
    }

    /** A mesma formatação de id que o {@code lpad} de {@link #seedBatch} gera. */
    private static String memberId(String batchId, int member) {
        return "%s-%09d".formatted(batchId, member);
    }

    private void transitionToSucceeded(Connection connection, String memberId) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(COMPLETE_EXECUTION)) {
            ps.setString(1, memberId);
            ps.executeUpdate();
        }
    }

    /** @return {@code true} enquanto sobrar membro vivo — é assim que o derivado sabe que o lote NÃO fechou. */
    private boolean hasLiveMember(Connection connection, String batchId) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(COMPLETION_PROBE)) {
            ps.setString(1, batchId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    /** Soma sem olhar: uma ida, um lock, e nenhuma informação sobre quem fechou o lote. */
    private void blindIncrement(Connection connection, String batchId) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(BLIND_INCREMENT)) {
            ps.setString(1, batchId);
            ps.executeUpdate();
        }
    }

    /**
     * O mesmo incremento cego, com o valor pós-incremento de volta na mesma ida.
     *
     * @return {@code true} para o único chamador que enxerga
     * {@code succeeded + failed == total} — é ele que o banco elege para
     * fechar o lote, sem CAS.
     */
    private boolean returningIncrement(Connection connection, String batchId) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(RETURNING_INCREMENT)) {
            ps.setString(1, batchId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("lote inexistente: " + batchId);
                }
                return rs.getInt(2) + rs.getInt(3) == rs.getInt(1);
            }
        }
    }

    /**
     * Read-decide-write: a guarda sobre o valor lido também elege quem fecha o
     * lote, ao preço de duas idas e de reler quando outro nó chega na frente.
     */
    private void casIncrement(Connection connection, String batchId) throws SQLException {
        while (true) {
            int current = readCounter(connection, batchId);
            try (PreparedStatement ps = connection.prepareStatement(CAS_INCREMENT)) {
                ps.setInt(1, current + 1);
                ps.setString(2, batchId);
                ps.setInt(3, current);
                if (ps.executeUpdate() == 1) {
                    return;
                }
            }
        }
    }

    private int readCounter(Connection connection, String batchId) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(READ_COUNTER)) {
            ps.setString(1, batchId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("lote inexistente: " + batchId);
                }
                return rs.getInt(1);
            }
        }
    }

    // ------------------------------------------------------------- seed

    private void seedJobDefinition() {
        new JdbcTemplate(dataSource).execute("""
                INSERT INTO mohs_job_definitions
                    (id, job_key, handler_type, schedule_type, misfire, source, created_at, updated_at)
                VALUES ('j1', 'bench-job', 'Handler', 'ON_DEMAND', 'FIRE_NOW', 'PROGRAMMATIC', now(), now())
                """);
    }

    private void seedBackgroundNoise() {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        jdbc.update("""
                INSERT INTO mohs_executions
                    (id, job_key, state, scheduled_at, actor, priority, payload, payload_type, created_at)
                SELECT 'noise-' || lpad(n::text, 9, '0'), 'bench-job', 'SUCCEEDED', now(), 'bench', 20,
                       '{}', 'java.util.Map', now()
                FROM generate_series(1, ?) n
                """, BACKGROUND_ROWS);
        jdbc.execute("ANALYZE mohs_executions");
        jdbc.execute("ANALYZE mohs_batches");
    }

    // ------------------------------------------------------- utilitários

    private static double secondsSince(long startedAtNanos) {
        return (System.nanoTime() - startedAtNanos) / 1_000_000_000.0;
    }

    /**
     * Mediana com [min-max] das repetições de um nível. A rodada anterior
     * variou 48% entre execuções: sem a dispersão ao lado, a mediana sozinha
     * seria anedota.
     */
    private record Spread(double median, double min, double max) {

        static Spread of(double[] runs) {
            double[] sorted = runs.clone();
            Arrays.sort(sorted);
            return new Spread(sorted[sorted.length / 2], sorted[0], sorted[sorted.length - 1]);
        }

        String asRowsPerSecond() {
            return "%.0f [%.0f-%.0f]".formatted(median, min, max);
        }

        String asMillis() {
            return "%.3f [%.3f-%.3f]".formatted(median, min, max);
        }
    }
}
