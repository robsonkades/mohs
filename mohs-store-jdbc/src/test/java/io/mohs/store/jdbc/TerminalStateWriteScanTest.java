package io.mohs.store.jdbc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Varredura de fonte que guarda um invariante da ADR-0043; mora neste módulo, e não no
 * {@code ArchitectureTest} de mohs-demo, porque varre {@code src/main/java} do módulo em
 * que roda — e todo SQL de escrita terminal em {@code mohs_executions} está aqui
 * ({@code JdbcExecutionStore}, {@code JdbcJobStore}).
 */
class TerminalStateWriteScanTest {

    /** Estados que encerram uma execução — escrevê-los é o gatilho da contagem de lote. */
    private static final Pattern TERMINAL_STATE_WRITE = Pattern.compile(
            "UPDATE\\s+mohs_executions\\s+SET\\s+state\\s*=\\s*(:newState|'SUCCEEDED'|'FAILED'|'CANCELLED')");

    /** O marcador que cada ponto de escrita terminal precisa carregar, com o porquê ao lado. */
    private static final String BATCH_COUNTED_MARKER = "batch-counted:";

    /**
     * A regra que a ADR-0043 precisava e não tinha: <b>quem escreve estado
     * terminal em {@code mohs_executions} conta no lote</b>. Quatro caminhos
     * violaram isso na implementação — reaper, cancelamento, aposentadoria de
     * job e retry manual — e os três primeiros deixavam o lote aberto para
     * sempre, sem erro e sem varredura de reconciliação que curasse (a ADR
     * dispensou a varredura justamente sob a premissa de que todo caminho
     * conta). Ver a errata da ADR-0043 e {@code docs/BATCH-ARCHITECTURE-REVIEW.md}.
     *
     * <p>O erro que a produziu foi de fronteira: procurou-se "quem chama
     * {@code complete()}" quando o que importa é "quem escreve estado
     * terminal". Esta regra protege a classe inteira, não os quatro casos
     * conhecidos — é o próximo caminho que nascer que ela existe para pegar.
     *
     * <p>Varredura de FONTE, não ArchUnit, e não por preguiça: o estado
     * terminal vive em literal de SQL, e {@code ArchUnit não lê SQL} — a mesma
     * lacuna que a regra {@code ids_are_generated_as_uuidv7_never_v4} do
     * {@code ArchitectureTest} registra ao deixar metade daquele invariante em
     * prosa. Aqui a metade que importa é justamente a do SQL, então a varredura
     * é o único instrumento que enxerga o invariante.
     *
     * <p>Marcador em vez de allowlist de propósito: uma lista de caminhos
     * aprovados envelhece em silêncio e não diz nada a quem lê o código. O
     * comentário obrigatório fica ao lado da escrita, nomeia quem conta, e
     * quem for adicionar um ponto novo é obrigado a responder a pergunta
     * antes de o teste passar.
     */
    @Test
    void every_terminal_state_write_declares_how_the_batch_is_counted() throws IOException {
        List<String> unmarked = new ArrayList<>();
        try (Stream<Path> sources = Files.walk(Path.of("src/main/java"))) {
            for (Path source : sources.filter(path -> path.toString().endsWith(".java")).toList()) {
                List<String> lines = Files.readAllLines(source);
                for (int line = 0; line < lines.size(); line++) {
                    if (TERMINAL_STATE_WRITE.matcher(lines.get(line)).find()
                            && !markedWithin(lines, line)) {
                        unmarked.add(source + ":" + (line + 1) + "  " + lines.get(line).strip());
                    }
                }
            }
        }

        assertThat(unmarked)
                .as("escritas de estado terminal em mohs_executions sem declarar a contagem de lote — "
                        + "acrescente '// " + BATCH_COUNTED_MARKER + " <quem conta>' acima, na MESMA transação, "
                        + "ou o lote desses membros nunca fecha (ADR-0043)")
                .isEmpty();
    }

    /**
     * Janela curta e para trás: o marcador tem que estar colado na escrita para
     * ser lido junto com ela. Longe demais, viraria carimbo que ninguém confere.
     */
    private static boolean markedWithin(List<String> lines, int writeLine) {
        int firstLineToInspect = Math.max(0, writeLine - MARKER_LOOKBEHIND);
        return lines.subList(firstLineToInspect, writeLine + 1).stream()
                .anyMatch(line -> line.contains(BATCH_COUNTED_MARKER));
    }

    private static final int MARKER_LOOKBEHIND = 6;
}
