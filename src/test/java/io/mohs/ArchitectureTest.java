package io.mohs;

import java.time.Instant;
import java.util.Set;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.UUID;
import java.util.stream.Collectors;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaAccess;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.domain.properties.HasName;
import com.tngtech.archunit.core.domain.properties.HasOwner;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import com.tngtech.archunit.library.dependencies.SlicesRuleDefinition;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;

import io.mohs.jdbc.DatabaseClock;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.assertj.core.api.Assertions.assertThat;

@AnalyzeClasses(packages = "io.mohs", importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

    /**
     * "API pública" = qualquer coisa em io.mohs.. que não seja um dos 5
     * pacotes internos conhecidos. Lista os internos (estável, fixados em
     * M0) em vez dos subpacotes públicos (cresce a cada milestone) — evita
     * que um subpacote público novo escape da regra por esquecimento (ver
     * docs/adr/0013-public-api-subpackaging.md).
     */
    private static final DescribedPredicate<JavaClass> PUBLIC_API =
            JavaClass.Predicates.resideInAPackage("io.mohs..")
                    .and(DescribedPredicate.not(JavaClass.Predicates.resideInAnyPackage(
                            "io.mohs.engine..", "io.mohs.jdbc..", "io.mohs.autoconfigure..",
                            "io.mohs.rest..", "io.mohs.test..")));

    @ArchTest
    static final ArchRule internal_packages_do_not_leak_into_public_api =
        noClasses().that(PUBLIC_API)
            .should().dependOnClassesThat().resideInAnyPackage("io.mohs.engine..", "io.mohs.jdbc..");

    @ArchTest
    static final ArchRule rest_only_sees_public_api =
        noClasses().that().resideInAPackage("io.mohs.rest..")
            .should().dependOnClassesThat().resideInAnyPackage("io.mohs.engine..", "io.mohs.jdbc..");

    @ArchTest
    static final ArchRule test_kit_does_not_leak_into_production =
        noClasses().that().resideOutsideOfPackage("io.mohs.test..")
            .should().dependOnClassesThat().resideInAPackage("io.mohs.test..");

    /**
     * {@link DatabaseClock} é a única exceção: é o próprio relógio
     * injetado, então ler o relógio de verdade ali é o propósito da
     * classe, não uma violação — {@code sync()} amostra o offset
     * banco×app pra que {@code instant()} nunca precise fazer I/O.
     */
    private static final DescribedPredicate<JavaClass> IS_DATABASE_CLOCK =
            JavaClass.Predicates.equivalentTo(DatabaseClock.class);

    /**
     * "Todo agora vem do Clock injetado, leitura direta proibida" (CLAUDE.md)
     * — {@code System.nanoTime()} fica de fora de propósito, é a duração
     * monotônica que o próprio CLAUDE.md pede pra medir intervalo, não
     * "agora".
     */
    @ArchTest
    static final ArchRule engine_never_reads_wall_clock_directly =
        noClasses().that().resideInAnyPackage("io.mohs.engine..", "io.mohs.jdbc..")
            .and(DescribedPredicate.not(IS_DATABASE_CLOCK))
            .should().callMethod(Instant.class, "now")
            .orShould().callMethod(System.class, "currentTimeMillis");

    /**
     * "Prefira ReentrantLock a synchronized/wait" (CLAUDE.md) — não é mais
     * questão de pinning do carrier (JEP 491, JDK 24, eliminou o pinning por
     * {@code synchronized}/{@code Object.wait()}), é sobre as capacidades que
     * só o lock explícito dá (JCIP cap. 13: {@code tryLock} com timeout,
     * aquisição interruptível, {@code Condition} múltiplas). Só pega o
     * modificador {@code synchronized} de método; bloco {@code
     * synchronized(lock) { ... }} não é modelado por ArchUnit (não há
     * inspeção de bytecode ao nível de instrução na API pública) — mesma
     * lacuna entre regra em prosa e regra executável que já existe para
     * outras checagens deste arquivo, registrada aqui em vez de escondida.
     */
    @ArchTest
    static final ArchRule no_synchronized_methods_in_concurrency_critical_code =
        noClasses().that().resideInAnyPackage("io.mohs.engine..", "io.mohs.jdbc..")
            .should(new ArchCondition<JavaClass>("declare no synchronized methods") {
                @Override
                public void check(JavaClass javaClass, ConditionEvents events) {
                    javaClass.getMethods().stream()
                            .filter(method -> method.getModifiers().contains(JavaModifier.SYNCHRONIZED))
                            .forEach(method -> events.add(SimpleConditionEvent.violated(
                                    method, method.getFullName() + " is declared synchronized")));
                }
            });

    /** "ScopedValue em vez de ThreadLocal" (CLAUDE.md). */
    @ArchTest
    static final ArchRule no_thread_local_in_concurrency_critical_code =
        noClasses().that().resideInAnyPackage("io.mohs.engine..", "io.mohs.jdbc..")
            .should().dependOnClassesThat().belongToAnyOf(ThreadLocal.class, InheritableThreadLocal.class);

    /**
     * "Todo PK gerado é UUIDv7, nunca sequencial nem v4" (CLAUDE.md,
     * invariante) — o v4 do JDK é aleatório puro: espalha pelo índice
     * inteiro os inserts que o v7 manteria localizados na cauda, e não é
     * ordenável no tempo (o que mantém keyset possível — ADR-0040). A
     * regra proíbe a GERAÇÃO, não o tipo:
     * {@code io.github.robsonkades.uuidv7.UUIDv7} devolve
     * {@code java.util.UUID} legitimamente e não dispara (o matching é
     * pelo owner do alvo). {@code accessTargetWhere} em vez de
     * {@code callMethod} de propósito: cobre também method reference
     * ({@code UUID::randomUUID} é {@code JavaMethodReference}, não
     * {@code JavaMethodCall} — escaparia do {@code callMethod}; review
     * desta regra). A metade dos schemas (nenhum {@code IDENTITY}/
     * {@code SERIAL}/{@code AUTO_INCREMENT}/{@code SEQUENCE}) fica em
     * prosa no CLAUDE.md — ArchUnit não lê SQL, mesma lacuna registrada
     * na regra de {@code synchronized} acima.
     */
    @ArchTest
    static final ArchRule ids_are_generated_as_uuidv7_never_v4 =
        noClasses().should().accessTargetWhere(
                JavaAccess.Predicates.target(HasOwner.Predicates.With.owner(JavaClass.Predicates.type(UUID.class)))
                        .and(JavaAccess.Predicates.target(HasName.Predicates.name("randomUUID"))))
                .as("no classes should call or reference java.util.UUID.randomUUID()");

    /**
     * Todo pacote de produção precisa do próprio {@code package-info.java}
     * com {@code @NullMarked} — não-nulo por padrão é a convenção do
     * projeto (CLAUDE.md), e um pacote novo sem isso degrada o sinal de
     * análise estática de JSpecify silenciosamente, sem erro de compilação.
     */
    @ArchTest
    static void all_production_packages_declare_null_marked(JavaClasses classes) {
        Set<String> nullMarkedPackages = classes.stream()
                .filter(javaClass -> javaClass.getSimpleName().equals("package-info"))
                .filter(javaClass -> javaClass.isAnnotatedWith(NullMarked.class))
                .map(JavaClass::getPackageName)
                .collect(Collectors.toSet());
        Set<String> allPackages = classes.stream().map(JavaClass::getPackageName).collect(Collectors.toSet());
        assertThat(nullMarkedPackages).containsAll(allPackages);
    }

    /**
     * "O grafo de dependência resultante é acíclico por construção" — ADR-0013
     * já declara essa propriedade para os subpacotes de {@code io.mohs.core}
     * (job/schedule/definition/execution/event/resource); esta regra é o que
     * a torna executável em vez de só prosa em {@code package-info.java}. Não
     * prescreve a direção exata de cada aresta (isso muda pouco a pouco
     * conforme o vocabulário cresce) — só proíbe que ela feche um ciclo, que
     * é a garantia que a ADR de fato promete.
     */
    @ArchTest
    static final ArchRule core_subpackages_are_free_of_cycles =
        SlicesRuleDefinition.slices().matching("io.mohs.core.(*)..").should().beFreeOfCycles();

    /**
     * Mesmo raciocínio de {@link #core_subpackages_are_free_of_cycles}, para
     * os subpacotes de recurso de {@code io.mohs.rest} (um por controller,
     * ver {@code io.mohs.rest}'s {@code package-info.java}).
     */
    @ArchTest
    static final ArchRule rest_subpackages_are_free_of_cycles =
        SlicesRuleDefinition.slices().matching("io.mohs.rest.(*)..").should().beFreeOfCycles();

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
     * lacuna que o Javadoc de {@link #generated_ids_are_uuid_v7} registra ao
     * deixar metade daquele invariante em prosa. Aqui a metade que importa é
     * justamente a do SQL, então a varredura é o único instrumento que
     * enxerga o invariante.
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
