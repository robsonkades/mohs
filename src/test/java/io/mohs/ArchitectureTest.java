package io.mohs;

import java.time.Instant;
import java.util.Set;
import java.util.stream.Collectors;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import org.jspecify.annotations.NullMarked;

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
}
