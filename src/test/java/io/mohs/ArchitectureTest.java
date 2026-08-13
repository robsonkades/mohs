package io.mohs;

import java.time.Instant;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import io.mohs.engine.DatabaseClock;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

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
}
