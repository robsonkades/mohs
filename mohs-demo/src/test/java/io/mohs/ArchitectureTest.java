/*
 * Copyright 2026 The Mohs Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mohs;

import java.time.Instant;
import java.util.Set;
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

import io.mohs.store.jdbc.DatabaseClock;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.assertj.core.api.Assertions.assertThat;

@AnalyzeClasses(packages = "io.mohs", importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

    /**
     * "Public API" means anything under io.mohs.. that is not one of the five known internal
     * packages. The rule lists the INTERNAL packages, which are stable, rather than the public
     * subpackages, which grow with every milestone — so a new public subpackage cannot slip out of
     * the rule by being forgotten.
     */
    private static final DescribedPredicate<JavaClass> PUBLIC_API =
            JavaClass.Predicates.resideInAPackage("io.mohs..")
                    .and(DescribedPredicate.not(JavaClass.Predicates.resideInAnyPackage(
                            "io.mohs.engine..", "io.mohs.store.jdbc..", "io.mohs.autoconfigure..",
                            "io.mohs.rest..", "io.mohs.test..")));

    @ArchTest
    static final ArchRule internal_packages_do_not_leak_into_public_api =
        noClasses().that(PUBLIC_API)
            .should().dependOnClassesThat().resideInAnyPackage("io.mohs.engine..", "io.mohs.store.jdbc..");

    @ArchTest
    static final ArchRule rest_only_sees_public_api =
        noClasses().that().resideInAPackage("io.mohs.rest..")
            .should().dependOnClassesThat().resideInAnyPackage("io.mohs.engine..", "io.mohs.store.jdbc..");

    @ArchTest
    static final ArchRule test_kit_does_not_leak_into_production =
        noClasses().that().resideOutsideOfPackage("io.mohs.test..")
            .should().dependOnClassesThat().resideInAPackage("io.mohs.test..");

    /**
     * The engine never sees JDBC: its ports are pure vocabulary, and it is the store module that
     * speaks SQL. The reactor already prevents the inverse (the store module depends on the
     * engine); this rule prevents the leak by TYPE — a {@code ResultSet} in a port signature, for
     * instance.
     */
    @ArchTest
    static final ArchRule engine_is_free_of_jdbc =
        noClasses().that().resideInAPackage("io.mohs.engine..")
            .should().dependOnClassesThat().resideInAnyPackage("java.sql..", "javax.sql..");

    /**
     * Only the starter knows about auto-configuration: no other module may quietly grow a hidden
     * {@code @AutoConfiguration}. There is exactly one named exception, the demo application's
     * bootstrap ({@code MohsApplication}), which is an application rather than a library.
     */
    @ArchTest
    static final ArchRule only_the_starter_speaks_boot_autoconfigure =
        noClasses().that().resideOutsideOfPackage("io.mohs.autoconfigure..")
            .and(DescribedPredicate.not(JavaClass.Predicates.equivalentTo(MohsApplication.class)))
            .should().dependOnClassesThat().resideInAPackage("org.springframework.boot.autoconfigure..");

    /**
     * {@link DatabaseClock} is the single exception: it IS the injected clock, so reading the real
     * clock there is the class's purpose rather than a violation — {@code sync()} samples the
     * database-to-application offset precisely so that {@code instant()} never has to do I/O.
     */
    private static final DescribedPredicate<JavaClass> IS_DATABASE_CLOCK =
            JavaClass.Predicates.equivalentTo(DatabaseClock.class);

    /**
     * Every "now" comes from the injected clock; reading it directly is forbidden.
     *
     * <p>{@code System.nanoTime()} is deliberately out of scope: it is monotonic time, which is
     * what measuring an interval requires, and it is not a wall-clock "now".
     */
    @ArchTest
    static final ArchRule engine_never_reads_wall_clock_directly =
        noClasses().that().resideInAnyPackage("io.mohs.engine..", "io.mohs.store.jdbc..")
            .and(DescribedPredicate.not(IS_DATABASE_CLOCK))
            .should().callMethod(Instant.class, "now")
            .orShould().callMethod(System.class, "currentTimeMillis");

    /**
     * Prefer {@code ReentrantLock} over {@code synchronized}/{@code wait}.
     *
     * <p>This is no longer about carrier pinning — JEP 491 in JDK 24 removed pinning by
     * {@code synchronized}/{@code Object.wait()} — but about the capabilities only an explicit lock
     * offers (JCIP ch. 13: {@code tryLock} with a timeout, interruptible acquisition, multiple
     * {@code Condition}s).
     *
     * <p>Only the {@code synchronized} METHOD modifier is caught; a {@code synchronized(lock) {…}}
     * block is not modelled by ArchUnit, which has no instruction-level bytecode inspection in its
     * public API. That is the same gap between a prose rule and an executable one that other checks
     * in this file have, recorded here rather than hidden.
     */
    @ArchTest
    static final ArchRule no_synchronized_methods_in_concurrency_critical_code =
        noClasses().that().resideInAnyPackage("io.mohs.engine..", "io.mohs.store.jdbc..")
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
        noClasses().that().resideInAnyPackage("io.mohs.engine..", "io.mohs.store.jdbc..")
            .should().dependOnClassesThat().belongToAnyOf(ThreadLocal.class, InheritableThreadLocal.class);

    /**
     * Every generated primary key is UUIDv7 — never sequential, never v4.
     *
     * <p>The JDK's v4 is pure randomness: it scatters across the whole index the inserts that v7
     * would keep localised at the tail, and it is not time-ordered, which is what keeps keyset
     * pagination possible.
     *
     * <p>The rule forbids the GENERATION, not the type:
     * {@code io.github.robsonkades.uuidv7.UUIDv7} legitimately returns a {@code java.util.UUID} and
     * does not trip it, because matching is by the target's owner. {@code accessTargetWhere} rather
     * than {@code callMethod} is deliberate: it also covers method references
     * ({@code UUID::randomUUID} is a {@code JavaMethodReference}, not a {@code JavaMethodCall}, and
     * would escape {@code callMethod}).
     *
     * <p>The other half of the invariant — no {@code IDENTITY}/{@code SERIAL}/
     * {@code AUTO_INCREMENT}/{@code SEQUENCE} in any schema — stays in prose, because ArchUnit does
     * not read SQL. Same gap recorded on the {@code synchronized} rule above.
     */
    @ArchTest
    static final ArchRule ids_are_generated_as_uuidv7_never_v4 =
        noClasses().should().accessTargetWhere(
                JavaAccess.Predicates.target(HasOwner.Predicates.With.owner(JavaClass.Predicates.type(UUID.class)))
                        .and(JavaAccess.Predicates.target(HasName.Predicates.name("randomUUID"))))
                .as("no classes should call or reference java.util.UUID.randomUUID()");

    /**
     * Every production package needs its own {@code package-info.java} carrying
     * {@code @NullMarked}: non-null by default is the project's convention, and a new package
     * without it silently degrades JSpecify's static-analysis signal with no compilation error to
     * show for it.
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
     * The dependency graph among the {@code io.mohs.core} subpackages
     * (job/schedule/definition/execution/event/resource) is acyclic by construction. That property
     * is stated in each {@code package-info.java}; this rule is what makes it executable rather
     * than merely written down.
     *
     * <p>It does not prescribe the exact direction of every edge — that shifts as the vocabulary
     * grows — only that no edge may close a cycle, which is the guarantee actually promised.
     */
    @ArchTest
    static final ArchRule core_subpackages_are_free_of_cycles =
        SlicesRuleDefinition.slices().matching("io.mohs.core.(*)..").should().beFreeOfCycles();

    /**
     * Same reasoning as {@link #core_subpackages_are_free_of_cycles}, applied to the resource
     * subpackages of {@code io.mohs.rest} (one per controller — see that package's
     * {@code package-info.java}).
     */
    @ArchTest
    static final ArchRule rest_subpackages_are_free_of_cycles =
        SlicesRuleDefinition.slices().matching("io.mohs.rest.(*)..").should().beFreeOfCycles();
}
